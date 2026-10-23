//! Worker configuration and trust-boundary validation.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, ensure};
use gpq_domain::{ModelAssetRole, ModelFamily, SamplingPreset};
use serde::{Deserialize, Deserializer, Serialize};

const DEVICE_SELECTOR_ENV_KEYS: &[&str] = &[
    "CUDA_VISIBLE_DEVICES",
    "HIP_VISIBLE_DEVICES",
    "ROCR_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL",
];

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CredentialStorage {
    /// Use the operating system's credential store (the default).
    #[default]
    Platform,
    /// Store the credential in an owner-only file below `state_dir`.
    File,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    pub name: String,
    pub remote_url: url::Url,
    pub state_dir: PathBuf,
    #[serde(default)]
    pub credential_storage: CredentialStorage,
    pub pools: Vec<PoolConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PoolConfig {
    pub key: String,
    /// Native backend/device selector: `metal`, `vulkan0`, `cuda0`, or `cpu`.
    pub backend: String,
    /// Parent and infer child use this authenticated loopback control address.
    pub listen: String,
    pub state_dir: PathBuf,
    #[serde(rename = "startup_timeout_secs", deserialize_with = "duration_secs")]
    pub startup_timeout: Duration,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub shape: ShapeLimits,
    #[serde(default)]
    pub models: Vec<ModelConfig>,
    #[serde(default)]
    pub extra_image_loras: Vec<PathBuf>,
    /// Optional configured SHA-256 per asset path; assets are always hashed
    /// locally and compared with this value when supplied.
    #[serde(default)]
    pub expected_hashes: BTreeMap<String, String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelConfig {
    pub family: ModelFamily,
    /// Files keyed by `ModelAssetRole`. Runtime-required roles are validated
    /// for the selected family before any native process is started.
    pub assets: BTreeMap<ModelAssetRole, PathBuf>,
    #[serde(default)]
    pub default_loras: Vec<ConfiguredLora>,
    pub sampling: Option<SamplingPreset>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConfiguredLora {
    pub path: PathBuf,
    pub scale: f32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub params_backend: Option<String>,
    pub max_vram: Option<String>,
    pub threads: u32,
    pub vae_tiling: bool,
    pub model_args: Vec<String>,
    pub module_backends: BTreeMap<String, String>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            params_backend: None,
            max_vram: None,
            threads: u32::try_from(
                std::thread::available_parallelism()
                    .map_or(1, |parallelism| parallelism.get().min(1024)),
            )
            .unwrap_or(1),
            vae_tiling: false,
            model_args: Vec::new(),
            module_backends: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ShapeLimits {
    #[serde(rename = "max_image_pixels")]
    pub image_pixels: u64,
    #[serde(rename = "max_video_pixels")]
    pub video_pixels: u64,
    #[serde(rename = "max_video_frames")]
    pub video_frames: u32,
    #[serde(rename = "max_steps")]
    pub steps: u32,
}

impl Default for ShapeLimits {
    fn default() -> Self {
        Self {
            image_pixels: 16_777_216,
            video_pixels: 64_000_000,
            video_frames: 97,
            steps: 100,
        }
    }
}

fn duration_secs<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Duration::from_secs(u64::deserialize(deserializer)?))
}

impl WorkerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading worker config {}", path.display()))?;
        let config: Self = toml::from_str(&raw)
            .with_context(|| format!("parsing worker config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.name.trim().is_empty(),
            "worker name must not be empty"
        );
        #[cfg(not(unix))]
        ensure!(
            self.credential_storage != CredentialStorage::File,
            "credential_storage = \"file\" is supported only on Unix"
        );
        ensure!(
            !self.pools.is_empty(),
            "worker config must declare at least one pool"
        );
        let mut seen_keys = BTreeSet::new();
        let mut seen_listeners = BTreeSet::new();
        for pool in &self.pools {
            ensure!(
                seen_keys.insert(pool.key.as_str()),
                "duplicate pool key `{}`",
                pool.key
            );
            pool.validate()?;
            let listener: SocketAddr = pool.listen.parse().with_context(|| {
                format!("pool `{}` listen must be an IP:port address", pool.key)
            })?;
            ensure!(
                listener.port() != 0,
                "pool `{}` listen port must be nonzero",
                pool.key
            );
            ensure!(
                seen_listeners.insert(listener),
                "pool `{}` duplicates a native listener",
                pool.key
            );
        }
        check_device_overlap(&self.pools)?;
        ensure_state_dir(&self.state_dir)?;
        Ok(())
    }
}

impl PoolConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.key.trim().is_empty(), "pool key must not be empty");
        ensure!(
            valid_device_selector(&self.backend),
            "pool `{}` backend must name a supported native device",
            self.key
        );
        ensure!(
            self.startup_timeout > Duration::ZERO,
            "pool `{}` startup_timeout_secs must be greater than zero",
            self.key
        );
        ensure!(
            self.runtime.threads > 0 && self.runtime.threads <= 1024,
            "pool `{}` runtime.threads must be within 1..=1024",
            self.key
        );
        if let Some(params_backend) = &self.runtime.params_backend {
            ensure!(
                valid_params_backend(params_backend),
                "pool `{}` runtime.params_backend is invalid",
                self.key
            );
        }
        if let Some(max_vram) = &self.runtime.max_vram {
            ensure!(
                valid_vram_assignment(max_vram),
                "pool `{}` runtime.max_vram must be a positive scalar GiB budget or device-to-GiB assignments",
                self.key
            );
        }
        for argument in &self.runtime.model_args {
            ensure!(
                !argument.is_empty() && !argument.contains('\0') && !argument.contains(','),
                "pool `{}` runtime.model_args entries must be nonempty and cannot contain NUL or comma",
                self.key
            );
        }
        for (module, backend) in &self.runtime.module_backends {
            ensure!(
                matches!(module.as_str(), "diffusion" | "te" | "clip_vision" | "vae"),
                "pool `{}` runtime.module_backends contains unknown module `{module}`",
                self.key
            );
            ensure!(
                valid_device_selector(backend),
                "pool `{}` module `{module}` has unsupported backend `{backend}`",
                self.key
            );
        }
        for name in self.env.keys() {
            ensure!(
                DEVICE_SELECTOR_ENV_KEYS.contains(&name.as_str()) || name == "VK_DRIVER_FILES",
                "pool `{}` env may contain only recognized accelerator device selectors or VK_DRIVER_FILES, not `{name}`",
                self.key
            );
        }
        ensure!(
            self.shape.image_pixels > 0,
            "pool `{}` max_image_pixels must be positive",
            self.key
        );
        ensure!(
            self.shape.video_pixels > 0,
            "pool `{}` max_video_pixels must be positive",
            self.key
        );
        ensure!(
            self.shape.video_frames >= 1,
            "pool `{}` max_video_frames must be positive",
            self.key
        );
        ensure!(
            self.shape.steps >= 1,
            "pool `{}` max_steps must be positive",
            self.key
        );
        ensure_state_dir(&self.state_dir)?;

        let mut model_hashes = BTreeSet::new();
        for model in &self.models {
            validate_model(self, model)?;
            // A single immutable manifest may be advertised only once.
            let canonical =
                serde_json::to_vec(model).context("serializing configured model manifest")?;
            ensure!(
                model_hashes.insert(gpq_domain::ContentHash::digest(&canonical)),
                "pool `{}` contains a duplicate model manifest",
                self.key
            );
        }
        for path in &self.extra_image_loras {
            ensure_asset_path(&self.key, "extra image LoRA", path)?;
        }
        for (path, hash) in &self.expected_hashes {
            ensure!(
                is_hex_sha256(hash),
                "pool `{}` expected hash for `{path}` must be 64 hex characters",
                self.key
            );
        }
        Ok(())
    }
}

fn validate_model(pool: &PoolConfig, model: &ModelConfig) -> anyhow::Result<()> {
    let mut required = BTreeSet::from([
        ModelAssetRole::Diffusion,
        ModelAssetRole::TextEncoder,
        ModelAssetRole::Vae,
    ]);
    match model.family {
        ModelFamily::QwenImage21 => {
            required.insert(ModelAssetRole::VisionEncoder);
        }
        ModelFamily::MinimaxH3 => {
            required.insert(ModelAssetRole::AudioVae);
        }
    }
    ensure!(
        required.iter().all(|role| model.assets.contains_key(role)),
        "pool `{}` model {:?} is missing required asset roles",
        pool.key,
        model.family
    );
    for (role, path) in &model.assets {
        ensure_asset_path(&pool.key, &format!("{role:?} model asset"), path)?;
    }
    let extra_roles: Vec<_> = model
        .assets
        .keys()
        .filter(|role| !required.contains(role))
        .collect();
    ensure!(
        extra_roles.is_empty(),
        "pool `{}` model {:?} declares unsupported asset roles {extra_roles:?}",
        pool.key,
        model.family
    );
    for lora in &model.default_loras {
        ensure!(
            lora.scale.is_finite(),
            "pool `{}` default LoRA scale must be finite",
            pool.key
        );
        ensure_asset_path(&pool.key, "default LoRA", &lora.path)?;
    }
    if let Some(sampling) = &model.sampling {
        sampling
            .resolve(None, None, None)
            .with_context(|| format!("pool `{}` sampling preset is invalid", pool.key))?;
    }
    Ok(())
}

fn ensure_asset_path(pool: &str, label: &str, path: &Path) -> anyhow::Result<()> {
    ensure!(
        path.is_absolute(),
        "pool `{pool}` {label} path must be absolute: {}",
        path.display()
    );
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("pool `{pool}` {label} {} is unavailable", path.display()))?;
    ensure!(
        metadata.is_file(),
        "pool `{pool}` {label} {} is not a regular file",
        path.display()
    );
    Ok(())
}

fn is_hex_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn valid_device_selector(value: &str) -> bool {
    if value == "cpu" {
        return true;
    }
    ["metal", "vulkan", "cuda"].iter().any(|prefix| {
        value.strip_prefix(prefix).is_some_and(|suffix| {
            suffix.is_empty() || suffix.bytes().all(|byte| byte.is_ascii_digit())
        })
    })
}

fn valid_params_backend(value: &str) -> bool {
    if value == "disk" {
        return true;
    }
    !value.is_empty()
        && value.split(',').all(|assignment| {
            let (module, device) = assignment.split_once('=').unwrap_or(("", assignment));
            (module.is_empty()
                || matches!(
                    module,
                    "all" | "default" | "*" | "diffusion" | "te" | "clip_vision" | "vae"
                ))
                && valid_device_selector(device)
        })
}

fn valid_vram_assignment(value: &str) -> bool {
    if value.parse::<u32>().is_ok_and(|amount| amount > 0) {
        return true;
    }
    !value.trim().is_empty()
        && value.split(',').all(|assignment| {
            let Some((device, gibibytes)) = assignment.split_once('=') else {
                return false;
            };
            valid_device_selector(device) && gibibytes.parse::<u32>().is_ok_and(|amount| amount > 0)
        })
}

fn check_device_overlap(pools: &[PoolConfig]) -> anyhow::Result<()> {
    for key in DEVICE_SELECTOR_ENV_KEYS {
        let mut claimed: BTreeMap<&str, &str> = BTreeMap::new();
        for pool in pools {
            let Some(value) = pool.env.get(*key) else {
                continue;
            };
            for device in value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                if let Some(other) = claimed.insert(device, pool.key.as_str()) {
                    anyhow::bail!(
                        "pools `{other}` and `{}` both claim device `{device}` via {key}",
                        pool.key
                    );
                }
            }
        }
    }

    for (index, left) in pools.iter().enumerate() {
        let left_devices = native_device_claims(left);
        for right in &pools[index + 1..] {
            let right_devices = native_device_claims(right);
            for left_device in &left_devices {
                for right_device in &right_devices {
                    if normalize_device(left_device) == normalize_device(right_device)
                        && !cuda_pools_are_partitioned(left, right, left_device)
                    {
                        anyhow::bail!(
                            "pools `{}` and `{}` both claim native device `{left_device}`",
                            left.key,
                            right.key
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn native_device_claims(pool: &PoolConfig) -> Vec<String> {
    let mut devices = vec![pool.backend.clone()];
    devices.extend(pool.runtime.module_backends.values().cloned());
    if let Some(params_backend) = &pool.runtime.params_backend
        && params_backend != "disk"
    {
        devices.extend(params_backend.split(',').map(|assignment| {
            assignment
                .split_once('=')
                .map_or(assignment, |(_, device)| device)
                .to_owned()
        }));
    }
    devices.retain(|device| device != "cpu");
    devices
}

fn normalize_device(device: &str) -> String {
    match device {
        "metal" => "metal0".to_owned(),
        "vulkan" => "vulkan0".to_owned(),
        "cuda" => "cuda0".to_owned(),
        other => other.to_owned(),
    }
}

fn cuda_pools_are_partitioned(left: &PoolConfig, right: &PoolConfig, device: &str) -> bool {
    if !normalize_device(device).starts_with("cuda") {
        return false;
    }
    DEVICE_SELECTOR_ENV_KEYS.iter().any(|key| {
        left.env
            .get(*key)
            .zip(right.env.get(*key))
            .is_some_and(|(left, right)| left != right)
    })
}

fn ensure_state_dir(path: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("creating state dir {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("securing state dir {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::Duration;

    use super::{
        PoolConfig, RuntimeConfig, ShapeLimits, check_device_overlap, is_hex_sha256,
        valid_device_selector, valid_params_backend, valid_vram_assignment,
    };

    fn pool(key: &str, backend: &str, env: BTreeMap<String, String>) -> PoolConfig {
        PoolConfig {
            key: key.to_owned(),
            backend: backend.to_owned(),
            listen: "127.0.0.1:4001".to_owned(),
            state_dir: PathBuf::from(format!("/tmp/{key}")),
            startup_timeout: Duration::from_secs(10),
            runtime: RuntimeConfig::default(),
            shape: ShapeLimits::default(),
            models: Vec::new(),
            extra_image_loras: Vec::new(),
            expected_hashes: BTreeMap::new(),
            env,
        }
    }

    #[test]
    fn sha256_config_validation() {
        assert!(is_hex_sha256(&"a".repeat(64)));
        assert!(!is_hex_sha256(&"a".repeat(63)));
        assert!(!is_hex_sha256(&"z".repeat(64)));
    }

    #[test]
    fn runtime_device_assignments_are_checked() {
        assert!(valid_device_selector("metal"));
        assert!(valid_device_selector("vulkan0"));
        assert!(!valid_device_selector("vulkan-any"));
        assert!(valid_params_backend("disk"));
        assert!(valid_params_backend("diffusion=vulkan0,te=cpu"));
        assert!(!valid_params_backend("diffusion=missing0"));
        assert!(valid_vram_assignment("vulkan0=20,metal0=32"));
        assert!(valid_vram_assignment("24"));
        assert!(valid_vram_assignment("12"));
        assert!(!valid_vram_assignment("0"));
        assert!(!valid_vram_assignment("vulkan0=0"));
    }

    #[test]
    fn native_device_pools_cannot_claim_the_same_device() {
        assert!(
            check_device_overlap(&[
                pool("pool-a", "vulkan0", BTreeMap::new()),
                pool("pool-b", "vulkan0", BTreeMap::new()),
            ])
            .is_err()
        );
        assert!(
            check_device_overlap(&[
                pool("pool-a", "metal", BTreeMap::new()),
                pool("pool-b", "metal0", BTreeMap::new()),
            ])
            .is_err()
        );
    }

    #[test]
    fn cuda_device_index_is_scoped_by_distinct_visibility_selectors() {
        assert!(
            check_device_overlap(&[
                pool(
                    "pool-a",
                    "cuda0",
                    BTreeMap::from([("CUDA_VISIBLE_DEVICES".to_owned(), "0".to_owned())]),
                ),
                pool(
                    "pool-b",
                    "cuda0",
                    BTreeMap::from([("CUDA_VISIBLE_DEVICES".to_owned(), "1".to_owned())]),
                ),
            ])
            .is_ok()
        );
    }
}
