//! Worker configuration (ADR 0005).
//!
//! A Worker TOML file defines each Device Pool's executable, argument vector,
//! environment, state directory, and startup timeout with no shell command
//! strings and no dynamic reload: [`WorkerConfig::load`] is the only load
//! path, run once at process startup.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use gpq_domain::BackendKind;
use serde::{Deserialize, Deserializer};

/// Environment variables used by common accelerator vendors to select which
/// physical devices a process may use. Two Pools naming the same device under
/// the same key would violate the non-overlapping Device Pool invariant of
/// ADR 0005.
const DEVICE_SELECTOR_ENV_KEYS: &[&str] = &[
    "CUDA_VISIBLE_DEVICES",
    "HIP_VISIBLE_DEVICES",
    "ROCR_VISIBLE_DEVICES",
    "GPU_DEVICE_ORDINAL",
];

/// Top-level Worker TOML configuration: one Worker per host (ADR 0005).
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    /// This Worker's stable name, used in enrollment and credential storage.
    pub name: String,
    /// Outbound-only address of `gpq-remote`'s Worker gRPC endpoint (ADR 0004).
    pub remote_url: url::Url,
    /// Root directory for Worker-local state: credential fallback storage and
    /// the model hash cache.
    pub state_dir: PathBuf,
    /// Every Device Pool this Worker supervises. Never empty.
    pub pools: Vec<PoolConfig>,
}

/// One exclusively-owned Device Pool and the backend process it supervises
/// (ADR 0005).
#[derive(Debug, Clone, Deserialize)]
pub struct PoolConfig {
    /// Stable identity for this Pool on this host; unique among `pools`.
    pub key: String,
    /// Which managed runtime this Pool switches to exclusively.
    pub backend: BackendKind,
    /// Absolute path to the backend executable. No shell strings (ADR 0005).
    pub executable: PathBuf,
    /// Argument vector passed directly to the executable, never through a
    /// shell.
    #[serde(default)]
    pub args: Vec<String>,
    /// Extra environment variables for the managed process, including any
    /// accelerator device selector.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Working/state directory for this Pool's backend process.
    pub state_dir: PathBuf,
    /// How long to wait for the backend to become ready before treating the
    /// Pool as unready.
    #[serde(rename = "startup_timeout_secs", deserialize_with = "duration_secs")]
    pub startup_timeout: Duration,
    /// Loopback address the backend listens on.
    pub base_url: url::Url,
    /// Execution Slot count; defaults to [`BackendKind::default_slots`] when
    /// unset.
    #[serde(default)]
    pub slots: Option<u32>,
    /// Model files this Pool expects to find on disk.
    #[serde(default)]
    pub model_paths: Vec<PathBuf>,
    /// Expected SHA-256 hash per model path (ADR 0005); a mismatch is
    /// rejected rather than silently trusted.
    #[serde(default)]
    pub expected_hashes: BTreeMap<String, String>,
}

fn duration_secs<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let secs = u64::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
}

impl WorkerConfig {
    /// Loads and validates a Worker TOML configuration file.
    ///
    /// This is the only supported way to obtain a `WorkerConfig`: there is no
    /// dynamic reload, and no field is ever sourced from a shell command
    /// string (ADR 0005).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, does not parse as valid
    /// TOML for this schema, or fails validation (empty/duplicate pool keys,
    /// overlapping device selectors, a non-absolute executable, a zero
    /// startup timeout, or a malformed expected model hash).
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
            !self.pools.is_empty(),
            "worker config must declare at least one pool"
        );

        let mut seen_keys = BTreeSet::new();
        for pool in &self.pools {
            ensure!(
                seen_keys.insert(pool.key.as_str()),
                "duplicate pool key `{}`",
                pool.key
            );
        }

        check_device_overlap(&self.pools)?;
        ensure_state_dir(&self.state_dir)?;
        for pool in &self.pools {
            pool.validate()?;
        }
        Ok(())
    }
}

impl PoolConfig {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(!self.key.is_empty(), "pool key must not be empty");
        ensure!(
            self.executable.is_absolute(),
            "pool `{}` executable must be an absolute path, got `{}`",
            self.key,
            self.executable.display()
        );
        ensure!(
            self.startup_timeout > Duration::ZERO,
            "pool `{}` startup_timeout_secs must be greater than zero",
            self.key
        );
        for (path, hash) in &self.expected_hashes {
            ensure!(
                is_hex_sha256(hash),
                "pool `{}` expected hash for `{path}` must be 64 hex characters, got `{hash}`",
                self.key
            );
        }
        ensure_state_dir(&self.state_dir)?;
        Ok(())
    }
}

fn is_hex_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Rejects two Pools that claim the same physical device through a known
/// accelerator device-selector environment variable (ADR 0005: Device Pools
/// are non-overlapping).
fn check_device_overlap(pools: &[PoolConfig]) -> anyhow::Result<()> {
    for key in DEVICE_SELECTOR_ENV_KEYS {
        let mut claimed: BTreeMap<&str, &str> = BTreeMap::new();
        for pool in pools {
            let Some(value) = pool.env.get(*key) else {
                continue;
            };
            for device in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if let Some(other) = claimed.insert(device, pool.key.as_str()) {
                    bail!(
                        "pools `{other}` and `{}` both claim device `{device}` via {key}",
                        pool.key
                    );
                }
            }
        }
    }
    Ok(())
}

/// Creates `path` if missing and, on Unix, restricts it to owner-only access.
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
    use std::io::Write as _;

    use super::{WorkerConfig, is_hex_sha256};

    /// A minimal but complete two-pool configuration, parameterized over
    /// temp directories so `validate()`'s filesystem checks pass.
    fn sample_toml(
        state_dir: &std::path::Path,
        pool_a: &std::path::Path,
        pool_b: &std::path::Path,
    ) -> String {
        format!(
            r#"
name = "worker-1"
remote_url = "https://remote.example.internal"
state_dir = "{state}"

[[pools]]
key = "gpu0"
backend = "llama_cpp"
executable = "/usr/bin/true"
args = ["--model", "foo.gguf"]
state_dir = "{a}"
startup_timeout_secs = 30
base_url = "http://127.0.0.1:8081"
slots = 4
model_paths = ["/models/foo.gguf"]

[pools.env]
CUDA_VISIBLE_DEVICES = "0"

[pools.expected_hashes]
"/models/foo.gguf" = "{hash}"

[[pools]]
key = "gpu1"
backend = "comfyui"
executable = "/usr/bin/true"
state_dir = "{b}"
startup_timeout_secs = 45
base_url = "http://127.0.0.1:8082"

[pools.env]
CUDA_VISIBLE_DEVICES = "1"
"#,
            state = state_dir.display(),
            a = pool_a.display(),
            b = pool_b.display(),
            hash = "a".repeat(64),
        )
    }

    fn write_config(contents: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let path = dir.path().join("worker.toml");
        let mut file =
            std::fs::File::create(&path).unwrap_or_else(|err| panic!("create config: {err}"));
        file.write_all(contents.as_bytes())
            .unwrap_or_else(|err| panic!("write config: {err}"));
        (dir, path)
    }

    #[test]
    fn parses_full_sample() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let state = root.path().join("state");
        let pool_a = root.path().join("gpu0");
        let pool_b = root.path().join("gpu1");
        let (_dir, path) = write_config(&sample_toml(&state, &pool_a, &pool_b));

        let Ok(config) = WorkerConfig::load(&path) else {
            panic!("expected a valid config to load");
        };
        assert_eq!(config.name, "worker-1");
        assert_eq!(config.pools.len(), 2);
        assert_eq!(config.pools[0].key, "gpu0");
        assert_eq!(config.pools[0].slots, Some(4));
        assert_eq!(config.pools[1].slots, None);
        assert!(state.is_dir());
        assert!(pool_a.is_dir());
        assert!(pool_b.is_dir());
    }

    #[test]
    fn rejects_empty_pools() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let (_dir, path) = write_config(&format!(
            r#"
name = "worker-1"
remote_url = "https://remote.example.internal"
state_dir = "{state}"
pools = []
"#,
            state = root.path().join("state").display(),
        ));

        let Err(err) = WorkerConfig::load(&path) else {
            panic!("expected empty pools to be rejected");
        };
        assert!(err.to_string().contains("at least one pool"));
    }

    #[test]
    fn rejects_duplicate_pool_keys() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let state = root.path().join("state");
        let pool_a = root.path().join("gpu0");
        let pool_b = root.path().join("gpu1");
        let mut contents = sample_toml(&state, &pool_a, &pool_b);
        contents = contents.replacen("key = \"gpu1\"", "key = \"gpu0\"", 1);
        let (_dir, path) = write_config(&contents);

        let Err(err) = WorkerConfig::load(&path) else {
            panic!("expected duplicate pool keys to be rejected");
        };
        assert!(err.to_string().contains("duplicate pool key"));
    }

    #[test]
    fn rejects_overlapping_device_selectors() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let state = root.path().join("state");
        let pool_a = root.path().join("gpu0");
        let pool_b = root.path().join("gpu1");
        let mut contents = sample_toml(&state, &pool_a, &pool_b);
        contents = contents.replacen(
            "CUDA_VISIBLE_DEVICES = \"1\"",
            "CUDA_VISIBLE_DEVICES = \"0\"",
            1,
        );
        let (_dir, path) = write_config(&contents);

        let Err(err) = WorkerConfig::load(&path) else {
            panic!("expected overlapping device selectors to be rejected");
        };
        assert!(err.to_string().contains("both claim device"));
    }

    #[test]
    fn rejects_relative_executable() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let state = root.path().join("state");
        let pool_a = root.path().join("gpu0");
        let pool_b = root.path().join("gpu1");
        let contents = sample_toml(&state, &pool_a, &pool_b).replacen(
            "executable = \"/usr/bin/true\"",
            "executable = \"llama-server\"",
            1,
        );
        let (_dir, path) = write_config(&contents);

        let Err(err) = WorkerConfig::load(&path) else {
            panic!("expected relative executable to be rejected");
        };
        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn rejects_zero_startup_timeout() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let state = root.path().join("state");
        let pool_a = root.path().join("gpu0");
        let pool_b = root.path().join("gpu1");
        let contents = sample_toml(&state, &pool_a, &pool_b).replacen(
            "startup_timeout_secs = 30",
            "startup_timeout_secs = 0",
            1,
        );
        let (_dir, path) = write_config(&contents);

        let Err(err) = WorkerConfig::load(&path) else {
            panic!("expected zero startup timeout to be rejected");
        };
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn rejects_malformed_expected_hash() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let state = root.path().join("state");
        let pool_a = root.path().join("gpu0");
        let pool_b = root.path().join("gpu1");
        let contents =
            sample_toml(&state, &pool_a, &pool_b).replacen(&"a".repeat(64), "not-a-hash", 1);
        let (_dir, path) = write_config(&contents);

        let Err(err) = WorkerConfig::load(&path) else {
            panic!("expected malformed hash to be rejected");
        };
        assert!(err.to_string().contains("64 hex characters"));
    }

    #[test]
    fn hex_sha256_validation() {
        assert!(is_hex_sha256(&"a".repeat(64)));
        assert!(!is_hex_sha256(&"a".repeat(63)));
        assert!(!is_hex_sha256(
            "not-hex-not-hex-not-hex-not-hex-not-hex-not-hex-not-hex-0123"
        ));
    }
}
