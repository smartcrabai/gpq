//! Device Pool supervision for the native inference child.
//!
//! Each configured pool owns exactly one managed `gpq infer` child and one
//! execution slot. The child is the same executable, isolated in the existing
//! process group/Windows Job Object and identified durably by `process.rs`.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use gpq_domain::{
    ArtifactId, AttemptId, ContentHash, FailureKind, GenerationParameters, ModelAssetRole,
    ModelManifest,
};
use gpq_inference::{InferenceRequest, ResolvedLora, ResolvedModelFiles, RuntimeOptions};
use tokio::io::AsyncBufReadExt;

use crate::backend::{self, Backend, BackendError};
use crate::config::{PoolConfig, ShapeLimits, WorkerConfig};
use crate::models::{self, ConfiguredExtraLora, ConfiguredModel};
use crate::process::{self, ManagedProcess};

pub(crate) const PROCESS_IDENTITY_FILE: &str = "process-identity.json";
const TERMINATE_GRACE: Duration = Duration::from_secs(10);
const PROBE_RETRY_INTERVAL: Duration = Duration::from_millis(500);
const IDLE_UNLOAD_AFTER: Duration = Duration::from_mins(5);

fn lock_state(mutex: &std::sync::Mutex<PoolState>) -> std::sync::MutexGuard<'_, PoolState> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Debug, Clone)]
pub struct ModelAdvertisementData {
    pub version_sha256: ContentHash,
    pub manifest: ModelManifest,
}

#[derive(Debug, Clone)]
pub struct PoolAdvertisementData {
    pub pool_key: String,
    pub accelerator: String,
    pub backend_version: String,
    pub ready: bool,
    pub unready_reason: Option<String>,
    pub slots_total: u32,
    pub slots_busy: Vec<AttemptId>,
    pub models: Vec<ModelAdvertisementData>,
    pub image_loras: Vec<ContentHash>,
    pub shape_limits: ShapeLimits,
}

pub struct SlotLease {
    pub attempt_id: AttemptId,
    entry: Arc<PoolEntry>,
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        let mut state = lock_state(&self.entry.state);
        state.release(self.attempt_id);
        drop(state);
        let _ = self.entry.changes_tx.send(());
    }
}

struct PoolState {
    process: Option<ManagedProcess>,
    backend: Option<Arc<dyn Backend>>,
    ready: bool,
    unready_reason: Option<String>,
    slots_total: u32,
    slots_free: u32,
    restart_requested: bool,
    termination_pending: bool,
    busy: Vec<AttemptId>,
    accelerator: String,
    accelerator_version: String,
    backend_version: String,
    probes: BTreeMap<String, bool>,
    last_activity: Instant,
}

impl PoolState {
    fn not_yet_started() -> Self {
        Self {
            process: None,
            backend: None,
            ready: false,
            unready_reason: Some("not yet started".to_owned()),
            slots_total: 0,
            slots_free: 0,
            restart_requested: false,
            termination_pending: false,
            busy: Vec::new(),
            accelerator: String::new(),
            accelerator_version: String::new(),
            backend_version: String::new(),
            probes: BTreeMap::new(),
            last_activity: Instant::now(),
        }
    }

    fn try_acquire(&mut self, attempt_id: AttemptId) -> bool {
        if !self.ready || self.slots_free == 0 {
            return false;
        }
        self.slots_free = 0;
        self.busy.push(attempt_id);
        self.last_activity = Instant::now();
        true
    }

    fn release(&mut self, attempt_id: AttemptId) {
        let before = self.busy.len();
        self.busy.retain(|id| *id != attempt_id);
        if self.busy.len() < before {
            self.slots_free = if self.ready { self.slots_total } else { 0 };
        }
        self.last_activity = Instant::now();
    }
}

struct PoolEntry {
    config: PoolConfig,
    config_path: PathBuf,
    models: Vec<ConfiguredModel>,
    image_loras: BTreeMap<ContentHash, ConfiguredExtraLora>,
    op_lock: tokio::sync::Mutex<()>,
    state: std::sync::Mutex<PoolState>,
    changes_tx: tokio::sync::watch::Sender<()>,
}

struct TerminatingProcessGuard {
    entry: Arc<PoolEntry>,
    process: Option<ManagedProcess>,
}

impl TerminatingProcessGuard {
    fn restore(&mut self, interrupted: bool) {
        let Some(process) = self.process.take() else {
            return;
        };
        let mut state = lock_state(&self.entry.state);
        state.process = Some(process);
        state.termination_pending = true;
        state.backend = None;
        state.ready = false;
        if interrupted {
            state.unready_reason = Some(
                "native inference child tree cleanup interrupted; verification pending".to_owned(),
            );
        }
        state.slots_total = 0;
        state.slots_free = 0;
        state.probes.clear();
        drop(state);
        let _ = self.entry.changes_tx.send(());
    }
}

impl Drop for TerminatingProcessGuard {
    fn drop(&mut self) {
        self.restore(true);
    }
}

pub struct PoolSupervisor {
    pools: BTreeMap<String, Arc<PoolEntry>>,
    changes_tx: tokio::sync::watch::Sender<()>,
}

impl PoolSupervisor {
    pub async fn start(config: &WorkerConfig, config_path: &Path) -> anyhow::Result<Self> {
        let config_path = std::fs::canonicalize(config_path)
            .with_context(|| format!("resolving worker config path {}", config_path.display()))?;
        let (changes_tx, _receiver) = tokio::sync::watch::channel(());
        let mut pools = BTreeMap::new();
        for pool_config in &config.pools {
            let identity_file = pool_config.state_dir.join(PROCESS_IDENTITY_FILE);
            let ownership = process::kill_stale(&identity_file, TERMINATE_GRACE)
                .await
                .with_context(|| {
                    format!(
                        "checking pool `{}` for a stale managed process",
                        pool_config.key
                    )
                })?;
            tracing::info!(pool = %pool_config.key, ?ownership, "checked for a previous run's managed process");
            let (models, image_loras, scan_error) = match models::scan_models(pool_config) {
                Ok((models, image_loras)) => (models, image_loras, None),
                Err(error) => (Vec::new(), BTreeMap::new(), Some(error.to_string())),
            };
            let mut initial_state = PoolState::not_yet_started();
            if let Some(reason) = &scan_error {
                initial_state.unready_reason = Some(reason.clone());
            }
            let entry = Arc::new(PoolEntry {
                config: pool_config.clone(),
                config_path: config_path.clone(),
                models,
                image_loras,
                op_lock: tokio::sync::Mutex::new(()),
                state: std::sync::Mutex::new(initial_state),
                changes_tx: changes_tx.clone(),
            });
            if scan_error.is_none()
                && let Err(error) = start_pool_process(&entry).await
            {
                lock_state(&entry.state).unready_reason = Some(error.message);
            }
            pools.insert(pool_config.key.clone(), entry);
        }
        Ok(Self { pools, changes_tx })
    }

    #[must_use]
    pub fn capabilities(&self) -> Vec<PoolAdvertisementData> {
        self.pools
            .values()
            .map(|entry| {
                let state = lock_state(&entry.state);
                PoolAdvertisementData {
                    pool_key: entry.config.key.clone(),
                    accelerator: state.accelerator.clone(),
                    backend_version: state.backend_version.clone(),
                    ready: state.ready,
                    unready_reason: state.unready_reason.clone(),
                    slots_total: state.slots_total,
                    slots_busy: state.busy.clone(),
                    models: entry
                        .models
                        .iter()
                        .map(|model| ModelAdvertisementData {
                            version_sha256: model.manifest_hash,
                            manifest: model.manifest.clone(),
                        })
                        .collect(),
                    image_loras: entry.image_loras.keys().copied().collect(),
                    shape_limits: entry.config.shape.clone(),
                }
            })
            .collect()
    }

    #[must_use]
    pub fn watch_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes_tx.subscribe()
    }

    #[must_use]
    pub fn backend(&self, pool_key: &str) -> Option<Arc<dyn Backend>> {
        let entry = self.pools.get(pool_key)?;
        lock_state(&entry.state).backend.clone()
    }

    pub fn build_inference_request(
        &self,
        pool_key: &str,
        model_version: ContentHash,
        parameters: GenerationParameters,
        inputs: BTreeMap<ArtifactId, PathBuf>,
        output_dir: PathBuf,
        seed: u64,
    ) -> anyhow::Result<InferenceRequest> {
        let entry = self
            .pools
            .get(pool_key)
            .with_context(|| format!("unknown pool `{pool_key}`"))?;
        let model = entry
            .models
            .iter()
            .find(|model| model.manifest_hash == model_version)
            .with_context(|| {
                format!("model {model_version} is not registered on pool `{pool_key}`")
            })?;
        model
            .verify_assets()
            .context("verifying pinned model assets remain unchanged")?;
        parameters
            .validate_for_family(model.manifest.family)
            .context("validating typed generation parameters for the pinned model")?;
        gpq_domain::validate_seed(seed)
            .context("generation seed exceeds the native signed 64-bit range")?;
        let mut extra_loras = BTreeMap::new();
        if let GenerationParameters::Image(image) = &parameters {
            for selection in &image.loras {
                let lora = entry.image_loras.get(&selection.sha256).with_context(|| {
                    format!(
                        "image LoRA {} is not configured on pool `{pool_key}`",
                        selection.sha256
                    )
                })?;
                lora.snapshot.verify()?;
                extra_loras.insert(selection.sha256, lora.path.clone());
            }
        }
        Ok(InferenceRequest {
            model: resolved_model(model),
            parameters,
            inputs,
            extra_loras,
            output_dir,
            seed,
            runtime: runtime_options(&entry.config),
        })
    }

    pub async fn ensure_runtime_and_acquire_slot(
        &self,
        pool_key: &str,
        model_version: ContentHash,
        attempt_id: AttemptId,
    ) -> Result<SlotLease, BackendError> {
        let Some(entry) = self.pools.get(pool_key).cloned() else {
            return Err(internal_error(format!("unknown pool `{pool_key}`")));
        };
        let Some(model) = entry
            .models
            .iter()
            .find(|model| model.manifest_hash == model_version)
        else {
            return Err(BackendError {
                kind: FailureKind::ModelUnavailable,
                message: format!("model {model_version} is not registered on pool `{pool_key}`"),
                retry_hint: false,
            });
        };
        if let Err(error) = model.verify_assets() {
            return Err(BackendError {
                kind: FailureKind::ModelUnavailable,
                message: format!("model {model_version} changed since advertisement: {error:#}"),
                retry_hint: false,
            });
        }
        let _op_guard = entry.op_lock.lock().await;
        let (running, has_active_attempt) = {
            let state = lock_state(&entry.state);
            (
                state.process.is_some() && state.ready,
                !state.busy.is_empty(),
            )
        };
        if !running && has_active_attempt {
            return Err(BackendError {
                kind: FailureKind::BackendCrashed,
                message: format!("pool `{pool_key}` still owns an active attempt during recovery"),
                retry_hint: true,
            });
        }
        if !running {
            let old_process = {
                let mut state = lock_state(&entry.state);
                state.restart_requested = true;
                state.termination_pending = state.process.is_some();
                state.backend = None;
                state.ready = false;
                state.unready_reason = Some("native inference child recovery pending".to_owned());
                state.slots_total = 0;
                state.slots_free = 0;
                state.probes.clear();
                state.process.take()
            };
            let _ = entry.changes_tx.send(());
            if let Some(old_process) = old_process
                && !terminate_managed_process(
                    &entry,
                    old_process,
                    "native inference child terminated for recovery",
                )
                .await
            {
                return Err(BackendError {
                    kind: FailureKind::BackendCrashed,
                    message: lock_state(&entry.state)
                        .unready_reason
                        .clone()
                        .unwrap_or_else(|| "native child tree cleanup failed".to_owned()),
                    retry_hint: true,
                });
            }
            start_pool_process(&entry).await?;
            let state = lock_state(&entry.state);
            if !state.ready {
                return Err(BackendError {
                    kind: FailureKind::BackendCrashed,
                    message: state
                        .unready_reason
                        .clone()
                        .unwrap_or_else(|| "native pool failed to become ready".to_owned()),
                    retry_hint: true,
                });
            }
        }
        if !lock_state(&entry.state).try_acquire(attempt_id) {
            return Err(BackendError {
                kind: FailureKind::Internal,
                message: format!("pool `{pool_key}` has no free execution slot"),
                retry_hint: true,
            });
        }
        let _ = entry.changes_tx.send(());
        Ok(SlotLease {
            attempt_id,
            entry: Arc::clone(&entry),
        })
    }

    pub async fn release_idle(&self, now: Instant) {
        for entry in self.pools.values() {
            let _op_guard = entry.op_lock.lock().await;
            let should_unload = {
                let state = lock_state(&entry.state);
                state.process.is_some()
                    && state.busy.is_empty()
                    && now.saturating_duration_since(state.last_activity) >= IDLE_UNLOAD_AFTER
            };
            if !should_unload {
                continue;
            }
            let backend = lock_state(&entry.state).backend.clone();
            let released = match backend {
                Some(backend) => backend.release_memory().await.unwrap_or(false),
                None => false,
            };
            if released {
                lock_state(&entry.state).last_activity = now;
                continue;
            }
            let process = {
                let mut state = lock_state(&entry.state);
                state.termination_pending = true;
                state.ready = false;
                state.unready_reason = Some("idle: native inference child unloading".to_owned());
                state.slots_total = 0;
                state.slots_free = 0;
                state.backend = None;
                state.probes.clear();
                state.process.take()
            };
            let _ = entry.changes_tx.send(());
            if let Some(process) = process {
                let _ = terminate_managed_process(
                    entry,
                    process,
                    "idle: native inference child unloaded",
                )
                .await;
            }
        }
    }

    pub async fn force_reclaim(&self, pool_key: &str, attempt_id: AttemptId) {
        let Some(entry) = self.pools.get(pool_key) else {
            return;
        };
        let _op_guard = entry.op_lock.lock().await;
        let process = {
            let mut state = lock_state(&entry.state);
            if !state.busy.contains(&attempt_id) {
                return;
            }
            let Some(process) = state.process.take() else {
                return;
            };
            state.restart_requested = true;
            state.termination_pending = true;
            state.ready = false;
            state.unready_reason = Some("native inference child force-terminating".to_owned());
            state.slots_total = 0;
            state.slots_free = 0;
            state.backend = None;
            state.probes.clear();
            drop(state);
            let _ = entry.changes_tx.send(());
            process
        };
        let _ =
            terminate_managed_process(entry, process, "native inference child force-terminated")
                .await;
    }

    pub async fn shutdown(&self) {
        for entry in self.pools.values() {
            let _op_guard = entry.op_lock.lock().await;
            let process = {
                let mut state = lock_state(&entry.state);
                state.restart_requested = false;
                state.termination_pending = false;
                state.process.take()
            };
            if let Some(mut process) = process {
                let _ = process.terminate_tree(TERMINATE_GRACE).await;
            }
            mark_unready(entry, "worker shutting down");
        }
    }

    pub async fn check_process_liveness(&self) {
        for entry in self.pools.values() {
            let _op_guard = entry.op_lock.lock().await;
            let (exited, terminate_pending, restart_requested) = {
                let mut state = lock_state(&entry.state);
                let exited = state
                    .process
                    .as_mut()
                    .and_then(|process| process.try_wait().ok().flatten());
                if let Some(status) = &exited {
                    state.restart_requested = true;
                    state.termination_pending = true;
                    state.ready = false;
                    state.unready_reason = Some(format!(
                        "native inference child exited; verifying process tree cleanup: {status}"
                    ));
                    state.slots_total = 0;
                    state.slots_free = 0;
                    state.backend = None;
                    state.probes.clear();
                }
                (
                    exited,
                    state.termination_pending && state.process.is_some(),
                    state.process.is_none() && state.restart_requested,
                )
            };
            if let Some(status) = exited {
                tracing::warn!(pool = %entry.config.key, %status, "native inference child exited; verifying process tree cleanup before restart");
                let _ = entry.changes_tx.send(());
            }
            if terminate_pending {
                let process = lock_state(&entry.state).process.take();
                if let Some(process) = process {
                    let terminated = terminate_managed_process(
                        entry,
                        process,
                        "native inference child tree terminated",
                    )
                    .await;
                    let restart_requested = lock_state(&entry.state).restart_requested;
                    if terminated
                        && restart_requested
                        && let Err(error) = start_pool_process(entry).await
                    {
                        lock_state(&entry.state).unready_reason = Some(error.message.clone());
                        tracing::warn!(pool = %entry.config.key, error = %error.message, "native child restart failed");
                        let _ = entry.changes_tx.send(());
                    }
                }
            } else if restart_requested && let Err(error) = start_pool_process(entry).await {
                lock_state(&entry.state).unready_reason = Some(error.message.clone());
                tracing::warn!(pool = %entry.config.key, error = %error.message, "native child recovery failed");
                let _ = entry.changes_tx.send(());
            }
        }
    }

    pub async fn refresh_capabilities(&self) {
        for entry in self.pools.values() {
            let _op_guard = entry.op_lock.lock().await;
            let backend = {
                let state = lock_state(&entry.state);
                if state.process.is_none() {
                    continue;
                }
                state.backend.clone()
            };
            let Some(backend) = backend else { continue };
            let observed = match backend.probe().await {
                Ok(capabilities) => capabilities,
                Err(error) => {
                    let mut state = lock_state(&entry.state);
                    if state.ready {
                        state.ready = false;
                        state.slots_total = 0;
                        state.slots_free = 0;
                        state.unready_reason = Some(error.message);
                        let _ = entry.changes_tx.send(());
                    }
                    continue;
                }
            };
            let ready = observed.probes.values().all(|passed| *passed);
            let mut state = lock_state(&entry.state);
            let changed = state.ready != ready
                || state.backend_version != observed.version
                || state.accelerator != observed.accelerator
                || state.accelerator_version != observed.accelerator_version
                || state.probes != observed.probes;
            state.ready = ready;
            state.unready_reason = if ready {
                None
            } else {
                Some(unready_reason(&observed.probes))
            };
            state.backend_version = observed.version;
            state.accelerator = observed.accelerator;
            state.accelerator_version = observed.accelerator_version;
            state.probes = observed.probes;
            state.slots_total = u32::from(ready);
            state.slots_free = u32::from(ready && state.busy.is_empty());
            drop(state);
            if changed {
                let _ = entry.changes_tx.send(());
            }
        }
    }
}

fn resolved_model(model: &ConfiguredModel) -> ResolvedModelFiles {
    ResolvedModelFiles {
        family: model.manifest.family,
        diffusion_model: model.asset_paths[&ModelAssetRole::Diffusion].clone(),
        text_encoder: model.asset_paths[&ModelAssetRole::TextEncoder].clone(),
        vision_encoder: model
            .asset_paths
            .get(&ModelAssetRole::VisionEncoder)
            .cloned(),
        vae: model.asset_paths[&ModelAssetRole::Vae].clone(),
        audio_vae: model.asset_paths.get(&ModelAssetRole::AudioVae).cloned(),
        default_loras: model
            .default_lora_paths
            .iter()
            .map(|lora| ResolvedLora {
                sha256: lora.sha256,
                path: lora.path.clone(),
                scale: lora.scale,
            })
            .collect(),
        sampling: model.manifest.sampling.clone(),
    }
}

pub(crate) fn runtime_options(pool: &PoolConfig) -> RuntimeOptions {
    RuntimeOptions {
        backend: pool.backend.clone(),
        module_backends: pool.runtime.module_backends.clone(),
        params_backend: pool.runtime.params_backend.clone(),
        max_vram: pool.runtime.max_vram.clone(),
        threads: pool.runtime.threads,
        vae_tiling: pool.runtime.vae_tiling,
        model_args: pool.runtime.model_args.clone(),
    }
}

async fn start_pool_process(entry: &Arc<PoolEntry>) -> Result<(), BackendError> {
    {
        let mut state = lock_state(&entry.state);
        state.restart_requested = true;
        state.termination_pending = false;
        state.backend = None;
        state.ready = false;
        state.slots_total = 0;
        state.slots_free = 0;
        state.probes.clear();
        state.unready_reason = Some("native inference child startup pending".to_owned());
    }
    let _ = entry.changes_tx.send(());
    let token = uuid::Uuid::new_v4().simple().to_string();
    let backend: Arc<dyn Backend> = Arc::new(
        backend::build(&entry.config, token.clone()).map_err(|error| BackendError {
            kind: FailureKind::BackendCrashed,
            message: error.to_string(),
            retry_hint: true,
        })?,
    );
    let executable = std::env::current_exe().map_err(|error| BackendError {
        kind: FailureKind::BackendCrashed,
        message: format!("resolving current executable: {error}"),
        retry_hint: false,
    })?;
    start_infer_child(entry, &executable, token).await?;
    probe_pool_process(entry, &backend).await
}

async fn start_infer_child(
    entry: &Arc<PoolEntry>,
    executable: &Path,
    token: String,
) -> Result<(), BackendError> {
    let args = vec![
        OsString::from("infer"),
        OsString::from("--config"),
        entry.config_path.as_os_str().to_os_string(),
        OsString::from("--pool"),
        entry.config.key.clone().into(),
        OsString::from("--listen"),
        entry.config.listen.clone().into(),
    ];
    let mut env = entry.config.env.clone();
    env.insert("GPQ_INFER_TOKEN".to_owned(), token);
    let process = ManagedProcess::spawn(
        executable,
        &args,
        &env,
        &entry.config.state_dir,
        PROCESS_IDENTITY_FILE,
    )
    .await
    .map_err(|error| BackendError {
        kind: FailureKind::BackendCrashed,
        message: error.to_string(),
        retry_hint: true,
    })?;
    let mut process = process::ProcessGuard::new(process, TERMINATE_GRACE);
    let pid = process.pid();
    tracing::info!(pool = %entry.config.key, pid, executable = %executable.display(), "spawned managed native inference child");
    if let Some(stdout) = process.take_stdout() {
        spawn_stream_logger(entry.config.key.clone(), "stdout", stdout);
    }
    if let Some(stderr) = process.take_stderr() {
        spawn_stream_logger(entry.config.key.clone(), "stderr", stderr);
    }
    {
        let mut state = lock_state(&entry.state);
        state.process = Some(process.disarm());
        state.termination_pending = true;
        state.unready_reason = Some("native inference child readiness probe pending".to_owned());
    }
    let _ = entry.changes_tx.send(());
    let identity_file = entry.config.state_dir.join(PROCESS_IDENTITY_FILE);
    let identity = match process::record_identity_file(pid, executable, &identity_file).await {
        Ok(identity) => identity,
        Err(error) => {
            let error = BackendError {
                kind: FailureKind::BackendCrashed,
                message: error.to_string(),
                retry_hint: true,
            };
            lock_state(&entry.state).unready_reason = Some(error.message.clone());
            let _ = entry.changes_tx.send(());
            return Err(error);
        }
    };
    {
        let mut state = lock_state(&entry.state);
        let process = state.process.as_mut().unwrap_or_else(|| {
            unreachable!("startup child remains pool-owned while identity is recorded")
        });
        process.set_identity(identity);
    }
    #[cfg(windows)]
    let startup_error = {
        let state = lock_state(&entry.state);
        state
            .process
            .as_ref()
            .and_then(ManagedProcess::startup_error)
            .map(str::to_owned)
    };
    #[cfg(windows)]
    if let Some(message) = startup_error {
        let error = BackendError {
            kind: FailureKind::BackendCrashed,
            message: format!("cannot supervise native child with a Windows Job Object: {message}"),
            retry_hint: false,
        };
        lock_state(&entry.state).unready_reason = Some(error.message.clone());
        let _ = entry.changes_tx.send(());
        return Err(error);
    }
    Ok(())
}

async fn probe_pool_process(
    entry: &Arc<PoolEntry>,
    backend: &Arc<dyn Backend>,
) -> Result<(), BackendError> {
    let ready_by = Instant::now() + entry.config.startup_timeout;
    let mut last_error = None;
    let capabilities = loop {
        let remaining = ready_by.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, backend.probe()).await {
            Ok(Ok(capabilities)) => {
                if capabilities.probes.values().all(|passed| *passed) {
                    break capabilities;
                }
                if Instant::now() >= ready_by {
                    break capabilities;
                }
            }
            Ok(Err(error)) => {
                if Instant::now() >= ready_by {
                    let error = last_error.unwrap_or(error);
                    lock_state(&entry.state).unready_reason = Some(error.message.clone());
                    let _ = entry.changes_tx.send(());
                    return Err(error);
                }
                last_error = Some(error);
            }
            Err(_) => {
                let error = BackendError {
                    kind: FailureKind::BackendCrashed,
                    message: format!(
                        "native inference readiness exceeded {:?}",
                        entry.config.startup_timeout
                    ),
                    retry_hint: true,
                };
                lock_state(&entry.state).unready_reason = Some(error.message.clone());
                let _ = entry.changes_tx.send(());
                return Err(error);
            }
        }
        tokio::time::sleep(PROBE_RETRY_INTERVAL).await;
    };
    let ready = capabilities.probes.values().all(|passed| *passed);
    let mut state = lock_state(&entry.state);
    state.restart_requested = false;
    state.termination_pending = false;
    state.backend = Some(Arc::clone(backend));
    state.ready = ready;
    state.unready_reason = (!ready).then(|| unready_reason(&capabilities.probes));
    state.slots_total = u32::from(ready);
    state.slots_free = u32::from(ready);
    state.busy.clear();
    state.accelerator = capabilities.accelerator;
    state.accelerator_version = capabilities.accelerator_version;
    state.backend_version = capabilities.version;
    state.probes = capabilities.probes;
    state.last_activity = Instant::now();
    drop(state);
    let _ = entry.changes_tx.send(());
    Ok(())
}

async fn terminate_managed_process(
    entry: &Arc<PoolEntry>,
    process: ManagedProcess,
    reason: &str,
) -> bool {
    let mut guard = TerminatingProcessGuard {
        entry: Arc::clone(entry),
        process: Some(process),
    };
    match guard
        .process
        .as_mut()
        .unwrap_or_else(|| unreachable!("termination guard owns its process until teardown"))
        .terminate_tree(TERMINATE_GRACE)
        .await
    {
        Ok(()) => {
            drop(guard.process.take());
            lock_state(&entry.state).termination_pending = false;
            mark_unready(entry, reason);
            true
        }
        Err(error) => {
            let mut state = lock_state(&entry.state);
            state.termination_pending = true;
            state.backend = None;
            state.ready = false;
            state.unready_reason = Some(format!(
                "native inference child tree cleanup failed: {error:#}"
            ));
            state.slots_total = 0;
            state.slots_free = 0;
            state.probes.clear();
            drop(state);
            tracing::error!(pool = %entry.config.key, error = %error, "failed to terminate native inference process tree");
            guard.restore(false);
            false
        }
    }
}

fn mark_unready(entry: &PoolEntry, reason: &str) {
    let mut state = lock_state(&entry.state);
    state.process = None;
    state.backend = None;
    state.ready = false;
    state.unready_reason = Some(reason.to_owned());
    state.slots_total = 0;
    state.slots_free = 0;
    state.busy.clear();
    state.termination_pending = false;
    state.probes.clear();
    drop(state);
    let _ = entry.changes_tx.send(());
}

fn unready_reason(probes: &BTreeMap<String, bool>) -> String {
    let missing = probes
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>();
    if missing.is_empty() {
        "native inference child is not ready".to_owned()
    } else {
        format!(
            "native inference child failed probes: {}",
            missing.join(", ")
        )
    }
}

fn internal_error(message: String) -> BackendError {
    BackendError {
        kind: FailureKind::Internal,
        message,
        retry_hint: false,
    }
}

fn spawn_stream_logger<R>(pool_key: String, stream: &'static str, reader: R)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(reader).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => tracing::info!(pool = %pool_key, stream, "{line}"),
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(pool = %pool_key, stream, error = %error, "native child output stream failed");
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use gpq_domain::AttemptId;

    use super::{PoolEntry, PoolState, SlotLease};
    use crate::config::{PoolConfig, RuntimeConfig, ShapeLimits};

    fn test_entry() -> std::sync::Arc<PoolEntry> {
        let config = PoolConfig {
            key: "test-pool".to_owned(),
            backend: "cpu".to_owned(),
            listen: "127.0.0.1:4001".to_owned(),
            state_dir: std::path::PathBuf::new(),
            startup_timeout: std::time::Duration::from_secs(1),
            runtime: RuntimeConfig::default(),
            shape: ShapeLimits::default(),
            models: Vec::new(),
            extra_image_loras: Vec::new(),
            expected_hashes: std::collections::BTreeMap::default(),
            env: std::collections::BTreeMap::default(),
        };
        let mut state = PoolState::not_yet_started();
        state.ready = true;
        state.unready_reason = None;
        state.slots_total = 1;
        state.slots_free = 1;
        let (changes_tx, _) = tokio::sync::watch::channel(());
        std::sync::Arc::new(PoolEntry {
            config,
            config_path: std::path::PathBuf::new(),
            models: Vec::new(),
            image_loras: std::collections::BTreeMap::default(),
            op_lock: tokio::sync::Mutex::new(()),
            state: Mutex::new(state),
            changes_tx,
        })
    }

    #[test]
    fn slot_lease_keeps_pool_busy_until_drop_and_releases_once() {
        let entry = test_entry();
        let attempt_id = AttemptId::new();
        assert!(
            entry
                .state
                .lock()
                .is_ok_and(|mut state| state.try_acquire(attempt_id))
        );
        assert!(entry.state.lock().is_ok_and(|state| state.slots_free == 0));

        drop(SlotLease {
            attempt_id,
            entry: std::sync::Arc::clone(&entry),
        });

        assert!(
            entry
                .state
                .lock()
                .is_ok_and(|state| state.slots_free == 1 && state.busy.is_empty())
        );
    }

    #[test]
    fn dropping_slot_lease_does_not_reopen_an_unready_pool() {
        let entry = test_entry();
        let attempt_id = AttemptId::new();
        assert!(
            entry
                .state
                .lock()
                .is_ok_and(|mut state| state.try_acquire(attempt_id))
        );
        let lease = SlotLease {
            attempt_id,
            entry: std::sync::Arc::clone(&entry),
        };
        if let Ok(mut state) = entry.state.lock() {
            state.ready = false;
            state.slots_total = 0;
            state.slots_free = 0;
        } else {
            panic!("lock pool state");
        }

        drop(lease);

        assert!(
            entry
                .state
                .lock()
                .is_ok_and(|state| state.slots_free == 0 && state.busy.is_empty())
        );
    }
}
