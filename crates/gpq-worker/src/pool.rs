//! Device Pool supervision (ADR 0002, ADR 0003, ADR 0005).
//!
//! [`PoolSupervisor`] owns every Device Pool configured for this Worker: it
//! kills verified-stale children and brings each Pool's managed backend
//! process up at startup, tracks Execution Slot occupancy exactly, switches
//! an idle Active Runtime out after five minutes or under incompatible
//! demand, and gives `session.rs`/`executor.rs` the only door into a Pool's
//! backend adapter.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use gpq_domain::{AttemptId, BackendKind, ContentHash, FailureKind};
use tokio::io::AsyncBufReadExt;

use crate::backend::{self, Backend, BackendError};
use crate::config::{PoolConfig, WorkerConfig};
use crate::models;
use crate::process::{self, ManagedProcess};

/// File name of a Pool's durable managed-process identity record, relative
/// to that Pool's `state_dir`.
pub(crate) const PROCESS_IDENTITY_FILE: &str = "process-identity.json";

/// Grace period between SIGTERM and SIGKILL (or the Windows equivalent) when
/// tearing down a managed process.
const TERMINATE_GRACE: Duration = Duration::from_secs(10);

/// How often to re-probe a freshly spawned backend while waiting for it to
/// become ready.
const PROBE_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// How long an Active Runtime may sit idle before it is unloaded (ADR 0005).
const IDLE_UNLOAD_AFTER: Duration = Duration::from_mins(5);

fn lock_state(mutex: &std::sync::Mutex<PoolState>) -> std::sync::MutexGuard<'_, PoolState> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A snapshot of one Pool's occupancy and capabilities, ready to be converted
/// to a proto `PoolAdvertisement` by the session layer.
#[derive(Debug, Clone)]
pub struct PoolAdvertisementData {
    /// This Pool's stable key, echoed back as `pool_id` on Attempt leases.
    pub pool_key: String,
    /// The managed runtime kind this Pool hosts.
    pub backend: BackendKind,
    /// Backend-reported version string.
    pub backend_version: String,
    /// Whether this Pool can currently accept work.
    pub ready: bool,
    /// Why this Pool is unready, when it is.
    pub unready_reason: Option<String>,
    /// Total configured Execution Slots.
    pub slots_total: u32,
    /// Attempts currently occupying a Slot.
    pub slots_busy: Vec<AttemptId>,
    /// The Model Version currently loaded, if any.
    pub resident_model: Option<ContentHash>,
    /// Accelerator memory, when the backend reports it.
    pub accelerator_memory_bytes: Option<u64>,
    /// Every Model Version registered on this Pool (ADR 0012).
    pub models: Vec<ContentHash>,
    /// Installed `ComfyUI` custom-node package name to exact version.
    pub custom_nodes: BTreeMap<String, String>,
    /// Required-endpoint probe name to whether it passed.
    pub probes: BTreeMap<String, bool>,
}

/// A held Execution Slot on one Pool, released back automatically on `Drop`.
pub struct SlotLease {
    /// The Attempt occupying the Slot.
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

/// Mutable occupancy and capability state for one Pool, guarded by a plain
/// [`std::sync::Mutex`]: every field here is read or written without holding
/// the lock across an `.await`, so a blocking mutex is sufficient and never
/// risks stalling the async runtime.
struct PoolState {
    process: Option<ManagedProcess>,
    backend: Option<Arc<dyn Backend>>,
    ready: bool,
    unready_reason: Option<String>,
    slots_total: u32,
    slots_free: u32,
    busy: Vec<AttemptId>,
    resident_model: Option<ContentHash>,
    accelerator_memory_bytes: Option<u64>,
    backend_version: String,
    custom_nodes: BTreeMap<String, String>,
    probes: BTreeMap<String, bool>,
    last_activity: Instant,
    /// A Model Version that was requested while this differed from
    /// `resident_model`: the "incompatible demand" ADR 0005 says should make
    /// an idle Active Runtime yield before its five-minute grace period.
    pending_model_demand: Option<ContentHash>,
}

impl PoolState {
    fn not_yet_started() -> Self {
        Self {
            process: None,
            backend: None,
            ready: false,
            unready_reason: Some("not yet started".to_string()),
            slots_total: 0,
            slots_free: 0,
            busy: Vec::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            backend_version: String::new(),
            custom_nodes: BTreeMap::new(),
            probes: BTreeMap::new(),
            last_activity: Instant::now(),
            pending_model_demand: None,
        }
    }

    /// Attempts to occupy one Slot for `attempt_id`. Never exceeds
    /// `slots_total` and never leases from an unready Pool.
    fn try_acquire(&mut self, attempt_id: AttemptId) -> bool {
        if !self.ready || self.slots_free == 0 {
            return false;
        }
        self.slots_free -= 1;
        self.busy.push(attempt_id);
        self.last_activity = Instant::now();
        true
    }

    /// Releases the Slot held by `attempt_id`, if any. Idempotent and never
    /// grows `slots_free` past `slots_total`.
    fn release(&mut self, attempt_id: AttemptId) {
        let before = self.busy.len();
        self.busy.retain(|id| *id != attempt_id);
        if self.busy.len() < before {
            self.slots_free = (self.slots_free + 1).min(self.slots_total);
        }
        self.last_activity = Instant::now();
    }
}

/// One configured Device Pool: its static configuration, its registered
/// Model Versions, and its mutable runtime state.
struct PoolEntry {
    config: PoolConfig,
    /// Model Versions this Pool can serve, scanned once at startup
    /// (ADR 0005: no dynamic reload, so this never changes for the life of
    /// the Worker process).
    models: Vec<(PathBuf, ContentHash)>,
    /// Serializes `ensure_runtime_and_acquire_slot`/`release_idle`/
    /// `shutdown` for this Pool so two callers never spawn or tear down its
    /// process concurrently, and so a Slot reservation can never be lost in
    /// the window between starting a runtime and claiming a Slot on it.
    op_lock: tokio::sync::Mutex<()>,
    state: std::sync::Mutex<PoolState>,
    changes_tx: tokio::sync::watch::Sender<()>,
}

/// Owns every Device Pool this Worker supervises (ADR 0005).
pub struct PoolSupervisor {
    pools: BTreeMap<String, Arc<PoolEntry>>,
    changes_tx: tokio::sync::watch::Sender<()>,
}

impl PoolSupervisor {
    /// Brings every configured Pool up: kills a verified-stale child from a
    /// previous run, scans registered models, spawns the managed process,
    /// and runs its readiness probes. A Pool that fails any of this is left
    /// `unready` with a reason rather than failing the whole Worker startup.
    pub async fn start(config: &WorkerConfig) -> anyhow::Result<Self> {
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

            let (models, scan_error) = match models::scan_models(pool_config) {
                Ok(models) => (models, None),
                Err(err) => (Vec::new(), Some(err.to_string())),
            };

            let mut initial_state = PoolState::not_yet_started();
            if let Some(reason) = &scan_error {
                initial_state.unready_reason = Some(reason.clone());
            }

            let entry = Arc::new(PoolEntry {
                config: pool_config.clone(),
                models,
                op_lock: tokio::sync::Mutex::new(()),
                state: std::sync::Mutex::new(initial_state),
                changes_tx: changes_tx.clone(),
            });

            if scan_error.is_none()
                && let Err(err) = start_pool_process(&entry).await
            {
                lock_state(&entry.state).unready_reason = Some(err.message);
            }

            pools.insert(pool_config.key.clone(), entry);
        }

        Ok(Self { pools, changes_tx })
    }

    /// One advertisement per configured Pool, in configured order.
    #[must_use]
    pub fn capabilities(&self) -> Vec<PoolAdvertisementData> {
        self.pools
            .values()
            .map(|entry| {
                let state = lock_state(&entry.state);
                PoolAdvertisementData {
                    pool_key: entry.config.key.clone(),
                    backend: entry.config.backend,
                    backend_version: state.backend_version.clone(),
                    ready: state.ready,
                    unready_reason: state.unready_reason.clone(),
                    slots_total: state.slots_total,
                    slots_busy: state.busy.clone(),
                    resident_model: state.resident_model,
                    accelerator_memory_bytes: state.accelerator_memory_bytes,
                    models: entry.models.iter().map(|(_, hash)| *hash).collect(),
                    custom_nodes: state.custom_nodes.clone(),
                    probes: state.probes.clone(),
                }
            })
            .collect()
    }

    /// Fires whenever a `capabilities()` call would return something new.
    #[must_use]
    pub fn watch_changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes_tx.subscribe()
    }

    /// The live backend adapter for `pool_key`, if its Active Runtime is up.
    #[must_use]
    pub fn backend(&self, pool_key: &str) -> Option<Arc<dyn Backend>> {
        let entry = self.pools.get(pool_key)?;
        lock_state(&entry.state).backend.clone()
    }

    /// The local path of the registered Model Version `content_hash` on
    /// `pool_key`, if this Pool has it (ADR 0012).
    #[must_use]
    pub fn resolve_model_path(&self, pool_key: &str, content_hash: ContentHash) -> Option<PathBuf> {
        let entry = self.pools.get(pool_key)?;
        entry
            .models
            .iter()
            .find(|(_, hash)| *hash == content_hash)
            .map(|(path, _)| path.clone())
    }

    /// Ensures `pool_key` can serve `backend`/`resident_model`, starting its
    /// managed process if it is not already running, and reserves one free
    /// Execution Slot on it for `attempt_id` — both under this Pool's
    /// `op_lock`, so the runtime this call just started or resumed can
    /// never be observed as idle by a concurrent [`Self::release_idle`]
    /// before the caller has actually claimed a Slot on it. Releasing the
    /// lock between "ensure a runtime" and "reserve a Slot" let a second,
    /// incompatible caller's `pending_model_demand` race the maintenance
    /// tick into tearing down an Active Runtime the first caller had just
    /// paid to load and was about to use (ADR 0005: a reserved-but-not-yet-
    /// busy runtime must not count as idle).
    ///
    /// Fails before any Attempt-shaped work happens (ADR 0003): a backend
    /// kind mismatch or an unregistered Model Version fails immediately and
    /// permanently, while a currently-loaded-but-different Model Version on
    /// a running llama.cpp process fails as `ModelUnavailable` since that
    /// runtime cannot swap models without a restart the caller did not ask
    /// for. Returns `Internal` if the Pool has no free Slot right after
    /// becoming ready.
    pub async fn ensure_runtime_and_acquire_slot(
        &self,
        pool_key: &str,
        backend: BackendKind,
        resident_model: Option<ContentHash>,
        attempt_id: AttemptId,
    ) -> Result<SlotLease, BackendError> {
        let Some(entry) = self.pools.get(pool_key).cloned() else {
            return Err(BackendError {
                kind: FailureKind::Internal,
                message: format!("unknown pool `{pool_key}`"),
                retry_hint: false,
            });
        };
        let hosted_kind = lock_state(&entry.state)
            .backend
            .as_ref()
            .map_or(entry.config.backend, |active| active.kind());
        let registered_models: Vec<ContentHash> =
            entry.models.iter().map(|(_, hash)| *hash).collect();
        runtime_precondition(
            pool_key,
            hosted_kind,
            backend,
            &registered_models,
            resident_model,
        )?;

        let _op_guard = entry.op_lock.lock().await;

        let already_running = {
            let mut state = lock_state(&entry.state);
            if state.process.is_some() && state.ready {
                if runtime_satisfies(backend, state.resident_model, resident_model) {
                    state.pending_model_demand = None;
                    state.last_activity = Instant::now();
                    Some(true)
                } else {
                    state.pending_model_demand = resident_model;
                    Some(false)
                }
            } else {
                None
            }
        };
        match already_running {
            Some(true) => {}
            Some(false) => {
                return Err(BackendError {
                    kind: FailureKind::ModelUnavailable,
                    message: format!(
                        "pool `{pool_key}` currently has a different Model Version loaded"
                    ),
                    retry_hint: false,
                });
            }
            None => {
                start_pool_process(&entry).await?;

                let state = lock_state(&entry.state);
                if !state.ready {
                    return Err(BackendError {
                        kind: FailureKind::BackendCrashed,
                        message: state
                            .unready_reason
                            .clone()
                            .unwrap_or_else(|| "pool failed to become ready".to_string()),
                        retry_hint: true,
                    });
                }
                if !runtime_satisfies(backend, state.resident_model, resident_model) {
                    return Err(BackendError {
                        kind: FailureKind::ModelUnavailable,
                        message: format!(
                            "pool `{pool_key}` started but is not serving the requested Model Version"
                        ),
                        retry_hint: false,
                    });
                }
            }
        }

        // Still under `_op_guard`: `release_idle` cannot see this runtime
        // as idle-and-unclaimed between the check above and this reserve.
        let acquired = lock_state(&entry.state).try_acquire(attempt_id);
        if !acquired {
            return Err(BackendError {
                kind: FailureKind::Internal,
                message: format!("pool `{pool_key}` has no free slot"),
                retry_hint: true,
            });
        }
        let _ = entry.changes_tx.send(());
        // Clone the handle into the lease rather than moving `entry`: the
        // `op_lock` guard borrows it and must stay held until this returns.
        let lease = SlotLease {
            attempt_id,
            entry: Arc::clone(&entry),
        };
        Ok(lease)
    }

    /// Unloads every Pool's Active Runtime that has sat idle for five
    /// minutes or that blocks a different Model Version's demand,
    /// preferring the backend's own release API before terminating the
    /// managed process (ADR 0005).
    pub async fn release_idle(&self, now: Instant) {
        for entry in self.pools.values() {
            let _op_guard = entry.op_lock.lock().await;

            let should_unload = {
                let state = lock_state(&entry.state);
                state.process.is_some()
                    && state.busy.is_empty()
                    && should_unload_idle(
                        now.saturating_duration_since(state.last_activity),
                        state.pending_model_demand.is_some(),
                    )
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
                let mut state = lock_state(&entry.state);
                state.resident_model = None;
                state.pending_model_demand = None;
                state.last_activity = now;
                drop(state);
                let _ = entry.changes_tx.send(());
                continue;
            }

            let process = lock_state(&entry.state).process.take();
            if let Some(mut process) = process {
                let _ = process.terminate_tree(TERMINATE_GRACE).await;
            }
            let mut state = lock_state(&entry.state);
            state.backend = None;
            state.ready = false;
            state.unready_reason = Some("idle: Active Runtime unloaded".to_string());
            state.slots_total = 0;
            state.slots_free = 0;
            state.resident_model = None;
            state.pending_model_demand = None;
            drop(state);
            let _ = entry.changes_tx.send(());
        }
    }

    /// Terminates every managed process, for a graceful Worker shutdown.
    pub async fn shutdown(&self) {
        for entry in self.pools.values() {
            let _op_guard = entry.op_lock.lock().await;
            let process = lock_state(&entry.state).process.take();
            if let Some(mut process) = process {
                let _ = process.terminate_tree(TERMINATE_GRACE).await;
            }
            let mut state = lock_state(&entry.state);
            state.backend = None;
            state.ready = false;
            state.unready_reason = Some("worker shutting down".to_string());
            state.slots_total = 0;
            state.slots_free = 0;
            state.busy.clear();
        }
    }

    /// Detects a managed backend process that exited without being asked to
    /// (crash, OOM-killer, an operator killing it outside the Worker),
    /// marks that Pool unready with a reason, and restarts it (ADR 0005:
    /// children are identified durably, not merely assumed alive).
    pub async fn check_process_liveness(&self) {
        for entry in self.pools.values() {
            let _op_guard = entry.op_lock.lock().await;

            let exited = {
                let mut state = lock_state(&entry.state);
                state
                    .process
                    .as_mut()
                    .and_then(|process| process.try_wait().ok().flatten())
            };
            let Some(status) = exited else { continue };

            tracing::warn!(
                pool = %entry.config.key,
                %status,
                "managed backend process exited unexpectedly; marking pool unready and restarting"
            );
            {
                let mut state = lock_state(&entry.state);
                mark_unready_for_exit(&mut state, &status.to_string());
            }
            let _ = entry.changes_tx.send(());

            if let Err(err) = start_pool_process(entry).await {
                lock_state(&entry.state).unready_reason = Some(err.message.clone());
                let _ = entry.changes_tx.send(());
                tracing::warn!(pool = %entry.config.key, error = %err.message, "restart after unexpected exit failed; pool remains unready");
            }
        }
    }

    /// Re-probes every running Pool and refreshes its advertised capabilities.
    ///
    /// A Pool that came up before its backend finished loading, or whose
    /// backend gained a model or custom node since startup, recovers here
    /// instead of staying unready until the next restart (ADR 0005: Workers
    /// advertise observed versions and probe results).
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
                Err(err) => {
                    let mut state = lock_state(&entry.state);
                    if state.ready {
                        state.ready = false;
                        state.unready_reason = Some(err.message.clone());
                        let _ = entry.changes_tx.send(());
                    }
                    continue;
                }
            };

            let all_probes_pass = observed.probes.values().all(|&passed| passed);
            let mut state = lock_state(&entry.state);
            let changed = state.ready != all_probes_pass
                || state.backend_version != observed.version
                || state.resident_model != observed.resident_model
                || state.probes != observed.probes;
            state.ready = all_probes_pass;
            state.unready_reason = if all_probes_pass {
                None
            } else {
                Some(unready_reason(&observed.probes))
            };
            state.backend_version = observed.version;
            state.resident_model = observed.resident_model;
            state.accelerator_memory_bytes = observed.accelerator_memory_bytes;
            state.custom_nodes = observed.custom_nodes;
            state.probes = observed.probes;
            if observed.slots > 0 && state.busy.is_empty() {
                state.slots_total = observed.slots;
                state.slots_free = observed.slots;
            }
            drop(state);
            if changed {
                let _ = entry.changes_tx.send(());
            }
        }
    }
}

/// Names the required operations a backend failed to answer, for the Pool's
/// `unready_reason` (ADR 0005: a missing core operation makes a Pool unready).
fn unready_reason(probes: &BTreeMap<String, bool>) -> String {
    let missing: Vec<&str> = probes
        .iter()
        .filter(|(_, passed)| !**passed)
        .map(|(name, _)| name.as_str())
        .collect();
    format!("missing required probes: {}", missing.join(", "))
}

/// Spawns `entry`'s managed process and runs its readiness probes, updating
/// `entry.state` on completion. Blocks up to `entry.config.startup_timeout`
/// for the backend to answer a probe.
async fn start_pool_process(entry: &Arc<PoolEntry>) -> Result<(), BackendError> {
    let backend: Arc<dyn Backend> = Arc::from(backend::build(&entry.config));
    let process = ManagedProcess::spawn(
        &entry.config.executable,
        &entry.config.args,
        &entry.config.env,
        &entry.config.state_dir,
        PROCESS_IDENTITY_FILE,
    )
    .await
    .map_err(|source| BackendError {
        kind: FailureKind::BackendCrashed,
        message: source.to_string(),
        retry_hint: true,
    })?;
    // Guarded from here on: any early return below (a probe timeout, most
    // notably) must not abandon this child to the OS still holding the GPU
    // (ADR 0005). `disarm()` below is the only way out other than the
    // guard's own drop-time `terminate_tree`.
    let mut process = process::ProcessGuard::new(process, TERMINATE_GRACE);
    tracing::info!(
        pool = %entry.config.key,
        pid = process.pid(),
        executable = %process.identity().executable.display(),
        "spawned managed backend process"
    );
    if let Some(stdout) = process.take_stdout() {
        spawn_stream_logger(entry.config.key.clone(), "stdout", stdout);
    }
    if let Some(stderr) = process.take_stderr() {
        spawn_stream_logger(entry.config.key.clone(), "stderr", stderr);
    }

    // A backend answers HTTP well before it can generate: llama.cpp loads the
    // model first, ComfyUI imports custom nodes first. Both report their
    // required-operation probes as failed rather than erroring (ADR 0005), so
    // wait for the probes themselves to pass, not merely for an answer, and
    // keep the last observation for the unready reason.
    let ready_by = Instant::now() + entry.config.startup_timeout;
    let mut last_error: Option<BackendError> = None;
    let capabilities = loop {
        match backend.probe().await {
            Ok(capabilities) => {
                if capabilities.probes.values().all(|&passed| passed) {
                    break capabilities;
                }
                if Instant::now() >= ready_by {
                    break capabilities;
                }
            }
            Err(err) => {
                if Instant::now() >= ready_by {
                    return Err(last_error.unwrap_or(err));
                }
                last_error = Some(err);
            }
        }
        tokio::time::sleep(PROBE_RETRY_INTERVAL).await;
    };

    let all_probes_pass = capabilities.probes.values().all(|&passed| passed);
    let slots_total = if capabilities.slots > 0 {
        capabilities.slots
    } else {
        entry
            .config
            .slots
            .unwrap_or_else(|| entry.config.backend.default_slots())
    };

    let mut state = lock_state(&entry.state);
    state.process = Some(process.disarm());
    state.backend = Some(backend);
    state.ready = all_probes_pass;
    state.unready_reason = if all_probes_pass {
        None
    } else {
        Some(unready_reason(&capabilities.probes))
    };
    state.slots_total = slots_total;
    state.slots_free = slots_total;
    state.busy.clear();
    state.resident_model = capabilities.resident_model;
    state.accelerator_memory_bytes = capabilities.accelerator_memory_bytes;
    state.backend_version = capabilities.version;
    state.custom_nodes = capabilities.custom_nodes;
    state.probes = capabilities.probes;
    state.last_activity = Instant::now();
    state.pending_model_demand = None;
    drop(state);
    let _ = entry.changes_tx.send(());
    Ok(())
}

/// Streams one managed process's `stdout`/`stderr` into `tracing`, tagged by
/// Pool key and stream name. Backend subprocesses are launched with nothing
/// secret in their environment or arguments (ADR 0009), so their own output
/// is safe to log verbatim.
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
                Err(err) => {
                    tracing::warn!(pool = %pool_key, stream, error = %err, "managed process output stream failed");
                    break;
                }
            }
        }
    });
}

/// Whether `ensure_runtime_and_acquire_slot` may proceed to start or reuse
/// `pool_key`'s process at all (ADR 0005): a backend-kind mismatch, or a
/// requested resident Model Version this Pool never registered, must fail
/// before any process is started or a Slot reserved. Pure so the
/// pre-flight gate is unit-testable without a managed process.
fn runtime_precondition(
    pool_key: &str,
    hosted_kind: BackendKind,
    requested_kind: BackendKind,
    registered_models: &[ContentHash],
    resident_model: Option<ContentHash>,
) -> Result<(), BackendError> {
    if requested_kind != hosted_kind {
        return Err(BackendError {
            kind: FailureKind::UnsupportedCapability,
            message: format!("pool `{pool_key}` hosts {hosted_kind} only, not {requested_kind}"),
            retry_hint: false,
        });
    }
    if let Some(hash) = resident_model
        && !registered_models.contains(&hash)
    {
        return Err(BackendError {
            kind: FailureKind::ModelUnavailable,
            message: format!("model {hash} is not registered on pool `{pool_key}`"),
            retry_hint: false,
        });
    }
    Ok(())
}

/// Whether an already-running Active Runtime can serve `requested` without a
/// restart.
///
/// llama.cpp loads exactly one model per process and cannot swap it without
/// restarting (ADR 0005), so a running llama.cpp Pool only satisfies a
/// request for the Model Version it already has resident. `ComfyUI` selects
/// its checkpoint from within each submitted Workflow graph rather than
/// holding one persistent resident model, so any running `ComfyUI` Pool
/// satisfies any request.
fn runtime_satisfies(
    backend: BackendKind,
    resident: Option<ContentHash>,
    requested: Option<ContentHash>,
) -> bool {
    match (backend, requested) {
        (_, None) | (BackendKind::ComfyUi, Some(_)) => true,
        (BackendKind::LlamaCpp, Some(hash)) => resident == Some(hash),
    }
}

/// Whether an idle Active Runtime should be unloaded now (ADR 0005: models
/// stay warm for five idle minutes, then yield under incompatible demand).
fn should_unload_idle(idle_for: Duration, incompatible_demand: bool) -> bool {
    incompatible_demand || idle_for >= IDLE_UNLOAD_AFTER
}

/// Clears `state` to unready after its managed process exited unexpectedly,
/// so it stops advertising Execution Slots or a resident Model Version it
/// can no longer serve (ADR 0005).
fn mark_unready_for_exit(state: &mut PoolState, detail: &str) {
    state.process = None;
    state.backend = None;
    state.ready = false;
    state.unready_reason = Some(format!(
        "managed backend process exited unexpectedly: {detail}"
    ));
    state.slots_total = 0;
    state.slots_free = 0;
    state.busy.clear();
    state.resident_model = None;
    state.pending_model_demand = None;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use gpq_domain::AttemptId;

    use super::{
        IDLE_UNLOAD_AFTER, PoolState, mark_unready_for_exit, runtime_precondition,
        runtime_satisfies, should_unload_idle, unready_reason,
    };

    #[test]
    fn acquire_never_exceeds_capacity() {
        let mut state = PoolState::not_yet_started();
        state.ready = true;
        state.slots_total = 2;
        state.slots_free = 2;

        let a = AttemptId::new();
        let b = AttemptId::new();
        let c = AttemptId::new();

        assert!(state.try_acquire(a));
        assert!(state.try_acquire(b));
        assert!(
            !state.try_acquire(c),
            "a third acquire on a 2-slot pool must fail"
        );
        assert_eq!(state.slots_free, 0);
        assert_eq!(state.busy, vec![a, b]);
    }

    #[test]
    fn acquire_fails_on_an_unready_pool() {
        let mut state = PoolState::not_yet_started();
        state.ready = false;
        state.slots_total = 4;
        state.slots_free = 4;

        assert!(!state.try_acquire(AttemptId::new()));
        assert_eq!(state.slots_free, 4);
    }

    #[test]
    fn release_restores_exactly_one_slot_and_tracks_busy_attempts() {
        let mut state = PoolState::not_yet_started();
        state.ready = true;
        state.slots_total = 2;
        state.slots_free = 2;
        let a = AttemptId::new();
        let b = AttemptId::new();
        assert!(state.try_acquire(a));
        assert!(state.try_acquire(b));

        state.release(a);

        assert_eq!(state.slots_free, 1);
        assert_eq!(state.busy, vec![b]);
        assert!(state.try_acquire(AttemptId::new()));
        assert_eq!(state.slots_free, 0);
    }

    #[test]
    fn releasing_an_attempt_twice_never_grows_free_slots_past_total() {
        let mut state = PoolState::not_yet_started();
        state.ready = true;
        state.slots_total = 1;
        state.slots_free = 1;
        let a = AttemptId::new();
        assert!(state.try_acquire(a));

        state.release(a);
        state.release(a);

        assert_eq!(
            state.slots_free, 1,
            "double release must not overshoot capacity"
        );
    }

    #[test]
    fn idle_unload_waits_for_the_five_minute_grace_period() {
        let Some(just_under) = IDLE_UNLOAD_AFTER.checked_sub(std::time::Duration::from_secs(1))
        else {
            panic!("IDLE_UNLOAD_AFTER must be at least one second")
        };
        assert!(!should_unload_idle(just_under, false));
        assert!(should_unload_idle(IDLE_UNLOAD_AFTER, false));
        assert!(should_unload_idle(
            IDLE_UNLOAD_AFTER + std::time::Duration::from_mins(1),
            false
        ));
    }

    #[test]
    fn incompatible_demand_preempts_the_grace_period() {
        assert!(should_unload_idle(std::time::Duration::from_secs(1), true));
    }

    #[test]
    fn exit_detection_marks_pool_unready_and_clears_capacity() {
        use gpq_domain::ContentHash;

        let mut state = PoolState::not_yet_started();
        state.ready = true;
        state.slots_total = 4;
        state.slots_free = 2;
        state.busy = vec![AttemptId::new()];
        state.resident_model = Some(ContentHash::digest(b"model"));

        mark_unready_for_exit(&mut state, "exit status: 1");

        assert!(!state.ready);
        assert_eq!(
            state.unready_reason.as_deref(),
            Some("managed backend process exited unexpectedly: exit status: 1")
        );
        assert_eq!(state.slots_total, 0);
        assert_eq!(state.slots_free, 0);
        assert!(state.busy.is_empty());
        assert!(state.resident_model.is_none());
    }

    #[test]
    fn llama_cpp_only_satisfies_its_own_resident_model() {
        use gpq_domain::{BackendKind, ContentHash};

        let resident = ContentHash::digest(b"model-a");
        let other = ContentHash::digest(b"model-b");

        assert!(runtime_satisfies(
            BackendKind::LlamaCpp,
            Some(resident),
            None
        ));
        assert!(runtime_satisfies(
            BackendKind::LlamaCpp,
            Some(resident),
            Some(resident)
        ));
        assert!(!runtime_satisfies(
            BackendKind::LlamaCpp,
            Some(resident),
            Some(other)
        ));
        assert!(!runtime_satisfies(BackendKind::LlamaCpp, None, Some(other)));
    }

    #[test]
    fn comfyui_satisfies_any_requested_model() {
        use gpq_domain::{BackendKind, ContentHash};

        let requested = ContentHash::digest(b"checkpoint");

        assert!(runtime_satisfies(
            BackendKind::ComfyUi,
            None,
            Some(requested)
        ));
        assert!(runtime_satisfies(
            BackendKind::ComfyUi,
            Some(ContentHash::digest(b"other")),
            Some(requested)
        ));
    }

    #[test]
    fn runtime_precondition_rejects_a_backend_kind_switch() {
        use gpq_domain::BackendKind;

        let result = runtime_precondition(
            "gpu0",
            BackendKind::LlamaCpp,
            BackendKind::ComfyUi,
            &[],
            None,
        );
        assert!(matches!(
            result,
            Err(err) if err.kind == gpq_domain::FailureKind::UnsupportedCapability
        ));
    }

    #[test]
    fn runtime_precondition_rejects_an_unregistered_resident_model() {
        use gpq_domain::{BackendKind, ContentHash};

        let registered = ContentHash::digest(b"registered");
        let requested = ContentHash::digest(b"not-registered");
        let result = runtime_precondition(
            "gpu0",
            BackendKind::LlamaCpp,
            BackendKind::LlamaCpp,
            &[registered],
            Some(requested),
        );
        assert!(matches!(
            result,
            Err(err) if err.kind == gpq_domain::FailureKind::ModelUnavailable
        ));
    }

    #[test]
    fn runtime_precondition_accepts_a_same_kind_reuse_with_no_model_demand() {
        use gpq_domain::BackendKind;

        assert!(
            runtime_precondition(
                "gpu0",
                BackendKind::ComfyUi,
                BackendKind::ComfyUi,
                &[],
                None
            )
            .is_ok()
        );
    }

    #[test]
    fn runtime_precondition_accepts_a_registered_resident_model() {
        use gpq_domain::{BackendKind, ContentHash};

        let hash = ContentHash::digest(b"registered");
        assert!(
            runtime_precondition(
                "gpu0",
                BackendKind::LlamaCpp,
                BackendKind::LlamaCpp,
                &[hash],
                Some(hash)
            )
            .is_ok()
        );
    }

    #[test]
    fn unready_reason_names_exactly_the_failed_probes_in_key_order() {
        let mut probes = BTreeMap::new();
        probes.insert("cancel".to_string(), true);
        probes.insert("generate".to_string(), false);
        probes.insert("progress".to_string(), false);
        probes.insert("stream".to_string(), true);

        assert_eq!(
            unready_reason(&probes),
            "missing required probes: generate, progress",
            "a passing probe must never appear in the unready reason, and a \
             failing one must never be silently dropped"
        );
    }
}
