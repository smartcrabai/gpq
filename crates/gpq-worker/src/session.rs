//! Outbound-only gRPC control session to Remote (ADR 0004).
//!
//! [`run`] holds the single bidirectional `WorkerSessionService::Session`
//! stream for the Worker's whole lifetime: it performs the version
//! handshake (failing hard on a protocol major mismatch, ADR 0004),
//! advertises Device Pool capabilities, heartbeats every live Attempt
//! (ADR 0003), dispatches `LeaseAssignment`/`CancelRequest`/`DeliverRequest`/
//! `DiscardOutput`/`Shutdown`, and reconnects with bounded exponential
//! backoff on transport failure. A lease-expiry sweep runs on its own
//! schedule, independent of the connection, so a lapsed lease is still
//! cooperatively cancelled through reconnect backoff (ADR 0003); the
//! Worker's own shutdown cancels every live Attempt and waits briefly for
//! its terminal report to reach Remote before closing the stream (ADR
//! 0020). Bulk Artifact transfer always goes through the separate
//! `WorkerTransferService` so it can never block a heartbeat or a
//! cancellation (ADR 0004).

use std::collections::HashMap;
use std::sync::Arc;

use buffa::MessageField;
use buffa_types::google::protobuf::Timestamp;
use chrono::{DateTime, Utc};
use gpq_domain::{
    AttemptId, ContentHash, FailureKind, HEARTBEAT_INTERVAL, LEASE_TTL, Modality,
    OUTPUT_ARTIFACT_TTL,
};
use gpq_proto::gpq::worker::v1::{
    ArtifactChunk, CancelRequest, CapabilityReport, DeliverArtifactRequest, DeliverArtifactStart,
    DeliverRequest, DiscardOutput, Handshake, Heartbeat, LeaseAssignment, LeaseRejected,
    PoolAdvertisement, RemoteMessage, SlotAdvertisement, WorkerMessage, WorkerSessionServiceClient,
    WorkerTransferServiceClient, remote_message, worker_message,
};
use rand::{Rng, RngExt};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::artifacts::LocalArtifactStore;
use crate::config::WorkerConfig;
use crate::executor::{self, ExecutionContext, Transport};
use crate::pool::{PoolAdvertisementData, PoolSupervisor};

/// Ceiling on reconnect backoff, regardless of how many attempts have
/// failed in a row.
const MAX_RECONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

/// How often the maintenance tick unloads idle Active Runtimes, restarts
/// crashed managed processes, and expires stale local output Artifacts
/// (ADR 0005, ADR 0008). Independent of the 10-second heartbeat cadence so a
/// slow filesystem scan never risks a missed heartbeat and a dropped lease.
const MAINTENANCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Bound on how long the Worker waits, during its own shutdown, for
/// in-flight Attempts to observe cancellation and flush their terminal
/// report (`AttemptResult`, `LeaseRejected`, or `CancelAcknowledged`) to
/// Remote before the control stream closes and `main` force-terminates
/// backend processes (ADR 0003, ADR 0020). Comfortably shorter than the
/// 45-second lease TTL: Remote reclaims an abandoned lease at that point
/// anyway, so waiting longer only delays process exit without changing the
/// outcome.
const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

type TransferClient = WorkerTransferServiceClient<Transport>;

/// Bookkeeping for one Attempt this Worker currently holds a lease for.
struct LiveAttempt {
    cancel: CancellationToken,
    lease_expires_at: DateTime<Utc>,
}

/// Attempts this Worker currently holds a lease for, shared between the
/// control loop (heartbeats, cancellation, lease-expiry sweeps) and the
/// spawned `executor::execute` tasks.
type LiveAttempts = Arc<std::sync::Mutex<HashMap<AttemptId, LiveAttempt>>>;

/// Aborts the wrapped task when dropped, so the independent lease-expiry
/// sweep task spawned in [`run`] never outlives it, on any exit path.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Bundles everything a `RemoteMessage` handler or a spawned lease/delivery
/// task needs, so those functions take one argument instead of seven.
#[derive(Clone)]
struct SessionCtx {
    config: Arc<WorkerConfig>,
    pools: Arc<PoolSupervisor>,
    artifacts: Arc<LocalArtifactStore>,
    live: LiveAttempts,
    http: reqwest::Client,
    transfer: TransferClient,
    outbound: mpsc::Sender<WorkerMessage>,
}

/// Why a connection attempt ended.
enum SessionError {
    /// The remote's protocol major version is incompatible; reconnecting
    /// would only fail again (ADR 0004).
    ProtocolMismatch(String),
    /// A transport or protocol-level failure; the caller should reconnect.
    Connection(anyhow::Error),
}

impl From<connectrpc::ConnectError> for SessionError {
    fn from(err: connectrpc::ConnectError) -> Self {
        Self::Connection(err.into())
    }
}

/// Maintains the outbound gRPC control session to Remote for the Worker's
/// whole lifetime, reconnecting with backoff until `shutdown` is cancelled.
///
/// # Errors
///
/// Returns an error only for an irrecoverable failure: a protocol major
/// version mismatch, or failure to open the local Artifact store.
pub async fn run(
    config: Arc<WorkerConfig>,
    credential: String,
    pools: Arc<PoolSupervisor>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let artifacts = Arc::new(LocalArtifactStore::open(config.state_dir.join("artifacts")).await?);
    let report = artifacts.reconcile_on_startup().await?;
    tracing::info!(
        available = report.available.len(),
        incomplete_removed = report.incomplete_removed.len(),
        lost = report.lost.len(),
        "reconciled local artifact store on startup"
    );

    let live: LiveAttempts = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let http = reqwest::Client::builder()
        .build()
        .map_err(|err| anyhow::anyhow!("failed to build http client: {err}"))?;

    // Runs on its own schedule, independent of the connection, so a lapsed
    // lease is still cooperatively cancelled while reconnect backoff (up to
    // `MAX_RECONNECT_BACKOFF`) leaves `connect_and_serve`'s select loop not
    // running at all (ADR 0003). `AbortOnDrop` stops this task on every
    // exit from `run`.
    let _lease_sweep = AbortOnDrop(tokio::spawn({
        let live = live.clone();
        let shutdown = shutdown.clone();
        async move {
            let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    _ = ticker.tick() => cancel_expired_leases(&live, Utc::now()),
                }
            }
        }
    }));

    let mut attempt = 0_u32;
    let mut rng = rand::rng();
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        match connect_and_serve(
            &config,
            &credential,
            &pools,
            &artifacts,
            &live,
            &http,
            &shutdown,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(SessionError::ProtocolMismatch(message)) => {
                anyhow::bail!("worker protocol incompatible with remote: {message}");
            }
            Err(SessionError::Connection(err)) => {
                tracing::warn!(error = %err, attempt, "worker control session dropped, will reconnect");
            }
        }
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let delay = jittered_backoff(attempt, MAX_RECONNECT_BACKOFF, &mut rng);
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
        attempt = attempt.saturating_add(1);
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one cohesive connect/handshake/event-loop unit sharing `stream` and `ctx` across every select arm; splitting it would scatter that shared state across helper functions"
)]
async fn connect_and_serve(
    config: &Arc<WorkerConfig>,
    credential: &str,
    pools: &Arc<PoolSupervisor>,
    artifacts: &Arc<LocalArtifactStore>,
    live: &LiveAttempts,
    http: &reqwest::Client,
    shutdown: &CancellationToken,
) -> Result<(), SessionError> {
    let uri: http::Uri =
        config
            .remote_url
            .as_str()
            .parse()
            .map_err(|err: http::uri::InvalidUri| {
                SessionError::Connection(anyhow::anyhow!("invalid remote url: {err}"))
            })?;
    let transport = connectrpc::client::Http2Connection::connect_plaintext(uri.clone())
        .await?
        .shared(64);
    let client_config = connectrpc::client::ClientConfig::new(uri)
        .with_protocol(connectrpc::Protocol::Grpc)
        .with_default_header("authorization", format!("Bearer {credential}"));
    let session_client = WorkerSessionServiceClient::new(transport.clone(), client_config.clone());
    let transfer = WorkerTransferServiceClient::new(transport, client_config);

    let mut stream = session_client.session().await?;
    stream
        .send(wrap_worker_message(Handshake {
            protocol_major: gpq_proto::PROTOCOL_MAJOR,
            protocol_minor: gpq_proto::PROTOCOL_MINOR,
            worker_version: env!("CARGO_PKG_VERSION").to_owned(),
            host_descriptor: crate::host_descriptor(),
            ..Default::default()
        }))
        .await?;

    let Some(ack_msg) = stream.message::<RemoteMessage>().await? else {
        return Err(SessionError::Connection(anyhow::anyhow!(
            "remote closed the stream before handshaking"
        )));
    };
    let Some(remote_message::Message::HandshakeAck(ack)) = ack_msg.to_owned_message().message
    else {
        return Err(SessionError::Connection(anyhow::anyhow!(
            "expected a HandshakeAck as the first message"
        )));
    };
    if !gpq_proto::protocol_compatible(ack.protocol_major) {
        return Err(SessionError::ProtocolMismatch(format!(
            "worker speaks protocol {}.{} but remote requires major {}",
            gpq_proto::PROTOCOL_MAJOR,
            gpq_proto::PROTOCOL_MINOR,
            ack.protocol_major
        )));
    }
    tracing::info!(session_id = %ack.session_id, resumable = ack.resumable_attempt_ids.len(), "worker session handshake complete");

    stream.send(capability_report_message(pools)).await?;

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<WorkerMessage>(256);
    let ctx = SessionCtx {
        config: config.clone(),
        pools: pools.clone(),
        artifacts: artifacts.clone(),
        live: live.clone(),
        http: http.clone(),
        transfer,
        outbound: outbound_tx,
    };

    let mut heartbeat_ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut maintenance_ticker = tokio::time::interval(MAINTENANCE_INTERVAL);
    maintenance_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut capability_changes = pools.watch_changes();
    let mut attempt_tasks: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                cancel_all_attempts(&ctx.live);
                let deadline = tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT);
                tokio::pin!(deadline);
                while !attempt_tasks.is_empty() {
                    tokio::select! {
                        () = &mut deadline => {
                            tracing::warn!(
                                abandoned = ?live_attempt_ids(&ctx.live),
                                "timed out waiting for in-flight attempts to report before shutdown"
                            );
                            break;
                        }
                        joined = attempt_tasks.join_next() => {
                            if let Some(Err(err)) = joined {
                                tracing::warn!(error = %err, "attempt task panicked during shutdown drain");
                            }
                        }
                        maybe_out = outbound_rx.recv() => {
                            if let Some(message) = maybe_out {
                                let _ = stream.send(message).await;
                            }
                        }
                    }
                }
                while let Ok(message) = outbound_rx.try_recv() {
                    let _ = stream.send(message).await;
                }
                stream.close_send();
                return Ok(());
            }
            _ = heartbeat_ticker.tick() => {
                let now = Utc::now();
                let ids = live_attempt_ids(&ctx.live);
                stream.send(heartbeat_message(&ids)).await?;
                renew_leases(&ctx.live, now);
            }
            _ = maintenance_ticker.tick() => {
                run_maintenance_tick(pools, artifacts).await;
            }
            changed = capability_changes.changed() => {
                if changed.is_err() {
                    // The Pool supervisor is shutting down.
                    return Ok(());
                }
                stream.send(capability_report_message(pools)).await?;
            }
            maybe_out = outbound_rx.recv() => {
                if let Some(message) = maybe_out {
                    stream.send(message).await?;
                }
            }
            joined = attempt_tasks.join_next(), if !attempt_tasks.is_empty() => {
                if let Some(Err(err)) = joined {
                    tracing::warn!(error = %err, "attempt task panicked");
                }
            }
            received = stream.message::<RemoteMessage>() => {
                let Some(received) = received? else {
                    return Err(SessionError::Connection(anyhow::anyhow!("remote closed the control stream")));
                };
                let Some(message) = received.to_owned_message().message else { continue };
                if handle_remote_message(message, &ctx, &mut attempt_tasks) {
                    stream.close_send();
                    return Ok(());
                }
            }
        }
    }
}

/// Runs one maintenance pass: restarts a crashed managed process, unloads
/// idle Active Runtimes, and expires stale local output Artifacts (ADR
/// 0005, ADR 0008).
async fn run_maintenance_tick(pools: &PoolSupervisor, artifacts: &LocalArtifactStore) {
    pools.check_process_liveness().await;
    // Re-probe so a Pool that was still loading at startup, or whose backend
    // gained a model or custom node since, becomes ready without a restart
    // (ADR 0005).
    pools.refresh_capabilities().await;
    pools.release_idle(std::time::Instant::now()).await;
    if let Err(err) = artifacts.expire(Utc::now(), OUTPUT_ARTIFACT_TTL).await {
        tracing::warn!(error = %err, "failed to expire local output artifacts");
    }
}

/// Dispatches one `RemoteMessage`. Returns `true` if Remote asked the
/// Worker to shut down. Lease-triggered Attempt executions are tracked in
/// `attempt_tasks` so shutdown can wait for them to finish and flush their
/// terminal report before the control stream closes.
fn handle_remote_message(
    message: remote_message::Message,
    ctx: &SessionCtx,
    attempt_tasks: &mut JoinSet<()>,
) -> bool {
    match message {
        remote_message::Message::HandshakeAck(_) => {
            tracing::warn!("received an unexpected second HandshakeAck; ignoring");
        }
        remote_message::Message::Lease(lease) => {
            let ctx = ctx.clone();
            attempt_tasks.spawn(async move { accept_lease(*lease, &ctx).await });
        }
        remote_message::Message::Cancel(cancel) => cancel_attempt(&ctx.live, &cancel),
        remote_message::Message::Deliver(deliver) => {
            let ctx = ctx.clone();
            tokio::spawn(async move { deliver_artifact(*deliver, &ctx).await });
        }
        remote_message::Message::Discard(discard) => {
            let artifacts = ctx.artifacts.clone();
            tokio::spawn(async move { discard_output(*discard, &artifacts).await });
        }
        remote_message::Message::Shutdown(shutdown) => {
            tracing::info!(reason = %shutdown.reason, "remote requested worker shutdown");
            return true;
        }
    }
    false
}

/// Everything about a `LeaseAssignment` that must be valid before touching a
/// Device Pool at all (ADR 0003): an unparseable attempt id is silently
/// dropped (there is no attempt id to reply about), while a bad modality or
/// model hash gets an explicit `LeaseRejected` instead of any Attempt-shaped
/// work happening. Pure so the classification is unit-testable without a
/// Pool or a backend.
enum LeaseAcceptance {
    /// The lease's own attempt id could not be parsed; there is no id to
    /// reply to Remote with, so it is dropped rather than rejected.
    Unidentifiable,
    /// Rejected before any Pool or backend interaction.
    Rejected { kind: FailureKind, message: String },
    /// Valid enough to proceed to Pool/backend acquisition.
    Accepted {
        attempt_id: AttemptId,
        modality: Modality,
        resident_model: Option<ContentHash>,
    },
}

fn classify_lease(lease: &LeaseAssignment) -> LeaseAcceptance {
    let Ok(attempt_id) = lease.attempt_id.parse::<AttemptId>() else {
        return LeaseAcceptance::Unidentifiable;
    };
    let modality = match executor::modality_from_proto(lease.modality) {
        Ok(modality) => modality,
        Err(message) => {
            return LeaseAcceptance::Rejected {
                kind: FailureKind::Internal,
                message,
            };
        }
    };
    let resident_model = if lease.model_sha256.is_empty() {
        None
    } else {
        match lease.model_sha256.parse::<ContentHash>() {
            Ok(hash) => Some(hash),
            Err(err) => {
                return LeaseAcceptance::Rejected {
                    kind: FailureKind::Internal,
                    message: format!("invalid model hash on lease: {err}"),
                };
            }
        }
    };
    LeaseAcceptance::Accepted {
        attempt_id,
        modality,
        resident_model,
    }
}

/// Acquires the lease's Slot and spawns its execution, or rejects it before
/// any Attempt-shaped work happens (ADR 0003: pre-execution capability
/// mismatches must not create work).
async fn accept_lease(lease: LeaseAssignment, ctx: &SessionCtx) {
    let (attempt_id, modality, resident_model) = match classify_lease(&lease) {
        LeaseAcceptance::Unidentifiable => {
            tracing::warn!(attempt_id = %lease.attempt_id, "lease carried an unparseable attempt id; dropping");
            return;
        }
        LeaseAcceptance::Rejected { kind, message } => {
            return reject_lease(ctx, &lease.attempt_id, kind, message).await;
        }
        LeaseAcceptance::Accepted {
            attempt_id,
            modality,
            resident_model,
        } => (attempt_id, modality, resident_model),
    };

    // One atomic operation under the Pool's `op_lock`: reserving the Slot in
    // a separate call let `release_idle` tear down the runtime this Attempt
    // had just paid to load, in the gap before the Slot was marked busy.
    let slot = match ctx
        .pools
        .ensure_runtime_and_acquire_slot(
            &lease.pool_id,
            modality.backend_kind(),
            resident_model,
            attempt_id,
        )
        .await
    {
        Ok(slot) => slot,
        Err(err) => return reject_lease(ctx, &lease.attempt_id, err.kind, err.message).await,
    };
    let Some(backend) = ctx.pools.backend(&lease.pool_id) else {
        return reject_lease(
            ctx,
            &lease.attempt_id,
            FailureKind::Internal,
            format!("pool {} has no backend handle", lease.pool_id),
        )
        .await;
    };

    let expires_at = lease
        .lease_expires_at
        .clone()
        .into_option()
        .and_then(|timestamp| DateTime::<Utc>::try_from(timestamp).ok())
        .unwrap_or_else(|| Utc::now() + lease_ttl_chrono());
    let cancel = CancellationToken::new();
    if let Ok(mut guard) = ctx.live.lock() {
        guard.insert(
            attempt_id,
            LiveAttempt {
                cancel: cancel.clone(),
                lease_expires_at: expires_at,
            },
        );
    }

    let attempt_dir = ctx
        .config
        .state_dir
        .join("attempts")
        .join(attempt_id.to_string());
    let exec_ctx = ExecutionContext {
        attempt_id,
        pool_key: lease.pool_id.clone(),
        lease,
        slot,
        backend,
        pools: ctx.pools.clone(),
        outbound: ctx.outbound.clone(),
        transfer: ctx.transfer.clone(),
        http: ctx.http.clone(),
        artifacts: ctx.artifacts.clone(),
        attempt_dir,
        cancel,
    };
    let live = ctx.live.clone();
    executor::execute(exec_ctx).await;
    if let Ok(mut guard) = live.lock() {
        guard.remove(&attempt_id);
    }
}

async fn reject_lease(ctx: &SessionCtx, attempt_id: &str, kind: FailureKind, message: String) {
    tracing::warn!(attempt_id, %kind, detail = %message, "rejecting lease before execution");
    let failure = executor::to_proto_failure(kind, message, false);
    let rejected = LeaseRejected {
        attempt_id: attempt_id.to_owned(),
        failure: MessageField::some(failure),
        ..Default::default()
    };
    let _ = ctx.outbound.send(wrap_worker_message(rejected)).await;
}

fn cancel_attempt(live: &LiveAttempts, cancel: &CancelRequest) {
    let Ok(attempt_id) = cancel.attempt_id.parse::<AttemptId>() else {
        return;
    };
    if let Ok(guard) = live.lock()
        && let Some(entry) = guard.get(&attempt_id)
    {
        entry.cancel.cancel();
    }
}

/// Cooperatively cancels every currently live Attempt, run once when the
/// Worker itself is shutting down so backends stop before `main` force
/// terminates their processes and each Attempt's terminal report has a
/// chance to reach Remote (ADR 0003, ADR 0020).
fn cancel_all_attempts(live: &LiveAttempts) {
    let Ok(guard) = live.lock() else { return };
    let tokens: Vec<CancellationToken> = guard.values().map(|entry| entry.cancel.clone()).collect();
    drop(guard);
    for token in tokens {
        token.cancel();
    }
}

async fn discard_output(discard: DiscardOutput, artifacts: &LocalArtifactStore) {
    if let Err(err) = artifacts.delete(&discard.delivery_token).await {
        tracing::warn!(error = %err, artifact_id = %discard.artifact_id, reason = %discard.reason, "failed to discard local output");
    }
}

/// Uploads a Worker-local output through `WorkerTransferService::DeliverArtifact`,
/// resuming from the requested offset (ADR 0008). Bulk bytes travel over
/// this separate RPC so they never block the control stream's heartbeats.
async fn deliver_artifact(request: DeliverRequest, ctx: &SessionCtx) {
    let mut reader = match ctx
        .artifacts
        .open_for_read(&request.delivery_token, request.offset)
        .await
    {
        Ok(reader) => reader,
        Err(err) => {
            tracing::warn!(error = %err, artifact_id = %request.artifact_id, "cannot open local artifact for delivery");
            return;
        }
    };
    let manifest = executor::manifest_to_proto(reader.manifest());
    let mut requests = vec![deliver_artifact_message(DeliverArtifactStart {
        artifact_id: request.artifact_id.clone(),
        delivery_token: request.delivery_token.clone(),
        manifest: MessageField::some(manifest),
        offset: request.offset,
        ..Default::default()
    })];

    loop {
        match reader.next_chunk().await {
            Ok(Some(chunk)) => {
                let last = chunk.last;
                requests.push(deliver_artifact_message(ArtifactChunk {
                    offset: chunk.offset,
                    data: chunk.data,
                    last,
                    ..Default::default()
                }));
                if last {
                    break;
                }
            }
            Ok(None) => break,
            Err(err) => {
                tracing::warn!(error = %err, artifact_id = %request.artifact_id, "failed reading local artifact for delivery");
                return;
            }
        }
    }
    if let Err(err) = ctx.transfer.deliver_artifact(requests).await {
        tracing::warn!(error = %err, artifact_id = %request.artifact_id, "delivering local artifact to remote failed");
    }
}

fn deliver_artifact_message(
    message: impl Into<gpq_proto::gpq::worker::v1::deliver_artifact_request::Message>,
) -> DeliverArtifactRequest {
    DeliverArtifactRequest {
        message: Some(message.into()),
        ..Default::default()
    }
}

fn wrap_worker_message(message: impl Into<worker_message::Message>) -> WorkerMessage {
    WorkerMessage {
        message: Some(message.into()),
        ..Default::default()
    }
}

fn capability_report_message(pools: &PoolSupervisor) -> WorkerMessage {
    wrap_worker_message(CapabilityReport {
        pools: pools
            .capabilities()
            .iter()
            .map(pool_advertisement)
            .collect(),
        ..Default::default()
    })
}

fn pool_advertisement(data: &PoolAdvertisementData) -> PoolAdvertisement {
    let mut slots = Vec::with_capacity(data.slots_total as usize);
    for index in 0..data.slots_total {
        let busy_attempt = data.slots_busy.get(index as usize).copied();
        slots.push(SlotAdvertisement {
            slot_id: format!("{}-slot-{index}", data.pool_key),
            busy: busy_attempt.is_some(),
            attempt_id: busy_attempt.map(|id| id.to_string()).unwrap_or_default(),
            ..Default::default()
        });
    }
    PoolAdvertisement {
        pool_id: data.pool_key.clone(),
        backend_kind: buffa::EnumValue::Known(executor::backend_kind_to_proto(data.backend)),
        backend_version: data.backend_version.clone(),
        ready: data.ready,
        unready_reason: data.unready_reason.clone().unwrap_or_default(),
        slots,
        resident_model_sha256: data
            .resident_model
            .map(|hash| hash.to_hex())
            .unwrap_or_default(),
        accelerator_memory_bytes: data.accelerator_memory_bytes.unwrap_or_default(),
        model_sha256: data.models.iter().map(ContentHash::to_hex).collect(),
        custom_nodes: data.custom_nodes.clone().into_iter().collect(),
        probes: data.probes.clone().into_iter().collect(),
        ..Default::default()
    }
}

fn heartbeat_message(attempt_ids: &[AttemptId]) -> WorkerMessage {
    wrap_worker_message(Heartbeat {
        attempt_ids: attempt_ids.iter().map(AttemptId::to_string).collect(),
        sent_at: MessageField::some(Timestamp::from(Utc::now())),
        ..Default::default()
    })
}

fn lease_ttl_chrono() -> chrono::Duration {
    chrono::Duration::from_std(LEASE_TTL).unwrap_or(chrono::Duration::seconds(45))
}

fn live_attempt_ids(live: &LiveAttempts) -> Vec<AttemptId> {
    live.lock()
        .map(|guard| guard.keys().copied().collect())
        .unwrap_or_default()
}

/// Optimistically renews every live Attempt's locally-tracked lease expiry
/// after a heartbeat is sent, mirroring the 45-second server-side lease this
/// Worker expects to have just refreshed (ADR 0003).
fn renew_leases(live: &LiveAttempts, now: DateTime<Utc>) {
    let renewed_until = now + lease_ttl_chrono();
    if let Ok(mut guard) = live.lock() {
        for entry in guard.values_mut() {
            entry.lease_expires_at = renewed_until;
        }
    }
}

/// Cooperatively cancels every Attempt whose locally-tracked lease has
/// lapsed, even without an explicit `CancelRequest` (ADR 0003).
fn cancel_expired_leases(live: &LiveAttempts, now: DateTime<Utc>) {
    let Ok(guard) = live.lock() else { return };
    let snapshot: HashMap<AttemptId, DateTime<Utc>> = guard
        .iter()
        .map(|(id, entry)| (*id, entry.lease_expires_at))
        .collect();
    let tokens: Vec<CancellationToken> = expired_attempt_ids(&snapshot, now)
        .into_iter()
        .filter_map(|id| guard.get(&id).map(|entry| entry.cancel.clone()))
        .collect();
    drop(guard);
    for token in tokens {
        token.cancel();
    }
}

/// Attempt ids whose locally-tracked lease has lapsed and must be
/// cooperatively cancelled even without an explicit `CancelRequest`
/// (ADR 0003). Pure decision logic, kept separate from the live-attempt
/// table so it is unit-testable without a real session.
fn expired_attempt_ids(
    leases: &HashMap<AttemptId, DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Vec<AttemptId> {
    leases
        .iter()
        .filter(|(_, expires_at)| **expires_at <= now)
        .map(|(id, _)| *id)
        .collect()
}

/// Base reconnect delay before jitter: doubles from one second, capped at
/// `MAX_RECONNECT_BACKOFF`.
fn backoff_base(attempt: u32) -> std::time::Duration {
    let shift = attempt.min(5); // 2^5 = 32s, already above the 30s cap.
    std::time::Duration::from_secs((1_u64 << shift).min(MAX_RECONNECT_BACKOFF.as_secs()))
}

/// Applies +/-25% jitter to the base reconnect delay for `attempt`, never
/// exceeding `max`.
fn jittered_backoff(
    attempt: u32,
    max: std::time::Duration,
    rng: &mut impl Rng,
) -> std::time::Duration {
    let base = backoff_base(attempt);
    let factor = rng.random_range(0.75_f64..=1.25_f64);
    #[expect(
        clippy::cast_precision_loss,
        reason = "reconnect backoff millis never approach 2^52; an approximate jittered delay is acceptable"
    )]
    let millis = (base.as_millis() as f64 * factor).round();
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "millis is rounded and clamped to >= 0.0 by max(0.0); reconnect delays never approach u64::MAX"
    )]
    let jittered_millis = millis.max(0.0) as u64;
    std::time::Duration::from_millis(jittered_millis).min(max)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    #[test]
    fn backoff_base_doubles_then_caps_at_thirty_seconds() {
        assert_eq!(backoff_base(0), Duration::from_secs(1));
        assert_eq!(backoff_base(1), Duration::from_secs(2));
        assert_eq!(backoff_base(2), Duration::from_secs(4));
        assert_eq!(backoff_base(3), Duration::from_secs(8));
        assert_eq!(backoff_base(4), Duration::from_secs(16));
        assert_eq!(backoff_base(5), Duration::from_secs(30));
        assert_eq!(backoff_base(20), Duration::from_secs(30));
    }

    #[test]
    fn jittered_backoff_stays_within_quarter_bounds_and_under_max() {
        let mut rng = StdRng::seed_from_u64(7);
        let max = Duration::from_secs(30);
        for attempt in 0..8 {
            let base = backoff_base(attempt);
            let delay = jittered_backoff(attempt, max, &mut rng);
            let lower = base.mul_f64(0.75);
            let upper = base.mul_f64(1.25).min(max);
            assert!(
                delay >= lower && delay <= upper,
                "attempt {attempt}: {delay:?} not within [{lower:?}, {upper:?}]"
            );
        }
    }

    #[test]
    fn jittered_backoff_never_exceeds_max_even_when_base_would() {
        let mut rng = StdRng::seed_from_u64(1);
        let max = Duration::from_secs(5);
        for _ in 0..50 {
            assert!(jittered_backoff(10, max, &mut rng) <= max);
        }
    }

    #[test]
    fn expired_attempt_ids_finds_only_lapsed_leases() {
        let now = Utc::now();
        let mut leases = HashMap::new();
        let live_id = AttemptId::new();
        let expired_id = AttemptId::new();
        leases.insert(live_id, now + chrono::Duration::seconds(10));
        leases.insert(expired_id, now - chrono::Duration::seconds(1));
        assert_eq!(expired_attempt_ids(&leases, now), vec![expired_id]);
    }

    #[test]
    fn expired_attempt_ids_treats_exact_expiry_as_expired() {
        let now = Utc::now();
        let id = AttemptId::new();
        let leases = HashMap::from([(id, now)]);
        assert_eq!(expired_attempt_ids(&leases, now), vec![id]);
    }

    #[test]
    fn expired_attempt_ids_is_empty_when_every_lease_is_live() {
        let now = Utc::now();
        let leases = HashMap::from([(AttemptId::new(), now + chrono::Duration::seconds(1))]);
        assert!(expired_attempt_ids(&leases, now).is_empty());
    }

    #[test]
    fn lease_with_unparseable_attempt_id_is_unidentifiable() {
        let lease = LeaseAssignment {
            attempt_id: "not-a-uuid".to_owned(),
            ..Default::default()
        };
        assert!(matches!(
            classify_lease(&lease),
            LeaseAcceptance::Unidentifiable
        ));
    }

    #[test]
    fn lease_with_unknown_modality_is_rejected_before_any_pool_work() {
        // ADR 0003: a bad modality is rejected before any Slot or backend
        // interaction, not merely dropped like an unidentifiable lease.
        let lease = LeaseAssignment {
            attempt_id: AttemptId::new().to_string(),
            modality: buffa::EnumValue::Known(gpq_proto::gpq::v1::Modality::MODALITY_UNSPECIFIED),
            ..Default::default()
        };
        assert!(matches!(
            classify_lease(&lease),
            LeaseAcceptance::Rejected {
                kind: FailureKind::Internal,
                ..
            }
        ));
    }

    #[test]
    fn lease_with_malformed_model_hash_is_rejected() {
        let lease = LeaseAssignment {
            attempt_id: AttemptId::new().to_string(),
            modality: buffa::EnumValue::Known(gpq_proto::gpq::v1::Modality::MODALITY_LLM),
            model_sha256: "not-hex".to_owned(),
            ..Default::default()
        };
        assert!(matches!(
            classify_lease(&lease),
            LeaseAcceptance::Rejected {
                kind: FailureKind::Internal,
                ..
            }
        ));
    }

    #[test]
    fn well_formed_lease_is_accepted_with_its_fields_parsed() {
        let attempt_id = AttemptId::new();
        let hash = ContentHash::digest(b"model");
        let lease = LeaseAssignment {
            attempt_id: attempt_id.to_string(),
            modality: buffa::EnumValue::Known(gpq_proto::gpq::v1::Modality::MODALITY_LLM),
            model_sha256: hash.to_hex(),
            ..Default::default()
        };
        let LeaseAcceptance::Accepted {
            attempt_id: parsed_id,
            modality,
            resident_model,
        } = classify_lease(&lease)
        else {
            panic!("expected a well-formed lease to be Accepted");
        };
        assert_eq!(parsed_id, attempt_id);
        assert_eq!(modality, Modality::Llm);
        assert_eq!(resident_model, Some(hash));
    }

    #[test]
    fn lease_without_a_model_hash_has_no_resident_model() {
        let lease = LeaseAssignment {
            attempt_id: AttemptId::new().to_string(),
            modality: buffa::EnumValue::Known(gpq_proto::gpq::v1::Modality::MODALITY_IMAGE),
            model_sha256: String::new(),
            ..Default::default()
        };
        let LeaseAcceptance::Accepted { resident_model, .. } = classify_lease(&lease) else {
            panic!("expected a well-formed lease to be Accepted");
        };
        assert_eq!(resident_model, None);
    }
}
