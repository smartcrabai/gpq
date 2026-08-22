//! Worker control Session (ADR 0003, ADR 0004, ADR 0010).
//!
//! The bidirectional `WorkerSessionService::Session` stream is the only
//! in-memory state Remote keeps about a Worker (ADR 0010): everything it
//! reports here is immediately persisted or published, and losing the
//! connection loses nothing but the socket itself — a reconnecting Worker
//! resumes through [`crate::db::attempts::live_for_worker`] and a fresh
//! `CapabilityReport`.

use buffa::EnumValue;
use connectrpc::{ConnectError, Encodable, ErrorCode, Response, ServiceResult, ServiceStream};
use futures::StreamExt;
use gpq_domain::{
    ArtifactId, ArtifactManifest, ArtifactPlacement, AttemptId, BackendKind, ContentHash,
    ExecutionLimits, FailureKind, GenerationId, GenerationState, MediaKind, Modality,
    RetryDecision, TenantId, WorkerId,
};
use gpq_proto::gpq::v1::{
    ArtifactManifest as ProtoArtifactManifest, ArtifactPlacement as ProtoArtifactPlacement,
    BackendKind as ProtoBackendKind, FailureKind as ProtoFailureKind, MediaKind as ProtoMediaKind,
};
use gpq_proto::gpq::worker::v1::__buffa::oneof::worker_message;
use gpq_proto::gpq::worker::v1::{
    AttemptFailure, AttemptOutput, AttemptProgress, AttemptResult, AttemptRunning,
    AttemptTokenDelta, CancelAcknowledged, CancelRequest, CapabilityReport, DiscardOutput,
    HandshakeAck, Heartbeat, LeaseRejected, PoolAdvertisement, RemoteMessage, WorkerMessage,
    WorkerSessionService,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::db::workers::PoolUpsert;
use crate::events::GenerationEvent;
use crate::scheduler::leased_output_key;
use crate::state::AppState;

/// Implements [`WorkerSessionService`] against shared Remote state.
#[derive(Clone)]
pub struct SessionApi {
    state: AppState,
}

impl SessionApi {
    /// Builds the service over `state`.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

/// Wraps any error as an internal Connect error. Generic over `Display`
/// rather than a concrete error type so it drops straight into `.map_err`
/// for both `sqlx::Result` and `anyhow::Result` call sites without an
/// intermediate closure.
fn internal(err: impl std::fmt::Display) -> ConnectError {
    ConnectError::new(ErrorCode::Internal, err.to_string())
}

fn unauthenticated() -> ConnectError {
    ConnectError::new(ErrorCode::Unauthenticated, "invalid worker credential")
}

/// Parses a wire id string into a typed id, as a `sqlx::Error` so callers
/// that are already threading `?` through `sqlx::Result` can reuse it.
fn parse_uuid_id<T>(text: &str, wrap: impl FnOnce(uuid::Uuid) -> T) -> Result<T, sqlx::Error> {
    text.parse::<uuid::Uuid>()
        .map(wrap)
        .map_err(|err| sqlx::Error::Decode(Box::new(err)))
}

/// Absolute one-shot download URL for an output Artifact, built from the
/// configured public base URL (ADR 0006, ADR 0008). Falls back to the bare
/// relative path on a URL-join failure so a misconfigured base URL never
/// blocks reporting that the output exists.
fn output_download_url(state: &AppState, artifact_id: ArtifactId) -> String {
    crate::artifacts::download_url(&state.config.public_base_url, artifact_id).map_or_else(
        |err| {
            tracing::error!(%err, %artifact_id, "failed to build absolute artifact download url");
            crate::artifacts::download_path(artifact_id)
        },
        |url| url.to_string(),
    )
}

impl WorkerSessionService for SessionApi {
    async fn session(
        &self,
        ctx: connectrpc::RequestContext,
        requests: connectrpc::InboundStream<WorkerMessage>,
    ) -> ServiceResult<ServiceStream<impl Encodable<RemoteMessage> + Send + use<>>> {
        let Some(token) = crate::auth::bearer_token(ctx.headers()) else {
            return Err(unauthenticated());
        };
        let Some((tenant_id, worker_id)) = self
            .state
            .db
            .authenticate_worker(token)
            .await
            .map_err(internal)?
        else {
            return Err(unauthenticated());
        };

        let mut requests = requests;
        let Some(first) = requests.next().await else {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "empty session stream",
            ));
        };
        let first = first?.to_owned_message();
        let Some(worker_message::Message::Handshake(handshake)) = first.message else {
            return Err(ConnectError::new(
                ErrorCode::InvalidArgument,
                "first message on a Session must be a Handshake",
            ));
        };
        let handshake = *handshake;

        // ADR 0004: a major protocol mismatch is rejected explicitly.
        if !gpq_proto::protocol_compatible(handshake.protocol_major) {
            return Err(ConnectError::new(
                ErrorCode::FailedPrecondition,
                format!(
                    "worker protocol major {} is incompatible with remote major {}",
                    handshake.protocol_major,
                    gpq_proto::PROTOCOL_MAJOR
                ),
            ));
        }

        let session_id = uuid::Uuid::now_v7().to_string();

        let mut tx = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        // ADR 0003: a reconnecting Worker resumes Attempts still leased to it.
        let resumable = crate::db::attempts::live_for_worker(&mut tx, tenant_id, worker_id)
            .await
            .map_err(internal)?;
        crate::db::workers::mark_session(&mut tx, tenant_id, worker_id, &session_id)
            .await
            .map_err(internal)?;
        tx.commit().await.map_err(internal)?;

        let (outbound_tx, outbound_rx) = mpsc::channel::<RemoteMessage>(64);
        let guard = self.state.workers.register(
            tenant_id,
            worker_id,
            session_id.clone(),
            outbound_tx.clone(),
        );

        let heartbeat_interval =
            buffa_types::google::protobuf::Duration::from(gpq_domain::HEARTBEAT_INTERVAL);
        let lease_ttl = buffa_types::google::protobuf::Duration::from(gpq_domain::LEASE_TTL);
        let ack = HandshakeAck {
            protocol_major: gpq_proto::PROTOCOL_MAJOR,
            protocol_minor: gpq_proto::PROTOCOL_MINOR,
            session_id: session_id.clone(),
            heartbeat_interval: heartbeat_interval.into(),
            lease_ttl: lease_ttl.into(),
            resumable_attempt_ids: resumable
                .iter()
                .map(|row| row.attempt_id().to_string())
                .collect(),
            ..Default::default()
        };
        if outbound_tx
            .send(RemoteMessage {
                message: ack.into(),
                ..Default::default()
            })
            .await
            .is_err()
        {
            return Err(ConnectError::new(
                ErrorCode::Unavailable,
                "worker disconnected before the handshake ack could be sent",
            ));
        }

        let state = self.state.clone();
        spawn_session_pump(state, tenant_id, worker_id, requests, outbound_tx, guard);

        let outbound_stream = ReceiverStream::new(outbound_rx).map(Ok::<_, ConnectError>);
        Response::stream_ok(outbound_stream)
    }
}

/// Spawns [`run_inbound_pump`] for a newly established session, clearing
/// the session marker once the pump ends (control stream closed, credential
/// revoked, or graceful shutdown) so a reconnecting Worker is not blocked
/// by a stale marker; live Attempts are left for `expiry.rs`'s lease sweep
/// to fail as `WorkerLost` once their lease lapses, rather than failed
/// eagerly here, since a fast reconnect may still resume them.
///
/// Boxes the pump's future: it is held across the whole spawned task's
/// suspended lifetime, and inlining it un-boxed would otherwise make the
/// task's own generated future needlessly large.
fn spawn_session_pump(
    state: AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    requests: connectrpc::InboundStream<WorkerMessage>,
    outbound_tx: mpsc::Sender<RemoteMessage>,
    guard: crate::registry::SessionGuard,
) {
    tokio::spawn(async move {
        let _guard = guard;
        Box::pin(run_inbound_pump(
            &state,
            tenant_id,
            worker_id,
            requests,
            &outbound_tx,
        ))
        .await;
        if let Ok(mut tx) = state.db.begin_tenant(tenant_id).await {
            let _ = crate::db::workers::clear_session(&mut tx, tenant_id, worker_id).await;
            let _ = tx.commit().await;
        }
    });
}

/// Reads Worker messages until the stream ends, dispatching each to its
/// handler. Errors from individual messages are logged and do not end the
/// session; the pump ends when the inbound stream ends (cleanly or on a
/// transport error), a `Heartbeat` finds the Worker Credential revoked
/// (ADR 0009, contract C3), or `state.shutdown` (ADR 0020) is cancelled for
/// Remote's graceful drain — the latter closes the outbound side too, by
/// simply returning and letting the caller's `outbound_tx` drop.
async fn run_inbound_pump(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    mut requests: connectrpc::InboundStream<WorkerMessage>,
    outbound: &mpsc::Sender<RemoteMessage>,
) {
    loop {
        let item = tokio::select! {
            () = state.shutdown.cancelled() => {
                tracing::info!(%worker_id, "ending worker session for remote's graceful shutdown");
                break;
            }
            item = requests.next() => item,
        };
        let Some(item) = item else {
            break;
        };
        let Ok(item) = item else {
            break;
        };
        let Some(message) = item.to_owned_message().message else {
            continue;
        };
        tracing::trace!(%worker_id, kind = worker_message_kind(&message), "dispatching worker message");
        match message {
            worker_message::Message::Handshake(_) => {
                tracing::warn!(%worker_id, "ignoring a duplicate Handshake on an established session");
            }
            worker_message::Message::CapabilityReport(report) => {
                handle_capability_report(state, tenant_id, worker_id, *report).await;
            }
            worker_message::Message::Heartbeat(heartbeat) => {
                if !handle_heartbeat(state, tenant_id, worker_id, *heartbeat, outbound).await {
                    break;
                }
            }
            worker_message::Message::AttemptRunning(running) => {
                log_on_err(
                    "AttemptRunning",
                    try_handle_attempt_running(state, tenant_id, *running),
                )
                .await;
            }
            worker_message::Message::AttemptProgress(progress) => {
                log_on_err(
                    "AttemptProgress",
                    try_handle_attempt_progress(state, tenant_id, *progress),
                )
                .await;
            }
            worker_message::Message::AttemptTokenDelta(delta) => {
                log_on_err(
                    "AttemptTokenDelta",
                    try_handle_token_delta(state, tenant_id, *delta),
                )
                .await;
            }
            worker_message::Message::AttemptResult(result) => {
                log_on_err(
                    "AttemptResult",
                    // Boxed: `try_handle_attempt_result`'s future is large
                    // and would otherwise be embedded inline in this
                    // dispatch loop's own generated future.
                    Box::pin(try_handle_attempt_result(
                        state, tenant_id, worker_id, *result, outbound,
                    )),
                )
                .await;
            }
            worker_message::Message::AttemptFailure(failure) => {
                log_on_err(
                    "AttemptFailure",
                    try_handle_attempt_failure(state, tenant_id, *failure),
                )
                .await;
            }
            worker_message::Message::CancelAcknowledged(ack) => {
                log_on_err(
                    "CancelAcknowledged",
                    try_handle_cancel_acknowledged(state, tenant_id, *ack),
                )
                .await;
            }
            worker_message::Message::LeaseRejected(rejected) => {
                log_on_err(
                    "LeaseRejected",
                    try_handle_lease_rejected(state, tenant_id, *rejected),
                )
                .await;
            }
        }
    }
}

/// Stable label naming each `WorkerMessage` variant (ADR 0004), used for
/// dispatch tracing. Kept exhaustive and pure so a newly added oneof variant
/// fails to compile here before it can silently dispatch unlabeled.
fn worker_message_kind(message: &worker_message::Message) -> &'static str {
    match message {
        worker_message::Message::Handshake(_) => "handshake",
        worker_message::Message::CapabilityReport(_) => "capability_report",
        worker_message::Message::Heartbeat(_) => "heartbeat",
        worker_message::Message::AttemptRunning(_) => "attempt_running",
        worker_message::Message::AttemptProgress(_) => "attempt_progress",
        worker_message::Message::AttemptTokenDelta(_) => "attempt_token_delta",
        worker_message::Message::AttemptResult(_) => "attempt_result",
        worker_message::Message::AttemptFailure(_) => "attempt_failure",
        worker_message::Message::CancelAcknowledged(_) => "cancel_acknowledged",
        worker_message::Message::LeaseRejected(_) => "lease_rejected",
    }
}

/// A minimal read of a Generation's current lifecycle, fetched purely to
/// build the [`GenerationEvent::State`] published after a Worker-reported
/// transition. Deliberately outside `db::generations` (owned by the
/// scheduler/admission slice): this is a read-only projection, not a
/// transition.
struct GenerationSnapshot {
    state: GenerationState,
    attempt_count: u32,
    failure: Option<(FailureKind, String)>,
}

async fn generation_snapshot(
    conn: &mut sqlx::PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
) -> sqlx::Result<Option<GenerationSnapshot>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        state: String,
        attempt_count: i32,
        failure_kind: Option<String>,
        failure_message: String,
    }

    let row: Option<Row> = sqlx::query_as(
        "SELECT state, attempt_count, failure_kind, failure_message \
         FROM generations WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let state = row
        .state
        .parse::<GenerationState>()
        .map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
    let attempt_count = u32::try_from(row.attempt_count).unwrap_or(0);
    let failure = match row.failure_kind {
        Some(kind_text) => {
            let kind = kind_text
                .parse::<FailureKind>()
                .unwrap_or(FailureKind::Internal);
            Some((kind, row.failure_message))
        }
        None => None,
    };
    Ok(Some(GenerationSnapshot {
        state,
        attempt_count,
        failure,
    }))
}

/// Persists and live-publishes a State transition through
/// [`crate::events::EventHub::record`] (ADR 0008: state transitions are
/// retained, so a reconnecting Native watcher can replay them).
async fn publish_state_event(
    state: &AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    snapshot: &GenerationSnapshot,
) -> anyhow::Result<()> {
    state
        .events
        .record(
            &state.db,
            tenant_id,
            generation_id,
            &GenerationEvent::State {
                state: snapshot.state,
                attempt_count: snapshot.attempt_count,
                failure: snapshot.failure.clone(),
            },
        )
        .await
}

/// Releases the underlying bytes of Artifacts returned by
/// `db::artifacts::delete_inputs_for_generation`, called at every
/// Worker-reported Generation terminal transition (ADR 0008: "Inputs are
/// deleted when the Generation terminates"). Object-store objects are
/// deleted directly; inline-relay buffers are dropped from memory. Inputs
/// never use Worker-local placement, so that case only logs a warning.
/// Best-effort: a failure here only leaks bytes behind an already-deleted
pub(crate) async fn release_deleted_inputs(
    artifacts: &crate::artifacts::ArtifactService,
    rows: Vec<crate::db::artifacts::ArtifactRow>,
) {
    for row in rows {
        match row.placement {
            ArtifactPlacement::ObjectStore => {
                let Some(key) = row.object_key.as_deref() else {
                    tracing::warn!(artifact_id = %row.id, "object-store input artifact is missing its object_key");
                    continue;
                };
                if let Err(err) = artifacts.delete(key).await {
                    tracing::warn!(%err, artifact_id = %row.id, "failed to delete object-store input artifact");
                }
            }
            ArtifactPlacement::InlineRelay => artifacts.discard_local(row.id),
            ArtifactPlacement::WorkerLocal => {
                tracing::warn!(artifact_id = %row.id, "unexpected worker-local input artifact at terminal cleanup");
            }
        }
    }
}

/// Converts a proto `BackendKind` to the domain type, `None` for unset or
/// unrecognized values.
fn domain_backend_kind(value: EnumValue<ProtoBackendKind>) -> Option<BackendKind> {
    match value {
        EnumValue::Known(ProtoBackendKind::BACKEND_KIND_LLAMA_CPP) => Some(BackendKind::LlamaCpp),
        EnumValue::Known(ProtoBackendKind::BACKEND_KIND_COMFYUI) => Some(BackendKind::ComfyUi),
        EnumValue::Known(ProtoBackendKind::BACKEND_KIND_UNSPECIFIED) | EnumValue::Unknown(_) => {
            None
        }
    }
}

/// Converts a proto `ArtifactPlacement` to the domain type, `None` for unset
/// or unrecognized values.
fn domain_placement(value: EnumValue<ProtoArtifactPlacement>) -> Option<ArtifactPlacement> {
    match value {
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE) => {
            Some(ArtifactPlacement::ObjectStore)
        }
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL) => {
            Some(ArtifactPlacement::WorkerLocal)
        }
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_INLINE_RELAY) => {
            Some(ArtifactPlacement::InlineRelay)
        }
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_UNSPECIFIED)
        | EnumValue::Unknown(_) => None,
    }
}

/// Converts a proto `FailureKind` to the domain type, `None` for unset or
/// unrecognized values.
fn domain_failure_kind(value: EnumValue<ProtoFailureKind>) -> Option<FailureKind> {
    match value {
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_INVALID_INPUT) => {
            Some(FailureKind::InvalidInput)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_UNSUPPORTED_CAPABILITY) => {
            Some(FailureKind::UnsupportedCapability)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_MODEL_UNAVAILABLE) => {
            Some(FailureKind::ModelUnavailable)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_OUT_OF_MEMORY) => {
            Some(FailureKind::OutOfMemory)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_BACKEND_CRASHED) => {
            Some(FailureKind::BackendCrashed)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_EXECUTION_TIMED_OUT) => {
            Some(FailureKind::ExecutionTimedOut)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_CANCELLED) => Some(FailureKind::Cancelled),
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_TRANSFER_FAILED) => {
            Some(FailureKind::TransferFailed)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_INTERNAL) => Some(FailureKind::Internal),
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_WORKER_LOST) => {
            Some(FailureKind::WorkerLost)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_LEASE_EXPIRED) => {
            Some(FailureKind::LeaseExpired)
        }
        EnumValue::Known(ProtoFailureKind::FAILURE_KIND_UNSPECIFIED) | EnumValue::Unknown(_) => {
            None
        }
    }
}

/// Converts a wire Artifact manifest to the domain type, `None` for a
/// malformed digest or an unset/unrecognized media kind.
fn domain_manifest_from_proto(proto: &ProtoArtifactManifest) -> Option<ArtifactManifest> {
    let digest = proto.digest_sha256.parse::<ContentHash>().ok()?;
    let kind = match proto.kind {
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_IMAGE) => MediaKind::Image,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_VIDEO) => MediaKind::Video,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_AUDIO) => MediaKind::Audio,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_TEXT) => MediaKind::Text,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_BINARY) => MediaKind::Binary,
        EnumValue::Known(ProtoMediaKind::MEDIA_KIND_UNSPECIFIED) | EnumValue::Unknown(_) => {
            return None;
        }
    };
    Some(ArtifactManifest {
        size_bytes: proto.size_bytes,
        digest,
        kind,
        mime_type: proto.mime_type.clone(),
    })
}

/// Builds a [`PoolUpsert`] from one advertised Pool, `None` if its backend
/// kind is unset or unrecognized (the Pool is dropped from this report; a
/// later report with a recognized kind will pick it back up).
///
/// The Worker's own busy/idle slot tally is deliberately NOT carried over:
/// `device_pools.free_slots` is generated from Remote's `claimed_slots`
/// counter (migration 0004), which `claim_slot`/`release_slot` own. A
/// capability report is a stale snapshot that predates leases already
/// dispatched, so letting it set free slots would resurrect claims the
/// scheduler has spent and double-book the Pool.
fn pool_upsert_from_proto(pool: &PoolAdvertisement) -> Option<PoolUpsert> {
    let backend_kind = domain_backend_kind(pool.backend_kind)?;
    let total_slots = u32::try_from(pool.slots.len()).unwrap_or(u32::MAX);
    let resident_model_sha256 = if pool.resident_model_sha256.is_empty() {
        None
    } else {
        pool.resident_model_sha256.parse().ok()
    };
    let accelerator_memory_bytes =
        (pool.accelerator_memory_bytes != 0).then_some(pool.accelerator_memory_bytes);
    let custom_nodes = pool
        .custom_nodes
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let probes = pool
        .probes
        .iter()
        .map(|(key, value)| (key.clone(), *value))
        .collect();
    let model_versions = pool
        .model_sha256
        .iter()
        .filter_map(|hex| hex.parse::<ContentHash>().ok())
        .collect();

    Some(PoolUpsert {
        pool_key: pool.pool_id.clone(),
        backend_kind,
        backend_version: pool.backend_version.clone(),
        ready: pool.ready,
        unready_reason: pool.unready_reason.clone(),
        total_slots,
        resident_model_sha256,
        accelerator_memory_bytes,
        custom_nodes,
        probes,
        model_versions,
    })
}

/// Awaits `fut`, logging any error as a warning naming `label` and
/// otherwise discarding it: one inbound Worker message failing to process
/// must never end the Session (see `run_inbound_pump`).
async fn log_on_err<E>(label: &str, fut: impl Future<Output = Result<(), E>> + Send)
where
    E: std::fmt::Display,
{
    if let Err(err) = fut.await {
        tracing::warn!(%err, "failed to process {label}");
    }
}

async fn handle_capability_report(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    report: CapabilityReport,
) {
    if let Err(err) = try_handle_capability_report(state, tenant_id, worker_id, report).await {
        tracing::warn!(%err, %worker_id, "failed to process CapabilityReport");
    }
}

async fn try_handle_capability_report(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    report: CapabilityReport,
) -> sqlx::Result<()> {
    let pools: Vec<PoolUpsert> = report
        .pools
        .iter()
        .filter_map(pool_upsert_from_proto)
        .collect();

    // ADR 0012: llama.cpp Model Versions are automatically registered by
    // content hash. ComfyUI's on-disk models are referenced through
    // Tenant-registered Workflow Versions instead, so they are not
    // auto-registered here.
    let llama_versions: Vec<(ContentHash, Modality, ExecutionLimits)> = report
        .pools
        .iter()
        .filter(|pool| {
            matches!(
                pool.backend_kind,
                EnumValue::Known(ProtoBackendKind::BACKEND_KIND_LLAMA_CPP)
            )
        })
        .flat_map(|pool| pool.model_sha256.iter())
        .filter_map(|hex| hex.parse::<ContentHash>().ok())
        .map(|hash| (hash, Modality::Llm, ExecutionLimits::default()))
        .collect();

    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !llama_versions.is_empty() {
        crate::db::workers::register_model_versions(&mut tx, tenant_id, &llama_versions).await?;
    }
    crate::db::workers::upsert_pools(&mut tx, tenant_id, worker_id, &pools).await?;
    crate::db::workers::touch_last_seen(&mut tx, tenant_id, worker_id).await?;
    tx.commit().await?;

    state.scheduler.wake_worker(worker_id);
    Ok(())
}

/// Handles a `Heartbeat`, returning whether the Session should stay open.
/// `false` means [`try_handle_heartbeat`] found the Worker Credential
/// revoked mid-Session (ADR 0009, contract C3) and the caller must end the
/// Session; any other failure is logged and never ends the Session (see
/// `run_inbound_pump`).
async fn handle_heartbeat(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    heartbeat: Heartbeat,
    outbound: &mpsc::Sender<RemoteMessage>,
) -> bool {
    match try_handle_heartbeat(state, tenant_id, worker_id, heartbeat, outbound).await {
        Ok(still_live) => {
            if !still_live {
                tracing::warn!(%worker_id, "ending worker session: credential revoked mid-session");
            }
            still_live
        }
        Err(err) => {
            tracing::warn!(%err, %worker_id, "failed to process Heartbeat");
            true
        }
    }
}

/// Renews the live Attempts a `Heartbeat` names, after first re-checking
/// that the Worker Credential is still enrolled and unrevoked — ADR 0009's
/// revocation is otherwise only enforced once, by `authenticate_worker` at
/// Session start (contract C3). Returns `Ok(false)` without renewing
/// anything when the Worker is no longer live, so the caller ends the
/// Session instead of continuing to hand it leases.
async fn try_handle_heartbeat(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    heartbeat: Heartbeat,
    outbound: &mpsc::Sender<RemoteMessage>,
) -> sqlx::Result<bool> {
    let attempt_ids: Vec<AttemptId> = heartbeat
        .attempt_ids
        .iter()
        .filter_map(|text| text.parse::<uuid::Uuid>().ok())
        .map(AttemptId::from_uuid)
        .collect();

    let now = state.db.now().await?;
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !crate::db::workers::is_enrolled_and_unrevoked(&mut tx, tenant_id, worker_id).await? {
        return Ok(false);
    }
    let not_renewed =
        crate::db::attempts::heartbeat(&mut tx, tenant_id, worker_id, &attempt_ids, now).await?;
    crate::db::workers::touch_last_seen(&mut tx, tenant_id, worker_id).await?;
    tx.commit().await?;

    for cancel in not_renewed_cancel_messages(&not_renewed) {
        let _ = outbound.send(cancel).await;
    }
    Ok(true)
}

/// The `CancelRequest`s owed to Attempts whose lease a `Heartbeat` could not
/// renew (ADR 0003): a `Heartbeat` never fails wholesale, but an Attempt no
/// longer live is told individually so the Worker stops treating it as
/// leased. Pure so the reply is unit-testable without a database.
fn not_renewed_cancel_messages(not_renewed: &[AttemptId]) -> Vec<RemoteMessage> {
    not_renewed
        .iter()
        .map(|&attempt_id| RemoteMessage {
            message: CancelRequest {
                attempt_id: attempt_id.to_string(),
                reason: "lease could not be renewed".to_owned(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .collect()
}

async fn try_handle_attempt_running(
    state: &AppState,
    tenant_id: TenantId,
    running: AttemptRunning,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&running.attempt_id, AttemptId::from_uuid)?;
    let now = state.db.now().await?;
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !crate::db::attempts::mark_running(&mut tx, tenant_id, attempt_id, now).await? {
        return Ok(());
    }
    let Some(generation_id) =
        crate::db::attempts::generation_id_of(&mut tx, tenant_id, attempt_id).await?
    else {
        return Ok(());
    };
    crate::db::generations::mark_running(&mut tx, tenant_id, generation_id, now).await?;
    let snapshot = generation_snapshot(&mut tx, tenant_id, generation_id).await?;
    tx.commit().await?;

    if let Some(snapshot) = snapshot {
        publish_state_event(state, tenant_id, generation_id, &snapshot).await?;
    }
    Ok(())
}

async fn try_handle_attempt_progress(
    state: &AppState,
    tenant_id: TenantId,
    progress: AttemptProgress,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&progress.attempt_id, AttemptId::from_uuid)?;
    let Some(progress) = progress.progress.into_option() else {
        return Ok(());
    };
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    let Some(generation_id) =
        crate::db::attempts::generation_id_of(&mut tx, tenant_id, attempt_id).await?
    else {
        return Ok(());
    };
    let now = state.db.now().await?;
    crate::db::generations::record_progress(
        &mut tx,
        tenant_id,
        generation_id,
        crate::db::generations::ProgressSnapshot {
            fraction: progress.fraction,
            stage: &progress.stage,
            step: progress.step,
            total_steps: progress.total_steps,
        },
        now,
    )
    .await?;
    tx.commit().await?;

    state
        .events
        .record(
            &state.db,
            tenant_id,
            generation_id,
            &GenerationEvent::Progress {
                fraction: progress.fraction,
                stage: progress.stage,
                step: progress.step,
                total_steps: progress.total_steps,
            },
        )
        .await?;
    Ok(())
}

async fn try_handle_token_delta(
    state: &AppState,
    tenant_id: TenantId,
    delta: AttemptTokenDelta,
) -> sqlx::Result<()> {
    // ADR 0008: token deltas are forwarded live and never persisted.
    let attempt_id = parse_uuid_id(&delta.attempt_id, AttemptId::from_uuid)?;
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    let Some(generation_id) =
        crate::db::attempts::generation_id_of(&mut tx, tenant_id, attempt_id).await?
    else {
        return Ok(());
    };
    tx.commit().await?;

    state
        .events
        .publish(generation_id, GenerationEvent::Token { text: delta.text });
    Ok(())
}

/// What a result-commitment outcome means for the Worker's reported outputs
/// (ADR 0003, ADR 0005): only the Attempt whose result became the
/// Generation's Accepted Result keeps them; a stale lease, a duplicate, or
/// an already-terminal Generation all mean the Worker must delete
/// everything it tried to commit. Pure so the outcome-to-reply mapping is
/// unit-testable without a database.
enum ResultReply {
    Accepted(GenerationId),
    Discard,
}

fn classify_result_outcome(outcome: crate::db::generations::AcceptOutcome) -> ResultReply {
    match outcome {
        crate::db::generations::AcceptOutcome::Accepted(generation_id) => {
            ResultReply::Accepted(generation_id)
        }
        crate::db::generations::AcceptOutcome::StaleLease
        | crate::db::generations::AcceptOutcome::AlreadyAccepted
        | crate::db::generations::AcceptOutcome::Terminal => ResultReply::Discard,
    }
}

/// Which Worker an output Artifact's placement should be attributed to
/// (ADR 0008): only Worker-local placement needs the owning Worker recorded,
/// for that Worker's own startup reconciliation scan; object-store and
/// inline-relay outputs are addressed by key/token instead.
fn output_placement_worker(placement: ArtifactPlacement, worker_id: WorkerId) -> Option<WorkerId> {
    matches!(placement, ArtifactPlacement::WorkerLocal).then_some(worker_id)
}

/// Loads the authenticated Tenant's mutable settings, for the output-size
/// gate in [`try_handle_attempt_result`] (mirrors `openai::tenant_settings`,
/// but returns `sqlx::Result` to match this file's other DB helpers).
async fn load_tenant_settings(
    state: &AppState,
    tenant_id: TenantId,
) -> sqlx::Result<gpq_domain::TenantSettings> {
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    crate::db::tenants::load_settings(&mut tx, tenant_id).await
}

/// Fails `attempt_id` and returns `Ok(true)` (the failure is already
/// settled and reported) if any of `outputs` reports a manifest whose size
/// does not fit an S3 `content-length` (`i64`) or exceeds
/// `settings.max_output_artifact_bytes`. Checked before
/// [`try_handle_attempt_result`] opens its result-commit transaction:
/// `db::artifacts::record_output` (called only after the Worker has
/// already uploaded the bytes) rejects an oversized manifest with a hard
/// `sqlx::Error::Decode` that would abort an already-in-progress commit and
/// orphan the upload, so an oversized output is instead treated as a
/// normal (permanent) Attempt failure, same as any other invalid input.
async fn reject_oversized_outputs(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    attempt_id: AttemptId,
    outputs: &[AttemptOutput],
    settings: &gpq_domain::TenantSettings,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<bool> {
    for output in outputs {
        let Some(manifest_proto) = output.manifest.as_option() else {
            continue;
        };
        let Some(manifest) = domain_manifest_from_proto(manifest_proto) else {
            continue;
        };
        if i64::try_from(manifest.size_bytes).is_err()
            || !manifest.fits_within(settings.max_output_artifact_bytes)
        {
            tracing::warn!(
                %worker_id,
                %attempt_id,
                size_bytes = manifest.size_bytes,
                limit_bytes = settings.max_output_artifact_bytes,
                "failing attempt: reported output artifact exceeds the tenant's max_output_artifact_bytes"
            );
            settle_attempt_failure(
                state,
                tenant_id,
                attempt_id,
                FailureKind::InvalidInput,
                "output artifact exceeds the tenant's configured max_output_artifact_bytes",
                false,
                now,
            )
            .await?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Records every output Artifact reported for an accepted result, deletes
/// the now-terminal Generation's input Artifacts (ADR 0008: "Inputs are
/// deleted when the Generation terminates"), commits `tx`, then publishes
/// the resulting state transition and per-output availability events
/// (ADR 0003, ADR 0005). Split out of [`try_handle_attempt_result`]'s
/// `ResultReply::Accepted` arm to keep that function under the line-count
/// lint; behavior is unchanged.
///
/// # Errors
///
/// Returns an error if an object-store output's reported `object_key`
/// does not match the key Remote presigned for this Generation
/// (ADR 0008's confused-deputy prevention), or if recording an output,
/// deleting inputs, reading the post-commit Generation snapshot,
/// committing `tx`, or publishing the state transition fails.
async fn record_accepted_result(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    attempt_id: AttemptId,
    generation_id: GenerationId,
    outputs: &[AttemptOutput],
    mut tx: sqlx::Transaction<'static, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let mut output_ids = Vec::new();
    for output in outputs {
        let Some(manifest_proto) = output.manifest.as_option() else {
            continue;
        };
        let Some(manifest) = domain_manifest_from_proto(manifest_proto) else {
            continue;
        };
        let Some(placement) = domain_placement(output.placement) else {
            continue;
        };

        // Confused-deputy prevention (ADR 0008): the one legitimate
        // object-store key for this Generation's output is the one
        // `scheduler::leased_output_key` presigned and handed to
        // the Worker; anything else — including an empty key —
        // would let a Worker Credential redirect Remote's own S3
        // credentials at an arbitrary key, across Tenants.
        let object_key = if placement == ArtifactPlacement::ObjectStore {
            let expected = leased_output_key(tenant_id, generation_id);
            if output.object_key != expected {
                tracing::warn!(
                    %worker_id,
                    reported_key = %output.object_key,
                    expected_key = %expected,
                    "rejecting AttemptResult: reported object_key does not match the key Remote leased"
                );
                anyhow::bail!(
                    "malformed AttemptResult: object_key does not match the leased key for generation {generation_id}"
                );
            }
            Some(output.object_key.as_str())
        } else {
            (!output.object_key.is_empty()).then_some(output.object_key.as_str())
        };
        let delivery_token =
            (!output.delivery_token.is_empty()).then_some(output.delivery_token.as_str());
        let placement_worker = output_placement_worker(placement, worker_id);

        let row = crate::db::artifacts::record_output(
            &mut tx,
            tenant_id,
            generation_id,
            attempt_id,
            placement_worker,
            &manifest,
            placement,
            object_key,
            delivery_token,
        )
        .await?;
        output_ids.push(row.id);
    }

    // ADR 0008: "Inputs are deleted when the Generation terminates".
    let deleted_inputs =
        crate::db::artifacts::delete_inputs_for_generation(&mut tx, tenant_id, generation_id)
            .await?;

    let snapshot = generation_snapshot(&mut tx, tenant_id, generation_id).await?;
    tx.commit().await?;

    if let Some(snapshot) = snapshot {
        publish_state_event(state, tenant_id, generation_id, &snapshot).await?;
    }
    release_deleted_inputs(&state.artifacts, deleted_inputs).await;
    for artifact_id in output_ids {
        tracing::info!(
            %generation_id,
            %artifact_id,
            download_url = %output_download_url(state, artifact_id),
            "output artifact available"
        );
        state.events.publish(generation_id, GenerationEvent::Output);
    }
    state.scheduler.wake_tenant(tenant_id);
    Ok(())
}

async fn try_handle_attempt_result(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    result: AttemptResult,
    outbound: &mpsc::Sender<RemoteMessage>,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&result.attempt_id, AttemptId::from_uuid)?;
    let now = state.db.now().await?;
    let usage = result.usage.into_option().map(|usage| {
        (
            usage.prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens,
        )
    });

    // Bound every output manifest against the Tenant's configured size
    // limit before opening the result-commit transaction below; see
    // `reject_oversized_outputs` for why.
    let settings = load_tenant_settings(state, tenant_id).await?;
    if reject_oversized_outputs(
        state,
        tenant_id,
        worker_id,
        attempt_id,
        &result.outputs,
        &settings,
        now,
    )
    .await?
    {
        return Ok(());
    }

    let mut tx = state.db.begin_tenant(tenant_id).await?;
    let outcome = crate::db::generations::accept_result(
        &mut tx,
        tenant_id,
        attempt_id,
        worker_id,
        &result.output_text,
        usage,
        now,
    )
    .await?;

    match classify_result_outcome(outcome) {
        ResultReply::Accepted(generation_id) => {
            record_accepted_result(
                state,
                tenant_id,
                worker_id,
                attempt_id,
                generation_id,
                &result.outputs,
                tx,
            )
            .await?;
        }
        ResultReply::Discard => {
            tx.commit().await?;
            // The Worker must delete every output it tried to commit
            // (ADR 0003, ADR 0008). Remote never recorded these Artifacts, so
            // there is no Remote-assigned artifact_id to hand back; the
            // Worker correlates by its own delivery_token.
            for output in &result.outputs {
                let discard = RemoteMessage {
                    message: DiscardOutput {
                        artifact_id: String::new(),
                        delivery_token: output.delivery_token.clone(),
                        reason: "result rejected: stale lease, duplicate, or terminal generation"
                            .to_owned(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                };
                let _ = outbound.send(discard).await;
            }
        }
    }
    Ok(())
}

/// Settles a live Attempt `Failed`, applies ADR 0003's retry policy to its
/// Generation, then publishes the resulting state transition and releases
/// any deleted input Artifacts (ADR 0008).
///
/// Shared by [`try_handle_attempt_failure`] (a Worker-reported failure) and
/// the oversized-output rejection in [`try_handle_attempt_result`], which
/// is itself an Attempt failure discovered by Remote before commit rather
/// than reported by the Worker.
async fn settle_attempt_failure(
    state: &AppState,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    failure_kind: FailureKind,
    message: &str,
    worker_retry_hint: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    let outcome = crate::db::attempts::record_failure(
        &mut tx,
        tenant_id,
        attempt_id,
        failure_kind,
        message,
        worker_retry_hint,
        now,
    )
    .await?;
    let Some((generation_id, decision)) = outcome else {
        tx.commit().await?;
        return Ok(());
    };
    // ADR 0008: "Inputs are deleted when the Generation terminates" — only
    // once the retry policy has actually settled the Generation `Failed`;
    // `Requeue` leaves it live for another Attempt.
    let deleted_inputs = if decision == RetryDecision::Fail {
        crate::db::artifacts::delete_inputs_for_generation(&mut tx, tenant_id, generation_id)
            .await?
    } else {
        Vec::new()
    };
    let snapshot = generation_snapshot(&mut tx, tenant_id, generation_id).await?;
    tx.commit().await?;

    if let Some(snapshot) = snapshot {
        publish_state_event(state, tenant_id, generation_id, &snapshot).await?;
    }
    release_deleted_inputs(&state.artifacts, deleted_inputs).await;
    state.scheduler.wake_tenant(tenant_id);
    Ok(())
}

async fn try_handle_attempt_failure(
    state: &AppState,
    tenant_id: TenantId,
    failure: AttemptFailure,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&failure.attempt_id, AttemptId::from_uuid)?;
    let Some(failure) = failure.failure.into_option() else {
        return Ok(());
    };
    let failure_kind = domain_failure_kind(failure.kind).unwrap_or(FailureKind::Internal);
    let now = state.db.now().await?;
    settle_attempt_failure(
        state,
        tenant_id,
        attempt_id,
        failure_kind,
        &failure.message,
        failure.worker_retry_hint,
        now,
    )
    .await
}

async fn try_handle_cancel_acknowledged(
    state: &AppState,
    tenant_id: TenantId,
    ack: CancelAcknowledged,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&ack.attempt_id, AttemptId::from_uuid)?;
    let now = state.db.now().await?;

    let mut tx = state.db.begin_tenant(tenant_id).await?;
    let generation_id =
        crate::db::attempts::acknowledge_cancel(&mut tx, tenant_id, attempt_id, now).await?;
    let mut deleted_inputs = Vec::new();
    let snapshot = match generation_id {
        Some(generation_id) => {
            // ADR 0008: "Inputs are deleted when the Generation terminates".
            deleted_inputs = crate::db::artifacts::delete_inputs_for_generation(
                &mut tx,
                tenant_id,
                generation_id,
            )
            .await?;
            generation_snapshot(&mut tx, tenant_id, generation_id).await?
        }
        None => None,
    };
    tx.commit().await?;

    if let (Some(generation_id), Some(snapshot)) = (generation_id, snapshot) {
        publish_state_event(state, tenant_id, generation_id, &snapshot).await?;
    }
    release_deleted_inputs(&state.artifacts, deleted_inputs).await;
    Ok(())
}

async fn try_handle_lease_rejected(
    state: &AppState,
    tenant_id: TenantId,
    rejected: LeaseRejected,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&rejected.attempt_id, AttemptId::from_uuid)?;
    let Some(failure) = rejected.failure.into_option() else {
        return Ok(());
    };
    // A pre-execution capability mismatch, not a runtime failure; default to
    // UnsupportedCapability rather than Internal if the Worker left the kind
    // unset.
    let failure_kind =
        domain_failure_kind(failure.kind).unwrap_or(FailureKind::UnsupportedCapability);
    let now = state.db.now().await?;

    let mut tx = state.db.begin_tenant(tenant_id).await?;
    let generation_id = crate::db::attempts::reject_lease(
        &mut tx,
        tenant_id,
        attempt_id,
        failure_kind,
        &failure.message,
        now,
    )
    .await?;
    let Some(generation_id) = generation_id else {
        tx.commit().await?;
        return Ok(());
    };
    let snapshot = generation_snapshot(&mut tx, tenant_id, generation_id).await?;
    tx.commit().await?;

    if let Some(snapshot) = snapshot {
        publish_state_event(state, tenant_id, generation_id, &snapshot).await?;
    }
    state.scheduler.wake_tenant(tenant_id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use gpq_proto::gpq::worker::v1::SlotAdvertisement;

    use super::*;

    #[test]
    fn failure_kind_maps_every_known_variant() {
        let cases = [
            (
                ProtoFailureKind::FAILURE_KIND_INVALID_INPUT,
                FailureKind::InvalidInput,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_UNSUPPORTED_CAPABILITY,
                FailureKind::UnsupportedCapability,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_MODEL_UNAVAILABLE,
                FailureKind::ModelUnavailable,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_OUT_OF_MEMORY,
                FailureKind::OutOfMemory,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_BACKEND_CRASHED,
                FailureKind::BackendCrashed,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_EXECUTION_TIMED_OUT,
                FailureKind::ExecutionTimedOut,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_CANCELLED,
                FailureKind::Cancelled,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_TRANSFER_FAILED,
                FailureKind::TransferFailed,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_INTERNAL,
                FailureKind::Internal,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_WORKER_LOST,
                FailureKind::WorkerLost,
            ),
            (
                ProtoFailureKind::FAILURE_KIND_LEASE_EXPIRED,
                FailureKind::LeaseExpired,
            ),
        ];
        for (proto, domain) in cases {
            assert_eq!(domain_failure_kind(EnumValue::Known(proto)), Some(domain));
        }
    }

    #[test]
    fn failure_kind_unspecified_and_unknown_map_to_none() {
        assert_eq!(
            domain_failure_kind(EnumValue::Known(ProtoFailureKind::FAILURE_KIND_UNSPECIFIED)),
            None
        );
        assert_eq!(domain_failure_kind(EnumValue::Unknown(999)), None);
    }

    #[test]
    fn manifest_conversion_rejects_malformed_digest() {
        let proto = ProtoArtifactManifest {
            size_bytes: 4,
            digest_sha256: "not-hex".to_owned(),
            kind: EnumValue::Known(ProtoMediaKind::MEDIA_KIND_TEXT),
            mime_type: "text/plain".to_owned(),
            ..Default::default()
        };
        assert!(domain_manifest_from_proto(&proto).is_none());
    }

    #[test]
    fn manifest_conversion_accepts_a_well_formed_manifest() {
        let hash = ContentHash::from_bytes([5; 32]);
        let proto = ProtoArtifactManifest {
            size_bytes: 10,
            digest_sha256: hash.to_hex(),
            kind: EnumValue::Known(ProtoMediaKind::MEDIA_KIND_VIDEO),
            mime_type: "video/mp4".to_owned(),
            ..Default::default()
        };
        let Some(manifest) = domain_manifest_from_proto(&proto) else {
            panic!("expected a valid manifest");
        };
        assert_eq!(manifest.digest, hash);
        assert_eq!(manifest.kind, MediaKind::Video);
    }

    #[test]
    fn placement_maps_every_known_variant() {
        assert_eq!(
            domain_placement(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE
            )),
            Some(ArtifactPlacement::ObjectStore)
        );
        assert_eq!(
            domain_placement(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL
            )),
            Some(ArtifactPlacement::WorkerLocal)
        );
        assert_eq!(
            domain_placement(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_INLINE_RELAY
            )),
            Some(ArtifactPlacement::InlineRelay)
        );
        assert_eq!(
            domain_placement(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_UNSPECIFIED
            )),
            None
        );
    }

    fn slot(busy: bool) -> SlotAdvertisement {
        SlotAdvertisement {
            slot_id: "s".to_owned(),
            busy,
            ..Default::default()
        }
    }

    #[test]
    fn pool_upsert_takes_total_slots_and_ignores_worker_busy_flags() {
        let pool = PoolAdvertisement {
            pool_id: "gpu-0".to_owned(),
            backend_kind: EnumValue::Known(ProtoBackendKind::BACKEND_KIND_LLAMA_CPP),
            ready: true,
            slots: vec![slot(false), slot(true), slot(false)],
            ..Default::default()
        };
        let Some(upsert) = pool_upsert_from_proto(&pool) else {
            panic!("expected a valid pool upsert");
        };
        // Total capacity is Worker-owned, so it is carried over verbatim.
        assert_eq!(upsert.total_slots, 3);
        assert_eq!(upsert.backend_kind, BackendKind::LlamaCpp);
        // Free slots are Remote's to compute from `claimed_slots`: the two
        // idle flags in this report must not reach the upsert at all.
    }

    #[test]
    fn pool_upsert_is_none_for_unspecified_backend_kind() {
        let pool = PoolAdvertisement {
            pool_id: "gpu-0".to_owned(),
            backend_kind: EnumValue::Known(ProtoBackendKind::BACKEND_KIND_UNSPECIFIED),
            ..Default::default()
        };
        assert!(pool_upsert_from_proto(&pool).is_none());
    }

    #[test]
    fn pool_upsert_treats_zero_accelerator_memory_as_unknown() {
        let pool = PoolAdvertisement {
            pool_id: "gpu-0".to_owned(),
            backend_kind: EnumValue::Known(ProtoBackendKind::BACKEND_KIND_COMFYUI),
            accelerator_memory_bytes: 0,
            ..Default::default()
        };
        let Some(upsert) = pool_upsert_from_proto(&pool) else {
            panic!("expected a valid pool upsert");
        };
        assert_eq!(upsert.accelerator_memory_bytes, None);
    }

    #[test]
    fn worker_message_kind_labels_every_variant() {
        // ADR 0004: a newly added oneof variant must fail to compile here
        // before it can dispatch silently unlabeled.
        assert_eq!(
            worker_message_kind(&worker_message::Message::Heartbeat(Box::default())),
            "heartbeat"
        );
        assert_eq!(
            worker_message_kind(&worker_message::Message::LeaseRejected(Box::default())),
            "lease_rejected"
        );
    }

    #[test]
    fn accepted_outcome_carries_the_generation_id_and_is_not_discarded() {
        // ADR 0003: only the Attempt whose result became the Accepted
        // Result keeps its outputs.
        let generation_id = GenerationId::new();
        let reply = classify_result_outcome(crate::db::generations::AcceptOutcome::Accepted(
            generation_id,
        ));
        let ResultReply::Accepted(id) = reply else {
            panic!("Accepted outcome must not discard");
        };
        assert_eq!(id, generation_id);
    }

    #[test]
    fn stale_lease_duplicate_and_terminal_outcomes_all_discard() {
        for outcome in [
            crate::db::generations::AcceptOutcome::StaleLease,
            crate::db::generations::AcceptOutcome::AlreadyAccepted,
            crate::db::generations::AcceptOutcome::Terminal,
        ] {
            assert!(matches!(
                classify_result_outcome(outcome),
                ResultReply::Discard
            ));
        }
    }

    #[test]
    fn only_worker_local_placement_attributes_an_owning_worker() {
        // ADR 0008: object-store and inline-relay outputs are addressed by
        // key/token, not by owning Worker.
        let worker_id = WorkerId::new();
        assert_eq!(
            output_placement_worker(ArtifactPlacement::WorkerLocal, worker_id),
            Some(worker_id)
        );
        assert_eq!(
            output_placement_worker(ArtifactPlacement::ObjectStore, worker_id),
            None
        );
        assert_eq!(
            output_placement_worker(ArtifactPlacement::InlineRelay, worker_id),
            None
        );
    }

    #[test]
    fn not_renewed_attempts_each_get_a_cancel_request() {
        let attempt_id = AttemptId::new();
        let messages = not_renewed_cancel_messages(&[attempt_id]);
        assert_eq!(messages.len(), 1);
        let expected = RemoteMessage {
            message: CancelRequest {
                attempt_id: attempt_id.to_string(),
                reason: "lease could not be renewed".to_owned(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        assert_eq!(messages[0], expected);
    }

    #[test]
    fn no_not_renewed_attempts_means_no_cancel_requests() {
        assert!(not_renewed_cancel_messages(&[]).is_empty());
    }
}
