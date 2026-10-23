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
    ArtifactManifest, ArtifactPlacement, AttemptId, ContentHash, FailureKind, GenerationId,
    GenerationState, LoraSelection, MediaKind, ModelAssetRole, ModelFamily, ModelManifest,
    RetryDecision, SamplingPreset, ShapeLimits, TenantId, WorkerId,
};
use gpq_proto::gpq::v1::{
    ArtifactManifest as ProtoArtifactManifest, ArtifactPlacement as ProtoArtifactPlacement,
    FailureKind as ProtoFailureKind, MediaKind as ProtoMediaKind, ModelAdvertisement,
    ModelAssetRole as ProtoModelAssetRole, ModelFamily as ProtoModelFamily,
    ModelManifest as ProtoModelManifest, ShapeLimits as ProtoShapeLimits,
};
use gpq_proto::gpq::worker::v1::__buffa::oneof::worker_message;
use gpq_proto::gpq::worker::v1::{
    AttemptFailure, AttemptOutput, AttemptProgress, AttemptResult, AttemptRunning,
    CancelAcknowledged, CancelRequest, CapabilityReport, DiscardOutput, HandshakeAck, Heartbeat,
    LeaseRejected, PoolAdvertisement, RemoteMessage, WorkerMessage, WorkerSessionService,
};
use std::collections::{BTreeMap, HashSet};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::db::workers::{PoolUpsert, lock_current_worker_credential};
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

impl WorkerSessionService for SessionApi {
    async fn session(
        &self,
        ctx: connectrpc::RequestContext,
        mut requests: connectrpc::InboundStream<WorkerMessage>,
    ) -> ServiceResult<ServiceStream<impl Encodable<RemoteMessage> + Send + use<>>> {
        let (tenant_id, worker_id, credential_hash) =
            authenticate_session(&self.state, ctx, &mut requests).await?;
        let session_id = uuid::Uuid::now_v7().to_string();

        let mut tx = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, &credential_hash)
            .await
            .map_err(internal)?
        {
            return Err(unauthenticated());
        }
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

        spawn_session_pump(
            self.state.clone(),
            tenant_id,
            worker_id,
            credential_hash,
            requests,
            outbound_tx,
            guard,
        );

        let outbound_stream = ReceiverStream::new(outbound_rx).map(Ok::<_, ConnectError>);
        Response::stream_ok(outbound_stream)
    }
}

/// Authenticates the Worker and consumes the stream's required protocol handshake.
async fn authenticate_session(
    state: &AppState,
    ctx: connectrpc::RequestContext,
    requests: &mut connectrpc::InboundStream<WorkerMessage>,
) -> Result<(TenantId, WorkerId, Vec<u8>), ConnectError> {
    let Some(token) = crate::auth::bearer_token(ctx.headers()) else {
        return Err(unauthenticated());
    };
    let Some((tenant_id, worker_id)) = state
        .db
        .authenticate_worker(token)
        .await
        .map_err(internal)?
    else {
        return Err(unauthenticated());
    };
    let credential_hash = state.db.hasher().hash(token);

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
    Ok((tenant_id, worker_id, credential_hash))
}

/// The authenticated Worker identity and credential hash bound to one session.
/// Stateful handlers still lock and revalidate the credential per message.
struct AuthenticatedWorker<'a> {
    tenant_id: TenantId,
    worker_id: WorkerId,
    credential_hash: &'a [u8],
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
    credential_hash: Vec<u8>,
    requests: connectrpc::InboundStream<WorkerMessage>,
    outbound_tx: mpsc::Sender<RemoteMessage>,
    guard: crate::registry::SessionGuard,
) {
    tokio::spawn(async move {
        let _guard = guard;
        let worker = AuthenticatedWorker {
            tenant_id,
            worker_id,
            credential_hash: &credential_hash,
        };
        Box::pin(run_inbound_pump(&state, &worker, requests, &outbound_tx)).await;
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
    worker: &AuthenticatedWorker<'_>,
    mut requests: connectrpc::InboundStream<WorkerMessage>,
    outbound: &mpsc::Sender<RemoteMessage>,
) {
    loop {
        let item = tokio::select! {
            () = state.shutdown.cancelled() => {
                tracing::info!(worker_id = %worker.worker_id, "ending worker session for remote's graceful shutdown");
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
        tracing::trace!(worker_id = %worker.worker_id, kind = worker_message_kind(&message), "dispatching worker message");
        if !dispatch_worker_message(state, worker, message, outbound).await {
            break;
        }
    }
}

/// Dispatches one already-authenticated message; `false` ends the session
/// only when a heartbeat finds that the Worker Credential has been revoked.
async fn dispatch_attempt_result(
    state: &AppState,
    worker: &AuthenticatedWorker<'_>,
    result: AttemptResult,
    outbound: &mpsc::Sender<RemoteMessage>,
) {
    log_on_err(
        "AttemptResult",
        Box::pin(try_handle_attempt_result(
            state,
            worker.tenant_id,
            worker.worker_id,
            worker.credential_hash,
            result,
            outbound,
        )),
    )
    .await;
}

async fn dispatch_attempt_failure(
    state: &AppState,
    worker: &AuthenticatedWorker<'_>,
    failure: AttemptFailure,
) {
    log_on_err(
        "AttemptFailure",
        Box::pin(try_handle_attempt_failure(
            state,
            worker.tenant_id,
            worker.worker_id,
            worker.credential_hash,
            failure,
        )),
    )
    .await;
}

async fn dispatch_worker_message(
    state: &AppState,
    worker: &AuthenticatedWorker<'_>,
    message: worker_message::Message,
    outbound: &mpsc::Sender<RemoteMessage>,
) -> bool {
    match message {
        worker_message::Message::Handshake(_) => {
            tracing::warn!(worker_id = %worker.worker_id, "ignoring a duplicate Handshake on an established session");
        }
        worker_message::Message::CapabilityReport(report) => {
            handle_capability_report(
                state,
                worker.tenant_id,
                worker.worker_id,
                worker.credential_hash,
                *report,
            )
            .await;
        }
        worker_message::Message::Heartbeat(heartbeat) => {
            return handle_heartbeat(
                state,
                worker.tenant_id,
                worker.worker_id,
                worker.credential_hash,
                *heartbeat,
                outbound,
            )
            .await;
        }
        worker_message::Message::AttemptRunning(running) => {
            log_on_err(
                "AttemptRunning",
                try_handle_attempt_running(
                    state,
                    worker.tenant_id,
                    worker.worker_id,
                    worker.credential_hash,
                    *running,
                ),
            )
            .await;
        }
        worker_message::Message::AttemptProgress(progress) => {
            log_on_err(
                "AttemptProgress",
                try_handle_attempt_progress(
                    state,
                    worker.tenant_id,
                    worker.worker_id,
                    worker.credential_hash,
                    *progress,
                ),
            )
            .await;
        }
        worker_message::Message::AttemptResult(result) => {
            dispatch_attempt_result(state, worker, *result, outbound).await;
        }
        worker_message::Message::AttemptFailure(failure) => {
            dispatch_attempt_failure(state, worker, *failure).await;
        }
        worker_message::Message::CancelAcknowledged(ack) => {
            log_on_err(
                "CancelAcknowledged",
                try_handle_cancel_acknowledged(
                    state,
                    worker.tenant_id,
                    worker.worker_id,
                    worker.credential_hash,
                    *ack,
                ),
            )
            .await;
        }
        worker_message::Message::LeaseRejected(rejected) => {
            log_on_err(
                "LeaseRejected",
                try_handle_lease_rejected(
                    state,
                    worker.tenant_id,
                    worker.worker_id,
                    worker.credential_hash,
                    *rejected,
                ),
            )
            .await;
        }
    }
    true
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
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_REMOTE_LOCAL) => {
            Some(ArtifactPlacement::RemoteLocal)
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

/// Converts a proto `ModelFamily` to the domain type, `None` for unset or
/// unrecognized values.
fn domain_model_family(value: EnumValue<ProtoModelFamily>) -> Option<ModelFamily> {
    match value {
        EnumValue::Known(ProtoModelFamily::MODEL_FAMILY_QWEN_IMAGE21) => {
            Some(ModelFamily::QwenImage21)
        }
        EnumValue::Known(ProtoModelFamily::MODEL_FAMILY_MINIMAX_H3) => Some(ModelFamily::MinimaxH3),
        EnumValue::Known(ProtoModelFamily::MODEL_FAMILY_UNSPECIFIED) | EnumValue::Unknown(_) => {
            None
        }
    }
}

/// Converts a proto `ModelAssetRole` to the domain type, `None` for unset or
/// unrecognized values.
fn domain_asset_role(value: EnumValue<ProtoModelAssetRole>) -> Option<ModelAssetRole> {
    match value {
        EnumValue::Known(ProtoModelAssetRole::MODEL_ASSET_ROLE_DIFFUSION) => {
            Some(ModelAssetRole::Diffusion)
        }
        EnumValue::Known(ProtoModelAssetRole::MODEL_ASSET_ROLE_TEXT_ENCODER) => {
            Some(ModelAssetRole::TextEncoder)
        }
        EnumValue::Known(ProtoModelAssetRole::MODEL_ASSET_ROLE_VISION_ENCODER) => {
            Some(ModelAssetRole::VisionEncoder)
        }
        EnumValue::Known(ProtoModelAssetRole::MODEL_ASSET_ROLE_VAE) => Some(ModelAssetRole::Vae),
        EnumValue::Known(ProtoModelAssetRole::MODEL_ASSET_ROLE_AUDIO_VAE) => {
            Some(ModelAssetRole::AudioVae)
        }
        EnumValue::Known(ProtoModelAssetRole::MODEL_ASSET_ROLE_UNSPECIFIED)
        | EnumValue::Unknown(_) => None,
    }
}

/// Converts an advertised Model manifest to the domain type. `None` for any
/// unset or unrecognized enum, a malformed or duplicate asset, a malformed
/// `LoRA` hash, or a missing sampling preset: the whole manifest is then not
/// registered, because a partially understood manifest cannot hash to its
/// advertised content identity.
fn model_manifest_from_proto(proto: &ProtoModelManifest) -> Option<ModelManifest> {
    let family = domain_model_family(proto.family)?;
    let mut assets = BTreeMap::new();
    for asset in &proto.assets {
        let role = domain_asset_role(asset.role)?;
        let sha256 = asset.sha256.parse::<ContentHash>().ok()?;
        if assets.insert(role, sha256).is_some() {
            return None;
        }
    }
    let default_loras = proto
        .default_loras
        .iter()
        .map(|lora| {
            Some(LoraSelection {
                sha256: lora.sha256.parse::<ContentHash>().ok()?,
                scale: lora.scale,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let sampling = proto.sampling.as_option()?;
    Some(ModelManifest {
        family,
        assets,
        default_loras,
        sampling: SamplingPreset {
            steps: sampling.steps,
            cfg_scale: sampling.cfg_scale,
            sigmas: sampling.sigmas.clone(),
        },
    })
}

/// Converts an advertised Pool's shape limits; an absent message means
/// every limit is unknown (no bound).
fn shape_limits_from_proto(limits: Option<&ProtoShapeLimits>) -> ShapeLimits {
    match limits {
        Some(limits) => ShapeLimits {
            max_image_pixels: limits.max_image_pixels,
            max_video_pixels: limits.max_video_pixels,
            max_video_frames: limits.max_video_frames,
            max_steps: limits.max_steps,
        },
        None => ShapeLimits::default(),
    }
}

/// The Model Versions a Pool advertised that Remote may trust: each manifest
/// must be valid and must hash (ADR 0012's canonical hash) to the content
/// hash it was advertised under. A mismatch is logged and dropped, so the
/// Pool still registers its other models but never offers the bad one.
fn verified_models(advertisements: &[ModelAdvertisement]) -> Vec<(ContentHash, ModelManifest)> {
    advertisements
        .iter()
        .filter_map(|advertisement| {
            let advertised = advertisement.version_sha256.parse::<ContentHash>().ok()?;
            let manifest = advertisement
                .manifest
                .as_option()
                .and_then(model_manifest_from_proto)?;
            let computed = manifest.canonical_hash().ok();
            if computed != Some(advertised) {
                tracing::warn!(
                    advertised = %advertisement.version_sha256,
                    "dropping advertised model: manifest does not hash to its advertised version"
                );
                return None;
            }
            if let Err(error) = manifest.validate() {
                tracing::warn!(%error, %advertised, "dropping advertised model: manifest is not runnable");
                return None;
            }
            Some((advertised, manifest))
        })
        .collect()
}

/// Builds a [`PoolUpsert`] from one advertised Pool plus the verified Model
/// Versions it carries (which the caller registers before upserting), `None`
/// if its accelerator is unset (the Pool is dropped from this report; a later
/// report with a named accelerator will pick it back up).
///
/// The Worker's own busy/idle slot tally is deliberately NOT carried over:
/// `device_pools.free_slots` is generated from Remote's `claimed_slots`
/// counter (migration 0004), which `claim_slot`/`release_slot` own. A
/// capability report is a stale snapshot that predates leases already
/// dispatched, so letting it set free slots would resurrect claims the
/// scheduler has spent and double-book the Pool.
fn pool_upsert_from_proto(
    pool: &PoolAdvertisement,
) -> Option<(PoolUpsert, Vec<(ContentHash, ModelManifest)>)> {
    if pool.accelerator.is_empty() {
        return None;
    }
    let models = verified_models(&pool.models);
    let model_hashes = models.iter().map(|(hash, _)| *hash).collect();
    let resident_model_sha256 = if pool.resident_model_sha256.is_empty() {
        None
    } else {
        pool.resident_model_sha256.parse::<ContentHash>().ok()
    };
    // Explicit presence on the wire: absent stays `None`, present zero stays `Some(0)`.
    let accelerator_memory_bytes = pool.accelerator_memory_bytes;
    let image_loras = pool
        .image_lora_sha256
        .iter()
        .filter_map(|hex| hex.parse::<ContentHash>().ok())
        .collect();

    let upsert = PoolUpsert {
        pool_key: pool.pool_id.clone(),
        accelerator: pool.accelerator.clone(),
        accelerator_version: pool.backend_version.clone(),
        ready: pool.ready,
        unready_reason: pool.unready_reason.clone(),
        total_slots: u32::try_from(pool.slots.len()).unwrap_or(u32::MAX),
        resident_model_sha256,
        accelerator_memory_bytes,
        probes: BTreeMap::new(),
        model_hashes,
        image_loras,
        shape_limits: shape_limits_from_proto(pool.shape_limits.as_option()),
    };
    Some((upsert, models))
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
    credential_hash: &[u8],
    report: CapabilityReport,
) {
    if let Err(err) =
        try_handle_capability_report(state, tenant_id, worker_id, credential_hash, report).await
    {
        tracing::warn!(%err, %worker_id, "failed to process CapabilityReport");
    }
}

async fn try_handle_capability_report(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    credential_hash: &[u8],
    report: CapabilityReport,
) -> sqlx::Result<()> {
    let mut pools = Vec::with_capacity(report.pools.len());
    // ADR 0012: Model Versions are registered by content hash before any
    // Pool that names them is upserted, so every `model_hashes` entry refers
    // to a registered version.
    let mut versions = BTreeMap::new();
    for pool in &report.pools {
        if let Some((upsert, models)) = pool_upsert_from_proto(pool) {
            pools.push(upsert);
            versions.extend(models);
        }
    }
    let versions: Vec<(ContentHash, ModelManifest)> = versions.into_iter().collect();

    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, credential_hash).await? {
        return Ok(());
    }
    crate::db::catalog::register_model_versions(&mut tx, tenant_id, &versions).await?;
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
    credential_hash: &[u8],
    heartbeat: Heartbeat,
    outbound: &mpsc::Sender<RemoteMessage>,
) -> bool {
    match try_handle_heartbeat(
        state,
        tenant_id,
        worker_id,
        credential_hash,
        heartbeat,
        outbound,
    )
    .await
    {
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
    credential_hash: &[u8],
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
    if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, credential_hash).await? {
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
    worker_id: WorkerId,
    credential_hash: &[u8],
    running: AttemptRunning,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&running.attempt_id, AttemptId::from_uuid)?;
    let now = state.db.now().await?;
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, credential_hash).await? {
        return Ok(());
    }
    if !crate::db::attempts::mark_running(&mut tx, tenant_id, attempt_id, worker_id, now).await? {
        return Ok(());
    }
    let Some(generation_id) =
        crate::db::attempts::generation_id_of(&mut tx, tenant_id, attempt_id, worker_id, now)
            .await?
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
    worker_id: WorkerId,
    credential_hash: &[u8],
    progress: AttemptProgress,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&progress.attempt_id, AttemptId::from_uuid)?;
    let Some(progress) = progress.progress.into_option() else {
        return Ok(());
    };
    let now = state.db.now().await?;
    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, credential_hash).await? {
        return Ok(());
    }
    let Some(generation_id) =
        crate::db::attempts::generation_id_of(&mut tx, tenant_id, attempt_id, worker_id, now)
            .await?
    else {
        return Ok(());
    };
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

/// What a result-commitment outcome means for the Worker's reported outputs
/// (ADR 0003, ADR 0005): the accepted Attempt keeps its outputs, and a replay
/// preserves only delivery tokens already referenced by that Accepted Result.
/// Stale attempts, different Attempts after acceptance, and terminal
/// Generations discard the unreferenced outputs.
enum ResultReply {
    Accepted(GenerationId),
    AcceptedReplay,
    Discard,
}

fn classify_result_outcome(outcome: crate::db::generations::AcceptOutcome) -> ResultReply {
    match outcome {
        crate::db::generations::AcceptOutcome::Accepted(generation_id) => {
            ResultReply::Accepted(generation_id)
        }
        crate::db::generations::AcceptOutcome::AcceptedReplay => ResultReply::AcceptedReplay,
        crate::db::generations::AcceptOutcome::StaleLease
        | crate::db::generations::AcceptOutcome::AlreadyAccepted
        | crate::db::generations::AcceptOutcome::Terminal => ResultReply::Discard,
    }
}

/// Which Worker an output Artifact's placement should be attributed to
/// (ADR 0008): only Worker-local placement needs the owning Worker recorded,
/// for that Worker's own startup reconciliation scan; object-store outputs
/// are addressed by key instead.
fn output_placement_worker(placement: ArtifactPlacement, worker_id: WorkerId) -> Option<WorkerId> {
    matches!(placement, ArtifactPlacement::WorkerLocal).then_some(worker_id)
}

/// Loads the authenticated Tenant's mutable settings, for the output-size
/// gate in [`try_handle_attempt_result`].
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
    worker: &AuthenticatedWorker<'_>,
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
                worker_id = %worker.worker_id,
                %attempt_id,
                size_bytes = manifest.size_bytes,
                limit_bytes = settings.max_output_artifact_bytes,
                "failing attempt: reported output artifact exceeds the tenant's max_output_artifact_bytes"
            );
            settle_attempt_failure(
                state,
                worker,
                attempt_id,
                crate::db::attempts::FailureDetails {
                    kind: FailureKind::InvalidInput,
                    message: "output artifact exceeds the tenant's configured max_output_artifact_bytes",
                    worker_retry_hint: false,
                },
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
/// (ADR 0003, ADR 0005).
///
/// # Errors
///
/// Returns an error if an object-store output's reported `object_key`
/// does not match the attempt-specific key Remote presigned for this Attempt
/// (ADR 0008's confused-deputy prevention), if an output reports a placement
/// Remote does not accept as an output, or if recording an output, deleting
/// inputs, reading the post-commit Generation snapshot, committing `tx`, or
/// publishing the state transition fails.
async fn record_accepted_result(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    attempt_id: AttemptId,
    generation_id: GenerationId,
    outputs: &[AttemptOutput],
    mut tx: sqlx::Transaction<'static, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let mut output_count = 0_usize;
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

        // Confused-deputy prevention (ADR 0008): this Attempt's object-store
        // key is the one Remote presigned and handed to its Worker; anything
        // else — including an empty key — would let a Worker Credential
        // redirect Remote's own S3 credentials at an arbitrary key, across
        // Tenants. Only object-store outputs carry a key at all.
        let object_key = match placement {
            ArtifactPlacement::ObjectStore => {
                let expected = leased_output_key(tenant_id, generation_id, attempt_id);
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
            }
            ArtifactPlacement::WorkerLocal => {
                if !output.object_key.is_empty() {
                    anyhow::bail!(
                        "malformed AttemptResult: worker_local output carries an object_key"
                    );
                }
                None
            }
            ArtifactPlacement::RemoteLocal => {
                anyhow::bail!(
                    "malformed AttemptResult: {} is not an output placement",
                    placement.as_str()
                );
            }
        };
        let delivery_token =
            (!output.delivery_token.is_empty()).then_some(output.delivery_token.as_str());
        let placement_worker = output_placement_worker(placement, worker_id);

        crate::db::artifacts::record_output(
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
        output_count += 1;
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
    crate::expiry::release_deleted_inputs(&state.artifacts, tenant_id, deleted_inputs).await;
    for _ in 0..output_count {
        state.events.publish(generation_id, GenerationEvent::Output);
    }
    state.scheduler.wake_tenant(tenant_id);
    Ok(())
}

async fn try_handle_attempt_result(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    credential_hash: &[u8],
    result: AttemptResult,
    outbound: &mpsc::Sender<RemoteMessage>,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&result.attempt_id, AttemptId::from_uuid)?;
    let now = state.db.now().await?;

    // Bound every output manifest against the Tenant's configured size
    // limit before opening the result-commit transaction below.
    let settings = load_tenant_settings(state, tenant_id).await?;
    let worker = AuthenticatedWorker {
        tenant_id,
        worker_id,
        credential_hash,
    };
    if reject_oversized_outputs(state, &worker, attempt_id, &result.outputs, &settings, now).await?
    {
        return Ok(());
    }

    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, credential_hash).await? {
        return Ok(());
    }
    let outcome =
        crate::db::generations::accept_result(&mut tx, tenant_id, attempt_id, worker_id, now)
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
        ResultReply::AcceptedReplay => {
            let accepted_tokens: HashSet<String> = sqlx::query_scalar(
                "SELECT delivery_token FROM artifacts \
                 WHERE tenant_id = $1 AND attempt_id = $2 AND direction = 'output' \
                    AND delivery_token IS NOT NULL",
            )
            .bind(tenant_id.as_uuid())
            .bind(attempt_id.as_uuid())
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .collect();
            tx.commit().await?;
            discard_worker_outputs(
                outbound,
                &result.outputs,
                &accepted_tokens,
                "result replay included unreferenced outputs",
            )
            .await;
        }
        ResultReply::Discard => {
            tx.commit().await?;
            discard_worker_outputs(
                outbound,
                &result.outputs,
                &HashSet::new(),
                "result rejected: stale lease, duplicate, or terminal generation",
            )
            .await;
        }
    }
    Ok(())
}

async fn discard_worker_outputs(
    outbound: &mpsc::Sender<RemoteMessage>,
    outputs: &[AttemptOutput],
    accepted_tokens: &HashSet<String>,
    reason: &str,
) {
    for output in outputs {
        if output.delivery_token.is_empty() || accepted_tokens.contains(&output.delivery_token) {
            continue;
        }
        let discard = RemoteMessage {
            message: DiscardOutput {
                artifact_id: String::new(),
                delivery_token: output.delivery_token.clone(),
                reason: reason.to_owned(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let _ = outbound.send(discard).await;
    }
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
    worker: &AuthenticatedWorker<'_>,
    attempt_id: AttemptId,
    failure: crate::db::attempts::FailureDetails<'_>,
    now: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    let mut tx = state.db.begin_tenant(worker.tenant_id).await?;
    if !lock_current_worker_credential(
        &mut tx,
        worker.tenant_id,
        worker.worker_id,
        worker.credential_hash,
    )
    .await?
    {
        return Ok(());
    }
    let outcome = crate::db::attempts::record_failure_for_worker(
        &mut tx,
        worker.tenant_id,
        attempt_id,
        worker.worker_id,
        failure,
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
        crate::db::artifacts::delete_inputs_for_generation(&mut tx, worker.tenant_id, generation_id)
            .await?
    } else {
        Vec::new()
    };
    let snapshot = generation_snapshot(&mut tx, worker.tenant_id, generation_id).await?;
    tx.commit().await?;

    if let Some(snapshot) = snapshot {
        publish_state_event(state, worker.tenant_id, generation_id, &snapshot).await?;
    }
    crate::expiry::release_deleted_inputs(&state.artifacts, worker.tenant_id, deleted_inputs).await;
    state.scheduler.wake_tenant(worker.tenant_id);
    Ok(())
}

async fn try_handle_attempt_failure(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    credential_hash: &[u8],
    failure: AttemptFailure,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&failure.attempt_id, AttemptId::from_uuid)?;
    let Some(failure) = failure.failure.into_option() else {
        return Ok(());
    };
    let failure = crate::db::attempts::FailureDetails {
        kind: domain_failure_kind(failure.kind).unwrap_or(FailureKind::Internal),
        message: &failure.message,
        worker_retry_hint: failure.worker_retry_hint,
    };
    let now = state.db.now().await?;
    let worker = AuthenticatedWorker {
        tenant_id,
        worker_id,
        credential_hash,
    };
    settle_attempt_failure(state, &worker, attempt_id, failure, now).await
}

async fn try_handle_cancel_acknowledged(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    credential_hash: &[u8],
    ack: CancelAcknowledged,
) -> anyhow::Result<()> {
    let attempt_id = parse_uuid_id(&ack.attempt_id, AttemptId::from_uuid)?;
    let now = state.db.now().await?;

    let mut tx = state.db.begin_tenant(tenant_id).await?;
    if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, credential_hash).await? {
        return Ok(());
    }
    let generation_id =
        crate::db::attempts::acknowledge_cancel(&mut tx, tenant_id, attempt_id, worker_id, now)
            .await?;
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
    crate::expiry::release_deleted_inputs(&state.artifacts, tenant_id, deleted_inputs).await;
    Ok(())
}

async fn try_handle_lease_rejected(
    state: &AppState,
    tenant_id: TenantId,
    worker_id: WorkerId,
    credential_hash: &[u8],
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
    if !lock_current_worker_credential(&mut tx, tenant_id, worker_id, credential_hash).await? {
        return Ok(());
    }
    let generation_id = crate::db::attempts::reject_lease(
        &mut tx,
        tenant_id,
        attempt_id,
        worker_id,
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
    use buffa::MessageField;
    use gpq_proto::gpq::v1::{
        LoraSelection as ProtoLoraSelection, ModelAsset as ProtoModelAsset,
        SamplingPreset as ProtoSamplingPreset,
    };
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
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_REMOTE_LOCAL
            )),
            Some(ArtifactPlacement::RemoteLocal)
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

    fn proto_manifest() -> ProtoModelManifest {
        let asset = |role: ProtoModelAssetRole, bytes: &[u8]| ProtoModelAsset {
            role: EnumValue::Known(role),
            sha256: ContentHash::digest(bytes).to_hex(),
            ..Default::default()
        };
        ProtoModelManifest {
            family: EnumValue::Known(ProtoModelFamily::MODEL_FAMILY_QWEN_IMAGE21),
            assets: vec![
                asset(
                    ProtoModelAssetRole::MODEL_ASSET_ROLE_DIFFUSION,
                    b"diffusion",
                ),
                asset(ProtoModelAssetRole::MODEL_ASSET_ROLE_TEXT_ENCODER, b"text"),
                asset(
                    ProtoModelAssetRole::MODEL_ASSET_ROLE_VISION_ENCODER,
                    b"vision",
                ),
                asset(ProtoModelAssetRole::MODEL_ASSET_ROLE_VAE, b"vae"),
            ],
            default_loras: vec![ProtoLoraSelection {
                sha256: ContentHash::digest(b"lora").to_hex(),
                scale: 0.5,
                ..Default::default()
            }],
            sampling: MessageField::some(ProtoSamplingPreset {
                steps: 40,
                cfg_scale: 6.0,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn pool_upsert_takes_total_slots_and_ignores_worker_busy_flags() {
        let pool = PoolAdvertisement {
            pool_id: "gpu-0".to_owned(),
            accelerator: "metal".to_owned(),
            ready: true,
            slots: vec![slot(false), slot(true), slot(false)],
            ..Default::default()
        };
        let Some((upsert, models)) = pool_upsert_from_proto(&pool) else {
            panic!("expected a valid pool upsert");
        };
        // Total capacity is Worker-owned, so it is carried over verbatim.
        assert_eq!(upsert.total_slots, 3);
        assert_eq!(upsert.accelerator, "metal");
        assert_eq!(models, Vec::<(ContentHash, ModelManifest)>::new());
        // Free slots are Remote's to compute from `claimed_slots`: the two
        // idle flags in this report must not reach the upsert at all.
    }

    #[test]
    fn pool_upsert_is_none_for_unset_accelerator() {
        let pool = PoolAdvertisement {
            pool_id: "gpu-0".to_owned(),
            ..Default::default()
        };
        assert!(pool_upsert_from_proto(&pool).is_none());
    }

    #[test]
    fn pool_upsert_preserves_accelerator_memory_presence() {
        let absent = PoolAdvertisement {
            pool_id: "gpu-0".to_owned(),
            accelerator: "metal".to_owned(),
            accelerator_memory_bytes: None,
            ..Default::default()
        };
        let present_zero = PoolAdvertisement {
            accelerator_memory_bytes: Some(0),
            ..absent.clone()
        };
        let Some((absent_upsert, _)) = pool_upsert_from_proto(&absent) else {
            panic!("expected a valid pool upsert");
        };
        let Some((zero_upsert, _)) = pool_upsert_from_proto(&present_zero) else {
            panic!("expected a valid pool upsert");
        };
        assert_eq!(absent_upsert.accelerator_memory_bytes, None);
        assert_eq!(zero_upsert.accelerator_memory_bytes, Some(0));
    }

    #[test]
    fn advertised_model_is_kept_when_its_manifest_hashes_to_its_version() {
        let proto = proto_manifest();
        let Some(manifest) = model_manifest_from_proto(&proto) else {
            panic!("a well-formed manifest must convert");
        };
        let Ok(version) = manifest.canonical_hash() else {
            panic!("a valid manifest must hash");
        };
        let advertisement = ModelAdvertisement {
            version_sha256: version.to_hex(),
            manifest: MessageField::some(proto),
            ..Default::default()
        };
        let verified = verified_models(&[advertisement]);
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].0, version);
    }

    #[test]
    fn advertised_model_whose_manifest_does_not_hash_to_its_version_is_dropped() {
        // ADR 0012: an advertised version must be the manifest's own hash;
        // a mismatch must never reach the catalog or a Pool's model set.
        let advertisement = ModelAdvertisement {
            version_sha256: ContentHash::digest(b"some other manifest").to_hex(),
            manifest: MessageField::some(proto_manifest()),
            ..Default::default()
        };
        assert_eq!(
            verified_models(&[advertisement]),
            Vec::<(ContentHash, ModelManifest)>::new()
        );
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
    fn replay_of_the_accepted_attempt_preserves_its_outputs() {
        assert!(matches!(
            classify_result_outcome(crate::db::generations::AcceptOutcome::AcceptedReplay),
            ResultReply::AcceptedReplay
        ));
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
        // ADR 0008: object-store and remote-local outputs are addressed by
        // key or by the Remote's own row, not by owning Worker.
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
            output_placement_worker(ArtifactPlacement::RemoteLocal, worker_id),
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
}
