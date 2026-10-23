//! GPU-utilization-first scheduling loop (ADR 0002, ADR 0005, ADR 0013).
//!
//! `PostgreSQL` rows remain the only queue truth: a `LISTEN`/`NOTIFY` wakeup on
//! `gpq_queue` (or an explicit [`SchedulerHandle::wake_tenant`]) triggers a
//! scheduling pass for one Tenant, and a one-second fallback tick sweeps every
//! known Tenant because notifications are not durable (ADR 0013). Each Attempt
//! assignment is its own `FOR UPDATE SKIP LOCKED`-backed transaction that
//! claims a free Execution Slot, creates the Attempt using
//! [`gpq_domain::select_batch`], and commits only once the lease has actually
//! been sent to the Worker — a failed send rolls the whole assignment back so
//! no lease leaks (ADR 0002).
//!
//! **Chosen invariant** (documented here because nothing else pins it down):
//! a Generation becomes `Running` optimistically, in the same transaction that
//! creates its first Attempt, without waiting for the Worker's `AttemptRunning`
//! confirmation. The *Attempt* itself starts `Leased` and only becomes
//! `Running` when the Worker confirms it (`db::attempts::mark_running`, called
//! from the Worker session), which is also when ADR 0003's execution-timeout
//! clock starts.

use std::collections::{HashMap, HashSet};
use std::time::Duration as StdDuration;

use buffa::MessageField;
use chrono::{DateTime, Utc};
use gpq_domain::{
    AttemptId, Candidate, DevicePoolId, GenerationId, MediaKind, SlotCapability, SlotContext,
    TenantId, WorkerId, lease_expiry_from, select_batch,
};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1::__buffa::oneof::lease_assignment;
use gpq_proto::gpq::worker::v1::{LeaseAssignment, LeaseInput, RemoteMessage};
use sqlx::PgConnection;
use sqlx::postgres::{PgListener, PgNotification};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::db::attempts::{AttemptRow, CreateAttemptError};
use crate::db::generations::GenerationRow;
use crate::state::AppState;

/// The `LISTEN/NOTIFY` channel the migration's trigger publishes to.
const QUEUE_CHANNEL: &str = "gpq_queue";

/// Fallback poll cadence: notifications are not durable (ADR 0013).
const FALLBACK_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);

/// Upper bound on queued Generations considered in one scheduling pass.
const CANDIDATE_FETCH_LIMIT: i64 = 500;

/// A scheduling wakeup.
#[derive(Debug, Clone, Copy)]
enum Wake {
    /// Reschedule one Tenant.
    Tenant(TenantId),
    /// A Worker's capability or occupancy changed; its Tenant is unknown to
    /// the scheduler, so this falls back to a full sweep (rare in practice —
    /// capability reports are far less frequent than new Generations).
    Worker(WorkerId),
}

/// A cheap, cloneable channel into the scheduling loop.
#[derive(Clone)]
pub struct SchedulerHandle {
    sender: mpsc::Sender<Wake>,
}

impl SchedulerHandle {
    /// Wakes the scheduler for one Tenant, e.g. right after admission queues
    /// a new Generation.
    pub fn wake_tenant(&self, tenant: TenantId) {
        let _ = self.sender.try_send(Wake::Tenant(tenant));
    }

    /// Wakes the scheduler because a Worker's capability or occupancy changed.
    pub fn wake_worker(&self, worker: WorkerId) {
        let _ = self.sender.try_send(Wake::Worker(worker));
    }

    /// A handle whose channel has no live receiver, so every wake is a silent
    /// no-op. Exists only to populate a bootstrap `AppState` passed into
    /// [`spawn`] before the real handle it returns is available — nothing
    /// inside the scheduling loop itself reads `AppState::scheduler`.
    #[must_use]
    pub fn inert() -> Self {
        let (sender, _receiver) = mpsc::channel(1);
        Self { sender }
    }
}

/// Starts the scheduling loop as a background task.
#[must_use]
pub fn spawn(state: AppState) -> (SchedulerHandle, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(1024);
    let handle = SchedulerHandle { sender };
    let join = tokio::spawn(run(state, receiver));
    (handle, join)
}

async fn connect_listener(state: &AppState) -> Option<PgListener> {
    let mut listener = match PgListener::connect_with(state.db.pool()).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::warn!(%error, "scheduler could not connect its LISTEN/NOTIFY session");
            return None;
        }
    };
    if let Err(error) = listener.listen(QUEUE_CHANNEL).await {
        tracing::warn!(%error, "scheduler could not LISTEN on {QUEUE_CHANNEL}");
        return None;
    }
    Some(listener)
}

/// Awaits the next notification, or never resolves while `listener` is `None`
/// so it can sit in a `tokio::select!` branch unconditionally.
async fn recv_or_pending(listener: &mut Option<PgListener>) -> Result<PgNotification, sqlx::Error> {
    match listener {
        Some(listener) => listener.recv().await,
        None => std::future::pending().await,
    }
}

async fn run(state: AppState, mut wake_rx: mpsc::Receiver<Wake>) {
    let mut listener = connect_listener(&state).await;
    if listener.is_none() {
        tracing::warn!("scheduler is relying on the {FALLBACK_POLL_INTERVAL:?} fallback tick only");
    }

    let mut tick = tokio::time::interval(FALLBACK_POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            notification = recv_or_pending(&mut listener) => {
                match notification {
                    Ok(notification) => {
                        match notification.payload().parse::<uuid::Uuid>() {
                            Ok(tenant_uuid) => schedule_tenant(&state, TenantId::from_uuid(tenant_uuid)).await,
                            Err(error) => tracing::warn!(%error, payload = notification.payload(), "malformed gpq_queue notification payload"),
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "lost the LISTEN/NOTIFY connection; falling back to polling until it reconnects");
                        listener = None;
                    }
                }
            }
            _ = tick.tick() => {
                for tenant in known_tenants(&state).await {
                    schedule_tenant(&state, tenant).await;
                }
                if listener.is_none() {
                    listener = connect_listener(&state).await;
                }
            }
            wake = wake_rx.recv() => {
                match wake {
                    Some(Wake::Tenant(tenant)) => schedule_tenant(&state, tenant).await,
                    Some(Wake::Worker(worker)) => {
                        tracing::debug!(%worker, "worker-scoped wake triggered a full tenant sweep");
                        for tenant in known_tenants(&state).await {
                            schedule_tenant(&state, tenant).await;
                        }
                    }
                    None => return,
                }
            }
        }
    }
}

/// Lists every Tenant, using one administrative (no-tenant-GUC) transaction —
/// the one cross-tenant read ADR 0011 reserves for administration — purely to
/// drive the per-tenant sweep below; every substantive scheduling decision
/// still runs tenant-scoped (`schedule_tenant` -> `begin_tenant`).
pub(crate) async fn known_tenants(state: &AppState) -> Vec<TenantId> {
    let mut tx = match state.db.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            tracing::error!(%error, "scheduler could not open an administrative transaction");
            return Vec::new();
        }
    };
    match crate::db::tenants::list_ids(&mut tx).await {
        Ok(ids) => ids,
        Err(error) => {
            tracing::error!(%error, "scheduler could not list tenants");
            Vec::new()
        }
    }
}

async fn schedule_tenant(state: &AppState, tenant: TenantId) {
    if let Err(error) = try_schedule_tenant(state, tenant).await {
        tracing::error!(%tenant, %error, "scheduling pass failed");
    }
}

async fn try_schedule_tenant(state: &AppState, tenant: TenantId) -> anyhow::Result<()> {
    // Skip the round trip entirely when nobody could execute anything anyway.
    if state.workers.online_workers(tenant).is_empty() {
        return Ok(());
    }

    let now = state.db.now().await?;
    let (capabilities, settings, candidates, exclusions) = {
        let mut tx = state.db.begin_tenant(tenant).await?;
        let capabilities = crate::db::workers::pool_capabilities(&mut tx, tenant).await?;
        let settings = crate::db::tenants::settings(&mut tx, tenant)
            .await?
            .unwrap_or_default();
        let candidates =
            crate::db::generations::queued_candidates(&mut tx, tenant, CANDIDATE_FETCH_LIMIT)
                .await?;
        let generation_ids: Vec<GenerationId> = candidates
            .iter()
            .map(|candidate| candidate.generation_id)
            .collect();
        let exclusions = crate::db::native_exclusions::excluded_pools_for_generations(
            &mut tx,
            tenant,
            &generation_ids,
        )
        .await?;
        tx.commit().await?;
        (capabilities, settings, candidates, exclusions)
    };

    if candidates.is_empty() {
        return Ok(());
    }

    for assignment in plan_assignments(
        &capabilities,
        &candidates,
        &exclusions,
        settings,
        now,
        |worker_id| state.workers.is_online(worker_id),
    ) {
        assign(state, tenant, &assignment, now).await;
    }
    Ok(())
}

/// One scheduling decision: a free Slot on a specific Worker's Device Pool
/// matched against a queued candidate Generation (ADR 0002, ADR 0005).
/// Grouped so [`assign`]/[`try_assign`] stay under clippy's argument-count
/// ceiling.
struct SlotAssignment<'a> {
    worker_id: WorkerId,
    pool_id: DevicePoolId,
    /// The Pool's own identity as configured on the Worker host, echoed back
    /// in the `LeaseAssignment` so the Worker knows which of its Pools to
    /// execute on (ADR 0005) — distinct from `pool_id`, Remote's surrogate key.
    pool_key: &'a str,
    capability: &'a SlotCapability,
    candidate: &'a Candidate,
}

/// One scheduling pass's decisions: which Pool/candidate pairs to attempt to
/// lease this tick (ADR 0002, ADR 0005). A Pool with no free Slot, or whose
/// Worker has no live control session, contributes nothing even if its last
/// reported capability would otherwise match; every remaining Pool is bounded
/// to its own advertised free-Slot count by `select_batch`, so one pass never
/// assigns more Attempts than a Pool can actually run. Pure so this
/// bookkeeping is unit-testable without a database or a live Worker session.
fn plan_assignments<'a>(
    capabilities: &'a [(SlotCapability, u32, WorkerId, String)],
    candidates: &'a [Candidate],
    exclusions: &HashMap<GenerationId, HashSet<DevicePoolId>>,
    settings: gpq_domain::TenantSettings,
    now: DateTime<Utc>,
    is_online: impl Fn(WorkerId) -> bool,
) -> Vec<SlotAssignment<'a>> {
    let mut assignments = Vec::new();
    for (capability, free_slots, worker_id, pool_key) in capabilities {
        if *free_slots == 0 || !is_online(*worker_id) {
            continue;
        }
        let context = SlotContext {
            capability,
            free_slots: *free_slots,
            settings,
            now,
        };
        let pool_rejected = |candidate: &Candidate| {
            exclusions
                .get(&candidate.generation_id)
                .is_some_and(|pools| pools.contains(&capability.pool_id))
        };
        let mut push_assignment = |candidate: &'a Candidate| {
            assignments.push(SlotAssignment {
                worker_id: *worker_id,
                pool_id: capability.pool_id,
                pool_key,
                capability,
                candidate,
            });
        };
        if candidates.iter().any(&pool_rejected) {
            // `select_batch` takes a `&[Candidate]`; make the filtered slice
            // only when this Pool has Generation-specific exclusions, then map
            // selected identities back to the canonical scheduler snapshot.
            let eligible: Vec<Candidate> = candidates
                .iter()
                .filter(|candidate| !pool_rejected(candidate))
                .cloned()
                .collect();
            for selected in select_batch(&context, &eligible) {
                if let Some(candidate) = candidates
                    .iter()
                    .find(|candidate| candidate.generation_id == selected.generation_id)
                {
                    push_assignment(candidate);
                }
            }
        } else {
            for candidate in select_batch(&context, candidates) {
                push_assignment(candidate);
            }
        }
    }
    assignments
}

async fn assign(
    state: &AppState,
    tenant: TenantId,
    assignment: &SlotAssignment<'_>,
    now: DateTime<Utc>,
) {
    if let Err(error) = try_assign(state, tenant, assignment, now).await {
        tracing::warn!(
            %tenant, generation = %assignment.candidate.generation_id, %error,
            "failed to assign a lease"
        );
    }
}

/// Claims one free Slot, creates the Attempt, and sends the lease — one
/// transaction, committed only if the send to the Worker actually succeeds.
async fn try_assign(
    state: &AppState,
    tenant: TenantId,
    assignment: &SlotAssignment<'_>,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let mut tx = state.db.begin_tenant(tenant).await?;
    let excluded = crate::db::native_exclusions::excluded_pools(
        &mut tx,
        tenant,
        assignment.candidate.generation_id,
    )
    .await?;
    if excluded.contains(&assignment.pool_id) {
        // The planning snapshot may predate a committed rejection or OOM.
        return Ok(());
    }

    if !crate::db::workers::claim_slot(&mut tx, tenant, assignment.pool_id).await? {
        // Raced with another pass, or the Worker's last capability report
        // already shrank this Pool's free_slots; try again next pass.
        return Ok(());
    }

    let slot_key = assignment.capability.slot_id.to_string();
    let attempt = match crate::db::attempts::create(
        &mut tx,
        tenant,
        assignment.candidate.generation_id,
        assignment.worker_id,
        assignment.pool_id,
        &slot_key,
        lease_expiry_from(now),
    )
    .await
    {
        Ok(attempt) => attempt,
        Err(CreateAttemptError::NotQueued | CreateAttemptError::MaxAttemptsReached { .. }) => {
            return Ok(());
        }
        Err(CreateAttemptError::Database(error)) => return Err(error.into()),
    };

    crate::db::events::append_attempt_created(
        &mut tx,
        tenant,
        assignment.candidate.generation_id,
        attempt.attempt_id(),
        attempt.attempt_number,
        assignment.worker_id,
        assignment.pool_id,
    )
    .await?;

    crate::db::generations::mark_running(&mut tx, tenant, assignment.candidate.generation_id, now)
        .await?;

    let Some(generation) =
        crate::db::generations::get(&mut tx, tenant, assignment.candidate.generation_id).await?
    else {
        return Ok(());
    };

    let lease = build_lease_assignment(
        state,
        &mut tx,
        tenant,
        assignment.pool_key,
        &attempt,
        &generation,
    )
    .await?;
    let sent = state
        .workers
        .send(
            assignment.worker_id,
            RemoteMessage {
                message: Some(lease.into()),
                ..Default::default()
            },
        )
        .is_delivered();

    if sent {
        tx.commit().await?;
    }
    // else: `tx` drops here without committing, rolling back the claimed
    // Slot, the new Attempt, and the Generation's `Running` transition so no
    // lease leaks (ADR 0002).
    Ok(())
}

/// The canonical object-store key for one Attempt's output Artifact (ADR 0008).
///
/// The attempt component prevents an older presigned PUT from overwriting a
/// later retry's accepted object.
#[must_use]
pub(crate) fn leased_output_key(
    tenant_id: TenantId,
    generation_id: GenerationId,
    attempt_id: AttemptId,
) -> String {
    format!("{tenant_id}/{generation_id}/attempts/{attempt_id}/output")
}

async fn build_lease_assignment(
    state: &AppState,
    conn: &mut PgConnection,
    tenant: TenantId,
    pool_key: &str,
    attempt: &AttemptRow,
    generation: &GenerationRow,
) -> anyhow::Result<LeaseAssignment> {
    let version = generation
        .version()
        .map_err(|error| anyhow::anyhow!("generation has an unparseable version: {error}"))?;
    let Some(model) = crate::db::catalog::get_model_version(conn, tenant, version).await? else {
        anyhow::bail!(
            "model version {version} pinned by generation {} is no longer registered",
            generation.generation_id()
        );
    };
    let parameters: gpq_domain::GenerationParameters =
        serde_json::from_value(generation.parameters.clone()).map_err(|error| {
            anyhow::anyhow!("generation parameters are not typed native parameters: {error}")
        })?;
    let parameters = match parameters {
        gpq_domain::GenerationParameters::Image(image) => {
            lease_assignment::GenerationParameters::ImageParameters(Box::new(
                proto_image_parameters(&image),
            ))
        }
        gpq_domain::GenerationParameters::Video(video) => {
            lease_assignment::GenerationParameters::VideoParameters(Box::new(
                proto_video_parameters(&video),
            ))
        }
    };

    let mut inputs = Vec::new();
    for row in crate::db::artifacts::list_inputs(conn, tenant, generation.generation_id()).await? {
        // Only object-store inputs are fetched by presigned URL; remote-local
        // bytes are pulled by the Worker through WorkerTransferService.
        let download_url = if row.placement == gpq_domain::ArtifactPlacement::ObjectStore {
            match &row.object_key {
                Some(key) => state.artifacts.presign_get(key).await?.0.to_string(),
                None => String::new(),
            }
        } else {
            String::new()
        };
        inputs.push(LeaseInput {
            artifact_id: row.id.to_string(),
            manifest: proto_artifact_manifest(&row.manifest).into(),
            placement: proto_artifact_placement(row.placement).into(),
            download_url,
            ..Default::default()
        });
    }

    let output_placement: gpq_domain::ArtifactPlacement = generation
        .output_placement
        .parse()
        .unwrap_or(gpq_domain::ArtifactPlacement::WorkerLocal);
    let (output_upload_url, output_object_key) =
        if output_placement == gpq_domain::ArtifactPlacement::ObjectStore {
            let key = leased_output_key(tenant, generation.generation_id(), attempt.attempt_id());
            let (url, _expires_at) = state.artifacts.presign_put_unsized(&key).await?;
            (url.to_string(), key)
        } else {
            (String::new(), String::new())
        };

    let seed = generation
        .seed
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0);
    let execution_timeout = proto_duration_from_micros(generation.execution_timeout.microseconds);

    Ok(LeaseAssignment {
        attempt_id: attempt.attempt_id().to_string(),
        generation_id: generation.generation_id().to_string(),
        pool_id: pool_key.to_owned(),
        slot_id: attempt.slot_key.clone(),
        model_sha256: version.to_hex(),
        model_manifest: MessageField::some(proto_model_manifest(&model.manifest)),
        generation_parameters: Some(parameters),
        inputs,
        output_placement: proto_artifact_placement(output_placement).into(),
        output_upload_url,
        output_object_key,
        seed,
        execution_timeout: execution_timeout.into(),
        lease_expires_at: buffa_types::google::protobuf::Timestamp::from(attempt.lease_expires_at)
            .into(),
        ..Default::default()
    })
}

fn proto_duration_from_micros(microseconds: i64) -> buffa_types::google::protobuf::Duration {
    let micros = u64::try_from(microseconds).unwrap_or(0);
    let delta = chrono::TimeDelta::from_std(StdDuration::from_micros(micros)).unwrap_or_default();
    buffa_types::google::protobuf::Duration::from(delta)
}

fn proto_artifact_placement(placement: gpq_domain::ArtifactPlacement) -> pb::ArtifactPlacement {
    match placement {
        gpq_domain::ArtifactPlacement::ObjectStore => pb::ArtifactPlacement::ObjectStore,
        gpq_domain::ArtifactPlacement::WorkerLocal => pb::ArtifactPlacement::WorkerLocal,
        gpq_domain::ArtifactPlacement::RemoteLocal => pb::ArtifactPlacement::RemoteLocal,
    }
}

fn proto_media_kind(kind: MediaKind) -> pb::MediaKind {
    match kind {
        MediaKind::Image => pb::MediaKind::Image,
        MediaKind::Video => pb::MediaKind::Video,
        MediaKind::Audio => pb::MediaKind::Audio,
        MediaKind::Text => pb::MediaKind::Text,
        MediaKind::Binary => pb::MediaKind::Binary,
    }
}

fn proto_artifact_manifest(manifest: &gpq_domain::ArtifactManifest) -> pb::ArtifactManifest {
    pb::ArtifactManifest {
        size_bytes: manifest.size_bytes,
        digest_sha256: manifest.digest.to_hex(),
        kind: proto_media_kind(manifest.kind).into(),
        mime_type: manifest.mime_type.clone(),
        ..Default::default()
    }
}

fn proto_model_family(family: gpq_domain::ModelFamily) -> pb::ModelFamily {
    match family {
        gpq_domain::ModelFamily::QwenImage21 => pb::ModelFamily::MODEL_FAMILY_QWEN_IMAGE21,
        gpq_domain::ModelFamily::MinimaxH3 => pb::ModelFamily::MODEL_FAMILY_MINIMAX_H3,
    }
}

fn proto_asset_role(role: gpq_domain::ModelAssetRole) -> pb::ModelAssetRole {
    match role {
        gpq_domain::ModelAssetRole::Diffusion => pb::ModelAssetRole::MODEL_ASSET_ROLE_DIFFUSION,
        gpq_domain::ModelAssetRole::TextEncoder => {
            pb::ModelAssetRole::MODEL_ASSET_ROLE_TEXT_ENCODER
        }
        gpq_domain::ModelAssetRole::VisionEncoder => {
            pb::ModelAssetRole::MODEL_ASSET_ROLE_VISION_ENCODER
        }
        gpq_domain::ModelAssetRole::Vae => pb::ModelAssetRole::MODEL_ASSET_ROLE_VAE,
        gpq_domain::ModelAssetRole::AudioVae => pb::ModelAssetRole::MODEL_ASSET_ROLE_AUDIO_VAE,
    }
}

fn proto_lora_selection(lora: &gpq_domain::LoraSelection) -> pb::LoraSelection {
    pb::LoraSelection {
        sha256: lora.sha256.to_hex(),
        scale: lora.scale,
        ..Default::default()
    }
}

/// The pinned Model Version's manifest as the Worker must receive it, so the
/// Worker can verify it hashes to `LeaseAssignment.model_sha256`.
fn proto_model_manifest(manifest: &gpq_domain::ModelManifest) -> pb::ModelManifest {
    pb::ModelManifest {
        family: proto_model_family(manifest.family).into(),
        assets: manifest
            .assets
            .iter()
            .map(|(role, sha256)| pb::ModelAsset {
                role: proto_asset_role(*role).into(),
                sha256: sha256.to_hex(),
                ..Default::default()
            })
            .collect(),
        default_loras: manifest
            .default_loras
            .iter()
            .map(proto_lora_selection)
            .collect(),
        sampling: MessageField::some(pb::SamplingPreset {
            steps: manifest.sampling.steps,
            cfg_scale: manifest.sampling.cfg_scale,
            sigmas: manifest.sampling.sigmas.clone(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn proto_image_parameters(params: &gpq_domain::ImageParameters) -> pb::ImageParameters {
    pb::ImageParameters {
        prompt: params.prompt.clone(),
        negative_prompt: params.negative_prompt.clone(),
        width: params.width,
        height: params.height,
        steps: params.steps,
        cfg_scale: params.cfg_scale,
        reference_artifact_ids: params.references.iter().map(ToString::to_string).collect(),
        loras: params.loras.iter().map(proto_lora_selection).collect(),
        // An absent sigma schedule means the model's own preset applies.
        sigmas: params.sigmas.clone().unwrap_or_default(),
        ..Default::default()
    }
}

fn proto_video_parameters(params: &gpq_domain::VideoParameters) -> pb::VideoParameters {
    pb::VideoParameters {
        prompt: params.prompt.clone(),
        negative_prompt: params.negative_prompt.clone(),
        width: params.width,
        height: params.height,
        frames: params.frames,
        first_frame_artifact_id: params.first_frame.as_ref().map(ToString::to_string),
        last_frame_artifact_id: params.last_frame.as_ref().map(ToString::to_string),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use gpq_domain::{
        ContentHash, ModelAssetRole, ModelFamily, ModelManifest, Requirement, ShapeLimits,
    };

    use super::*;

    #[test]
    fn duration_from_micros_round_trips() {
        let proto = proto_duration_from_micros(1_500_000);
        assert_eq!(proto.seconds, 1);
        assert_eq!(proto.nanos, 500_000_000);
    }

    #[test]
    fn duration_from_negative_micros_falls_back_to_zero() {
        let proto = proto_duration_from_micros(-1);
        assert_eq!(proto.seconds, 0);
        assert_eq!(proto.nanos, 0);
    }

    fn sample_manifest() -> ModelManifest {
        ModelManifest {
            family: ModelFamily::QwenImage21,
            assets: BTreeMap::from([
                (
                    ModelAssetRole::Diffusion,
                    ContentHash::digest(b"sample diffusion weights"),
                ),
                (
                    ModelAssetRole::TextEncoder,
                    ContentHash::digest(b"sample text encoder weights"),
                ),
                (
                    ModelAssetRole::VisionEncoder,
                    ContentHash::digest(b"sample vision encoder weights"),
                ),
                (
                    ModelAssetRole::Vae,
                    ContentHash::digest(b"sample vae weights"),
                ),
            ]),
            default_loras: Vec::new(),
            sampling: ModelFamily::QwenImage21.default_sampling_preset(),
        }
    }

    fn sample_version() -> ContentHash {
        sample_manifest()
            .canonical_hash()
            .unwrap_or_else(|err| panic!("sample manifest has canonical JSON: {err}"))
    }

    fn sample_capability(
        tenant: TenantId,
        worker_id: WorkerId,
        pool_id: DevicePoolId,
        version: ContentHash,
    ) -> SlotCapability {
        SlotCapability {
            tenant_id: tenant,
            worker_id,
            pool_id,
            slot_id: gpq_domain::SlotId::new(),
            accelerator: "metal".to_owned(),
            accelerator_version: "1.0".to_owned(),
            model_manifests: BTreeMap::from([(version, sample_manifest())]),
            image_loras: BTreeSet::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            shape_limits: ShapeLimits::default(),
        }
    }

    fn sample_candidate(
        tenant: TenantId,
        version: ContentHash,
        created_at: DateTime<Utc>,
    ) -> Candidate {
        let parameters = gpq_domain::GenerationParameters::Image(gpq_domain::ImageParameters {
            prompt: "a lighthouse".to_owned(),
            negative_prompt: String::new(),
            width: 64,
            height: 64,
            steps: None,
            cfg_scale: None,
            references: Vec::new(),
            loras: Vec::new(),
            sigmas: None,
        });
        let Ok(requirement) =
            Requirement::for_generation(tenant, version, &sample_manifest(), &parameters)
        else {
            panic!("a plain image request must yield a requirement");
        };
        Candidate {
            generation_id: GenerationId::new(),
            created_at,
            priority: gpq_domain::Priority::DEFAULT,
            requirement,
        }
    }

    #[test]
    fn pool_with_no_free_slots_is_skipped() {
        // ADR 0002: an unready (zero-free-slot) Pool never receives work.
        let tenant = TenantId::new();
        let worker_id = WorkerId::new();
        let pool_id = DevicePoolId::new();
        let version = sample_version();
        let capabilities = vec![(
            sample_capability(tenant, worker_id, pool_id, version),
            0_u32,
            worker_id,
            "gpu-0".to_owned(),
        )];
        let candidates = vec![sample_candidate(tenant, version, Utc::now())];

        let assignments = plan_assignments(
            &capabilities,
            &candidates,
            &HashMap::new(),
            gpq_domain::TenantSettings::default(),
            Utc::now(),
            |_| true,
        );
        assert!(assignments.is_empty());
    }

    #[test]
    fn pool_whose_worker_has_no_live_session_is_skipped() {
        // ADR 0005: a Worker with no live control session cannot receive a
        // lease no matter what its last-reported capability claimed.
        let tenant = TenantId::new();
        let worker_id = WorkerId::new();
        let pool_id = DevicePoolId::new();
        let version = sample_version();
        let capabilities = vec![(
            sample_capability(tenant, worker_id, pool_id, version),
            2_u32,
            worker_id,
            "gpu-0".to_owned(),
        )];
        let candidates = vec![sample_candidate(tenant, version, Utc::now())];

        let assignments = plan_assignments(
            &capabilities,
            &candidates,
            &HashMap::new(),
            gpq_domain::TenantSettings::default(),
            Utc::now(),
            |_| false,
        );
        assert!(assignments.is_empty());
    }

    #[test]
    fn eligible_pool_is_bounded_by_its_advertised_free_slots() {
        // ADR 0002: never assign more Attempts than a Pool's advertised
        // free Slots, even with more matching candidates queued.
        let tenant = TenantId::new();
        let worker_id = WorkerId::new();
        let pool_id = DevicePoolId::new();
        let version = sample_version();
        let capabilities = vec![(
            sample_capability(tenant, worker_id, pool_id, version),
            1_u32,
            worker_id,
            "gpu-0".to_owned(),
        )];
        let now = Utc::now();
        let candidates = vec![
            sample_candidate(tenant, version, now - chrono::Duration::seconds(2)),
            sample_candidate(tenant, version, now - chrono::Duration::seconds(1)),
        ];

        let assignments = plan_assignments(
            &capabilities,
            &candidates,
            &HashMap::new(),
            gpq_domain::TenantSettings::default(),
            now,
            |_| true,
        );
        assert_eq!(assignments.len(), 1);
    }

    #[test]
    fn oom_excluded_pool_does_not_receive_generation_again() {
        let tenant = TenantId::new();
        let worker_a = WorkerId::new();
        let worker_b = WorkerId::new();
        let pool_a = DevicePoolId::new();
        let pool_b = DevicePoolId::new();
        let version = sample_version();
        let capabilities = vec![
            (
                sample_capability(tenant, worker_a, pool_a, version),
                1,
                worker_a,
                "gpu-a".to_owned(),
            ),
            (
                sample_capability(tenant, worker_b, pool_b, version),
                1,
                worker_b,
                "gpu-b".to_owned(),
            ),
        ];
        let candidate = sample_candidate(tenant, version, Utc::now());
        let exclusions = HashMap::from([(candidate.generation_id, HashSet::from([pool_a]))]);

        let assignments = plan_assignments(
            &capabilities,
            std::slice::from_ref(&candidate),
            &exclusions,
            gpq_domain::TenantSettings::default(),
            Utc::now(),
            |_| true,
        );
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].pool_id, pool_b);
        assert_eq!(
            assignments[0].candidate.generation_id,
            candidate.generation_id
        );
    }

    #[test]
    fn retry_attempts_get_distinct_output_object_keys() {
        let tenant = TenantId::new();
        let generation = GenerationId::new();
        let first_attempt = AttemptId::new();
        let second_attempt = AttemptId::new();

        assert_ne!(
            leased_output_key(tenant, generation, first_attempt),
            leased_output_key(tenant, generation, second_attempt)
        );
    }
}
