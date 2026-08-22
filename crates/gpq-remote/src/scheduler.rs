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

use std::time::Duration as StdDuration;

use buffa::MessageField;
use chrono::{DateTime, Utc};
use gpq_domain::{
    Candidate, ContentHash, DevicePoolId, ExecutionTarget, GenerationId, MediaKind, SlotCapability,
    SlotContext, TenantId, WorkerId, lease_expiry_from, select_batch,
};
use gpq_proto::gpq::v1 as pb;
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
    let (capabilities, settings, candidates) = {
        let mut tx = state.db.begin_tenant(tenant).await?;
        let capabilities = crate::db::workers::pool_capabilities(&mut tx, tenant).await?;
        let settings = crate::db::tenants::settings(&mut tx, tenant)
            .await?
            .unwrap_or_default();
        let candidates =
            crate::db::generations::queued_candidates(&mut tx, tenant, CANDIDATE_FETCH_LIMIT)
                .await?;
        tx.commit().await?;
        (capabilities, settings, candidates)
    };

    if candidates.is_empty() {
        return Ok(());
    }

    for assignment in plan_assignments(&capabilities, &candidates, settings, now, |worker_id| {
        state.workers.is_online(worker_id)
    }) {
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
        // Never over-assign, never preempt (ADR 0002): `select_batch` only
        // ever returns queued work, bounded by this Pool's free Slots.
        for candidate in select_batch(&context, candidates) {
            assignments.push(SlotAssignment {
                worker_id: *worker_id,
                pool_id: capability.pool_id,
                pool_key,
                capability,
                candidate,
            });
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

/// The single canonical object-store key for a Generation's output
/// Artifact (ADR 0008).
///
/// Computed once here so this scheduler (which presigns the Worker's
/// upload URL) and `session::try_handle_attempt_result` (which must reject
/// any other key an `AttemptResult` reports back) can never drift — a
/// mismatch would otherwise let a Worker Credential read or delete
/// arbitrary keys in the shared bucket via Remote's own S3 credentials
/// (confused deputy).
#[must_use]
pub(crate) fn leased_output_key(tenant_id: TenantId, generation_id: GenerationId) -> String {
    format!("{tenant_id}/{generation_id}/output")
}

async fn build_lease_assignment(
    state: &AppState,
    conn: &mut PgConnection,
    tenant: TenantId,
    pool_key: &str,
    attempt: &AttemptRow,
    generation: &GenerationRow,
) -> anyhow::Result<LeaseAssignment> {
    let target = generation
        .target()
        .map_err(|error| anyhow::anyhow!("generation has an unparseable target: {error}"))?;
    let modality = generation
        .modality()
        .map_err(|error| anyhow::anyhow!("generation has an unparseable modality: {error}"))?;

    // A llama.cpp lease carries no Workflow: the graph and manifest fields must
    // stay unset rather than default-constructed, or the Worker would try to
    // revalidate an empty manifest (ADR 0007 keeps the two targets distinct).
    let resolved_workflow = match target {
        ExecutionTarget::Model { .. } => None,
        ExecutionTarget::Workflow { version } => {
            let Some(resolved) =
                crate::db::catalog::get_workflow_version_row(conn, tenant, version).await?
            else {
                anyhow::bail!(
                    "workflow version {version} pinned by generation {} is no longer registered",
                    generation.id
                );
            };
            Some((resolved.graph, resolved.manifest, resolved.limits))
        }
    };
    let (model_sha256, workflow_sha256, workflow_graph, workflow_manifest) =
        lease_target_fields(target, resolved_workflow)?;

    let parameters: buffa_types::google::protobuf::Struct =
        serde_json::from_value(generation.parameters.clone()).map_err(|error| {
            anyhow::anyhow!("generation parameters is not a JSON object: {error}")
        })?;

    let mut inputs = Vec::new();
    for row in crate::db::artifacts::list_inputs(conn, tenant, generation.generation_id()).await? {
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
            let key = leased_output_key(tenant, generation.generation_id());
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
        modality: proto_modality(modality).into(),
        model_sha256,
        workflow_sha256,
        workflow_graph: workflow_graph.map_or_else(MessageField::none, MessageField::some),
        workflow_manifest: workflow_manifest.map_or_else(MessageField::none, MessageField::some),
        parameters: parameters.into(),
        inputs,
        output_placement: proto_artifact_placement(output_placement).into(),
        output_upload_url,
        output_object_key,
        seed,
        execution_timeout: execution_timeout.into(),
        lease_expires_at: buffa_types::google::protobuf::Timestamp::from(attempt.lease_expires_at)
            .into(),
        stream_tokens: generation.stream_tokens,
        ..Default::default()
    })
}

/// Which target-specific fields a `LeaseAssignment` carries, by
/// [`ExecutionTarget`] (ADR 0007 keeps a Model and a Workflow lease
/// distinct): a Model lease leaves the Workflow graph and manifest unset
/// rather than default-constructed, so the Worker never tries to revalidate
/// an empty manifest; a Workflow lease carries its graph, its manifest, and
/// the pinned Version hash. `resolved_workflow` is `None` only for a Model
/// target; the caller has already fetched it for a Workflow target before
/// calling this, so that combination is a defensive internal-invariant
/// error rather than a normal outcome.
fn lease_target_fields(
    target: ExecutionTarget,
    resolved_workflow: Option<(
        serde_json::Value,
        gpq_domain::WorkflowManifest,
        gpq_domain::ExecutionLimits,
    )>,
) -> anyhow::Result<(
    String,
    String,
    Option<buffa_types::google::protobuf::Struct>,
    Option<pb::WorkflowManifest>,
)> {
    match target {
        ExecutionTarget::Model { version } => Ok((version.to_hex(), String::new(), None, None)),
        ExecutionTarget::Workflow { version } => {
            let Some((graph_value, manifest, limits)) = resolved_workflow else {
                anyhow::bail!(
                    "workflow version {version} pinned by a generation has no resolved workflow version"
                );
            };
            let graph = serde_json::from_value(graph_value)
                .map_err(|error| anyhow::anyhow!("workflow graph is not a JSON object: {error}"))?;
            Ok((
                String::new(),
                version.to_hex(),
                Some(graph),
                Some(proto_workflow_manifest(&manifest, limits)),
            ))
        }
    }
}

fn proto_duration_from_micros(microseconds: i64) -> buffa_types::google::protobuf::Duration {
    let micros = u64::try_from(microseconds).unwrap_or(0);
    let delta = chrono::TimeDelta::from_std(StdDuration::from_micros(micros)).unwrap_or_default();
    buffa_types::google::protobuf::Duration::from(delta)
}

fn proto_modality(modality: gpq_domain::Modality) -> pb::Modality {
    match modality {
        gpq_domain::Modality::Llm => pb::Modality::Llm,
        gpq_domain::Modality::Image => pb::Modality::Image,
        gpq_domain::Modality::Video => pb::Modality::Video,
        gpq_domain::Modality::Music => pb::Modality::Music,
    }
}

fn proto_artifact_placement(placement: gpq_domain::ArtifactPlacement) -> pb::ArtifactPlacement {
    match placement {
        gpq_domain::ArtifactPlacement::ObjectStore => pb::ArtifactPlacement::ObjectStore,
        gpq_domain::ArtifactPlacement::WorkerLocal => pb::ArtifactPlacement::WorkerLocal,
        gpq_domain::ArtifactPlacement::InlineRelay => pb::ArtifactPlacement::InlineRelay,
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

fn proto_workflow_manifest(
    manifest: &gpq_domain::WorkflowManifest,
    limits: gpq_domain::ExecutionLimits,
) -> pb::WorkflowManifest {
    pb::WorkflowManifest {
        output_node: manifest.output_node.clone(),
        output_name: manifest.output_name.clone(),
        artifact_kind: proto_media_kind(manifest.artifact_kind).into(),
        artifact_mime: manifest.artifact_mime.clone(),
        required_model_sha256: manifest
            .required_models
            .iter()
            .map(ContentHash::to_hex)
            .collect(),
        required_custom_nodes: manifest.required_custom_nodes.clone().into_iter().collect(),
        estimated_vram_bytes: limits.estimated_vram_bytes.unwrap_or(0),
        execution_timeout: limits
            .execution_timeout
            .map(|duration| {
                let delta = chrono::TimeDelta::from_std(duration).unwrap_or_default();
                buffa_types::google::protobuf::Duration::from(delta)
            })
            .unwrap_or_default()
            .into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modality_mapping_is_total_and_faithful() {
        assert_eq!(proto_modality(gpq_domain::Modality::Llm), pb::Modality::Llm);
        assert_eq!(
            proto_modality(gpq_domain::Modality::Image),
            pb::Modality::Image
        );
        assert_eq!(
            proto_modality(gpq_domain::Modality::Video),
            pb::Modality::Video
        );
        assert_eq!(
            proto_modality(gpq_domain::Modality::Music),
            pb::Modality::Music
        );
    }

    #[test]
    fn artifact_placement_mapping_is_total_and_faithful() {
        assert_eq!(
            proto_artifact_placement(gpq_domain::ArtifactPlacement::ObjectStore),
            pb::ArtifactPlacement::ObjectStore
        );
        assert_eq!(
            proto_artifact_placement(gpq_domain::ArtifactPlacement::WorkerLocal),
            pb::ArtifactPlacement::WorkerLocal
        );
        assert_eq!(
            proto_artifact_placement(gpq_domain::ArtifactPlacement::InlineRelay),
            pb::ArtifactPlacement::InlineRelay
        );
    }

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

    #[test]
    fn model_target_leaves_workflow_fields_unset() {
        // ADR 0007: a Model lease must never carry a default-constructed
        // Workflow graph/manifest that the Worker would try to revalidate.
        let version = ContentHash::from_bytes([7; 32]);
        let Ok((model_sha256, workflow_sha256, graph, manifest)) =
            lease_target_fields(ExecutionTarget::Model { version }, None)
        else {
            panic!("a Model target must resolve without a workflow lookup");
        };
        assert_eq!(model_sha256, version.to_hex());
        assert!(workflow_sha256.is_empty());
        assert!(graph.is_none());
        assert!(manifest.is_none());
    }

    #[test]
    fn workflow_target_carries_graph_manifest_and_pinned_hash() {
        // ADR 0007: a Workflow lease carries its graph, its manifest, and
        // the pinned Version hash — never left unset like a Model lease.
        let version = ContentHash::from_bytes([8; 32]);
        let workflow_manifest = gpq_domain::WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "IMAGE".to_owned(),
            artifact_kind: MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: Vec::new(),
            required_custom_nodes: std::collections::BTreeMap::new(),
        };
        let Ok((model_sha256, workflow_sha256, graph, manifest)) = lease_target_fields(
            ExecutionTarget::Workflow { version },
            Some((
                serde_json::json!({"1": {}}),
                workflow_manifest,
                gpq_domain::ExecutionLimits::default(),
            )),
        ) else {
            panic!("a resolved Workflow target must succeed");
        };
        assert!(model_sha256.is_empty());
        assert_eq!(workflow_sha256, version.to_hex());
        assert!(graph.is_some());
        assert!(manifest.is_some());
    }

    #[test]
    fn unresolved_workflow_target_is_rejected() {
        let version = ContentHash::from_bytes([9; 32]);
        assert!(lease_target_fields(ExecutionTarget::Workflow { version }, None).is_err());
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
            backend_kind: gpq_domain::BackendKind::LlamaCpp,
            backend_version: "1.0".to_owned(),
            model_versions: std::collections::BTreeSet::from([version]),
            custom_nodes: std::collections::BTreeMap::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            incapable_versions: std::collections::BTreeSet::new(),
        }
    }

    fn sample_candidate(
        tenant: TenantId,
        version: ContentHash,
        created_at: DateTime<Utc>,
    ) -> Candidate {
        Candidate {
            generation_id: gpq_domain::GenerationId::new(),
            created_at,
            priority: gpq_domain::Priority::DEFAULT,
            requirement: gpq_domain::Requirement::for_model(tenant, version, None),
        }
    }

    #[test]
    fn pool_with_no_free_slots_is_skipped() {
        // ADR 0002: an unready (zero-free-slot) Pool never receives work.
        let tenant = TenantId::new();
        let worker_id = WorkerId::new();
        let pool_id = DevicePoolId::new();
        let version = ContentHash::from_bytes([1; 32]);
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
        let version = ContentHash::from_bytes([2; 32]);
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
        let version = ContentHash::from_bytes([3; 32]);
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
            gpq_domain::TenantSettings::default(),
            now,
            |_| true,
        );
        assert_eq!(assignments.len(), 1);
    }
}
