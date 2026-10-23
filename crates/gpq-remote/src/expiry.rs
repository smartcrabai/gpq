//! Time-driven cleanup: lease expiry, execution-deadline enforcement, queue
//! starvation expiry, and Artifact expiry (ADR 0002, ADR 0003, ADR 0008,
//! ADR 0013).
//!
//! Every sweep here runs as its own short tenant-scoped transaction
//! (`Db::begin_tenant`, ADR 0011) so row-level security protects every
//! tenant-owned table it touches; only the roster of Tenant ids to sweep comes
//! from the one administrative read [`crate::scheduler::known_tenants`]
//! already uses.

use std::collections::HashSet;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use gpq_domain::{
    ArtifactPlacement, AttemptId, DevicePoolId, FailureKind, GenerationId, Requirement,
    RetryDecision, SlotCapability, TenantId, any_candidate_remains,
};
use gpq_proto::gpq::worker::v1::{CancelRequest, RemoteMessage};
use tokio::task::JoinHandle;

use crate::db::artifacts::ArtifactRow;
use crate::events::GenerationEvent;
use crate::state::AppState;

/// Sweep cadence for every concern in this module (ADR 0013's fallback tick
/// cadence; the same one-second interval also drives lease-expiry precision).
const SWEEP_INTERVAL: StdDuration = StdDuration::from_secs(1);

/// Upper bound on rows considered per concern, per Tenant, per sweep.
const SWEEP_LIMIT: i64 = 200;

/// A queued Generation older than `maximum_queue_age` times this multiplier,
/// with no capable candidate at all, expires outright rather than waiting
/// indefinitely. Ordinary starvation (a capable but busy fleet) is instead
/// handled by `gpq_domain::select_next`'s overdue-first ordering, which never
/// needs this — this sweep only catches Generations nothing could ever run.
const STARVATION_EXPIRY_MULTIPLIER: u32 = 2;

/// Starts the expiry loop as a background task.
#[must_use]
pub fn spawn(state: AppState) -> JoinHandle<()> {
    tokio::spawn(run(state))
}

async fn run(state: AppState) {
    let mut tick = tokio::time::interval(SWEEP_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = state.shutdown.cancelled() => {
                tracing::info!("expiry loop observed shutdown; exiting");
                return;
            }
            _ = tick.tick() => {}
        }
        for tenant in crate::scheduler::known_tenants(&state).await {
            sweep_tenant(&state, tenant).await;
        }
    }
}

async fn sweep_tenant(state: &AppState, tenant: TenantId) {
    if let Err(error) = expire_leases(state, tenant).await {
        tracing::error!(%tenant, %error, "lease expiry sweep failed");
    }
    if let Err(error) = enforce_execution_deadlines(state, tenant).await {
        tracing::error!(%tenant, %error, "execution deadline sweep failed");
    }
    if let Err(error) = expire_starved_queue(state, tenant).await {
        tracing::error!(%tenant, %error, "queue starvation sweep failed");
    }
    if let Err(error) = propagate_cancellations(state, tenant).await {
        tracing::error!(%tenant, %error, "cancellation propagation sweep failed");
    }
    if let Err(error) = expire_artifacts(state, tenant).await {
        tracing::error!(%tenant, %error, "artifact expiry sweep failed");
    }
}

/// Re-delivers cooperative cancellation to the Workers still executing
/// Attempts of `Cancelling` Generations (ADR 0003).
///
/// Cancellation is requested by whoever observed it — an API call or a
/// client disconnecting — which only moves the Generation to
/// `Cancelling` in `PostgreSQL`. This sweep is what actually reaches the
/// Worker, and re-sending every tick is what makes cancellation survive a
/// Worker reconnect or a Remote restart. Repeating it is safe because ADR 0003
/// makes cancellation idempotent.
async fn propagate_cancellations(state: &AppState, tenant: TenantId) -> anyhow::Result<()> {
    let now = state.db.now().await?;
    let cancelling = {
        let mut tx = state.db.begin_tenant(tenant).await?;
        let cancelling =
            crate::db::attempts::live_for_cancelling_generations(&mut tx, tenant, SWEEP_LIMIT)
                .await?;
        for attempt in &cancelling {
            if crate::db::attempts::request_cancel(&mut tx, tenant, attempt.attempt_id(), now)
                .await?
            {
                tracing::info!(
                    %tenant,
                    attempt_id = %attempt.attempt_id(),
                    generation_id = %attempt.generation(),
                    "requesting cooperative cancellation of a running attempt"
                );
            }
        }
        tx.commit().await?;
        cancelling
    };

    for attempt in &cancelling {
        if !state
            .workers
            .send(
                attempt.worker(),
                cancel_request_message(attempt.attempt_id(), "cancellation requested"),
            )
            .is_delivered()
        {
            tracing::debug!(
                %tenant,
                attempt_id = %attempt.attempt_id(),
                worker_id = %attempt.worker(),
                "cooperative cancellation not delivered; worker offline or backpressured"
            );
        }
    }
    Ok(())
}

/// Whether an Attempt's retry decision means its Generation just reached a
/// terminal state (ADR 0003: a `Fail` decision settles the Generation
/// `Failed`, or `Cancelled` when it raced a cancellation). Kept as a pure
/// predicate so every sweep that gates terminal cleanup — deleting a
/// Generation's input Artifacts (ADR 0008) — on it agrees without duplicating
/// the comparison.
fn settles_generation(decision: RetryDecision) -> bool {
    decision == RetryDecision::Fail
}

/// A `RemoteMessage` cooperatively cancelling `attempt_id` for `reason`
/// (ADR 0003).
fn cancel_request_message(attempt_id: AttemptId, reason: &str) -> RemoteMessage {
    RemoteMessage {
        message: Some(
            CancelRequest {
                attempt_id: attempt_id.to_string(),
                reason: reason.to_owned(),
                ..Default::default()
            }
            .into(),
        ),
        ..Default::default()
    }
}

/// The object-store key whose bytes must be deleted along with an input
/// Artifact's row (ADR 0008). Remote-local inputs keep their bytes in the row
/// itself and are removed with it; Worker-local placement never applies to
/// inputs. An object-store row missing its key is left alone rather than
/// guessed at. Pure so the per-placement rule is unit-testable without
/// touching object storage.
fn input_object_key(artifact: &ArtifactRow) -> Option<&str> {
    match artifact.placement {
        ArtifactPlacement::ObjectStore => artifact.object_key.as_deref(),
        ArtifactPlacement::WorkerLocal | ArtifactPlacement::RemoteLocal => None,
    }
}

/// Deletes the object-store bytes of input Artifacts already deleted from
/// `PostgreSQL` at a Generation's terminal transition (ADR 0008). Shared by
/// every terminal path that removes inputs, including the Worker session.
pub(crate) async fn release_deleted_inputs(
    artifacts: &crate::artifacts::ArtifactService,
    tenant: TenantId,
    rows: Vec<ArtifactRow>,
) {
    for artifact in rows {
        let Some(key) = input_object_key(&artifact) else {
            continue;
        };
        if let Err(error) = artifacts.delete(key).await {
            tracing::warn!(
                %tenant, %error, key,
                "failed to delete a terminated generation's input artifact"
            );
        }
    }
}

/// Re-fetches `generation` and durably records + live-publishes its current
/// state (ADR 0008: state transitions are retained; ADR 0006: subscribers
/// observe them live), mirroring the snapshot every other state-changing
/// caller publishes after its own transition.
async fn publish_generation_state(state: &AppState, tenant: TenantId, generation: GenerationId) {
    let Ok(mut conn) = state.db.begin_tenant(tenant).await else {
        return;
    };
    let row = match crate::db::generations::get(&mut conn, tenant, generation).await {
        Ok(Some(row)) => row,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%tenant, %generation, %error, "failed to re-fetch an expired generation's state");
            return;
        }
    };
    let Ok(gen_state) = row.state() else {
        return;
    };
    let attempt_count = u32::try_from(row.attempt_count).unwrap_or(0);
    let failure = row
        .failure()
        .ok()
        .flatten()
        .map(|(kind, message)| (kind, message.to_owned()));
    let event = GenerationEvent::State {
        state: gen_state,
        attempt_count,
        failure,
    };
    if let Err(error) = state
        .events
        .record(&state.db, tenant, generation, &event)
        .await
    {
        tracing::warn!(%tenant, %generation, %error, "failed to record a generation state event");
    }
}

/// Expires live Attempts whose lease lapsed without a renewing heartbeat,
/// cooperatively cancels them on their Worker, and applies ADR 0003's retry
/// policy to each one's Generation.
async fn expire_leases(state: &AppState, tenant: TenantId) -> anyhow::Result<()> {
    let now = state.db.now().await?;
    let mut deleted_inputs = Vec::new();
    let mut changed_generations = Vec::new();
    let expired = {
        let mut tx = state.db.begin_tenant(tenant).await?;
        let expired =
            crate::db::attempts::expired_leases(&mut tx, tenant, now, SWEEP_LIMIT).await?;
        for attempt in &expired {
            tracing::info!(
                %tenant,
                attempt_id = %attempt.attempt_id(),
                generation_id = %attempt.generation(),
                pool_id = %attempt.pool(),
                attempt_number = attempt.attempt_number,
                prior_state = ?attempt.state(),
                created_at = %attempt.created_at,
                started_at = ?attempt.started_at,
                last_heartbeat_at = ?attempt.last_heartbeat_at,
                already_cancel_requested = attempt.cancel_requested_at.is_some(),
                "attempt lease expired without a renewing heartbeat"
            );
            crate::db::attempts::request_cancel(&mut tx, tenant, attempt.attempt_id(), now).await?;
            if let Some((generation_id, decision)) =
                crate::db::attempts::record_lease_expiry(&mut tx, tenant, attempt.attempt_id(), now)
                    .await?
            {
                changed_generations.push(generation_id);
                if settles_generation(decision) {
                    let rows = crate::db::artifacts::delete_inputs_for_generation(
                        &mut tx,
                        tenant,
                        generation_id,
                    )
                    .await?;
                    deleted_inputs.extend(rows);
                }
            }
        }
        tx.commit().await?;
        expired
    };

    release_deleted_inputs(&state.artifacts, tenant, deleted_inputs).await;
    for generation_id in changed_generations {
        publish_generation_state(state, tenant, generation_id).await;
    }
    for attempt in &expired {
        if !state
            .workers
            .send(
                attempt.worker(),
                cancel_request_message(attempt.attempt_id(), "lease expired"),
            )
            .is_delivered()
        {
            tracing::debug!(
                %tenant,
                attempt_id = %attempt.attempt_id(),
                worker_id = %attempt.worker(),
                "lease-expiry cancellation not delivered; worker offline or backpressured"
            );
        }
    }
    Ok(())
}

/// Fails Attempts that ran past their execution deadline (ADR 0003: this
/// failure is never retried) and cooperatively cancels them on their Worker.
async fn enforce_execution_deadlines(state: &AppState, tenant: TenantId) -> anyhow::Result<()> {
    let now = state.db.now().await?;
    let mut deleted_inputs = Vec::new();
    let mut changed_generations = Vec::new();
    let overdue = {
        let mut tx = state.db.begin_tenant(tenant).await?;
        let overdue =
            crate::db::attempts::overdue_executions(&mut tx, tenant, now, SWEEP_LIMIT).await?;
        for attempt in &overdue {
            let overdue_by = attempt.execution_deadline.map(|deadline| now - deadline);
            tracing::info!(
                %tenant,
                attempt_id = %attempt.attempt_id(),
                generation_id = %attempt.generation(),
                attempt_number = attempt.attempt_number,
                prior_state = ?attempt.state(),
                started_at = ?attempt.started_at,
                overdue_by = ?overdue_by,
                "attempt exceeded its execution deadline"
            );
            if let Some((generation_id, decision)) = crate::db::attempts::record_failure(
                &mut tx,
                tenant,
                attempt.attempt_id(),
                crate::db::attempts::FailureDetails {
                    kind: FailureKind::ExecutionTimedOut,
                    message: "execution exceeded its resolved timeout",
                    worker_retry_hint: false,
                },
                now,
            )
            .await?
            {
                changed_generations.push(generation_id);
                if settles_generation(decision) {
                    let rows = crate::db::artifacts::delete_inputs_for_generation(
                        &mut tx,
                        tenant,
                        generation_id,
                    )
                    .await?;
                    deleted_inputs.extend(rows);
                }
            }
        }
        tx.commit().await?;
        overdue
    };

    release_deleted_inputs(&state.artifacts, tenant, deleted_inputs).await;
    for generation_id in changed_generations {
        publish_generation_state(state, tenant, generation_id).await;
    }
    for attempt in &overdue {
        if !state
            .workers
            .send(
                attempt.worker(),
                cancel_request_message(attempt.attempt_id(), "execution timed out"),
            )
            .is_delivered()
        {
            tracing::debug!(
                %tenant,
                attempt_id = %attempt.attempt_id(),
                worker_id = %attempt.worker(),
                "execution-deadline cancellation not delivered; worker offline or backpressured"
            );
        }
    }
    Ok(())
}

/// Returns whether any Pool capable of the Generation remains after exclusions.
fn has_unrejected_candidate<'a>(
    capabilities: impl IntoIterator<Item = &'a SlotCapability>,
    requirement: &Requirement,
    excluded: Option<&HashSet<DevicePoolId>>,
) -> bool {
    any_candidate_remains(
        capabilities.into_iter().filter(|capability| {
            !excluded.is_some_and(|pools| pools.contains(&capability.pool_id))
        }),
        requirement,
    )
}

/// Expires `Queued` Generations old enough that ADR 0002's starvation guard
/// has long since kicked in, but for which no registered, unrejected Slot
/// could ever satisfy the Requirement (a capable-but-busy fleet is not expired
/// here — `select_next` already prioritizes it ahead of everything else).
async fn expire_starved_queue(state: &AppState, tenant: TenantId) -> anyhow::Result<()> {
    let now = state.db.now().await?;
    let mut deleted_inputs = Vec::new();
    let mut expired_generations = Vec::new();
    {
        let mut tx = state.db.begin_tenant(tenant).await?;
        let settings = crate::db::tenants::settings(&mut tx, tenant)
            .await?
            .unwrap_or_default();

        let Some(std_threshold) = settings
            .maximum_queue_age
            .checked_mul(STARVATION_EXPIRY_MULTIPLIER)
        else {
            return Ok(());
        };
        let Ok(threshold) = chrono::Duration::from_std(std_threshold) else {
            return Ok(());
        };
        let cutoff: DateTime<Utc> = now - threshold;

        let candidates =
            crate::db::generations::queued_candidates(&mut tx, tenant, SWEEP_LIMIT).await?;
        if candidates.is_empty() {
            return Ok(());
        }
        let capabilities = crate::db::workers::pool_capabilities(&mut tx, tenant).await?;
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

        for candidate in candidates
            .iter()
            .filter(|candidate| candidate.created_at <= cutoff)
        {
            let excluded = exclusions.get(&candidate.generation_id);
            let has_candidate = has_unrejected_candidate(
                capabilities.iter().map(|(capability, ..)| capability),
                &candidate.requirement,
                excluded,
            );
            if !has_candidate
                && crate::db::generations::expire(&mut tx, tenant, candidate.generation_id, now)
                    .await?
            {
                expired_generations.push(candidate.generation_id);
                let rows = crate::db::artifacts::delete_inputs_for_generation(
                    &mut tx,
                    tenant,
                    candidate.generation_id,
                )
                .await?;
                deleted_inputs.extend(rows);
            }
        }
        tx.commit().await?;
    }

    release_deleted_inputs(&state.artifacts, tenant, deleted_inputs).await;
    for generation_id in expired_generations {
        publish_generation_state(state, tenant, generation_id).await;
    }
    Ok(())
}

/// Expires lapsed Artifacts and deletes the underlying object-store bytes of
/// any that were placed there (ADR 0008); Worker-local bytes are reconciled by
/// the Worker itself on its own startup scan.
async fn expire_artifacts(state: &AppState, tenant: TenantId) -> anyhow::Result<()> {
    let now = state.db.now().await?;
    let expired = {
        let mut tx = state.db.begin_tenant(tenant).await?;
        let expired = crate::db::artifacts::expire_due(&mut tx, now).await?;
        tx.commit().await?;
        expired
    };

    for artifact in expired {
        if artifact.placement == ArtifactPlacement::ObjectStore
            && let Some(key) = &artifact.object_key
            && let Err(error) = state.artifacts.delete(key).await
        {
            tracing::warn!(%tenant, %error, key, "failed to delete an expired object-store artifact");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starvation_sees_no_candidates_only_after_every_capable_pool_is_excluded() {
        use std::collections::{BTreeMap, BTreeSet};

        let tenant_id = TenantId::new();
        let worker_id = gpq_domain::WorkerId::new();
        let pool_a = DevicePoolId::new();
        let pool_b = DevicePoolId::new();
        let version = gpq_domain::ContentHash::from_bytes([1; 32]);
        let manifest = gpq_domain::ModelManifest {
            family: gpq_domain::ModelFamily::QwenImage21,
            assets: BTreeMap::new(),
            default_loras: Vec::new(),
            sampling: gpq_domain::ModelFamily::QwenImage21.default_sampling_preset(),
        };
        let capability = |pool_id| SlotCapability {
            tenant_id,
            worker_id,
            pool_id,
            slot_id: gpq_domain::SlotId::new(),
            accelerator: "metal".to_owned(),
            accelerator_version: "test".to_owned(),
            model_manifests: BTreeMap::from([(version, manifest.clone())]),
            image_loras: BTreeSet::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            shape_limits: gpq_domain::ShapeLimits::default(),
        };
        let requirement = Requirement {
            tenant_id,
            modality: gpq_domain::Modality::Image,
            version,
            required_loras: BTreeSet::new(),
            shape: gpq_domain::GenerationShape {
                width: 64,
                height: 64,
                steps: Some(4),
                frames: None,
            },
        };
        let capabilities = [capability(pool_a), capability(pool_b)];
        let only_first_rejected = HashSet::from([pool_a]);
        assert!(has_unrejected_candidate(
            capabilities.iter(),
            &requirement,
            Some(&only_first_rejected)
        ));

        let all_rejected = HashSet::from([pool_a, pool_b]);
        assert!(!has_unrejected_candidate(
            capabilities.iter(),
            &requirement,
            Some(&all_rejected)
        ));
    }

    #[test]
    fn starvation_multiplier_widens_the_tenant_configured_queue_age() {
        let settings = gpq_domain::TenantSettings::default();
        let Some(widened) = settings
            .maximum_queue_age
            .checked_mul(STARVATION_EXPIRY_MULTIPLIER)
        else {
            panic!("default queue age times a small multiplier must not overflow");
        };
        assert!(widened > settings.maximum_queue_age);
        assert_eq!(widened, settings.maximum_queue_age * 2);
    }

    #[test]
    fn only_a_fail_decision_settles_the_generation() {
        assert!(settles_generation(RetryDecision::Fail));
        assert!(!settles_generation(RetryDecision::Requeue));
    }

    fn sample_input_row(placement: ArtifactPlacement, object_key: Option<&str>) -> ArtifactRow {
        ArtifactRow {
            id: gpq_domain::ArtifactId::new(),
            direction: crate::db::artifacts::ArtifactDirection::Input,
            state: gpq_domain::ArtifactState::Available,
            placement,
            manifest: gpq_domain::ArtifactManifest {
                size_bytes: 4,
                digest: gpq_domain::ContentHash::from_bytes([0; 32]),
                kind: gpq_domain::MediaKind::Binary,
                mime_type: "application/octet-stream".to_owned(),
            },
            object_key: object_key.map(str::to_owned),
            worker_id: None,
            delivery_token: None,
            committed_offset: 0,
        }
    }

    #[test]
    fn object_store_input_with_a_key_is_deleted_by_key() {
        // ADR 0008: an object-store input's bytes are removed directly.
        let row = sample_input_row(ArtifactPlacement::ObjectStore, Some("tenant/gen/input"));
        assert_eq!(input_object_key(&row), Some("tenant/gen/input"));
    }

    #[test]
    fn object_store_input_missing_its_key_takes_no_action() {
        // An orphaned row (placement says object store, but no key was ever
        // recorded) must not be guessed at; it is simply left alone.
        let row = sample_input_row(ArtifactPlacement::ObjectStore, None);
        assert_eq!(input_object_key(&row), None);
    }

    #[test]
    fn remote_local_input_is_released_with_its_row() {
        // ADR 0008: remote-local bytes live in the row itself, so deleting
        // the row is the whole cleanup and there is no external key.
        let row = sample_input_row(ArtifactPlacement::RemoteLocal, None);
        assert_eq!(input_object_key(&row), None);
    }

    #[test]
    fn worker_local_input_never_applies_and_takes_no_action() {
        // ADR 0008: Worker-local placement never applies to inputs.
        let row = sample_input_row(ArtifactPlacement::WorkerLocal, None);
        assert_eq!(input_object_key(&row), None);
    }

    #[test]
    fn cancel_request_carries_the_attempt_id_and_reason() {
        // ADR 0003: cooperative cancellation names exactly the Attempt and
        // the reason it was cancelled.
        let attempt_id = AttemptId::new();
        let message = cancel_request_message(attempt_id, "lease expired");
        let Some(gpq_proto::gpq::worker::v1::__buffa::oneof::remote_message::Message::Cancel(
            cancel,
        )) = message.message
        else {
            panic!("expected a Cancel message");
        };
        assert_eq!(cancel.attempt_id, attempt_id.to_string());
        assert_eq!(cancel.reason, "lease expired");
    }
}
