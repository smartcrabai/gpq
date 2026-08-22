//! Time-driven cleanup: lease expiry, execution-deadline enforcement, queue
//! starvation expiry, and Artifact expiry (ADR 0002, ADR 0003, ADR 0008,
//! ADR 0013).
//!
//! Every sweep here runs as its own short tenant-scoped transaction
//! (`Db::begin_tenant`, ADR 0011) so row-level security protects every
//! tenant-owned table it touches; only the roster of Tenant ids to sweep comes
//! from the one administrative read [`crate::scheduler::known_tenants`]
//! already uses.

use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use gpq_domain::{
    ArtifactPlacement, AttemptId, FailureKind, GenerationId, GenerationState, RetryDecision,
    TenantId, any_candidate_remains,
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
/// Cancellation is requested by whoever observed it — an API call or an
/// `OpenAI` client disconnecting — which only moves the Generation to
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

/// What discarding an already-deleted input Artifact's underlying bytes
/// requires, by placement (ADR 0008): object-store bytes are deleted by
/// key; inline-relay bytes are dropped from the in-process buffer;
/// Worker-local placement never applies to inputs, so it needs no action.
/// An object-store row missing its key is left alone rather than guessed
/// at, mirroring the orphaned-row guard already in place. Pure so the
/// per-placement branch is unit-testable without touching object storage.
enum InputDiscardAction {
    DeleteObjectStoreKey(String),
    DiscardLocal,
    None,
}

fn input_discard_action(artifact: &ArtifactRow) -> InputDiscardAction {
    match artifact.placement {
        ArtifactPlacement::ObjectStore => artifact.object_key.clone().map_or(
            InputDiscardAction::None,
            InputDiscardAction::DeleteObjectStoreKey,
        ),
        ArtifactPlacement::InlineRelay => InputDiscardAction::DiscardLocal,
        ArtifactPlacement::WorkerLocal => InputDiscardAction::None,
    }
}

/// Deletes the underlying bytes of Artifacts already deleted from `PostgreSQL`
/// (ADR 0008): object-store bytes are removed directly; bytes relayed inline
/// through a connected request are dropped from the in-process buffer.
/// Worker-local placement never applies to inputs (ADR 0008), so it needs no
/// action here.
async fn discard_input_artifacts(state: &AppState, tenant: TenantId, rows: Vec<ArtifactRow>) {
    for artifact in rows {
        match input_discard_action(&artifact) {
            InputDiscardAction::DeleteObjectStoreKey(key) => {
                if let Err(error) = state.artifacts.delete(&key).await {
                    tracing::warn!(
                        %tenant, %error, key,
                        "failed to delete a terminated generation's input artifact"
                    );
                }
            }
            InputDiscardAction::DiscardLocal => state.artifacts.discard_local(artifact.id),
            InputDiscardAction::None => {}
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

    discard_input_artifacts(state, tenant, deleted_inputs).await;
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
                FailureKind::ExecutionTimedOut,
                "execution exceeded its resolved timeout",
                false,
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

    discard_input_artifacts(state, tenant, deleted_inputs).await;
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

/// Expires `Queued` Generations old enough that ADR 0002's starvation guard
/// has long since kicked in, but for which no registered Slot could ever
/// satisfy the Requirement at all (a capable-but-busy fleet is not expired
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

        for candidate in candidates
            .iter()
            .filter(|candidate| candidate.created_at <= cutoff)
        {
            let has_candidate = any_candidate_remains(
                capabilities.iter().map(|(capability, ..)| capability),
                &candidate.requirement,
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

    discard_input_artifacts(state, tenant, deleted_inputs).await;
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

/// Cancels every nonterminal synchronous (`OpenAI`) Generation across every
/// Tenant before Worker sessions are accepted (ADR 0003): their original HTTP
/// connection cannot have survived a Remote restart, so leaving them `Running`
/// or `Queued` would leak invisible work with no client left to deliver to.
///
/// # Errors
///
/// Currently infallible: it never returns `Err`. Per-tenant cancellation
/// failures (a database fault fetching or updating that Tenant's
/// Generations) are logged and skipped rather than propagated, so one
/// Tenant's failure does not stop startup cancellation for the rest.
pub async fn cancel_synchronous_on_startup(state: &AppState) -> anyhow::Result<()> {
    let tenants = crate::scheduler::known_tenants(state).await;
    for tenant in tenants {
        if let Err(error) = cancel_synchronous_for_tenant(state, tenant).await {
            tracing::error!(
                %tenant, %error,
                "failed to cancel nonterminal synchronous generations on startup"
            );
        }
    }
    Ok(())
}

async fn cancel_synchronous_for_tenant(state: &AppState, tenant: TenantId) -> anyhow::Result<()> {
    let now = state.db.now().await?;
    let mut deleted_inputs = Vec::new();
    let mut changed_generations = Vec::new();
    {
        let mut tx = state.db.begin_tenant(tenant).await?;
        for row in crate::db::generations::nonterminal_synchronous(&mut tx, tenant).await? {
            let id = row.generation_id();
            let caller_kind = row.caller_kind();
            let priority = row.priority();
            match row.state() {
                Ok(GenerationState::Queued) => {
                    if crate::db::generations::cancel_queued(&mut tx, tenant, id, now).await? {
                        tracing::info!(
                            %tenant, %id, ?caller_kind, ?priority,
                            "cancelled a queued synchronous generation on startup"
                        );
                        changed_generations.push(id);
                        let rows =
                            crate::db::artifacts::delete_inputs_for_generation(&mut tx, tenant, id)
                                .await?;
                        deleted_inputs.extend(rows);
                    }
                }
                Ok(GenerationState::Running | GenerationState::Cancelling) => {
                    if crate::db::generations::request_cancel_running(&mut tx, tenant, id, now)
                        .await?
                    {
                        tracing::info!(
                            %tenant, %id, ?caller_kind, ?priority,
                            "requested cooperative cancellation for a running synchronous generation on startup"
                        );
                        changed_generations.push(id);
                    }
                }
                Ok(_) | Err(_) => {}
            }
        }
        tx.commit().await?;
    }

    discard_input_artifacts(state, tenant, deleted_inputs).await;
    for generation_id in changed_generations {
        publish_generation_state(state, tenant, generation_id).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(matches!(
            input_discard_action(&row),
            InputDiscardAction::DeleteObjectStoreKey(key) if key == "tenant/gen/input"
        ));
    }

    #[test]
    fn object_store_input_missing_its_key_takes_no_action() {
        // An orphaned row (placement says object store, but no key was ever
        // recorded) must not be guessed at; it is simply left alone.
        let row = sample_input_row(ArtifactPlacement::ObjectStore, None);
        assert!(matches!(
            input_discard_action(&row),
            InputDiscardAction::None
        ));
    }

    #[test]
    fn inline_relay_input_discards_the_in_process_buffer() {
        // ADR 0008: inline-relay bytes only ever live in Remote's memory.
        let row = sample_input_row(ArtifactPlacement::InlineRelay, None);
        assert!(matches!(
            input_discard_action(&row),
            InputDiscardAction::DiscardLocal
        ));
    }

    #[test]
    fn worker_local_input_never_applies_and_takes_no_action() {
        // ADR 0008: Worker-local placement never applies to inputs.
        let row = sample_input_row(ArtifactPlacement::WorkerLocal, None);
        assert!(matches!(
            input_discard_action(&row),
            InputDiscardAction::None
        ));
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
