//! Attempt persistence, leasing, and the retry orchestration that couples an
//! Attempt's outcome to its Generation (ADR 0002, ADR 0003, ADR 0013).
//!
//! Every function here operates on an already tenant-scoped connection
//! (`Db::begin_tenant`, ADR 0011): row-level security confines each statement
//! to the caller's Tenant. `tenant_id` still appears in every `WHERE` clause as
//! defense in depth and because the primary key is the composite
//! `(tenant_id, id)`.

use chrono::{DateTime, Utc};
use gpq_domain::{
    AttemptId, AttemptState, DevicePoolId, FailureKind, GenerationId, RetryDecision, TenantId,
    WorkerId, lease_expiry_from, state::UnknownState,
};
use sqlx::PgConnection;
use uuid::Uuid;

/// Maximum number of Attempts per Generation, including the first (ADR 0003).
const MAX_ATTEMPTS: i32 = gpq_domain::failure::MAX_ATTEMPTS.cast_signed();

/// One row of the `attempts` table (see `migrations/0001_initial.sql`).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AttemptRow {
    /// Stored identity.
    pub id: Uuid,
    /// The Generation this Attempt executes.
    pub generation_id: Uuid,
    /// One-based Attempt ordinal for its Generation (ADR 0003, max 3).
    pub attempt_number: i32,
    /// Persisted `AttemptState` name.
    pub state: String,
    /// The Worker holding the lease.
    pub worker_id: Uuid,
    /// The Device Pool executing the Attempt.
    pub pool_id: Uuid,
    /// Worker-local Execution Slot identity.
    pub slot_key: String,
    /// Lease expiry in database time (ADR 0003, ADR 0013).
    pub lease_expires_at: DateTime<Utc>,
    /// Absolute execution deadline, set once the Worker confirms `Running`
    /// (ADR 0003: "execution timeout begins at `Running`").
    pub execution_deadline: Option<DateTime<Utc>>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// When the Worker reported the Attempt running.
    pub started_at: Option<DateTime<Utc>>,
    /// Last accepted heartbeat.
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// When cooperative cancellation was requested.
    pub cancel_requested_at: Option<DateTime<Utc>>,
}

impl AttemptRow {
    /// Typed identity of this Attempt.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        AttemptId::from_uuid(self.id)
    }

    /// Typed identity of the executed Generation.
    #[must_use]
    pub const fn generation(&self) -> GenerationId {
        GenerationId::from_uuid(self.generation_id)
    }

    /// Typed identity of the leasing Worker.
    #[must_use]
    pub const fn worker(&self) -> WorkerId {
        WorkerId::from_uuid(self.worker_id)
    }

    /// Typed identity of the executing Device Pool.
    #[must_use]
    pub const fn pool(&self) -> DevicePoolId {
        DevicePoolId::from_uuid(self.pool_id)
    }

    /// Parses the persisted `AttemptState`.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownState`] if the persisted `state` column holds a
    /// name that is not a recognized [`AttemptState`] variant.
    pub fn state(&self) -> Result<AttemptState, UnknownState> {
        self.state.parse()
    }
}

/// Failure to create a new Attempt.
#[derive(Debug, thiserror::Error)]
pub enum CreateAttemptError {
    /// The Generation is not `Queued` or this pool was rejected for it.
    #[error("generation is not queued or pool is excluded for it")]
    NotQueued,
    /// The Generation already used every automatic retry (ADR 0003).
    #[error("generation already used the maximum of {max} attempts")]
    MaxAttemptsReached {
        /// The configured maximum.
        max: u32,
    },
    /// A database error occurred.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Creates the next Attempt for a queued Generation and leases it to one Slot.
///
/// Asserts `attempt_number = generations.attempt_count + 1`, rejecting a fourth
/// Attempt (ADR 0003's `MAX_ATTEMPTS`). The new Attempt starts `Leased` with no
/// `execution_deadline`: ADR 0003 starts that clock only once the Worker
/// confirms `Running` (see [`mark_running`]). The caller is expected to hold a
/// `FOR UPDATE SKIP LOCKED` claim on the Generation already; this function
/// locks it again defensively and is safe to call without that pre-claim too.
///
/// # Errors
///
/// Returns [`CreateAttemptError::NotQueued`] if the Generation does not
/// exist, is not `Queued`, or `pool_id` has been rejected for this Generation.
/// The Generation lock makes the latter check atomic with concurrent
/// pre-execution rejections. Returns [`CreateAttemptError::MaxAttemptsReached`]
/// if the Generation already has `MAX_ATTEMPTS` Attempts (ADR 0003). Returns
/// [`CreateAttemptError::Database`] if a query or write fails.
pub async fn create(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
    worker_id: WorkerId,
    pool_id: DevicePoolId,
    slot_key: &str,
    lease_expiry: DateTime<Utc>,
) -> Result<AttemptRow, CreateAttemptError> {
    #[derive(sqlx::FromRow)]
    struct GenerationLock {
        state: String,
        attempt_count: i32,
    }

    let Some(generation) = sqlx::query_as::<_, GenerationLock>(
        "SELECT state, attempt_count FROM generations WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Err(CreateAttemptError::NotQueued);
    };
    if generation.state != "queued" {
        return Err(CreateAttemptError::NotQueued);
    }
    if crate::db::native_exclusions::excluded_pools(conn, tenant_id, generation_id)
        .await?
        .contains(&pool_id)
    {
        return Err(CreateAttemptError::NotQueued);
    }

    let attempt_number = generation.attempt_count + 1;
    if attempt_number > MAX_ATTEMPTS {
        return Err(CreateAttemptError::MaxAttemptsReached {
            max: gpq_domain::failure::MAX_ATTEMPTS,
        });
    }

    let attempt_id = AttemptId::new();
    let row = sqlx::query_as::<_, AttemptRow>(
        "INSERT INTO attempts \
            (tenant_id, id, generation_id, attempt_number, state, worker_id, pool_id, slot_key, \
             lease_expires_at) \
         VALUES ($1, $2, $3, $4, 'leased', $5, $6, $7, $8) \
         RETURNING *",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(generation_id.as_uuid())
    .bind(attempt_number)
    .bind(worker_id.as_uuid())
    .bind(pool_id.as_uuid())
    .bind(slot_key)
    .bind(lease_expiry)
    .fetch_one(&mut *conn)
    .await?;

    sqlx::query("UPDATE generations SET attempt_count = $3 WHERE tenant_id = $1 AND id = $2")
        .bind(tenant_id.as_uuid())
        .bind(generation_id.as_uuid())
        .bind(attempt_number)
        .execute(&mut *conn)
        .await?;

    Ok(row)
}

/// Renews the lease of every named, still-live Attempt owned by `worker_id`.
///
/// Returns the subset of `attempt_ids` that could **not** be renewed (unowned,
/// already terminal, or already lease-expired), so the caller (the Worker
/// control session) knows which ones to send a `CancelRequest` for.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn heartbeat(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
    attempt_ids: &[AttemptId],
    now: DateTime<Utc>,
) -> sqlx::Result<Vec<AttemptId>> {
    if attempt_ids.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<Uuid> = attempt_ids.iter().map(AttemptId::as_uuid).collect();
    let expiry = lease_expiry_from(now);

    let renewed: Vec<(Uuid,)> = sqlx::query_as(
        "UPDATE attempts SET lease_expires_at = clock_timestamp() + ($4 - $3), \
            last_heartbeat_at = clock_timestamp() \
         WHERE tenant_id = $1 AND worker_id = $2 AND id = ANY($5) \
            AND state IN ('leased', 'running') \
            AND lease_expires_at > clock_timestamp() \
         RETURNING id",
    )
    .bind(tenant_id.as_uuid())
    .bind(worker_id.as_uuid())
    .bind(now)
    .bind(expiry)
    .bind(&ids)
    .fetch_all(conn)
    .await?;

    let renewed: std::collections::HashSet<Uuid> = renewed.into_iter().map(|(id,)| id).collect();
    Ok(attempt_ids
        .iter()
        .filter(|id| !renewed.contains(&id.as_uuid()))
        .copied()
        .collect())
}

/// The Generation this Attempt executes, without locking anything.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn generation_id_of(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    worker_id: WorkerId,
    now: DateTime<Utc>,
) -> sqlx::Result<Option<GenerationId>> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "SELECT generation_id FROM attempts \
         WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 \
            AND state IN ('leased', 'running') AND lease_expires_at > $4",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(worker_id.as_uuid())
    .bind(now)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|(id,)| GenerationId::from_uuid(id)))
}

/// `Leased -> Running`, recording `started_at` and starting the execution
/// timeout clock: `execution_deadline = now + generations.execution_timeout`
/// (ADR 0003).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn mark_running(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: AttemptId,
    worker_id: WorkerId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE attempts a SET state = 'running', started_at = $4, \
            execution_deadline = $4 + g.execution_timeout \
         FROM generations g \
         WHERE a.tenant_id = $1 AND a.id = $2 AND a.worker_id = $3 \
            AND a.state = 'leased' AND a.lease_expires_at > $4 \
            AND g.tenant_id = a.tenant_id AND g.id = a.generation_id",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(worker_id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Settles a live Attempt into a terminal state, recording an optional failure,
/// and releases the Execution Slot it held (ADR 0005: a terminal Attempt
/// never keeps its Slot occupied).
///
/// `to` must be one of `Succeeded`, `Failed`, `Cancelled`, or `LeaseExpired`;
/// illegal targets are rejected without touching the database.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the settling update, or the Execution Slot
/// release that follows a successful settlement, fails.
pub async fn finish(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: AttemptId,
    worker_id: Option<WorkerId>,
    to: AttemptState,
    failure: Option<(FailureKind, &str, bool)>,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    if !to.is_terminal() {
        return Ok(false);
    }
    let (failure_kind, failure_message, worker_retry_hint) = match failure {
        Some((kind, message, hint)) => (Some(kind.as_str()), message, hint),
        None => (None, "", false),
    };
    let settled: Option<(Uuid,)> = sqlx::query_as(
        "UPDATE attempts SET state = $3, finished_at = $4, failure_kind = $5, \
            failure_message = $6, worker_retry_hint = $7 \
         WHERE tenant_id = $1 AND id = $2 AND state IN ('leased', 'running') \
            AND ($8::uuid IS NULL OR worker_id = $8) \
         RETURNING pool_id",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(to.as_str())
    .bind(now)
    .bind(failure_kind)
    .bind(failure_message)
    .bind(worker_retry_hint)
    .bind(worker_id.map(|id| id.as_uuid()))
    .fetch_optional(&mut *conn)
    .await?;
    let Some((pool_id,)) = settled else {
        return Ok(false);
    };
    crate::db::workers::release_slot(conn, tenant_id, DevicePoolId::from_uuid(pool_id)).await?;
    Ok(true)
}

/// Failure information shared by the Worker-facing and internal settlement
/// entry points. The message is borrowed through the transaction.
pub struct FailureDetails<'a> {
    /// The normalized cause that drives Remote's retry policy.
    pub kind: FailureKind,
    /// Raw diagnostic text stored with the failure.
    pub message: &'a str,
    /// The Worker's retry hint, recorded but not obeyed (ADR 0003).
    pub worker_retry_hint: bool,
}

/// A terminal Attempt state paired with the failure details it records.
struct Settlement<'a> {
    /// The terminal `AttemptState` this settlement records.
    to: AttemptState,
    /// Classified failure details, if the settlement records a failure.
    failure: FailureDetails<'a>,
}

/// Shared core of [`record_failure`], [`record_failure_for_worker`], and
/// [`record_lease_expiry`]: settles a live Attempt into `settlement.to` and
/// applies ADR 0003's retry policy to its Generation in the same transaction.
///
/// OOM Attempts and pre-execution rejections are excluded for this Generation
/// only; neither creates a future-model or shape-wide blacklist. A Generation
/// already `Cancelling` settles `Cancelled` regardless of retry policy.
/// Returns `None` only when the Attempt was already settled or gone.
async fn finish_and_decide(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    worker_id: Option<WorkerId>,
    settlement: Settlement<'_>,
    now: DateTime<Utc>,
) -> sqlx::Result<Option<(GenerationId, RetryDecision)>> {
    #[derive(sqlx::FromRow)]
    struct Lock {
        generation_id: Uuid,
    }
    let Some(lock) = sqlx::query_as::<_, Lock>(
        "SELECT generation_id FROM attempts \
         WHERE tenant_id = $1 AND id = $2 AND state IN ('leased', 'running') \
            AND ($3::uuid IS NULL OR worker_id = $3)",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(worker_id.map(|id| id.as_uuid()))
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let generation_id = GenerationId::from_uuid(lock.generation_id);

    let settled = finish(
        conn,
        tenant_id,
        attempt_id,
        worker_id,
        settlement.to,
        Some((
            settlement.failure.kind,
            settlement.failure.message,
            settlement.failure.worker_retry_hint,
        )),
        now,
    )
    .await?;
    if !settled {
        return Ok(None);
    }

    let (attempt_count, generation_state): (i32, String) = sqlx::query_as(
        "SELECT attempt_count, state FROM generations WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_one(&mut *conn)
    .await?;

    if generation_state == "cancelling" {
        crate::db::generations::finish_cancelled(conn, tenant_id, generation_id, now).await?;
        return Ok(Some((generation_id, RetryDecision::Fail)));
    }

    let requirement =
        crate::db::generations::requirement_of(conn, tenant_id, generation_id).await?;

    // Generation-local OOM history and pre-execution rejections filter only
    // this Generation's retry candidates.
    let excluded_pools =
        crate::db::native_exclusions::excluded_pools(conn, tenant_id, generation_id).await?;
    let capabilities = crate::db::workers::pool_capabilities(conn, tenant_id).await?;
    let eligible_candidates_remain = requirement.as_ref().is_some_and(|req| {
        gpq_domain::any_candidate_remains(
            capabilities
                .iter()
                .filter(|(capability, ..)| !excluded_pools.contains(&capability.pool_id))
                .map(|(capability, ..)| capability),
            req,
        )
    });
    let attempts_used = u32::try_from(attempt_count).unwrap_or(u32::MAX);
    let decision = settlement
        .failure
        .kind
        .retry_decision(attempts_used, eligible_candidates_remain);
    match decision {
        RetryDecision::Requeue => {
            crate::db::generations::requeue(conn, tenant_id, generation_id, now).await?;
        }
        RetryDecision::Fail => {
            crate::db::generations::fail(
                conn,
                tenant_id,
                generation_id,
                settlement.failure.kind,
                settlement.failure.message,
                now,
            )
            .await?;
        }
    }
    Ok(Some((generation_id, decision)))
}

/// Marks a live Attempt `Failed` (e.g. from a Worker's `AttemptFailure`) and
/// applies ADR 0003's retry policy to its Generation. See [`finish_and_decide`].
///
/// # Errors
///
/// Returns [`sqlx::Error`] if any query in [`finish_and_decide`]'s
/// settlement, retry-policy, or capability lookups fails.
pub async fn record_failure(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    failure: FailureDetails<'_>,
    now: DateTime<Utc>,
) -> sqlx::Result<Option<(GenerationId, RetryDecision)>> {
    finish_and_decide(
        conn,
        tenant_id,
        attempt_id,
        None,
        Settlement {
            to: AttemptState::Failed,
            failure,
        },
        now,
    )
    .await
}

/// Settles a Worker-reported failure only when the attempt belongs to it.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if any query in [`finish_and_decide`]'s
/// settlement, retry-policy, or capability lookups fails.
pub async fn record_failure_for_worker(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    worker_id: WorkerId,
    failure: FailureDetails<'_>,
    now: DateTime<Utc>,
) -> sqlx::Result<Option<(GenerationId, RetryDecision)>> {
    finish_and_decide(
        conn,
        tenant_id,
        attempt_id,
        Some(worker_id),
        Settlement {
            to: AttemptState::Failed,
            failure,
        },
        now,
    )
    .await
}

/// Marks a live Attempt `LeaseExpired` (the expiry sweep found no renewing
/// heartbeat) and applies ADR 0003's retry policy to its Generation. See
/// [`finish_and_decide`].
///
/// # Errors
///
/// Returns [`sqlx::Error`] if any query in [`finish_and_decide`]'s
/// settlement, retry-policy, or capability lookups fails.
pub async fn record_lease_expiry(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    now: DateTime<Utc>,
) -> sqlx::Result<Option<(GenerationId, RetryDecision)>> {
    finish_and_decide(
        conn,
        tenant_id,
        attempt_id,
        None,
        Settlement {
            to: AttemptState::LeaseExpired,
            failure: FailureDetails {
                kind: FailureKind::LeaseExpired,
                message: "lease expired without a renewing heartbeat",
                worker_retry_hint: false,
            },
        },
        now,
    )
    .await
}

/// Handles a pre-execution `LeaseRejected`: capability mismatches discovered
/// before execution starts never count as an Attempt (ADR 0003), so the
/// Attempt row is deleted outright, its claimed Execution Slot is released,
/// `attempt_count` is rolled back, and the Generation returns to `Queued`.
/// Returns `None` unless the named Worker owns a live `Leased` Attempt and its
/// Generation is `Running`; no slot, attempt, or Generation state changes then.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if recording the generation-scoped pool rejection,
/// deleting the Attempt, releasing its Slot, or requeuing the Generation fails.
pub async fn reject_lease(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    worker_id: WorkerId,
    failure_kind: FailureKind,
    message: &str,
    now: DateTime<Utc>,
) -> sqlx::Result<Option<GenerationId>> {
    #[derive(sqlx::FromRow)]
    struct LeaseLock {
        generation_id: Uuid,
        pool_id: Uuid,
        lease_expires_at: DateTime<Utc>,
    }
    let Some(lease) = sqlx::query_as::<_, LeaseLock>(
        "SELECT generation_id, pool_id, lease_expires_at FROM attempts \
         WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 AND state = 'leased' \
         FOR UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(worker_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    if lease.lease_expires_at <= now {
        return Ok(None);
    }
    let generation_id = GenerationId::from_uuid(lease.generation_id);
    let generation_state: Option<String> = sqlx::query_scalar(
        "SELECT state FROM generations WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    if generation_state.as_deref() != Some("running") {
        return Ok(None);
    }

    let pool_id = DevicePoolId::from_uuid(lease.pool_id);
    crate::db::native_exclusions::record_preexecution_rejection(
        conn,
        tenant_id,
        generation_id,
        pool_id,
    )
    .await?;
    sqlx::query(
        "DELETE FROM attempts WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 \
         AND state = 'leased'",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(worker_id.as_uuid())
    .execute(&mut *conn)
    .await?;
    crate::db::workers::release_slot(conn, tenant_id, pool_id).await?;

    sqlx::query(
        "UPDATE generations SET state = 'queued', attempt_count = greatest(attempt_count - 1, 0), \
            updated_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state = 'running'",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .bind(now)
    .execute(&mut *conn)
    .await?;

    tracing::info!(%tenant_id, %attempt_id, %failure_kind, detail = message, "worker rejected a lease before execution");
    Ok(Some(generation_id))
}

/// Terminal compare-and-set for a Worker's `CancelAcknowledged`: the Attempt
/// settles `Cancelled` (if still live) and the Generation settles `Cancelled`
/// (ADR 0003). Cancellation acknowledgement races result commitment through
/// this same kind of compare-and-set. `None` means the Attempt is not owned
/// by the named Worker or its Generation is not `Cancelling`; neither row or
/// slot is then changed.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the locking select, the Attempt settlement,
/// or the Generation's [`finish_cancelled`](crate::db::generations::finish_cancelled)
/// settlement fails.
pub async fn acknowledge_cancel(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    worker_id: WorkerId,
    now: DateTime<Utc>,
) -> sqlx::Result<Option<GenerationId>> {
    #[derive(sqlx::FromRow)]
    struct Lock {
        generation_id: Uuid,
        state: String,
    }
    let Some(lock) = sqlx::query_as::<_, Lock>(
        "SELECT generation_id, state FROM attempts \
         WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 FOR UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(worker_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let generation_id = GenerationId::from_uuid(lock.generation_id);
    // The update below rechecks this state without locking Generation ahead
    // of Attempt/Slot, preserving the settlement lock order.
    let generation_state: Option<String> =
        sqlx::query_scalar("SELECT state FROM generations WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id.as_uuid())
            .bind(generation_id.as_uuid())
            .fetch_optional(&mut *conn)
            .await?;
    if generation_state.as_deref() != Some("cancelling") {
        return Ok(None);
    }

    if let Ok(state) = lock.state.parse::<AttemptState>()
        && state.is_live()
    {
        let pool_id: Option<Uuid> = sqlx::query_scalar(
            "UPDATE attempts SET state = 'cancelled', finished_at = $4, failure_kind = NULL, \
                failure_message = '', worker_retry_hint = false \
             WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 \
                AND state IN ('leased', 'running') \
                AND EXISTS (SELECT 1 FROM generations g \
                    WHERE g.tenant_id = $1 AND g.id = $5 AND g.state = 'cancelling') \
             RETURNING pool_id",
        )
        .bind(tenant_id.as_uuid())
        .bind(attempt_id.as_uuid())
        .bind(worker_id.as_uuid())
        .bind(now)
        .bind(generation_id.as_uuid())
        .fetch_optional(&mut *conn)
        .await?;
        if let Some(pool_id) = pool_id {
            crate::db::workers::release_slot(conn, tenant_id, DevicePoolId::from_uuid(pool_id))
                .await?;
        } else {
            return Ok(None);
        }
    }
    let settled =
        crate::db::generations::finish_cancelled(conn, tenant_id, generation_id, now).await?;
    Ok(settled.then_some(generation_id))
}

/// Live Attempts whose lease has lapsed at `now`, locked for the expiry sweep.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn expired_leases(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    now: DateTime<Utc>,
    limit: i64,
) -> sqlx::Result<Vec<AttemptRow>> {
    sqlx::query_as::<_, AttemptRow>(
        "SELECT * FROM attempts \
         WHERE tenant_id = $1 AND state IN ('leased', 'running') AND lease_expires_at <= $2 \
         ORDER BY lease_expires_at \
         LIMIT $3 \
         FOR UPDATE SKIP LOCKED",
    )
    .bind(tenant_id.as_uuid())
    .bind(now)
    .bind(limit)
    .fetch_all(conn)
    .await
}

/// Live Attempts past their execution deadline at `now` (ADR 0003: never retried).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn overdue_executions(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    now: DateTime<Utc>,
    limit: i64,
) -> sqlx::Result<Vec<AttemptRow>> {
    sqlx::query_as::<_, AttemptRow>(
        "SELECT * FROM attempts \
         WHERE tenant_id = $1 AND state IN ('leased', 'running') \
            AND execution_deadline IS NOT NULL AND execution_deadline <= $2 \
         ORDER BY execution_deadline \
         LIMIT $3 \
         FOR UPDATE SKIP LOCKED",
    )
    .bind(tenant_id.as_uuid())
    .bind(now)
    .bind(limit)
    .fetch_all(conn)
    .await
}

/// Every live Attempt currently leased to `worker_id`.
///
/// Used on session start so a reconnecting Worker's in-flight Attempts can be
/// resumed rather than abandoned (ADR 0003).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn live_for_worker(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    worker_id: WorkerId,
) -> sqlx::Result<Vec<AttemptRow>> {
    sqlx::query_as::<_, AttemptRow>(
        "SELECT * FROM attempts WHERE tenant_id = $1 AND worker_id = $2 AND state IN ('leased', 'running')",
    )
    .bind(tenant_id.as_uuid())
    .bind(worker_id.as_uuid())
    .fetch_all(conn)
    .await
}

/// Live Attempts whose Generation is `Cancelling`, i.e. cancellation was
/// requested but the Worker has not acknowledged it yet (ADR 0003).
///
/// The sweep that consumes this re-sends the cooperative `CancelRequest` every
/// tick, so a Worker that reconnects mid-cancellation still learns about it and
/// a Remote restart does not lose the intent.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn live_for_cancelling_generations(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    limit: i64,
) -> sqlx::Result<Vec<AttemptRow>> {
    sqlx::query_as::<_, AttemptRow>(
        "SELECT a.* FROM attempts a \
         JOIN generations g ON g.tenant_id = a.tenant_id AND g.id = a.generation_id \
         WHERE a.tenant_id = $1 AND a.state IN ('leased', 'running') \
           AND g.state = 'cancelling' \
         ORDER BY a.created_at \
         LIMIT $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(limit)
    .fetch_all(conn)
    .await
}

/// Records cooperative cancellation intent on a live Attempt.
///
/// Idempotent: returns `false` (not an error) when cancellation was already
/// requested or the Attempt is no longer live, matching ADR 0003's "repeated
/// cancellation is idempotent".
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn request_cancel(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: AttemptId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE attempts SET cancel_requested_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state IN ('leased', 'running') \
            AND cancel_requested_at IS NULL",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(state: &str) -> AttemptRow {
        let now = Utc::now();
        AttemptRow {
            id: Uuid::nil(),
            generation_id: Uuid::nil(),
            attempt_number: 1,
            state: state.to_owned(),
            worker_id: Uuid::nil(),
            pool_id: Uuid::nil(),
            slot_key: "slot-0".to_owned(),
            lease_expires_at: now,
            execution_deadline: None,
            created_at: now,
            started_at: None,
            last_heartbeat_at: None,
            cancel_requested_at: None,
        }
    }

    #[test]
    fn state_parses_persisted_names() {
        let Ok(state) = row("running").state() else {
            panic!("expected a parseable state");
        };
        assert_eq!(state, AttemptState::Running);
    }

    #[test]
    fn state_rejects_unknown_names() {
        assert!(row("bogus").state().is_err());
    }

    #[test]
    fn only_terminal_states_are_valid_finish_targets() {
        assert!(!AttemptState::Leased.is_terminal());
        assert!(!AttemptState::Running.is_terminal());
        assert!(AttemptState::Succeeded.is_terminal());
        assert!(AttemptState::Failed.is_terminal());
        assert!(AttemptState::Cancelled.is_terminal());
        assert!(AttemptState::LeaseExpired.is_terminal());
    }

    #[test]
    fn retry_decision_table_matches_adr_0003() {
        // Retryable causes requeue below MAX_ATTEMPTS with a remaining candidate.
        assert_eq!(
            FailureKind::WorkerLost.retry_decision(1, true),
            RetryDecision::Requeue
        );
        // The same cause fails once MAX_ATTEMPTS is reached.
        assert_eq!(
            FailureKind::WorkerLost.retry_decision(3, true),
            RetryDecision::Fail
        );
        // OutOfMemory fails once no candidate remains, even under the limit.
        assert_eq!(
            FailureKind::OutOfMemory.retry_decision(1, false),
            RetryDecision::Fail
        );
        // OutOfMemory still retries while another candidate remains.
        assert_eq!(
            FailureKind::OutOfMemory.retry_decision(1, true),
            RetryDecision::Requeue
        );
        // Non-retryable causes always fail immediately.
        assert_eq!(
            FailureKind::InvalidInput.retry_decision(1, true),
            RetryDecision::Fail
        );
    }
}
