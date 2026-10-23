//! Generation persistence, listing, and the ADR 0003 result-acceptance
//! transaction (ADR 0002, ADR 0003, ADR 0008, ADR 0012, ADR 0013).
//!
//! Every function here operates on an already tenant-scoped connection
//! (`Db::begin_tenant`, ADR 0011): row-level security confines each statement
//! to the caller's Tenant. `tenant_id` still appears in every `WHERE` clause as
//! defense in depth and because the primary key is the composite
//! `(tenant_id, id)`.

use std::time::Duration;

use chrono::{DateTime, Utc};
use gpq_domain::{
    ArtifactPlacement, AttemptId, AttemptState, Candidate, ContentHash, DevicePoolId,
    ExecutionTarget, FailureKind, GenerationId, GenerationParameters, GenerationState, Modality,
    ModelManifest, Priority, Requirement, TenantId, WorkerId, generation::PriorityOutOfRange,
    hash::ContentHashError, state::UnknownState,
};
use serde_json::Value as Json;
use sqlx::PgConnection;
use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

/// One row of the `generations` table (see `migrations/0001_initial.sql`).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GenerationRow {
    /// Stored identity.
    pub id: Uuid,
    /// Persisted `GenerationState` name.
    pub state: String,
    /// Persisted `Modality` name.
    pub modality: String,
    /// The logical alias the caller requested.
    pub alias: String,
    /// The pinned Model Version's canonical manifest hash (ADR 0012).
    pub version_sha256: String,
    /// Opaque backend-shaped payload (ADR 0007).
    pub parameters: Json,
    /// Requested priority, zero through nine.
    pub priority: i16,
    /// Optional deterministic seed.
    pub seed: Option<i64>,
    /// Resolved Attempt execution timeout (ADR 0003).
    pub execution_timeout: PgInterval,
    /// Persisted `ArtifactPlacement` name for outputs.
    pub output_placement: String,
    /// Attempts created so far, at most `MAX_ATTEMPTS` (ADR 0003).
    pub attempt_count: i32,
    /// Latest progress snapshot (ADR 0008 retains only the latest).
    pub latest_progress: Option<Json>,
    /// Persisted `FailureKind` name, when failed.
    pub failure_kind: Option<String>,
    /// Raw diagnostic failure text.
    pub failure_message: String,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// Last state or progress update.
    pub updated_at: DateTime<Utc>,
}

/// Failure to interpret a persisted column as its domain type.
#[derive(Debug, thiserror::Error)]
pub enum GenerationRowError {
    /// A state, modality, or failure-kind column held an unknown name.
    #[error(transparent)]
    State(#[from] UnknownState),
    /// `version_sha256` was not a valid hex-encoded SHA-256 digest.
    #[error(transparent)]
    Hash(#[from] ContentHashError),
    /// `priority` decoded but fell outside the accepted range.
    #[error(transparent)]
    Priority(#[from] PriorityOutOfRange),
    /// `priority` did not fit a `u8` at all.
    #[error("priority {0} is outside 0..=9")]
    PriorityRange(i16),
}

impl GenerationRow {
    /// Typed identity of this Generation.
    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        GenerationId::from_uuid(self.id)
    }

    /// Parses the persisted `GenerationState`.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownState`] if the persisted `state` column holds a name
    /// that is not a recognized [`GenerationState`] variant.
    pub fn state(&self) -> Result<GenerationState, UnknownState> {
        self.state.parse()
    }

    /// Parses the persisted `Modality`.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownState`] if the persisted `modality` column holds a
    /// name that is not a recognized [`Modality`] variant.
    pub fn modality(&self) -> Result<Modality, UnknownState> {
        self.modality.parse()
    }

    /// Parses the pinned version hash.
    ///
    /// # Errors
    ///
    /// Returns [`ContentHashError`] if `version_sha256` is not a valid
    /// hex-encoded SHA-256 digest.
    pub fn version(&self) -> Result<ContentHash, ContentHashError> {
        self.version_sha256.parse()
    }

    /// Parses the requested priority.
    ///
    /// # Errors
    ///
    /// Returns [`GenerationRowError::PriorityRange`] if the persisted
    /// `priority` does not fit a `u8`, or [`GenerationRowError::Priority`]
    /// if it fits but falls outside the accepted `0..=9` range.
    pub fn priority(&self) -> Result<Priority, GenerationRowError> {
        let raw = u8::try_from(self.priority)
            .map_err(|_| GenerationRowError::PriorityRange(self.priority))?;
        Ok(Priority::new(raw)?)
    }

    /// Parses the persisted failure, if any.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownState`] if `failure_kind` is set but holds a name
    /// that is not a recognized [`FailureKind`] variant.
    pub fn failure(&self) -> Result<Option<(FailureKind, &str)>, UnknownState> {
        match &self.failure_kind {
            Some(kind) => Ok(Some((kind.parse()?, self.failure_message.as_str()))),
            None => Ok(None),
        }
    }
}

/// Fields required to admit a new Generation (ADR 0002, ADR 0006, ADR 0012).
#[derive(Debug, Clone)]
pub struct NewGeneration {
    /// Pre-generated identity (`UUIDv7`, time-ordered).
    pub id: GenerationId,
    /// Owning Tenant.
    pub tenant_id: TenantId,
    /// The logical alias the caller requested.
    pub alias: String,
    /// The pinned Model Version and derived modality (ADR 0012).
    pub target: ExecutionTarget,
    /// Typed `GenerationParameters` JSON (ADR 0007).
    pub parameters: Json,
    /// Resolved priority (the Tenant default is already applied by the caller).
    pub priority: Priority,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
    /// Resolved Attempt execution timeout (ADR 0003).
    pub execution_timeout: Duration,
    /// Where output Artifacts land.
    pub output_placement: ArtifactPlacement,
}

/// Failure to insert a new Generation.
#[derive(Debug, thiserror::Error)]
pub enum InsertGenerationError {
    /// `seed` did not fit the signed 64-bit `seed` column.
    #[error("seed {0} does not fit a signed 64-bit column")]
    SeedOutOfRange(u64),
    /// A database error occurred, including an execution timeout too large for
    /// a `PostgreSQL` `interval` (astronomically unlikely given ADR 0006's
    /// tenant-configurable ceiling, but not `unwrap`-away-able).
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Inserts a new `Queued` Generation (ADR 0002's scheduler then picks it up).
///
/// # Errors
///
/// Returns [`InsertGenerationError::SeedOutOfRange`] if `seed` does not fit
/// a signed 64-bit column. Returns [`InsertGenerationError::Database`] if
/// the execution timeout does not fit a `PostgreSQL` interval, or the
/// insert itself fails.
pub async fn insert(
    conn: &mut PgConnection,
    new: NewGeneration,
) -> Result<GenerationRow, InsertGenerationError> {
    let seed = match new.seed {
        Some(value) => {
            Some(i64::try_from(value).map_err(|_| InsertGenerationError::SeedOutOfRange(value))?)
        }
        None => None,
    };
    let execution_timeout: PgInterval = new
        .execution_timeout
        .try_into()
        .map_err(sqlx::Error::Configuration)?;

    let row = sqlx::query_as::<_, GenerationRow>(
        "INSERT INTO generations \
            (tenant_id, id, state, modality, alias, version_sha256, \
             parameters, priority, seed, execution_timeout, output_placement) \
         VALUES ($1, $2, 'queued', $3, $4, $5, $6, $7, $8, $9, $10) \
         RETURNING *",
    )
    .bind(new.tenant_id.as_uuid())
    .bind(new.id.as_uuid())
    .bind(new.target.modality().as_str())
    .bind(&new.alias)
    .bind(new.target.version().to_hex())
    .bind(new.parameters)
    .bind(i16::from(new.priority))
    .bind(seed)
    .bind(execution_timeout)
    .bind(new.output_placement.as_str())
    .fetch_one(conn)
    .await?;
    Ok(row)
}

/// Fetches one Generation by id.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails or the row cannot be decoded
/// into a [`GenerationRow`].
pub async fn get(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
) -> sqlx::Result<Option<GenerationRow>> {
    sqlx::query_as::<_, GenerationRow>("SELECT * FROM generations WHERE tenant_id = $1 AND id = $2")
        .bind(tenant_id.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(conn)
        .await
}

/// Lists Generations newest-first, keyset-paginated by id.
///
/// `UUIDv7` identities sort consistently with creation time (ADR 0017), so `id`
/// alone is a stable, collision-free cursor: `after` is an exclusive cursor,
/// the id of the last item on the previous page.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails or a row cannot be decoded
/// into a [`GenerationRow`].
pub async fn list(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    page_size: u32,
    after: Option<GenerationId>,
    state_filter: Option<GenerationState>,
) -> sqlx::Result<Vec<GenerationRow>> {
    let limit = i64::from(page_size);
    sqlx::query_as::<_, GenerationRow>(
        "SELECT * FROM generations WHERE tenant_id = $1 \
         AND ($2::uuid IS NULL OR id < $2) \
         AND ($3::text IS NULL OR state = $3) \
         ORDER BY id DESC LIMIT $4",
    )
    .bind(tenant_id.as_uuid())
    .bind(after.map(|id| id.as_uuid()))
    .bind(state_filter.as_ref().map(GenerationState::as_str))
    .bind(limit)
    .fetch_all(conn)
    .await
}

/// Counts every nonterminal Generation of a Tenant (ADR 0006's queue capacity).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn count_nonterminal(conn: &mut PgConnection, tenant_id: TenantId) -> sqlx::Result<i64> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM generations \
         WHERE tenant_id = $1 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'expired')",
    )
    .bind(tenant_id.as_uuid())
    .fetch_one(conn)
    .await?;
    Ok(count)
}

/// `Queued -> Running`, recording `started_at` when a Slot leases the first Attempt.
///
/// Called by the scheduler immediately after creating the first Attempt
/// (`RemoteQueue`'s chosen invariant: the Generation optimistically becomes
/// `Running` as soon as it is leased, without waiting for the Worker's
/// `AttemptRunning` confirmation — that confirmation instead drives the
/// *Attempt's* own `Leased -> Running` transition, see `db::attempts::mark_running`).
/// `false` means the Generation was not `Queued`; treat it as already handled.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn mark_running(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET state = 'running', started_at = $3, updated_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state = 'queued'",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// `Running -> Queued`: the sole backward edge, taken after a retryable Attempt
/// failure so another Slot can lease a fresh Attempt (ADR 0003).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn requeue(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET state = 'queued', updated_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state = 'running'",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Outcome of the ADR 0003 result-acceptance compare-and-set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// This Attempt's result became the Accepted Result of the named Generation.
    Accepted(GenerationId),
    /// This Attempt is the already-accepted result; its persisted output must be preserved.
    AcceptedReplay,
    /// The Attempt no longer holds a live lease; the Worker must delete its output.
    StaleLease,
    /// Another Attempt already committed the Accepted Result.
    AlreadyAccepted,
    /// The Generation cannot accept a result in its current state (e.g. it was
    /// cancelled or expired while this Attempt was executing).
    Terminal,
}

/// Verifies the live lease, a nonterminal Generation, and the absence of a
/// prior Accepted Result, then atomically settles the Attempt and Generation
/// as succeeded and releases the Attempt's Execution Slot (ADR 0003, ADR
/// 0005). Rejection never mutates anything.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if any of the locking selects or the settling
/// updates fail. Rejection outcomes ([`AcceptOutcome::StaleLease`],
/// [`AcceptOutcome::AlreadyAccepted`], [`AcceptOutcome::Terminal`]) are
/// returned as `Ok`, not as errors: this compare-and-set never mutates
/// anything when it rejects (ADR 0003).
pub async fn accept_result(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    attempt_id: AttemptId,
    worker_id: WorkerId,
    now: DateTime<Utc>,
) -> sqlx::Result<AcceptOutcome> {
    #[derive(sqlx::FromRow)]
    struct AttemptLock {
        generation_id: Uuid,
        pool_id: Uuid,
        state: String,
        lease_expires_at: DateTime<Utc>,
    }
    #[derive(sqlx::FromRow)]
    struct GenerationLock {
        state: String,
        accepted_attempt_id: Option<Uuid>,
    }

    let Some(attempt) = sqlx::query_as::<_, AttemptLock>(
        "SELECT generation_id, pool_id, state, lease_expires_at FROM attempts \
         WHERE tenant_id = $1 AND id = $2 AND worker_id = $3 FOR UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(worker_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(AcceptOutcome::StaleLease);
    };

    let attempt_state: AttemptState = attempt.state.parse().unwrap_or(AttemptState::LeaseExpired);

    let Some(generation) = sqlx::query_as::<_, GenerationLock>(
        "SELECT state, accepted_attempt_id FROM generations WHERE tenant_id = $1 AND id = $2 FOR UPDATE",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt.generation_id)
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(AcceptOutcome::Terminal);
    };

    if attempt_state == AttemptState::Succeeded
        && generation.accepted_attempt_id == Some(attempt_id.as_uuid())
    {
        return Ok(AcceptOutcome::AcceptedReplay);
    }
    if !attempt_state.is_live() || attempt.lease_expires_at <= now {
        return Ok(AcceptOutcome::StaleLease);
    }
    if generation.accepted_attempt_id.is_some() {
        return Ok(AcceptOutcome::AlreadyAccepted);
    }
    let gen_state: GenerationState = generation.state.parse().unwrap_or(GenerationState::Failed);
    if !gen_state.can_transition_to(GenerationState::Succeeded) {
        return Ok(AcceptOutcome::Terminal);
    }

    sqlx::query(
        "UPDATE attempts SET state = 'succeeded', finished_at = $4 \
         WHERE tenant_id = $1 AND id = $2 AND worker_id = $3",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt_id.as_uuid())
    .bind(worker_id.as_uuid())
    .bind(now)
    .execute(&mut *conn)
    .await?;
    crate::db::workers::release_slot(conn, tenant_id, DevicePoolId::from_uuid(attempt.pool_id))
        .await?;

    let generation_id = GenerationId::from_uuid(attempt.generation_id);
    sqlx::query(
        "UPDATE generations SET state = 'succeeded', accepted_attempt_id = $3, \
            terminated_at = $4, updated_at = $4 \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(attempt.generation_id)
    .bind(attempt_id.as_uuid())
    .bind(now)
    .execute(&mut *conn)
    .await?;

    Ok(AcceptOutcome::Accepted(generation_id))
}

/// Settles the Generation as `Failed` from any nonterminal state (ADR 0003).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn fail(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    kind: FailureKind,
    message: &str,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET state = 'failed', failure_kind = $3, failure_message = $4, \
            terminated_at = $5, updated_at = $5 \
         WHERE tenant_id = $1 AND id = $2 AND state IN ('queued', 'running', 'cancelling')",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(kind.as_str())
    .bind(message)
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// `Queued -> Cancelled`: queued cancellation terminates immediately (ADR 0003).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn cancel_queued(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET state = 'cancelled', terminated_at = $3, updated_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state = 'queued'",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// `Running -> Cancelling`: running cancellation awaits Worker acknowledgement
/// (ADR 0003). Idempotent: a Generation already `Cancelling` returns `false`.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn request_cancel_running(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET state = 'cancelling', updated_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state = 'running'",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// `Cancelling -> Cancelled`: the Worker acknowledged cooperative cancellation
/// (ADR 0003). Cancellation acknowledgement and result commitment race through
/// this same kind of compare-and-set, so `false` means this call lost the race.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn finish_cancelled(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET state = 'cancelled', terminated_at = $3, updated_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state = 'cancelling'",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// `Queued -> Expired`: the starvation guard's terminal outcome (ADR 0002),
/// taken only when no registered candidate can ever run the Generation.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn expire(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET state = 'expired', terminated_at = $3, updated_at = $3 \
         WHERE tenant_id = $1 AND id = $2 AND state = 'queued'",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Records the latest progress snapshot of an active Generation as raw JSON
/// (ADR 0008 retains only the latest, never token deltas or transport frames).
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the update fails.
pub async fn update_progress(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    progress: Json,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE generations SET latest_progress = $3, updated_at = now() \
         WHERE tenant_id = $1 AND id = $2 AND state IN ('running', 'cancelling')",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .bind(progress)
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// A backend-reported progress snapshot (ADR 0007), grouped so
/// [`record_progress`] stays under clippy's argument-count ceiling.
pub struct ProgressSnapshot<'a> {
    /// Fraction complete, `0.0..=1.0`.
    pub fraction: f64,
    /// A human-readable stage name.
    pub stage: &'a str,
    /// The current step, for step-counted backends.
    pub step: u32,
    /// The total number of steps, for step-counted backends.
    pub total_steps: u32,
}

/// Convenience wrapper over [`update_progress`] building the JSON shape
/// (`fraction`, `stage`, `step`, `total_steps`, `observed_at`) other modules
/// (e.g. Native's event stream) deserialize back into `gpq.v1.Progress`.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the underlying [`update_progress`] call fails.
pub async fn record_progress(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
    snapshot: ProgressSnapshot<'_>,
    now: DateTime<Utc>,
) -> sqlx::Result<bool> {
    let progress = serde_json::json!({
        "fraction": snapshot.fraction,
        "stage": snapshot.stage,
        "step": snapshot.step,
        "total_steps": snapshot.total_steps,
        "observed_at": now.to_rfc3339(),
    });
    update_progress(conn, tenant_id, id, progress).await
}

/// Why one queued or requeued Generation cannot be rebuilt into a [`Requirement`].
#[derive(Debug, thiserror::Error)]
enum CandidateRowError {
    #[error(transparent)]
    Hash(#[from] ContentHashError),
    #[error(transparent)]
    Priority(#[from] PriorityOutOfRange),
    #[error("priority {0} is outside 0..=9")]
    PriorityRange(i16),
    #[error("generation {0} pins a model version that is not registered")]
    MissingManifest(Uuid),
    #[error("model manifest for generation {generation} failed to parse: {source}")]
    Manifest {
        generation: Uuid,
        #[source]
        source: serde_json::Error,
    },
    #[error("generation parameters for {generation} failed to parse: {source}")]
    Parameters {
        generation: Uuid,
        #[source]
        source: serde_json::Error,
    },
    #[error("generation {generation} cannot be scheduled: {reason}")]
    Requirement { generation: Uuid, reason: String },
}

/// Rebuilds one Generation's [`Requirement`] from its stored typed parameters
/// and the immutable manifest its pinned version names (ADR 0012).
fn requirement_from_parts(
    tenant_id: TenantId,
    generation_id: Uuid,
    version_sha256: &str,
    parameters: Json,
    manifest: Option<Json>,
) -> Result<Requirement, CandidateRowError> {
    let version: ContentHash = version_sha256.parse()?;
    let manifest_json = manifest.ok_or(CandidateRowError::MissingManifest(generation_id))?;
    let manifest: ModelManifest =
        serde_json::from_value(manifest_json).map_err(|source| CandidateRowError::Manifest {
            generation: generation_id,
            source,
        })?;
    let parameters: GenerationParameters =
        serde_json::from_value(parameters).map_err(|source| CandidateRowError::Parameters {
            generation: generation_id,
            source,
        })?;
    Requirement::for_generation(tenant_id, version, &manifest, &parameters).map_err(|error| {
        CandidateRowError::Requirement {
            generation: generation_id,
            reason: error.to_string(),
        }
    })
}

/// One queued Generation joined against its pinned Model Version.
#[derive(sqlx::FromRow)]
struct CandidateRow {
    id: Uuid,
    created_at: DateTime<Utc>,
    priority: i16,
    version_sha256: String,
    parameters: Json,
    manifest: Option<Json>,
}

impl CandidateRow {
    fn into_candidate(self, tenant_id: TenantId) -> Result<Candidate, CandidateRowError> {
        let priority_raw = u8::try_from(self.priority)
            .map_err(|_| CandidateRowError::PriorityRange(self.priority))?;
        let priority = Priority::new(priority_raw)?;
        let requirement = requirement_from_parts(
            tenant_id,
            self.id,
            &self.version_sha256,
            self.parameters,
            self.manifest,
        )?;
        Ok(Candidate {
            generation_id: GenerationId::from_uuid(self.id),
            created_at: self.created_at,
            priority,
            requirement,
        })
    }
}

/// The Tenant's queued Generations offered to the scheduler (ADR 0002, ADR 0013).
///
/// A malformed row is logged and skipped rather than failing the whole
/// scheduling pass for every other Generation of the Tenant.
///
/// # Errors
///
/// Returns [`sqlx::Error`] if the query fails.
pub async fn queued_candidates(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    limit: i64,
) -> sqlx::Result<Vec<Candidate>> {
    let rows = sqlx::query_as::<_, CandidateRow>(
        "SELECT g.id, g.created_at, g.priority, g.version_sha256, g.parameters, \
            mv.manifest \
         FROM generations g \
         LEFT JOIN model_versions mv \
             ON mv.tenant_id = g.tenant_id AND mv.content_sha256 = g.version_sha256 \
         WHERE g.tenant_id = $1 AND g.state = 'queued' \
         ORDER BY g.created_at \
         LIMIT $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(limit)
    .fetch_all(conn)
    .await?;

    Ok(rows
        .into_iter()
        .filter_map(|row| match row.into_candidate(tenant_id) {
            Ok(candidate) => Some(candidate),
            Err(error) => {
                tracing::warn!(%tenant_id, %error, "skipping malformed queued candidate");
                None
            }
        })
        .collect())
}

/// Rebuilds the [`Requirement`] of one Generation regardless of its state.
/// Used by the retry path (`db::attempts::record_failure`) to decide whether
/// any other registered Slot can still run it (ADR 0003).
pub(crate) async fn requirement_of(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    id: GenerationId,
) -> sqlx::Result<Option<Requirement>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        version_sha256: String,
        parameters: Json,
        manifest: Option<Json>,
    }
    let Some(row) = sqlx::query_as::<_, Row>(
        "SELECT g.version_sha256, g.parameters, mv.manifest \
         FROM generations g \
         LEFT JOIN model_versions mv \
             ON mv.tenant_id = g.tenant_id AND mv.content_sha256 = g.version_sha256 \
         WHERE g.tenant_id = $1 AND g.id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(id.as_uuid())
    .fetch_optional(conn)
    .await?
    else {
        return Ok(None);
    };

    match requirement_from_parts(
        tenant_id,
        id.as_uuid(),
        &row.version_sha256,
        row.parameters,
        row.manifest,
    ) {
        Ok(requirement) => Ok(Some(requirement)),
        Err(error) => {
            tracing::warn!(%tenant_id, %id, %error, "failed to rebuild requirement for generation");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_outcome_variants_are_distinct() {
        let generation = GenerationId::new();
        assert_ne!(
            AcceptOutcome::Accepted(generation),
            AcceptOutcome::StaleLease
        );
        assert_ne!(AcceptOutcome::AlreadyAccepted, AcceptOutcome::Terminal);
    }
}
