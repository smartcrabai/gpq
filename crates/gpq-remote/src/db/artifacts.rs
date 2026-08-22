//! Persistence for Artifacts (ADR 0008).
//!
//! Every row mirrors the `artifacts` table from `migrations/0001_initial.sql`
//! and is scoped by `tenant_id` under Postgres RLS (ADR 0011). Mutations that
//! change `state` go through [`set_state`], which enforces
//! [`gpq_domain::ArtifactState::can_transition_to`] before writing, or through
//! the dedicated [`begin_delivery`] compare-and-swap for the one legal edge
//! the one-shot download route drives directly.

use chrono::{DateTime, Utc};
use gpq_domain::{
    ArtifactId, ArtifactManifest, ArtifactPlacement, ArtifactState, AttemptId, ContentHash,
    GenerationId, MediaKind, TenantId, TransitionError, WorkerId,
};
use sqlx::postgres::PgRow;
use sqlx::{FromRow, PgConnection, Row};
use uuid::Uuid;

/// Whether an Artifact feeds a Generation (`input`) or is produced by one
/// (`output`). Not a domain lifecycle concept, so it lives here rather than in
/// `gpq-domain`: it only shapes how this crate queries and cleans up rows.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ArtifactDirection {
    /// Consumed by the Generation; deleted at its terminal transition (ADR 0008).
    Input,
    /// Produced by an Attempt; delivered once, then expires (ADR 0008).
    Output,
}

impl ArtifactDirection {
    /// The stable name stored in `PostgreSQL`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

impl std::fmt::Display for ArtifactDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ArtifactDirection {
    type Err = gpq_domain::state::UnknownState;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "input" => Ok(Self::Input),
            "output" => Ok(Self::Output),
            other => Err(gpq_domain::state::UnknownState(other.to_owned())),
        }
    }
}

/// One persisted Artifact row. `tenant_id`, `generation_id`, `attempt_id`,
/// and the row's timestamps are RLS/query-scoping concerns the caller
/// already knows before fetching a row (it supplied `tenant`/`generation`/
/// `attempt` to find it), so they are not carried here — only the fields
/// actual callers read after the fetch are.
#[derive(Clone, Debug)]
pub struct ArtifactRow {
    /// Primary key.
    pub id: ArtifactId,
    /// Whether this is a Generation input or an Attempt output.
    pub direction: ArtifactDirection,
    /// Current lifecycle state (ADR 0008).
    pub state: ArtifactState,
    /// Where the bytes live.
    pub placement: ArtifactPlacement,
    /// Immutable size/digest/kind/MIME description.
    pub manifest: ArtifactManifest,
    /// Object storage key, set only when `placement` is `ObjectStore`.
    pub object_key: Option<String>,
    /// Producing Worker, set only when `placement` is `WorkerLocal`.
    pub worker_id: Option<WorkerId>,
    /// Opaque token authenticating a resumable Worker-local delivery.
    pub delivery_token: Option<String>,
    /// Bytes already accepted by a resumable transfer.
    pub committed_offset: u64,
}

/// Parses a stable database string into its typed representation.
fn decode<T>(value: &str) -> sqlx::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|err: T::Err| sqlx::Error::Decode(err.to_string().into()))
}

/// Narrows a non-negative `bigint` column into its `u64` domain type.
fn decode_u64(value: i64) -> sqlx::Result<u64> {
    u64::try_from(value).map_err(|err| sqlx::Error::Decode(err.to_string().into()))
}

impl FromRow<'_, PgRow> for ArtifactRow {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let id: Uuid = row.try_get("id")?;
        let direction: String = row.try_get("direction")?;
        let state: String = row.try_get("state")?;
        let placement: String = row.try_get("placement")?;
        let size_bytes: i64 = row.try_get("size_bytes")?;
        let digest_sha256: String = row.try_get("digest_sha256")?;
        let kind: String = row.try_get("kind")?;
        let mime_type: String = row.try_get("mime_type")?;
        let object_key: Option<String> = row.try_get("object_key")?;
        let worker_id: Option<Uuid> = row.try_get("worker_id")?;
        let delivery_token: Option<String> = row.try_get("delivery_token")?;
        let committed_offset: i64 = row.try_get("committed_offset")?;

        Ok(Self {
            id: ArtifactId::from_uuid(id),
            direction: decode(&direction)?,
            state: decode(&state)?,
            placement: decode(&placement)?,
            manifest: ArtifactManifest {
                size_bytes: decode_u64(size_bytes)?,
                digest: decode::<ContentHash>(&digest_sha256)?,
                kind: decode::<MediaKind>(&kind)?,
                mime_type,
            },
            object_key,
            worker_id: worker_id.map(WorkerId::from_uuid),
            delivery_token,
            committed_offset: decode_u64(committed_offset)?,
        })
    }
}

/// Errors from state-guarded Artifact mutations.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactStoreError {
    /// The underlying query failed.
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    /// The requested transition is illegal for the Artifact's current state.
    #[error(transparent)]
    Transition(#[from] TransitionError),
    /// No Artifact exists for this Tenant and id.
    #[error("artifact not found")]
    NotFound,
    /// The Artifact's state changed concurrently between load and update.
    #[error("artifact state changed concurrently")]
    Conflict,
}

/// Creates a `pending` input Artifact row ahead of its transfer. `object_key`
/// is required when `placement` is `ObjectStore` and ignored otherwise; the
/// caller chooses the key (it need not relate to the returned [`ArtifactId`],
/// which this function generates). The row is linked to a Generation later,
/// when one is submitted referencing it.
///
/// # Errors
/// Returns [`sqlx::Error::Decode`] if `manifest.size_bytes` overflows
/// `i64`, or [`sqlx::Error`] if the insert fails (e.g. connection loss).
pub async fn create_input(
    conn: &mut PgConnection,
    tenant: TenantId,
    manifest: &ArtifactManifest,
    placement: ArtifactPlacement,
    object_key: Option<&str>,
) -> sqlx::Result<ArtifactRow> {
    let id = ArtifactId::new();
    let size_bytes = i64::try_from(manifest.size_bytes)
        .map_err(|err| sqlx::Error::Decode(err.to_string().into()))?;
    sqlx::query_as::<_, ArtifactRow>(
        "INSERT INTO artifacts (
            tenant_id, id, direction, state, placement,
            size_bytes, digest_sha256, kind, mime_type, object_key
        ) VALUES ($1, $2, 'input', 'pending', $3, $4, $5, $6, $7, $8)
        RETURNING *",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .bind(placement.as_str())
    .bind(size_bytes)
    .bind(manifest.digest.to_string())
    .bind(manifest.kind.as_str())
    .bind(&manifest.mime_type)
    .bind(object_key)
    .fetch_one(conn)
    .await
}

/// Creates an already-`available` input Artifact for bytes relayed inline
/// through a connected request (ADR 0008: synchronous `OpenAI` image relay).
/// The bytes themselves are never persisted; only the manifest is recorded.
///
/// # Errors
/// Returns [`sqlx::Error::Decode`] if `manifest.size_bytes` overflows
/// `i64`, or [`sqlx::Error`] if the insert fails (e.g. connection loss).
pub async fn create_inline_input(
    conn: &mut PgConnection,
    tenant: TenantId,
    manifest: &ArtifactManifest,
) -> sqlx::Result<ArtifactRow> {
    let id = ArtifactId::new();
    let size_bytes = i64::try_from(manifest.size_bytes)
        .map_err(|err| sqlx::Error::Decode(err.to_string().into()))?;
    sqlx::query_as::<_, ArtifactRow>(
        "INSERT INTO artifacts (
            tenant_id, id, direction, state, placement,
            size_bytes, digest_sha256, kind, mime_type, available_at
        ) VALUES ($1, $2, 'input', 'available', 'inline_relay', $3, $4, $5, $6, now())
        RETURNING *",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .bind(size_bytes)
    .bind(manifest.digest.to_string())
    .bind(manifest.kind.as_str())
    .bind(&manifest.mime_type)
    .fetch_one(conn)
    .await
}

/// Records a completed output Artifact as `available`, expiring one hour
/// after completion unless claimed first (ADR 0008). `worker` and
/// `delivery_token` are only meaningful for `WorkerLocal` placement;
/// `object_key` only for `ObjectStore`.
///
/// # Errors
/// Returns [`sqlx::Error::Decode`] if `manifest.size_bytes` overflows
/// `i64`, or [`sqlx::Error`] if the insert fails (e.g. connection loss).
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the artifacts table's output-specific columns 1:1"
)]
pub async fn record_output(
    conn: &mut PgConnection,
    tenant: TenantId,
    generation: GenerationId,
    attempt: AttemptId,
    worker: Option<WorkerId>,
    manifest: &ArtifactManifest,
    placement: ArtifactPlacement,
    object_key: Option<&str>,
    delivery_token: Option<&str>,
) -> sqlx::Result<ArtifactRow> {
    let id = ArtifactId::new();
    let size_bytes = i64::try_from(manifest.size_bytes)
        .map_err(|err| sqlx::Error::Decode(err.to_string().into()))?;
    let ttl_seconds = gpq_domain::OUTPUT_ARTIFACT_TTL.as_secs_f64();
    sqlx::query_as::<_, ArtifactRow>(
        "INSERT INTO artifacts (
            tenant_id, id, generation_id, attempt_id, direction, state, placement,
            size_bytes, digest_sha256, kind, mime_type, object_key, worker_id, delivery_token,
            available_at, expires_at
        ) VALUES (
            $1, $2, $3, $4, 'output', 'available', $5,
            $6, $7, $8, $9, $10, $11, $12,
            now(), now() + make_interval(secs => $13)
        )
        RETURNING *",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .bind(generation.as_uuid())
    .bind(attempt.as_uuid())
    .bind(placement.as_str())
    .bind(size_bytes)
    .bind(manifest.digest.to_string())
    .bind(manifest.kind.as_str())
    .bind(&manifest.mime_type)
    .bind(object_key)
    .bind(worker.map(|w| w.as_uuid()))
    .bind(delivery_token)
    .bind(ttl_seconds)
    .fetch_one(conn)
    .await
}

/// Fetches one Artifact by id.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or the row cannot be decoded
/// into an [`ArtifactRow`] (see [`ArtifactRow::from_row`]).
pub async fn get(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
) -> sqlx::Result<Option<ArtifactRow>> {
    sqlx::query_as::<_, ArtifactRow>("SELECT * FROM artifacts WHERE tenant_id = $1 AND id = $2")
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(conn)
        .await
}

/// Lists just the output Artifacts attached to a Generation, oldest first —
/// what a Native `GetGeneration`/`ListGenerations`/`WatchGeneration` snapshot
/// reports as `output_artifacts`.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or any row cannot be decoded.
pub async fn list_outputs(
    conn: &mut PgConnection,
    tenant: TenantId,
    generation: GenerationId,
) -> sqlx::Result<Vec<ArtifactRow>> {
    sqlx::query_as::<_, ArtifactRow>(
        "SELECT * FROM artifacts
         WHERE tenant_id = $1 AND generation_id = $2 AND direction = 'output'
         ORDER BY created_at",
    )
    .bind(tenant.as_uuid())
    .bind(generation.as_uuid())
    .fetch_all(conn)
    .await
}

/// Lists just the input Artifacts attached to a Generation, oldest first —
/// what the scheduler assembles into a `LeaseAssignment`'s `LeaseInput` list.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or any row cannot be decoded.
pub async fn list_inputs(
    conn: &mut PgConnection,
    tenant: TenantId,
    generation: GenerationId,
) -> sqlx::Result<Vec<ArtifactRow>> {
    sqlx::query_as::<_, ArtifactRow>(
        "SELECT * FROM artifacts
         WHERE tenant_id = $1 AND generation_id = $2 AND direction = 'input'
         ORDER BY created_at",
    )
    .bind(tenant.as_uuid())
    .bind(generation.as_uuid())
    .fetch_all(conn)
    .await
}

/// Atomically transitions an Artifact's state, rejecting the update outright
/// when [`ArtifactState::can_transition_to`] disallows it and rejecting it at
/// the database when the state changed concurrently (compare-and-set on the
/// previous state).
///
/// # Errors
/// Returns [`ArtifactStoreError::NotFound`] if no Artifact exists for
/// `tenant`/`id`. Returns [`ArtifactStoreError::Transition`] if `to` is not
/// a legal edge from the Artifact's current state (ADR 0008's state
/// machine). Returns [`ArtifactStoreError::Conflict`] if the Artifact's
/// state changed between the initial read and this compare-and-set update
/// — losing that race means another writer already moved the Artifact on,
/// and the caller must re-read to see where. Returns
/// [`ArtifactStoreError::Sql`] on any other database failure.
pub async fn set_state(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
    to: ArtifactState,
) -> Result<ArtifactRow, ArtifactStoreError> {
    let Some(row) = get(conn, tenant, id).await? else {
        return Err(ArtifactStoreError::NotFound);
    };
    let from = row.state;
    from.transition(to)?;

    let now_available = to == ArtifactState::Available;
    let terminal = to.is_terminal();
    let result = sqlx::query(
        "UPDATE artifacts SET
            state = $1,
            available_at = CASE WHEN $2 THEN now() ELSE available_at END,
            terminated_at = CASE WHEN $3 THEN now() ELSE terminated_at END
         WHERE tenant_id = $4 AND id = $5 AND state = $6",
    )
    .bind(to.as_str())
    .bind(now_available)
    .bind(terminal)
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .bind(from.as_str())
    .execute(&mut *conn)
    .await?;

    if result.rows_affected() == 0 {
        return Err(ArtifactStoreError::Conflict);
    }
    get(conn, tenant, id)
        .await?
        .ok_or(ArtifactStoreError::NotFound)
}

/// Compare-and-swaps an Artifact from `available` to `delivering`. Returns
/// `false` (rather than an error) when it was not `available` — the caller
/// distinguishes "already delivering" from "terminal" by re-reading the row.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection lost).
/// Losing the `available` → `delivering` race is not an error: it returns
/// `Ok(false)`, and the caller must re-read the row to tell "already
/// delivering" apart from a terminal state.
pub async fn begin_delivery(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE artifacts SET state = 'delivering' WHERE tenant_id = $1 AND id = $2 AND state = 'available'",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Records how many bytes of a `delivering` Artifact have been accepted, so
/// an interrupted transfer can resume (ADR 0008). Never regresses the offset.
///
/// # Errors
/// Returns [`sqlx::Error::Decode`] if `offset` overflows `i64`, or
/// [`sqlx::Error`] if the update fails. An Artifact that is not
/// `delivering`, or an `offset` that does not advance
/// `committed_offset`, silently updates zero rows rather than erroring.
pub async fn commit_offset(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
    offset: u64,
) -> sqlx::Result<()> {
    let offset =
        i64::try_from(offset).map_err(|err| sqlx::Error::Decode(err.to_string().into()))?;
    sqlx::query(
        "UPDATE artifacts SET committed_offset = $1
         WHERE tenant_id = $2 AND id = $3 AND state = 'delivering' AND committed_offset < $1",
    )
    .bind(offset)
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .execute(conn)
    .await?;
    Ok(())
}

/// Marks a `delivering` Artifact `consumed`: its one-shot transfer completed.
///
/// # Errors
/// Returns [`ArtifactStoreError::NotFound`] if no Artifact exists for
/// `tenant`/`id`, [`ArtifactStoreError::Transition`] if the Artifact is not
/// currently `delivering` (only `delivering -> consumed` is a legal edge),
/// [`ArtifactStoreError::Conflict`] if its state changed concurrently, or
/// [`ArtifactStoreError::Sql`] on any other database failure (see
/// [`set_state`]).
pub async fn mark_consumed(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
) -> Result<(), ArtifactStoreError> {
    set_state(conn, tenant, id, ArtifactState::Consumed)
        .await
        .map(drop)
}

/// Marks an Artifact `lost`: its bytes are confirmed unrecoverable (e.g. the
/// producing Worker's state directory was lost, ADR 0008).
///
/// # Errors
/// Returns [`ArtifactStoreError::NotFound`] if no Artifact exists for
/// `tenant`/`id`, [`ArtifactStoreError::Transition`] if `lost` is not a
/// legal edge from the Artifact's current state,
/// [`ArtifactStoreError::Conflict`] if its state changed concurrently, or
/// [`ArtifactStoreError::Sql`] on any other database failure (see
/// [`set_state`]).
pub async fn mark_lost(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
) -> Result<(), ArtifactStoreError> {
    set_state(conn, tenant, id, ArtifactState::Lost)
        .await
        .map(drop)
}

/// Deletes every input Artifact attached to a Generation, returning the
/// deleted rows so the caller can clean up their underlying bytes. Called at
/// the Generation's terminal transition (ADR 0008: "Inputs are deleted when
/// the Generation terminates").
///
/// # Errors
/// Returns [`sqlx::Error`] if the delete fails or a deleted row cannot be
/// decoded.
pub async fn delete_inputs_for_generation(
    conn: &mut PgConnection,
    tenant: TenantId,
    generation: GenerationId,
) -> sqlx::Result<Vec<ArtifactRow>> {
    sqlx::query_as::<_, ArtifactRow>(
        "DELETE FROM artifacts
         WHERE tenant_id = $1 AND generation_id = $2 AND direction = 'input'
         RETURNING *",
    )
    .bind(tenant.as_uuid())
    .bind(generation.as_uuid())
    .fetch_all(conn)
    .await
}

/// Expires every past-due `pending` or `available` Artifact, returning the
/// expired rows so the caller can delete their underlying bytes (ADR 0008
/// keeps the database record but not the bytes).
///
/// `delivering` rows are deliberately excluded from this sweep: a
/// Worker-local delivery in progress owns its own terminal transition, and
/// sweeping it out from under an active transfer would mark `expired` a
/// Result that was still succeeding (ADR 0008: output Artifacts must live
/// through one delivery attempt or their expiry, not less than either). No
/// `delivering` row is stranded forever by this exclusion: every exit path
/// in `artifacts::stream_worker_local` that ends a Worker-local delivery
/// drives the Artifact to a terminal state, and that function's inter-chunk
/// silence timeout (`DELIVERY_CHUNK_TIMEOUT`) bounds the one case — a Worker
/// that accepted a `DeliverRequest` and then went quiet — that would
/// otherwise wait forever for that terminal transition.
///
/// `pending` and `available` both retain a legal `-> Expired` edge in
/// [`ArtifactState::can_transition_to`].
/// Scope this call to one Tenant at a time via a `begin_tenant` transaction
/// (ADR 0011); RLS then limits it to that Tenant's rows without a `tenant_id`
/// filter in this query.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails or a row cannot be decoded.
pub async fn expire_due(
    conn: &mut PgConnection,
    now: DateTime<Utc>,
) -> sqlx::Result<Vec<ArtifactRow>> {
    sqlx::query_as::<_, ArtifactRow>(
        "UPDATE artifacts SET state = 'expired', terminated_at = $1
         WHERE state IN ('pending', 'available')
           AND expires_at IS NOT NULL AND expires_at <= $1
         RETURNING *",
    )
    .bind(now)
    .fetch_all(conn)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_names_round_trip() {
        assert_eq!(
            "input"
                .parse::<ArtifactDirection>()
                .unwrap_or(ArtifactDirection::Output),
            ArtifactDirection::Input
        );
        assert_eq!(
            "output"
                .parse::<ArtifactDirection>()
                .unwrap_or(ArtifactDirection::Input),
            ArtifactDirection::Output
        );
        assert!("sideways".parse::<ArtifactDirection>().is_err());
    }

    #[test]
    fn decode_rejects_unknown_state_name() {
        let result: sqlx::Result<ArtifactState> = decode("not-a-state");
        assert!(matches!(result, Err(sqlx::Error::Decode(_))));
    }

    #[test]
    fn decode_u64_rejects_negative_bigint() {
        assert!(decode_u64(-1).is_err());
        assert_eq!(decode_u64(42).unwrap_or(0), 42);
    }
}
