//! Persistence for Artifacts (ADR 0008).
//!
//! Every row mirrors the `artifacts` table from `migrations/0001_initial.sql`
//! (and the `content` column added by `migrations/0008_native_cutover.sql`)
//! and is scoped by `tenant_id` under Postgres RLS (ADR 0011). Mutations that
//! change `state` go through [`set_state`], which enforces
//! [`gpq_domain::ArtifactState::can_transition_to`] before writing, or through
//! the dedicated [`begin_delivery`] compare-and-swap for the one legal edge
//! the one-shot download route drives directly.
//!
//! Queries spell out their column lists rather than `SELECT *`: `content` holds
//! the bytes of a `remote_local` input and is only ever read in bounded slices
//! by [`remote_local_chunk`], never as part of an [`ArtifactRow`].

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

/// One persisted Artifact row, without its bytes. `tenant_id`, `generation_id`,
/// `attempt_id`, and the row's timestamps are RLS/query-scoping concerns the
/// caller already knows before fetching a row (it supplied `tenant`/
/// `generation`/`attempt` to find it), so they are not carried here — only the
/// fields actual callers read after the fetch are.
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
/// is required when `placement` is `ObjectStore` and ignored otherwise. The
/// row receives a one-hour database-clock TTL and is linked to a Generation
/// only after the requested input is ready.
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
            size_bytes, digest_sha256, kind, mime_type, object_key, expires_at
        ) VALUES ($1, $2, 'input', 'pending', $3, $4, $5, $6, $7, $8, now() + interval '1 hour')
        RETURNING id, direction, state, placement, size_bytes, digest_sha256, kind,
                  mime_type, object_key, worker_id, delivery_token, committed_offset",
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
/// Reads a bounded `remote_local` input chunk only while the assigned Worker
/// still owns an unexpired live Attempt linked to this Artifact.
///
/// # Errors
/// Returns [`sqlx::Error::Decode`] if `offset + 1` or either PostgreSQL `int4`
/// position/length conversion overflows; returns [`sqlx::Error`] if the query
/// fails. Missing, unavailable, or unauthorized chunks return `None`.
pub async fn remote_local_chunk(
    conn: &mut PgConnection,
    tenant: TenantId,
    attempt: AttemptId,
    worker: WorkerId,
    id: ArtifactId,
    offset: u64,
    max_len: u64,
) -> sqlx::Result<Option<Vec<u8>>> {
    let offset = offset
        .checked_add(1)
        .ok_or_else(|| sqlx::Error::Decode("remote-local input offset overflow".into()))?;
    let offset =
        i32::try_from(offset).map_err(|err| sqlx::Error::Decode(err.to_string().into()))?;
    let max_len =
        i32::try_from(max_len).map_err(|err| sqlx::Error::Decode(err.to_string().into()))?;
    // PostgreSQL bytea substring uses one-based int4 positions and lengths.
    // Recheck authorization with database time for every chunk.
    let row: Option<(Vec<u8>,)> = sqlx::query_as(
        "SELECT substring(a.content FROM $5 FOR $6) FROM artifacts a
         WHERE a.tenant_id = $1 AND a.id = $2 AND a.direction = 'input'
           AND a.placement = 'remote_local' AND a.state = 'available'
           AND a.content IS NOT NULL
           AND EXISTS (
               SELECT 1 FROM attempts t
               WHERE t.tenant_id = a.tenant_id AND t.id = $3
                 AND t.generation_id = a.generation_id AND t.worker_id = $4
                 AND t.state IN ('leased', 'running')
                 AND t.lease_expires_at > statement_timestamp()
                 AND EXISTS (
                     SELECT 1 FROM workers w
                     WHERE w.tenant_id = t.tenant_id AND w.id = t.worker_id
                       AND w.revoked_at IS NULL
                 )
           )",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .bind(attempt.as_uuid())
    .bind(worker.as_uuid())
    .bind(offset)
    .bind(max_len)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|(bytes,)| bytes))
}

/// Fetches an input Artifact if it belongs to the Generation of a live,
/// unexpired Attempt held by `worker`; stale, settled, expired, or unowned
/// attempts are intentionally indistinguishable from a missing artifact.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails. Stale, settled, expired, or
/// unowned attempts, and artifacts that do not match the query, return `None`.
pub async fn attempt_input(
    conn: &mut PgConnection,
    tenant: TenantId,
    attempt: AttemptId,
    worker: WorkerId,
    artifact: ArtifactId,
) -> sqlx::Result<Option<ArtifactRow>> {
    sqlx::query_as::<_, ArtifactRow>(
        "SELECT a.id, a.direction, a.state, a.placement, a.size_bytes, a.digest_sha256, a.kind,
                a.mime_type, a.object_key, a.worker_id, a.delivery_token, a.committed_offset
         FROM artifacts a
         WHERE a.tenant_id = $1 AND a.id = $2 AND a.direction = 'input'
           AND EXISTS (
               SELECT 1 FROM attempts t
               WHERE t.tenant_id = a.tenant_id AND t.id = $3
                 AND t.generation_id = a.generation_id AND t.worker_id = $4
                 AND t.state IN ('leased', 'running')
                 AND t.lease_expires_at > statement_timestamp()
                 AND EXISTS (
                     SELECT 1 FROM workers w
                     WHERE w.tenant_id = t.tenant_id AND w.id = t.worker_id
                       AND w.revoked_at IS NULL
                 )
           )",
    )
    .bind(tenant.as_uuid())
    .bind(artifact.as_uuid())
    .bind(attempt.as_uuid())
    .bind(worker.as_uuid())
    .fetch_optional(conn)
    .await
}

/// Creates a `pending` input Artifact whose bytes Remote stores itself
/// (`remote_local` placement). The bytes arrive via
/// [`set_remote_local_content`]; the row expires one hour after creation if
/// they never do.
///
/// # Errors
/// Returns [`sqlx::Error::Decode`] if `manifest.size_bytes` overflows
/// `i64`, or [`sqlx::Error`] if the insert fails (e.g. connection loss).
pub async fn create_remote_local_input(
    conn: &mut PgConnection,
    tenant: TenantId,
    manifest: &ArtifactManifest,
) -> sqlx::Result<(ArtifactRow, DateTime<Utc>)> {
    let id = ArtifactId::new();
    let size_bytes = i64::try_from(manifest.size_bytes)
        .map_err(|err| sqlx::Error::Decode(err.to_string().into()))?;
    // The expiry is stamped by the database clock, the single time source for
    // Artifact TTLs, and returned so callers never recompute it locally.
    let (expires_at,): (DateTime<Utc>,) = sqlx::query_as(
        "INSERT INTO artifacts (
            tenant_id, id, direction, state, placement,
            size_bytes, digest_sha256, kind, mime_type, expires_at
        ) VALUES ($1, $2, 'input', 'pending', 'remote_local', $3, $4, $5, $6, now() + interval '1 hour')
        RETURNING expires_at",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .bind(size_bytes)
    .bind(manifest.digest.to_string())
    .bind(manifest.kind.as_str())
    .bind(&manifest.mime_type)
    .fetch_one(&mut *conn)
    .await?;
    let row = get(conn, tenant, id)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    Ok((row, expires_at))
}

/// Stores the verified bytes of a pending `remote_local` input and marks it
/// `available`, expiring one hour from now unless a Generation claims it first.
/// The caller verifies size and SHA-256 against the stored manifest before
/// calling; this function only enforces the state guard.
///
/// # Errors
/// Returns [`ArtifactStoreError::NotFound`] if no Artifact exists for
/// `tenant`/`id`, [`ArtifactStoreError::Conflict`] if the row is no longer a
/// pending `remote_local` input or has already expired, or
/// [`ArtifactStoreError::Sql`] on any other database failure.
pub async fn set_remote_local_content(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
    bytes: &[u8],
) -> Result<ArtifactRow, ArtifactStoreError> {
    let result = sqlx::query(
        "UPDATE artifacts SET
            content = $1,
            state = 'available',
            available_at = now(),
            expires_at = now() + interval '1 hour'
         WHERE tenant_id = $2 AND id = $3
           AND direction = 'input' AND placement = 'remote_local'
           AND state = 'pending' AND expires_at > statement_timestamp()",
    )
    .bind(bytes)
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .execute(&mut *conn)
    .await?;
    if result.rows_affected() == 0 {
        return Err(match get(conn, tenant, id).await? {
            None => ArtifactStoreError::NotFound,
            Some(_) => ArtifactStoreError::Conflict,
        });
    }
    get(conn, tenant, id)
        .await?
        .ok_or(ArtifactStoreError::NotFound)
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
        RETURNING id, direction, state, placement, size_bytes, digest_sha256, kind,
                  mime_type, object_key, worker_id, delivery_token, committed_offset",
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
    sqlx::query_as::<_, ArtifactRow>(
        "SELECT id, direction, state, placement, size_bytes, digest_sha256, kind,
                mime_type, object_key, worker_id, delivery_token, committed_offset
         FROM artifacts WHERE tenant_id = $1 AND id = $2",
    )
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
        "SELECT id, direction, state, placement, size_bytes, digest_sha256, kind,
                mime_type, object_key, worker_id, delivery_token, committed_offset
         FROM artifacts
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
        "SELECT id, direction, state, placement, size_bytes, digest_sha256, kind,
                mime_type, object_key, worker_id, delivery_token, committed_offset
         FROM artifacts
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
/// previous state). Entering a terminal state also drops any stored
/// `remote_local` bytes.
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
            terminated_at = CASE WHEN $3 THEN now() ELSE terminated_at END,
            content = CASE WHEN $3 THEN NULL ELSE content END
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

/// Compare-and-swaps an unexpired, available output Artifact to `delivering`.
/// Expiry is checked against the database clock rather than the periodic sweep.
///
/// # Errors
/// Returns [`sqlx::Error`] if the update fails (e.g. connection loss).
pub async fn begin_delivery(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
) -> sqlx::Result<bool> {
    let result = sqlx::query(
        "UPDATE artifacts SET state = 'delivering'
         WHERE tenant_id = $1 AND id = $2 AND state = 'available'
           AND expires_at > statement_timestamp()",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .execute(conn)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Whether a failed acquisition was due to a database TTL that elapsed before
/// the periodic sweep.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails.
pub async fn delivery_expired(
    conn: &mut PgConnection,
    tenant: TenantId,
    id: ArtifactId,
) -> sqlx::Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM artifacts
            WHERE tenant_id = $1 AND id = $2 AND state = 'available'
              AND (expires_at IS NULL OR expires_at <= statement_timestamp())
        )",
    )
    .bind(tenant.as_uuid())
    .bind(id.as_uuid())
    .fetch_one(conn)
    .await
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
/// the Generation terminates"). A `remote_local` input's bytes are removed
/// with its row.
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
         RETURNING id, direction, state, placement, size_bytes, digest_sha256, kind,
                   mime_type, object_key, worker_id, delivery_token, committed_offset",
    )
    .bind(tenant.as_uuid())
    .bind(generation.as_uuid())
    .fetch_all(conn)
    .await
}

/// Expires every past-due `pending` or `available` Artifact, returning the
/// expired rows so the caller can delete their underlying bytes (ADR 0008
/// keeps the database record but not the bytes). Expired rows also drop any
/// stored `remote_local` content.
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
        "UPDATE artifacts SET state = 'expired', terminated_at = $1, content = NULL
         WHERE state IN ('pending', 'available')
           AND expires_at IS NOT NULL AND expires_at <= $1
         RETURNING id, direction, state, placement, size_bytes, digest_sha256, kind,
                   mime_type, object_key, worker_id, delivery_token, committed_offset",
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
