//! Generation event log persistence (ADR 0008).
//!
//! Only state transitions, progress snapshots, and Attempt creation are
//! retained; token deltas and transport frames are not (ADR 0008 explicitly
//! excludes them from durable storage - they only ever travel through the
//! in-process [`crate::events::EventHub`]). Sequence numbers are monotonic
//! per Generation so a resumed reader can ask for everything strictly after
//! a known sequence without gaps or duplicates.

use chrono::{DateTime, Utc};
use gpq_domain::{AttemptId, DevicePoolId, GenerationId, TenantId, WorkerId};
use sqlx::PgConnection;

/// The kind of a persisted Generation event (ADR 0008).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    /// The Generation's lifecycle state changed.
    StateChanged,
    /// A new latest-progress snapshot was recorded.
    Progress,
    /// A new Attempt was created for the Generation.
    AttemptCreated,
}

impl EventKind {
    /// The stable name stored in `PostgreSQL`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StateChanged => "state_changed",
            Self::Progress => "progress",
            Self::AttemptCreated => "attempt_created",
        }
    }
}

/// One persisted Generation event, as replayed to a `WatchGeneration`
/// caller. `id` and `generation_id` are not projected: the row's own
/// identity is never surfaced past this layer, and the owning Generation is
/// already known to every caller (it is the query's own filter).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EventRow {
    /// Monotonic sequence number, unique per Generation.
    pub sequence: i64,
    /// The stable event kind name (`"state_changed"`, `"progress"`, or
    /// `"attempt_created"`).
    pub kind: String,
    /// Event-shaped payload; opaque JSON at this layer (ADR 0007/0008).
    pub payload: serde_json::Value,
    /// When the event was recorded; carried through to a replayed wire
    /// event's `emitted_at` so a replay reports the original time rather
    /// than the moment it was replayed.
    pub created_at: DateTime<Utc>,
}

/// Appends a Generation event, assigning it the next monotonic sequence
/// number for that Generation, and returns the assigned sequence.
///
/// Serializes concurrent appends for the same Generation by locking its
/// parent `generations` row first (the events table itself has no row to
/// lock before a Generation's first event), so callers must run this inside
/// a transaction that can see that row (ADR 0011: a tenant-scoped
/// transaction).
///
/// # Errors
/// Returns [`sqlx::Error`] if the row lock, sequence lookup, or insert
/// fails, e.g. on a lost connection. Two concurrent appends for the same
/// Generation cannot race to the same sequence: the `FOR UPDATE` lock on
/// the Generation's row serializes them.
pub async fn append(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
    kind: EventKind,
    payload: serde_json::Value,
) -> sqlx::Result<i64> {
    sqlx::query("SELECT 1 FROM generations WHERE tenant_id = $1 AND id = $2 FOR UPDATE")
        .bind(tenant_id.as_uuid())
        .bind(generation_id.as_uuid())
        .fetch_optional(&mut *conn)
        .await?;
    let (sequence,): (i64,) = sqlx::query_as(
        "SELECT coalesce(max(sequence), 0) + 1 FROM generation_events \
         WHERE tenant_id = $1 AND generation_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO generation_events (tenant_id, id, generation_id, sequence, kind, payload) \
         VALUES ($1, gen_random_uuid(), $2, $3, $4, $5)",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .bind(sequence)
    .bind(kind.as_str())
    .bind(payload)
    .execute(&mut *conn)
    .await?;
    Ok(sequence)
}

/// Appends an `attempt_created` audit event (ADR 0008) recording which
/// Worker and Device Pool a new Attempt was assigned to. Unlike
/// `state_changed`/`progress`, this kind has no [`crate::events::GenerationEvent`]
/// counterpart: it is never replayed to a Native `WatchGeneration` stream
/// (`crate::native::generation::WatchGeneration` only replays
/// `state_changed`/`progress`), only kept as durable history.
///
/// Runs the same locked-append as [`append`], so it must be called from a
/// transaction that can see the Generation's row.
///
/// # Errors
/// Returns [`sqlx::Error`] under the same conditions as [`append`], which
/// this delegates to.
pub async fn append_attempt_created(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
    attempt_id: AttemptId,
    attempt_number: i32,
    worker_id: WorkerId,
    pool_id: DevicePoolId,
) -> sqlx::Result<i64> {
    let payload = serde_json::json!({
        "attempt_id": attempt_id.as_uuid(),
        "attempt_number": attempt_number,
        "worker_id": worker_id.as_uuid(),
        "pool_id": pool_id.as_uuid(),
    });
    append(
        conn,
        tenant_id,
        generation_id,
        EventKind::AttemptCreated,
        payload,
    )
    .await
}

/// Loads every event of a Generation with a sequence strictly greater than
/// `after_sequence`, oldest first.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails.
pub async fn load_since(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
    after_sequence: i64,
) -> sqlx::Result<Vec<EventRow>> {
    sqlx::query_as(
        "SELECT sequence, kind, payload, created_at \
         FROM generation_events \
         WHERE tenant_id = $1 AND generation_id = $2 AND sequence > $3 \
         ORDER BY sequence",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .bind(after_sequence)
    .fetch_all(&mut *conn)
    .await
}

/// Loads the highest-sequence event of a Generation, if any.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails.
pub async fn latest(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
) -> sqlx::Result<Option<EventRow>> {
    sqlx::query_as(
        "SELECT sequence, kind, payload, created_at \
         FROM generation_events \
         WHERE tenant_id = $1 AND generation_id = $2 \
         ORDER BY sequence DESC \
         LIMIT 1",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await
}

#[cfg(test)]
mod tests {
    use super::EventKind;

    #[test]
    fn event_kind_names_match_the_schema_check_constraint() {
        assert_eq!(EventKind::StateChanged.as_str(), "state_changed");
        assert_eq!(EventKind::Progress.as_str(), "progress");
        assert_eq!(EventKind::AttemptCreated.as_str(), "attempt_created");
    }
}
