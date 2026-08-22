//! Tenant lifecycle, Tenant Master Keys, and per-Tenant settings.
//!
//! ADR 0009: Tenant creation, key rotation, and deletion are local
//! administration operations; Master Keys are stored only as keyed hashes.
//! ADR 0006: queue age, capacity, Artifact limits, timeout ceilings, and
//! default priority are the Tenant settings a Master-Key-authenticated Tenant
//! service can read and mutate. ADR 0011: every statement here either scopes
//! the `gpq.tenant_id` GUC to the Tenant it is about to touch (so the forced
//! row-level security policy in the migration is satisfied even for the
//! administration paths that don't already know a caller's Tenant) or, for
//! [`list_ids`] and [`list_tenants`], runs inside the one legitimate
//! cross-tenant administration transaction ([`crate::db::Db::begin`]).

use std::time::Duration;

use chrono::{DateTime, Utc};
use gpq_domain::{Priority, TenantId, TenantSettings};
use sqlx::PgConnection;
use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

/// A Tenant as returned by [`list_tenants`].
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct TenantSummary {
    #[sqlx(try_from = "Uuid")]
    pub id: TenantId,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

/// A Tenant Master Key as returned by [`list_master_keys`], never including
/// the secret itself (only its keyed hash is ever stored, ADR 0009).
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct MasterKeySummary {
    pub id: Uuid,
    pub label: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Sets the `gpq.tenant_id` GUC for the rest of this transaction, satisfying
/// the forced row-level security policy for a statement that already knows
/// which Tenant it targets (ADR 0011).
async fn scope_to_tenant(conn: &mut PgConnection, tenant: TenantId) -> sqlx::Result<()> {
    sqlx::query("SELECT set_config('gpq.tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(conn)
        .await?;
    Ok(())
}

/// Creates a Tenant with default settings and returns its fresh id.
///
/// # Errors
/// Returns an error if the insert fails (e.g. a duplicate name).
pub async fn create_tenant(conn: &mut PgConnection, name: &str) -> sqlx::Result<TenantId> {
    let id = TenantId::new();
    // The row does not exist yet, so nothing satisfies `WITH CHECK (id =
    // gpq_current_tenant())` until the GUC is scoped to the id we are about
    // to insert.
    scope_to_tenant(conn, id).await?;
    sqlx::query("INSERT INTO tenants (id, name) VALUES ($1, $2)")
        .bind(id.as_uuid())
        .bind(name)
        .execute(conn)
        .await?;
    Ok(id)
}

/// Soft-deletes a Tenant. Idempotent: deleting an already-deleted or unknown
/// Tenant affects zero rows without error.
///
/// # Errors
/// Returns an error if the query fails.
pub async fn delete_tenant(conn: &mut PgConnection, tenant: TenantId) -> sqlx::Result<()> {
    scope_to_tenant(conn, tenant).await?;
    sqlx::query("UPDATE tenants SET deleted_at = now() WHERE id = $1 AND deleted_at IS NULL")
        .bind(tenant.as_uuid())
        .execute(conn)
        .await?;
    Ok(())
}

/// Lists every Tenant, including soft-deleted ones. Cross-tenant by nature;
/// callers must run this inside an administration transaction
/// ([`crate::db::Db::begin`]).
///
/// # Errors
/// Returns an error if the query fails.
pub async fn list_tenants(conn: &mut PgConnection) -> sqlx::Result<Vec<TenantSummary>> {
    sqlx::query_as("SELECT id, name, created_at, deleted_at FROM tenants ORDER BY created_at")
        .fetch_all(conn)
        .await
}

/// Lists the ids of every live (not soft-deleted) Tenant, for the scheduler's
/// and expiry sweep's periodic fallback tick (ADR 0002, ADR 0013), which must
/// visit every Tenant even though `LISTEN/NOTIFY` only names one at a time.
/// Cross-tenant by nature; callers must run this inside an administration
/// transaction ([`crate::db::Db::begin`]).
///
/// # Errors
/// Returns an error if the query fails.
pub async fn list_ids(conn: &mut PgConnection) -> sqlx::Result<Vec<TenantId>> {
    // Forced RLS hides `tenants` from the serving role until a Tenant is
    // already scoped, so enumeration goes through the definer-rights function
    // from migration `0003_tenant_enumeration` (ADR 0011).
    let rows: Vec<(Uuid,)> = sqlx::query_as("SELECT gpq_active_tenants() AS id")
        .fetch_all(conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id,)| TenantId::from_uuid(id))
        .collect())
}

/// Issues a new Tenant Master Key digest for `tenant`, returning the key's
/// id. The caller is responsible for generating the secret
/// ([`crate::auth::generate_secret`]) and hashing it
/// ([`crate::auth::KeyedHasher::hash`]) before calling this, and for
/// displaying the secret to the operator exactly once — only the digest is
/// ever persisted (ADR 0009).
///
/// # Errors
/// Returns an error if the insert fails.
pub async fn insert_master_key(
    conn: &mut PgConnection,
    tenant: TenantId,
    digest: &[u8],
    label: &str,
    expires_at: Option<DateTime<Utc>>,
) -> sqlx::Result<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenant_master_keys (tenant_id, id, key_hash, label, expires_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(tenant.as_uuid())
    .bind(id)
    .bind(digest)
    .bind(label)
    .bind(expires_at)
    .execute(conn)
    .await?;
    Ok(id)
}

/// Revokes a Tenant Master Key. Idempotent: revoking an already-revoked or
/// unknown key affects zero rows without error. Rotation keeps the old key
/// live until this is called, permitting temporary old/new overlap (ADR 0009).
///
/// # Errors
/// Returns an error if the query fails.
pub async fn revoke_master_key(
    conn: &mut PgConnection,
    tenant: TenantId,
    key_id: Uuid,
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE tenant_master_keys SET revoked_at = now() \
         WHERE tenant_id = $1 AND id = $2 AND revoked_at IS NULL",
    )
    .bind(tenant.as_uuid())
    .bind(key_id)
    .execute(conn)
    .await?;
    Ok(())
}

/// Lists every Master Key of `tenant`, live and revoked, oldest first.
///
/// # Errors
/// Returns an error if the query fails.
pub async fn list_master_keys(
    conn: &mut PgConnection,
    tenant: TenantId,
) -> sqlx::Result<Vec<MasterKeySummary>> {
    sqlx::query_as(
        "SELECT id, label, created_at, expires_at, revoked_at FROM tenant_master_keys \
         WHERE tenant_id = $1 ORDER BY created_at",
    )
    .bind(tenant.as_uuid())
    .fetch_all(conn)
    .await
}

/// Row shape matching the mutable settings columns of `tenants`.
#[derive(sqlx::FromRow)]
struct TenantSettingsRow {
    maximum_queue_age: PgInterval,
    max_queued_generations: i32,
    max_input_artifact_bytes: i64,
    max_output_artifact_bytes: i64,
    execution_timeout_ceiling: PgInterval,
    default_priority: i16,
}

// `load_settings` and `settings` differ only in whether a soft-deleted Tenant
// is treated as present (`load_settings`) or as `None` (`settings`); see each
// function's doc comment for why that distinction is preserved rather than
// merged away. Their column lists are spelled out twice because sqlx 0.9 only
// accepts static SQL strings, and a shared literal would cost more machinery
// than the two lines it saves.

/// Reads `tenant`'s mutable settings (ADR 0006).
///
/// # Errors
/// Returns an error if the query fails, the Tenant does not exist, or a
/// stored value cannot be represented as a [`TenantSettings`] (see
/// [`crate::db::interval_to_duration`]).
pub async fn load_settings(
    conn: &mut PgConnection,
    tenant: TenantId,
) -> sqlx::Result<TenantSettings> {
    let row: TenantSettingsRow = sqlx::query_as(
        "SELECT maximum_queue_age, max_queued_generations, \
         max_input_artifact_bytes, max_output_artifact_bytes, execution_timeout_ceiling, \
         default_priority FROM tenants WHERE id = $1",
    )
    .bind(tenant.as_uuid())
    .fetch_one(conn)
    .await?;
    tenant_settings_from_row(&row)
}

/// Like [`load_settings`], but for a caller (admission) that must treat an
/// unknown or soft-deleted Tenant as a clean `None` rather than a
/// [`sqlx::Error::RowNotFound`] — Tenant authentication has already resolved
/// the id by this point, so this should only be `None` in a race with
/// deletion.
///
/// # Errors
/// Returns an error if the query fails or a stored value cannot be
/// represented as a [`TenantSettings`].
pub async fn settings(
    conn: &mut PgConnection,
    tenant: TenantId,
) -> sqlx::Result<Option<TenantSettings>> {
    let row: Option<TenantSettingsRow> = sqlx::query_as(
        "SELECT maximum_queue_age, max_queued_generations, \
         max_input_artifact_bytes, max_output_artifact_bytes, execution_timeout_ceiling, \
         default_priority FROM tenants WHERE id = $1 AND deleted_at IS NULL",
    )
    .bind(tenant.as_uuid())
    .fetch_optional(conn)
    .await?;
    row.as_ref().map(tenant_settings_from_row).transpose()
}

/// Writes `settings` as `tenant`'s new mutable settings (ADR 0006).
///
/// # Errors
/// Returns an error if the query fails or a value does not fit its column.
pub async fn update_settings(
    conn: &mut PgConnection,
    tenant: TenantId,
    settings: &TenantSettings,
) -> sqlx::Result<()> {
    let max_queued_generations =
        i32::try_from(settings.max_queued_generations).map_err(encode_error)?;
    let max_input_artifact_bytes =
        i64::try_from(settings.max_input_artifact_bytes).map_err(encode_error)?;
    let max_output_artifact_bytes =
        i64::try_from(settings.max_output_artifact_bytes).map_err(encode_error)?;
    scope_to_tenant(conn, tenant).await?;
    sqlx::query(
        "UPDATE tenants SET \
             maximum_queue_age = $2, \
             max_queued_generations = $3, \
             max_input_artifact_bytes = $4, \
             max_output_artifact_bytes = $5, \
             execution_timeout_ceiling = $6, \
             default_priority = $7, \
             updated_at = now() \
         WHERE id = $1",
    )
    .bind(tenant.as_uuid())
    .bind(interval_from_duration(settings.maximum_queue_age))
    .bind(max_queued_generations)
    .bind(max_input_artifact_bytes)
    .bind(max_output_artifact_bytes)
    .bind(interval_from_duration(settings.execution_timeout_ceiling))
    .bind(i16::from(settings.default_priority))
    .execute(conn)
    .await?;
    Ok(())
}

fn tenant_settings_from_row(row: &TenantSettingsRow) -> sqlx::Result<TenantSettings> {
    Ok(TenantSettings {
        maximum_queue_age: super::interval_to_duration(
            "tenants.maximum_queue_age",
            row.maximum_queue_age,
        )?,
        max_queued_generations: u32::try_from(row.max_queued_generations).map_err(decode_error)?,
        max_input_artifact_bytes: u64::try_from(row.max_input_artifact_bytes)
            .map_err(decode_error)?,
        max_output_artifact_bytes: u64::try_from(row.max_output_artifact_bytes)
            .map_err(decode_error)?,
        execution_timeout_ceiling: super::interval_to_duration(
            "tenants.execution_timeout_ceiling",
            row.execution_timeout_ceiling,
        )?,
        default_priority: Priority::try_from(i32::from(row.default_priority))
            .map_err(decode_error)?,
    })
}

fn decode_error<E: std::error::Error + Send + Sync + 'static>(error: E) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(error))
}

fn encode_error<E: std::error::Error + Send + Sync + 'static>(error: E) -> sqlx::Error {
    sqlx::Error::Encode(Box::new(error))
}

/// Converts a [`Duration`] to a [`PgInterval`], always storing the whole
/// value in `microseconds` (never `months`/`days`) so the conversion is exact
/// for any duration under about 292,000 years.
fn interval_from_duration(duration: Duration) -> PgInterval {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Tenant settings durations (queue age, timeout ceilings) never approach i64::MAX microseconds"
    )]
    let microseconds = duration.as_micros() as i64;
    PgInterval {
        months: 0,
        days: 0,
        microseconds,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_round_trips_through_microseconds() {
        let duration = Duration::from_mins(30);
        let interval = interval_from_duration(duration);
        assert_eq!(
            interval,
            PgInterval {
                months: 0,
                days: 0,
                microseconds: 1_800_000_000
            }
        );
        let Ok(back) = crate::db::interval_to_duration("test", interval) else {
            panic!("a microseconds-only interval must convert back cleanly")
        };
        assert_eq!(back, duration);
    }
}
