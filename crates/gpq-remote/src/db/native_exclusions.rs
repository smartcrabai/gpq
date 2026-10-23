//! Generation-local failed-pool history for scheduling and retry decisions.

use std::collections::{HashMap, HashSet};

use gpq_domain::{DevicePoolId, GenerationId, TenantId};
use sqlx::PgConnection;
use uuid::Uuid;

/// Records one pool rejected before execution without consuming a runtime attempt.
///
/// Call only after the current leased Attempt's worker ownership and state have
/// been checked, in the same transaction that removes that lease.
///
/// Returns whether this was the first rejection of the pool for this Generation.
///
/// # Errors
/// Returns [`sqlx::Error`] if the insert fails.
pub async fn record_preexecution_rejection(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
    pool_id: DevicePoolId,
) -> sqlx::Result<bool> {
    let inserted: Option<Uuid> = sqlx::query_scalar(
        "INSERT INTO generation_pool_rejections (tenant_id, generation_id, pool_id) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (tenant_id, generation_id, pool_id) DO NOTHING \
         RETURNING pool_id",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .bind(pool_id.as_uuid())
    .fetch_optional(&mut *conn)
    .await?;
    Ok(inserted.is_some())
}

/// Returns pools already rejected for this Generation, combining execution
/// OOM history with pre-execution capability rejections.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails.
pub async fn excluded_pools(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
) -> sqlx::Result<HashSet<DevicePoolId>> {
    let rows: Vec<Uuid> = sqlx::query_scalar(
        "SELECT pool_id FROM attempts \
         WHERE tenant_id = $1 AND generation_id = $2 AND failure_kind = 'out_of_memory' \
         UNION \
         SELECT pool_id FROM generation_pool_rejections \
         WHERE tenant_id = $1 AND generation_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(DevicePoolId::from_uuid).collect())
}

/// Bulk form used by one scheduler or expiry snapshot to avoid a query per
/// queued Generation. The returned sets contain only the supplied generations.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails.
pub async fn excluded_pools_for_generations(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_ids: &[GenerationId],
) -> sqlx::Result<HashMap<GenerationId, HashSet<DevicePoolId>>> {
    if generation_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let ids: Vec<Uuid> = generation_ids.iter().map(GenerationId::as_uuid).collect();
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "SELECT generation_id, pool_id FROM attempts \
         WHERE tenant_id = $1 AND generation_id = ANY($2) AND failure_kind = 'out_of_memory' \
         UNION \
         SELECT generation_id, pool_id FROM generation_pool_rejections \
         WHERE tenant_id = $1 AND generation_id = ANY($2)",
    )
    .bind(tenant_id.as_uuid())
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await?;

    let mut exclusions: HashMap<GenerationId, HashSet<DevicePoolId>> = HashMap::new();
    for (generation_id, pool_id) in rows {
        exclusions
            .entry(GenerationId::from_uuid(generation_id))
            .or_default()
            .insert(DevicePoolId::from_uuid(pool_id));
    }
    Ok(exclusions)
}
