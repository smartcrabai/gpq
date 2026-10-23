//! Direct-SQL read helpers used only for test assertions (ADR 0013:
//! `PostgreSQL` is the queue's source of truth, so asserting against it
//! directly is the most faithful check available). These connect through
//! the harness's schema-owner pool, which bypasses RLS as a superuser —
//! fine for read-only assertions; RLS *enforcement* itself is exercised
//! through the wire API in `cross_tenant_isolation_over_the_api`.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// A `generations` row, projected to the columns tests assert on.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GenerationRow {
    pub id: Uuid,
    pub state: String,
    pub attempt_count: i32,
}

/// An `attempts` row, projected to the columns tests assert on.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AttemptRow {
    pub attempt_number: i32,
    pub state: String,
    pub failure_kind: Option<String>,
    /// Renewed by every accepted heartbeat, so a suite can watch a live
    /// lease move forward (ADR 0003).
    pub lease_expires_at: DateTime<Utc>,
}

/// Reads one Generation by id, tenant-scoped even though the connection
/// itself bypasses RLS.
pub async fn generation_row(
    pool: &PgPool,
    tenant_id: Uuid,
    generation_id: Uuid,
) -> anyhow::Result<Option<GenerationRow>> {
    let row = sqlx::query_as::<_, GenerationRow>(
        "SELECT id, state, attempt_count FROM generations \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id)
    .bind(generation_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// The most recently created Generation in `state` for `tenant_id`.
pub async fn latest_generation_row(
    pool: &PgPool,
    tenant_id: Uuid,
    state: &str,
) -> anyhow::Result<Option<GenerationRow>> {
    let row = sqlx::query_as::<_, GenerationRow>(
        "SELECT id, state, attempt_count FROM generations \
         WHERE tenant_id = $1 AND state = $2 \
         ORDER BY created_at DESC LIMIT 1",
    )
    .bind(tenant_id)
    .bind(state)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Every Attempt of one Generation, oldest first.
pub async fn attempt_rows(
    pool: &PgPool,
    tenant_id: Uuid,
    generation_id: Uuid,
) -> anyhow::Result<Vec<AttemptRow>> {
    let rows = sqlx::query_as::<_, AttemptRow>(
        "SELECT attempt_number, state, failure_kind, lease_expires_at FROM attempts \
         WHERE tenant_id = $1 AND generation_id = $2 \
         ORDER BY attempt_number",
    )
    .bind(tenant_id)
    .bind(generation_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// The distinct `generation_events.kind` values recorded for one
/// Generation (ADR 0008: `attempt_created`/`state_changed`/`progress`).
pub async fn event_kinds(
    pool: &PgPool,
    tenant_id: Uuid,
    generation_id: Uuid,
) -> anyhow::Result<Vec<String>> {
    let kinds: Vec<String> = sqlx::query_scalar(
        "SELECT kind FROM generation_events \
         WHERE tenant_id = $1 AND generation_id = $2 \
         ORDER BY sequence",
    )
    .bind(tenant_id)
    .bind(generation_id)
    .fetch_all(pool)
    .await?;
    Ok(kinds)
}

/// The `state` and stored `content` length (`None` when no bytes are stored)
/// of one Artifact, tenant-scoped; `None` when the row does not exist.
pub async fn artifact_state(
    pool: &PgPool,
    tenant_id: Uuid,
    artifact_id: Uuid,
) -> anyhow::Result<Option<(String, Option<i32>)>> {
    let row: Option<(String, Option<i32>)> = sqlx::query_as(
        "SELECT state, octet_length(content) FROM artifacts \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant_id)
    .bind(artifact_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}
