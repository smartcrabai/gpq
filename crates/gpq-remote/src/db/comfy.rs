//! Persistence for raw `ComfyUI` Server API prompts and history metadata.

use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use sqlx::PgConnection;
use uuid::Uuid;

use gpq_domain::{GenerationId, TenantId};

/// Durable metadata associated with one raw `ComfyUI` prompt.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ComfyPromptRow {
    /// The owning Generation.
    pub generation_id: Uuid,
    /// The client-visible Comfy prompt id.
    pub prompt_id: Uuid,
    /// The client correlation id, when supplied.
    pub client_id: Option<String>,
    /// Tenant-local queue number.
    pub number: i64,
    /// Accepted Comfy history outputs, if execution succeeded.
    pub outputs: Option<Json>,
    /// Output node ids from the backend prompt tuple.
    pub output_node_ids: Vec<String>,
}

/// History row joined with the Generation lifecycle columns needed by the API.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ComfyHistoryRow {
    /// The owning Generation.
    pub generation_id: Uuid,
    /// The client-visible Comfy prompt id.
    pub prompt_id: Uuid,
    /// The client correlation id, when supplied.
    pub client_id: Option<String>,
    /// Tenant-local queue number.
    pub number: i64,
    /// The original normalized prompt payload.
    pub parameters: Json,
    /// Accepted Comfy history outputs, if execution succeeded.
    pub outputs: Option<Json>,
    /// Output node ids from the backend prompt tuple.
    pub output_node_ids: Vec<String>,
    /// Persisted Generation state.
    pub state: String,
    /// Normalized failure kind, when failed.
    pub failure_kind: Option<String>,
    /// Safe failure message retained by the queue.
    pub failure_message: String,
    /// Terminal timestamp, when terminal.
    pub terminated_at: Option<DateTime<Utc>>,
}

/// Inserts raw Comfy metadata after its Generation row exists.
///
/// # Errors
/// Returns [`sqlx::Error`] when the insert fails.
pub async fn insert(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
    prompt_id: Uuid,
    client_id: Option<&str>,
    number: i64,
) -> sqlx::Result<()> {
    sqlx::query(
        "INSERT INTO comfy_prompts \
         (tenant_id, generation_id, prompt_id, client_id, number) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .bind(prompt_id)
    .bind(client_id)
    .bind(number)
    .execute(conn)
    .await?;
    Ok(())
}

/// Reads one raw prompt by its client-visible id.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails or the row cannot be decoded.
pub async fn get_by_prompt_id(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    prompt_id: Uuid,
) -> sqlx::Result<Option<ComfyPromptRow>> {
    sqlx::query_as::<_, ComfyPromptRow>(
        "SELECT generation_id, prompt_id, client_id, number, outputs, output_node_ids \
         FROM comfy_prompts WHERE tenant_id = $1 AND prompt_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(prompt_id)
    .fetch_optional(conn)
    .await
}

/// Returns whether a client-visible prompt id already exists for the Tenant.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails.
pub async fn prompt_id_exists(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    prompt_id: Uuid,
) -> sqlx::Result<bool> {
    let (exists,): (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM comfy_prompts WHERE tenant_id = $1 AND prompt_id = $2)",
    )
    .bind(tenant_id.as_uuid())
    .bind(prompt_id)
    .fetch_one(conn)
    .await?;
    Ok(exists)
}

/// Reads raw metadata by its internal Generation id.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails or the row cannot be decoded.
pub async fn get_by_generation(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
) -> sqlx::Result<Option<ComfyPromptRow>> {
    sqlx::query_as::<_, ComfyPromptRow>(
        "SELECT generation_id, prompt_id, client_id, number, outputs, output_node_ids \
         FROM comfy_prompts WHERE tenant_id = $1 AND generation_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .fetch_optional(conn)
    .await
}

/// Lists terminal raw prompts in deterministic completion order.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails or a row cannot be decoded.
pub async fn list_history(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    limit: i64,
    offset: i64,
) -> sqlx::Result<Vec<ComfyHistoryRow>> {
    sqlx::query_as::<_, ComfyHistoryRow>(
        "SELECT cp.generation_id, cp.prompt_id, cp.client_id, cp.number, \
                g.parameters, cp.outputs, cp.output_node_ids, g.state, \
                g.failure_kind, g.failure_message, g.terminated_at \
         FROM comfy_prompts cp \
         JOIN generations g ON g.tenant_id = cp.tenant_id AND g.id = cp.generation_id \
         WHERE cp.tenant_id = $1 AND g.state IN ('succeeded', 'failed', 'cancelled', 'expired') \
         ORDER BY g.terminated_at ASC NULLS LAST, cp.generation_id ASC \
         LIMIT $2 OFFSET $3",
    )
    .bind(tenant_id.as_uuid())
    .bind(limit)
    .bind(offset)
    .fetch_all(conn)
    .await
}

/// Counts nonterminal raw prompts for Comfy queue status.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails.
pub async fn count_nonterminal(conn: &mut PgConnection, tenant_id: TenantId) -> sqlx::Result<i64> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM comfy_prompts cp \
         JOIN generations g ON g.tenant_id = cp.tenant_id AND g.id = cp.generation_id \
         WHERE cp.tenant_id = $1 \
           AND g.state NOT IN ('succeeded', 'failed', 'cancelled', 'expired')",
    )
    .bind(tenant_id.as_uuid())
    .fetch_one(conn)
    .await?;
    Ok(count)
}

/// Stores accepted raw outputs and their output node ids in the same result transaction.
///
/// # Errors
/// Returns [`sqlx::Error`] when the update fails.
pub async fn update_outputs(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    generation_id: GenerationId,
    outputs: &Json,
    output_node_ids: &[String],
) -> sqlx::Result<()> {
    sqlx::query(
        "UPDATE comfy_prompts SET outputs = $3, output_node_ids = $4 \
         WHERE tenant_id = $1 AND generation_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(generation_id.as_uuid())
    .bind(outputs)
    .bind(output_node_ids)
    .execute(conn)
    .await?;
    Ok(())
}

/// Reads one terminal raw prompt joined with its Generation.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails or the row cannot be decoded.
pub async fn history_by_prompt_id(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    prompt_id: Uuid,
) -> sqlx::Result<Option<ComfyHistoryRow>> {
    sqlx::query_as::<_, ComfyHistoryRow>(
        "SELECT cp.generation_id, cp.prompt_id, cp.client_id, cp.number, \
                g.parameters, cp.outputs, cp.output_node_ids, g.state, \
                g.failure_kind, g.failure_message, g.terminated_at \
         FROM comfy_prompts cp \
         JOIN generations g ON g.tenant_id = cp.tenant_id AND g.id = cp.generation_id \
         WHERE cp.tenant_id = $1 AND cp.prompt_id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(prompt_id)
    .fetch_optional(conn)
    .await
}

/// Counts terminal raw prompts for history pagination.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails.
pub async fn count_terminal(conn: &mut PgConnection, tenant_id: TenantId) -> sqlx::Result<i64> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM comfy_prompts cp \
         JOIN generations g ON g.tenant_id = cp.tenant_id AND g.id = cp.generation_id \
         WHERE cp.tenant_id = $1 AND g.state IN ('succeeded', 'failed', 'cancelled', 'expired')",
    )
    .bind(tenant_id.as_uuid())
    .fetch_one(conn)
    .await?;
    Ok(count)
}

/// Returns a Tenant-local number watermark for raw prompts.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails.
pub async fn max_number(conn: &mut PgConnection, tenant_id: TenantId) -> sqlx::Result<i64> {
    let (number,): (i64,) =
        sqlx::query_as("SELECT COALESCE(MAX(number), -1) FROM comfy_prompts WHERE tenant_id = $1")
            .bind(tenant_id.as_uuid())
            .fetch_one(conn)
            .await?;
    Ok(number)
}

/// Lists raw prompts for one client after a Tenant-local number watermark.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails or a row cannot be decoded.
pub async fn list_client_after_number(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    client_id: &str,
    after_number: i64,
    limit: i64,
) -> sqlx::Result<Vec<ComfyHistoryRow>> {
    sqlx::query_as::<_, ComfyHistoryRow>(
        "SELECT cp.generation_id, cp.prompt_id, cp.client_id, cp.number, \
                g.parameters, cp.outputs, cp.output_node_ids, g.state, \
                g.failure_kind, g.failure_message, g.terminated_at \
         FROM comfy_prompts cp \
         JOIN generations g ON g.tenant_id = cp.tenant_id AND g.id = cp.generation_id \
         WHERE cp.tenant_id = $1 AND cp.client_id = $2 AND cp.number > $3 \
         ORDER BY cp.number ASC LIMIT $4",
    )
    .bind(tenant_id.as_uuid())
    .bind(client_id)
    .bind(after_number)
    .bind(limit)
    .fetch_all(conn)
    .await
}

/// Checks that an artifact owns the exact public Comfy view requested.
///
/// # Errors
/// Returns [`sqlx::Error`] when the query fails or a row cannot be decoded.
pub async fn artifact_view_matches(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    artifact_id: Uuid,
    filename: &str,
    subfolder: &str,
    kind: &str,
) -> sqlx::Result<bool> {
    #[derive(sqlx::FromRow)]
    struct ViewRow {
        direction: String,
        target_kind: Option<String>,
        pointers: Vec<String>,
        outputs: Option<Json>,
    }
    let Some(row) = sqlx::query_as::<_, ViewRow>(
        "SELECT a.direction, g.target_kind, \
                a.comfy_output_pointers AS pointers, cp.outputs \
         FROM artifacts a \
         LEFT JOIN generations g ON g.tenant_id = a.tenant_id AND g.id = a.generation_id \
         LEFT JOIN comfy_prompts cp ON cp.tenant_id = a.tenant_id AND cp.generation_id = a.generation_id \
         WHERE a.tenant_id = $1 AND a.id = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(artifact_id)
    .fetch_optional(conn)
    .await?
    else {
        return Ok(false);
    };
    if row.direction != "output"
        || row.target_kind.as_deref() != Some("comfy_prompt")
        || row.pointers.is_empty()
    {
        return Ok(false);
    }
    let Some(outputs) = row.outputs else {
        return Ok(false);
    };
    for pointer in row.pointers {
        let Some(value) = outputs.pointer(&pointer) else {
            continue;
        };
        let Some(object) = value.as_object() else {
            continue;
        };
        if object.get("filename").and_then(Json::as_str) == Some(filename)
            && object.get("subfolder").and_then(Json::as_str).unwrap_or("") == subfolder
            && object
                .get("type")
                .and_then(Json::as_str)
                .unwrap_or("output")
                == kind
        {
            return Ok(true);
        }
    }
    Ok(false)
}
