//! Model and Workflow catalog persistence (ADR 0012).
//!
//! Workers register immutable Model Versions by content hash; Tenant APIs
//! register immutable Workflow graphs and manifests and map aliases onto
//! either kind of version. Alias deletion or reassignment never mutates a
//! version or any past Generation: aliases are mutable pointers, versions are
//! append-only and referenced by content hash everywhere else in the system.
//!
//! The `workflow_versions.manifest` column has no sibling columns for
//! execution limits (unlike `model_versions`, which has dedicated
//! `execution_timeout`/`estimated_vram_bytes` columns), so this module stores
//! the domain [`WorkflowManifest`] and its [`ExecutionLimits`] together as one
//! JSON document (see [`StoredWorkflowManifest`]) and splits them back apart
//! on read.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use gpq_domain::{ContentHash, ExecutionLimits, Modality, TenantId, WorkflowManifest};
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use sqlx::postgres::types::PgInterval;

/// A Model Version resolved from an alias at admission (ADR 0012).
pub struct ResolvedModel {
    /// Exact model material advertised by capable Workers.
    pub content_hash: ContentHash,
    /// Modality the version serves.
    pub modality: Modality,
    /// Version-declared execution limits.
    pub limits: ExecutionLimits,
}

/// A Workflow Version resolved from an alias at admission (ADR 0012).
pub struct ResolvedWorkflow {
    /// Content hash of the immutable graph and manifest.
    pub content_hash: ContentHash,
    /// Modality the version serves.
    pub modality: Modality,
    /// Version-declared execution limits.
    pub limits: ExecutionLimits,
    /// Output and requirement manifest (ADR 0007).
    pub manifest: WorkflowManifest,
}

/// One registered Model alias.
pub struct ModelAliasRow {
    /// The alias name.
    pub alias: String,
    /// Lowercase hex SHA-256 of the aliased Model Version.
    pub content_hash: String,
    /// The stable modality name (`"llm"`, `"image"`, ...) of that version.
    pub modality: String,
    /// When the alias was first created.
    pub created_at: DateTime<Utc>,
}

/// One registered Workflow alias.
pub struct WorkflowAliasRow {
    /// The alias name.
    pub alias: String,
    /// Lowercase hex SHA-256 of the aliased Workflow Version.
    pub content_hash: String,
    /// The stable modality name of that version.
    pub modality: String,
}

/// One registered Model Version, as advertised by capable Workers.
pub struct ModelVersionRow {
    /// Lowercase hex SHA-256 of the model file.
    pub content_hash: ContentHash,
    /// Modality the version serves.
    pub modality: Modality,
    /// Version-declared execution limits.
    pub limits: ExecutionLimits,
    /// When a Worker first registered this content hash.
    pub first_seen_at: DateTime<Utc>,
}

/// One registered Workflow Version.
pub struct WorkflowVersionRow {
    /// Content hash of the immutable graph and manifest.
    pub content_hash: ContentHash,
    /// Modality the version serves.
    pub modality: Modality,
    /// Output and requirement manifest (ADR 0007).
    pub manifest: WorkflowManifest,
    /// Version-declared execution limits.
    pub limits: ExecutionLimits,
    /// Opaque `ComfyUI` API-format graph.
    pub graph: serde_json::Value,
    /// When the Workflow Version was registered.
    pub created_at: DateTime<Utc>,
}

/// Failure to point an alias at a content hash that has no registered
/// version (ADR 0012: admission and alias assignment only ever resolve to a
/// version that actually exists).
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// No Model or Workflow Version is registered under this hash for the
    /// Tenant.
    #[error("content hash {0} is not a registered version")]
    UnknownVersion(ContentHash),
    /// A database error unrelated to the alias target.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// On-disk shape of the `workflow_versions.manifest` column: the domain
/// manifest plus its execution limits, stored together since the schema has
/// no separate limit columns for Workflow Versions.
#[derive(Serialize, Deserialize)]
struct StoredWorkflowManifest {
    #[serde(flatten)]
    manifest: WorkflowManifest,
    #[serde(default)]
    limits: ExecutionLimits,
}

fn decode_error(context: &str, err: impl std::fmt::Display) -> sqlx::Error {
    sqlx::Error::decode(format!("{context}: {err}"))
}

fn parse_content_hash(context: &str, raw: &str) -> Result<ContentHash, sqlx::Error> {
    ContentHash::from_str(raw).map_err(|err| decode_error(context, err))
}

fn parse_modality(context: &str, raw: &str) -> Result<Modality, sqlx::Error> {
    Modality::from_str(raw).map_err(|err| decode_error(context, err))
}

/// Re-exported so existing crate-internal callers (e.g. `native::generation`,
/// decoding `generations.execution_timeout`) keep resolving
/// `catalog::interval_to_duration` now that the canonical implementation
/// lives in `db::mod`, shared by every interval-bearing table.
pub(crate) use super::interval_to_duration;

fn decode_workflow_manifest(
    context: &str,
    manifest: serde_json::Value,
) -> Result<(WorkflowManifest, ExecutionLimits), sqlx::Error> {
    let stored: StoredWorkflowManifest =
        serde_json::from_value(manifest).map_err(|err| decode_error(context, err))?;
    Ok((stored.manifest, stored.limits))
}

/// Hashes the immutable content identity of a Workflow Version: its graph and
/// output manifest, but not its (mutable-in-spirit) execution limits (ADR
/// 0012). `serde_json::Value` objects in this workspace are `BTreeMap`-backed
/// (no `preserve_order` feature), so key order in the source `graph` never
/// affects the hash.
fn workflow_content_hash(
    graph: &serde_json::Value,
    manifest: &WorkflowManifest,
) -> Result<ContentHash, serde_json::Error> {
    let combined = serde_json::json!({ "graph": graph, "manifest": manifest });
    let bytes = serde_json::to_vec(&combined)?;
    Ok(ContentHash::digest(&bytes))
}

/// Registers an immutable Workflow graph and manifest, returning its content
/// hash (ADR 0012). Registration is idempotent: re-registering the same
/// graph and manifest is a no-op, and the first registration's `limits` win
/// if a later call disagrees.
///
/// # Errors
/// Returns [`sqlx::Error::Encode`] if `graph`/`manifest` (or their bundled
/// [`ExecutionLimits`]) cannot be encoded as JSON, or [`sqlx::Error`] if the
/// insert fails. The `ON CONFLICT (tenant_id, content_sha256) DO NOTHING`
/// means re-registering the same graph and manifest never surfaces as a
/// constraint violation.
pub async fn register_workflow_version(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    modality: Modality,
    graph: &serde_json::Value,
    manifest: &WorkflowManifest,
    limits: ExecutionLimits,
) -> sqlx::Result<ContentHash> {
    let content_hash = workflow_content_hash(graph, manifest)
        .map_err(|err| decode_error("workflow content hash", err))?;
    let stored = StoredWorkflowManifest {
        manifest: manifest.clone(),
        limits,
    };
    let stored_json =
        serde_json::to_value(&stored).map_err(|err| decode_error("workflow manifest", err))?;
    let id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO workflow_versions (tenant_id, id, content_sha256, modality, graph, manifest) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (tenant_id, content_sha256) DO NOTHING",
    )
    .bind(tenant_id.as_uuid())
    .bind(id)
    .bind(content_hash.to_hex())
    .bind(modality.as_str())
    .bind(graph)
    .bind(&stored_json)
    .execute(&mut *conn)
    .await?;
    Ok(content_hash)
}

#[derive(sqlx::FromRow)]
struct WorkflowVersionRawRow {
    content_sha256: String,
    modality: String,
    graph: serde_json::Value,
    manifest: serde_json::Value,
    created_at: DateTime<Utc>,
}

impl WorkflowVersionRawRow {
    fn into_row(self) -> Result<WorkflowVersionRow, sqlx::Error> {
        let content_hash =
            parse_content_hash("workflow_versions.content_sha256", &self.content_sha256)?;
        let modality = parse_modality("workflow_versions.modality", &self.modality)?;
        let (manifest, limits) =
            decode_workflow_manifest("workflow_versions.manifest", self.manifest)?;
        Ok(WorkflowVersionRow {
            content_hash,
            modality,
            manifest,
            limits,
            graph: self.graph,
            created_at: self.created_at,
        })
    }
}

/// Fetches one registered Workflow Version by content hash, including its
/// registration timestamp (used to build the Catalog API's `WorkflowVersion`
/// wire message). Resolving by pinned hash rather than through an alias is
/// what lets a retry reuse the exact version an earlier Attempt used even if
/// the alias has since moved (ADR 0012).
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if the row's
/// `content_sha256`, `modality`, or `manifest` column cannot be decoded
/// (see [`WorkflowVersionRawRow::into_row`]).
pub async fn get_workflow_version_row(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    content_hash: ContentHash,
) -> sqlx::Result<Option<WorkflowVersionRow>> {
    let raw: Option<WorkflowVersionRawRow> = sqlx::query_as(
        "SELECT content_sha256, modality, graph, manifest, created_at \
         FROM workflow_versions WHERE tenant_id = $1 AND content_sha256 = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(content_hash.to_hex())
    .fetch_optional(&mut *conn)
    .await?;
    raw.map(WorkflowVersionRawRow::into_row).transpose()
}

/// Lists every registered Workflow Version, oldest first.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if any row's
/// `content_sha256`, `modality`, or `manifest` column cannot be decoded.
pub async fn list_workflow_versions(
    conn: &mut PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<WorkflowVersionRow>> {
    let rows: Vec<WorkflowVersionRawRow> = sqlx::query_as(
        "SELECT content_sha256, modality, graph, manifest, created_at \
         FROM workflow_versions WHERE tenant_id = $1 ORDER BY created_at",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter()
        .map(WorkflowVersionRawRow::into_row)
        .collect()
}

#[derive(sqlx::FromRow)]
struct ModelVersionRawRow {
    content_sha256: String,
    modality: String,
    execution_timeout: Option<PgInterval>,
    estimated_vram_bytes: Option<i64>,
    first_seen_at: DateTime<Utc>,
}

/// Decodes the `model_versions.execution_timeout` / `estimated_vram_bytes`
/// pair shared by [`ModelVersionRawRow::into_row`] and
/// [`resolve_model_alias`]'s alias-joined row into an [`ExecutionLimits`].
fn decode_model_execution_limits(
    execution_timeout: Option<PgInterval>,
    estimated_vram_bytes: Option<i64>,
) -> Result<ExecutionLimits, sqlx::Error> {
    let execution_timeout = execution_timeout
        .map(|interval| interval_to_duration("model_versions.execution_timeout", interval))
        .transpose()?;
    let estimated_vram_bytes = estimated_vram_bytes
        .map(|bytes| {
            u64::try_from(bytes)
                .map_err(|err| decode_error("model_versions.estimated_vram_bytes", err))
        })
        .transpose()?;
    Ok(ExecutionLimits {
        execution_timeout,
        estimated_vram_bytes,
    })
}

impl ModelVersionRawRow {
    fn into_row(self) -> Result<ModelVersionRow, sqlx::Error> {
        let content_hash =
            parse_content_hash("model_versions.content_sha256", &self.content_sha256)?;
        let modality = parse_modality("model_versions.modality", &self.modality)?;
        let limits =
            decode_model_execution_limits(self.execution_timeout, self.estimated_vram_bytes)?;
        Ok(ModelVersionRow {
            content_hash,
            modality,
            limits,
            first_seen_at: self.first_seen_at,
        })
    }
}

/// Lists every registered Model Version, oldest first.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if any row's
/// `content_sha256`, `modality`, `execution_timeout`, or
/// `estimated_vram_bytes` column cannot be decoded (see
/// [`decode_model_execution_limits`]).
pub async fn list_model_versions(
    conn: &mut PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<ModelVersionRow>> {
    let rows: Vec<ModelVersionRawRow> = sqlx::query_as(
        "SELECT content_sha256, modality, execution_timeout, estimated_vram_bytes, first_seen_at \
         FROM model_versions WHERE tenant_id = $1 ORDER BY first_seen_at",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter().map(ModelVersionRawRow::into_row).collect()
}

/// Fetches one registered Model Version by content hash.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if the row cannot be
/// decoded (see [`list_model_versions`]).
pub async fn get_model_version(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    content_hash: ContentHash,
) -> sqlx::Result<Option<ModelVersionRow>> {
    let raw: Option<ModelVersionRawRow> = sqlx::query_as(
        "SELECT content_sha256, modality, execution_timeout, estimated_vram_bytes, first_seen_at \
         FROM model_versions WHERE tenant_id = $1 AND content_sha256 = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(content_hash.to_hex())
    .fetch_optional(&mut *conn)
    .await?;
    raw.map(ModelVersionRawRow::into_row).transpose()
}

/// Shared existence probe for the two version tables; `query` differs only in
/// which table it names, so both callers pass a static literal.
async fn version_exists(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    query: &'static str,
    content_hash: ContentHash,
) -> sqlx::Result<bool> {
    let found: Option<(i32,)> = sqlx::query_as(query)
        .bind(tenant_id.as_uuid())
        .bind(content_hash.to_hex())
        .fetch_optional(&mut *conn)
        .await?;
    Ok(found.is_some())
}

async fn model_version_exists(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    content_hash: ContentHash,
) -> sqlx::Result<bool> {
    version_exists(
        conn,
        tenant_id,
        "SELECT 1 FROM model_versions WHERE tenant_id = $1 AND content_sha256 = $2",
        content_hash,
    )
    .await
}

async fn workflow_version_exists(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    content_hash: ContentHash,
) -> sqlx::Result<bool> {
    version_exists(
        conn,
        tenant_id,
        "SELECT 1 FROM workflow_versions WHERE tenant_id = $1 AND content_sha256 = $2",
        content_hash,
    )
    .await
}

#[derive(sqlx::FromRow)]
struct ModelAliasRawRow {
    alias: String,
    content_sha256: String,
    modality: String,
    created_at: DateTime<Utc>,
}

impl From<ModelAliasRawRow> for ModelAliasRow {
    fn from(raw: ModelAliasRawRow) -> Self {
        Self {
            alias: raw.alias,
            content_hash: raw.content_sha256,
            modality: raw.modality,
            created_at: raw.created_at,
        }
    }
}

/// Points a Model alias at an already-registered content hash (ADR 0012).
/// Reassigning an existing alias never touches past Generations, which
/// pinned the previous hash at admission time.
///
/// # Errors
/// Returns [`CatalogError::UnknownVersion`] if `content_hash` has no
/// registered Model Version for `tenant_id`. Returns
/// [`CatalogError::Database`] if the upsert fails.
pub async fn set_model_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
    content_hash: ContentHash,
) -> Result<ModelAliasRow, CatalogError> {
    if !model_version_exists(conn, tenant_id, content_hash).await? {
        return Err(CatalogError::UnknownVersion(content_hash));
    }
    let raw: ModelAliasRawRow = sqlx::query_as(
        "WITH upsert AS ( \
             INSERT INTO model_aliases (tenant_id, alias, content_sha256) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, alias) \
             DO UPDATE SET content_sha256 = excluded.content_sha256, updated_at = now() \
             RETURNING tenant_id, alias, content_sha256, created_at \
         ) \
         SELECT upsert.alias, upsert.content_sha256, model_versions.modality, upsert.created_at \
         FROM upsert \
         JOIN model_versions \
             ON model_versions.tenant_id = upsert.tenant_id \
             AND model_versions.content_sha256 = upsert.content_sha256",
    )
    .bind(tenant_id.as_uuid())
    .bind(alias)
    .bind(content_hash.to_hex())
    .fetch_one(&mut *conn)
    .await?;
    Ok(raw.into())
}

#[derive(sqlx::FromRow)]
struct WorkflowAliasRawRow {
    alias: String,
    content_sha256: String,
    modality: String,
}

impl From<WorkflowAliasRawRow> for WorkflowAliasRow {
    fn from(raw: WorkflowAliasRawRow) -> Self {
        Self {
            alias: raw.alias,
            content_hash: raw.content_sha256,
            modality: raw.modality,
        }
    }
}

/// Points a Workflow alias at an already-registered content hash (ADR 0012).
///
/// # Errors
/// Returns [`CatalogError::UnknownVersion`] if `content_hash` has no
/// registered Workflow Version for `tenant_id`. Returns
/// [`CatalogError::Database`] if the upsert fails.
pub async fn set_workflow_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
    content_hash: ContentHash,
) -> Result<WorkflowAliasRow, CatalogError> {
    if !workflow_version_exists(conn, tenant_id, content_hash).await? {
        return Err(CatalogError::UnknownVersion(content_hash));
    }
    let raw: WorkflowAliasRawRow = sqlx::query_as(
        "WITH upsert AS ( \
             INSERT INTO workflow_aliases (tenant_id, alias, content_sha256) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, alias) \
             DO UPDATE SET content_sha256 = excluded.content_sha256, updated_at = now() \
             RETURNING tenant_id, alias, content_sha256 \
         ) \
         SELECT upsert.alias, upsert.content_sha256, workflow_versions.modality \
         FROM upsert \
         JOIN workflow_versions \
             ON workflow_versions.tenant_id = upsert.tenant_id \
             AND workflow_versions.content_sha256 = upsert.content_sha256",
    )
    .bind(tenant_id.as_uuid())
    .bind(alias)
    .bind(content_hash.to_hex())
    .fetch_one(&mut *conn)
    .await?;
    Ok(raw.into())
}

/// Deletes a Model alias. Returns whether an alias existed to delete; never
/// touches the underlying version or any Generation that already resolved
/// through it.
///
/// # Errors
/// Returns [`sqlx::Error`] if the delete fails (e.g. connection lost).
pub async fn delete_model_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
) -> sqlx::Result<bool> {
    let result = sqlx::query("DELETE FROM model_aliases WHERE tenant_id = $1 AND alias = $2")
        .bind(tenant_id.as_uuid())
        .bind(alias)
        .execute(&mut *conn)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Deletes a Workflow alias. Returns whether an alias existed to delete.
///
/// # Errors
/// Returns [`sqlx::Error`] if the delete fails (e.g. connection lost).
pub async fn delete_workflow_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
) -> sqlx::Result<bool> {
    let result = sqlx::query("DELETE FROM workflow_aliases WHERE tenant_id = $1 AND alias = $2")
        .bind(tenant_id.as_uuid())
        .bind(alias)
        .execute(&mut *conn)
        .await?;
    Ok(result.rows_affected() > 0)
}

#[derive(sqlx::FromRow)]
struct ResolvedModelRawRow {
    content_sha256: String,
    modality: String,
    execution_timeout: Option<PgInterval>,
    estimated_vram_bytes: Option<i64>,
}

/// Resolves a Model alias to its pinned content hash, modality, and limits
/// (ADR 0012). `Ok(None)` means the alias does not exist.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if the resolved row's
/// `content_sha256`, `modality`, `execution_timeout`, or
/// `estimated_vram_bytes` column cannot be decoded.
pub async fn resolve_model_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
) -> sqlx::Result<Option<ResolvedModel>> {
    let Some(raw) = sqlx::query_as::<_, ResolvedModelRawRow>(
        "SELECT model_versions.content_sha256, model_versions.modality, \
                model_versions.execution_timeout, model_versions.estimated_vram_bytes \
         FROM model_aliases \
         JOIN model_versions \
             ON model_versions.tenant_id = model_aliases.tenant_id \
             AND model_versions.content_sha256 = model_aliases.content_sha256 \
         WHERE model_aliases.tenant_id = $1 AND model_aliases.alias = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(alias)
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let content_hash = parse_content_hash("model_aliases.content_sha256", &raw.content_sha256)?;
    let modality = parse_modality("model_versions.modality", &raw.modality)?;
    let limits = decode_model_execution_limits(raw.execution_timeout, raw.estimated_vram_bytes)?;
    Ok(Some(ResolvedModel {
        content_hash,
        modality,
        limits,
    }))
}

#[derive(sqlx::FromRow)]
struct ResolvedWorkflowRawRow {
    content_sha256: String,
    modality: String,
    manifest: serde_json::Value,
}

/// Resolves a Workflow alias to its pinned content hash, modality, manifest,
/// and limits (ADR 0012). `Ok(None)` means the alias does not exist.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails, or if the resolved row's
/// `content_sha256`, `modality`, or `manifest` column cannot be decoded.
pub async fn resolve_workflow_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
) -> sqlx::Result<Option<ResolvedWorkflow>> {
    let Some(raw) = sqlx::query_as::<_, ResolvedWorkflowRawRow>(
        "SELECT workflow_versions.content_sha256, workflow_versions.modality, \
                workflow_versions.manifest \
         FROM workflow_aliases \
         JOIN workflow_versions \
             ON workflow_versions.tenant_id = workflow_aliases.tenant_id \
             AND workflow_versions.content_sha256 = workflow_aliases.content_sha256 \
         WHERE workflow_aliases.tenant_id = $1 AND workflow_aliases.alias = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(alias)
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let content_hash = parse_content_hash("workflow_aliases.content_sha256", &raw.content_sha256)?;
    let modality = parse_modality("workflow_versions.modality", &raw.modality)?;
    let (manifest, limits) = decode_workflow_manifest("workflow_versions.manifest", raw.manifest)?;
    Ok(Some(ResolvedWorkflow {
        content_hash,
        modality,
        limits,
        manifest,
    }))
}

/// Lists every Model alias for a Tenant, alphabetically.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails.
pub async fn list_model_aliases(
    conn: &mut PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<ModelAliasRow>> {
    let rows: Vec<ModelAliasRawRow> = sqlx::query_as(
        "SELECT model_aliases.alias, model_aliases.content_sha256, model_versions.modality, \
                model_aliases.created_at \
         FROM model_aliases \
         JOIN model_versions \
             ON model_versions.tenant_id = model_aliases.tenant_id \
             AND model_versions.content_sha256 = model_aliases.content_sha256 \
         WHERE model_aliases.tenant_id = $1 \
         ORDER BY model_aliases.alias",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(ModelAliasRow::from).collect())
}

/// Lists every Workflow alias for a Tenant, alphabetically.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails.
pub async fn list_workflow_aliases(
    conn: &mut PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<WorkflowAliasRow>> {
    let rows: Vec<WorkflowAliasRawRow> = sqlx::query_as(
        "SELECT workflow_aliases.alias, workflow_aliases.content_sha256, workflow_versions.modality \
         FROM workflow_aliases \
         JOIN workflow_versions \
             ON workflow_versions.tenant_id = workflow_aliases.tenant_id \
             AND workflow_versions.content_sha256 = workflow_aliases.content_sha256 \
         WHERE workflow_aliases.tenant_id = $1 \
         ORDER BY workflow_aliases.alias",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows.into_iter().map(WorkflowAliasRow::from).collect())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use gpq_domain::{ContentHash, ExecutionLimits, MediaKind, WorkflowManifest};
    use serde_json::json;
    use sqlx::postgres::types::PgInterval;

    use super::{StoredWorkflowManifest, workflow_content_hash};

    fn sample_manifest() -> WorkflowManifest {
        WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "IMAGE".to_owned(),
            artifact_kind: MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![ContentHash::digest(b"model")],
            required_custom_nodes: BTreeMap::from([("pkg".to_owned(), "1.0.0".to_owned())]),
        }
    }

    #[test]
    fn content_hash_is_stable_under_key_reordering() {
        let manifest = sample_manifest();
        let graph_a = json!({ "a": 1, "b": { "x": 1, "y": 2 } });
        let graph_b = json!({ "b": { "y": 2, "x": 1 }, "a": 1 });
        let Ok(hash_a) = workflow_content_hash(&graph_a, &manifest) else {
            panic!("hash a");
        };
        let Ok(hash_b) = workflow_content_hash(&graph_b, &manifest) else {
            panic!("hash b");
        };
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn content_hash_changes_with_graph() {
        let manifest = sample_manifest();
        let graph_a = json!({ "a": 1 });
        let graph_b = json!({ "a": 2 });
        let Ok(hash_a) = workflow_content_hash(&graph_a, &manifest) else {
            panic!("hash a");
        };
        let Ok(hash_b) = workflow_content_hash(&graph_b, &manifest) else {
            panic!("hash b");
        };
        assert_ne!(hash_a, hash_b);
    }

    #[test]
    fn content_hash_ignores_limits() {
        // `limits` is not part of `workflow_content_hash`'s inputs at all, so
        // this documents that two registrations differing only in limits
        // hash identically - the first registration's limits win (ADR 0012).
        let manifest = sample_manifest();
        let graph = json!({ "a": 1 });
        let Ok(hash_a) = workflow_content_hash(&graph, &manifest) else {
            panic!("hash a");
        };
        let Ok(hash_b) = workflow_content_hash(&graph, &manifest) else {
            panic!("hash b");
        };
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn stored_manifest_round_trips_through_json() {
        let stored = StoredWorkflowManifest {
            manifest: sample_manifest(),
            limits: ExecutionLimits {
                execution_timeout: Some(Duration::from_secs(30)),
                estimated_vram_bytes: Some(8_000_000_000),
            },
        };
        let Ok(value) = serde_json::to_value(&stored) else {
            panic!("serialize");
        };
        let Ok(back) = serde_json::from_value::<StoredWorkflowManifest>(value) else {
            panic!("deserialize");
        };
        assert_eq!(back.manifest, stored.manifest);
        assert_eq!(back.limits, stored.limits);
    }

    #[test]
    fn interval_rejects_sub_microsecond_precision() {
        let duration = Duration::from_nanos(1);
        assert!(PgInterval::try_from(duration).is_err());
    }
}
