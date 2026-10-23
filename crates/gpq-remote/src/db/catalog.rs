//! Native Model catalog persistence (ADR 0012).
//!
//! Workers advertise immutable [`ModelManifest`]s; Remote recomputes each
//! manifest's canonical hash, rejects any mismatch, and stores the manifest as
//! the Model Version identified by that hash. Tenant APIs map aliases onto
//! those versions. Alias deletion or reassignment never mutates a version or
//! any past Generation: aliases are mutable pointers, versions are append-only
//! and referenced by content hash everywhere else in the system.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use gpq_domain::{ContentHash, ExecutionLimits, Modality, ModelManifest, TenantId};
use sqlx::PgConnection;
use sqlx::postgres::types::PgInterval;

/// A Model Version resolved from an alias at admission (ADR 0012).
pub struct ResolvedModel {
    /// Canonical manifest hash of the pinned Model Version.
    pub content_hash: ContentHash,
    /// The immutable manifest every Attempt of the Generation runs against.
    pub manifest: ModelManifest,
    /// Version-declared execution limits.
    pub limits: ExecutionLimits,
}

/// One registered Model alias.
pub struct ModelAliasRow {
    /// The alias name.
    pub alias: String,
    /// Canonical manifest hash of the aliased Model Version.
    pub content_hash: ContentHash,
    /// Modality the aliased version serves.
    pub modality: Modality,
    /// When the alias was first created.
    pub created_at: DateTime<Utc>,
}

/// One registered Model Version, as advertised by capable Workers.
pub struct ModelVersionRow {
    /// Canonical manifest hash of the model material.
    pub content_hash: ContentHash,
    /// The immutable manifest.
    pub manifest: ModelManifest,
    /// Version-declared execution limits.
    pub limits: ExecutionLimits,
    /// When a Worker first registered this content hash.
    pub first_seen_at: DateTime<Utc>,
}

/// Why a catalog write failed.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// No Model Version is registered under this hash for the Tenant.
    #[error("content hash {0} is not a registered model version")]
    UnknownVersion(ContentHash),
    /// A database error unrelated to the alias target.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
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

fn decode_limits(
    execution_timeout: Option<PgInterval>,
    estimated_memory_bytes: Option<i64>,
) -> Result<ExecutionLimits, sqlx::Error> {
    let execution_timeout = execution_timeout
        .map(|interval| super::interval_to_duration("model_versions.execution_timeout", interval))
        .transpose()?;
    let estimated_memory_bytes = estimated_memory_bytes
        .map(|bytes| {
            u64::try_from(bytes)
                .map_err(|err| decode_error("model_versions.estimated_memory_bytes", err))
        })
        .transpose()?;
    Ok(ExecutionLimits {
        execution_timeout,
        estimated_memory_bytes,
    })
}

fn decode_manifest(value: serde_json::Value) -> Result<ModelManifest, sqlx::Error> {
    serde_json::from_value(value).map_err(|err| decode_error("model_versions.manifest", err))
}

#[derive(sqlx::FromRow)]
struct ModelVersionRaw {
    content_sha256: String,
    manifest: serde_json::Value,
    execution_timeout: Option<PgInterval>,
    estimated_memory_bytes: Option<i64>,
    first_seen_at: DateTime<Utc>,
}

impl ModelVersionRaw {
    fn into_row(self) -> Result<ModelVersionRow, sqlx::Error> {
        Ok(ModelVersionRow {
            content_hash: parse_content_hash(
                "model_versions.content_sha256",
                &self.content_sha256,
            )?,
            manifest: decode_manifest(self.manifest)?,
            limits: decode_limits(self.execution_timeout, self.estimated_memory_bytes)?,
            first_seen_at: self.first_seen_at,
        })
    }
}

/// Registers immutable Model Versions advertised by a Worker. Each entry's
/// hash must be the manifest's canonical hash; callers verify that before
/// reaching here. Re-registering an existing version is a no-op.
///
/// # Errors
/// Returns [`sqlx::Error`] if a manifest cannot be encoded or the insert fails.
pub async fn register_model_versions(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    versions: &[(ContentHash, ModelManifest)],
) -> sqlx::Result<()> {
    for (hash, manifest) in versions {
        let family = serde_json::to_value(manifest.family)
            .map_err(|err| decode_error("model_versions.family", err))?;
        let family = family
            .as_str()
            .ok_or_else(|| decode_error("model_versions.family", "family is not a string"))?
            .to_owned();
        let manifest_json = serde_json::to_value(manifest)
            .map_err(|err| decode_error("model_versions.manifest", err))?;
        sqlx::query(
            "INSERT INTO model_versions \
                 (tenant_id, id, content_sha256, family, modality, manifest) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, content_sha256) DO NOTHING",
        )
        .bind(tenant_id.as_uuid())
        .bind(uuid::Uuid::now_v7())
        .bind(hash.to_hex())
        .bind(family)
        .bind(manifest.modality().as_str())
        .bind(manifest_json)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Lists every registered Model Version, oldest first.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or a row cannot be decoded.
pub async fn list_model_versions(
    conn: &mut PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<ModelVersionRow>> {
    let rows: Vec<ModelVersionRaw> = sqlx::query_as(
        "SELECT content_sha256, manifest, execution_timeout, estimated_memory_bytes, first_seen_at \
         FROM model_versions WHERE tenant_id = $1 ORDER BY first_seen_at",
    )
    .bind(tenant_id.as_uuid())
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter().map(ModelVersionRaw::into_row).collect()
}

/// Fetches one registered Model Version by canonical hash.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or the row cannot be decoded.
pub async fn get_model_version(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    content_hash: ContentHash,
) -> sqlx::Result<Option<ModelVersionRow>> {
    let raw: Option<ModelVersionRaw> = sqlx::query_as(
        "SELECT content_sha256, manifest, execution_timeout, estimated_memory_bytes, first_seen_at \
         FROM model_versions WHERE tenant_id = $1 AND content_sha256 = $2",
    )
    .bind(tenant_id.as_uuid())
    .bind(content_hash.to_hex())
    .fetch_optional(&mut *conn)
    .await?;
    raw.map(ModelVersionRaw::into_row).transpose()
}

/// Points a Model alias at an already-registered content hash (ADR 0012).
/// Reassigning an existing alias never touches past Generations, which
/// pinned the previous hash at admission time.
///
/// # Errors
/// Returns [`CatalogError::UnknownVersion`] if `content_hash` has no
/// registered Model Version for `tenant_id`, or [`CatalogError::Database`].
pub async fn set_model_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
    content_hash: ContentHash,
) -> Result<ModelAliasRow, CatalogError> {
    let Some(version) = get_model_version(conn, tenant_id, content_hash).await? else {
        return Err(CatalogError::UnknownVersion(content_hash));
    };
    let (created_at,): (DateTime<Utc>,) = sqlx::query_as(
        "INSERT INTO model_aliases (tenant_id, alias, content_sha256) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (tenant_id, alias) \
         DO UPDATE SET content_sha256 = excluded.content_sha256, updated_at = now() \
         RETURNING created_at",
    )
    .bind(tenant_id.as_uuid())
    .bind(alias)
    .bind(content_hash.to_hex())
    .fetch_one(&mut *conn)
    .await?;
    Ok(ModelAliasRow {
        alias: alias.to_owned(),
        content_hash,
        modality: version.manifest.modality(),
        created_at,
    })
}

/// Deletes a Model alias. Returns whether an alias existed to delete.
///
/// # Errors
/// Returns [`sqlx::Error`] if the delete fails.
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

#[derive(sqlx::FromRow)]
struct ResolvedModelRaw {
    content_sha256: String,
    manifest: serde_json::Value,
    execution_timeout: Option<PgInterval>,
    estimated_memory_bytes: Option<i64>,
}

/// Resolves a Model alias to its pinned manifest and limits (ADR 0012).
/// `Ok(None)` means the alias does not exist.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or the row cannot be decoded.
pub async fn resolve_model_alias(
    conn: &mut PgConnection,
    tenant_id: TenantId,
    alias: &str,
) -> sqlx::Result<Option<ResolvedModel>> {
    let Some(raw) = sqlx::query_as::<_, ResolvedModelRaw>(
        "SELECT model_versions.content_sha256, model_versions.manifest, \
                model_versions.execution_timeout, model_versions.estimated_memory_bytes \
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
    Ok(Some(ResolvedModel {
        content_hash: parse_content_hash("model_versions.content_sha256", &raw.content_sha256)?,
        manifest: decode_manifest(raw.manifest)?,
        limits: decode_limits(raw.execution_timeout, raw.estimated_memory_bytes)?,
    }))
}

#[derive(sqlx::FromRow)]
struct ModelAliasRaw {
    alias: String,
    content_sha256: String,
    modality: String,
    created_at: DateTime<Utc>,
}

/// Lists every Model alias for a Tenant, alphabetically.
///
/// # Errors
/// Returns [`sqlx::Error`] if the query fails or a row cannot be decoded.
pub async fn list_model_aliases(
    conn: &mut PgConnection,
    tenant_id: TenantId,
) -> sqlx::Result<Vec<ModelAliasRow>> {
    let rows: Vec<ModelAliasRaw> = sqlx::query_as(
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
    rows.into_iter()
        .map(|row| {
            Ok(ModelAliasRow {
                alias: row.alias,
                content_hash: parse_content_hash(
                    "model_aliases.content_sha256",
                    &row.content_sha256,
                )?,
                modality: parse_modality("model_versions.modality", &row.modality)?,
                created_at: row.created_at,
            })
        })
        .collect()
}
