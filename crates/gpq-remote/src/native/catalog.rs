//! Native Catalog service (ADR 0012).
//!
//! Workers register immutable Model Versions by content hash, and this service
//! lists them and maps Tenant aliases onto them. A version or alias is
//! `available` when some online Worker's Slot advertises that manifest; that is
//! recomputed on every read, never stored.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use buffa::{EnumValue, MessageField};
use connectrpc::{ConnectError, ErrorCode, Response, ServiceRequest, ServiceResult};
use gpq_domain::{ContentHash, ModelManifest, SlotCapability, TenantId};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::v1::{
    CatalogService, DeleteAliasRequest, DeleteAliasResponse, ListModelsRequest, ListModelsResponse,
    ListWorkersRequest, ListWorkersResponse, SetModelAliasRequest, SetModelAliasResponse,
};

use crate::db::catalog::{CatalogError, ModelAliasRow, ModelVersionRow};
use crate::state::AppState;

/// `CatalogService` implementation backed by `db::catalog` and `db::workers`.
pub struct CatalogApi {
    state: AppState,
}

impl CatalogApi {
    /// Builds the service over shared application state.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Every online Worker's advertised Slot, ready for availability checks
    /// (ADR 0012). Recomputed per call; nothing here is cached.
    async fn online_candidates(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<SlotCapability>, ConnectError> {
        let online: HashSet<_> = self
            .state
            .workers
            .online_workers(tenant_id)
            .into_iter()
            .collect();
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let pools = crate::db::workers::pool_capabilities(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        conn.commit().await.map_err(internal)?;
        Ok(pools
            .into_iter()
            .filter(|(_, _, worker_id, _)| online.contains(worker_id))
            .map(|(capability, _, _, _)| capability)
            .collect())
    }
}

fn internal(err: impl std::fmt::Display) -> ConnectError {
    ConnectError::new(ErrorCode::Internal, err.to_string())
}

fn invalid(err: impl std::fmt::Display) -> ConnectError {
    ConnectError::new(ErrorCode::InvalidArgument, err.to_string())
}

/// Whether some online Worker's Slot advertises `content_hash` right now.
/// Slots only list the versions they can run, so advertisement is availability.
fn model_available(candidates: &[SlotCapability], content_hash: ContentHash) -> bool {
    candidates
        .iter()
        .any(|candidate| candidate.model_manifests.contains_key(&content_hash))
}

/// Counts distinct online Workers whose Slot capabilities advertise
/// `content_hash`.
fn online_worker_count(candidates: &[SlotCapability], content_hash: ContentHash) -> u32 {
    let workers: HashSet<_> = candidates
        .iter()
        .filter(|candidate| candidate.model_manifests.contains_key(&content_hash))
        .map(|candidate| candidate.worker_id)
        .collect();
    u32::try_from(workers.len()).unwrap_or(u32::MAX)
}

fn model_family_to_proto(family: gpq_domain::ModelFamily) -> EnumValue<pb::ModelFamily> {
    use gpq_domain::ModelFamily as D;
    EnumValue::Known(match family {
        D::QwenImage21 => pb::ModelFamily::MODEL_FAMILY_QWEN_IMAGE21,
        D::MinimaxH3 => pb::ModelFamily::MODEL_FAMILY_MINIMAX_H3,
    })
}

fn model_asset_role_to_proto(role: gpq_domain::ModelAssetRole) -> EnumValue<pb::ModelAssetRole> {
    use gpq_domain::ModelAssetRole as D;
    EnumValue::Known(match role {
        D::Diffusion => pb::ModelAssetRole::MODEL_ASSET_ROLE_DIFFUSION,
        D::TextEncoder => pb::ModelAssetRole::MODEL_ASSET_ROLE_TEXT_ENCODER,
        D::VisionEncoder => pb::ModelAssetRole::MODEL_ASSET_ROLE_VISION_ENCODER,
        D::Vae => pb::ModelAssetRole::MODEL_ASSET_ROLE_VAE,
        D::AudioVae => pb::ModelAssetRole::MODEL_ASSET_ROLE_AUDIO_VAE,
    })
}

/// Converts an immutable domain manifest to its wire shape.
fn model_manifest_to_proto(manifest: &ModelManifest) -> pb::ModelManifest {
    pb::ModelManifest {
        family: model_family_to_proto(manifest.family),
        assets: manifest
            .assets
            .iter()
            .map(|(role, hash)| pb::ModelAsset {
                role: model_asset_role_to_proto(*role),
                sha256: hash.to_hex(),
                ..Default::default()
            })
            .collect(),
        default_loras: manifest
            .default_loras
            .iter()
            .map(|lora| pb::LoraSelection {
                sha256: lora.sha256.to_hex(),
                scale: lora.scale,
                ..Default::default()
            })
            .collect(),
        sampling: MessageField::some(pb::SamplingPreset {
            steps: manifest.sampling.steps,
            cfg_scale: manifest.sampling.cfg_scale,
            sigmas: manifest.sampling.sigmas.clone(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn model_version_row_to_proto(row: &ModelVersionRow, online_worker_count: u32) -> pb::ModelVersion {
    pb::ModelVersion {
        content_sha256: row.content_hash.to_hex(),
        manifest: model_manifest_to_proto(&row.manifest).into(),
        execution_timeout: row
            .limits
            .execution_timeout
            .map_or_else(buffa::MessageField::none, crate::native::duration_to_proto),
        estimated_memory_bytes: row.limits.estimated_memory_bytes,
        online_worker_count,
        first_seen_at: crate::native::timestamp_to_proto(row.first_seen_at),
        ..Default::default()
    }
}

fn model_alias_to_proto(row: ModelAliasRow, available: bool) -> pb::ModelAlias {
    pb::ModelAlias {
        alias: row.alias,
        content_sha256: row.content_hash.to_hex(),
        available,
        ..Default::default()
    }
}

/// `ServiceResult<T>` names a concrete response type at every call site
/// below, while `CatalogService`'s generated trait methods declare an
/// opaque `impl Encodable<T> + Send` return; that is a deliberate, harmless
/// refinement rustc's `refining_impl_trait` warns about only because a
/// generic caller could otherwise observe a narrower type than the trait
/// promises — impossible here since this is a binary crate (no `lib.rs`)
/// with no external consumer of `CatalogService` at all.
#[expect(
    refining_impl_trait_reachable,
    reason = "binary crate: CatalogService has no external caller that could observe the refinement"
)]
impl CatalogService for CatalogApi {
    async fn set_model_alias(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, SetModelAliasRequest>,
    ) -> ServiceResult<SetModelAliasResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let content_hash = ContentHash::from_str(&request.content_sha256)
            .map_err(|err| invalid(format!("invalid content_sha256: {err}")))?;
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let alias_row =
            crate::db::catalog::set_model_alias(&mut conn, tenant_id, &request.alias, content_hash)
                .await
                .map_err(catalog_error)?;
        conn.commit().await.map_err(internal)?;
        let candidates = self.online_candidates(tenant_id).await?;
        let available = model_available(&candidates, content_hash);
        Response::ok(SetModelAliasResponse {
            alias: model_alias_to_proto(alias_row, available).into(),
            ..Default::default()
        })
    }

    async fn delete_model_alias(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, DeleteAliasRequest>,
    ) -> ServiceResult<DeleteAliasResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let deleted = crate::db::catalog::delete_model_alias(&mut conn, tenant_id, &request.alias)
            .await
            .map_err(internal)?;
        conn.commit().await.map_err(internal)?;
        if !deleted {
            return Err(ConnectError::new(
                ErrorCode::NotFound,
                format!("model alias {:?} not found", request.alias),
            ));
        }
        Response::ok(DeleteAliasResponse::default())
    }

    async fn list_models(
        &self,
        ctx: connectrpc::RequestContext,
        _request: ServiceRequest<'_, ListModelsRequest>,
    ) -> ServiceResult<ListModelsResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let alias_rows = crate::db::catalog::list_model_aliases(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        let version_rows = crate::db::catalog::list_model_versions(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        conn.commit().await.map_err(internal)?;

        let candidates = self.online_candidates(tenant_id).await?;
        let versions = version_rows
            .iter()
            .map(|row| {
                model_version_row_to_proto(row, online_worker_count(&candidates, row.content_hash))
            })
            .collect();
        let aliases = alias_rows
            .into_iter()
            .map(|row| {
                let available = model_available(&candidates, row.content_hash);
                model_alias_to_proto(row, available)
            })
            .collect();
        Response::ok(ListModelsResponse {
            aliases,
            versions,
            ..Default::default()
        })
    }

    async fn list_workers(
        &self,
        ctx: connectrpc::RequestContext,
        _request: ServiceRequest<'_, ListWorkersRequest>,
    ) -> ServiceResult<ListWorkersResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let worker_rows = crate::db::workers::list_workers(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        let pool_rows = crate::db::workers::list_pools(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        conn.commit().await.map_err(internal)?;

        let mut pools_by_worker: HashMap<gpq_domain::WorkerId, Vec<pb::PoolSummary>> =
            HashMap::new();
        let mut models_by_worker: HashMap<gpq_domain::WorkerId, Vec<String>> = HashMap::new();
        for pool in pool_rows {
            models_by_worker
                .entry(pool.worker_id)
                .or_default()
                .extend(pool.model_hashes.iter().map(ContentHash::to_hex));
            pools_by_worker
                .entry(pool.worker_id)
                .or_default()
                .push(pb::PoolSummary {
                    pool_id: pool.pool_id.as_uuid().to_string(),
                    accelerator: pool.accelerator,
                    backend_version: pool.accelerator_version,
                    total_slots: pool.total_slots,
                    free_slots: pool.free_slots,
                    resident_model_sha256: pool
                        .resident_model_sha256
                        .map(|hash| hash.to_hex())
                        .unwrap_or_default(),
                    accelerator_memory_bytes: pool.accelerator_memory_bytes,
                    ..Default::default()
                });
        }

        let workers = worker_rows
            .into_iter()
            .map(|row| {
                let worker_id = row.id();
                pb::WorkerSummary {
                    worker_id: worker_id.as_uuid().to_string(),
                    name: row.name,
                    online: self.state.workers.is_online(worker_id),
                    worker_version: row.worker_version,
                    last_seen_at: row
                        .last_seen_at
                        .map_or_else(Default::default, crate::native::timestamp_to_proto),
                    pools: pools_by_worker.remove(&worker_id).unwrap_or_default(),
                    model_sha256: models_by_worker.remove(&worker_id).unwrap_or_default(),
                    ..Default::default()
                }
            })
            .collect();
        Response::ok(ListWorkersResponse {
            workers,
            ..Default::default()
        })
    }
}

fn catalog_error(err: CatalogError) -> ConnectError {
    match err {
        CatalogError::UnknownVersion(hash) => ConnectError::new(
            ErrorCode::NotFound,
            format!("content hash {hash} is not a registered version"),
        ),
        CatalogError::Database(err) => internal(err),
    }
}
