//! Native Catalog service (ADR 0012).
//!
//! Workers register immutable Model Versions by content hash; this service
//! registers immutable Workflow graphs and manifests and maps aliases onto
//! either kind of version. A version or alias is `available` when some
//! online Worker's Slot admits its [`Requirement`] right now
//! (`gpq_domain::any_candidate_remains`); that is recomputed on every read,
//! never stored.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use connectrpc::{ConnectError, ErrorCode, Response, ServiceRequest, ServiceResult};
use gpq_domain::{
    ContentHash, Modality, Requirement, SlotCapability, TenantId, any_candidate_remains,
};
use gpq_proto::gpq::v1::{
    CatalogService, DeleteAliasRequest, DeleteAliasResponse, ListModelsRequest, ListModelsResponse,
    ListWorkersRequest, ListWorkersResponse, ListWorkflowsRequest, ListWorkflowsResponse,
    ModelAlias, ModelVersion, PoolSummary, RegisterWorkflowVersionRequest,
    RegisterWorkflowVersionResponse, SetModelAliasRequest, SetModelAliasResponse,
    SetWorkflowAliasRequest, SetWorkflowAliasResponse, WorkerSummary, WorkflowAlias,
    WorkflowManifest as WireWorkflowManifest, WorkflowVersion,
};

use crate::db::catalog::{CatalogError, ModelVersionRow, WorkflowVersionRow};
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

    /// Every online Worker's advertised Slot, ready for capability matching
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

/// Validates a Workflow manifest before registration: it must name an output
/// node, an output name, and a MIME type, and every custom node it requires
/// must carry an exact version (ADR 0007, ADR 0018 - no version ranges, no
/// "install whatever is present").
fn validate_manifest(manifest: &gpq_domain::WorkflowManifest) -> Result<(), String> {
    if manifest.output_node.trim().is_empty() {
        return Err("manifest.output_node must be set".to_owned());
    }
    if manifest.output_name.trim().is_empty() {
        return Err("manifest.output_name must be set".to_owned());
    }
    if manifest.artifact_mime.trim().is_empty() {
        return Err("manifest.artifact_mime must be set".to_owned());
    }
    for (package, version) in &manifest.required_custom_nodes {
        if package.trim().is_empty() {
            return Err("manifest.required_custom_nodes has an empty package name".to_owned());
        }
        if version.trim().is_empty() {
            return Err(format!("custom node {package:?} has no exact version"));
        }
    }
    Ok(())
}

/// Splits a wire `WorkflowManifest` (which flattens the domain manifest and
/// its execution limits into one message) into the two domain types.
fn workflow_manifest_from_proto(
    manifest: WireWorkflowManifest,
) -> Result<(gpq_domain::WorkflowManifest, gpq_domain::ExecutionLimits), String> {
    let artifact_kind = crate::native::media_kind_from_proto(manifest.artifact_kind)
        .ok_or_else(|| "manifest.artifact_kind must be set".to_owned())?;
    let mut required_models = Vec::with_capacity(manifest.required_model_sha256.len());
    for hex in &manifest.required_model_sha256 {
        let hash = ContentHash::from_str(hex)
            .map_err(|err| format!("invalid required model hash {hex:?}: {err}"))?;
        required_models.push(hash);
    }
    let domain_manifest = gpq_domain::WorkflowManifest {
        output_node: manifest.output_node,
        output_name: manifest.output_name,
        artifact_kind,
        artifact_mime: manifest.artifact_mime,
        required_models,
        required_custom_nodes: manifest.required_custom_nodes.into_iter().collect(),
    };
    let limits = gpq_domain::ExecutionLimits {
        execution_timeout: crate::native::duration_from_proto(manifest.execution_timeout),
        estimated_vram_bytes: (manifest.estimated_vram_bytes != 0)
            .then_some(manifest.estimated_vram_bytes),
    };
    Ok((domain_manifest, limits))
}

/// Rejoins a domain manifest and its limits into the wire shape.
fn workflow_manifest_to_proto(
    manifest: &gpq_domain::WorkflowManifest,
    limits: gpq_domain::ExecutionLimits,
) -> WireWorkflowManifest {
    WireWorkflowManifest {
        output_node: manifest.output_node.clone(),
        output_name: manifest.output_name.clone(),
        artifact_kind: crate::native::media_kind_to_proto(manifest.artifact_kind),
        artifact_mime: manifest.artifact_mime.clone(),
        required_model_sha256: manifest
            .required_models
            .iter()
            .map(ContentHash::to_hex)
            .collect(),
        required_custom_nodes: manifest
            .required_custom_nodes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        estimated_vram_bytes: limits.estimated_vram_bytes.unwrap_or(0),
        execution_timeout: limits
            .execution_timeout
            .map_or_else(Default::default, crate::native::duration_to_proto),
        ..Default::default()
    }
}

fn workflow_version_row_to_proto(row: &WorkflowVersionRow, available: bool) -> WorkflowVersion {
    WorkflowVersion {
        content_sha256: row.content_hash.to_hex(),
        modality: crate::native::modality_to_proto(row.modality),
        manifest: workflow_manifest_to_proto(&row.manifest, row.limits).into(),
        created_at: crate::native::timestamp_to_proto(row.created_at),
        available,
        ..Default::default()
    }
}

fn model_version_row_to_proto(row: &ModelVersionRow, online_worker_count: u32) -> ModelVersion {
    ModelVersion {
        content_sha256: row.content_hash.to_hex(),
        modality: crate::native::modality_to_proto(row.modality),
        execution_timeout: row
            .limits
            .execution_timeout
            .map_or_else(Default::default, crate::native::duration_to_proto),
        estimated_vram_bytes: row.limits.estimated_vram_bytes.unwrap_or(0),
        online_worker_count,
        first_seen_at: crate::native::timestamp_to_proto(row.first_seen_at),
        ..Default::default()
    }
}

/// Counts distinct online Workers whose Slot capabilities advertise
/// `content_hash`.
fn online_worker_count(candidates: &[SlotCapability], content_hash: ContentHash) -> u32 {
    let workers: HashSet<_> = candidates
        .iter()
        .filter(|candidate| candidate.model_versions.contains(&content_hash))
        .map(|candidate| candidate.worker_id)
        .collect();
    u32::try_from(workers.len()).unwrap_or(u32::MAX)
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
    async fn register_workflow_version(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, RegisterWorkflowVersionRequest>,
    ) -> ServiceResult<RegisterWorkflowVersionResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let request = request.to_owned_message();
        let modality = crate::native::modality_from_proto(request.modality)
            .ok_or_else(|| invalid("modality must be set"))?;
        let graph_struct = request.graph.into_option().unwrap_or_default();
        let graph = serde_json::to_value(&graph_struct)
            .map_err(|err| invalid(format!("invalid graph: {err}")))?;
        let wire_manifest = request
            .manifest
            .into_option()
            .ok_or_else(|| invalid("manifest is required"))?;
        let (manifest, limits) = workflow_manifest_from_proto(wire_manifest).map_err(invalid)?;
        validate_manifest(&manifest).map_err(invalid)?;

        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let content_hash = crate::db::catalog::register_workflow_version(
            &mut conn, tenant_id, modality, &graph, &manifest, limits,
        )
        .await
        .map_err(internal)?;
        let Some(row) =
            crate::db::catalog::get_workflow_version_row(&mut conn, tenant_id, content_hash)
                .await
                .map_err(internal)?
        else {
            return Err(internal(
                "workflow version vanished immediately after registration",
            ));
        };
        conn.commit().await.map_err(internal)?;

        let candidates = self.online_candidates(tenant_id).await?;
        let requirement = Requirement::for_workflow(
            tenant_id,
            content_hash,
            &row.manifest,
            row.limits.estimated_vram_bytes,
        );
        let available = any_candidate_remains(&candidates, &requirement);
        Response::ok(RegisterWorkflowVersionResponse {
            version: workflow_version_row_to_proto(&row, available).into(),
            ..Default::default()
        })
    }

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
        let version_row = crate::db::catalog::get_model_version(&mut conn, tenant_id, content_hash)
            .await
            .map_err(internal)?;
        conn.commit().await.map_err(internal)?;
        let vram = version_row.and_then(|row| row.limits.estimated_vram_bytes);
        let candidates = self.online_candidates(tenant_id).await?;
        let requirement = Requirement::for_model(tenant_id, content_hash, vram);
        let available = any_candidate_remains(&candidates, &requirement);
        let modality = Modality::from_str(&alias_row.modality).map_err(internal)?;
        Response::ok(SetModelAliasResponse {
            alias: ModelAlias {
                alias: alias_row.alias,
                content_sha256: alias_row.content_hash,
                modality: crate::native::modality_to_proto(modality),
                available,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
    }

    async fn set_workflow_alias(
        &self,
        ctx: connectrpc::RequestContext,
        request: ServiceRequest<'_, SetWorkflowAliasRequest>,
    ) -> ServiceResult<SetWorkflowAliasResponse> {
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
        let alias_row = crate::db::catalog::set_workflow_alias(
            &mut conn,
            tenant_id,
            &request.alias,
            content_hash,
        )
        .await
        .map_err(catalog_error)?;
        let version_row =
            crate::db::catalog::get_workflow_version_row(&mut conn, tenant_id, content_hash)
                .await
                .map_err(internal)?;
        conn.commit().await.map_err(internal)?;
        let candidates = self.online_candidates(tenant_id).await?;
        let available = version_row.is_some_and(|row| {
            let requirement = Requirement::for_workflow(
                tenant_id,
                content_hash,
                &row.manifest,
                row.limits.estimated_vram_bytes,
            );
            any_candidate_remains(&candidates, &requirement)
        });
        let modality = Modality::from_str(&alias_row.modality).map_err(internal)?;
        Response::ok(SetWorkflowAliasResponse {
            alias: WorkflowAlias {
                alias: alias_row.alias,
                content_sha256: alias_row.content_hash,
                modality: crate::native::modality_to_proto(modality),
                available,
                ..Default::default()
            }
            .into(),
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

    async fn delete_workflow_alias(
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
        let deleted =
            crate::db::catalog::delete_workflow_alias(&mut conn, tenant_id, &request.alias)
                .await
                .map_err(internal)?;
        conn.commit().await.map_err(internal)?;
        if !deleted {
            return Err(ConnectError::new(
                ErrorCode::NotFound,
                format!("workflow alias {:?} not found", request.alias),
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
        let mut availability = HashMap::with_capacity(version_rows.len());
        for row in &version_rows {
            let requirement = Requirement::for_model(
                tenant_id,
                row.content_hash,
                row.limits.estimated_vram_bytes,
            );
            availability.insert(
                row.content_hash,
                any_candidate_remains(&candidates, &requirement),
            );
        }
        let mut versions = Vec::with_capacity(version_rows.len());
        for row in &version_rows {
            let count = online_worker_count(&candidates, row.content_hash);
            versions.push(model_version_row_to_proto(row, count));
        }

        let mut aliases = Vec::with_capacity(alias_rows.len());
        for row in alias_rows {
            let content_hash = ContentHash::from_str(&row.content_hash).map_err(internal)?;
            let modality = Modality::from_str(&row.modality).map_err(internal)?;
            let available = availability.get(&content_hash).copied().unwrap_or(false);
            aliases.push(ModelAlias {
                alias: row.alias,
                content_sha256: row.content_hash,
                modality: crate::native::modality_to_proto(modality),
                available,
                ..Default::default()
            });
        }
        Response::ok(ListModelsResponse {
            aliases,
            versions,
            ..Default::default()
        })
    }

    async fn list_workflows(
        &self,
        ctx: connectrpc::RequestContext,
        _request: ServiceRequest<'_, ListWorkflowsRequest>,
    ) -> ServiceResult<ListWorkflowsResponse> {
        let tenant_id = crate::native::authenticate(&self.state, &ctx).await?;
        let mut conn = self
            .state
            .db
            .begin_tenant(tenant_id)
            .await
            .map_err(internal)?;
        let alias_rows = crate::db::catalog::list_workflow_aliases(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        let version_rows = crate::db::catalog::list_workflow_versions(&mut conn, tenant_id)
            .await
            .map_err(internal)?;
        conn.commit().await.map_err(internal)?;

        let candidates = self.online_candidates(tenant_id).await?;
        let mut availability = HashMap::with_capacity(version_rows.len());
        for row in &version_rows {
            let requirement = Requirement::for_workflow(
                tenant_id,
                row.content_hash,
                &row.manifest,
                row.limits.estimated_vram_bytes,
            );
            availability.insert(
                row.content_hash,
                any_candidate_remains(&candidates, &requirement),
            );
        }
        let mut versions = Vec::with_capacity(version_rows.len());
        for row in &version_rows {
            let available = availability
                .get(&row.content_hash)
                .copied()
                .unwrap_or(false);
            versions.push(workflow_version_row_to_proto(row, available));
        }

        let mut aliases = Vec::with_capacity(alias_rows.len());
        for row in alias_rows {
            let content_hash = ContentHash::from_str(&row.content_hash).map_err(internal)?;
            let modality = Modality::from_str(&row.modality).map_err(internal)?;
            let available = availability.get(&content_hash).copied().unwrap_or(false);
            aliases.push(WorkflowAlias {
                alias: row.alias,
                content_sha256: row.content_hash,
                modality: crate::native::modality_to_proto(modality),
                available,
                ..Default::default()
            });
        }
        Response::ok(ListWorkflowsResponse {
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

        let mut pools_by_worker: HashMap<gpq_domain::WorkerId, Vec<PoolSummary>> = HashMap::new();
        let mut models_by_worker: HashMap<gpq_domain::WorkerId, Vec<String>> = HashMap::new();
        let mut custom_nodes_by_worker: HashMap<
            gpq_domain::WorkerId,
            std::collections::BTreeMap<String, String>,
        > = HashMap::new();
        for pool in pool_rows {
            models_by_worker
                .entry(pool.worker_id)
                .or_default()
                .extend(pool.model_hashes.iter().cloned());
            custom_nodes_by_worker
                .entry(pool.worker_id)
                .or_default()
                .extend(pool.custom_nodes.clone());
            pools_by_worker
                .entry(pool.worker_id)
                .or_default()
                .push(PoolSummary {
                    pool_id: pool.pool_id.as_uuid().to_string(),
                    backend_kind: crate::native::backend_kind_to_proto(pool.backend_kind),
                    backend_version: pool.backend_version,
                    total_slots: pool.total_slots,
                    free_slots: pool.free_slots,
                    resident_model_sha256: pool
                        .resident_model_sha256
                        .map(|hash| hash.to_hex())
                        .unwrap_or_default(),
                    accelerator_memory_bytes: pool.accelerator_memory_bytes.unwrap_or(0),
                    ..Default::default()
                });
        }

        let workers = worker_rows
            .into_iter()
            .map(|row| {
                let worker_id = row.id();
                WorkerSummary {
                    worker_id: worker_id.as_uuid().to_string(),
                    name: row.name,
                    online: self.state.workers.is_online(worker_id),
                    worker_version: row.worker_version,
                    last_seen_at: row
                        .last_seen_at
                        .map_or_else(Default::default, crate::native::timestamp_to_proto),
                    pools: pools_by_worker.remove(&worker_id).unwrap_or_default(),
                    model_sha256: models_by_worker.remove(&worker_id).unwrap_or_default(),
                    custom_nodes: custom_nodes_by_worker
                        .remove(&worker_id)
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use gpq_domain::MediaKind;

    use super::*;

    fn valid_manifest() -> gpq_domain::WorkflowManifest {
        gpq_domain::WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "IMAGE".to_owned(),
            artifact_kind: MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: Vec::new(),
            required_custom_nodes: BTreeMap::from([("pkg".to_owned(), "1.2.3".to_owned())]),
        }
    }

    #[test]
    fn valid_manifest_passes() {
        assert!(validate_manifest(&valid_manifest()).is_ok());
    }

    #[test]
    fn missing_output_node_is_rejected() {
        let mut manifest = valid_manifest();
        manifest.output_node.clear();
        assert!(validate_manifest(&manifest).is_err());
    }

    #[test]
    fn missing_mime_type_is_rejected() {
        let mut manifest = valid_manifest();
        manifest.artifact_mime.clear();
        assert!(validate_manifest(&manifest).is_err());
    }

    #[test]
    fn custom_node_without_exact_version_is_rejected() {
        let mut manifest = valid_manifest();
        manifest
            .required_custom_nodes
            .insert("other_pkg".to_owned(), String::new());
        assert!(validate_manifest(&manifest).is_err());
    }

    #[test]
    fn workflow_manifest_round_trips_through_proto() {
        let manifest = valid_manifest();
        let limits = gpq_domain::ExecutionLimits {
            execution_timeout: Some(std::time::Duration::from_mins(2)),
            estimated_vram_bytes: Some(4_000_000_000),
        };
        let wire = workflow_manifest_to_proto(&manifest, limits);
        let Ok((back_manifest, back_limits)) = workflow_manifest_from_proto(wire) else {
            panic!("round trip should decode");
        };
        assert_eq!(back_manifest, manifest);
        assert_eq!(back_limits, limits);
    }

    #[test]
    fn workflow_manifest_from_proto_rejects_unset_artifact_kind() {
        let wire = WireWorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "IMAGE".to_owned(),
            artifact_mime: "image/png".to_owned(),
            ..Default::default()
        };
        assert!(workflow_manifest_from_proto(wire).is_err());
    }
}
