//! Generation admission (ADR 0002, ADR 0003, ADR 0006, ADR 0008, ADR 0012).
//!
//! Admission resolves a Model or Workflow alias to its pinned immutable
//! Version, derives the modality and resolves the execution timeout, enforces
//! Tenant policy, and inserts the new Generation `Queued`. It never leases or
//! executes anything itself — [`crate::scheduler`] picks queued work up
//! separately, woken by [`crate::state::AppState::scheduler`].

use std::time::Duration;

use gpq_domain::{
    ArtifactId, ArtifactPlacement, CallerKind, ExecutionTarget, Priority, Requirement, TenantId,
    TenantSettings, any_candidate_remains, resolve_execution_timeout,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::generations::{self, GenerationRow, NewGeneration};
use crate::state::AppState;

/// Which catalog a requested alias names (ADR 0012).
#[derive(Debug, Clone)]
pub enum AliasTarget {
    /// An LLM Model alias.
    Model(String),
    /// A `ComfyUI` Workflow alias.
    Workflow(String),
}

/// A request to admit one new Generation.
#[derive(Debug, Clone)]
pub struct AdmissionRequest {
    /// The Model or Workflow alias to resolve.
    pub alias_target: AliasTarget,
    /// Opaque backend-shaped payload (ADR 0007).
    pub parameters: serde_json::Value,
    /// Input Artifacts the Attempt must read before execution.
    pub input_artifact_ids: Vec<ArtifactId>,
    /// Where output Artifacts land.
    pub output_placement: ArtifactPlacement,
    /// Requested priority; the Tenant default applies when absent.
    pub priority: Option<Priority>,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
    /// A caller-requested execution timeout; it may only shorten the resolved
    /// default (ADR 0003).
    pub execution_timeout: Option<Duration>,
    /// Whether the caller holds a connection open for the whole Generation.
    pub caller_kind: CallerKind,
    /// Whether the caller wants incremental LLM token events.
    pub stream_tokens: bool,
    /// Deduplicates a retried admission call carrying the same request.
    pub idempotency_key: Option<String>,
}

/// Why admission rejected a request.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// The Model or Workflow alias does not resolve to any Version.
    #[error("unknown model or workflow alias")]
    UnknownAlias,
    /// No capable Worker is online for a synchronous caller (ADR 0006's
    /// `503 model_not_available`); busy online Workers do not trigger this.
    #[error("no capable worker is online")]
    Unavailable,
    /// The request itself is unusable, independent of Worker availability.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The Tenant already has `max_queued_generations` nonterminal Generations.
    #[error("tenant queue capacity exceeded")]
    CapacityExceeded,
    /// The request needs object storage, but Remote has none configured (ADR 0008).
    #[error("object storage is not configured")]
    ObjectStoreUnavailable,
    /// An unexpected internal fault.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

fn internal(error: sqlx::Error) -> AdmissionError {
    AdmissionError::Internal(error.into())
}

/// Deterministically hashes the parts of a request an idempotency key must
/// match on replay, so a reused key with a different payload is rejected
/// instead of silently returning the wrong Generation.
fn idempotency_digest(request: &AdmissionRequest) -> Vec<u8> {
    #[derive(Serialize)]
    struct DigestPayload<'a> {
        alias_kind: &'static str,
        alias: &'a str,
        parameters: &'a serde_json::Value,
        input_artifact_ids: Vec<String>,
        output_placement: &'static str,
        priority: Option<u8>,
        seed: Option<u64>,
        execution_timeout_secs: Option<u64>,
        stream_tokens: bool,
    }

    let (alias_kind, alias) = match &request.alias_target {
        AliasTarget::Model(alias) => ("model", alias.as_str()),
        AliasTarget::Workflow(alias) => ("workflow", alias.as_str()),
    };
    let payload = DigestPayload {
        alias_kind,
        alias,
        parameters: &request.parameters,
        input_artifact_ids: request
            .input_artifact_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        output_placement: request.output_placement.as_str(),
        priority: request.priority.map(u8::from),
        seed: request.seed,
        execution_timeout_secs: request.execution_timeout.map(|d| d.as_secs()),
        stream_tokens: request.stream_tokens,
    };
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    Sha256::digest(bytes).to_vec()
}

/// Replays an already-admitted Generation for a reused idempotency key, or
/// `None` when `key` has not been seen before.
///
/// # Errors
/// Returns [`AdmissionError::InvalidInput`] when `key` was previously used
/// with a request that hashes differently, and [`AdmissionError::Internal`]
/// on a database fault or a dangling key referencing a missing Generation.
async fn replay_idempotent(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    key: &str,
    request: &AdmissionRequest,
) -> Result<Option<GenerationRow>, AdmissionError> {
    #[derive(sqlx::FromRow)]
    struct ExistingKey {
        request_digest: Vec<u8>,
        generation_id: Uuid,
    }
    let Some(existing) = sqlx::query_as::<_, ExistingKey>(
        "SELECT request_digest, generation_id FROM idempotency_keys \
         WHERE tenant_id = $1 AND key = $2",
    )
    .bind(tenant.as_uuid())
    .bind(key)
    .fetch_optional(&mut *tx)
    .await
    .map_err(internal)?
    else {
        return Ok(None);
    };
    if existing.request_digest != idempotency_digest(request) {
        return Err(AdmissionError::InvalidInput(
            "idempotency key was already used with a different request".to_owned(),
        ));
    }
    let existing_id = gpq_domain::GenerationId::from_uuid(existing.generation_id);
    let row = generations::get(tx, tenant, existing_id)
        .await
        .map_err(internal)?
        .ok_or_else(|| {
            AdmissionError::Internal(anyhow::anyhow!(
                "idempotency key referenced a missing generation"
            ))
        })?;
    Ok(Some(row))
}

/// Whether one already-fetched input Artifact row is rejected by admission
/// (ADR 0002): it must be an `input`-direction Artifact, and fit within the
/// Tenant's size limit. Extracted from the per-id database fetch so the
/// per-row gate is unit-testable without a database.
fn validate_input_artifact_row(
    artifact_id: ArtifactId,
    artifact: &crate::db::artifacts::ArtifactRow,
    max_input_artifact_bytes: u64,
) -> Result<(), AdmissionError> {
    if artifact.direction != crate::db::artifacts::ArtifactDirection::Input {
        return Err(AdmissionError::InvalidInput(format!(
            "artifact {artifact_id} is not an input artifact"
        )));
    }
    if !artifact.manifest.fits_within(max_input_artifact_bytes) {
        return Err(AdmissionError::InvalidInput(format!(
            "input artifact {artifact_id} exceeds the tenant's size limit"
        )));
    }
    Ok(())
}

/// Rejects a request whose input Artifacts do not exist, are not `input`
/// direction, or exceed the Tenant's size limit.
async fn validate_input_artifacts(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    request: &AdmissionRequest,
    settings: &TenantSettings,
) -> Result<(), AdmissionError> {
    for artifact_id in &request.input_artifact_ids {
        let Some(artifact) = crate::db::artifacts::get(tx, tenant, *artifact_id)
            .await
            .map_err(internal)?
        else {
            return Err(AdmissionError::InvalidInput(format!(
                "input artifact {artifact_id} does not exist"
            )));
        };
        validate_input_artifact_row(*artifact_id, &artifact, settings.max_input_artifact_bytes)?;
    }
    Ok(())
}

/// Resolves a requested alias to its pinned Version, modality, execution
/// limits, and scheduling [`Requirement`] (ADR 0012).
async fn resolve_target(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    alias_target: &AliasTarget,
) -> Result<
    (
        ExecutionTarget,
        gpq_domain::Modality,
        gpq_domain::ExecutionLimits,
        Requirement,
    ),
    AdmissionError,
> {
    match alias_target {
        AliasTarget::Model(alias) => {
            let Some(resolved) = crate::db::catalog::resolve_model_alias(tx, tenant, alias)
                .await
                .map_err(internal)?
            else {
                return Err(AdmissionError::UnknownAlias);
            };
            let target = ExecutionTarget::Model {
                version: resolved.content_hash,
            };
            let requirement = Requirement::for_model(
                tenant,
                resolved.content_hash,
                resolved.limits.estimated_vram_bytes,
            );
            Ok((target, resolved.modality, resolved.limits, requirement))
        }
        AliasTarget::Workflow(alias) => {
            let Some(resolved) = crate::db::catalog::resolve_workflow_alias(tx, tenant, alias)
                .await
                .map_err(internal)?
            else {
                return Err(AdmissionError::UnknownAlias);
            };
            let target = ExecutionTarget::Workflow {
                version: resolved.content_hash,
            };
            let requirement = Requirement::for_workflow(
                tenant,
                resolved.content_hash,
                &resolved.manifest,
                resolved.limits.estimated_vram_bytes,
            );
            Ok((target, resolved.modality, resolved.limits, requirement))
        }
    }
}

/// Whether the Tenant's queue-capacity gate rejects a new admission (ADR
/// 0002): `max_queued_generations` counts every nonterminal Generation, not
/// just `Queued` ones, and the limit is inclusive — reaching it exactly
/// already rejects the next admission.
fn queue_capacity_exceeded(nonterminal: i64, max_queued_generations: u32) -> bool {
    u64::try_from(nonterminal).unwrap_or(u64::MAX) >= u64::from(max_queued_generations)
}

/// Rejects a synchronous caller's request when no online Worker could ever
/// satisfy `requirement` (ADR 0006's `model_not_available`); a capable but
/// busy fleet does not reject here.
async fn ensure_synchronous_capacity(
    state: &AppState,
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    requirement: &Requirement,
) -> Result<(), AdmissionError> {
    let capabilities = crate::db::workers::pool_capabilities(tx, tenant)
        .await
        .map_err(internal)?;
    let online_capable = capabilities
        .iter()
        .filter(|(_, _, worker_id, _)| state.workers.is_online(*worker_id))
        .map(|(capability, ..)| capability);
    if any_candidate_remains(online_capable, requirement) {
        Ok(())
    } else {
        Err(AdmissionError::Unavailable)
    }
}

/// Links every requested input Artifact to the freshly inserted Generation.
async fn link_input_artifacts(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    generation: gpq_domain::GenerationId,
    input_artifact_ids: &[ArtifactId],
) -> Result<(), AdmissionError> {
    if input_artifact_ids.is_empty() {
        return Ok(());
    }
    let ids: Vec<Uuid> = input_artifact_ids.iter().map(ArtifactId::as_uuid).collect();
    sqlx::query(
        "UPDATE artifacts SET generation_id = $1 \
         WHERE tenant_id = $2 AND id = ANY($3) AND direction = 'input'",
    )
    .bind(generation.as_uuid())
    .bind(tenant.as_uuid())
    .bind(&ids)
    .execute(tx)
    .await
    .map_err(internal)?;
    Ok(())
}

/// Records `key` against the freshly inserted Generation so a retried
/// admission call with the same key replays it instead of duplicating work.
async fn record_idempotency_key(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    key: &str,
    request: &AdmissionRequest,
    generation: gpq_domain::GenerationId,
) -> Result<(), AdmissionError> {
    let digest = idempotency_digest(request);
    sqlx::query(
        "INSERT INTO idempotency_keys (tenant_id, key, request_digest, generation_id) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(tenant.as_uuid())
    .bind(key)
    .bind(digest)
    .bind(generation.as_uuid())
    .execute(tx)
    .await
    .map_err(internal)?;
    Ok(())
}

/// Whether admission must reject `output_placement` outright because it
/// needs S3 and Remote has none configured (ADR 0008).
fn requires_missing_object_store(
    output_placement: ArtifactPlacement,
    object_store_available: bool,
) -> bool {
    output_placement.requires_object_store() && !object_store_available
}

/// Admits a new Generation (ADR 0002, ADR 0003, ADR 0006, ADR 0008, ADR 0012).
///
/// # Errors
///
/// Returns [`AdmissionError::ObjectStoreUnavailable`] if `request.output_placement`
/// needs object storage and this Remote has none configured (ADR 0008);
/// [`AdmissionError::UnknownAlias`] if the Model or Workflow alias does not
/// resolve to any Version; [`AdmissionError::CapacityExceeded`] if the
/// Tenant already has `max_queued_generations` nonterminal Generations;
/// [`AdmissionError::Unavailable`] if the request is synchronous and no
/// capable Worker is online; [`AdmissionError::InvalidInput`] if an input
/// Artifact reference is invalid, mismatched in direction, over the
/// Tenant's size limit, or if `request.seed` does not fit a signed 64-bit
/// column; and [`AdmissionError::Internal`] on a database failure, a
/// dangling idempotency key referencing a missing Generation, or missing
/// Tenant settings.
pub async fn admit(
    state: &AppState,
    tenant: TenantId,
    request: AdmissionRequest,
) -> Result<GenerationRow, AdmissionError> {
    if requires_missing_object_store(
        request.output_placement,
        state.artifacts.object_store_available(),
    ) {
        return Err(AdmissionError::ObjectStoreUnavailable);
    }

    let mut tx = state.db.begin_tenant(tenant).await.map_err(internal)?;

    if let Some(key) = request.idempotency_key.as_deref()
        && let Some(row) = replay_idempotent(&mut tx, tenant, key, &request).await?
    {
        tx.commit().await.map_err(internal)?;
        return Ok(row);
    }

    let settings = crate::db::tenants::settings(&mut tx, tenant)
        .await
        .map_err(internal)?
        .ok_or_else(|| AdmissionError::Internal(anyhow::anyhow!("tenant settings not found")))?;

    let nonterminal = generations::count_nonterminal(&mut tx, tenant)
        .await
        .map_err(internal)?;
    if queue_capacity_exceeded(nonterminal, settings.max_queued_generations) {
        return Err(AdmissionError::CapacityExceeded);
    }

    validate_input_artifacts(&mut tx, tenant, &request, &settings).await?;

    let (target, modality, limits, requirement) =
        resolve_target(&mut tx, tenant, &request.alias_target).await?;

    if request.caller_kind == CallerKind::Synchronous {
        ensure_synchronous_capacity(state, &mut tx, tenant, &requirement).await?;
    }

    let execution_timeout = resolve_execution_timeout(
        modality,
        limits,
        request.execution_timeout,
        settings.execution_timeout_ceiling,
    );
    let priority = request.priority.unwrap_or(settings.default_priority);
    let alias = match &request.alias_target {
        AliasTarget::Model(alias) | AliasTarget::Workflow(alias) => alias.clone(),
    };

    let row = generations::insert(
        &mut tx,
        NewGeneration {
            id: gpq_domain::GenerationId::new(),
            tenant_id: tenant,
            modality,
            caller_kind: request.caller_kind,
            alias,
            target,
            parameters: request.parameters.clone(),
            priority,
            seed: request.seed,
            execution_timeout,
            output_placement: request.output_placement,
            stream_tokens: request.stream_tokens,
        },
    )
    .await
    .map_err(|error| match error {
        generations::InsertGenerationError::SeedOutOfRange(seed) => {
            AdmissionError::InvalidInput(format!("seed {seed} does not fit a signed 64-bit column"))
        }
        generations::InsertGenerationError::Database(source) => internal(source),
    })?;

    link_input_artifacts(
        &mut tx,
        tenant,
        row.generation_id(),
        &request.input_artifact_ids,
    )
    .await?;

    if let Some(key) = request.idempotency_key.as_deref() {
        record_idempotency_key(&mut tx, tenant, key, &request, row.generation_id()).await?;
    }

    tx.commit().await.map_err(internal)?;
    state.scheduler.wake_tenant(tenant);

    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request(key: Option<&str>) -> AdmissionRequest {
        AdmissionRequest {
            alias_target: AliasTarget::Model("llama-3".to_owned()),
            parameters: serde_json::json!({"temperature": 0.7}),
            input_artifact_ids: Vec::new(),
            output_placement: ArtifactPlacement::WorkerLocal,
            priority: None,
            seed: Some(42),
            execution_timeout: None,
            caller_kind: CallerKind::Durable,
            stream_tokens: false,
            idempotency_key: key.map(str::to_owned),
        }
    }

    #[test]
    fn identical_requests_hash_identically() {
        assert_eq!(
            idempotency_digest(&sample_request(Some("k"))),
            idempotency_digest(&sample_request(Some("k")))
        );
    }

    #[test]
    fn differing_parameters_hash_differently() {
        let mut other = sample_request(Some("k"));
        other.parameters = serde_json::json!({"temperature": 0.9});
        assert_ne!(
            idempotency_digest(&sample_request(Some("k"))),
            idempotency_digest(&other)
        );
    }

    #[test]
    fn differing_seed_hashes_differently() {
        let mut other = sample_request(Some("k"));
        other.seed = Some(43);
        assert_ne!(
            idempotency_digest(&sample_request(Some("k"))),
            idempotency_digest(&other)
        );
    }

    /// Whether an output placement needing S3 is rejected when Remote has
    /// no object store configured (ADR 0008).
    #[test]
    fn object_store_placement_without_s3_is_rejected() {
        assert!(requires_missing_object_store(
            ArtifactPlacement::ObjectStore,
            false
        ));
        assert!(!requires_missing_object_store(
            ArtifactPlacement::ObjectStore,
            true
        ));
    }

    #[test]
    fn placements_that_never_need_s3_are_never_rejected() {
        assert!(!requires_missing_object_store(
            ArtifactPlacement::WorkerLocal,
            false
        ));
        assert!(!requires_missing_object_store(
            ArtifactPlacement::InlineRelay,
            false
        ));
    }

    #[test]
    fn queue_capacity_gate_rejects_at_and_above_the_limit() {
        // ADR 0002: max_queued_generations counts every nonterminal
        // Generation, and the limit is inclusive.
        assert!(!queue_capacity_exceeded(4, 5));
        assert!(queue_capacity_exceeded(5, 5));
        assert!(queue_capacity_exceeded(6, 5));
    }

    fn sample_input_artifact(
        direction: crate::db::artifacts::ArtifactDirection,
        size_bytes: u64,
    ) -> crate::db::artifacts::ArtifactRow {
        crate::db::artifacts::ArtifactRow {
            id: ArtifactId::new(),
            direction,
            state: gpq_domain::ArtifactState::Available,
            placement: ArtifactPlacement::WorkerLocal,
            manifest: gpq_domain::ArtifactManifest {
                size_bytes,
                digest: gpq_domain::ContentHash::from_bytes([1; 32]),
                kind: gpq_domain::MediaKind::Binary,
                mime_type: "application/octet-stream".to_owned(),
            },
            object_key: None,
            worker_id: None,
            delivery_token: None,
            committed_offset: 0,
        }
    }

    #[test]
    fn output_direction_artifact_is_rejected_as_input() {
        let artifact = sample_input_artifact(crate::db::artifacts::ArtifactDirection::Output, 10);
        let result = validate_input_artifact_row(ArtifactId::new(), &artifact, 1_000);
        assert!(matches!(result, Err(AdmissionError::InvalidInput(_))));
    }

    #[test]
    fn oversized_input_artifact_is_rejected() {
        let artifact = sample_input_artifact(crate::db::artifacts::ArtifactDirection::Input, 2_000);
        let result = validate_input_artifact_row(ArtifactId::new(), &artifact, 1_000);
        assert!(matches!(result, Err(AdmissionError::InvalidInput(_))));
    }

    #[test]
    fn input_artifact_within_the_limit_is_accepted() {
        let artifact = sample_input_artifact(crate::db::artifacts::ArtifactDirection::Input, 1_000);
        assert!(validate_input_artifact_row(ArtifactId::new(), &artifact, 1_000).is_ok());
    }

    #[test]
    fn a_model_and_a_workflow_alias_of_the_same_name_hash_differently() {
        // The idempotency digest must fold in which catalog an alias names,
        // not just its text, or a Model and Workflow request sharing a key
        // could replay across catalogs.
        let mut workflow_request = sample_request(Some("k"));
        workflow_request.alias_target = AliasTarget::Workflow("llama-3".to_owned());
        assert_ne!(
            idempotency_digest(&sample_request(Some("k"))),
            idempotency_digest(&workflow_request)
        );
    }
}
