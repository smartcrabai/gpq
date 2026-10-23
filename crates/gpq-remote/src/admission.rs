//! Generation admission (ADR 0002, ADR 0003, ADR 0008, ADR 0012).
//!
//! Admission resolves a Model alias to its pinned immutable manifest, validates
//! the typed parameters against that manifest's family, derives the modality
//! and resolves the execution timeout, enforces Tenant policy, and inserts the
//! new Generation `Queued`. Input Artifacts are the artifact references carried
//! inside the parameters. It never leases or executes anything itself —
//! [`crate::scheduler`] picks queued work up separately, woken by
//! [`crate::state::AppState::scheduler`].

use std::time::Duration;

use gpq_domain::{
    ArtifactId, ArtifactPlacement, ExecutionTarget, GenerationParameters, Priority, TenantId,
    TenantSettings, resolve_execution_timeout,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::db::generations::{self, GenerationRow, NewGeneration};
use crate::state::AppState;

/// Which execution target a request names (ADR 0012).
#[derive(Debug, Clone)]
pub enum AdmissionTarget {
    /// A native Model alias.
    Model(String),
}

/// A request to admit one new Generation.
#[derive(Debug, Clone)]
pub struct AdmissionRequest {
    /// The Model alias the Generation runs against.
    pub target: AdmissionTarget,
    /// Typed native parameters. Their artifact references are the Generation's
    /// input Artifacts.
    pub parameters: GenerationParameters,
    /// Where output Artifacts land: `ObjectStore` or `WorkerLocal`.
    pub output_placement: ArtifactPlacement,
    /// Requested priority; the Tenant default applies when absent.
    pub priority: Option<Priority>,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
    /// A caller-requested execution timeout; it may only shorten the resolved
    /// default (ADR 0003).
    pub execution_timeout: Option<Duration>,
    /// Deduplicates a retried admission call carrying the same request.
    pub idempotency_key: Option<String>,
}

/// Why admission rejected a request.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// The Model alias does not resolve to any Version.
    #[error("unknown model alias")]
    UnknownAlias,
    /// The request itself is unusable.
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
        alias: &'a str,
        parameters: &'a GenerationParameters,
        output_placement: &'static str,
        priority: Option<u8>,
        seed: Option<u64>,
        execution_timeout_secs: Option<u64>,
    }

    let AdmissionTarget::Model(alias) = &request.target;
    let payload = DigestPayload {
        alias,
        parameters: &request.parameters,
        output_placement: request.output_placement.as_str(),
        priority: request.priority.map(u8::from),
        seed: request.seed,
        execution_timeout_secs: request.execution_timeout.map(|d| d.as_secs()),
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

/// Whether one already-fetched input Artifact row is correctly directed,
/// within the Tenant's size limit, and in its placement-specific ready state.
/// TTL and remote-local bytes are rechecked in the atomic claim query.
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
    let ready = match artifact.placement {
        ArtifactPlacement::RemoteLocal | ArtifactPlacement::WorkerLocal => {
            artifact.state == gpq_domain::ArtifactState::Available
        }
        ArtifactPlacement::ObjectStore => artifact.state == gpq_domain::ArtifactState::Pending,
    };
    if !ready {
        return Err(AdmissionError::InvalidInput(format!(
            "input artifact {artifact_id} is not ready"
        )));
    }
    Ok(())
}

/// Rejects inputs that are unready, expired, already linked, missing, or
/// exceed the Tenant's size limit. Object-store inputs are checked with HEAD;
/// bytes are never downloaded just to validate readiness.
async fn validate_input_artifacts(
    tx: &mut sqlx::PgConnection,
    tenant: TenantId,
    input_artifact_ids: &[ArtifactId],
    settings: &TenantSettings,
    artifacts: &crate::artifacts::ArtifactService,
) -> Result<(), AdmissionError> {
    for artifact_id in input_artifact_ids {
        let Some(artifact) = crate::db::artifacts::get(tx, tenant, *artifact_id)
            .await
            .map_err(internal)?
        else {
            return Err(AdmissionError::InvalidInput(format!(
                "input artifact {artifact_id} does not exist"
            )));
        };
        validate_input_artifact_row(*artifact_id, &artifact, settings.max_input_artifact_bytes)?;
        if artifact.placement == ArtifactPlacement::ObjectStore {
            let Some(object_key) = artifact.object_key.as_deref() else {
                return Err(AdmissionError::InvalidInput(format!(
                    "object-store input artifact {artifact_id} has no object key"
                )));
            };
            if !artifacts
                .input_is_ready(object_key, &artifact.manifest)
                .await
                .map_err(AdmissionError::Internal)?
            {
                return Err(AdmissionError::InvalidInput(format!(
                    "object-store input artifact {artifact_id} has not been uploaded"
                )));
            }
        }
    }
    Ok(())
}

/// Whether the Tenant's queue-capacity gate rejects a new admission (ADR
/// 0002): `max_queued_generations` counts every nonterminal Generation, not
/// just `Queued` ones, and the limit is inclusive — reaching it exactly
/// already rejects the next admission.
fn queue_capacity_exceeded(nonterminal: i64, max_queued_generations: u32) -> bool {
    u64::try_from(nonterminal).unwrap_or(u64::MAX) >= u64::from(max_queued_generations)
}

/// Links every input Artifact to the freshly inserted Generation. The update
/// only claims rows that are still unlinked `input` Artifacts of this Tenant;
/// any row that does not match means a concurrent admission or a stale
/// reference won the race, so the whole admission is rejected and rolled back.
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
    let linked = sqlx::query(
        "UPDATE artifacts SET generation_id = $1, expires_at = NULL \
         WHERE tenant_id = $2 AND id = ANY($3) AND direction = 'input' \
           AND generation_id IS NULL AND expires_at > statement_timestamp() \
           AND (
               (placement = 'remote_local' AND state = 'available'
                AND content IS NOT NULL AND octet_length(content) = size_bytes)
               OR (placement = 'object_store' AND state = 'pending')
               OR (placement = 'worker_local' AND state = 'available')
           )",
    )
    .bind(generation.as_uuid())
    .bind(tenant.as_uuid())
    .bind(&ids)
    .execute(tx)
    .await
    .map_err(internal)?
    .rows_affected();
    if linked != u64::try_from(ids.len()).unwrap_or(u64::MAX) {
        return Err(AdmissionError::InvalidInput(
            "an input artifact is missing or already linked to a generation".to_owned(),
        ));
    }
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

/// Rejects output placements that Workers cannot produce into. Remote-local
/// bytes exist only as inputs Remote stores for a Generation (ADR 0008).
fn validate_output_placement(placement: ArtifactPlacement) -> Result<(), AdmissionError> {
    match placement {
        ArtifactPlacement::ObjectStore | ArtifactPlacement::WorkerLocal => Ok(()),
        ArtifactPlacement::RemoteLocal => Err(AdmissionError::InvalidInput(
            "output_placement must be object_store or worker_local".to_owned(),
        )),
    }
}

/// Admits a new Generation (ADR 0002, ADR 0003, ADR 0008, ADR 0012).
///
/// # Errors
///
/// Returns [`AdmissionError::InvalidInput`] if `request.output_placement` is
/// not `ObjectStore` or `WorkerLocal`, the typed parameters are invalid for the
/// alias's model family, an input Artifact reference is missing, mismatched in
/// direction, already linked, or over the Tenant's size limit, or
/// `request.seed` does not fit a signed 64-bit column;
/// [`AdmissionError::ObjectStoreUnavailable`] if the output placement needs
/// object storage and this Remote has none configured (ADR 0008);
/// [`AdmissionError::UnknownAlias`] if the Model alias does not resolve to any
/// Version; [`AdmissionError::CapacityExceeded`] if the Tenant already has
/// `max_queued_generations` nonterminal Generations; and
/// [`AdmissionError::Internal`] on a database failure, a dangling idempotency
/// key referencing a missing Generation, or missing Tenant settings.
pub async fn admit(
    state: &AppState,
    tenant: TenantId,
    request: AdmissionRequest,
) -> Result<GenerationRow, AdmissionError> {
    validate_output_placement(request.output_placement)?;
    if requires_missing_object_store(
        request.output_placement,
        state.artifacts.object_store_available(),
    ) {
        return Err(AdmissionError::ObjectStoreUnavailable);
    }
    let AdmissionTarget::Model(alias) = &request.target;

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

    let model = crate::db::catalog::resolve_model_alias(&mut tx, tenant, alias)
        .await
        .map_err(internal)?
        .ok_or(AdmissionError::UnknownAlias)?;
    request
        .parameters
        .validate_for_family(model.manifest.family)
        .map_err(|err| AdmissionError::InvalidInput(err.to_string()))?;
    let sampling_overrides = match &request.parameters {
        GenerationParameters::Image(image) => {
            (image.steps, image.cfg_scale, image.sigmas.as_deref())
        }
        GenerationParameters::Video(_) => (None, None, None),
    };
    model
        .manifest
        .sampling
        .resolve(
            sampling_overrides.0,
            sampling_overrides.1,
            sampling_overrides.2,
        )
        .map_err(|err| AdmissionError::InvalidInput(err.to_string()))?;
    let inputs = request.parameters.staging_artifact_ids();
    validate_input_artifacts(&mut tx, tenant, &inputs, &settings, &state.artifacts).await?;

    let target = ExecutionTarget::for_model(model.content_hash, model.manifest.family);
    let execution_timeout = resolve_execution_timeout(
        model.manifest.modality(),
        model.limits,
        request.execution_timeout,
        settings.execution_timeout_ceiling,
    );
    let priority = request.priority.unwrap_or(settings.default_priority);
    if let Some(seed) = request.seed {
        gpq_domain::validate_seed(seed).map_err(|_| {
            AdmissionError::InvalidInput(format!("seed {seed} does not fit a signed 64-bit column"))
        })?;
    }
    let parameters = serde_json::to_value(&request.parameters)
        .map_err(|err| AdmissionError::Internal(err.into()))?;
    let generation_id = gpq_domain::GenerationId::new();
    let row = generations::insert(
        &mut tx,
        NewGeneration {
            id: generation_id,
            tenant_id: tenant,
            alias: alias.clone(),
            target,
            parameters,
            priority,
            seed: request.seed,
            execution_timeout,
            output_placement: request.output_placement,
        },
    )
    .await
    .map_err(|error| match error {
        generations::InsertGenerationError::SeedOutOfRange(seed) => {
            AdmissionError::InvalidInput(format!("seed {seed} does not fit a signed 64-bit column"))
        }
        generations::InsertGenerationError::Database(source) => internal(source),
    })?;

    link_input_artifacts(&mut tx, tenant, row.generation_id(), &inputs).await?;

    if let Some(key) = request.idempotency_key.as_deref() {
        record_idempotency_key(&mut tx, tenant, key, &request, row.generation_id()).await?;
    }

    tx.commit().await.map_err(internal)?;
    state.scheduler.wake_tenant(tenant);

    Ok(row)
}

#[cfg(test)]
mod tests {
    use gpq_domain::ImageParameters;

    use super::*;

    fn sample_image(prompt: &str) -> GenerationParameters {
        GenerationParameters::Image(ImageParameters {
            prompt: prompt.to_owned(),
            negative_prompt: String::new(),
            width: 1024,
            height: 1024,
            steps: None,
            cfg_scale: None,
            references: Vec::new(),
            loras: Vec::new(),
            sigmas: None,
        })
    }

    fn sample_request(key: Option<&str>) -> AdmissionRequest {
        AdmissionRequest {
            target: AdmissionTarget::Model("qwen-image".to_owned()),
            parameters: sample_image("a red bicycle"),
            output_placement: ArtifactPlacement::WorkerLocal,
            priority: None,
            seed: Some(42),
            execution_timeout: None,
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
        other.parameters = sample_image("a blue bicycle");
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
            ArtifactPlacement::RemoteLocal,
            false
        ));
    }

    #[test]
    fn remote_local_is_never_an_output_placement() {
        assert!(validate_output_placement(ArtifactPlacement::RemoteLocal).is_err());
        assert!(validate_output_placement(ArtifactPlacement::WorkerLocal).is_ok());
        assert!(validate_output_placement(ArtifactPlacement::ObjectStore).is_ok());
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
            placement: ArtifactPlacement::RemoteLocal,
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
}
