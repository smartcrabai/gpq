//! `POST /v1/images/generations` (OpenAI-compatible image generation).
//!
//! Image-capable Workflow aliases provide the implementation. Portable request
//! fields become `$`-prefixed `ComfyUI` graph placeholders; backend-specific
//! options follow the same convention. The response is always base64 JSON,
//! matching the wire contract used by `@ai-sdk/openai-compatible`.

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use gpq_domain::{ArtifactPlacement, CallerKind, GenerationState, MediaKind};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::sse::CancelOnDrop;
use super::{ApiError, TenantAuth};
use crate::admission::{AdmissionRequest, AliasTarget};
use crate::state::AppState;

const MAX_IMAGES_PER_REQUEST: u32 = 10;
const MAX_IMAGE_RESPONSE_BYTES: u64 = 64 * 1024 * 1024;

/// OpenAI-compatible image generation request.
#[derive(Debug, Deserialize)]
pub struct CreateImageRequest {
    #[serde(default)]
    model: String,
    #[serde(default)]
    prompt: String,
    #[serde(default)]
    n: Option<u32>,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    response_format: Option<String>,
    #[serde(default)]
    seed: Option<u64>,
    #[serde(default)]
    stream: bool,
    #[serde(default, rename = "user")]
    _user: Option<Value>,
    #[serde(flatten)]
    options: Map<String, Value>,
}

#[derive(Debug, Serialize)]
struct ImageData {
    b64_json: String,
}

#[derive(Debug, Serialize)]
struct ImagesResponse {
    created: i64,
    data: Vec<ImageData>,
}

fn image_count(request: &CreateImageRequest) -> Result<u32, ApiError> {
    let count = request.n.unwrap_or(1);
    if !(1..=MAX_IMAGES_PER_REQUEST).contains(&count) {
        return Err(ApiError::invalid_request(format!(
            "n must be between 1 and {MAX_IMAGES_PER_REQUEST}"
        )));
    }
    Ok(count)
}

fn image_size(size: Option<&str>) -> Result<Option<(u32, u32)>, ApiError> {
    let Some(size) = size else {
        return Ok(None);
    };
    let Some((width, height)) = size.split_once('x') else {
        return Err(ApiError::invalid_request(
            "size must use the WIDTHxHEIGHT format",
        ));
    };
    let width = width
        .parse::<u32>()
        .map_err(|_| ApiError::invalid_request("size width must be a positive integer"))?;
    let height = height
        .parse::<u32>()
        .map_err(|_| ApiError::invalid_request("size height must be a positive integer"))?;
    if width == 0 || height == 0 {
        return Err(ApiError::invalid_request(
            "size dimensions must be positive",
        ));
    }
    Ok(Some((width, height)))
}

fn validate(request: &CreateImageRequest) -> Result<(u32, Option<(u32, u32)>), ApiError> {
    if request.model.trim().is_empty() {
        return Err(ApiError::invalid_request("model is required"));
    }
    if request.prompt.trim().is_empty() {
        return Err(ApiError::invalid_request("prompt is required"));
    }
    if request.stream {
        return Err(ApiError::invalid_request(
            "streaming image generation is not supported",
        ));
    }
    if request
        .response_format
        .as_deref()
        .is_some_and(|format| format != "b64_json")
    {
        return Err(ApiError::invalid_request(
            "response_format must be b64_json",
        ));
    }
    Ok((image_count(request)?, image_size(request.size.as_deref())?))
}

fn workflow_parameters(
    request: &CreateImageRequest,
    count: u32,
    size: Option<(u32, u32)>,
) -> Value {
    let mut parameters = Map::new();
    parameters.insert("$prompt".to_owned(), Value::String(request.prompt.clone()));
    if count != 1 {
        parameters.insert("$n".to_owned(), Value::from(count));
    }
    if let Some((width, height)) = size {
        parameters.insert("$width".to_owned(), Value::from(width));
        parameters.insert("$height".to_owned(), Value::from(height));
    }
    if let Some(seed) = request.seed {
        parameters.insert("$seed_value".to_owned(), Value::from(seed));
    }
    for (name, value) in &request.options {
        parameters.insert(format!("${name}"), value.clone());
    }
    Value::Object(parameters)
}

async fn output_artifacts(
    state: &AppState,
    tenant_id: gpq_domain::TenantId,
    generation_id: gpq_domain::GenerationId,
) -> Result<Vec<crate::db::artifacts::ArtifactRow>, ApiError> {
    let mut tx = state.db.begin_tenant(tenant_id).await.map_err(|error| {
        tracing::error!(%error, "failed to begin image-output transaction");
        ApiError::internal("Internal error.")
    })?;
    let outputs = crate::db::artifacts::list_outputs(&mut tx, tenant_id, generation_id)
        .await
        .map_err(|error| {
            tracing::error!(%error, "failed to list image outputs");
            ApiError::internal("Internal error.")
        })?;
    tx.commit().await.map_err(|error| {
        tracing::error!(%error, "failed to commit image-output transaction");
        ApiError::internal("Internal error.")
    })?;
    Ok(outputs)
}

fn validate_outputs(
    outputs: &[crate::db::artifacts::ArtifactRow],
    count: u32,
) -> Result<(), ApiError> {
    let expected = usize::try_from(count)
        .map_err(|_| ApiError::internal("Requested image count cannot be represented."))?;
    if outputs.len() != expected {
        return Err(ApiError::internal(format!(
            "The image workflow produced {} images; {count} were requested.",
            outputs.len()
        )));
    }
    let total = outputs.iter().try_fold(0_u64, |total, output| {
        if output.manifest.kind != MediaKind::Image
            || output.placement != ArtifactPlacement::WorkerLocal
        {
            return None;
        }
        total.checked_add(output.manifest.size_bytes)
    });
    let Some(total) = total else {
        return Err(ApiError::internal(
            "The image workflow produced an invalid output artifact.",
        ));
    };
    if total > MAX_IMAGE_RESPONSE_BYTES {
        return Err(ApiError::internal(
            "The generated images are too large for an inline response.",
        ));
    }
    Ok(())
}

/// Handles `POST /v1/images/generations`.
pub async fn create_image(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
    Json(raw): Json<Value>,
) -> Result<Response, ApiError> {
    let request: CreateImageRequest = serde_json::from_value(raw)
        .map_err(|error| ApiError::invalid_request(format!("invalid request body: {error}")))?;
    let (count, size) = validate(&request)?;
    let admission_request = AdmissionRequest {
        alias_target: AliasTarget::Workflow(request.model.clone()),
        parameters: workflow_parameters(&request, count, size),
        input_artifact_ids: Vec::new(),
        output_placement: ArtifactPlacement::WorkerLocal,
        priority: None,
        seed: request.seed,
        execution_timeout: None,
        caller_kind: CallerKind::Synchronous,
        stream_tokens: false,
        idempotency_key: None,
    };
    let generation = crate::admission::admit(&state, tenant_id, admission_request)
        .await
        .map_err(ApiError::from_admission)?;
    let generation_id = generation.generation_id();
    let receiver = state.events.subscribe(generation_id);
    let guard = CancelOnDrop::new(
        state.db.clone(),
        state.artifacts.clone(),
        tenant_id,
        generation_id,
    );
    let row = super::await_terminal_generation(&state, tenant_id, generation_id, receiver).await?;
    guard.disarm();
    let generation_state = row
        .state()
        .map_err(|_| ApiError::internal("generation has an unrecognized state"))?;
    if generation_state != GenerationState::Succeeded {
        return Err(super::terminal_failure_response(&row, generation_state));
    }

    let outputs = output_artifacts(&state, tenant_id, generation_id).await?;
    validate_outputs(&outputs, count)?;
    let limit = usize::try_from(MAX_IMAGE_RESPONSE_BYTES)
        .map_err(|_| ApiError::internal("Image response limit cannot be represented."))?;
    let mut data = Vec::with_capacity(outputs.len());
    for output in outputs {
        let artifact_id = output.id;
        let bytes = crate::artifacts::consume_worker_local_output(&state, tenant_id, output, limit)
            .await
            .map_err(|error| {
                tracing::error!(%error, %artifact_id, "failed to consume image output");
                ApiError::internal("The generated image could not be read.")
            })?;
        data.push(ImageData {
            b64_json: base64::engine::general_purpose::STANDARD.encode(bytes),
        });
    }

    Ok(Json(ImagesResponse {
        created: row.created_at.timestamp(),
        data,
    })
    .into_response())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn openai_fields_become_workflow_placeholders() {
        let request: CreateImageRequest = serde_json::from_value(json!({
            "model": "image-workflow",
            "prompt": "a red panda astronaut",
            "n": 2,
            "size": "1024x768",
            "seed": 42,
            "quality": "high",
            "response_format": "b64_json"
        }))
        .unwrap_or_else(|error| panic!("request must decode: {error}"));
        let Ok((count, size)) = validate(&request) else {
            panic!("request must validate");
        };
        assert_eq!(
            workflow_parameters(&request, count, size),
            json!({
                "$prompt": "a red panda astronaut",
                "$n": 2,
                "$width": 1024,
                "$height": 768,
                "$seed_value": 42,
                "$quality": "high"
            })
        );
    }
}
