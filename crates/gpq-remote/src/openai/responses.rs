//! `POST /v1/responses` (ADR 0006).
//!
//! The Responses API subset this surface implements: text and multimodal
//! `input`, streaming and non-streaming output, ending in `response.created`,
//! `response.output_text.delta`, and `response.completed` SSE events for the
//! streaming path. Conversation continuation (`previous_response_id`) is out
//! of scope: GPQ keeps no cross-Generation conversation state.

use std::collections::VecDeque;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::Stream;
use futures::future::BoxFuture;
use gpq_domain::{ArtifactId, GenerationId, GenerationState, TenantId};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use super::sse::CancelOnDrop;
use super::{ApiError, ErrorDetail, TenantAuth, UsageDto};
use crate::events::GenerationEvent;
use crate::state::AppState;

/// One content part of a Responses API input item.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputContentPart {
    /// A plain text segment. Its `text` is not read here: it travels
    /// untouched inside the raw request captured as the opaque `parameters`
    /// payload.
    InputText,
    /// A multimodal image reference, an `http(s)://` or `data:` URL.
    InputImage {
        /// Absent when the caller instead supplies a Files API `file_id`,
        /// which this surface does not support (ADR 0006 excludes Files).
        #[serde(default)]
        image_url: Option<String>,
    },
}

/// One message-shaped input item.
#[derive(Debug, Clone, Deserialize)]
pub struct InputItem {
    /// The item's content parts. The item's `role` is not read here: it
    /// travels untouched inside the raw request captured as the opaque
    /// `parameters` payload.
    #[serde(default)]
    pub content: Vec<InputContentPart>,
}

/// The Responses API `input` field: plain text or structured items.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ResponsesInput {
    /// A single text turn.
    Text(String),
    /// Structured message items, optionally multimodal.
    Items(Vec<InputItem>),
}

/// The `POST /v1/responses` request body.
///
/// Fields recognized here are validated; every field (recognized or not) is
/// forwarded verbatim as the opaque `parameters` payload (ADR 0007), since
/// the request is captured as raw JSON before this struct is parsed from it.
/// `instructions` is accepted by `OpenAI` clients but not captured here: it
/// travels untouched inside that raw payload.
#[derive(Debug, Clone, Deserialize)]
pub struct CreateResponseRequest {
    /// The requested Model alias.
    #[serde(default)]
    pub model: String,
    /// The input turn(s).
    #[serde(default)]
    pub input: Option<ResponsesInput>,
    /// Whether to stream the response as SSE.
    #[serde(default)]
    pub stream: bool,
    /// Rejected: GPQ keeps no cross-Generation conversation state.
    #[serde(default)]
    pub previous_response_id: Option<String>,
    /// Deterministic sampling seed, when the backend supports it.
    #[serde(default)]
    pub seed: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "field names mirror OpenAI's wire-format usage keys and must not change"
)]
struct ResponsesUsage {
    input_tokens: u32,
    output_tokens: u32,
    total_tokens: u32,
}

impl From<UsageDto> for ResponsesUsage {
    fn from(usage: UsageDto) -> Self {
        Self {
            input_tokens: usage.prompt_tokens,
            output_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct OutputTextPart {
    #[serde(rename = "type")]
    kind: &'static str,
    text: String,
    annotations: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize)]
struct OutputMessage {
    #[serde(rename = "type")]
    kind: &'static str,
    id: String,
    status: &'static str,
    role: &'static str,
    content: Vec<OutputTextPart>,
}

#[derive(Debug, Clone, Serialize)]
struct ResponseObject {
    id: String,
    object: &'static str,
    created_at: i64,
    model: String,
    status: &'static str,
    output: Vec<OutputMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<ResponsesUsage>,
}

#[derive(Serialize)]
struct CreatedEvent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    response: &'a ResponseObject,
}

#[derive(Serialize)]
struct DeltaEvent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    item_id: &'a str,
    output_index: u32,
    content_index: u32,
    delta: &'a str,
}

#[derive(Serialize)]
struct CompletedEvent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    response: &'a ResponseObject,
}

fn build_response_object(
    response_id: &str,
    message_id: &str,
    created: i64,
    model: &str,
    status: &'static str,
    output_text: Option<String>,
    usage: Option<ResponsesUsage>,
) -> ResponseObject {
    let output = match output_text {
        Some(text) => vec![OutputMessage {
            kind: "message",
            id: message_id.to_owned(),
            status: "completed",
            role: "assistant",
            content: vec![OutputTextPart {
                kind: "output_text",
                text,
                annotations: Vec::new(),
            }],
        }],
        None => Vec::new(),
    };
    ResponseObject {
        id: response_id.to_owned(),
        object: "response",
        created_at: created,
        model: model.to_owned(),
        status,
        output,
        usage,
    }
}

fn status_for(state: GenerationState) -> &'static str {
    match state {
        GenerationState::Succeeded => "completed",
        GenerationState::Cancelled => "cancelled",
        _ => "failed",
    }
}

fn validate(request: &CreateResponseRequest) -> Result<(), ApiError> {
    if request.model.trim().is_empty() {
        return Err(ApiError::invalid_request("model is required"));
    }
    if request.previous_response_id.is_some() {
        return Err(ApiError::invalid_request(
            "previous_response_id is not supported",
        ));
    }
    match &request.input {
        None => Err(ApiError::invalid_request("input is required")),
        Some(ResponsesInput::Text(text)) if text.trim().is_empty() => {
            Err(ApiError::invalid_request("input must not be empty"))
        }
        Some(ResponsesInput::Items(items)) if items.is_empty() => {
            Err(ApiError::invalid_request("input must not be empty"))
        }
        Some(_) => Ok(()),
    }
}

async fn collect_input_artifacts(
    state: &AppState,
    tenant_id: TenantId,
    input: Option<&ResponsesInput>,
    max_input_artifact_bytes: u64,
) -> Result<Vec<ArtifactId>, ApiError> {
    let Some(ResponsesInput::Items(items)) = input else {
        return Ok(Vec::new());
    };
    let urls = items
        .iter()
        .flat_map(|item| item.content.iter())
        .filter_map(|part| match part {
            InputContentPart::InputImage {
                image_url: Some(url),
            } => Some(url.as_str()),
            _ => None,
        });
    super::resolve_and_store_images(state, tenant_id, urls, max_input_artifact_bytes).await
}

/// Handles `POST /v1/responses`.
pub async fn create_response(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
    Json(raw): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let request: CreateResponseRequest = serde_json::from_value(raw.clone())
        .map_err(|err| ApiError::invalid_request(format!("invalid request body: {err}")))?;
    validate(&request)?;

    let settings = super::tenant_settings(&state, tenant_id).await?;
    let input_artifact_ids = collect_input_artifacts(
        &state,
        tenant_id,
        request.input.as_ref(),
        settings.max_input_artifact_bytes,
    )
    .await?;
    let mut input_guard = super::InputArtifactGuard::new(state.clone(), tenant_id);
    input_guard.extend(input_artifact_ids.iter().copied());

    let admission_request = super::build_admission_request(
        &request.model,
        raw,
        input_artifact_ids,
        request.seed,
        request.stream,
    );

    let generation = crate::admission::admit(&state, tenant_id, admission_request)
        .await
        .map_err(ApiError::from_admission)?;
    input_guard.disarm();
    let generation_id = generation.generation_id();
    let created = generation.created_at.timestamp();
    let response_id = format!("resp_{generation_id}");
    let message_id = format!("msg_{generation_id}");
    let receiver = state.events.subscribe(generation_id);

    if request.stream {
        Ok(stream_response(
            state,
            tenant_id,
            generation_id,
            response_id,
            message_id,
            request.model,
            created,
            receiver,
        )
        .into_response())
    } else {
        let guard = CancelOnDrop::new(
            state.db.clone(),
            state.artifacts.clone(),
            tenant_id,
            generation_id,
        );
        let row =
            super::await_terminal_generation(&state, tenant_id, generation_id, receiver).await?;
        guard.disarm();
        let Ok(generation_state) = row.state() else {
            return Err(ApiError::internal("generation has an unrecognized state"));
        };
        if generation_state != GenerationState::Succeeded {
            return Err(super::terminal_failure_response(&row, generation_state));
        }
        let usage = super::usage_from_row(&row);
        let response = build_response_object(
            &response_id,
            &message_id,
            created,
            &request.model,
            "completed",
            Some(row.output_text.clone()),
            Some(usage.into()),
        );
        Ok(Json(response).into_response())
    }
}

struct StreamCtx {
    response_id: String,
    message_id: String,
    model: String,
    created: i64,
}

/// Builds the frames a terminal Generation state produces on the Responses
/// stream: one `response.completed` event carrying the final `output_text`
/// and usage read back from the Generation row (ADR 0007's envelope has no
/// other place to read them from). A failure to read that row — a
/// transient database fault, not the Generation's own outcome — reports an
/// `error` event instead: a `response.completed` with `output: []` would
/// otherwise tell the caller, falsely, that the Generation produced nothing,
/// when its real output may simply be unreadable right now. `[DONE]` and the
/// drop guard are handled by [`super::sse::unfold_broadcast`], the shared
/// driver.
async fn terminal_event(
    state: AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    ctx: StreamCtx,
    generation_state: GenerationState,
) -> VecDeque<Event> {
    let mut events = VecDeque::new();
    let Ok(row) = super::fetch_generation_row(&state, tenant_id, generation_id).await else {
        if let Ok(event) = super::sse::named_event(
            "error",
            &ErrorDetail {
                message: "the generation's result could not be read".to_owned(),
                kind: "server_error".to_owned(),
                code: "internal_error".to_owned(),
            },
        ) {
            events.push_back(event);
        }
        return events;
    };
    let status = status_for(generation_state);
    let output_text = if generation_state == GenerationState::Succeeded {
        Some(row.output_text.clone())
    } else {
        None
    };
    let usage = Some(ResponsesUsage::from(super::usage_from_row(&row)));
    let response = build_response_object(
        &ctx.response_id,
        &ctx.message_id,
        ctx.created,
        &ctx.model,
        status,
        output_text,
        usage,
    );
    if let Ok(event) = super::sse::named_event(
        "response.completed",
        &CompletedEvent {
            kind: "response.completed",
            response: &response,
        },
    ) {
        events.push_back(event);
    }
    events
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument seeds one immutable streaming-response field"
)]
fn stream_response(
    state: AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    response_id: String,
    message_id: String,
    model: String,
    created: i64,
    rx: broadcast::Receiver<GenerationEvent>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let guard = CancelOnDrop::new(
        state.db.clone(),
        state.artifacts.clone(),
        tenant_id,
        generation_id,
    );
    let ctx = StreamCtx {
        response_id,
        message_id,
        model,
        created,
    };
    let initial_response = build_response_object(
        &ctx.response_id,
        &ctx.message_id,
        ctx.created,
        &ctx.model,
        "in_progress",
        None,
        None,
    );
    let mut initial_queue = VecDeque::new();
    if let Ok(event) = super::sse::named_event(
        "response.created",
        &CreatedEvent {
            kind: "response.created",
            response: &initial_response,
        },
    ) {
        initial_queue.push_back(event);
    }
    let token_message_id = ctx.message_id.clone();
    let on_token: super::sse::OnToken = Box::new(move |text| {
        let payload = DeltaEvent {
            kind: "response.output_text.delta",
            item_id: &token_message_id,
            output_index: 0,
            content_index: 0,
            delta: &text,
        };
        super::sse::named_event("response.output_text.delta", &payload).ok()
    });
    let terminal_state = state.clone();
    let on_terminal: super::sse::OnTerminal = Box::new(
        move |generation_state| -> BoxFuture<'static, VecDeque<Event>> {
            Box::pin(terminal_event(
                terminal_state,
                tenant_id,
                generation_id,
                ctx,
                generation_state,
            ))
        },
    );
    let stream = super::sse::unfold_broadcast(
        state,
        tenant_id,
        generation_id,
        rx,
        guard,
        initial_queue,
        on_token,
        on_terminal,
    );
    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> CreateResponseRequest {
        CreateResponseRequest {
            model: "llama-3".to_owned(),
            input: Some(ResponsesInput::Text("hi".to_owned())),
            stream: false,
            previous_response_id: None,
            seed: None,
        }
    }

    /// `instructions` and item `text` are validated for shape only, never
    /// read into typed fields: the raw JSON body (not this struct) is what
    /// admission receives as the opaque `parameters` payload (ADR 0007), so
    /// an unrecognized-but-well-formed `instructions`/`text` value must
    /// still deserialize without error.
    #[test]
    fn instructions_and_item_text_are_accepted_but_not_captured() {
        let raw = serde_json::json!({
            "model": "llama-3",
            "instructions": "be terse",
            "input": [
                {
                    "content": [
                        {"type": "input_text", "text": "hello"},
                        {"type": "input_image", "image_url": "https://example.com/x.png"},
                    ],
                },
            ],
        });
        let Ok(request) = serde_json::from_value::<CreateResponseRequest>(raw.clone()) else {
            panic!("well-formed body must parse");
        };
        assert!(validate(&request).is_ok());
        assert_eq!(raw["instructions"], "be terse");
        assert_eq!(raw["input"][0]["content"][0]["text"], "hello");
    }

    #[test]
    fn valid_request_is_accepted() {
        assert!(validate(&sample_request()).is_ok());
    }

    #[test]
    fn missing_model_is_rejected() {
        let mut request = sample_request();
        request.model = String::new();
        assert!(validate(&request).is_err());
    }

    #[test]
    fn missing_input_is_rejected() {
        let mut request = sample_request();
        request.input = None;
        assert!(validate(&request).is_err());
    }

    #[test]
    fn empty_text_input_is_rejected() {
        let mut request = sample_request();
        request.input = Some(ResponsesInput::Text(String::new()));
        assert!(validate(&request).is_err());
    }

    #[test]
    fn empty_item_input_is_rejected() {
        let mut request = sample_request();
        request.input = Some(ResponsesInput::Items(Vec::new()));
        assert!(validate(&request).is_err());
    }

    #[test]
    fn previous_response_id_is_rejected() {
        let mut request = sample_request();
        request.previous_response_id = Some("resp_123".to_owned());
        assert!(validate(&request).is_err());
    }

    #[test]
    fn status_maps_generation_states() {
        assert_eq!(status_for(GenerationState::Succeeded), "completed");
        assert_eq!(status_for(GenerationState::Cancelled), "cancelled");
        assert_eq!(status_for(GenerationState::Failed), "failed");
    }

    #[test]
    fn completed_response_carries_output_text_and_usage() {
        let usage = ResponsesUsage {
            input_tokens: 10,
            output_tokens: 4,
            total_tokens: 14,
        };
        let response = build_response_object(
            "resp_1",
            "msg_1",
            1_700_000_000,
            "llama-3",
            "completed",
            Some("hello".to_owned()),
            Some(usage),
        );
        assert_eq!(response.status, "completed");
        assert_eq!(response.output.len(), 1);
        assert_eq!(response.output[0].content[0].text, "hello");
        let Some(response_usage) = response.usage else {
            panic!("expected usage to be set");
        };
        assert_eq!(response_usage.total_tokens, 14);
    }

    #[test]
    fn in_progress_response_has_no_output() {
        let response = build_response_object(
            "resp_1",
            "msg_1",
            1_700_000_000,
            "llama-3",
            "in_progress",
            None,
            None,
        );
        assert!(response.output.is_empty());
        assert!(response.usage.is_none());
    }

    #[test]
    fn delta_event_json_shape_matches_responses_api() {
        let payload = DeltaEvent {
            kind: "response.output_text.delta",
            item_id: "msg_1",
            output_index: 0,
            content_index: 0,
            delta: "hi",
        };
        let Ok(value) = serde_json::to_value(&payload) else {
            panic!("expected delta event to serialize");
        };
        assert_eq!(value["type"], "response.output_text.delta");
        assert_eq!(value["delta"], "hi");
    }
}
