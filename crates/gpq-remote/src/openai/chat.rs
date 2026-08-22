//! `POST /v1/chat/completions` (ADR 0006).
//!
//! Streaming and non-streaming Chat Completions over the synchronous
//! (connection-held) `OpenAI` caller path. The whole request body becomes the
//! opaque backend `parameters` payload (ADR 0007); multimodal image content
//! parts are resolved to `inline_relay` input Artifacts before admission
//! (ADR 0008) so the Worker fetches them itself rather than Remote handing
//! out raw bytes over an uncontrolled channel.

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
use super::{ApiError, ErrorBody, ErrorDetail, TenantAuth, UsageDto};
use crate::events::GenerationEvent;
use crate::state::AppState;

/// One `image_url` content part's inner object.
#[derive(Debug, Clone, Deserialize)]
pub struct ImageUrlDto {
    /// An `http(s)://` or `data:` URL. `detail` is accepted by `OpenAI`
    /// clients but not captured here: GPQ has no concept of image detail
    /// levels, and it travels untouched inside the raw request captured as
    /// the opaque `parameters` payload.
    pub url: String,
}

/// One element of a multimodal `content` array.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    /// A plain text segment. Its `text` is not read here: it travels
    /// untouched inside the raw request captured as the opaque `parameters`
    /// payload.
    Text,
    /// A multimodal image reference.
    ImageUrl {
        /// The image location.
        image_url: ImageUrlDto,
    },
}

/// A message's `content`, either plain text or a multimodal parts array.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// Plain text content.
    Text(String),
    /// Multimodal content parts.
    Parts(Vec<ContentPart>),
}

/// One Chat Completions message.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatMessage {
    /// The message content; absent for some tool-call-only assistant turns.
    /// The message's `role` is not read here: it travels untouched inside
    /// the raw request captured as the opaque `parameters` payload.
    #[serde(default)]
    pub content: Option<MessageContent>,
}

/// `stream_options` of a Chat Completions request.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct StreamOptionsDto {
    /// Whether to emit a final usage-only chunk before `[DONE]`.
    #[serde(default)]
    pub include_usage: bool,
}

/// The `POST /v1/chat/completions` request body.
///
/// Fields recognized here are validated; every field (recognized or not) is
/// forwarded verbatim as the opaque `parameters` payload (ADR 0007), since
/// the request is captured as raw JSON before this struct is parsed from it.
#[derive(Debug, Clone, Deserialize)]
pub struct ChatCompletionRequest {
    /// The requested Model alias.
    #[serde(default)]
    pub model: String,
    /// The conversation so far.
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    /// Whether to stream the response as SSE.
    #[serde(default)]
    pub stream: bool,
    /// Streaming-only extra behavior.
    #[serde(default)]
    pub stream_options: Option<StreamOptionsDto>,
    /// Deterministic sampling seed, when the backend supports it.
    #[serde(default)]
    pub seed: Option<u64>,
    /// Number of completions to generate; only `1` is supported.
    #[serde(default)]
    pub n: Option<u32>,
}

/// The full `data: <json>` streaming choice delta.
#[derive(Debug, Serialize, Default)]
struct DeltaDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

#[derive(Debug, Serialize)]
struct ChunkChoice {
    index: u32,
    delta: DeltaDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    finish_reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct ChatCompletionChunk {
    id: String,
    object: &'static str,
    created: i64,
    model: String,
    choices: Vec<ChunkChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<UsageDto>,
}

#[derive(Debug, Serialize)]
struct ChatResponseMessage {
    role: &'static str,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatChoice {
    index: u32,
    message: ChatResponseMessage,
    finish_reason: &'static str,
}

#[derive(Debug, Serialize)]
struct ChatCompletionResponse {
    id: String,
    object: &'static str,
    created: i64,
    model: String,
    choices: Vec<ChatChoice>,
    usage: UsageDto,
}

fn validate(request: &ChatCompletionRequest) -> Result<(), ApiError> {
    if request.model.trim().is_empty() {
        return Err(ApiError::invalid_request("model is required"));
    }
    if request.messages.is_empty() {
        return Err(ApiError::invalid_request("messages must not be empty"));
    }
    for message in &request.messages {
        if let Some(MessageContent::Text(text)) = &message.content
            && text.trim().is_empty()
        {
            return Err(ApiError::invalid_request(
                "message content must not be empty",
            ));
        }
    }
    super::validate_n(request.n)
}

async fn collect_input_artifacts(
    state: &AppState,
    tenant_id: TenantId,
    messages: &[ChatMessage],
    max_input_artifact_bytes: u64,
) -> Result<Vec<ArtifactId>, ApiError> {
    let urls = messages
        .iter()
        .flat_map(|message| match &message.content {
            Some(MessageContent::Parts(parts)) => parts.as_slice(),
            _ => &[],
        })
        .filter_map(|part| match part {
            ContentPart::ImageUrl { image_url } => Some(image_url.url.as_str()),
            ContentPart::Text => None,
        });
    super::resolve_and_store_images(state, tenant_id, urls, max_input_artifact_bytes).await
}

/// Handles `POST /v1/chat/completions`.
pub async fn chat_completions(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
    Json(raw): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let request: ChatCompletionRequest = serde_json::from_value(raw.clone())
        .map_err(|err| ApiError::invalid_request(format!("invalid request body: {err}")))?;
    validate(&request)?;

    let settings = super::tenant_settings(&state, tenant_id).await?;
    let input_artifact_ids = collect_input_artifacts(
        &state,
        tenant_id,
        &request.messages,
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
    let completion_id = format!("chatcmpl-{generation_id}");
    let receiver = state.events.subscribe(generation_id);

    if request.stream {
        let include_usage = request
            .stream_options
            .as_ref()
            .is_some_and(|options| options.include_usage);
        Ok(stream_chat_completion(
            state,
            tenant_id,
            generation_id,
            completion_id,
            request.model,
            created,
            include_usage,
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
        Ok(Json(ChatCompletionResponse {
            id: completion_id,
            object: "chat.completion",
            created,
            model: request.model,
            choices: vec![ChatChoice {
                index: 0,
                message: ChatResponseMessage {
                    role: "assistant",
                    content: row.output_text.clone(),
                },
                finish_reason: "stop",
            }],
            usage,
        })
        .into_response())
    }
}

/// The immutable, pure data needed to render one `chat.completion.chunk`
/// frame; kept separate from the runtime handles below so chunk rendering
/// is unit-testable without a live `AppState`.
#[derive(Clone)]
struct ChunkCtx {
    completion_id: String,
    model: String,
    created: i64,
}

fn role_chunk(ctx: &ChunkCtx) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: ctx.completion_id.clone(),
        object: "chat.completion.chunk",
        created: ctx.created,
        model: ctx.model.clone(),
        choices: vec![ChunkChoice {
            index: 0,
            delta: DeltaDto {
                role: Some("assistant"),
                content: None,
            },
            finish_reason: None,
        }],
        usage: None,
    }
}

fn content_chunk(ctx: &ChunkCtx, text: String) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: ctx.completion_id.clone(),
        object: "chat.completion.chunk",
        created: ctx.created,
        model: ctx.model.clone(),
        choices: vec![ChunkChoice {
            index: 0,
            delta: DeltaDto {
                role: None,
                content: Some(text),
            },
            finish_reason: None,
        }],
        usage: None,
    }
}

fn finish_chunk(ctx: &ChunkCtx, finish_reason: &'static str) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: ctx.completion_id.clone(),
        object: "chat.completion.chunk",
        created: ctx.created,
        model: ctx.model.clone(),
        choices: vec![ChunkChoice {
            index: 0,
            delta: DeltaDto::default(),
            finish_reason: Some(finish_reason),
        }],
        usage: None,
    }
}

/// Maps a terminal `GenerationState` to the wire `finish_reason` for
/// `chat.completion.chunk`/`chat.completion` frames, so a Generation that
/// failed, was cancelled, or expired is never reported as a clean `"stop"`
/// completion.
fn chat_finish_reason(generation_state: GenerationState) -> &'static str {
    match generation_state {
        GenerationState::Succeeded => "stop",
        GenerationState::Cancelled => "cancelled",
        _ => "error",
    }
}

/// Builds the `{"error": {...}}` SSE frame reported alongside a non-success
/// terminal Generation, reusing the terminal HTTP failure's message/code so
/// the streaming and non-streaming surfaces describe the same failure the
/// same way.
fn terminal_error_body(
    row: Option<&crate::db::generations::GenerationRow>,
    generation_state: GenerationState,
) -> ErrorBody {
    match row {
        Some(row) => super::terminal_failure_response(row, generation_state).into_error_body(),
        None => ErrorBody {
            error: ErrorDetail {
                message: format!(
                    "generation ended in state {generation_state} and its details could not be read"
                ),
                kind: "server_error".to_owned(),
                code: "internal_error".to_owned(),
            },
        },
    }
}

fn usage_chunk(ctx: &ChunkCtx, usage: UsageDto) -> ChatCompletionChunk {
    ChatCompletionChunk {
        id: ctx.completion_id.clone(),
        object: "chat.completion.chunk",
        created: ctx.created,
        model: ctx.model.clone(),
        choices: Vec::new(),
        usage: Some(usage),
    }
}

/// Builds the frames a terminal Generation state produces on the Chat
/// stream: for a failed, cancelled, or expired Generation, an `{"error":
/// {...}}` frame an `OpenAI` SDK actually notices (ADR 0006) precedes the
/// `finish_reason` chunk instead of the wire lying with a clean `"stop"`;
/// then, when the caller asked for `stream_options.include_usage`, a
/// usage-only chunk. `[DONE]` and the drop guard are handled by
/// [`super::sse::unfold_broadcast`], the shared driver.
async fn terminal_chunks(
    state: AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    chunk: ChunkCtx,
    include_usage: bool,
    generation_state: GenerationState,
) -> VecDeque<Event> {
    let succeeded = generation_state == GenerationState::Succeeded;
    let row = if succeeded && !include_usage {
        None
    } else {
        super::fetch_generation_row(&state, tenant_id, generation_id)
            .await
            .ok()
    };
    let mut events = VecDeque::new();
    if !succeeded {
        tracing::warn!(%generation_id, ?generation_state, "chat completion ended without success");
        if let Ok(event) =
            super::sse::data_event(&terminal_error_body(row.as_ref(), generation_state))
        {
            events.push_back(event);
        }
    }
    if let Ok(event) =
        super::sse::data_event(&finish_chunk(&chunk, chat_finish_reason(generation_state)))
    {
        events.push_back(event);
    }
    if include_usage
        && let Some(row) = &row
        && let Ok(event) = super::sse::data_event(&usage_chunk(&chunk, super::usage_from_row(row)))
    {
        events.push_back(event);
    }
    events
}

#[expect(
    clippy::too_many_arguments,
    reason = "each argument seeds one immutable streaming-response field"
)]
fn stream_chat_completion(
    state: AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    completion_id: String,
    model: String,
    created: i64,
    include_usage: bool,
    rx: broadcast::Receiver<GenerationEvent>,
) -> Sse<impl Stream<Item = Result<Event, std::convert::Infallible>>> {
    let guard = CancelOnDrop::new(
        state.db.clone(),
        state.artifacts.clone(),
        tenant_id,
        generation_id,
    );
    let chunk = ChunkCtx {
        completion_id,
        model,
        created,
    };
    let mut initial_queue = VecDeque::new();
    if let Ok(event) = super::sse::data_event(&role_chunk(&chunk)) {
        initial_queue.push_back(event);
    }
    let token_chunk = chunk.clone();
    let on_token: super::sse::OnToken =
        Box::new(move |text| super::sse::data_event(&content_chunk(&token_chunk, text)).ok());
    let terminal_state = state.clone();
    let on_terminal: super::sse::OnTerminal = Box::new(
        move |generation_state| -> BoxFuture<'static, VecDeque<Event>> {
            Box::pin(terminal_chunks(
                terminal_state,
                tenant_id,
                generation_id,
                chunk,
                include_usage,
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

    fn sample_request() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "llama-3".to_owned(),
            messages: vec![ChatMessage {
                content: Some(MessageContent::Text("hi".to_owned())),
            }],
            stream: false,
            stream_options: None,
            seed: None,
            n: None,
        }
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
    fn empty_messages_are_rejected() {
        let mut request = sample_request();
        request.messages.clear();
        assert!(validate(&request).is_err());
    }

    #[test]
    fn n_other_than_one_is_rejected() {
        let mut request = sample_request();
        request.n = Some(2);
        assert!(validate(&request).is_err());
    }

    #[test]
    fn n_of_one_is_accepted() {
        let mut request = sample_request();
        request.n = Some(1);
        assert!(validate(&request).is_ok());
    }

    fn sample_ctx() -> ChunkCtx {
        ChunkCtx {
            completion_id: "chatcmpl-test".to_owned(),
            model: "llama-3".to_owned(),
            created: 1_700_000_000,
        }
    }

    #[test]
    fn content_chunk_carries_the_delta_text() {
        let chunk = content_chunk(&sample_ctx(), "hello".to_owned());
        assert_eq!(chunk.choices.len(), 1);
        assert_eq!(chunk.choices[0].delta.content.as_deref(), Some("hello"));
        assert!(chunk.choices[0].delta.role.is_none());
        assert!(chunk.choices[0].finish_reason.is_none());
        assert!(chunk.usage.is_none());
    }

    #[test]
    fn finish_chunk_reports_the_given_finish_reason() {
        let chunk = finish_chunk(&sample_ctx(), "stop");
        assert_eq!(chunk.choices[0].finish_reason, Some("stop"));
        assert!(chunk.choices[0].delta.content.is_none());
        assert!(chunk.choices[0].delta.role.is_none());
    }

    #[test]
    fn chat_finish_reason_maps_succeeded_to_stop() {
        assert_eq!(chat_finish_reason(GenerationState::Succeeded), "stop");
    }

    #[test]
    fn chat_finish_reason_maps_cancelled_distinctly_from_stop() {
        assert_eq!(chat_finish_reason(GenerationState::Cancelled), "cancelled");
    }

    #[test]
    fn chat_finish_reason_never_reports_stop_for_a_failed_generation() {
        assert_ne!(chat_finish_reason(GenerationState::Failed), "stop");
    }

    #[test]
    fn terminal_error_body_without_a_row_still_reports_an_error() {
        let body = terminal_error_body(None, GenerationState::Failed);
        assert_eq!(body.error.kind, "server_error");
        assert!(body.error.message.contains("failed"));
    }

    #[test]
    fn usage_chunk_carries_usage_with_no_choices() {
        let usage = UsageDto {
            prompt_tokens: 3,
            completion_tokens: 5,
            total_tokens: 8,
        };
        let chunk = usage_chunk(&sample_ctx(), usage);
        assert!(chunk.choices.is_empty());
        let Some(chunk_usage) = chunk.usage else {
            panic!("expected usage to be set");
        };
        assert_eq!(chunk_usage.total_tokens, 8);
    }

    #[test]
    fn role_chunk_announces_the_assistant_role() {
        let chunk = role_chunk(&sample_ctx());
        assert_eq!(chunk.choices[0].delta.role, Some("assistant"));
        assert!(chunk.choices[0].delta.content.is_none());
    }

    #[test]
    fn data_event_builds_successfully_from_a_chunk() {
        assert!(
            super::super::sse::data_event(&content_chunk(&sample_ctx(), "hi".to_owned())).is_ok()
        );
    }

    #[test]
    fn content_chunk_json_shape_matches_openai() {
        let chunk = content_chunk(&sample_ctx(), "hi".to_owned());
        let Ok(value) = serde_json::to_value(&chunk) else {
            panic!("expected chunk to serialize to JSON");
        };
        assert_eq!(value["object"], "chat.completion.chunk");
        assert_eq!(value["choices"][0]["delta"]["content"], "hi");
        assert!(value["choices"][0]["delta"].get("role").is_none());
    }
}
