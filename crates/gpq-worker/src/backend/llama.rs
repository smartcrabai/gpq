//! Adapter over a managed `llama-server` process (ADR 0005).
//!
//! Every operation is a plain HTTP request against the loopback address
//! configured for the owning Device Pool (`PoolConfig::base_url`); this
//! adapter never uses FFI or Python and never talks to any address other
//! than that Pool's own `llama-server` (ADR 0005: "Adapters use only managed
//! subprocesses and loopback llama-server HTTP/SSE ... APIs, never C/C++ FFI
//! or Python imports"). The process itself is spawned and supervised by
//! `crate::process`; this module only speaks the wire protocol.
//!
//! Endpoints used (llama.cpp `tools/server/README.md`, verified against
//! `ggml-org/llama.cpp@9ee9fc0`):
//! - `GET /health` — liveness/readiness (README.md:462-473).
//! - `GET /props` — model metadata, `total_slots`, `model_path`,
//!   `build_info` (README.md:823-914).
//! - `GET /slots` — one entry per continuous-batching slot (README.md:967-1114).
//! - `POST /v1/chat/completions` — OpenAI-compatible chat, SSE `data:` frames
//!   terminated by `data: [DONE]` when `stream: true` (README.md:1301-1452).
//!
//! llama-server exposes no per-request cancel-by-id endpoint: the only way to
//! stop an in-flight completion server-side is to close the HTTP connection
//! (README-dev.md / server-queue.cpp `should_stop`), which is what
//! `execute` does when its `CancellationToken` fires. Likewise a plain
//! single-model `llama-server` (as opposed to a `--models-dir` router
//! server) exposes no manual unload endpoint, so `release_memory` always
//! reports `Ok(false)` and lets the supervisor terminate the process
//! (ADR 0005: "failed release triggers process termination").

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use futures::StreamExt;
use gpq_domain::{BackendKind, ContentHash, FailureKind};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use super::{Backend, BackendCapabilities, BackendError, ExecutionEvent, ExecutionRequest};
use crate::config::PoolConfig;
use crate::models::hash_model;

/// Adapter over one managed `llama-server` process, reachable only at the
/// loopback `base_url` configured for its Device Pool (ADR 0005).
pub struct LlamaBackend {
    client: reqwest::Client,
    base_url: url::Url,
    state_dir: std::path::PathBuf,
    /// The Model Version this adapter's process was first observed holding
    /// resident, bound once and held fixed for this adapter's lifetime
    /// (ADR 0012). `pool.rs::start_pool_process` constructs a fresh
    /// `LlamaBackend` every time it (re)spawns the managed process, so this
    /// adapter's lifetime already matches the process's: the first probe or
    /// Attempt after construction is "the moment the runtime is started".
    resident: Mutex<Option<ResidentModel>>,
}

/// A cheap fingerprint of a file's size and modification time, used to
/// detect the Model Version file on disk changing without re-reading (and
/// re-hashing) its contents on every check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileSnapshot {
    size: u64,
    modified: std::time::SystemTime,
}

impl FileSnapshot {
    fn read(path: &Path) -> std::io::Result<Self> {
        let metadata = std::fs::metadata(path)?;
        Ok(Self {
            size: metadata.len(),
            modified: metadata.modified()?,
        })
    }
}

/// The Model Version hash bound to a running `llama-server` process the
/// first time its `/props.model_path` was observed (ADR 0012).
///
/// llama-server loads weights into RAM once at startup and never re-reads
/// the file, so the hash that matters is whatever was on disk at that
/// moment, not whatever happens to be on disk right now; holding it fixed
/// here also turns a repeated check into a metadata comparison instead of a
/// full re-hash of a potentially multi-gigabyte file.
struct ResidentModel {
    path: PathBuf,
    snapshot: FileSnapshot,
    hash: ContentHash,
}

impl LlamaBackend {
    /// Builds an adapter for the `llama-server` process backing `pool`.
    ///
    /// This does not spawn or probe the process; it only records where to
    /// reach it once `crate::process` has it running.
    #[must_use]
    pub fn new(pool: &PoolConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: pool.base_url.clone(),
            state_dir: pool.state_dir.clone(),
            resident: Mutex::new(None),
        }
    }

    /// Builds the absolute URL for a `llama-server` route under `base_url`.
    fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.base_url.as_str().trim_end_matches('/'))
    }

    /// `GET /props` (README.md:823-914): model metadata and slot count.
    /// Returns `None` on any transport, status, or decode failure so `probe`
    /// can report a failed probe instead of an error (ADR 0005).
    async fn fetch_props(&self) -> Option<PropsResponse> {
        let response = self.client.get(self.endpoint("/props")).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json::<PropsResponse>().await.ok()
    }

    /// `GET /slots` (README.md:967-1114): one entry per continuous-batching
    /// slot. Only the count is used here; `--no-slots` deployments simply
    /// yield `None` and `probe` falls back to `/props.total_slots`.
    async fn fetch_slots(&self) -> Option<Vec<serde_json::Value>> {
        let response = self.client.get(self.endpoint("/slots")).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json::<Vec<serde_json::Value>>().await.ok()
    }

    /// Sends the translated chat-completion request and dispatches to the
    /// streaming or non-streaming response reader. `attempt_id` is only
    /// used for diagnostics (e.g. logging tool calls this adapter cannot
    /// yet forward).
    async fn run_chat_completion(
        &self,
        payload: Value,
        stream_tokens: bool,
        events: &Sender<ExecutionEvent>,
        cancel: &CancellationToken,
        attempt_id: &str,
    ) -> Result<(), BackendError> {
        let send = self
            .client
            .post(self.endpoint("/v1/chat/completions"))
            .json(&payload)
            .send();
        let response = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(cancelled_error()),
            result = send => result,
        }
        .map_err(|err| super::normalize_transport_error(&err))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(classify_http_status(status, &body));
        }

        if stream_tokens {
            stream_response(response, events, cancel, attempt_id).await
        } else {
            consume_response(response, events, attempt_id).await
        }
    }

    /// Confirms the Model Version llama-server currently has loaded matches
    /// the Attempt's pinned hash (ADR 0012), against the hash bound to this
    /// process by [`Self::resident_model_hash`] rather than whatever bytes
    /// happen to be on disk right now.
    async fn verify_pinned_model(&self, request: &ExecutionRequest) -> Result<(), BackendError> {
        let Some(expected_hash) = request.model_sha256 else {
            return Ok(());
        };
        let Some(props) = self.fetch_props().await else {
            return Err(BackendError {
                kind: FailureKind::BackendCrashed,
                message: "llama-server did not answer /props while verifying the pinned model version (ADR 0012)".to_string(),
                retry_hint: true,
            });
        };
        let loaded = match props.model_path.as_deref() {
            Some(path) => Some(self.resident_model_hash(path).await?),
            None => None,
        };
        verify_resident_model(loaded, expected_hash, request.model_path.as_deref())
    }

    /// Returns the Model Version hash bound to this adapter's process,
    /// hashing `model_path` the first time it is observed and refusing to
    /// trust it again once the file changes on disk out from under the
    /// still-running server (ADR 0012): llama-server never re-reads the
    /// file after loading, so a changed file no longer describes what is
    /// actually resident, in either direction (a swapped file must not pass
    /// verification just because it happens to match, and a restored file
    /// must not fail just because it briefly didn't).
    ///
    /// The hash itself runs on a blocking-pool thread
    /// ([`tokio::task::spawn_blocking`]) since `hash_model` streams and
    /// digests the whole file synchronously, which would otherwise stall
    /// this adapter's async executor thread — including this Pool's other
    /// concurrent Attempts (ADR 0005: llama.cpp continuous batching serves
    /// several Attempts per Pool) — for however long a multi-gigabyte model
    /// takes to read. Holding `resident`'s lock across that await is
    /// intentional: it serializes concurrent first-bind callers onto one
    /// hash instead of each racing `hash_model`'s on-disk cache.
    async fn resident_model_hash(&self, model_path: &str) -> Result<ContentHash, BackendError> {
        let path = PathBuf::from(model_path);
        let snapshot = FileSnapshot::read(&path).map_err(|err| BackendError {
            kind: FailureKind::ModelUnavailable,
            message: format!("reading metadata for model {model_path}: {err}"),
            retry_hint: true,
        })?;

        let mut resident = self.resident.lock().await;
        match resident.as_ref() {
            Some(bound) if bound.path == path && bound.snapshot == snapshot => {
                return Ok(bound.hash);
            }
            Some(bound) if bound.path == path => {
                return Err(BackendError {
                    kind: FailureKind::ModelUnavailable,
                    message: format!(
                        "model file {model_path} changed on disk after llama-server loaded it; refusing to trust a rehash against the still-running process (ADR 0012)"
                    ),
                    retry_hint: false,
                });
            }
            _ => {}
        }

        let state_dir = self.state_dir.clone();
        let hash_path = path.clone();
        let hash = tokio::task::spawn_blocking(move || hash_model(&state_dir, &hash_path))
            .await
            .map_err(|err| BackendError {
                kind: FailureKind::Internal,
                message: format!("model hashing task panicked: {err}"),
                retry_hint: FailureKind::Internal.is_retryable(),
            })?
            .map_err(|err| BackendError {
                kind: FailureKind::ModelUnavailable,
                message: format!("hashing model {model_path}: {err}"),
                retry_hint: true,
            })?;
        *resident = Some(ResidentModel {
            path,
            snapshot,
            hash,
        });
        Ok(hash)
    }
}

#[async_trait::async_trait]
impl Backend for LlamaBackend {
    async fn probe(&self) -> Result<BackendCapabilities, BackendError> {
        let mut probes = BTreeMap::new();

        // GET /health (README.md:462-473): without a live, model-loaded
        // server none of the required operations are possible.
        let health_ok = self
            .client
            .get(self.endpoint("/health"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        if !health_ok {
            probes.insert("generation".to_string(), false);
            probes.insert("streaming".to_string(), false);
            probes.insert("cancellation".to_string(), false);
            return Ok(BackendCapabilities {
                probes,
                ..BackendCapabilities::default()
            });
        }

        let props = self.fetch_props().await;
        let slots = self.fetch_slots().await;

        // A reachable, healthy server always answers /v1/chat/completions;
        // that single route serves both streaming and non-streaming modes
        // (README.md:1301-1338), so a successful /props probe stands in for
        // both. Cancellation is purely HTTP-disconnect-driven, so any
        // reachable server supports it.
        let generation_ok = props.is_some();
        probes.insert("generation".to_string(), generation_ok);
        probes.insert("streaming".to_string(), generation_ok);
        probes.insert("cancellation".to_string(), health_ok);

        let slot_count = slots
            .as_ref()
            .map(|slots| u32::try_from(slots.len()).unwrap_or(u32::MAX))
            .or_else(|| props.as_ref().and_then(|props| props.total_slots))
            .unwrap_or(0);

        let resident_model = props
            .as_ref()
            .and_then(|props| props.model_path.as_ref())
            .and_then(|path| hash_model(&self.state_dir, Path::new(path)).ok());

        Ok(BackendCapabilities {
            version: props
                .as_ref()
                .and_then(|props| props.build_info.clone())
                .unwrap_or_default(),
            slots: slot_count,
            resident_model,
            // llama.cpp's /props response carries no accelerator memory
            // figures; ADR 0005 treats accelerator memory as optional,
            // backend-derived telemetry.
            accelerator_memory_bytes: None,
            // llama.cpp has no custom-node concept; that field is ComfyUI-only.
            custom_nodes: BTreeMap::new(),
            probes,
        })
    }

    async fn execute(
        &self,
        request: ExecutionRequest,
        events: Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<(), BackendError> {
        self.verify_pinned_model(&request).await?;
        let deadline = request.deadline;
        let stream_tokens = request.stream_tokens;
        let payload = build_chat_request(&request)?;

        match tokio::time::timeout(
            deadline,
            self.run_chat_completion(
                payload,
                stream_tokens,
                &events,
                &cancel,
                &request.attempt_id,
            ),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(_) => Err(BackendError {
                kind: FailureKind::ExecutionTimedOut,
                message: format!("execution exceeded the {deadline:?} deadline (ADR 0003)"),
                retry_hint: FailureKind::ExecutionTimedOut.is_retryable(),
            }),
        }
    }

    async fn release_memory(&self) -> Result<bool, BackendError> {
        // No unload endpoint exists for a single-model llama-server
        // (`/models/unload` is router-server-only, README.md:1875-1915);
        // the supervisor must terminate the process instead (ADR 0005).
        Ok(false)
    }

    fn kind(&self) -> BackendKind {
        BackendKind::LlamaCpp
    }
}

/// `GET /props` response shape (README.md:823-914); only the fields this
/// adapter consumes are modeled, extras are ignored by default.
#[derive(Deserialize)]
struct PropsResponse {
    total_slots: Option<u32>,
    model_path: Option<String>,
    build_info: Option<String>,
}

/// Translates the opaque OpenAI-shaped `parameters` payload (ADR 0007) into
/// the request body for `POST /v1/chat/completions` (README.md:1301-1338):
/// `stream` is forced to the Attempt's requested mode and the Attempt's
/// pinned seed, when present, overrides whatever `parameters` carries.
/// Every other field (`messages`, `tools`, `response_format`, `stop`, ...)
/// passes through unchanged, since ADR 0007 keeps these payloads opaque.
fn build_chat_request(request: &ExecutionRequest) -> Result<Value, BackendError> {
    let Value::Object(mut body) = request.parameters.clone() else {
        return Err(BackendError {
            kind: FailureKind::InvalidInput,
            message: "chat completion parameters must be a JSON object".to_string(),
            retry_hint: FailureKind::InvalidInput.is_retryable(),
        });
    };
    body.insert("stream".to_string(), Value::Bool(request.stream_tokens));
    if let Some(seed) = request.seed {
        body.insert("seed".to_string(), Value::Number(seed.into()));
    }
    Ok(Value::Object(body))
}

/// Reads a non-streaming `/v1/chat/completions` response to completion and
/// emits its text, usage, and (final) decode timings.
async fn consume_response(
    response: reqwest::Response,
    events: &Sender<ExecutionEvent>,
    attempt_id: &str,
) -> Result<(), BackendError> {
    let body = response
        .bytes()
        .await
        .map_err(|err| super::normalize_transport_error(&err))?;
    let completion: ChatCompletion = serde_json::from_slice(&body).map_err(|err| BackendError {
        kind: FailureKind::Internal,
        message: format!("malformed response from llama-server /v1/chat/completions: {err}"),
        retry_hint: FailureKind::Internal.is_retryable(),
    })?;

    let message = completion
        .choices
        .into_iter()
        .next()
        .and_then(|choice| choice.message);
    if let Some(message) = message.as_ref()
        && !message.tool_calls.is_empty()
    {
        return Err(unsupported_tool_calls(attempt_id, message.tool_calls.len()));
    }
    let text = message
        .and_then(|message| message.content)
        .unwrap_or_default();
    let _ = events.send(ExecutionEvent::Text { text }).await;
    emit_usage_and_progress(events, completion.usage, completion.timings).await;
    Ok(())
}

/// Reads an SSE `/v1/chat/completions` stream, forwarding token deltas as
/// they arrive and the assembled text, usage, and timings once it ends.
/// Honors `cancel` by dropping the response (and its connection) mid-read,
/// which is llama-server's only cancellation signal (README-dev.md,
/// server-queue.cpp `should_stop`).
async fn stream_response(
    response: reqwest::Response,
    events: &Sender<ExecutionEvent>,
    cancel: &CancellationToken,
    attempt_id: &str,
) -> Result<(), BackendError> {
    let stream = response.bytes_stream();
    futures::pin_mut!(stream);

    // Raw bytes, not a `String`: `bytes_stream` chunk boundaries fall at
    // arbitrary byte offsets, so decoding each chunk on its own replaces
    // the halves of any multi-byte character split across a read with
    // U+FFFD. Decoding is deferred to whole frames below.
    let mut buffer: Vec<u8> = Vec::new();
    let mut assembled = String::new();

    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(cancelled_error()),
            chunk = stream.next() => chunk,
        };
        let Some(chunk) = next else { break };
        let bytes = chunk.map_err(|err| super::normalize_transport_error(&err))?;
        buffer.extend_from_slice(&bytes);

        while let Some(pos) = find_frame_end(&buffer) {
            let frame = buffer.drain(..=pos + 1).collect::<Vec<u8>>();
            // A frame ends at `\n\n`, which can never split a UTF-8
            // sequence, so the frame is always a whole character boundary.
            let frame = std::str::from_utf8(&frame).map_err(|err| BackendError {
                kind: FailureKind::Internal,
                message: format!("llama-server sent a non-UTF-8 SSE frame: {err}"),
                retry_hint: FailureKind::Internal.is_retryable(),
            })?;
            for event in parse_sse_frame(frame.trim_end(), attempt_id)? {
                match event {
                    SseEvent::Done => {
                        finish_stream(events, assembled).await;
                        return Ok(());
                    }
                    SseEvent::Token(text) => {
                        assembled.push_str(&text);
                        if events.send(ExecutionEvent::Token { text }).await.is_err() {
                            return Ok(());
                        }
                    }
                    SseEvent::Usage {
                        prompt_tokens,
                        completion_tokens,
                        total_tokens,
                    } => {
                        let _ = events
                            .send(ExecutionEvent::Usage {
                                prompt_tokens,
                                completion_tokens,
                                total_tokens,
                            })
                            .await;
                    }
                    SseEvent::Progress { step, total_steps } => {
                        let _ = events
                            .send(ExecutionEvent::Progress {
                                fraction: 1.0,
                                stage: "decode".to_string(),
                                step,
                                total_steps,
                            })
                            .await;
                    }
                }
            }
        }
    }

    finish_stream(events, assembled).await;
    Ok(())
}

/// Emits the final assembled text once an SSE stream ends without a
/// trailing `usage`/`timings` frame having already reported it.
async fn finish_stream(events: &Sender<ExecutionEvent>, assembled: String) {
    if !assembled.is_empty() {
        let _ = events.send(ExecutionEvent::Text { text: assembled }).await;
    }
}

/// Finds the index of the first byte of a `\n\n` SSE frame delimiter.
fn find_frame_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|pair| pair == b"\n\n")
}

/// The failure returned when llama-server reports a model-initiated tool
/// call. ADR 0006 promises tool calls on the OpenAI-compatible surface, but
/// neither [`ExecutionEvent`] nor the `AttemptResult` wire contract can
/// carry them yet, so this adapter refuses the Attempt loudly instead of
/// returning the empty completion a dropped `tool_calls` array produces —
/// a caller that sent `tools` must not be told the model simply said
/// nothing.
fn unsupported_tool_calls(attempt_id: &str, count: usize) -> BackendError {
    BackendError {
        kind: FailureKind::UnsupportedCapability,
        message: format!(
            "llama-server returned {count} tool call(s) for attempt {attempt_id}, which this Worker cannot forward (ADR 0006)"
        ),
        retry_hint: FailureKind::UnsupportedCapability.is_retryable(),
    }
}

/// Emits `Usage` and `Progress` events for whichever fields a completion
/// response reported (README.md:1421-1452: `usage`, `timings`).
async fn emit_usage_and_progress(
    events: &Sender<ExecutionEvent>,
    usage: Option<ChatUsage>,
    timings: Option<ChatTimings>,
) {
    if let Some(usage) = usage {
        let _ = events
            .send(ExecutionEvent::Usage {
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens,
            })
            .await;
    }
    if let Some(timings) = timings {
        let _ = events
            .send(ExecutionEvent::Progress {
                fraction: 1.0,
                stage: "decode".to_string(),
                step: timings.predicted_n,
                total_steps: timings.predicted_n,
            })
            .await;
    }
}

/// One decoded SSE `data:` frame from `/v1/chat/completions` streaming
/// output (README.md:1301-1452).
#[derive(Debug, Clone, PartialEq)]
enum SseEvent {
    /// A token delta to surface as `ExecutionEvent::Token`.
    Token(String),
    /// Final usage accounting reported on a chunk.
    Usage {
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
    },
    /// llama.cpp's cumulative prompt/decode timings, reported on the final
    /// streamed chunk (README.md:1421-1436) — the only progress signal this
    /// backend exposes, since llama.cpp has no per-step progress event.
    Progress { step: u32, total_steps: u32 },
    /// The `data: [DONE]` stream terminator.
    Done,
}

/// Parses one buffered SSE frame (the bytes between two `\n\n` delimiters)
/// into zero or more [`SseEvent`]s. Comment lines (`:` keep-alives) and
/// frames without a `data:` line produce no events; `[DONE]` short-circuits
/// to `Done` without attempting JSON decode.
fn parse_sse_frame(frame: &str, attempt_id: &str) -> Result<Vec<SseEvent>, BackendError> {
    let data_lines: Vec<&str> = frame
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .collect();
    if data_lines.is_empty() {
        return Ok(Vec::new());
    }
    let data = data_lines.join("\n");
    if data == "[DONE]" {
        return Ok(vec![SseEvent::Done]);
    }

    let chunk: ChatChunk = serde_json::from_str(&data).map_err(|err| BackendError {
        kind: FailureKind::Internal,
        message: format!("malformed SSE chunk from llama-server /v1/chat/completions: {err}"),
        retry_hint: FailureKind::Internal.is_retryable(),
    })?;

    let mut events = Vec::new();
    let delta = chunk
        .choices
        .first()
        .and_then(|choice| choice.delta.as_ref());
    if let Some(delta) = delta
        && !delta.tool_calls.is_empty()
    {
        return Err(unsupported_tool_calls(attempt_id, delta.tool_calls.len()));
    }
    if let Some(text) = delta
        .and_then(|delta| delta.content.clone())
        .filter(|text| !text.is_empty())
    {
        events.push(SseEvent::Token(text));
    }
    if let Some(usage) = chunk.usage {
        events.push(SseEvent::Usage {
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
        });
    }
    if let Some(timings) = chunk.timings {
        events.push(SseEvent::Progress {
            step: timings.predicted_n,
            total_steps: timings.predicted_n,
        });
    }
    Ok(events)
}

/// Maps an HTTP error response from `/v1/chat/completions` to the ADR 0003
/// failure taxonomy. Checked in priority order: accelerator OOM text first
/// (it can arrive on any status code the server chooses), then a
/// model-not-found/not-loaded message, then generic 4xx validation errors,
/// with anything else falling back to `Internal`.
fn classify_http_status(status: reqwest::StatusCode, body: &str) -> BackendError {
    let lower = body.to_lowercase();
    let mentions_oom = lower.contains("out of memory")
        || lower.contains("oom")
        || (lower.contains("cuda error") && lower.contains("memory"));
    let mentions_missing_model = lower.contains("model")
        && (lower.contains("not found")
            || lower.contains("not loaded")
            || lower.contains("unknown")
            || lower.contains("loading model"));

    if mentions_oom {
        return BackendError {
            kind: FailureKind::OutOfMemory,
            message: body.to_string(),
            retry_hint: FailureKind::OutOfMemory.is_retryable(),
        };
    }
    if mentions_missing_model {
        return BackendError {
            kind: FailureKind::ModelUnavailable,
            message: body.to_string(),
            retry_hint: FailureKind::ModelUnavailable.is_retryable(),
        };
    }
    if status.is_client_error() {
        return BackendError {
            kind: FailureKind::InvalidInput,
            message: body.to_string(),
            retry_hint: FailureKind::InvalidInput.is_retryable(),
        };
    }
    if status.is_server_error() {
        // The backend itself failed, which ADR 0003 treats as a retryable
        // backend crash rather than an unclassified internal error.
        return BackendError {
            kind: FailureKind::BackendCrashed,
            message: format!("llama-server returned {status}: {body}"),
            retry_hint: FailureKind::BackendCrashed.is_retryable(),
        };
    }
    BackendError {
        kind: FailureKind::Internal,
        message: format!("llama-server returned {status}: {body}"),
        retry_hint: FailureKind::Internal.is_retryable(),
    }
}

/// Compares the Model Version llama-server reports as loaded against the
/// Attempt's pinned hash (ADR 0012). `expected_path` is included only for
/// the diagnostic message; the hash comparison is authoritative.
fn verify_resident_model(
    loaded: Option<ContentHash>,
    expected_hash: ContentHash,
    expected_path: Option<&Path>,
) -> Result<(), BackendError> {
    match loaded {
        Some(hash) if hash == expected_hash => Ok(()),
        Some(hash) => Err(BackendError {
            kind: FailureKind::ModelUnavailable,
            message: format!(
                "llama-server has model {hash} loaded but this attempt is pinned to {expected_hash}{}",
                expected_path.map_or_else(String::new, |path| format!(" ({})", path.display()))
            ),
            retry_hint: false,
        }),
        None => Err(BackendError {
            kind: FailureKind::ModelUnavailable,
            message: "llama-server reports no loaded model".to_string(),
            retry_hint: true,
        }),
    }
}

/// Builds the `Cancelled` error reported when `execute`'s `CancellationToken`
/// fires before the Attempt finished (ADR 0003).
fn cancelled_error() -> BackendError {
    BackendError {
        kind: FailureKind::Cancelled,
        message: "attempt cancelled".to_string(),
        retry_hint: FailureKind::Cancelled.is_retryable(),
    }
}

/// Non-streaming `/v1/chat/completions` response body (README.md:1301-1452).
#[derive(Deserialize)]
struct ChatCompletion {
    #[serde(default)]
    choices: Vec<ChatCompletionChoice>,
    usage: Option<ChatUsage>,
    timings: Option<ChatTimings>,
}

#[derive(Deserialize)]
struct ChatCompletionChoice {
    message: Option<ChatMessage>,
}

#[derive(Deserialize)]
struct ChatMessage {
    content: Option<String>,
    /// Model-initiated tool calls. Parsed only so they cannot be silently
    /// dropped into an empty completion; see [`unsupported_tool_calls`].
    #[serde(default)]
    tool_calls: Vec<serde_json::Value>,
}

/// One streamed `/v1/chat/completions` SSE chunk (README.md:1301-1436).
#[derive(Deserialize)]
struct ChatChunk {
    #[serde(default)]
    choices: Vec<ChatChunkChoice>,
    usage: Option<ChatUsage>,
    timings: Option<ChatTimings>,
}

#[derive(Deserialize)]
struct ChatChunkChoice {
    delta: Option<ChatDelta>,
}

#[derive(Deserialize)]
struct ChatDelta {
    content: Option<String>,
    /// Streaming counterpart of [`ChatMessage::tool_calls`].
    #[serde(default)]
    tool_calls: Vec<serde_json::Value>,
}

/// OpenAI-compatible `usage` object (README.md:1440-1451).
#[derive(Deserialize)]
#[expect(
    clippy::struct_field_names,
    reason = "these fields mirror llama-server's exact OpenAI-compatible JSON field names (README.md:1440-1451); renaming would require per-field #[serde(rename)] for no benefit"
)]
struct ChatUsage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

/// llama.cpp's `timings` extension (README.md:1421-1436); only the decoded
/// token count is used, as the closest available progress signal.
#[derive(Deserialize)]
struct ChatTimings {
    predicted_n: u32,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use gpq_domain::Modality;
    use serde_json::json;

    use super::*;

    fn sample_request() -> ExecutionRequest {
        ExecutionRequest {
            attempt_id: "attempt-1".to_string(),
            modality: Modality::Llm,
            model_sha256: None,
            model_path: None,
            workflow_graph: None,
            workflow_manifest: None,
            parameters: json!({"messages": []}),
            inputs: Vec::new(),
            seed: None,
            stream_tokens: false,
            deadline: Duration::from_mins(1),
        }
    }

    #[test]
    fn verify_resident_model_accepts_matching_hash() {
        let hash = ContentHash::digest(b"model-a");
        assert!(verify_resident_model(Some(hash), hash, None).is_ok());
    }

    #[test]
    fn verify_resident_model_rejects_mismatched_pinned_version() {
        let loaded = ContentHash::digest(b"model-a");
        let pinned = ContentHash::digest(b"model-b");
        let Err(error) = verify_resident_model(Some(loaded), pinned, None) else {
            panic!("a resident model different from the pinned hash must be rejected");
        };
        assert_eq!(error.kind, FailureKind::ModelUnavailable);
        assert!(!error.retry_hint);
    }

    #[test]
    fn verify_resident_model_rejects_no_loaded_model() {
        let pinned = ContentHash::digest(b"model-a");
        let Err(error) = verify_resident_model(None, pinned, None) else {
            panic!("no resident model must be rejected");
        };
        assert_eq!(error.kind, FailureKind::ModelUnavailable);
        assert!(error.retry_hint);
    }

    #[test]
    fn build_chat_request_forces_stream_and_passes_through_fields() {
        let mut request = sample_request();
        request.stream_tokens = true;
        request.parameters = json!({
            "model": "ignored-by-worker",
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [{"type": "function", "function": {"name": "f"}}],
            "response_format": {"type": "json_object"},
            "stop": ["</s>"],
            "stream": false,
        });

        let Ok(body) = build_chat_request(&request) else {
            panic!("expected translation to succeed");
        };
        assert_eq!(body.get("stream"), Some(&Value::Bool(true)));
        assert_eq!(body.get("messages"), request.parameters.get("messages"));
        assert_eq!(body.get("tools"), request.parameters.get("tools"));
        assert_eq!(
            body.get("response_format"),
            request.parameters.get("response_format")
        );
        assert_eq!(body.get("stop"), request.parameters.get("stop"));
        assert!(
            !body
                .as_object()
                .is_some_and(|object| object.contains_key("seed"))
        );
    }

    #[test]
    fn build_chat_request_injects_seed_overriding_existing() {
        let mut request = sample_request();
        request.parameters = json!({"messages": [], "seed": 1});
        request.seed = Some(42);

        let Ok(body) = build_chat_request(&request) else {
            panic!("expected translation to succeed");
        };
        assert_eq!(body.get("seed"), Some(&Value::from(42)));
    }

    #[test]
    fn build_chat_request_omits_seed_when_absent() {
        let request = sample_request();

        let Ok(body) = build_chat_request(&request) else {
            panic!("expected translation to succeed");
        };
        assert!(
            !body
                .as_object()
                .is_some_and(|object| object.contains_key("seed"))
        );
    }

    #[test]
    fn build_chat_request_rejects_non_object_parameters() {
        let mut request = sample_request();
        request.parameters = Value::Array(Vec::new());

        let Err(error) = build_chat_request(&request) else {
            panic!("expected rejection of non-object parameters");
        };
        assert_eq!(error.kind, FailureKind::InvalidInput);
    }

    #[test]
    fn parse_sse_frame_returns_done_on_sentinel() {
        let Ok(events) = parse_sse_frame("data: [DONE]", "attempt-test") else {
            panic!("expected sentinel to parse");
        };
        assert_eq!(events, vec![SseEvent::Done]);
    }

    #[test]
    fn parse_sse_frame_extracts_token_delta() {
        let frame = r#"data: {"choices":[{"delta":{"content":"hello"}}]}"#;
        let Ok(events) = parse_sse_frame(frame, "attempt-test") else {
            panic!("expected chunk to parse");
        };
        assert_eq!(events, vec![SseEvent::Token("hello".to_string())]);
    }

    #[test]
    fn parse_sse_frame_extracts_usage() {
        let frame = r#"data: {"choices":[],"usage":{"prompt_tokens":44,"completion_tokens":48,"total_tokens":92}}"#;
        let Ok(events) = parse_sse_frame(frame, "attempt-test") else {
            panic!("expected usage chunk to parse");
        };
        assert_eq!(
            events,
            vec![SseEvent::Usage {
                prompt_tokens: 44,
                completion_tokens: 48,
                total_tokens: 92,
            }]
        );
    }

    #[test]
    fn parse_sse_frame_extracts_timings_as_progress() {
        let frame = r#"data: {"choices":[],"timings":{"predicted_n":35}}"#;
        let Ok(events) = parse_sse_frame(frame, "attempt-test") else {
            panic!("expected timings chunk to parse");
        };
        assert_eq!(
            events,
            vec![SseEvent::Progress {
                step: 35,
                total_steps: 35,
            }]
        );
    }

    #[test]
    fn parse_sse_frame_ignores_frames_without_data() {
        let Ok(events) = parse_sse_frame(": keep-alive", "attempt-test") else {
            panic!("expected comment frame to parse to no events");
        };
        assert!(events.is_empty());
    }

    #[test]
    fn parse_sse_frame_rejects_malformed_json() {
        let Err(error) = parse_sse_frame("data: {not json}", "attempt-test") else {
            panic!("expected malformed JSON to be rejected");
        };
        assert_eq!(error.kind, FailureKind::Internal);
    }

    #[test]
    fn classify_http_status_maps_client_errors_to_invalid_input() {
        let error =
            classify_http_status(reqwest::StatusCode::BAD_REQUEST, "missing field messages");
        assert_eq!(error.kind, FailureKind::InvalidInput);
        assert!(!error.retry_hint);
    }

    #[test]
    fn classify_http_status_maps_missing_model_to_model_unavailable() {
        let error =
            classify_http_status(reqwest::StatusCode::NOT_FOUND, "model 'llama-3' not found");
        assert_eq!(error.kind, FailureKind::ModelUnavailable);
        assert!(!error.retry_hint);
    }

    #[test]
    fn classify_http_status_maps_oom_text_to_out_of_memory() {
        let error = classify_http_status(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "CUDA error: out of memory",
        );
        assert_eq!(error.kind, FailureKind::OutOfMemory);
        assert!(error.retry_hint);
    }

    #[test]
    fn classify_http_status_maps_server_errors_to_backend_crashed() {
        let error = classify_http_status(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "unexpected panic",
        );
        assert_eq!(error.kind, FailureKind::BackendCrashed);
        assert!(error.retry_hint, "a backend crash is retryable (ADR 0003)");
    }

    #[test]
    fn classify_http_status_maps_unclassifiable_statuses_to_internal() {
        let error = classify_http_status(reqwest::StatusCode::SEE_OTHER, "redirected");
        assert_eq!(error.kind, FailureKind::Internal);
        assert!(!error.retry_hint);
    }
}
