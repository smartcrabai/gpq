//! A fake `llama-server` implementing exactly the surface
//! `crates/gpq-worker/src/backend/llama.rs` speaks: `GET /health`, `GET
//! /props`, `GET /slots`, and `POST /v1/chat/completions` (JSON or SSE).
//! Runs inside the test process on an ephemeral port; the Worker's managed
//! process (a plain `sleep`) never talks to it directly, so killing that
//! process and restarting it (`backend_crash_recovery_restores_pool_readiness`)
//! never disturbs this server.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream;
use serde_json::json;

/// Behavior the fake backend should exhibit for the next request(s).
#[derive(Clone)]
pub enum FakeMode {
    /// Answer with `reply` after `delay`, as a `chat.completion` or an SSE
    /// `chat.completion.chunk` stream depending on the request's `stream`
    /// field.
    Reply { reply: String, delay: Duration },
    /// Answer every `/v1/chat/completions` call with a bare HTTP 500,
    /// regardless of `stream` (ADR 0003: classified `backend_crashed`).
    Failing,
}

impl FakeMode {
    #[must_use]
    pub fn reply(text: impl Into<String>) -> Self {
        Self::Reply {
            reply: text.into(),
            delay: Duration::ZERO,
        }
    }

    #[must_use]
    pub fn with_delay(self, delay: Duration) -> Self {
        match self {
            Self::Reply { reply, .. } => Self::Reply { reply, delay },
            Self::Failing => Self::Failing,
        }
    }
}

/// Handle to the running fake backend: flips its behavior between requests.
#[derive(Clone)]
pub struct FakeLlama {
    model_path: String,
    mode: Arc<Mutex<FakeMode>>,
}

fn lock(mutex: &Mutex<FakeMode>) -> MutexGuard<'_, FakeMode> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl FakeLlama {
    /// Binds `port` and serves the fake backend for the remainder of the
    /// test process, reporting `model_path` from `GET /props` so the
    /// Worker's pinned-model verification (ADR 0012) matches the same file
    /// the Worker itself hashed from `model_paths`.
    pub async fn spawn(port: u16, model_path: std::path::PathBuf) -> anyhow::Result<Self> {
        let handle = Self {
            model_path: model_path.display().to_string(),
            mode: Arc::new(Mutex::new(FakeMode::reply(
                "hello from the default fake reply",
            ))),
        };
        let router = Router::new()
            .route("/health", get(health))
            .route("/props", get(props))
            .route("/slots", get(slots))
            .route("/v1/chat/completions", post(chat_completions))
            .with_state(handle.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(handle)
    }

    pub fn set_mode(&self, mode: FakeMode) {
        *lock(&self.mode) = mode;
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

async fn props(State(fake): State<FakeLlama>) -> Json<serde_json::Value> {
    Json(json!({
        "total_slots": 2,
        "model_path": fake.model_path,
        "build_info": "fake-llama-1.0",
    }))
}

async fn slots() -> Json<serde_json::Value> {
    Json(json!([{}, {}]))
}

async fn chat_completions(
    State(fake): State<FakeLlama>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let mode = lock(&fake.mode).clone();
    let streaming = body
        .get("stream")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let (reply, delay) = match mode {
        FakeMode::Failing => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "synthetic backend failure",
            )
                .into_response();
        }
        FakeMode::Reply { reply, delay } => (reply, delay),
    };
    if delay > Duration::ZERO {
        tokio::time::sleep(delay).await;
    }

    if streaming {
        stream_reply(&reply).into_response()
    } else {
        Json(json!({
            "choices": [{"message": {"content": reply}}],
            "usage": {"prompt_tokens": 4, "completion_tokens": 3, "total_tokens": 7},
        }))
        .into_response()
    }
}

/// Splits `reply` into a handful of word-sized SSE `chat.completion.chunk`
/// deltas, followed by a usage-only chunk and the `[DONE]` sentinel — the
/// exact frame shapes `crates/gpq-worker/src/backend/llama.rs::parse_sse_frame`
/// decodes.
fn stream_reply(
    reply: &str,
) -> Sse<impl futures::Stream<Item = Result<Event, std::convert::Infallible>> + use<>> {
    let mut chunks: Vec<serde_json::Value> = reply
        .split_inclusive(' ')
        .map(|piece| json!({"choices": [{"delta": {"content": piece}}]}))
        .collect();
    if chunks.is_empty() {
        chunks.push(json!({"choices": [{"delta": {"content": reply}}]}));
    }
    chunks.push(json!({
        "choices": [],
        "usage": {"prompt_tokens": 4, "completion_tokens": 3, "total_tokens": 7},
    }));

    let events = chunks
        .into_iter()
        .map(|chunk| Ok(Event::default().data(chunk.to_string())))
        .chain(std::iter::once(Ok(Event::default().data("[DONE]"))));
    Sse::new(stream::iter(events))
}
