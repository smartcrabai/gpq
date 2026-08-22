//! Server-Sent Event helpers, the generic broadcast-to-SSE driver, and
//! disconnect-triggered cancellation for the OpenAI-compatible surface
//! (ADR 0006).
//!
//! `GenerationEvent` values coming off `EventHub::subscribe` are rendered
//! into Axum SSE frames by `chat.rs` and `responses.rs`, which each own the
//! wire shape of their own event/chunk types. This module holds what both
//! need: a uniform way to emit a `data: <json>` frame and the `[DONE]`
//! sentinel, [`unfold_broadcast`], which drives the shared
//! queue-then-broadcast-recv state machine behind their `Sse` streams, and
//! `CancelOnDrop`, which cancels a synchronous Generation when the
//! connection serving it disappears before completion (ADR 0003: caller
//! disconnect cancels the Generation).

use std::collections::VecDeque;
use std::convert::Infallible;

use axum::response::sse::Event;
use futures::Stream;
use futures::future::BoxFuture;
use gpq_domain::{GenerationId, GenerationState, TenantId};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::db::Db;
use crate::events::GenerationEvent;
use crate::state::AppState;

/// The literal SSE `data:` payload `OpenAI` clients use to end a stream.
pub(crate) const DONE: &str = "[DONE]";

/// Serializes `payload` into an unnamed `data: <json>` SSE frame, the shape
/// used by `chat.completion.chunk` frames.
pub(crate) fn data_event<T: Serialize>(payload: &T) -> Result<Event, serde_json::Error> {
    Ok(Event::default().data(serde_json::to_string(payload)?))
}

/// Serializes `payload` into a named `event: <name>` / `data: <json>` SSE
/// frame, the shape the Responses API streaming surface uses.
pub(crate) fn named_event<T: Serialize>(
    name: &'static str,
    payload: &T,
) -> Result<Event, serde_json::Error> {
    Ok(Event::default()
        .event(name)
        .data(serde_json::to_string(payload)?))
}

/// The literal `[DONE]` sentinel frame.
pub(crate) fn done_event() -> Event {
    Event::default().data(DONE)
}

/// Cancels a synchronous Generation when the connection serving it is
/// dropped before it reaches a terminal state.
///
/// Held as a plain local variable (or moved into a response stream's state),
/// this guard relies on Axum/Hyper dropping the handler future or response
/// stream when the underlying h2c connection resets (ADR 0019: plaintext h2c
/// transport), which is how mid-flight client disconnects surface here.
///
/// Transport caveat, verified end to end: over HTTP/2 a client reset cancels
/// the in-flight handler immediately, and over HTTP/1.1 a streaming response
/// notices the reset on its next frame write. An HTTP/1.1 *non-streaming*
/// request is the one case where a disconnect is not observed while the handler
/// waits, because Hyper does not read a half-closed HTTP/1.1 connection until
/// it writes the response; those Generations still terminate through their
/// execution deadline and lease machinery (ADR 0003).
///
/// Callers that reach a terminal state through the normal path MUST call
/// [`CancelOnDrop::disarm`] first so a completed Generation is never raced
/// with a spurious cancellation.
pub(crate) struct CancelOnDrop {
    db: Db,
    artifacts: crate::artifacts::ArtifactService,
    tenant_id: TenantId,
    generation_id: GenerationId,
    disarmed: bool,
}

impl CancelOnDrop {
    /// Arms cancellation for `generation_id`.
    #[must_use]
    pub(crate) fn new(
        db: Db,
        artifacts: crate::artifacts::ArtifactService,
        tenant_id: TenantId,
        generation_id: GenerationId,
    ) -> Self {
        Self {
            db,
            artifacts,
            tenant_id,
            generation_id,
            disarmed: false,
        }
    }

    /// Disarms the guard: the Generation already reached a terminal state
    /// through the normal completion path, so `Drop` must not cancel it.
    pub(crate) fn disarm(mut self) {
        self.disarmed = true;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        let db = self.db.clone();
        let artifacts = self.artifacts.clone();
        let tenant_id = self.tenant_id;
        let generation_id = self.generation_id;
        tokio::spawn(async move {
            let Ok(mut tx) = db.begin_tenant(tenant_id).await else {
                tracing::warn!(%generation_id, "disconnect cancellation: failed to begin tenant transaction");
                return;
            };
            let now = match sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT now()")
                .fetch_one(&mut *tx)
                .await
            {
                Ok(now) => now,
                Err(err) => {
                    tracing::warn!(%generation_id, error = %err, "disconnect cancellation: failed to read database time");
                    return;
                }
            };
            let cancelled_queued =
                crate::db::generations::cancel_queued(&mut tx, tenant_id, generation_id, now)
                    .await
                    .unwrap_or(false);
            if !cancelled_queued
                && let Err(err) = crate::db::generations::request_cancel_running(
                    &mut tx,
                    tenant_id,
                    generation_id,
                    now,
                )
                .await
            {
                tracing::warn!(%generation_id, error = %err, "disconnect cancellation: request_cancel_running failed");
            }
            // ADR 0008: "Inputs are deleted when the Generation terminates" —
            // `cancel_queued` settles it immediately; `request_cancel_running`
            // only requests cooperative cancellation, so the Generation is not
            // yet terminal and its inputs are still needed by the Worker.
            let deleted_inputs = if cancelled_queued {
                crate::db::artifacts::delete_inputs_for_generation(
                    &mut tx,
                    tenant_id,
                    generation_id,
                )
                .await
                .unwrap_or_default()
            } else {
                Vec::new()
            };
            if let Err(err) = tx.commit().await {
                tracing::warn!(%generation_id, error = %err, "disconnect cancellation: commit failed");
                return;
            }
            crate::session::release_deleted_inputs(&artifacts, deleted_inputs).await;
        });
    }
}

/// One incoming-token frame hook: turns a `GenerationEvent::Token`'s text
/// into the caller's wire-shaped frame, or `None` if it failed to
/// serialize — dropping the frame and continuing the stream, matching the
/// existing Chat and Responses behavior.
pub(crate) type OnToken = Box<dyn FnMut(String) -> Option<Event> + Send>;

/// One terminal-state frame hook: turns the terminal `GenerationState`
/// (reached directly, or recovered after a `Lagged` broadcast by
/// re-reading the Generation row) into the caller's ordered frames that
/// precede `[DONE]`. [`unfold_broadcast`] calls this at most once per
/// stream, so it is `FnOnce`.
pub(crate) type OnTerminal =
    Box<dyn FnOnce(GenerationState) -> BoxFuture<'static, VecDeque<Event>> + Send>;

/// Shared state driving one SSE stream off a Generation's broadcast
/// channel; see [`unfold_broadcast`].
struct BroadcastFrames {
    state: AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    rx: broadcast::Receiver<GenerationEvent>,
    queue: VecDeque<Event>,
    finished: bool,
    guard: Option<CancelOnDrop>,
    on_token: OnToken,
    on_terminal: Option<OnTerminal>,
}

/// Runs `st`'s terminal hook (if it has not already fired), appends its
/// frames followed by `[DONE]`, and disarms the drop guard — the tail
/// shared by every termination path except a bare `RecvError::Closed`.
async fn finish_with(st: &mut BroadcastFrames, generation_state: GenerationState) {
    if let Some(on_terminal) = st.on_terminal.take() {
        st.queue.extend(on_terminal(generation_state).await);
    }
    st.queue.push_back(done_event());
    if let Some(guard) = st.guard.take() {
        guard.disarm();
    }
}

async fn advance(mut st: BroadcastFrames) -> Option<(Result<Event, Infallible>, BroadcastFrames)> {
    loop {
        if let Some(event) = st.queue.pop_front() {
            return Some((Ok(event), st));
        }
        if st.finished {
            return None;
        }
        match st.rx.recv().await {
            Ok(GenerationEvent::Token { text }) => {
                if let Some(event) = (st.on_token)(text) {
                    st.queue.push_back(event);
                }
            }
            Ok(GenerationEvent::State {
                state: generation_state,
                ..
            }) if generation_state.is_terminal() => {
                finish_with(&mut st, generation_state).await;
                st.finished = true;
            }
            Ok(
                GenerationEvent::Progress { .. }
                | GenerationEvent::Output
                | GenerationEvent::State { .. },
            ) => {}
            Ok(GenerationEvent::Discontinuity { reason }) => {
                tracing::warn!(generation_id = %st.generation_id, %reason, "generation event discontinuity");
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                if let Ok(row) =
                    super::fetch_generation_row(&st.state, st.tenant_id, st.generation_id).await
                    && let Ok(generation_state) = row.state()
                    && generation_state.is_terminal()
                {
                    finish_with(&mut st, generation_state).await;
                    st.finished = true;
                }
            }
            Err(broadcast::error::RecvError::Closed) => {
                st.queue.push_back(done_event());
                if let Some(guard) = st.guard.take() {
                    guard.disarm();
                }
                st.finished = true;
            }
        }
    }
}

/// Drives one SSE stream off a Generation's broadcast channel, the engine
/// shared by the Chat Completions and Responses streaming surfaces (ADR
/// 0006). Each poll first drains `initial_queue`/any already-queued frames
/// (the caller's priming frame — the Chat `role` chunk or the Responses
/// `response.created` event — lives there), and only then waits for the
/// next [`GenerationEvent`]:
///
/// - `Token` becomes a frame through `on_token`.
/// - A terminal `State` — reached directly, or recovered after a `Lagged`
///   broadcast by re-reading the Generation row — becomes the caller's
///   frames through `on_terminal`, followed by the `[DONE]` sentinel.
/// - `Progress`, `Output`, and a non-terminal `State` are no-ops.
/// - `Discontinuity` is logged and otherwise ignored.
/// - `RecvError::Closed` ends the stream with a bare `[DONE]`.
///
/// `guard` disarms exactly once, on whichever of those two termination
/// paths fires first.
#[expect(
    clippy::too_many_arguments,
    reason = "each argument seeds one immutable streaming-response field"
)]
pub(crate) fn unfold_broadcast(
    state: AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    rx: broadcast::Receiver<GenerationEvent>,
    guard: CancelOnDrop,
    initial_queue: VecDeque<Event>,
    on_token: OnToken,
    on_terminal: OnTerminal,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let initial = BroadcastFrames {
        state,
        tenant_id,
        generation_id,
        rx,
        queue: initial_queue,
        finished: false,
        guard: Some(guard),
        on_token,
        on_terminal: Some(on_terminal),
    };
    futures::stream::unfold(initial, advance)
}
