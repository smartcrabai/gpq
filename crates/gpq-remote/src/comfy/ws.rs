//! Client-scoped `ComfyUI` WebSocket event translation.

use std::collections::BTreeMap;
use std::time::Duration;

use axum::Router;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::Utc;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use super::{ComfyError, error_response, is_terminal};
use crate::events::GenerationEvent;
use crate::state::AppState;
use gpq_domain::{GenerationId, GenerationState, TenantId};

/// Builds the `ComfyUI` WebSocket route.
pub fn router() -> Router<AppState> {
    Router::new().route("/ws", get(websocket))
}

#[derive(Debug, Clone)]
struct PromptTrack {
    row: crate::db::comfy::ComfyHistoryRow,
    cursor: i64,
    terminal_sent: bool,
}

fn requested_client_id(uri: &Uri) -> Result<Option<String>, ComfyError> {
    let mut client_id = None;
    for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "clientId" => client_id = Some(value.into_owned()),
            _ => {
                return Err(ComfyError::invalid(
                    "invalid_request",
                    format!("unsupported WebSocket option '{key}'"),
                ));
            }
        }
    }
    if let Some(client_id) = client_id {
        if client_id.len() > 200 {
            return Err(ComfyError::invalid(
                "invalid_client_id",
                "clientId must be at most 200 bytes",
            ));
        }
        if !client_id.is_empty() {
            return Ok(Some(client_id));
        }
    }
    Ok(None)
}

async fn websocket(
    State(state): State<AppState>,
    crate::openai::TenantAuth(tenant_id): crate::openai::TenantAuth,
    ws: WebSocketUpgrade,
    uri: Uri,
) -> Response {
    let client_id = match requested_client_id(&uri) {
        Ok(Some(client_id)) => client_id,
        Ok(None) => Uuid::now_v7().to_string(),
        Err(error) => return error_response(error),
    };
    // Subscribe before taking the database snapshot. The receiver is moved into
    // the upgrade task, so commits concurrent with the snapshot cannot be lost.
    let notifications = state.events.subscribe_tenant(tenant_id);
    ws.on_upgrade(move |socket| run(socket, state, tenant_id, client_id, notifications))
        .into_response()
}

async fn run(
    mut socket: WebSocket,
    state: AppState,
    tenant_id: TenantId,
    client_id: String,
    mut notifications: broadcast::Receiver<GenerationId>,
) {
    let snapshot = match snapshot(&state, tenant_id, &client_id).await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(%error, "Comfy WebSocket snapshot failed");
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    if send_json(
        &mut socket,
        serde_json::json!({
            "type": "status",
            "data": {
                "status": {"exec_info": {"queue_remaining": snapshot.queue_remaining}},
                "sid": client_id,
            }
        }),
    )
    .await
    .is_err()
    {
        return;
    }

    let mut tracks = snapshot.tracks;
    for track in tracks.values_mut() {
        if replay_track(&state, tenant_id, &client_id, track, &mut socket)
            .await
            .is_err()
        {
            return;
        }
    }
    let mut watermark = snapshot.watermark;
    let mut queue_remaining = snapshot.queue_remaining;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let shutdown = state.shutdown.clone();

    loop {
        tokio::select! {
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() { return; }
                    }
                    Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                    Some(Ok(Message::Pong(_) | Message::Text(_) | Message::Binary(_))) => {}
                }
            }
            notification = notifications.recv() => {
                match notification {
                    Ok(generation_id) => {
                        if process_notification(&state, tenant_id, &client_id, generation_id, &mut tracks, &mut socket).await.is_err() { return; }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if scan_new(&state, tenant_id, &client_id, &mut watermark, &mut tracks, &mut socket).await.is_err() { return; }
                        if replay_all(&state, tenant_id, &client_id, &mut tracks, &mut socket).await.is_err() { return; }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            _ = interval.tick() => {
                if scan_new(&state, tenant_id, &client_id, &mut watermark, &mut tracks, &mut socket).await.is_err() { return; }
                if replay_all(&state, tenant_id, &client_id, &mut tracks, &mut socket).await.is_err() { return; }
            }
            () = shutdown.cancelled() => return,
        }
        if maybe_send_status(&state, tenant_id, &mut queue_remaining, &mut socket)
            .await
            .is_err()
        {
            return;
        }
        remove_scanned_terminal_tracks(watermark, &mut tracks);
    }
}

struct Snapshot {
    watermark: i64,
    queue_remaining: i64,
    tracks: BTreeMap<GenerationId, PromptTrack>,
}

async fn snapshot(
    state: &AppState,
    tenant_id: TenantId,
    client_id: &str,
) -> anyhow::Result<Snapshot> {
    let mut tx = state.db.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT set_config('gpq.tenant_id', $1, true)")
        .bind(tenant_id.to_string())
        .execute(&mut *tx)
        .await?;
    let watermark = crate::db::comfy::max_number(&mut tx, tenant_id).await?;
    let queue_remaining = crate::db::comfy::count_nonterminal(&mut tx, tenant_id).await?;
    let mut tracks = BTreeMap::new();
    let mut after_number = -1;
    loop {
        let rows = crate::db::comfy::list_client_after_number(
            &mut tx,
            tenant_id,
            client_id,
            after_number,
            200,
        )
        .await?;
        let Some(last_number) = rows.last().map(|row| row.number) else {
            break;
        };
        let full_page = rows.len() == 200;
        for row in rows {
            if !is_terminal(&row.state) {
                let generation_id = GenerationId::from_uuid(row.generation_id);
                tracks.insert(
                    generation_id,
                    PromptTrack {
                        row,
                        cursor: 0,
                        terminal_sent: false,
                    },
                );
            }
        }
        after_number = last_number;
        if !full_page {
            break;
        }
    }
    tx.commit().await?;
    Ok(Snapshot {
        watermark,
        queue_remaining,
        tracks,
    })
}

async fn process_notification(
    state: &AppState,
    tenant_id: TenantId,
    client_id: &str,
    generation_id: GenerationId,
    tracks: &mut BTreeMap<GenerationId, PromptTrack>,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    if let Some(track) = tracks.get_mut(&generation_id) {
        replay_track(state, tenant_id, client_id, track, socket).await?;
        return Ok(());
    }
    let mut conn = state.db.begin_tenant(tenant_id).await?;
    let row = crate::db::comfy::get_by_generation(&mut conn, tenant_id, generation_id).await?;
    conn.commit().await?;
    let Some(row) = row else {
        return Ok(());
    };
    if row.client_id.as_deref() != Some(client_id) {
        return Ok(());
    }
    let Some(row) = fetch_history_row(state, tenant_id, row.prompt_id).await? else {
        return Ok(());
    };
    let mut track = PromptTrack {
        row,
        cursor: 0,
        terminal_sent: false,
    };
    replay_track(state, tenant_id, client_id, &mut track, socket).await?;
    tracks.insert(generation_id, track);
    Ok(())
}

async fn scan_new(
    state: &AppState,
    tenant_id: TenantId,
    client_id: &str,
    watermark: &mut i64,
    tracks: &mut BTreeMap<GenerationId, PromptTrack>,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    loop {
        let mut conn = state.db.begin_tenant(tenant_id).await?;
        let rows = crate::db::comfy::list_client_after_number(
            &mut conn, tenant_id, client_id, *watermark, 200,
        )
        .await?;
        conn.commit().await?;
        let Some(last) = rows.last().map(|row| row.number) else {
            break;
        };
        let full_page = rows.len() == 200;
        for row in rows {
            let generation_id = GenerationId::from_uuid(row.generation_id);
            if let Some(track) = tracks.get_mut(&generation_id) {
                replay_track(state, tenant_id, client_id, track, socket).await?;
            } else {
                let mut track = PromptTrack {
                    row,
                    cursor: 0,
                    terminal_sent: false,
                };
                replay_track(state, tenant_id, client_id, &mut track, socket).await?;
                tracks.insert(generation_id, track);
            }
        }
        *watermark = last;
        if !full_page {
            break;
        }
    }
    Ok(())
}

async fn replay_all(
    state: &AppState,
    tenant_id: TenantId,
    client_id: &str,
    tracks: &mut BTreeMap<GenerationId, PromptTrack>,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    for track in tracks.values_mut() {
        replay_track(state, tenant_id, client_id, track, socket).await?;
    }
    Ok(())
}

async fn replay_track(
    state: &AppState,
    tenant_id: TenantId,
    client_id: &str,
    track: &mut PromptTrack,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    let Some(row) = fetch_history_row(state, tenant_id, track.row.prompt_id).await? else {
        return Ok(());
    };
    if row.client_id.as_deref() != Some(client_id) {
        return Ok(());
    }
    // Refresh the row before replaying events. A track created while the
    // prompt was queued otherwise still carries its nonterminal snapshot when
    // a terminal State event is emitted, so emit_success cannot include the
    // accepted raw output metadata in the corresponding `executed` frames.
    track.row = row.clone();
    let generation_id = GenerationId::from_uuid(row.generation_id);
    let mut conn = state.db.begin_tenant(tenant_id).await?;
    let events =
        crate::db::events::load_since(&mut conn, tenant_id, generation_id, track.cursor).await?;
    conn.commit().await?;
    for event_row in events {
        track.cursor = track.cursor.max(event_row.sequence);
        let Some(event) = crate::events::decode_persisted(&event_row) else {
            continue;
        };
        emit_event(
            track,
            event,
            event_row.created_at.timestamp_millis(),
            socket,
        )
        .await?;
    }
    track.row = row;
    if is_terminal(&track.row.state) && !track.terminal_sent {
        emit_terminal_from_row(track, socket).await?;
    }
    Ok(())
}

async fn emit_event(
    track: &mut PromptTrack,
    event: GenerationEvent,
    timestamp: i64,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    let prompt_id = track.row.prompt_id.to_string();
    match event {
        GenerationEvent::State { state, failure, .. } => match state {
            GenerationState::Running => {
                send_json(socket, serde_json::json!({"type":"execution_start","data":{"prompt_id":prompt_id,"timestamp":timestamp}})).await?;
            }
            GenerationState::Succeeded if !track.terminal_sent => {
                emit_success(track, timestamp, socket).await?;
            }
            GenerationState::Failed | GenerationState::Expired if !track.terminal_sent => {
                let (kind, message) = failure.map_or_else(
                    || {
                        (
                            if state == GenerationState::Expired {
                                "expired".to_owned()
                            } else {
                                "internal".to_owned()
                            },
                            "generation failed".to_owned(),
                        )
                    },
                    |(kind, message)| (kind.as_str().to_owned(), message),
                );
                send_json(socket, serde_json::json!({"type":"execution_error","data":{"prompt_id":prompt_id,"exception_type":kind,"exception_message":message,"traceback":[]}})).await?;
                track.terminal_sent = true;
            }
            GenerationState::Cancelled if !track.terminal_sent => {
                send_json(socket, serde_json::json!({"type":"execution_interrupted","data":{"prompt_id":prompt_id}})).await?;
                track.terminal_sent = true;
            }
            GenerationState::Succeeded
            | GenerationState::Failed
            | GenerationState::Expired
            | GenerationState::Cancelled
            | GenerationState::Queued
            | GenerationState::Cancelling => {}
        },
        GenerationEvent::Progress {
            stage,
            step,
            total_steps,
            ..
        } => {
            if !stage.is_empty() {
                send_json(socket, serde_json::json!({"type":"executing","data":{"prompt_id":prompt_id,"node":stage}})).await?;
            }
            if total_steps > 0 {
                send_json(socket, serde_json::json!({"type":"progress","data":{"prompt_id":prompt_id,"node":stage,"value":step,"max":total_steps}})).await?;
            }
        }
        GenerationEvent::Token { .. }
        | GenerationEvent::Output
        | GenerationEvent::Discontinuity { .. } => {}
    }
    Ok(())
}

async fn emit_success(
    track: &mut PromptTrack,
    timestamp: i64,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    let prompt_id = track.row.prompt_id.to_string();
    if let Some(outputs) = track.row.outputs.as_ref().and_then(Value::as_object) {
        for node_id in &track.row.output_node_ids {
            if let Some(output) = outputs.get(node_id) {
                send_json(socket, serde_json::json!({"type":"executed","data":{"prompt_id":prompt_id,"node":node_id,"output":output}})).await?;
            }
        }
    }
    send_json(socket, serde_json::json!({"type":"execution_success","data":{"prompt_id":prompt_id,"timestamp":timestamp}})).await?;
    send_json(
        socket,
        serde_json::json!({"type":"executing","data":{"prompt_id":prompt_id,"node":Value::Null}}),
    )
    .await?;
    track.terminal_sent = true;
    Ok(())
}

async fn emit_terminal_from_row(
    track: &mut PromptTrack,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    let prompt_id = track.row.prompt_id.to_string();
    let timestamp = track.row.terminated_at.map_or_else(
        || Utc::now().timestamp_millis(),
        |time| time.timestamp_millis(),
    );
    match track.row.state.as_str() {
        "succeeded" => emit_success(track, timestamp, socket).await?,
        "cancelled" => {
            send_json(
                socket,
                serde_json::json!({"type":"execution_interrupted","data":{"prompt_id":prompt_id}}),
            )
            .await?;
            track.terminal_sent = true;
        }
        "failed" | "expired" => {
            send_json(socket, serde_json::json!({"type":"execution_error","data":{"prompt_id":prompt_id,"exception_type":track.row.failure_kind.as_deref().unwrap_or(if track.row.state == "expired" {"expired"} else {"internal"}),"exception_message":track.row.failure_message,"traceback":[]}})).await?;
            track.terminal_sent = true;
        }
        _ => {}
    }
    Ok(())
}

async fn fetch_history_row(
    state: &AppState,
    tenant_id: TenantId,
    prompt_id: Uuid,
) -> anyhow::Result<Option<crate::db::comfy::ComfyHistoryRow>> {
    let mut conn = state.db.begin_tenant(tenant_id).await?;
    let row = crate::db::comfy::history_by_prompt_id(&mut conn, tenant_id, prompt_id).await?;
    conn.commit().await?;
    Ok(row)
}

async fn maybe_send_status(
    state: &AppState,
    tenant_id: TenantId,
    previous: &mut i64,
    socket: &mut WebSocket,
) -> anyhow::Result<()> {
    let mut conn = state.db.begin_tenant(tenant_id).await?;
    let count = crate::db::comfy::count_nonterminal(&mut conn, tenant_id).await?;
    conn.commit().await?;
    if count != *previous {
        *previous = count;
        send_json(socket, serde_json::json!({"type":"status","data":{"status":{"exec_info":{"queue_remaining":count}}}})).await?;
    }
    Ok(())
}

fn remove_scanned_terminal_tracks(
    watermark: i64,
    tracks: &mut BTreeMap<GenerationId, PromptTrack>,
) {
    tracks.retain(|_, track| !(track.terminal_sent && track.row.number <= watermark));
}

async fn send_json(socket: &mut WebSocket, value: Value) -> anyhow::Result<()> {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .map_err(|error| anyhow::anyhow!(error))
}
