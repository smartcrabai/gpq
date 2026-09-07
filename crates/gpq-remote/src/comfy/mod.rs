//! `ComfyUI` Server API-compatible durable prompt surface.

mod ws;

use std::collections::BTreeMap;

use axum::Router;
use axum::body::to_bytes;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::admission::{AdmissionRequest, AdmissionTarget};
use crate::openai::TenantAuth;
use crate::state::AppState;

/// Maximum raw `ComfyUI` JSON request size.
pub(crate) const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_HISTORY_LIMIT: i64 = 50;
const MAX_HISTORY_LIMIT: i64 = 200;

/// Builds the ComfyUI-compatible HTTP and WebSocket routes.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/prompt", get(get_prompt).post(post_prompt))
        .route("/history", get(get_history))
        .route("/history/{prompt_id}", get(get_history_prompt))
        .route("/view", get(view))
        .merge(ws::router())
        .with_state(state)
}

#[derive(Debug, Clone)]
pub(crate) struct ComfyError {
    pub(crate) status: StatusCode,
    pub(crate) kind: &'static str,
    pub(crate) message: String,
}

impl ComfyError {
    fn new(status: StatusCode, kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            kind,
            message: message.into(),
        }
    }

    fn invalid(kind: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, kind, message)
    }
}

#[derive(Serialize)]
struct ErrorDetail {
    #[serde(rename = "type")]
    kind: &'static str,
    message: String,
    details: Value,
    extra_info: Value,
}

#[derive(Serialize)]
struct ErrorBody {
    error: ErrorDetail,
    node_errors: BTreeMap<String, Value>,
}

pub(crate) fn error_response(error: ComfyError) -> Response {
    (
        error.status,
        axum::Json(ErrorBody {
            error: ErrorDetail {
                kind: error.kind,
                message: error.message,
                details: Value::Object(Map::new()),
                extra_info: Value::Object(Map::new()),
            },
            node_errors: BTreeMap::new(),
        }),
    )
        .into_response()
}

fn internal_response(error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, "Comfy API database failure");
    error_response(ComfyError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "internal server error",
    ))
}

#[derive(Debug)]
struct ParsedPrompt {
    target: AdmissionTarget,
    parameters: Value,
}

fn parse_prompt_id(value: Option<&Value>) -> Result<Option<Uuid>, ComfyError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            let parsed = Uuid::parse_str(value).map_err(|_| {
                ComfyError::invalid("invalid_prompt_id", "prompt_id must be a valid UUID")
            })?;
            if parsed.to_string() != *value {
                return Err(ComfyError::invalid(
                    "invalid_prompt_id",
                    "prompt_id must be a canonical lowercase hyphenated UUID",
                ));
            }
            Ok(Some(parsed))
        }
        Some(_) => Err(ComfyError::invalid(
            "invalid_prompt_id",
            "prompt_id must be a UUID string or null",
        )),
    }
}

fn parse_client_id(value: Option<&Value>) -> Result<Option<String>, ComfyError> {
    let Some(value) = value else { return Ok(None) };
    let Some(value) = value.as_str() else {
        return Err(ComfyError::invalid(
            "invalid_client_id",
            "client_id must be a string",
        ));
    };
    if value.len() > 200 {
        return Err(ComfyError::invalid(
            "invalid_client_id",
            "client_id must be at most 200 bytes",
        ));
    }
    Ok(Some(value.to_owned()))
}

#[expect(
    clippy::too_many_lines,
    reason = "prompt validation keeps the upstream request contract in one ordered parser"
)]
fn parse_prompt_body(value: Value) -> Result<ParsedPrompt, ComfyError> {
    const ALLOWED: &[&str] = &[
        "prompt",
        "client_id",
        "prompt_id",
        "extra_data",
        "partial_execution_targets",
        "front",
        "number",
    ];
    let Value::Object(body) = value else {
        return Err(ComfyError::invalid(
            "invalid_prompt",
            "prompt request must be a JSON object",
        ));
    };
    if let Some(key) = body.keys().find(|key| !ALLOWED.contains(&key.as_str())) {
        return Err(ComfyError::invalid(
            "unsupported_option",
            format!("unsupported prompt option '{key}'"),
        ));
    }
    if body.contains_key("number") {
        return Err(ComfyError::invalid(
            "unsupported_option",
            "number is managed by the queue",
        ));
    }
    if body.get("front").is_some_and(|value| value != false) {
        return Err(ComfyError::invalid(
            "unsupported_option",
            "front=true is not supported",
        ));
    }
    let Some(prompt) = body.get("prompt") else {
        return Err(ComfyError::invalid("no_prompt", "No prompt provided"));
    };
    let Some(prompt) = prompt.as_object() else {
        return Err(ComfyError::invalid(
            "invalid_prompt",
            "prompt must be a non-empty object",
        ));
    };
    if prompt.is_empty() {
        return Err(ComfyError::invalid(
            "invalid_prompt",
            "prompt must be a non-empty object",
        ));
    }
    for (node_id, node) in prompt {
        let Some(node) = node.as_object() else {
            return Err(ComfyError::invalid(
                "invalid_prompt",
                format!("node '{node_id}' must be an object"),
            ));
        };
        if node
            .get("class_type")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
            || !node.get("inputs").is_some_and(Value::is_object)
        {
            return Err(ComfyError::invalid(
                "invalid_prompt",
                format!("node '{node_id}' must have class_type and inputs"),
            ));
        }
    }
    let client_id = parse_client_id(body.get("client_id"))?;
    let prompt_id = parse_prompt_id(body.get("prompt_id"))?;
    let extra_data = match body.get("extra_data") {
        None => Value::Object(Map::new()),
        Some(Value::Object(extra_data)) => Value::Object(extra_data.clone()),
        Some(_) => {
            return Err(ComfyError::invalid(
                "invalid_prompt",
                "extra_data must be an object",
            ));
        }
    };
    let partial_execution_targets = match body.get("partial_execution_targets") {
        None => None,
        Some(Value::Array(targets)) if !targets.is_empty() => {
            let mut parsed = Vec::with_capacity(targets.len());
            for target in targets {
                let Some(target) = target.as_str() else {
                    return Err(ComfyError::invalid(
                        "invalid_prompt",
                        "partial_execution_targets must contain strings",
                    ));
                };
                if !prompt.contains_key(target) {
                    return Err(ComfyError::invalid(
                        "invalid_prompt",
                        format!("partial execution target '{target}' is not in the prompt"),
                    ));
                }
                parsed.push(Value::String(target.to_owned()));
            }
            Some(Value::Array(parsed))
        }
        Some(Value::Array(_)) => {
            return Err(ComfyError::invalid(
                "invalid_prompt",
                "partial_execution_targets must not be empty",
            ));
        }
        Some(_) => {
            return Err(ComfyError::invalid(
                "invalid_prompt",
                "partial_execution_targets must be an array",
            ));
        }
    };

    let mut parameters = Map::new();
    parameters.insert("prompt".to_owned(), Value::Object(prompt.clone()));
    parameters.insert("extra_data".to_owned(), extra_data);
    if let Some(targets) = partial_execution_targets {
        parameters.insert("partial_execution_targets".to_owned(), targets);
    }
    Ok(ParsedPrompt {
        target: AdmissionTarget::ComfyPrompt {
            prompt_id,
            client_id,
        },
        parameters: Value::Object(parameters),
    })
}

async fn post_prompt(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
    request: Request,
) -> Response {
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_BODY_BYTES)
    {
        return error_response(ComfyError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "request body exceeds the 2 MiB limit",
        ));
    }
    let body = match to_bytes(request.into_body(), MAX_BODY_BYTES + 1).await {
        Ok(body) if body.len() <= MAX_BODY_BYTES => body,
        Ok(_) | Err(_) => {
            return error_response(ComfyError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "request body exceeds the 2 MiB limit",
            ));
        }
    };
    let parsed = match serde_json::from_slice::<Value>(&body) {
        Ok(value) => match parse_prompt_body(value) {
            Ok(value) => value,
            Err(error) => return error_response(error),
        },
        Err(error) => {
            return error_response(ComfyError::invalid(
                "invalid_json",
                format!("invalid JSON body: {error}"),
            ));
        }
    };
    let generation = match crate::admission::admit(
        &state,
        tenant_id,
        AdmissionRequest {
            target: parsed.target,
            parameters: parsed.parameters,
            input_artifact_ids: Vec::new(),
            output_placement: gpq_domain::ArtifactPlacement::WorkerLocal,
            priority: None,
            seed: None,
            execution_timeout: None,
            caller_kind: gpq_domain::CallerKind::Durable,
            stream_tokens: false,
            idempotency_key: None,
        },
    )
    .await
    {
        Ok(generation) => generation,
        Err(error) => return error_response(map_admission_error(error)),
    };
    let mut conn = match state.db.begin_tenant(tenant_id).await {
        Ok(conn) => conn,
        Err(error) => return internal_response(error),
    };
    let Some(prompt) = (match crate::db::comfy::get_by_generation(
        &mut conn,
        tenant_id,
        generation.generation_id(),
    )
    .await
    {
        Ok(prompt) => prompt,
        Err(error) => return internal_response(error),
    }) else {
        return internal_response("admitted raw prompt metadata is missing");
    };
    if let Err(error) = conn.commit().await {
        return internal_response(error);
    }
    (
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "prompt_id": prompt.prompt_id.to_string(),
            "number": prompt.number,
            "node_errors": {},
        })),
    )
        .into_response()
}

fn map_admission_error(error: crate::admission::AdmissionError) -> ComfyError {
    use crate::admission::AdmissionError;
    match error {
        AdmissionError::InvalidInput(message) => ComfyError::invalid("invalid_prompt", message),
        AdmissionError::PromptIdConflict => ComfyError::new(
            StatusCode::CONFLICT,
            "prompt_id_conflict",
            "prompt_id already exists",
        ),
        AdmissionError::CapacityExceeded => {
            ComfyError::new(StatusCode::TOO_MANY_REQUESTS, "queue_full", "queue is full")
        }
        AdmissionError::Unavailable => ComfyError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "model_not_available",
            "no capable Worker is online",
        ),
        AdmissionError::UnknownAlias => {
            ComfyError::invalid("invalid_prompt", "raw prompts do not use catalog aliases")
        }
        AdmissionError::ObjectStoreUnavailable => ComfyError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "internal_error",
            "object storage is not configured",
        ),
        AdmissionError::Internal(error) => {
            tracing::error!(%error, "Comfy prompt admission failed");
            ComfyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal server error",
            )
        }
    }
}

async fn get_prompt(State(state): State<AppState>, TenantAuth(tenant_id): TenantAuth) -> Response {
    let mut conn = match state.db.begin_tenant(tenant_id).await {
        Ok(conn) => conn,
        Err(error) => return internal_response(error),
    };
    let count = match crate::db::comfy::count_nonterminal(&mut conn, tenant_id).await {
        Ok(count) => count,
        Err(error) => return internal_response(error),
    };
    if let Err(error) = conn.commit().await {
        return internal_response(error);
    }
    axum::Json(serde_json::json!({"exec_info": {"queue_remaining": count}})).into_response()
}

fn parse_history_options(uri: &Uri) -> Result<(i64, i64), ComfyError> {
    let mut limit = DEFAULT_HISTORY_LIMIT;
    let mut offset = -1;
    for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "max_items" => {
                limit = value.parse().map_err(|_| {
                    ComfyError::invalid("invalid_request", "max_items must be an integer")
                })?;
            }
            "offset" => {
                offset = value.parse().map_err(|_| {
                    ComfyError::invalid("invalid_request", "offset must be an integer")
                })?;
            }
            _ => {
                return Err(ComfyError::invalid(
                    "invalid_request",
                    format!("unsupported history option '{key}'"),
                ));
            }
        }
    }
    if !(0..=MAX_HISTORY_LIMIT).contains(&limit) || offset < -1 {
        return Err(ComfyError::invalid(
            "invalid_request",
            "max_items must be 0..=200 and offset must be -1 or non-negative",
        ));
    }
    Ok((limit, offset))
}

async fn get_history(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
    uri: Uri,
) -> Response {
    let (limit, offset) = match parse_history_options(&uri) {
        Ok(value) => value,
        Err(error) => return error_response(error),
    };
    let mut conn = match state.db.begin_tenant(tenant_id).await {
        Ok(conn) => conn,
        Err(error) => return internal_response(error),
    };
    let total = match crate::db::comfy::count_terminal(&mut conn, tenant_id).await {
        Ok(total) => total,
        Err(error) => return internal_response(error),
    };
    let db_offset = if offset == -1 {
        total.saturating_sub(limit)
    } else {
        offset
    };
    let rows = match crate::db::comfy::list_history(&mut conn, tenant_id, limit, db_offset).await {
        Ok(rows) => rows,
        Err(error) => return internal_response(error),
    };
    if let Err(error) = conn.commit().await {
        return internal_response(error);
    }
    let mut response = BTreeMap::new();
    for row in rows {
        let prompt_id = row.prompt_id.to_string();
        match history_entry(&state, tenant_id, row).await {
            Ok(entry) => {
                response.insert(prompt_id, entry);
            }
            Err(error) => return internal_response(error),
        }
    }
    axum::Json(response).into_response()
}

async fn get_history_prompt(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
    Path(prompt_id): Path<String>,
) -> Response {
    let Ok(prompt_id) = canonical_uuid(&prompt_id) else {
        return axum::Json(BTreeMap::<String, Value>::new()).into_response();
    };
    let mut conn = match state.db.begin_tenant(tenant_id).await {
        Ok(conn) => conn,
        Err(error) => return internal_response(error),
    };
    let row = match crate::db::comfy::history_by_prompt_id(&mut conn, tenant_id, prompt_id).await {
        Ok(row) => row,
        Err(error) => return internal_response(error),
    };
    if let Err(error) = conn.commit().await {
        return internal_response(error);
    }
    let Some(row) = row else {
        return axum::Json(BTreeMap::<String, Value>::new()).into_response();
    };
    if !is_terminal(&row.state) {
        return axum::Json(BTreeMap::<String, Value>::new()).into_response();
    }
    let entry = match history_entry(&state, tenant_id, row).await {
        Ok(entry) => entry,
        Err(error) => return internal_response(error),
    };
    let mut response = BTreeMap::new();
    response.insert(prompt_id.to_string(), entry);
    axum::Json(response).into_response()
}

pub(crate) fn canonical_uuid(value: &str) -> Result<Uuid, ()> {
    let uuid = Uuid::parse_str(value).map_err(|_| ())?;
    (uuid.to_string() == value).then_some(uuid).ok_or(())
}

fn is_terminal(state: &str) -> bool {
    matches!(state, "succeeded" | "failed" | "cancelled" | "expired")
}

fn public_extra_data(parameters: &Value, client_id: Option<&str>) -> Value {
    let mut extra = parameters
        .get("extra_data")
        .cloned()
        .filter(Value::is_object)
        .unwrap_or_else(|| Value::Object(Map::new()));
    redact_extra_data(&mut extra);
    if let Some(client_id) = client_id
        && let Some(object) = extra.as_object_mut()
    {
        object.insert("client_id".to_owned(), Value::String(client_id.to_owned()));
    }
    extra
}

fn redact_extra_data(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("auth_token_comfy_org");
            object.remove("api_key_comfy_org");
            for value in object.values_mut() {
                redact_extra_data(value);
            }
        }
        Value::Array(values) => values.iter_mut().for_each(redact_extra_data),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

async fn history_entry(
    state: &AppState,
    tenant_id: gpq_domain::TenantId,
    row: crate::db::comfy::ComfyHistoryRow,
) -> anyhow::Result<Value> {
    let graph = row
        .parameters
        .get("prompt")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    let extra_data = public_extra_data(&row.parameters, row.client_id.as_deref());
    let prompt_id = row.prompt_id.to_string();
    let mut messages = load_messages(
        state,
        tenant_id,
        gpq_domain::GenerationId::from_uuid(row.generation_id),
        &prompt_id,
    )
    .await?;
    let success = row.state == "succeeded";
    if is_terminal(&row.state)
        && !messages.iter().any(|message| {
            message.get(0).and_then(Value::as_str).is_some_and(|kind| {
                matches!(
                    kind,
                    "execution_success" | "execution_error" | "execution_interrupted"
                )
            })
        })
    {
        let timestamp = row.terminated_at.map_or(0, |time| time.timestamp_millis());
        messages.push(terminal_message(
            &row.state,
            &prompt_id,
            timestamp,
            row.failure_kind.as_deref(),
            &row.failure_message,
        ));
    }
    Ok(serde_json::json!({
        "prompt": [
            row.number,
            row.client_id.unwrap_or_default(),
            graph,
            extra_data,
            row.output_node_ids
        ],
        "outputs": if success { row.outputs.unwrap_or_else(|| Value::Object(Map::new())) } else { Value::Object(Map::new()) },
        "status": {
            "status_str": if success { "success" } else { "error" },
            "completed": success,
            "messages": messages,
        }
    }))
}

async fn load_messages(
    state: &AppState,
    tenant_id: gpq_domain::TenantId,
    generation_id: gpq_domain::GenerationId,
    prompt_id: &str,
) -> anyhow::Result<Vec<Value>> {
    let mut conn = state.db.begin_tenant(tenant_id).await?;
    let events = crate::db::events::load_since(&mut conn, tenant_id, generation_id, 0).await?;
    conn.commit().await?;
    let mut messages = Vec::new();
    for row in events {
        let Some(event) = crate::events::decode_persisted(&row) else {
            continue;
        };
        match event {
            crate::events::GenerationEvent::State { state, failure, .. } => {
                if let Some(message) = state_message(
                    state,
                    prompt_id,
                    row.created_at.timestamp_millis(),
                    failure.as_ref(),
                ) {
                    messages.push(message);
                }
            }
            crate::events::GenerationEvent::Progress {
                stage,
                step,
                total_steps,
                ..
            } => {
                if !stage.is_empty() {
                    messages.push(
                        serde_json::json!(["executing", {"prompt_id": prompt_id, "node": stage}]),
                    );
                }
                if total_steps > 0 {
                    messages.push(serde_json::json!(["progress", {"prompt_id": prompt_id, "value": step, "max": total_steps}]));
                }
            }
            crate::events::GenerationEvent::Token { .. }
            | crate::events::GenerationEvent::Output
            | crate::events::GenerationEvent::Discontinuity { .. } => {}
        }
    }
    Ok(messages)
}

fn state_message(
    state: gpq_domain::GenerationState,
    prompt_id: &str,
    timestamp: i64,
    failure: Option<&(gpq_domain::FailureKind, String)>,
) -> Option<Value> {
    match state {
        gpq_domain::GenerationState::Running => Some(serde_json::json!([
            "execution_start",
            {"prompt_id": prompt_id, "timestamp": timestamp}
        ])),
        gpq_domain::GenerationState::Succeeded => Some(serde_json::json!([
            "execution_success",
            {"prompt_id": prompt_id, "timestamp": timestamp}
        ])),
        gpq_domain::GenerationState::Failed | gpq_domain::GenerationState::Expired => {
            Some(terminal_message(
                state.as_str(),
                prompt_id,
                timestamp,
                failure.map(|(kind, _)| kind.as_str()),
                failure.map_or("generation failed", |(_, message)| message),
            ))
        }
        gpq_domain::GenerationState::Cancelled => Some(serde_json::json!([
            "execution_interrupted",
            {"prompt_id": prompt_id}
        ])),
        gpq_domain::GenerationState::Queued | gpq_domain::GenerationState::Cancelling => None,
    }
}

fn terminal_message(
    state: &str,
    prompt_id: &str,
    timestamp: i64,
    failure_kind: Option<&str>,
    failure_message: &str,
) -> Value {
    if state == "succeeded" {
        serde_json::json!(["execution_success", {"prompt_id": prompt_id, "timestamp": timestamp}])
    } else if state == "cancelled" {
        serde_json::json!(["execution_interrupted", {"prompt_id": prompt_id}])
    } else {
        serde_json::json!(["execution_error", {
            "prompt_id": prompt_id,
            "exception_type": failure_kind.unwrap_or(if state == "expired" { "expired" } else { "internal" }),
            "exception_message": failure_message,
            "traceback": []
        }])
    }
}

fn requires_download(content_type: Option<&header::HeaderValue>) -> bool {
    let Some(content_type) = content_type else {
        return false;
    };
    let Ok(content_type) = content_type.to_str() else {
        return true;
    };
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "text/html"
            | "text/xml"
            | "text/javascript"
            | "text/ecmascript"
            | "application/xhtml+xml"
            | "application/xml"
            | "application/javascript"
            | "application/ecmascript"
            | "image/svg+xml"
    )
}

async fn view(
    State(state): State<AppState>,
    TenantAuth(tenant_id): TenantAuth,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if headers.contains_key(header::RANGE) {
        return error_response(ComfyError::invalid(
            "unsupported_option",
            "Range requests are not supported",
        ));
    }
    let mut filename = None;
    let mut subfolder = String::new();
    let mut kind = "output".to_owned();
    for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()) {
        match key.as_ref() {
            "filename" => filename = Some(value.into_owned()),
            "subfolder" => subfolder = value.into_owned(),
            "type" => kind = value.into_owned(),
            "preview" | "channel" => {
                return error_response(ComfyError::invalid(
                    "unsupported_option",
                    format!("{key} is not supported"),
                ));
            }
            _ => {}
        }
    }
    let Some(filename) = filename else {
        return error_response(ComfyError::invalid(
            "invalid_request",
            "filename is required",
        ));
    };
    if !subfolder.is_empty() || kind != "output" {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some((artifact_text, _basename)) = filename.split_once('_') else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(artifact_uuid) = canonical_uuid(artifact_text) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let artifact_id = gpq_domain::ArtifactId::from_uuid(artifact_uuid);
    let mut conn = match state.db.begin_tenant(tenant_id).await {
        Ok(conn) => conn,
        Err(error) => return internal_response(error),
    };
    let matches = match crate::db::comfy::artifact_view_matches(
        &mut conn,
        tenant_id,
        artifact_uuid,
        &filename,
        &subfolder,
        &kind,
    )
    .await
    {
        Ok(matches) => matches,
        Err(error) => return internal_response(error),
    };
    if let Err(error) = conn.commit().await {
        return internal_response(error);
    }
    if !matches {
        return StatusCode::NOT_FOUND.into_response();
    }
    let mut response = crate::artifacts::download_for_tenant(&state, tenant_id, artifact_id).await;
    let download_as_attachment = response.status().is_success()
        && requires_download(response.headers().get(header::CONTENT_TYPE));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        axum::http::HeaderValue::from_static("nosniff"),
    );
    if download_as_attachment {
        response.headers_mut().insert(
            header::CONTENT_DISPOSITION,
            axum::http::HeaderValue::from_static("attachment"),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use axum::http::header::HeaderValue;

    use super::requires_download;

    #[test]
    fn active_content_types_are_downloaded() {
        for content_type in [
            "text/html; charset=utf-8",
            "application/javascript",
            "image/svg+xml",
        ] {
            assert!(requires_download(Some(&HeaderValue::from_static(
                content_type
            ))));
        }
    }

    #[test]
    fn ordinary_media_types_remain_fetchable_inline() {
        for content_type in ["image/png", "video/mp4", "audio/flac", "text/plain"] {
            assert!(!requires_download(Some(&HeaderValue::from_static(
                content_type
            ))));
        }
    }
}
