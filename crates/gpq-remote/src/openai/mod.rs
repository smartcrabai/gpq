//! OpenAI-compatible surface (ADR 0006).
//!
//! Four routes are served: `GET /v1/models`, `POST /v1/chat/completions`,
//! `POST /v1/responses`, and `POST /v1/images/generations`. Files and every
//! other `OpenAI` API are intentionally absent. Requests authenticate with the Tenant
//! Master Key as an `Authorization: Bearer` token and hold their connection
//! open while queued, cancelling their Generation on disconnect (ADR 0003,
//! ADR 0006). Multimodal image inputs are resolved here under SSRF-safe
//! network and size limits and relayed inline as `inline_relay` input
//! Artifacts (ADR 0008) rather than exposing Worker egress.

mod chat;
mod images;
mod models;
mod responses;
mod sse;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use axum::Router;
use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use gpq_domain::{
    ArtifactId, ArtifactManifest, ArtifactState, CallerKind, ContentHash, GenerationId, MediaKind,
    TenantId,
};
use ipnet::{Ipv4Net, Ipv6Net};
use percent_encoding::percent_decode;
use serde::Serialize;

use crate::admission::{AdmissionRequest, AliasTarget};
use crate::db::generations::GenerationRow;
use crate::events::GenerationEvent;
use crate::state::AppState;

/// Fetch timeout applied to one externally supplied image URL (ADR 0006).
/// Not Tenant-configurable; Tenant settings only cover queue age, capacity,
/// Artifact size, timeout ceilings, and priority.
pub const DEFAULT_IMAGE_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum redirect hops followed while fetching an externally supplied
/// image URL; each hop is re-checked against the same SSRF rules.
const MAX_IMAGE_REDIRECTS: usize = 5;

/// Hard cap on how many multimodal image inputs one request may reference.
/// Each accepted image costs up to [`DEFAULT_IMAGE_FETCH_TIMEOUT`] (plus
/// redirect hops) of connection-held handler time *before* admission's
/// `max_queued_generations` capacity check ever runs (ADR 0006), so an
/// unbounded count would let a small JSON body pin a request task for far
/// longer than any single fetch timeout.
const MAX_IMAGE_INPUTS_PER_REQUEST: usize = 8;

/// Hard ceiling on the total wall-clock time spent resolving every image
/// input of one request, however many images or redirect hops that takes;
/// bounds the aggregate independently of the per-fetch timeout (ADR 0006).
const MAX_IMAGE_RESOLUTION_TOTAL: Duration = Duration::from_secs(30);

/// Builds the Axum router serving the OpenAI-compatible surface.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/models", get(models::list_models))
        .route("/v1/chat/completions", post(chat::chat_completions))
        .route("/v1/images/generations", post(images::create_image))
        .route("/v1/responses", post(responses::create_response))
        .with_state(state)
}

// --------------------------------------------------------------------------
// Error envelope
// --------------------------------------------------------------------------

/// The `error` object of an OpenAI-shaped error response.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorDetail {
    /// Human-readable description.
    pub message: String,
    /// The `OpenAI` error category, e.g. `invalid_request_error`.
    #[serde(rename = "type")]
    pub kind: String,
    /// A stable machine-readable code.
    pub code: String,
}

/// The full `{"error": {...}}` `OpenAI` error envelope.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    /// The error detail.
    pub error: ErrorDetail,
}

/// An OpenAI-shaped HTTP error response.
#[derive(Debug, Clone)]
pub struct ApiError {
    status: StatusCode,
    body: ErrorBody,
}

impl ApiError {
    /// Builds an error with an explicit status, `OpenAI` error type, and code.
    #[must_use]
    pub fn new(status: StatusCode, kind: &str, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: ErrorBody {
                error: ErrorDetail {
                    message: message.into(),
                    kind: kind.to_owned(),
                    code: code.to_owned(),
                },
            },
        }
    }

    /// `401` for a missing or unrecognized Tenant Master Key.
    #[must_use]
    pub fn invalid_api_key() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "invalid_request_error",
            "invalid_api_key",
            "Incorrect API key provided.",
        )
    }

    /// `400` for a malformed or unsupported request body.
    #[must_use]
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid_request_error",
            message,
        )
    }

    /// `500` for a failure this surface cannot attribute to the caller.
    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "internal_error",
            message,
        )
    }

    /// Maps admission's outcome to the `OpenAI` error surface (ADR 0006: "if no
    /// capable Worker is online, `OpenAI` requests fail with `503
    /// model_not_available`").
    #[must_use]
    pub fn from_admission(error: crate::admission::AdmissionError) -> Self {
        use crate::admission::AdmissionError;
        match error {
            AdmissionError::Unavailable => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "invalid_request_error",
                "model_not_available",
                "No capable Worker is online for this model.",
            ),
            AdmissionError::UnknownAlias => Self::new(
                StatusCode::NOT_FOUND,
                "invalid_request_error",
                "model_not_found",
                "The model does not exist.",
            ),
            AdmissionError::InvalidInput(message) => Self::new(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "invalid_request_error",
                message,
            ),
            AdmissionError::CapacityExceeded => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "invalid_request_error",
                "rate_limit_exceeded",
                "Too many queued Generations for this Tenant.",
            ),
            AdmissionError::ObjectStoreUnavailable => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "invalid_request_error",
                "model_not_available",
                "This request requires object storage, which is not configured.",
            ),
            AdmissionError::Internal(err) => {
                tracing::error!(error = %err, "admission failed");
                Self::internal("Internal error.")
            }
        }
    }

    /// Extracts the `{"error": {...}}` envelope, discarding the HTTP status:
    /// lets a streaming Chat/Responses terminal handler reuse this error's
    /// shape as an in-band SSE error frame instead of an HTTP response (ADR
    /// 0006: a non-success terminal Generation must surface as an error an
    /// `OpenAI` SDK actually notices, not only through `finish_reason`).
    #[must_use]
    pub(crate) fn into_error_body(self) -> ErrorBody {
        self.body
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, axum::Json(self.body)).into_response()
    }
}

/// Maps a `FailureKind` to the status and machine code shared by the Chat and
/// Responses failure surfaces.
#[must_use]
pub(crate) fn failure_status_and_code(kind: gpq_domain::FailureKind) -> (StatusCode, &'static str) {
    use gpq_domain::FailureKind;
    match kind {
        FailureKind::InvalidInput => (StatusCode::BAD_REQUEST, "invalid_input"),
        FailureKind::UnsupportedCapability | FailureKind::ModelUnavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "model_not_available")
        }
        FailureKind::Cancelled => (StatusCode::CONFLICT, "cancelled"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
    }
}

/// Builds the HTTP error for a non-streaming request whose Generation ended
/// in a non-`Succeeded` terminal state.
pub(crate) fn terminal_failure_response(
    row: &GenerationRow,
    state: gpq_domain::GenerationState,
) -> ApiError {
    if let Some((kind, _)) = row.failure().ok().flatten() {
        let (status, code) = failure_status_and_code(kind);
        let message = if row.failure_message.is_empty() {
            kind.to_string()
        } else {
            row.failure_message.clone()
        };
        return ApiError::new(status, "server_error", code, message);
    }
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "server_error",
        state.as_str(),
        format!("generation ended in state {state}"),
    )
}

// --------------------------------------------------------------------------
// Tenant Master Key authentication
// --------------------------------------------------------------------------

/// The authenticated Tenant of an OpenAI-compatible request.
pub struct TenantAuth(pub TenantId);

impl FromRequestParts<AppState> for TenantAuth {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token =
            crate::auth::bearer_token(&parts.headers).ok_or_else(ApiError::invalid_api_key)?;
        let tenant_id = state
            .db
            .authenticate_master_key(token)
            .await
            .map_err(|err| {
                tracing::error!(error = %err, "master key lookup failed");
                ApiError::internal("Internal error.")
            })?
            .ok_or_else(ApiError::invalid_api_key)?;
        Ok(Self(tenant_id))
    }
}

// --------------------------------------------------------------------------
// Shared request validation and DB helpers
// --------------------------------------------------------------------------

/// `OpenAI`'s `n` parameter is accepted only when it requests exactly one
/// completion.
pub(crate) fn validate_n(n: Option<u32>) -> Result<(), ApiError> {
    match n {
        None | Some(1) => Ok(()),
        Some(_) => Err(ApiError::invalid_request(
            "n must be 1; multiple completions are not supported",
        )),
    }
}

/// Loads the authenticated Tenant's mutable settings (ADR 0006).
pub(crate) async fn tenant_settings(
    state: &AppState,
    tenant_id: TenantId,
) -> Result<gpq_domain::TenantSettings, ApiError> {
    let mut tx = state.db.begin_tenant(tenant_id).await.map_err(|err| {
        tracing::error!(error = %err, "failed to begin tenant transaction");
        ApiError::internal("Internal error.")
    })?;
    crate::db::tenants::load_settings(&mut tx, tenant_id)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to load tenant settings");
            ApiError::internal("Internal error.")
        })
}

/// Re-reads a Generation row, used after a terminal event since the event
/// itself carries no output text or usage.
pub(crate) async fn fetch_generation_row(
    state: &AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
) -> Result<GenerationRow, ApiError> {
    let mut tx = state.db.begin_tenant(tenant_id).await.map_err(|err| {
        tracing::error!(error = %err, "failed to begin tenant transaction");
        ApiError::internal("Internal error.")
    })?;
    let row = crate::db::generations::get(&mut tx, tenant_id, generation_id)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to fetch generation");
            ApiError::internal("Internal error.")
        })?;
    row.ok_or_else(|| ApiError::internal("generation row disappeared after admission"))
}

/// Blocks until a Generation reaches a terminal state, tolerating broadcast
/// lag by falling back to a direct row read (ADR 0006: "discontinuity is
/// explicit").
pub(crate) async fn await_terminal_generation(
    state: &AppState,
    tenant_id: TenantId,
    generation_id: GenerationId,
    mut rx: tokio::sync::broadcast::Receiver<GenerationEvent>,
) -> Result<GenerationRow, ApiError> {
    loop {
        match rx.recv().await {
            Ok(GenerationEvent::State {
                state: generation_state,
                ..
            }) if generation_state.is_terminal() => {
                return fetch_generation_row(state, tenant_id, generation_id).await;
            }
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                let row = fetch_generation_row(state, tenant_id, generation_id).await?;
                if row.state().is_ok_and(|state| state.is_terminal()) {
                    return Ok(row);
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return fetch_generation_row(state, tenant_id, generation_id).await;
            }
        }
    }
}

/// Builds the `AdmissionRequest` shared by the Chat Completions and
/// Responses synchronous caller paths (ADR 0006, ADR 0007): both admit
/// against a Model alias, forward the raw request body verbatim as
/// `parameters`, and hold their connection open for the whole Generation.
pub(crate) fn build_admission_request(
    model: &str,
    parameters: serde_json::Value,
    input_artifact_ids: Vec<ArtifactId>,
    seed: Option<u64>,
    stream_tokens: bool,
) -> AdmissionRequest {
    AdmissionRequest {
        alias_target: AliasTarget::Model(model.to_owned()),
        parameters,
        input_artifact_ids,
        output_placement: gpq_domain::ArtifactPlacement::WorkerLocal,
        priority: None,
        seed,
        execution_timeout: None,
        caller_kind: CallerKind::Synchronous,
        stream_tokens,
        idempotency_key: None,
    }
}

// --------------------------------------------------------------------------
// Multimodal image resolution (ADR 0006, ADR 0008)
// --------------------------------------------------------------------------

/// Bytes and MIME type resolved from one multimodal image reference.
pub(crate) struct ResolvedImage {
    pub bytes: Vec<u8>,
    pub mime_type: String,
}

fn media_kind_from_mime(mime: &str) -> MediaKind {
    let essence = mime.split(';').next().unwrap_or(mime).trim();
    if essence.starts_with("image/") {
        MediaKind::Image
    } else if essence.starts_with("video/") {
        MediaKind::Video
    } else if essence.starts_with("audio/") {
        MediaKind::Audio
    } else if essence.starts_with("text/") {
        MediaKind::Text
    } else {
        MediaKind::Binary
    }
}

/// Whether an IP address is safe to connect to while resolving an externally
/// supplied image URL (ADR 0006). Rejects loopback, link-local, private,
/// carrier-grade NAT (100.64.0.0/10), documentation, and IPv6 unique-local
/// ranges (including IPv4-mapped IPv6 addresses in those ranges); only
/// globally routable addresses are accepted.
#[must_use]
pub(crate) fn is_publicly_routable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_publicly_routable_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(mapped) => is_publicly_routable_v4(mapped),
            None => is_publicly_routable_v6(v6),
        },
    }
}

fn is_publicly_routable_v4(ip: Ipv4Addr) -> bool {
    if ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
    {
        return false;
    }
    // Carrier-grade NAT, RFC 6598 (not covered by std's Ipv4Addr helpers).
    !matches!(Ipv4Net::new(Ipv4Addr::new(100, 64, 0, 0), 10), Ok(net) if net.contains(&ip))
}

fn is_publicly_routable_v6(ip: Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
        return false;
    }
    let link_local = matches!(Ipv6Net::new(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10), Ok(net) if net.contains(&ip));
    let unique_local = matches!(Ipv6Net::new(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7), Ok(net) if net.contains(&ip));
    !link_local && !unique_local
}

/// Parses a `data:` URL, decoding a base64 or percent-encoded payload and
/// enforcing `max_bytes`.
pub(crate) fn parse_data_url(raw: &str, max_bytes: u64) -> Result<ResolvedImage, ApiError> {
    let rest = raw
        .strip_prefix("data:")
        .ok_or_else(|| ApiError::invalid_request("not a data url"))?;
    let comma = rest
        .find(',')
        .ok_or_else(|| ApiError::invalid_request("malformed data url: missing comma"))?;
    let (meta, rest) = rest.split_at(comma);
    let payload = &rest[1..];
    let is_base64 = meta.ends_with(";base64");
    let mime_type = meta.strip_suffix(";base64").unwrap_or(meta);
    let mime_type = if mime_type.is_empty() {
        "text/plain;charset=US-ASCII"
    } else {
        mime_type
    }
    .to_owned();
    let bytes = if is_base64 {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(payload)
            .map_err(|_| ApiError::invalid_request("invalid base64 data url payload"))?
    } else {
        percent_decode(payload.as_bytes()).collect()
    };
    if bytes.len() as u64 > max_bytes {
        return Err(ApiError::invalid_request(
            "data url exceeds the tenant's max input artifact size",
        ));
    }
    Ok(ResolvedImage { bytes, mime_type })
}

/// Resolves `host` via DNS (or parses it directly if it is already a
/// literal IP) and returns every socket address it resolved to at `port`,
/// rejecting the host outright unless *every* resolved address is publicly
/// routable (ADR 0006). A hostname that resolves to a mix of public and
/// private addresses is exactly as unsafe as one that resolves to only a
/// private address, since a later connection attempt could pick either.
async fn resolve_safe_addrs(host: &str, port: u16) -> Result<Vec<SocketAddr>, ApiError> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| ApiError::invalid_request("image url host could not be resolved"))?
        .collect();
    if addrs.is_empty() {
        return Err(ApiError::invalid_request(
            "image url host resolved to no addresses",
        ));
    }
    if addrs.iter().any(|addr| !is_publicly_routable(addr.ip())) {
        return Err(ApiError::invalid_request(
            "image url host is not publicly routable",
        ));
    }
    Ok(addrs)
}

/// Fetches an externally supplied image URL under SSRF-safe network limits
/// (ADR 0006). Every hop (the initial request and each redirect) is resolved
/// and validated by [`resolve_safe_addrs`] and the connection is pinned to
/// exactly the validated addresses via [`reqwest::ClientBuilder::resolve_to_addrs`],
/// so a DNS answer that changes between validation and connection (a DNS
/// rebind) cannot slip an unvalidated address through. Redirects are
/// followed manually — `reqwest::redirect::Policy::custom`'s closure is
/// synchronous and cannot await a DNS lookup — bounded by
/// [`MAX_IMAGE_REDIRECTS`] and the same `http`/`https` scheme allowlist as
/// before. The whole hop chain, including every DNS lookup, is bounded by
/// `fetch_timeout`.
async fn fetch_http_image(
    url: &url::Url,
    max_bytes: u64,
    fetch_timeout: Duration,
) -> Result<ResolvedImage, ApiError> {
    let deadline = std::time::Instant::now() + fetch_timeout;
    let mut current = url.clone();
    let mut redirects = 0usize;
    loop {
        if current.scheme() != "http" && current.scheme() != "https" {
            return Err(ApiError::invalid_request(format!(
                "unsupported image url scheme {:?}",
                current.scheme()
            )));
        }
        let host = current
            .host_str()
            .ok_or_else(|| ApiError::invalid_request("image url is missing a host"))?
            .to_owned();
        let port = current
            .port_or_known_default()
            .ok_or_else(|| ApiError::invalid_request("image url has no known port"))?;
        let safe_addrs = resolve_safe_addrs(&host, port).await?;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(ApiError::invalid_request("image fetch timed out"));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .resolve_to_addrs(&host, &safe_addrs)
            .timeout(remaining)
            .build()
            .map_err(|err| {
                tracing::error!(error = %err, "failed to build image fetch client");
                ApiError::internal("Internal error.")
            })?;
        let response = client
            .get(current.clone())
            .send()
            .await
            .map_err(|_| ApiError::invalid_request("failed to fetch image url"))?;
        if response.status().is_redirection() {
            redirects += 1;
            if redirects > MAX_IMAGE_REDIRECTS {
                return Err(ApiError::invalid_request("too many redirects"));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    ApiError::invalid_request("redirect response missing Location header")
                })?;
            current = current
                .join(location)
                .map_err(|_| ApiError::invalid_request("redirect target is not a valid URL"))?;
            continue;
        }
        if !response.status().is_success() {
            return Err(ApiError::invalid_request(format!(
                "image url returned HTTP {}",
                response.status()
            )));
        }
        if let Some(len) = response.content_length()
            && len > max_bytes
        {
            return Err(ApiError::invalid_request(
                "image exceeds the tenant's max input artifact size",
            ));
        }
        let mime_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let mut bytes = Vec::new();
        {
            use futures::StreamExt as _;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|_| ApiError::invalid_request("image download failed"))?;
                if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
                    return Err(ApiError::invalid_request(
                        "image exceeds the tenant's max input artifact size",
                    ));
                }
                bytes.extend_from_slice(&chunk);
            }
        }
        return Ok(ResolvedImage { bytes, mime_type });
    }
}

/// Resolves one multimodal image reference (`http(s)://` or `data:` URL)
/// into its raw bytes, applying SSRF-safe network limits and the Tenant's
/// `max_input_artifact_bytes` cap (ADR 0006, ADR 0008).
pub(crate) async fn resolve_image_input(
    raw_url: &str,
    max_bytes: u64,
    fetch_timeout: Duration,
) -> Result<ResolvedImage, ApiError> {
    if raw_url.starts_with("data:") {
        return parse_data_url(raw_url, max_bytes);
    }
    let url = url::Url::parse(raw_url)
        .map_err(|_| ApiError::invalid_request("image url is not a valid URL"))?;
    match url.scheme() {
        "http" | "https" => fetch_http_image(&url, max_bytes, fetch_timeout).await,
        other => Err(ApiError::invalid_request(format!(
            "unsupported image url scheme {other:?}"
        ))),
    }
}

/// Persists resolved image bytes as an `inline_relay` input Artifact
/// (ADR 0008) and buffers the bytes for the Worker's `FetchArtifact` RPC.
pub(crate) async fn store_inline_input_artifact(
    state: &AppState,
    tenant_id: TenantId,
    image: ResolvedImage,
) -> Result<ArtifactId, ApiError> {
    let ResolvedImage { bytes, mime_type } = image;
    let manifest = ArtifactManifest {
        size_bytes: bytes.len() as u64,
        digest: ContentHash::digest(&bytes),
        kind: media_kind_from_mime(&mime_type),
        mime_type,
    };
    let mut tx = state.db.begin_tenant(tenant_id).await.map_err(|err| {
        tracing::error!(error = %err, "failed to begin tenant transaction for inline artifact");
        ApiError::internal("Internal error.")
    })?;
    let row = crate::db::artifacts::create_inline_input(&mut tx, tenant_id, &manifest)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, "failed to create inline input artifact");
            ApiError::internal("Internal error.")
        })?;
    tx.commit().await.map_err(|err| {
        tracing::error!(error = %err, "failed to commit inline artifact transaction");
        ApiError::internal("Internal error.")
    })?;
    let artifact_id = row.id;
    state
        .artifacts
        .put_local(artifact_id, 0, &bytes)
        .map_err(|err| {
            tracing::error!(error = %err, "failed to buffer inline artifact bytes");
            ApiError::internal("Internal error.")
        })?;
    Ok(artifact_id)
}

/// RAII guard releasing input Artifacts created for a synchronous `OpenAI`
/// request if the caller drops it before `admission::admit` links them to a
/// Generation (ADR 0006, ADR 0008): `admit` only links input Artifacts to a
/// Generation after inserting its row, so any failure between resolving an
/// image and a successful `admit` call — most commonly a request naming a
/// nonexistent Model alias — would otherwise leak the orphaned Artifact row
/// and its buffered bytes forever. Also used internally by
/// [`resolve_and_store_images`] to roll back the Artifacts it already
/// created when a later image in the same request fails to resolve.
pub(crate) struct InputArtifactGuard {
    state: AppState,
    tenant_id: TenantId,
    artifact_ids: Vec<ArtifactId>,
}

impl InputArtifactGuard {
    /// Starts tracking no Artifacts; call [`Self::push`] or [`Self::extend`]
    /// as they are created.
    #[must_use]
    pub(crate) fn new(state: AppState, tenant_id: TenantId) -> Self {
        Self {
            state,
            tenant_id,
            artifact_ids: Vec::new(),
        }
    }

    /// Tracks one more Artifact for release if the guard is dropped armed.
    pub(crate) fn push(&mut self, artifact_id: ArtifactId) {
        self.artifact_ids.push(artifact_id);
    }

    /// Tracks every Artifact in `ids` for release if the guard is dropped
    /// armed.
    pub(crate) fn extend(&mut self, ids: impl IntoIterator<Item = ArtifactId>) {
        self.artifact_ids.extend(ids);
    }

    /// The tracked Artifacts were (or are about to be) linked to a
    /// Generation, so `Drop` must not release them. Returns the tracked ids
    /// so a caller that built the list incrementally (like
    /// [`resolve_and_store_images`]) can hand them onward.
    pub(crate) fn disarm(mut self) -> Vec<ArtifactId> {
        std::mem::take(&mut self.artifact_ids)
    }
}

impl Drop for InputArtifactGuard {
    fn drop(&mut self) {
        if self.artifact_ids.is_empty() {
            return;
        }
        let state = self.state.clone();
        let tenant_id = self.tenant_id;
        let artifact_ids = std::mem::take(&mut self.artifact_ids);
        tokio::spawn(async move {
            for artifact_id in artifact_ids {
                state.artifacts.discard_local(artifact_id);
                let Ok(mut tx) = state.db.begin_tenant(tenant_id).await else {
                    tracing::warn!(%artifact_id, "input artifact cleanup: failed to begin tenant transaction");
                    continue;
                };
                if let Err(err) = crate::db::artifacts::set_state(
                    &mut tx,
                    tenant_id,
                    artifact_id,
                    ArtifactState::Expired,
                )
                .await
                {
                    tracing::warn!(%artifact_id, error = %err, "input artifact cleanup: failed to expire an orphaned input artifact");
                    continue;
                }
                if let Err(err) = tx.commit().await {
                    tracing::warn!(%artifact_id, error = %err, "input artifact cleanup: commit failed");
                }
            }
        });
    }
}

/// Resolves and stores every image reference in `urls` as an `inline_relay`
/// input Artifact (ADR 0006, ADR 0008), shared by the Chat Completions and
/// Responses multimodal input paths. Rejects a request naming more than
/// [`MAX_IMAGE_INPUTS_PER_REQUEST`] images outright, and bounds the total
/// time spent resolving every image (including redirects) by
/// [`MAX_IMAGE_RESOLUTION_TOTAL`] regardless of the per-image fetch timeout,
/// so a small JSON body cannot pin a request task open indefinitely (ADR
/// 0006). Any Artifact already created before a later image fails is rolled
/// back before returning the error (ADR 0008).
pub(crate) async fn resolve_and_store_images<'a>(
    state: &AppState,
    tenant_id: TenantId,
    urls: impl Iterator<Item = &'a str>,
    max_input_artifact_bytes: u64,
) -> Result<Vec<ArtifactId>, ApiError> {
    let urls: Vec<&str> = urls.collect();
    if urls.len() > MAX_IMAGE_INPUTS_PER_REQUEST {
        return Err(ApiError::invalid_request(format!(
            "at most {MAX_IMAGE_INPUTS_PER_REQUEST} image inputs are accepted per request"
        )));
    }
    let deadline = std::time::Instant::now() + MAX_IMAGE_RESOLUTION_TOTAL;
    let mut guard = InputArtifactGuard::new(state.clone(), tenant_id);
    for url in urls {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(ApiError::invalid_request(
                "resolving this request's image inputs took too long",
            ));
        }
        let fetch_timeout = DEFAULT_IMAGE_FETCH_TIMEOUT.min(remaining);
        let resolved = resolve_image_input(url, max_input_artifact_bytes, fetch_timeout).await?;
        let artifact_id = store_inline_input_artifact(state, tenant_id, resolved).await?;
        guard.push(artifact_id);
    }
    Ok(guard.disarm())
}

/// Generic execution usage counters shared by the Chat and Responses shapes
/// (ADR 0007: usage is part of the unified Generation envelope).
#[derive(Debug, Clone, Copy, Default, Serialize, serde::Deserialize)]
pub struct UsageDto {
    /// Tokens consumed by the prompt/input.
    pub prompt_tokens: u32,
    /// Tokens produced by the model.
    pub completion_tokens: u32,
    /// `prompt_tokens + completion_tokens`.
    pub total_tokens: u32,
}

/// Deserializes a Generation row's stored usage JSON, defaulting to zero
/// counters when it is absent or unrecognized.
#[must_use]
pub(crate) fn usage_from_row(row: &GenerationRow) -> UsageDto {
    row.usage
        .as_ref()
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_ipv4_is_routable() {
        assert!(is_publicly_routable(IpAddr::V4(Ipv4Addr::new(
            93, 184, 216, 34
        ))));
    }

    #[test]
    fn private_ipv4_is_rejected() {
        assert!(!is_publicly_routable(IpAddr::V4(Ipv4Addr::new(
            10, 0, 0, 1
        ))));
        assert!(!is_publicly_routable(IpAddr::V4(Ipv4Addr::new(
            192, 168, 1, 1
        ))));
        assert!(!is_publicly_routable(IpAddr::V4(Ipv4Addr::new(
            172, 16, 0, 1
        ))));
    }

    #[test]
    fn loopback_is_rejected() {
        assert!(!is_publicly_routable(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(!is_publicly_routable(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn link_local_is_rejected() {
        assert!(!is_publicly_routable(IpAddr::V4(Ipv4Addr::new(
            169, 254, 1, 1
        ))));
        assert!(!is_publicly_routable(IpAddr::V6(Ipv6Addr::new(
            0xfe80, 0, 0, 0, 0, 0, 0, 1
        ))));
    }

    #[test]
    fn cgnat_is_rejected() {
        assert!(!is_publicly_routable(IpAddr::V4(Ipv4Addr::new(
            100, 64, 0, 1
        ))));
        assert!(is_publicly_routable(IpAddr::V4(Ipv4Addr::new(
            100, 63, 255, 255
        ))));
    }

    #[test]
    fn ipv6_mapped_private_is_rejected() {
        let mapped = Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0a00, 0x0001); // ::ffff:10.0.0.1
        assert!(!is_publicly_routable(IpAddr::V6(mapped)));
    }

    #[test]
    fn ipv6_unique_local_is_rejected() {
        assert!(!is_publicly_routable(IpAddr::V6(Ipv6Addr::new(
            0xfc00, 0, 0, 0, 0, 0, 0, 1
        ))));
    }

    #[test]
    fn data_url_decodes_base64() {
        let Ok(image) = parse_data_url("data:image/png;base64,aGVsbG8=", 1024) else {
            panic!("expected valid data url");
        };
        assert_eq!(image.bytes, b"hello");
        assert_eq!(image.mime_type, "image/png");
    }

    #[test]
    fn data_url_decodes_percent_encoded_text() {
        let Ok(image) = parse_data_url("data:,hello%20world", 1024) else {
            panic!("expected valid data url");
        };
        assert_eq!(image.bytes, b"hello world");
    }

    #[test]
    fn data_url_enforces_size_cap() {
        let Err(err) = parse_data_url("data:image/png;base64,aGVsbG8=", 2) else {
            panic!("expected data url to exceed size cap");
        };
        drop(err);
    }

    #[test]
    fn data_url_rejects_missing_comma() {
        assert!(parse_data_url("data:image/png;base64", 1024).is_err());
    }

    #[tokio::test]
    async fn hostname_resolving_to_loopback_is_rejected() {
        // `localhost` resolves via the system resolver without any network
        // access, and always resolves to a loopback address — the case
        // `check_url_host_is_safe`'s `host.parse::<IpAddr>()` used to miss
        // entirely, since a hostname is never itself a valid `IpAddr`.
        assert!(resolve_safe_addrs("localhost", 80).await.is_err());
    }

    #[tokio::test]
    async fn ip_literal_host_public_address_is_accepted() {
        let Ok(addrs) = resolve_safe_addrs("93.184.216.34", 443).await else {
            panic!("expected a public IP literal host to resolve safely");
        };
        assert_eq!(addrs, vec![SocketAddr::from(([93, 184, 216, 34], 443))]);
    }

    #[tokio::test]
    async fn ip_literal_host_private_address_is_rejected() {
        assert!(resolve_safe_addrs("127.0.0.1", 80).await.is_err());
        assert!(resolve_safe_addrs("169.254.169.254", 80).await.is_err());
    }
}
