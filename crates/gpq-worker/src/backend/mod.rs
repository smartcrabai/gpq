//! Authenticated client for the managed native inference child.
//!
//! The child is always the `infer` role of this executable. It exposes only a
//! loopback HTTP control protocol protected by a per-process bearer token.

use std::time::Duration;

use async_trait::async_trait;

use gpq_domain::{ContentHash, FailureKind};
use gpq_inference::{InferenceRequest, NativeOutput};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::PoolConfig;

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionRequest {
    pub model_version: ContentHash,
    pub inference: InferenceRequest,
    #[serde(skip)]
    pub deadline: Duration,
}
#[derive(Serialize)]
struct NativeExecutionRequest<'a> {
    job_id: &'a str,
    model_version: ContentHash,
    inference: &'a InferenceRequest,
}

#[derive(Serialize)]
struct NativeCancelRequest<'a> {
    job_id: &'a str,
}

#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    Progress { fraction: f32 },
    Output { output: NativeOutput },
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct BackendError {
    pub kind: FailureKind,
    pub message: String,
    pub retry_hint: bool,
}

#[derive(Debug, Clone)]
pub struct BackendCapabilities {
    pub version: String,
    pub accelerator: String,
    pub accelerator_version: String,
    pub probes: std::collections::BTreeMap<String, bool>,
}

#[async_trait]
pub trait Backend: Send + Sync {
    async fn probe(&self) -> Result<BackendCapabilities, BackendError>;

    async fn execute(
        &self,
        request: ExecutionRequest,
        events: mpsc::Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<(), BackendError>;

    async fn release_memory(&self) -> Result<bool, BackendError>;
}

#[derive(Clone)]
pub struct NativeBackend {
    client: reqwest::Client,
    base_url: String,
    token: String,
    configured_backend: String,
}

#[derive(Debug, Deserialize)]
struct HealthResponse {
    ready: bool,
    backend: String,
    backend_version: String,
    devices: Vec<gpq_inference::BackendDevice>,
    detail: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReleaseResponse {
    released: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum NativeEvent {
    Progress { fraction: f32 },
    Output { output: NativeOutput },
    Complete,
    Cancelled,
    Error { kind: String, message: String },
}

impl NativeBackend {
    pub fn new(pool: &PoolConfig, token: String) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            client,
            base_url: format!("http://{}", pool.listen),
            token,
            configured_backend: pool.backend.clone(),
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
    }
}

#[async_trait]
impl Backend for NativeBackend {
    async fn probe(&self) -> Result<BackendCapabilities, BackendError> {
        let response = self
            .request(reqwest::Method::GET, "/health")
            .send()
            .await
            .map_err(|error| normalize_transport_error(&error))?
            .error_for_status()
            .map_err(|error| normalize_transport_error(&error))?;
        let health: HealthResponse = response.json().await.map_err(|error| BackendError {
            kind: FailureKind::BackendCrashed,
            message: format!("invalid native health response: {error}"),
            retry_hint: true,
        })?;
        let device = gpq_inference::resolve_device_id(&self.configured_backend, &health.devices)
            .ok()
            .and_then(|name| health.devices.iter().find(|device| device.name == name));
        let ready = health.ready && health.backend == self.configured_backend && device.is_some();
        let mut probes = std::collections::BTreeMap::new();
        probes.insert("native_inference".to_owned(), ready);
        if !ready {
            let detail = health.detail.unwrap_or_else(|| {
                device.map_or_else(
                    || {
                        format!(
                            "configured device {} is unavailable",
                            self.configured_backend
                        )
                    },
                    |device| format!("native runtime {} is not ready", device.name),
                )
            });
            tracing::warn!(backend = %self.configured_backend, detail, "native inference child is not ready");
        }
        let (accelerator, accelerator_version) = device.map_or_else(
            || (self.configured_backend.clone(), String::new()),
            |device| (device.name.clone(), device.description.clone()),
        );
        Ok(BackendCapabilities {
            version: health.backend_version,
            accelerator,
            accelerator_version,
            probes,
        })
    }

    async fn execute(
        &self,
        request: ExecutionRequest,
        events: mpsc::Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<(), BackendError> {
        let job_id = uuid::Uuid::now_v7().to_string();
        let body = NativeExecutionRequest {
            job_id: &job_id,
            model_version: request.model_version,
            inference: &request.inference,
        };
        let mut response = self
            .request(reqwest::Method::POST, "/execute")
            .json(&body)
            .send()
            .await
            .map_err(|error| normalize_transport_error(&error))?
            .error_for_status()
            .map_err(|error| normalize_transport_error(&error))?;
        let mut pending = Vec::new();
        let mut cancel_sent = false;
        let mut terminal = false;
        loop {
            tokio::select! {
                () = cancel.cancelled(), if !cancel_sent => {
                    cancel_sent = true;
                    let cancel_request = NativeCancelRequest { job_id: &job_id };
                    if let Err(error) = self
                        .request(reqwest::Method::POST, "/cancel")
                        .json(&cancel_request)
                        .send()
                        .await
                    {
                        tracing::warn!(error = %error, "native cancellation request failed");
                    }
                }
                chunk = response.chunk() => {
                    let Some(chunk) = chunk.map_err(|error| normalize_transport_error(&error))? else {
                        break;
                    };
                    pending.extend_from_slice(&chunk);
                    while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                        let line: Vec<u8> = pending.drain(..=newline).collect();
                        let line = std::str::from_utf8(&line)
                            .map_err(|error| protocol_error(format!("invalid SSE UTF-8: {error}")))?
                            .trim();
                        let Some(data) = line.strip_prefix("data:").map(str::trim) else {
                            continue;
                        };
                        let event: NativeEvent = serde_json::from_str(data)
                            .map_err(|error| protocol_error(format!("invalid native event: {error}")))?;
                        match event {
                            NativeEvent::Progress { fraction } => {
                                if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
                                    return Err(protocol_error("native progress fraction is out of range"));
                                }
                                let _ = events.send(ExecutionEvent::Progress { fraction }).await;
                            }
                            NativeEvent::Output { output } => {
                                if !output.path.is_absolute() {
                                    return Err(protocol_error("native output path is not absolute"));
                                }
                                let _ = events.send(ExecutionEvent::Output { output }).await;
                            }
                            NativeEvent::Complete => {
                                terminal = true;
                                break;
                            }
                            NativeEvent::Cancelled => {
                                return Err(BackendError {
                                    kind: FailureKind::Cancelled,
                                    message: "native inference acknowledged cancellation".to_owned(),
                                    retry_hint: false,
                                });
                            }
                            NativeEvent::Error { kind, message } => {
                                return Err(BackendError {
                                    kind: parse_failure_kind(&kind),
                                    message,
                                    retry_hint: false,
                                });
                            }
                        }
                    }
                    if terminal {
                        break;
                    }
                }
            }
        }
        if !terminal {
            return Err(BackendError {
                kind: FailureKind::BackendCrashed,
                message: "native event stream ended without a terminal event".to_owned(),
                retry_hint: true,
            });
        }
        let _ = request.deadline;
        Ok(())
    }

    async fn release_memory(&self) -> Result<bool, BackendError> {
        let response = self
            .request(reqwest::Method::POST, "/release")
            .send()
            .await
            .map_err(|error| normalize_transport_error(&error))?
            .error_for_status()
            .map_err(|error| normalize_transport_error(&error))?;
        let release: ReleaseResponse = response.json().await.map_err(|error| BackendError {
            kind: FailureKind::BackendCrashed,
            message: format!("invalid native release response: {error}"),
            retry_hint: true,
        })?;
        Ok(release.released)
    }
}

fn parse_failure_kind(value: &str) -> FailureKind {
    match value {
        "invalid_input" => FailureKind::InvalidInput,
        "model_unavailable" => FailureKind::ModelUnavailable,
        "unsupported_capability" => FailureKind::UnsupportedCapability,
        "execution_timed_out" => FailureKind::ExecutionTimedOut,
        "out_of_memory" => FailureKind::OutOfMemory,
        "backend_crashed" => FailureKind::BackendCrashed,
        "transfer_failed" => FailureKind::TransferFailed,
        "cancelled" => FailureKind::Cancelled,
        _ => FailureKind::Internal,
    }
}

fn normalize_transport_error(error: &reqwest::Error) -> BackendError {
    let kind = if error.is_timeout() {
        FailureKind::TransferFailed
    } else if error.is_connect() {
        FailureKind::BackendCrashed
    } else {
        FailureKind::TransferFailed
    };
    BackendError {
        kind,
        message: error.to_string(),
        retry_hint: kind.is_retryable(),
    }
}

fn protocol_error(message: impl Into<String>) -> BackendError {
    BackendError {
        kind: FailureKind::BackendCrashed,
        message: message.into(),
        retry_hint: true,
    }
}

pub fn build(pool: &PoolConfig, token: String) -> anyhow::Result<NativeBackend> {
    NativeBackend::new(pool, token)
}
