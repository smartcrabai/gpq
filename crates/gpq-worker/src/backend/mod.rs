//! Backend adapter trait, shared execution types, and generic transport-error
//! normalization (ADR 0003, ADR 0005).
//!
//! Adapters use only managed subprocesses and loopback HTTP/WebSocket APIs
//! (ADR 0005); this module defines the boundary every adapter implements so
//! `pool.rs` and `executor.rs` never depend on `llama-server` or `ComfyUI`
//! specifics directly.

mod comfy;
mod llama;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use gpq_domain::{
    ArtifactManifest, BackendKind, ContentHash, FailureKind, Modality, WorkflowManifest,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::PoolConfig;

/// Everything a `Backend` needs to run one Attempt.
#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    /// The Attempt this execution belongs to, for correlation in adapter logs.
    pub attempt_id: String,
    /// The kind of result this Generation produces.
    pub modality: Modality,
    /// The pinned Model Version, when this Attempt uses a model (ADR 0012).
    pub model_sha256: Option<ContentHash>,
    /// Local path of the resolved Model Version, when applicable.
    pub model_path: Option<PathBuf>,
    /// Opaque `ComfyUI` graph, when this Attempt runs a Workflow (ADR 0007).
    pub workflow_graph: Option<serde_json::Value>,
    /// The pinned Workflow Version's manifest, when applicable (ADR 0012).
    pub workflow_manifest: Option<WorkflowManifest>,
    /// Opaque, backend-specific parameters (ADR 0007: no universal schema).
    pub parameters: serde_json::Value,
    /// Input Artifacts already staged on local disk.
    pub inputs: Vec<InputArtifact>,
    /// Caller-requested seed, when applicable.
    pub seed: Option<u64>,
    /// Whether the caller wants incremental token deltas (LLM only).
    pub stream_tokens: bool,
    /// The resolved execution timeout for this Attempt (ADR 0003).
    pub deadline: Duration,
}

/// One input Artifact staged on local disk before execution.
#[derive(Debug, Clone)]
pub struct InputArtifact {
    /// The Artifact's remote identifier.
    pub artifact_id: String,
    /// Local path the Artifact bytes were staged to.
    pub path: PathBuf,
    /// The Artifact's immutable manifest.
    pub manifest: ArtifactManifest,
}

/// One event emitted by a `Backend` while executing an Attempt.
#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    /// Fractional progress toward completion (`ComfyUI` graphs).
    Progress {
        fraction: f64,
        stage: String,
        step: u32,
        total_steps: u32,
    },
    /// One incremental token delta (LLM streaming).
    Token { text: String },
    /// One produced output Artifact, staged on local disk.
    Output {
        path: PathBuf,
        manifest: ArtifactManifest,
    },
    /// Complete non-streamed text output (LLM).
    Text { text: String },
    /// Token accounting for a completed LLM Attempt.
    Usage {
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
    },
}

/// A backend failure normalized to the closed [`FailureKind`] enum (ADR 0003).
///
/// Remote applies the authoritative retry policy from `kind`; `retry_hint` is
/// advisory context only, never a substitute for `FailureKind::is_retryable`.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct BackendError {
    /// The normalized cause.
    pub kind: FailureKind,
    /// Raw diagnostic text; never parsed by Remote for classification.
    pub message: String,
    /// The adapter's advisory opinion on whether a retry might succeed.
    pub retry_hint: bool,
}

/// Observed capabilities of a probed or running backend (ADR 0005).
#[derive(Debug, Clone, Default)]
pub struct BackendCapabilities {
    /// Backend-reported version string, advertised verbatim (ADR 0005: no
    /// version allowlist).
    pub version: String,
    /// Execution Slots this backend currently exposes.
    pub slots: u32,
    /// The Model Version currently loaded, if any.
    pub resident_model: Option<ContentHash>,
    /// Accelerator memory, when the backend reports it (optional, ADR 0005).
    pub accelerator_memory_bytes: Option<u64>,
    /// Installed `ComfyUI` custom-node package name to exact version.
    pub custom_nodes: BTreeMap<String, String>,
    /// Required-endpoint probe name to whether it succeeded (ADR 0005:
    /// compatibility comes from probes, not version allowlists).
    pub probes: BTreeMap<String, bool>,
}

/// A managed generation backend: llama.cpp or `ComfyUI`, reached only through
/// managed subprocesses and loopback HTTP/WebSocket APIs (ADR 0005).
#[async_trait]
pub trait Backend: Send + Sync {
    /// Runs the required-endpoint probes and reports capabilities.
    ///
    /// A missing core generation, streaming, progress, result, or
    /// cancellation operation makes the backend unready; the caller decides
    /// readiness from `probes`, this method only reports what it observed.
    async fn probe(&self) -> Result<BackendCapabilities, BackendError>;

    /// Runs one Attempt to completion, emitting `events` as it progresses.
    async fn execute(
        &self,
        request: ExecutionRequest,
        events: mpsc::Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<(), BackendError>;

    /// Asks the backend to release its resident model through its own API.
    ///
    /// `Ok(false)` means the backend has no such API or it failed: the
    /// caller must terminate the process instead, since process termination
    /// is the universal memory-release fallback (ADR 0005).
    async fn release_memory(&self) -> Result<bool, BackendError>;

    /// Which managed runtime this adapter drives.
    fn kind(&self) -> BackendKind;
}

/// Constructs the adapter matching `pool.backend` (ADR 0005: a Pool switches
/// exclusively between backend kinds).
#[must_use]
pub fn build(pool: &PoolConfig) -> Box<dyn Backend> {
    match pool.backend {
        BackendKind::LlamaCpp => Box::new(llama::LlamaBackend::new(pool)),
        BackendKind::ComfyUi => Box::new(comfy::ComfyBackend::new(pool)),
    }
}

/// Normalizes a `reqwest` transport failure talking to a loopback backend to
/// the closed [`FailureKind`] enum (ADR 0003).
///
/// A failed TCP connect to the backend's own loopback port means the managed
/// process is not accepting connections (crashed or not yet up); a request
/// timeout while talking to an already-connected backend is treated the same
/// as a transient Artifact transfer failure, since both are retryable and
/// carry no information about the Generation's validity.
#[must_use]
pub fn normalize_transport_error(error: &reqwest::Error) -> BackendError {
    classify_transport_error(
        error.is_timeout(),
        error.is_connect(),
        error.status().map(|status| status.is_server_error()),
        error.to_string(),
    )
}

fn classify_transport_error(
    is_timeout: bool,
    is_connect: bool,
    server_error: Option<bool>,
    message: String,
) -> BackendError {
    if is_timeout {
        return BackendError {
            kind: FailureKind::TransferFailed,
            message,
            retry_hint: true,
        };
    }
    if is_connect {
        return BackendError {
            kind: FailureKind::BackendCrashed,
            message,
            retry_hint: true,
        };
    }
    match server_error {
        Some(true) => BackendError {
            kind: FailureKind::BackendCrashed,
            message,
            retry_hint: true,
        },
        _ => BackendError {
            kind: FailureKind::Internal,
            message,
            retry_hint: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use gpq_domain::FailureKind;

    use super::classify_transport_error;

    #[test]
    fn timeout_normalizes_to_transfer_failed() {
        let error = classify_transport_error(true, false, None, "timed out".to_string());

        assert_eq!(error.kind, FailureKind::TransferFailed);
        assert!(error.retry_hint);
    }

    #[test]
    fn connect_failure_normalizes_to_backend_crashed() {
        let error = classify_transport_error(false, true, None, "connection refused".to_string());

        assert_eq!(error.kind, FailureKind::BackendCrashed);
        assert!(error.retry_hint);
    }

    #[test]
    fn server_error_status_normalizes_to_backend_crashed() {
        let error = classify_transport_error(false, false, Some(true), "500".to_string());

        assert_eq!(error.kind, FailureKind::BackendCrashed);
        assert!(error.retry_hint);
    }

    #[test]
    fn client_error_status_normalizes_to_internal() {
        let error = classify_transport_error(false, false, Some(false), "400".to_string());

        assert_eq!(error.kind, FailureKind::Internal);
        assert!(!error.retry_hint);
    }

    #[test]
    fn unclassified_failure_normalizes_to_internal() {
        let error = classify_transport_error(false, false, None, "builder error".to_string());

        assert_eq!(error.kind, FailureKind::Internal);
        assert!(!error.retry_hint);
    }

    #[test]
    fn timeout_takes_priority_over_connect_and_status() {
        let error = classify_transport_error(true, true, Some(true), "ambiguous".to_string());

        assert_eq!(error.kind, FailureKind::TransferFailed);
    }
}
