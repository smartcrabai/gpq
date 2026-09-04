//! Adapter over a managed `mlx-dspark serve` process.
//!
//! mlx-dspark exposes the same `OpenAI` chat-completion and SSE surface as
//! llama-server, plus `GET /health` and `GET /metrics` for readiness and slot
//! discovery. Models must be configured as local paths so the Worker can pin
//! the complete MLX model directory by content hash (ADR 0012).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use gpq_domain::{BackendKind, ContentHash, FailureKind};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use super::llama::{
    OpenAiChatContext, build_chat_request, cancelled_error, run_openai_chat_completion,
    verify_resident_model,
};
use super::{
    Backend, BackendCapabilities, BackendError, ExecutionEvent, ExecutionRequest, endpoint,
    http_client, run_with_timeout,
};
use crate::config::PoolConfig;
use crate::models::{ModelSnapshot, hash_model_fresh_cancellable};

const BACKEND_NAME: &str = "mlx-dspark";
/// Bounds backend-reported/configured slots before capability serialization
/// allocates one wire entry per slot.
const MAX_SLOTS: u32 = 1024;
/// Bounds control-plane requests during readiness refreshes. Execution
/// requests use their leased deadline instead.
const CONTROL_PLANE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Adapter over one managed `mlx-dspark serve` process.
pub struct MlxDsparkBackend {
    client: reqwest::Client,
    base_url: url::Url,
    model_path: PathBuf,
    configured_slots: Option<u32>,
    resident: Mutex<Option<ResidentModel>>,
}

/// The Model Version bound to the running `mlx-dspark` process: its content
/// hash plus the directory fingerprint that hash was computed against.
struct ResidentModel {
    snapshot: ModelSnapshot,
    hash: ContentHash,
}

struct CancelHashOnDrop(CancellationToken);

impl Drop for CancelHashOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

#[derive(Deserialize)]
struct HealthResponse {
    status: String,
    mode: Option<String>,
    target: Option<String>,
}

#[derive(Deserialize)]
struct MetricsResponse {
    batching: Option<BatchingMetrics>,
}

#[derive(Deserialize)]
struct BatchingMetrics {
    max_batch: u32,
}

impl MlxDsparkBackend {
    /// Builds an adapter without spawning or probing the configured process.
    #[must_use]
    pub fn new(pool: &PoolConfig) -> Self {
        Self {
            client: http_client(),
            base_url: pool.base_url.clone(),
            model_path: pool.model_paths[0].clone(),
            configured_slots: pool.slots,
            resident: Mutex::new(None),
        }
    }

    async fn fetch_health(&self) -> Option<HealthResponse> {
        let response = self
            .client
            .get(endpoint(&self.base_url, "/health"))
            .timeout(CONTROL_PLANE_TIMEOUT)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json().await.ok()
    }

    async fn fetch_slots(&self) -> u32 {
        let reported = match self
            .client
            .get(endpoint(&self.base_url, "/metrics"))
            .timeout(CONTROL_PLANE_TIMEOUT)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response
                .json::<MetricsResponse>()
                .await
                .ok()
                .and_then(|metrics| metrics.batching)
                .map(|batching| batching.max_batch),
            _ => None,
        };
        bounded_slots(
            reported
                .or(self.configured_slots)
                .unwrap_or_else(|| BackendKind::MlxDspark.default_slots()),
        )
    }

    /// Sends an invalid, non-queuing completion request to verify that the
    /// generation route exists before advertising generation and streaming.
    async fn probe_generation(&self) -> bool {
        self.client
            .post(endpoint(&self.base_url, "/v1/chat/completions"))
            .timeout(CONTROL_PLANE_TIMEOUT)
            .json(&serde_json::json!({ "messages": [], "stream": true }))
            .send()
            .await
            .is_ok_and(|response| {
                response.status().is_success()
                    || matches!(
                        response.status(),
                        reqwest::StatusCode::BAD_REQUEST
                            | reqwest::StatusCode::UNPROCESSABLE_ENTITY
                    )
            })
    }

    /// Resolves the loaded target to its pinned Model Version hash. The
    /// directory is digested once per process; while its metadata fingerprint
    /// is unchanged the bound hash is reused, since rehashing a multi-gigabyte
    /// MLX checkpoint on every probe and Attempt would take longer than the
    /// lease TTL. A changed fingerprint is refused rather than rehashed
    /// against the still-running process (ADR 0012), matching llama.cpp.
    async fn resident_model_hash(
        &self,
        target: &str,
        cancel: &CancellationToken,
    ) -> Result<ContentHash, BackendError> {
        let path = self.model_path.clone();
        if !resolve_target(&path, target) {
            return Err(BackendError {
                kind: FailureKind::ModelUnavailable,
                message: format!(
                    "mlx-dspark reports target `{target}`, which is not a configured local model path"
                ),
                retry_hint: false,
            });
        }

        let snapshot = ModelSnapshot::read(&path).map_err(|err| BackendError {
            kind: FailureKind::ModelUnavailable,
            message: format!("reading metadata for model {}: {err}", path.display()),
            retry_hint: true,
        })?;
        let mut resident = self.resident.lock().await;
        match resident.as_ref() {
            Some(bound) if bound.snapshot == snapshot => return Ok(bound.hash),
            Some(_) => {
                return Err(BackendError {
                    kind: FailureKind::ModelUnavailable,
                    message: format!(
                        "model {} changed on disk after mlx-dspark loaded it",
                        path.display()
                    ),
                    retry_hint: false,
                });
            }
            None => {}
        }
        let hash_path = path.clone();
        let hash_cancel = CancellationToken::new();
        let task_cancel = hash_cancel.clone();
        let hash_task = tokio::task::spawn_blocking(move || {
            hash_model_fresh_cancellable(&hash_path, &task_cancel)
        });
        // `spawn_blocking` cannot be aborted once running. Cancel its token on
        // every exit path, including an outer timeout dropping this future, so
        // the task stops at its next bounded read or directory entry.
        let _cancel_on_drop = CancelHashOnDrop(hash_cancel.clone());
        let hash_result = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(cancelled_error()),
            result = hash_task => result,
        };
        let hash = hash_result
            .map_err(|err| BackendError {
                kind: FailureKind::Internal,
                message: format!("model hashing task panicked: {err}"),
                retry_hint: FailureKind::Internal.is_retryable(),
            })?
            .map_err(|err| {
                if cancel.is_cancelled() {
                    cancelled_error()
                } else {
                    BackendError {
                        kind: FailureKind::ModelUnavailable,
                        message: format!("hashing mlx-dspark model {}: {err}", path.display()),
                        retry_hint: true,
                    }
                }
            })?;

        *resident = Some(ResidentModel { snapshot, hash });
        Ok(hash)
    }

    async fn ready_model(
        &self,
        cancel: &CancellationToken,
    ) -> Result<Option<(HealthResponse, ContentHash)>, BackendError> {
        let Some(health) = self.fetch_health().await else {
            return Ok(None);
        };
        if health.status != "ok" {
            return Ok(None);
        }
        let Some(target) = health.target.as_deref() else {
            return Err(BackendError {
                kind: FailureKind::ModelUnavailable,
                message: "mlx-dspark /health omitted the loaded target".to_owned(),
                retry_hint: true,
            });
        };
        let hash = self.resident_model_hash(target, cancel).await?;
        Ok(Some((health, hash)))
    }

    async fn verify_pinned_model(
        &self,
        request: &ExecutionRequest,
        cancel: &CancellationToken,
    ) -> Result<(), BackendError> {
        let Some(expected) = request.model_sha256 else {
            return Ok(());
        };
        let loaded = self.ready_model(cancel).await?.map(|(_, hash)| hash);
        verify_resident_model(
            loaded,
            expected,
            request.model_path.as_deref(),
            BACKEND_NAME,
        )
    }
}

#[async_trait::async_trait]
impl Backend for MlxDsparkBackend {
    async fn probe(&self) -> Result<BackendCapabilities, BackendError> {
        let probe_cancel = CancellationToken::new();
        let Some((health, resident_model)) = self.ready_model(&probe_cancel).await? else {
            return Ok(BackendCapabilities {
                probes: BTreeMap::from([
                    ("generation".to_owned(), false),
                    ("streaming".to_owned(), false),
                    ("cancellation".to_owned(), false),
                ]),
                ..BackendCapabilities::default()
            });
        };
        let (generation_ok, slots) = tokio::join!(self.probe_generation(), self.fetch_slots());
        let version = health.mode.map_or_else(
            || BACKEND_NAME.to_owned(),
            |mode| format!("{BACKEND_NAME} ({mode})"),
        );

        Ok(BackendCapabilities {
            version,
            slots,
            resident_model: Some(resident_model),
            accelerator_memory_bytes: None,
            custom_nodes: BTreeMap::new(),
            probes: BTreeMap::from([
                ("generation".to_owned(), generation_ok),
                ("streaming".to_owned(), generation_ok),
                ("cancellation".to_owned(), generation_ok),
            ]),
        })
    }

    async fn execute(
        &self,
        request: ExecutionRequest,
        events: Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<(), BackendError> {
        let mut payload = build_chat_request(&request)?;
        // mlx-dspark only detects a disconnected client while writing its SSE
        // stream. Force streaming even for non-streaming queue requests so a
        // cancellation cannot leave the single MLX engine generating in the
        // background.
        if let serde_json::Value::Object(body) = &mut payload {
            body.insert("stream".to_owned(), serde_json::Value::Bool(true));
        }
        run_with_timeout(request.deadline, async {
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err(cancelled_error()),
                result = self.verify_pinned_model(&request, &cancel) => result,
            }?;
            run_openai_chat_completion(
                &self.client,
                endpoint(&self.base_url, "/v1/chat/completions"),
                payload,
                true,
                OpenAiChatContext {
                    events: &events,
                    cancel: &cancel,
                    attempt_id: &request.attempt_id,
                    backend_name: BACKEND_NAME,
                    emit_tokens: request.stream_tokens,
                },
            )
            .await
        })
        .await
    }
}

fn bounded_slots(slots: u32) -> u32 {
    slots.min(MAX_SLOTS)
}

fn resolve_target(model_path: &Path, target: &str) -> bool {
    let target = Path::new(target);
    target == model_path
        || target
            .canonicalize()
            .is_ok_and(|target| model_path.canonicalize().is_ok_and(|path| path == target))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use gpq_domain::Modality;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn fake_mlx_server(target: String) -> (url::Url, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|err| panic!("bind fake mlx server: {err}"));
        let address = listener
            .local_addr()
            .unwrap_or_else(|err| panic!("fake mlx server address: {err}"));
        let base_url = format!("http://{address}")
            .parse()
            .unwrap_or_else(|err| panic!("fake mlx server URL: {err}"));
        let task = tokio::spawn(async move {
            for _ in 0..5 {
                let (mut socket, _) = listener
                    .accept()
                    .await
                    .unwrap_or_else(|err| panic!("accept fake mlx request: {err}"));
                let mut request = vec![0_u8; 8192];
                let read = socket
                    .read(&mut request)
                    .await
                    .unwrap_or_else(|err| panic!("read fake mlx request: {err}"));
                let request = String::from_utf8_lossy(&request[..read]);
                let path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or_else(|| panic!("missing fake mlx request path"));
                let body = match path {
                    "/health" => json!({
                        "status": "ok",
                        "model": "model",
                        "mode": "dspark",
                        "target": target,
                    })
                    .to_string(),
                    "/metrics" => json!({"batching": {"max_batch": 2}}).to_string(),
                    "/v1/chat/completions" => format!(
                        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                        json!({
                            "choices": [{"delta": {"content": "hello"}}]
                        }),
                        json!({
                            "choices": [],
                            "usage": {
                                "prompt_tokens": 1,
                                "completion_tokens": 1,
                                "total_tokens": 2
                            }
                        })
                    ),
                    other => panic!("unexpected fake mlx route {other}"),
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .unwrap_or_else(|err| panic!("write fake mlx response: {err}"));
            }
        });
        (base_url, task)
    }

    fn test_pool(state_dir: &Path, model_path: PathBuf, base_url: url::Url) -> PoolConfig {
        PoolConfig {
            key: "mlx".to_owned(),
            backend: BackendKind::MlxDspark,
            executable: std::env::current_exe()
                .unwrap_or_else(|err| panic!("current test executable: {err}")),
            args: Vec::new(),
            env: BTreeMap::new(),
            state_dir: state_dir.to_owned(),
            startup_timeout: Duration::from_secs(1),
            base_url,
            slots: None,
            model_paths: vec![model_path],
            expected_hashes: BTreeMap::new(),
        }
    }

    #[test]
    fn reported_slots_are_bounded_before_advertisement() {
        assert_eq!(bounded_slots(u32::MAX), MAX_SLOTS);
        assert_eq!(bounded_slots(2), 2);
    }

    #[test]
    fn current_health_payload_decodes() {
        let health: HealthResponse = serde_json::from_value(serde_json::json!({
            "status": "ok",
            "model": "Qwen3-8B-8bit",
            "mode": "dspark",
            "target": "/models/Qwen3-8B-8bit",
            "warnings": []
        }))
        .unwrap_or_else(|err| panic!("health payload: {err}"));

        assert_eq!(health.status, "ok");
        assert_eq!(health.mode.as_deref(), Some("dspark"));
        assert_eq!(health.target.as_deref(), Some("/models/Qwen3-8B-8bit"));
    }

    #[test]
    fn target_must_name_a_configured_model_path() {
        let path = PathBuf::from("/models/Qwen3-8B-8bit");
        assert!(resolve_target(&path, "/models/Qwen3-8B-8bit"));
        assert!(!resolve_target(&path, "mlx-community/Qwen3-8B-8bit"));
    }

    /// The directory digest binds once; an unchanged fingerprint reuses it,
    /// and a member rewritten under the running process is refused instead
    /// of being rehashed (ADR 0012).
    #[tokio::test]
    async fn bound_model_hash_is_reused_until_the_directory_changes() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let model_path = root.path().join("model");
        std::fs::create_dir(&model_path)
            .unwrap_or_else(|err| panic!("create model directory: {err}"));
        std::fs::write(model_path.join("config.json"), b"{}")
            .unwrap_or_else(|err| panic!("write model config: {err}"));
        let state_dir = root.path().join("state");
        std::fs::create_dir(&state_dir).unwrap_or_else(|err| panic!("create state dir: {err}"));
        let unreachable = "http://127.0.0.1:9"
            .parse()
            .unwrap_or_else(|err| panic!("unreachable URL: {err}"));
        let backend =
            MlxDsparkBackend::new(&test_pool(&state_dir, model_path.clone(), unreachable));
        let target = model_path.to_string_lossy().into_owned();
        let cancel = CancellationToken::new();

        let first = backend
            .resident_model_hash(&target, &cancel)
            .await
            .unwrap_or_else(|err| panic!("first bind: {err}"));
        let again = backend
            .resident_model_hash(&target, &cancel)
            .await
            .unwrap_or_else(|err| panic!("rebind: {err}"));
        assert_eq!(first, again);

        std::fs::write(model_path.join("config.json"), b"{\"changed\": true}")
            .unwrap_or_else(|err| panic!("rewrite model config: {err}"));
        let Err(error) = backend.resident_model_hash(&target, &cancel).await else {
            panic!("a changed model directory must be refused");
        };
        assert_eq!(error.kind, FailureKind::ModelUnavailable);
        assert!(
            error.message.contains("changed on disk"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn probes_and_executes_against_mlx_dspark_api() {
        let root = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let model_path = root.path().join("model");
        std::fs::create_dir(&model_path)
            .unwrap_or_else(|err| panic!("create model directory: {err}"));
        std::fs::write(model_path.join("config.json"), b"{}")
            .unwrap_or_else(|err| panic!("write model config: {err}"));
        let state_dir = root.path().join("state");
        std::fs::create_dir(&state_dir).unwrap_or_else(|err| panic!("create state dir: {err}"));
        let (base_url, server) = fake_mlx_server(model_path.to_string_lossy().into_owned()).await;
        let backend = MlxDsparkBackend::new(&test_pool(&state_dir, model_path.clone(), base_url));

        let capabilities = backend
            .probe()
            .await
            .unwrap_or_else(|err| panic!("probe mlx-dspark: {err}"));
        assert_eq!(capabilities.slots, 2);
        let resident = capabilities
            .resident_model
            .unwrap_or_else(|| panic!("probe omitted resident model"));
        assert!(capabilities.probes.values().all(|passed| *passed));

        let request = ExecutionRequest {
            attempt_id: "attempt-mlx".to_owned(),
            modality: Modality::Llm,
            model_sha256: Some(resident),
            model_path: Some(model_path),
            workflow_graph: None,
            workflow_manifest: None,
            parameters: json!({"messages": [{"role": "user", "content": "hi"}]}),
            inputs: Vec::new(),
            seed: Some(7),
            stream_tokens: false,
            deadline: Duration::from_secs(1),
        };
        let (events, mut received) = tokio::sync::mpsc::channel(4);
        backend
            .execute(request, events, CancellationToken::new())
            .await
            .unwrap_or_else(|err| panic!("execute mlx-dspark request: {err}"));

        assert!(matches!(
            received.recv().await,
            Some(ExecutionEvent::Text { text }) if text == "hello"
        ));
        assert!(matches!(
            received.recv().await,
            Some(ExecutionEvent::Usage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 2,
            })
        ));
        server
            .await
            .unwrap_or_else(|err| panic!("fake mlx server task: {err}"));
    }
}
