//! `ComfyUI` backend adapter (ADR 0005, ADR 0007, ADR 0008, ADR 0012, ADR 0018).
//!
//! [`ComfyBackend`] talks to one operator-managed `ComfyUI` process reached only
//! over loopback HTTP and WebSocket (ADR 0005: "Adapters use only managed
//! subprocesses and loopback backend HTTP/SSE or `ComfyUI` HTTP/WebSocket
//! APIs, never C/C++ FFI or Python imports."). It never installs, downloads,
//! or otherwise manages custom nodes: those are entirely an operator
//! responsibility. Registered Workflow graphs that name an absent node or
//! model are rejected by capability checks or Worker revalidation, while raw
//! prompts are unregistered and may fail backend validation during their
//! Attempt (ADR 0007, ADR 0018).
//!
//! # Parameter and seed injection contract (ADR 0007)
//!
//! `ComfyUI` API-format workflow graphs and their parameters are opaque backend
//! payloads; there is no universal modality schema. This adapter defines one
//! narrow, self-contained convention for how the Generation envelope's
//! opaque `parameters` document customizes a graph:
//!
//! - The reserved key `"$seed"`, when present, MUST contain a
//!   `"<node_id>.<input_name>"` pointer. When the Generation has a shared seed,
//!   the adapter writes it to that input.
//! - Every other key beginning with `$` replaces every equal string placeholder
//!   in the graph with its value. This lets portable API fields such as
//!   `$prompt`, `$width`, and `$height` bind to an operator-defined workflow
//!   without guessing node ids. A missing placeholder is `InvalidInput`.
//! - Every remaining key MUST be a string of the form
//!   `"<node_id>.<input_name>"` and is applied as
//!   `graph[node_id]["inputs"][input_name] = value`. A `node_id` absent from
//!   the graph is an `InvalidInput` failure, never a silent no-op.
//! - Input Artifacts are wired into the graph by placeholder substitution:
//!   after every `request.inputs` entry is uploaded via `POST /upload/image`,
//!   every JSON string in the graph equal to that Artifact's `artifact_id` is
//!   replaced with the ComfyUI-relative filename `ComfyUI` assigned it (e.g. a
//!   `LoadImage` node's `image` input holds the Artifact id as a placeholder
//!   until this substitution runs).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use gpq_domain::hash::Hasher;
use gpq_domain::{
    ArtifactManifest, BackendKind, ContentHash, FailureKind, MediaKind, Modality, WorkflowManifest,
};
use reqwest::StatusCode;
use reqwest::multipart::{Form, Part};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use url::Url;
use uuid::Uuid;

use crate::backend::{
    Backend, BackendCapabilities, BackendError, ExecutionEvent, ExecutionRequest, InputArtifact,
    endpoint, http_client,
};
use crate::config::PoolConfig;
use crate::models::hash_model_fresh;

/// Per-request HTTP timeout for control-plane calls (submit, interrupt,
/// history, probes). Large `/view` downloads use the same client but rely on
/// the Attempt's own deadline to bound total wall time.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a cooperative cancellation or timeout waits for `ComfyUI` to
/// acknowledge `POST /interrupt` with a terminal WebSocket event before this
/// adapter gives up and reports the failure anyway (ADR 0003: Workers "must
/// cooperatively cancel", not wait forever).
const INTERRUPT_GRACE: Duration = Duration::from_secs(30);

/// How long the execution WebSocket may go without receiving any frame —
/// data, ping, or pong — before this adapter treats a still-open
/// connection as wedged and fails the Attempt fast rather than waiting out
/// its full deadline, which ADR 0003 allows up to 24 hours for video, music,
/// and raw `ComfyUI` Generations.
const WS_IDLE_TIMEOUT: Duration = Duration::from_mins(2);

/// How often this adapter sends a WebSocket ping while otherwise idle, so
/// a `ComfyUI` process that accepted the TCP connection but stopped
/// servicing it is detected several pings inside `WS_IDLE_TIMEOUT` rather
/// than only once the whole window elapses.
const WS_PING_INTERVAL: Duration = Duration::from_secs(30);

/// Reserved `parameters` key carrying the graph pointer that receives the
/// Generation's shared seed. See the module documentation for the contract.
const SEED_POINTER_KEY: &str = "$seed";

/// Adapter over one operator-managed `ComfyUI` process (ADR 0005).
pub struct ComfyBackend {
    client: reqwest::Client,
    base_url: Url,
    state_dir: PathBuf,
    slots: u32,
    /// Model paths this Pool expects to find on disk
    /// (`PoolConfig::model_paths`), used to resolve a graph's
    /// checkpoint-loader filename to a concrete path for pinned-Model-
    /// Version revalidation (ADR 0012).
    model_paths: Vec<PathBuf>,
    /// Operator-declared exact custom-node versions, used when `ComfyUI` cannot
    /// expose a package version through its API (ADR 0007).
    custom_node_versions: BTreeMap<String, String>,
}

impl ComfyBackend {
    /// Builds an adapter targeting the loopback `ComfyUI` instance described by
    /// `pool`.
    #[must_use]
    pub fn new(pool: &PoolConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|_| http_client());
        Self {
            client,
            base_url: pool.base_url.clone(),
            state_dir: pool.state_dir.clone(),
            slots: pool
                .slots
                .unwrap_or_else(|| BackendKind::ComfyUi.default_slots()),
            model_paths: pool.model_paths.clone(),
            custom_node_versions: pool.custom_node_versions.clone(),
        }
    }

    /// Builds the `ws://` (or `wss://`) URL for the live event stream scoped
    /// to `client_id`.
    fn ws_url(&self, client_id: &str) -> Result<Url, BackendError> {
        let mut url = self.base_url.clone();
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme).map_err(|()| {
            internal_error("comfyui base url has an unsupported scheme for websocket upgrade")
        })?;
        url.set_path("/ws");
        url.query_pairs_mut()
            .clear()
            .append_pair("clientId", client_id);
        Ok(url)
    }

    /// `GET /system_stats`: version and optional accelerator telemetry.
    async fn system_stats(&self) -> Result<SystemStatsResponse, BackendError> {
        let resp = self
            .client
            .get(endpoint(&self.base_url, "/system_stats"))
            .send()
            .await
            .map_err(|e| super::normalize_transport_error(&e))?;
        if !resp.status().is_success() {
            return Err(internal_error(format!(
                "comfyui system_stats returned {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| internal_error(format!("malformed comfyui system_stats response: {e}")))
    }

    /// `GET /object_info`: every registered node class, used to derive
    /// installed custom-node packages (ADR 0007, ADR 0018).
    async fn object_info(&self) -> Result<BTreeMap<String, ObjectInfoEntry>, BackendError> {
        let resp = self
            .client
            .get(endpoint(&self.base_url, "/object_info"))
            .send()
            .await
            .map_err(|e| super::normalize_transport_error(&e))?;
        if !resp.status().is_success() {
            return Err(internal_error(format!(
                "comfyui object_info returned {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| internal_error(format!("malformed comfyui object_info response: {e}")))
    }

    /// Combines `/system_stats` and `/object_info` into the installed
    /// custom-node package/version map used both for capability advertising
    /// and for Worker revalidation before execution (ADR 0007).
    async fn installed_custom_nodes(&self) -> Result<BTreeMap<String, String>, BackendError> {
        let stats = self.system_stats().await?;
        let object_info = self.object_info().await?;
        Ok(custom_node_versions(
            &object_info,
            &stats.system.package_versions(),
            &self.custom_node_versions,
        ))
    }

    /// Safe probe of `POST /prompt`: an empty body deterministically triggers
    /// `ComfyUI`'s `no_prompt` validation error without queuing any work.
    async fn probe_generation(&self) -> bool {
        probe_ok(
            self.client
                .post(endpoint(&self.base_url, "/prompt"))
                .json(&Value::Object(Map::new())),
            |status| status == StatusCode::BAD_REQUEST,
        )
        .await
    }

    /// Safe probe of `GET /ws`: connects and waits briefly for the initial
    /// `status` frame every connection receives. Progress delivery rides the
    /// same channel, so it shares this probe result (ADR 0005: "required
    /// endpoint probes rather than version allowlists").
    async fn probe_streaming(&self) -> bool {
        let Ok(url) = self.ws_url(&format!("probe-{}", Uuid::now_v7())) else {
            return false;
        };
        let Ok((mut stream, _response)) = tokio_tungstenite::connect_async(url.as_str()).await
        else {
            return false;
        };
        tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .is_ok_and(|item| item.is_some())
    }

    /// Safe probe of `GET /history`, `ComfyUI`'s result-retrieval surface.
    async fn probe_result(&self) -> bool {
        probe_ok(
            self.client
                .get(endpoint(&self.base_url, "/history?max_items=0")),
            |status| status.is_success(),
        )
        .await
    }

    /// Safe probe of `POST /interrupt`: a random UUID never matches a
    /// currently running job, so this is a documented no-op that still
    /// proves the cancellation endpoint is reachable.
    async fn probe_cancellation(&self) -> bool {
        let probe_id = Uuid::now_v7().to_string();
        probe_ok(
            self.client
                .post(endpoint(&self.base_url, "/interrupt"))
                .json(&serde_json::json!({ "prompt_id": probe_id })),
            |status| status.is_success(),
        )
        .await
    }

    /// Safe probe of `POST /free` with both flags `false`, which changes
    /// nothing but proves the memory-release endpoint exists. A missing
    /// `/free` is not fatal: the caller falls back to process restart
    /// (ADR 0005).
    async fn probe_memory_release(&self) -> bool {
        probe_ok(
            self.client
                .post(endpoint(&self.base_url, "/free"))
                .json(&serde_json::json!({ "unload_models": false, "free_memory": false })),
            |status| status.is_success(),
        )
        .await
    }

    /// Rejects registered Workflow graphs referencing custom nodes or Models
    /// absent from this `ComfyUI` instance before an Attempt starts wasting execution time
    /// (ADR 0007: "Worker revalidation before execution"; ADR 0018: absent
    /// nodes are rejected, never installed). When the pinned Workflow
    /// Version requires a Model Version, this also rehashes the actual
    /// checkpoint file the graph's loader node names on this Worker's
    /// disk, so a checkpoint swapped locally between Attempts of the same
    /// Generation is caught here rather than silently executed (ADR 0012).
    async fn revalidate(
        &self,
        manifest: &WorkflowManifest,
        request: &ExecutionRequest,
    ) -> Result<(), BackendError> {
        let Some(expected_kind) = expected_media_kind(request.modality) else {
            return Err(BackendError {
                kind: FailureKind::InvalidInput,
                message: "raw ComfyUI prompts must not use a workflow manifest".to_owned(),
                retry_hint: false,
            });
        };
        if expected_kind != manifest.artifact_kind {
            return Err(BackendError {
                kind: FailureKind::InvalidInput,
                message: format!(
                    "generation modality {} implies {expected_kind} output but the pinned workflow version declares {} (ADR 0007)",
                    request.modality, manifest.artifact_kind
                ),
                retry_hint: false,
            });
        }
        if !manifest.required_custom_nodes.is_empty() {
            let installed = self.installed_custom_nodes().await?;
            validate_custom_node_requirements(&manifest.required_custom_nodes, &installed)?;
        }
        if !manifest.required_models.is_empty() {
            self.verify_pinned_checkpoint(
                &manifest.required_models,
                request.workflow_graph.as_ref(),
            )
            .await?;
        }
        Ok(())
    }

    /// Confirms the checkpoint file this Worker will actually feed to the
    /// graph's loader node still matches the Attempt's pinned Model Version
    /// (ADR 0012).
    ///
    /// `ComfyUI` has no persistently resident model the way LLM backends do
    /// (this adapter's `probe` always reports `resident_model: None`),
    /// so there is no already-loaded process state to trust here: this
    /// rehashes whatever is on disk right now, on every Attempt, which is
    /// exactly what would catch a checkpoint file swapped locally between
    /// Attempts of the same Generation.
    ///
    /// A graph naming no checkpoint at all (no node with a `ckpt_name`
    /// input, e.g. a post-processing-only Workflow Version) has nothing
    /// for this to verify against; that is a legitimate graph shape, not a
    /// failure, so it passes.
    async fn verify_pinned_checkpoint(
        &self,
        expected_hashes: &[ContentHash],
        graph: Option<&Value>,
    ) -> Result<(), BackendError> {
        let Some(graph) = graph else {
            return Ok(());
        };
        for ckpt_name in checkpoint_names(graph) {
            let Some(path) = resolve_checkpoint_path(&self.model_paths, ckpt_name) else {
                return Err(BackendError {
                    kind: FailureKind::ModelUnavailable,
                    message: format!(
                        "comfyui graph names checkpoint '{ckpt_name}' but no configured model path on this pool resolves it"
                    ),
                    retry_hint: false,
                });
            };
            let path = path.to_path_buf();
            let hash = tokio::task::spawn_blocking(move || hash_model_fresh(&path))
                .await
                .map_err(|err| BackendError {
                    kind: FailureKind::Internal,
                    message: format!("model hashing task panicked: {err}"),
                    retry_hint: FailureKind::Internal.is_retryable(),
                })?
                .map_err(|err| BackendError {
                    kind: FailureKind::ModelUnavailable,
                    message: format!("hashing checkpoint '{ckpt_name}': {err}"),
                    retry_hint: true,
                })?;
            if !expected_hashes.contains(&hash) {
                return Err(BackendError {
                    kind: FailureKind::ModelUnavailable,
                    message: format!(
                        "checkpoint '{ckpt_name}' on disk is {hash} but this attempt is pinned to one of {expected_hashes:?}"
                    ),
                    retry_hint: false,
                });
            }
        }
        Ok(())
    }

    /// Uploads one input Artifact via `POST /upload/image` and returns the
    /// ComfyUI-relative filename (including subfolder, if any) it was stored
    /// under.
    async fn upload_input(&self, artifact: &InputArtifact) -> Result<String, BackendError> {
        let bytes = tokio::fs::read(&artifact.path).await.map_err(|e| {
            internal_error(format!(
                "reading input artifact '{}': {e}",
                artifact.artifact_id
            ))
        })?;
        let file_name = artifact
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(|| format!("{}.bin", artifact.artifact_id), str::to_string);
        let mime = artifact.manifest.mime_type.clone();
        let part = Part::bytes(bytes)
            .file_name(file_name)
            .mime_str(&mime)
            .map_err(|e| {
                internal_error(format!(
                    "invalid mime type '{mime}' for uploaded artifact: {e}"
                ))
            })?;
        let form = Form::new()
            .part("image", part)
            .text("type", "input")
            .text("overwrite", "true");
        let resp = self
            .client
            .post(endpoint(&self.base_url, "/upload/image"))
            .multipart(form)
            .send()
            .await
            .map_err(|e| super::normalize_transport_error(&e))?;
        if !resp.status().is_success() {
            return Err(BackendError {
                kind: FailureKind::TransferFailed,
                message: format!("comfyui upload/image returned {}", resp.status()),
                retry_hint: true,
            });
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| internal_error(format!("malformed comfyui upload/image response: {e}")))?;
        let Some(name) = body.get("name").and_then(Value::as_str) else {
            return Err(internal_error(
                "comfyui upload/image response missing 'name'",
            ));
        };
        let subfolder = body.get("subfolder").and_then(Value::as_str).unwrap_or("");
        Ok(if subfolder.is_empty() {
            name.to_string()
        } else {
            format!("{subfolder}/{name}")
        })
    }

    /// `POST /prompt`: submits the finalized graph and returns the minted
    /// `prompt_id`.
    async fn submit_prompt(
        &self,
        prompt: Value,
        client_id: &str,
        extra_data: Option<&Map<String, Value>>,
        partial_execution_targets: Option<&[String]>,
    ) -> Result<String, BackendError> {
        let mut body = Map::new();
        body.insert("prompt".to_owned(), prompt);
        body.insert("client_id".to_owned(), Value::String(client_id.to_owned()));
        if let Some(extra_data) = extra_data {
            let mut extra_data = extra_data.clone();
            extra_data.insert("client_id".to_owned(), Value::String(client_id.to_owned()));
            body.insert("extra_data".to_owned(), Value::Object(extra_data));
        }
        if let Some(targets) = partial_execution_targets {
            body.insert(
                "partial_execution_targets".to_owned(),
                Value::Array(targets.iter().cloned().map(Value::String).collect()),
            );
        }
        let resp = self
            .client
            .post(endpoint(&self.base_url, "/prompt"))
            .json(&body)
            .send()
            .await
            .map_err(|e| super::normalize_transport_error(&e))?;
        let status = resp.status();
        let payload: Value = resp
            .json()
            .await
            .map_err(|e| internal_error(format!("malformed comfyui /prompt response: {e}")))?;
        if status == StatusCode::BAD_REQUEST {
            return Err(BackendError {
                kind: classify_prompt_validation_error(&payload),
                message: prompt_error_message(&payload),
                retry_hint: false,
            });
        }
        if status.is_server_error() {
            // ComfyUI itself failed, which ADR 0003 treats as a retryable
            // backend crash.
            return Err(BackendError {
                kind: FailureKind::BackendCrashed,
                message: format!("comfyui /prompt returned {status}"),
                retry_hint: FailureKind::BackendCrashed.is_retryable(),
            });
        }
        if !status.is_success() {
            return Err(internal_error(format!("comfyui /prompt returned {status}")));
        }
        let Some(prompt_id) = payload.get("prompt_id").and_then(Value::as_str) else {
            return Err(internal_error(
                "comfyui /prompt response missing 'prompt_id'",
            ));
        };
        Ok(prompt_id.to_string())
    }

    /// `POST /interrupt`: best-effort cooperative cancellation. Failures are
    /// swallowed here; the caller's grace timeout is the real backstop.
    async fn interrupt(&self, prompt_id: &str) {
        let outcome = self
            .client
            .post(endpoint(&self.base_url, "/interrupt"))
            .json(&serde_json::json!({ "prompt_id": prompt_id }))
            .send()
            .await;
        if let Err(err) = outcome {
            warn!(prompt_id, error = %err, "comfyui interrupt request failed");
        }
    }

    /// `GET /history/{prompt_id}`: authoritative post-completion result
    /// lookup, used when a node's output was not already captured from a
    /// live `executed` event.
    async fn get_history(&self, prompt_id: &str) -> Result<Value, BackendError> {
        let resp = self
            .client
            .get(endpoint(&self.base_url, &format!("/history/{prompt_id}")))
            .send()
            .await
            .map_err(|e| super::normalize_transport_error(&e))?;
        if !resp.status().is_success() {
            return Err(internal_error(format!(
                "comfyui history returned {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|e| internal_error(format!("malformed comfyui history response: {e}")))
    }

    /// `GET /view`: streams one declared output file's bytes straight to
    /// `dest`, hashing incrementally over the same pass (ADR 0008: the
    /// reported manifest is always computed from the actual transferred
    /// bytes, never assumed). A video or music Workflow Version, or a raw
    /// `ComfyUI` prompt, can produce a multi-gigabyte file — ADR 0003 budgets a 24-hour deadline
    /// for exactly those — so this holds only one `reqwest` chunk in memory
    /// at a time rather than the whole file, on a host already loaded with
    /// GPU work.
    #[expect(
        clippy::too_many_lines,
        reason = "streaming a Comfy output must keep request, cancellation, deadline, hashing, and file-write handling together"
    )]
    async fn download_view(
        &self,
        entry: &ViewRef,
        dest: &Path,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<(ContentHash, u64, String), BackendError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(BackendError {
                kind: FailureKind::ExecutionTimedOut,
                message: "comfyui output download exceeded the Attempt deadline".to_owned(),
                retry_hint: false,
            });
        }
        let request = self
            .client
            .get(endpoint(&self.base_url, "/view"))
            .query(&[
                ("filename", entry.filename.as_str()),
                ("subfolder", entry.subfolder.as_str()),
                ("type", entry.kind.as_str()),
            ])
            .timeout(remaining);
        let resp = tokio::select! {
            () = cancel.cancelled() => {
                return Err(BackendError {
                    kind: FailureKind::Cancelled,
                    message: "comfyui output download cancelled".to_owned(),
                    retry_hint: false,
                });
            }
            result = request.send() => result.map_err(|e| {
                if Instant::now() >= deadline {
                    BackendError {
                        kind: FailureKind::ExecutionTimedOut,
                        message: "comfyui output download exceeded the Attempt deadline".to_owned(),
                        retry_hint: false,
                    }
                } else {
                    super::normalize_transport_error(&e)
                }
            })?
        };
        if !resp.status().is_success() {
            return Err(BackendError {
                kind: FailureKind::TransferFailed,
                message: format!("comfyui view returned {}", resp.status()),
                retry_hint: true,
            });
        }
        let mime_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.split(';').next().unwrap_or(value).trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("application/octet-stream")
            .to_owned();
        let mut file = tokio::fs::File::create(dest).await.map_err(|e| {
            internal_error(format!(
                "creating comfyui output file '{}': {e}",
                dest.display()
            ))
        })?;
        let mut hasher = Hasher::new();
        let mut size = 0_u64;
        let mut chunks = resp.bytes_stream();
        loop {
            let chunk = tokio::select! {
                () = cancel.cancelled() => {
                    return Err(BackendError {
                        kind: FailureKind::Cancelled,
                        message: "comfyui output download cancelled".to_owned(),
                        retry_hint: false,
                    });
                }
                () = tokio::time::sleep_until(deadline) => {
                    return Err(BackendError {
                        kind: FailureKind::ExecutionTimedOut,
                        message: "comfyui output download exceeded the Attempt deadline".to_owned(),
                        retry_hint: false,
                    });
                }
                chunk = chunks.next() => chunk,
            };
            let Some(chunk) = chunk else { break };
            let chunk = chunk.map_err(|e| BackendError {
                kind: FailureKind::TransferFailed,
                message: format!("comfyui view transfer failed: {e}"),
                retry_hint: true,
            })?;
            hasher.update(&chunk);
            size += chunk.len() as u64;
            file.write_all(&chunk).await.map_err(|e| {
                internal_error(format!("writing comfyui output '{}': {e}", dest.display()))
            })?;
        }
        if Instant::now() >= deadline {
            return Err(BackendError {
                kind: FailureKind::ExecutionTimedOut,
                message: "comfyui output download exceeded the Attempt deadline".to_owned(),
                retry_hint: false,
            });
        }
        file.flush().await.map_err(|e| {
            internal_error(format!("flushing comfyui output '{}': {e}", dest.display()))
        })?;
        Ok((hasher.finish(), size, mime_type))
    }

    /// Downloads every declared output of `manifest.output_node` /
    /// `manifest.output_name` by streaming each straight to the Attempt's
    /// output directory (see [`Self::download_view`]), and emits one
    /// [`ExecutionEvent::Output`] per file (ADR 0008: the reported manifest
    /// is always computed from the actual transferred bytes, never assumed).
    #[expect(
        clippy::too_many_arguments,
        reason = "registered Comfy output collection needs its prompt, manifest, live output, event, cancellation, and deadline context"
    )]
    async fn collect_outputs(
        &self,
        prompt_id: &str,
        manifest: &WorkflowManifest,
        live_outputs: &HashMap<String, Value>,
        out_dir: &Path,
        events: &mpsc::Sender<ExecutionEvent>,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), BackendError> {
        let output = if let Some(value) = live_outputs.get(&manifest.output_node) {
            value.clone()
        } else {
            let history = wait_for_history(self, prompt_id, cancel, deadline).await?;
            history_output(&history, prompt_id, &manifest.output_node)?
        };
        let entries = extract_output_entries(&output, &manifest.output_name)?;
        for (index, entry) in entries.iter().enumerate() {
            let path = out_dir.join(format!("{index}_{}", entry.filename));
            let (digest, size_bytes, _mime_type) =
                self.download_view(entry, &path, cancel, deadline).await?;
            let output_manifest = ArtifactManifest {
                size_bytes,
                digest,
                kind: manifest.artifact_kind,
                mime_type: manifest.artifact_mime.clone(),
            };
            events
                .send(ExecutionEvent::Output {
                    path,
                    manifest: output_manifest,
                    comfy_output_pointers: Vec::new(),
                })
                .await
                .map_err(|_| internal_error("worker executor dropped the output event channel"))?;
        }
        Ok(())
    }
    /// Downloads each unique file referenced by raw history outputs, preserving
    /// every JSON pointer that refers to it and retaining the complete metadata.
    async fn collect_raw_outputs(
        &self,
        outputs: &Value,
        output_node_ids: Vec<String>,
        out_dir: &Path,
        events: &mpsc::Sender<ExecutionEvent>,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), BackendError> {
        let plans = plan_raw_outputs(outputs)?;
        for (index, plan) in plans.into_iter().enumerate() {
            let path = out_dir.join(index.to_string());
            let (digest, size_bytes, mime_type) = tokio::select! {
                () = cancel.cancelled() => {
                    return Err(BackendError {
                        kind: FailureKind::Cancelled,
                        message: "raw ComfyUI output recovery cancelled".to_owned(),
                        retry_hint: false,
                    });
                }
                result = self.download_view(&plan.view, &path, cancel, deadline) => result?,
            };
            let output_manifest = ArtifactManifest {
                size_bytes,
                digest,
                kind: MediaKind::from_mime(&mime_type),
                mime_type,
            };
            events
                .send(ExecutionEvent::Output {
                    path,
                    manifest: output_manifest,
                    comfy_output_pointers: plan.pointers,
                })
                .await
                .map_err(|_| internal_error("worker executor dropped the output event channel"))?;
        }
        events
            .send(ExecutionEvent::ComfyOutputs {
                outputs: outputs.clone(),
                output_node_ids,
            })
            .await
            .map_err(|_| internal_error("worker executor dropped the output event channel"))?;
        Ok(())
    }
}

/// Sends `request` and reports whether it succeeded and its response status
/// satisfies `accept`. Shared by the safe `ComfyUI` control-plane probes
/// (ADR 0005): each probe differs only in the request built and the status
/// predicate it checks.
async fn probe_ok(
    request: reqwest::RequestBuilder,
    accept: impl FnOnce(StatusCode) -> bool,
) -> bool {
    matches!(request.send().await, Ok(resp) if accept(resp.status()))
}

/// The `MediaKind` a declared Workflow's modality implies for its output.
fn expected_media_kind(modality: Modality) -> Option<MediaKind> {
    match modality {
        Modality::Image => Some(MediaKind::Image),
        Modality::Video => Some(MediaKind::Video),
        Modality::Music => Some(MediaKind::Audio),
        Modality::Llm | Modality::Comfy => None,
    }
}

/// Extracts every checkpoint filename an execution graph's loader nodes name.
///
/// `ComfyUI` API-format graphs are opaque backend payloads (ADR 0007); the
/// one convention this adapter relies on is that every checkpoint-loading
/// node — `CheckpointLoaderSimple` and every custom checkpoint loader this
/// adapter has encountered — exposes its pinned file through an
/// `inputs.ckpt_name` string. A graph with no such input names no
/// checkpoint at all (e.g. a post-processing-only Workflow Version), which
/// is a legitimate shape the caller must treat as "nothing to verify", not
/// a missing-input error.
fn checkpoint_names(graph: &Value) -> Vec<&str> {
    graph
        .as_object()
        .map(|object| {
            object
                .values()
                .filter_map(|node| node.get("inputs")?.get("ckpt_name")?.as_str())
                .collect()
        })
        .unwrap_or_default()
}

/// Resolves a `ComfyUI`-relative checkpoint filename (as named by a
/// graph's `ckpt_name` input, e.g. `"sdxl/base.safetensors"`) to the
/// configured `model_paths` entry it refers to, matching on the path's
/// trailing components so both a bare filename and a subfolder-qualified
/// one resolve correctly regardless of platform path separators.
fn resolve_checkpoint_path<'a>(model_paths: &'a [PathBuf], ckpt_name: &str) -> Option<&'a Path> {
    let wanted: Vec<&str> = ckpt_name
        .split(['/', '\\'])
        .filter(|segment| !segment.is_empty())
        .collect();
    if wanted.is_empty() {
        return None;
    }
    model_paths.iter().map(PathBuf::as_path).find(|path| {
        let components: Vec<&str> = path
            .components()
            .filter_map(|component| component.as_os_str().to_str())
            .collect();
        components.len() >= wanted.len()
            && components[components.len() - wanted.len()..] == wanted[..]
    })
}

#[async_trait]
impl Backend for ComfyBackend {
    async fn probe(&self) -> Result<BackendCapabilities, BackendError> {
        let stats = self.system_stats().await?;
        let object_info = self.object_info().await?;
        let custom_nodes = custom_node_versions(
            &object_info,
            &stats.system.package_versions(),
            &self.custom_node_versions,
        );

        let generation_ok = self.probe_generation().await;
        let streaming_ok = self.probe_streaming().await;
        let result_ok = self.probe_result().await;
        let cancellation_ok = self.probe_cancellation().await;
        let memory_release_ok = self.probe_memory_release().await;
        let mut probes = BTreeMap::new();
        probes.insert("generation".to_string(), generation_ok);
        probes.insert("streaming".to_string(), streaming_ok);
        // Progress events ride the same WebSocket channel as generation
        // streaming; ComfyUI exposes no separate endpoint to probe (ADR 0005).
        probes.insert("progress".to_string(), streaming_ok);
        probes.insert("result".to_string(), result_ok);
        probes.insert("cancellation".to_string(), cancellation_ok);
        probes.insert("memory_release".to_string(), memory_release_ok);
        probes.insert(
            "comfy_prompt".to_string(),
            generation_ok && streaming_ok && result_ok,
        );
        let accelerator_memory_bytes = stats
            .devices
            .first()
            .and_then(|device| device.vram_free.or(device.vram_total));

        Ok(BackendCapabilities {
            version: stats.system.comfyui_version,
            slots: self.slots,
            // ComfyUI has no single persistently "resident model" concept the
            // way LLM backends do: graphs load whatever checkpoints their own
            // nodes name (ADR 0007).
            resident_model: None,
            accelerator_memory_bytes,
            custom_nodes,
            probes,
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "raw and registered ComfyUI execution share one cancellation and event state machine"
    )]
    async fn execute(
        &self,
        request: ExecutionRequest,
        events: mpsc::Sender<ExecutionEvent>,
        cancel: CancellationToken,
    ) -> Result<(), BackendError> {
        let (graph, manifest, raw_prompt) = if let Some(raw_payload) = request.comfy_prompt.as_ref()
        {
            if request.modality != Modality::Comfy {
                return Err(internal_error(
                    "raw ComfyUI payload requires comfy modality",
                ));
            }
            let raw = parse_raw_prompt(raw_payload)?;
            (raw.graph.clone(), None, Some(raw))
        } else {
            let Some(manifest) = request.workflow_manifest.clone() else {
                return Err(BackendError {
                    kind: FailureKind::InvalidInput,
                    message: "comfyui execution requires a workflow manifest".to_string(),
                    retry_hint: false,
                });
            };
            let Some(Value::Object(mut graph)) = request.workflow_graph.clone() else {
                return Err(BackendError {
                    kind: FailureKind::InvalidInput,
                    message: "comfyui execution requires an API-format workflow graph object"
                        .to_string(),
                    retry_hint: false,
                });
            };

            self.revalidate(&manifest, &request).await?;

            if !request.inputs.is_empty() {
                let mut uploads = HashMap::with_capacity(request.inputs.len());
                for artifact in &request.inputs {
                    let uploaded_name = self.upload_input(artifact).await?;
                    uploads.insert(artifact.artifact_id.clone(), uploaded_name);
                }
                for value in graph.values_mut() {
                    substitute_artifact_placeholders(value, &uploads);
                }
            }

            apply_parameters(&mut graph, &request.parameters, request.seed)?;
            (graph, Some(manifest), None)
        };
        let client_id = Uuid::now_v7().to_string();
        let ws_url = self.ws_url(&client_id)?;
        let (ws_stream, _response) = tokio_tungstenite::connect_async(ws_url.as_str())
            .await
            .map_err(|e| BackendError {
                kind: FailureKind::BackendCrashed,
                message: format!("connecting to comfyui websocket: {e}"),
                retry_hint: true,
            })?;
        let (mut ws_write, mut ws_stream) = ws_stream.split();

        let prompt_id = self
            .submit_prompt(
                Value::Object(graph),
                &client_id,
                raw_prompt.as_ref().map(|raw| &raw.extra_data),
                raw_prompt
                    .as_ref()
                    .and_then(|raw| raw.partial_execution_targets.as_deref()),
            )
            .await?;

        let out_dir = self
            .state_dir
            .join("attempts")
            .join(&request.attempt_id)
            .join("outputs");
        tokio::fs::create_dir_all(&out_dir)
            .await
            .map_err(|e| internal_error(format!("creating comfyui output directory: {e}")))?;

        let mut live_outputs: HashMap<String, Value> = HashMap::new();
        let deadline_instant = Instant::now() + request.deadline;
        let mut pending: Option<FailureKind> = None;
        let mut grace_deadline: Option<Instant> = None;
        let mut idle_deadline = Instant::now() + WS_IDLE_TIMEOUT;
        let mut ping_interval = tokio::time::interval(WS_PING_INTERVAL);

        'wait: loop {
            tokio::select! {
                () = cancel.cancelled(), if pending.is_none() => {
                    self.interrupt(&prompt_id).await;
                    pending = Some(FailureKind::Cancelled);
                    grace_deadline = Some(Instant::now() + INTERRUPT_GRACE);
                }
                () = tokio::time::sleep_until(deadline_instant), if pending.is_none() => {
                    self.interrupt(&prompt_id).await;
                    pending = Some(FailureKind::ExecutionTimedOut);
                    grace_deadline = Some(Instant::now() + INTERRUPT_GRACE);
                }
                () = tokio::time::sleep_until(grace_deadline.unwrap_or_else(Instant::now)), if grace_deadline.is_some() => {
                    let Some(kind) = pending else {
                        return Err(internal_error("comfyui interrupt grace timer fired without a pending reason"));
                    };
                    return Err(BackendError {
                        kind,
                        message: "comfyui did not acknowledge cooperative cancellation in time".to_string(),
                        retry_hint: kind.is_retryable(),
                    });
                }
                () = tokio::time::sleep_until(idle_deadline), if pending.is_none() => {
                    return Err(BackendError {
                        kind: FailureKind::BackendCrashed,
                        message: format!(
                            "comfyui websocket received no frame for {WS_IDLE_TIMEOUT:?}; treating the connection as wedged"
                        ),
                        retry_hint: true,
                    });
                }
                _ = ping_interval.tick(), if pending.is_none() => {
                    if ws_write.send(Message::Ping(Vec::new().into())).await.is_err() {
                        return Err(BackendError {
                            kind: FailureKind::BackendCrashed,
                            message: "comfyui websocket ping failed; connection appears dead".to_string(),
                            retry_hint: true,
                        });
                    }
                }
                frame = ws_stream.next() => {
                    let Some(frame) = frame else {
                        return Err(BackendError {
                            kind: FailureKind::BackendCrashed,
                            message: "comfyui websocket closed unexpectedly".to_string(),
                            retry_hint: true,
                        });
                    };
                    let frame = frame.map_err(|e| BackendError {
                        kind: FailureKind::BackendCrashed,
                        message: format!("comfyui websocket error: {e}"),
                        retry_hint: true,
                    })?;
                    idle_deadline = Instant::now() + WS_IDLE_TIMEOUT;
                    let Ok(text) = frame.to_text() else { continue 'wait };
                    let Some(event) = parse_event(text) else { continue 'wait };
                    match event {
                        ComfyEvent::Progress { prompt_id: pid, node, value, max } if pid == prompt_id => {
                            #[expect(
                                clippy::cast_precision_loss,
                                reason = "comfyui step/max counters never approach 2^52; an approximate progress fraction is acceptable"
                            )]
                            let fraction = if max == 0 { 0.0 } else { value as f64 / max as f64 };
                            let _ = events.send(ExecutionEvent::Progress {
                                fraction,
                                stage: node.unwrap_or_default(),
                                step: u32::try_from(value).unwrap_or(u32::MAX),
                                total_steps: u32::try_from(max).unwrap_or(u32::MAX),
                            }).await;
                        }
                        ComfyEvent::Executing { prompt_id: pid, node: Some(node) } if pid == prompt_id => {
                            let _ = events.send(ExecutionEvent::Progress {
                                fraction: 0.0,
                                stage: node,
                                step: 0,
                                total_steps: 0,
                            }).await;
                        }
                        // Official ComfyUI uses `executing` with `node: null`
                        // as the completion marker. Some versions also emit
                        // `execution_success`, which is handled below.
                        ComfyEvent::Executing { prompt_id: pid, node: None }
                            if pid == prompt_id && pending.is_none() =>
                        {
                            break 'wait;
                        }
                        ComfyEvent::Executed { prompt_id: pid, node, output: Some(output) } if pid == prompt_id => {
                            live_outputs.insert(node, output);
                        }
                        ComfyEvent::ExecutionSuccess { prompt_id: pid } if pid == prompt_id => {
                            break 'wait;
                        }
                        ComfyEvent::ExecutionError { prompt_id: pid, exception_type, exception_message } if pid == prompt_id => {
                            let kind = pending.unwrap_or_else(|| classify_execution_error(&exception_type, &exception_message));
                            return Err(BackendError {
                                kind,
                                message: format!("{exception_type}: {exception_message}"),
                                retry_hint: kind.is_retryable(),
                            });
                        }
                        ComfyEvent::ExecutionInterrupted { prompt_id: pid } if pid == prompt_id => {
                            let requested = pending.is_some();
                            let kind = pending.unwrap_or(FailureKind::BackendCrashed);
                            let message = if requested {
                                "comfyui execution interrupted by worker request".to_string()
                            } else {
                                "comfyui execution interrupted unexpectedly".to_string()
                            };
                            return Err(BackendError { kind, message, retry_hint: kind.is_retryable() });
                        }
                        _ => {}
                    }
                }
            }
        }

        if raw_prompt.is_some() {
            let history = wait_for_history(self, &prompt_id, &cancel, deadline_instant).await?;
            let (outputs, output_node_ids) = raw_history_result(&history, &prompt_id)?;
            self.collect_raw_outputs(
                &outputs,
                output_node_ids,
                &out_dir,
                &events,
                &cancel,
                deadline_instant,
            )
            .await
        } else {
            let Some(manifest) = manifest else {
                return Err(internal_error(
                    "declared Comfy execution lost its workflow manifest",
                ));
            };
            self.collect_outputs(
                &prompt_id,
                &manifest,
                &live_outputs,
                &out_dir,
                &events,
                &cancel,
                deadline_instant,
            )
            .await
        }
    }

    async fn release_memory(&self) -> Result<bool, BackendError> {
        let outcome = self
            .client
            .post(endpoint(&self.base_url, "/free"))
            .json(&serde_json::json!({ "unload_models": true, "free_memory": true }))
            .send()
            .await;
        outcome
            .map(|resp| resp.status().is_success())
            .map_err(|err| super::normalize_transport_error(&err))
    }
}

/// `GET /system_stats` response shape (fields beyond what this adapter uses
/// are ignored by `serde`'s default struct handling).
#[derive(Debug, Deserialize)]
struct SystemStatsResponse {
    system: SystemStatsSystem,
    #[serde(default)]
    devices: Vec<SystemStatsDevice>,
}

#[derive(Debug, Deserialize)]
struct SystemStatsSystem {
    comfyui_version: String,
    /// Upstream reports every `comfy*` package pinned in its
    /// `requirements.txt` as `{name, installed, required}` entries
    /// (`app/frontend_management.py::get_comfy_package_versions`); builds
    /// older than that field omit it entirely.
    #[serde(default)]
    comfy_package_versions: Vec<ComfyPackageVersion>,
}

#[derive(Debug, Deserialize)]
struct ComfyPackageVersion {
    name: String,
    #[serde(default)]
    installed: Option<String>,
}

impl SystemStatsSystem {
    /// Installed package -> version map; packages `ComfyUI` could not resolve
    /// an installed version for are omitted so they fall back to `"unknown"`
    /// in [`custom_node_versions`].
    fn package_versions(&self) -> BTreeMap<String, String> {
        self.comfy_package_versions
            .iter()
            .filter_map(|package| {
                package
                    .installed
                    .as_ref()
                    .map(|installed| (package.name.clone(), installed.clone()))
            })
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct SystemStatsDevice {
    #[serde(default)]
    vram_free: Option<u64>,
    #[serde(default)]
    vram_total: Option<u64>,
}

/// One entry of a `GET /object_info` response; only `python_module` is used,
/// to distinguish core node classes from custom-node packages.
#[derive(Debug, Clone, Deserialize)]
struct ObjectInfoEntry {
    #[serde(default)]
    python_module: String,
}

/// One declared output file inside a `history[...].outputs[node_id][name]`
/// array (ADR 0007; the file-reference shape used by `ComfyUI` core and
/// custom nodes).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct ViewRef {
    filename: String,
    #[serde(default)]
    subfolder: String,
    #[serde(default = "default_view_kind", rename = "type")]
    kind: String,
}

fn default_view_kind() -> String {
    "output".to_string()
}

#[derive(Debug, Clone)]
struct RawPrompt {
    graph: Map<String, Value>,
    extra_data: Map<String, Value>,
    partial_execution_targets: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct RawOutputPlan {
    view: ViewRef,
    pointers: Vec<String>,
}

fn parse_raw_prompt(value: &Value) -> Result<RawPrompt, BackendError> {
    let Value::Object(envelope) = value else {
        return Err(internal_error(
            "raw ComfyUI prompt envelope must be an object",
        ));
    };
    if envelope.keys().any(|key| {
        !matches!(
            key.as_str(),
            "prompt" | "extra_data" | "partial_execution_targets"
        )
    }) {
        return Err(internal_error(
            "raw ComfyUI prompt envelope has unknown fields",
        ));
    }
    let Some(Value::Object(graph)) = envelope.get("prompt") else {
        return Err(internal_error(
            "raw ComfyUI prompt envelope has no graph object",
        ));
    };
    if graph.is_empty() {
        return Err(internal_error("raw ComfyUI prompt graph is empty"));
    }
    for (node_id, node) in graph {
        let Some(node) = node.as_object() else {
            return Err(internal_error(format!(
                "raw ComfyUI node '{node_id}' is not an object"
            )));
        };
        if node
            .get("class_type")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
            || !node.get("inputs").is_some_and(Value::is_object)
        {
            return Err(internal_error(format!(
                "raw ComfyUI node '{node_id}' has an invalid class_type or inputs"
            )));
        }
    }
    let extra_data = match envelope.get("extra_data") {
        None => Map::new(),
        Some(Value::Object(extra_data)) => extra_data.clone(),
        Some(_) => return Err(internal_error("raw ComfyUI extra_data is not an object")),
    };
    let partial_execution_targets = match envelope.get("partial_execution_targets") {
        None => None,
        Some(Value::Array(targets)) if !targets.is_empty() => Some(
            targets
                .iter()
                .map(|target| {
                    target.as_str().map(str::to_owned).ok_or_else(|| {
                        internal_error("raw ComfyUI partial execution target is not a string")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Some(Value::Array(_)) => {
            return Err(internal_error(
                "raw ComfyUI partial execution targets are empty",
            ));
        }
        Some(_) => {
            return Err(internal_error(
                "raw ComfyUI partial execution targets are not an array",
            ));
        }
    };
    if partial_execution_targets
        .as_ref()
        .is_some_and(|targets| targets.iter().any(|target| !graph.contains_key(target)))
    {
        return Err(internal_error(
            "raw ComfyUI partial execution target is not in the graph",
        ));
    }
    Ok(RawPrompt {
        graph: graph.clone(),
        extra_data,
        partial_execution_targets,
    })
}

async fn wait_for_history(
    backend: &ComfyBackend,
    prompt_id: &str,
    cancel: &CancellationToken,
    deadline: Instant,
) -> Result<Value, BackendError> {
    let history_deadline = (Instant::now() + Duration::from_secs(30)).min(deadline);
    loop {
        let history = tokio::select! {
            () = cancel.cancelled() => {
                return Err(BackendError {
                    kind: FailureKind::Cancelled,
                    message: "raw ComfyUI history lookup cancelled".to_owned(),
                    retry_hint: false,
                });
            }
            history = backend.get_history(prompt_id) => history?,
        };
        if history.get(prompt_id).is_some() {
            return Ok(history);
        }
        let now = Instant::now();
        if now >= history_deadline {
            return Err(BackendError {
                kind: FailureKind::TransferFailed,
                message: "ComfyUI history was not published before the deadline".to_owned(),
                retry_hint: true,
            });
        }
        let next = (now + Duration::from_millis(100)).min(history_deadline);
        tokio::select! {
            () = cancel.cancelled() => {
                return Err(BackendError {
                    kind: FailureKind::Cancelled,
                    message: "raw ComfyUI history lookup cancelled".to_owned(),
                    retry_hint: false,
                });
            }
            () = tokio::time::sleep_until(next) => {}
        }
    }
}

fn raw_history_result(
    history: &Value,
    prompt_id: &str,
) -> Result<(Value, Vec<String>), BackendError> {
    let Some(entry) = history.get(prompt_id).and_then(Value::as_object) else {
        return Err(internal_error("ComfyUI history entry is not an object"));
    };
    let Some(outputs) = entry.get("outputs").filter(|value| value.is_object()) else {
        return Err(internal_error(
            "ComfyUI history entry has no outputs object",
        ));
    };
    let Some(prompt) = entry.get("prompt").and_then(Value::as_array) else {
        return Err(internal_error("ComfyUI history entry has no prompt tuple"));
    };
    let Some(node_ids) = prompt.get(4).and_then(Value::as_array) else {
        return Err(internal_error(
            "ComfyUI history prompt tuple has no output node ids",
        ));
    };
    let mut output_node_ids = Vec::with_capacity(node_ids.len());
    for node_id in node_ids {
        let Some(node_id) = node_id.as_str() else {
            return Err(internal_error("ComfyUI output node id is not a string"));
        };
        if output_node_ids.iter().any(|seen| seen == node_id) {
            return Err(internal_error("ComfyUI output node ids contain duplicates"));
        }
        output_node_ids.push(node_id.to_owned());
    }
    Ok((outputs.clone(), output_node_ids))
}

fn plan_raw_outputs(outputs: &Value) -> Result<Vec<RawOutputPlan>, BackendError> {
    if !outputs.is_object() {
        return Err(internal_error("ComfyUI outputs metadata is not an object"));
    }
    let mut plans = Vec::new();
    let mut indexes = BTreeMap::new();
    collect_raw_output_refs(outputs, "", &mut plans, &mut indexes)?;
    Ok(plans)
}

fn collect_raw_output_refs(
    value: &Value,
    pointer: &str,
    plans: &mut Vec<RawOutputPlan>,
    indexes: &mut BTreeMap<ViewRef, usize>,
) -> Result<(), BackendError> {
    match value {
        Value::Object(map) => {
            if map.contains_key("filename") {
                let view: ViewRef = serde_json::from_value(value.clone()).map_err(|error| {
                    internal_error(format!("malformed ComfyUI file reference: {error}"))
                })?;
                validate_view_ref(&view)?;
                let index = if let Some(index) = indexes.get(&view).copied() {
                    index
                } else {
                    let index = plans.len();
                    indexes.insert(view.clone(), index);
                    plans.push(RawOutputPlan {
                        view,
                        pointers: Vec::new(),
                    });
                    index
                };
                plans[index].pointers.push(pointer.to_owned());
            }
            for (key, child) in map {
                let escaped = key.replace('~', "~0").replace('/', "~1");
                let child_pointer = format!("{pointer}/{escaped}");
                collect_raw_output_refs(child, &child_pointer, plans, indexes)?;
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                collect_raw_output_refs(child, &format!("{pointer}/{index}"), plans, indexes)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn validate_view_ref(view: &ViewRef) -> Result<(), BackendError> {
    if view.filename.is_empty()
        || view.filename == "."
        || view.filename == ".."
        || view.filename.contains(['/', '\\', ':'])
        || view.filename.starts_with(['/', '\\'])
        || Path::new(&view.filename).is_absolute()
    {
        return Err(internal_error("ComfyUI output filename is not a basename"));
    }
    if view.subfolder.starts_with(['/', '\\'])
        || view.subfolder.contains('\\')
        || view.subfolder.contains(':')
        || (!view.subfolder.is_empty()
            && view
                .subfolder
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."))
    {
        return Err(internal_error("ComfyUI output subfolder is not relative"));
    }
    if !matches!(view.kind.as_str(), "input" | "output" | "temp") {
        return Err(internal_error("ComfyUI output type is unsupported"));
    }
    Ok(())
}

/// One parsed `ComfyUI` WebSocket event frame (`{"type": ..., "data": ...}`).
#[derive(Debug, Clone, PartialEq)]
enum ComfyEvent {
    Executing {
        prompt_id: String,
        node: Option<String>,
    },
    Progress {
        prompt_id: String,
        node: Option<String>,
        value: u64,
        max: u64,
    },
    Executed {
        prompt_id: String,
        node: String,
        output: Option<Value>,
    },
    ExecutionSuccess {
        prompt_id: String,
    },
    ExecutionError {
        prompt_id: String,
        exception_type: String,
        exception_message: String,
    },
    ExecutionInterrupted {
        prompt_id: String,
    },
    /// A recognized-but-unhandled frame type (e.g. `progress_state`), or one
    /// this adapter does not need to act on.
    Other,
}

/// Parses one WebSocket text frame. Returns `None` only for frames that
/// cannot be interpreted at all (malformed JSON, or a known type missing its
/// required fields); unrecognized event `type`s parse to [`ComfyEvent::Other`].
fn parse_event(frame: &str) -> Option<ComfyEvent> {
    let value: Value = serde_json::from_str(frame).ok()?;
    let event_type = value.get("type")?.as_str()?;
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    match event_type {
        "executing" => Some(ComfyEvent::Executing {
            prompt_id: data.get("prompt_id")?.as_str()?.to_string(),
            node: data.get("node").and_then(Value::as_str).map(str::to_string),
        }),
        "progress" => Some(ComfyEvent::Progress {
            prompt_id: data.get("prompt_id")?.as_str()?.to_string(),
            node: data.get("node").and_then(Value::as_str).map(str::to_string),
            value: data.get("value")?.as_u64()?,
            max: data.get("max")?.as_u64()?,
        }),
        "executed" => Some(ComfyEvent::Executed {
            prompt_id: data.get("prompt_id")?.as_str()?.to_string(),
            node: data.get("node")?.as_str()?.to_string(),
            output: data.get("output").cloned().filter(|v| !v.is_null()),
        }),
        "execution_success" => Some(ComfyEvent::ExecutionSuccess {
            prompt_id: data.get("prompt_id")?.as_str()?.to_string(),
        }),
        "execution_error" => Some(ComfyEvent::ExecutionError {
            prompt_id: data.get("prompt_id")?.as_str()?.to_string(),
            exception_type: data
                .get("exception_type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            exception_message: data
                .get("exception_message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "execution_interrupted" => Some(ComfyEvent::ExecutionInterrupted {
            prompt_id: data.get("prompt_id")?.as_str()?.to_string(),
        }),
        _ => Some(ComfyEvent::Other),
    }
}

/// Extracts the installed custom-node package -> version map from
/// `/object_info`'s per-node `python_module` field, cross-referenced against
/// `/system_stats`' `comfy_package_versions` for a version where one is
/// resolvable. `ComfyUI` core exposes no dedicated "installed custom node
/// packages" endpoint (ADR 0007's capability advertising relies on this
/// best-effort derivation).
fn custom_node_versions(
    object_info: &BTreeMap<String, ObjectInfoEntry>,
    package_versions: &BTreeMap<String, String>,
    configured_versions: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut nodes = BTreeMap::new();
    for entry in object_info.values() {
        let Some(package) = package_name_from_python_module(&entry.python_module) else {
            continue;
        };
        nodes.entry(package.to_string()).or_insert_with(|| {
            configured_versions
                .get(package)
                .cloned()
                .or_else(|| package_versions.get(package).cloned())
                .unwrap_or_else(|| "unknown".to_string())
        });
    }
    nodes
}

/// Verifies that every required custom-node package is installed at its exact
/// manifest version, not merely present in the backend's node registry.
fn validate_custom_node_requirements(
    required: &BTreeMap<String, String>,
    installed: &BTreeMap<String, String>,
) -> Result<(), BackendError> {
    for (package, required_version) in required {
        let Some(installed_version) = installed.get(package) else {
            return Err(BackendError {
                kind: FailureKind::UnsupportedCapability,
                message: format!("comfyui custom node package '{package}' is not installed"),
                retry_hint: false,
            });
        };
        if installed_version == "unknown" || installed_version != required_version {
            return Err(BackendError {
                kind: FailureKind::UnsupportedCapability,
                message: format!(
                    "comfyui custom node package '{package}' requires version {required_version}, found {installed_version}"
                ),
                retry_hint: false,
            });
        }
    }
    Ok(())
}

/// Extracts the custom-node package (folder) name from a node's
/// `python_module` value, e.g. `"custom_nodes.ComfyUI-Impact-Pack.nodes"` ->
/// `"ComfyUI-Impact-Pack"`. Core nodes report `"nodes"` and yield `None`.
fn package_name_from_python_module(python_module: &str) -> Option<&str> {
    let rest = python_module.strip_prefix("custom_nodes.")?;
    Some(rest.split('.').next().unwrap_or(rest))
}

/// Applies the Generation's opaque `parameters` and shared `seed` onto the
/// graph per the module-level injection contract.
fn apply_parameters(
    graph: &mut Map<String, Value>,
    parameters: &Value,
    seed: Option<u64>,
) -> Result<(), BackendError> {
    let Value::Object(overrides) = parameters else {
        return Err(BackendError {
            kind: FailureKind::InvalidInput,
            message: "comfyui parameters must be a JSON object".to_string(),
            retry_hint: false,
        });
    };

    if let Some(seed_value) = seed
        && let Some(pointer) = overrides.get(SEED_POINTER_KEY).and_then(Value::as_str)
    {
        apply_override(graph, pointer, Value::from(seed_value))?;
    }

    for (key, value) in overrides {
        if key == SEED_POINTER_KEY {
            continue;
        }
        if key.starts_with('$') {
            let mut replaced = false;
            for graph_value in graph.values_mut() {
                replaced |= substitute_parameter_placeholder(graph_value, key, value);
            }
            if !replaced {
                return Err(BackendError {
                    kind: FailureKind::InvalidInput,
                    message: format!(
                        "comfyui parameter placeholder '{key}' does not occur in the workflow graph"
                    ),
                    retry_hint: false,
                });
            }
            continue;
        }
        apply_override(graph, key, value.clone())?;
    }
    Ok(())
}

/// Applies one `"<node_id>.<input_name>"`-addressed override to the graph.
fn apply_override(
    graph: &mut Map<String, Value>,
    pointer: &str,
    value: Value,
) -> Result<(), BackendError> {
    let Some((node_id, input_name)) = pointer.split_once('.') else {
        return Err(BackendError {
            kind: FailureKind::InvalidInput,
            message: format!(
                "malformed comfyui parameter override '{pointer}', expected '<node_id>.<input_name>'"
            ),
            retry_hint: false,
        });
    };
    let Some(node) = graph.get_mut(node_id) else {
        return Err(BackendError {
            kind: FailureKind::InvalidInput,
            message: format!("comfyui parameter override references unknown node '{node_id}'"),
            retry_hint: false,
        });
    };
    let Some(inputs) = node.get_mut("inputs").and_then(Value::as_object_mut) else {
        return Err(BackendError {
            kind: FailureKind::InvalidInput,
            message: format!("comfyui node '{node_id}' has no 'inputs' object to override"),
            retry_hint: false,
        });
    };
    inputs.insert(input_name.to_string(), value);
    Ok(())
}

/// Replaces every JSON string equal to `placeholder` with `replacement`.
fn substitute_parameter_placeholder(
    value: &mut Value,
    placeholder: &str,
    replacement: &Value,
) -> bool {
    match value {
        Value::String(text) if text == placeholder => {
            *value = replacement.clone();
            true
        }
        Value::Array(items) => {
            let mut replaced = false;
            for item in items {
                replaced |= substitute_parameter_placeholder(item, placeholder, replacement);
            }
            replaced
        }
        Value::Object(map) => {
            let mut replaced = false;
            for item in map.values_mut() {
                replaced |= substitute_parameter_placeholder(item, placeholder, replacement);
            }
            replaced
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

/// Recursively replaces any JSON string equal to an uploaded input
/// Artifact's id with the ComfyUI-relative filename it was uploaded as.
fn substitute_artifact_placeholders(value: &mut Value, uploads: &HashMap<String, String>) {
    match value {
        Value::String(text) => {
            if let Some(replacement) = uploads.get(text.as_str()) {
                text.clone_from(replacement);
            }
        }
        Value::Array(items) => {
            for item in items {
                substitute_artifact_placeholders(item, uploads);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                substitute_artifact_placeholders(item, uploads);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Classifies a `POST /prompt` `400` validation payload into the ADR 0003
/// failure taxonomy. `ComfyUI`'s validation errors are free-text, so this is a
/// best-effort heuristic over the reported details.
fn classify_prompt_validation_error(payload: &Value) -> FailureKind {
    let text = prompt_error_message(payload).to_lowercase();
    if text.contains("not in list")
        || (text.contains("model") && (text.contains("not found") || text.contains("missing")))
    {
        FailureKind::ModelUnavailable
    } else if text.contains("class_type")
        || text.contains("does not exist")
        || (text.contains("node") && (text.contains("unknown") || text.contains("not found")))
    {
        FailureKind::UnsupportedCapability
    } else {
        FailureKind::InvalidInput
    }
}

/// Extracts the most specific human-readable message from a `/prompt`
/// validation error payload.
fn prompt_error_message(payload: &Value) -> String {
    payload
        .pointer("/error/details")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            payload
                .pointer("/error/message")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| payload.to_string())
}

/// Classifies an `execution_error` WebSocket event into the ADR 0003 failure
/// taxonomy. `retry_decision` in `gpq-domain` applies the actual retry
/// policy; this only normalizes `ComfyUI`'s free-text exception into the
/// closed enum (ADR 0003: "Remote applies the authoritative retry policy
/// from the enum rather than parsing backend text").
fn classify_execution_error(exception_type: &str, exception_message: &str) -> FailureKind {
    let haystack = format!("{exception_type} {exception_message}").to_lowercase();
    if haystack.contains("out of memory") || haystack.contains("oom") {
        FailureKind::OutOfMemory
    } else if haystack.contains("model")
        && (haystack.contains("not found")
            || haystack.contains("missing")
            || haystack.contains("no such file"))
    {
        FailureKind::ModelUnavailable
    } else if haystack.contains("value not in list")
        || haystack.contains("keyerror")
        || (haystack.contains("node")
            && (haystack.contains("not found")
                || haystack.contains("unknown")
                || haystack.contains("does not exist")))
    {
        FailureKind::UnsupportedCapability
    } else {
        FailureKind::Internal
    }
}

/// Reads `history[prompt_id].outputs[node_id]` from a `GET
/// /history/{prompt_id}` response body.
fn history_output(history: &Value, prompt_id: &str, node_id: &str) -> Result<Value, BackendError> {
    history
        .get(prompt_id)
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("outputs"))
        .and_then(Value::as_object)
        .and_then(|outputs| outputs.get(node_id))
        .cloned()
        .ok_or_else(|| {
            internal_error(format!(
                "comfyui history has no outputs recorded for node '{node_id}'"
            ))
        })
}

/// Extracts the declared output file list at `output[output_name]`.
fn extract_output_entries(output: &Value, output_name: &str) -> Result<Vec<ViewRef>, BackendError> {
    let Some(entries) = output.get(output_name).and_then(Value::as_array) else {
        return Err(internal_error(format!(
            "comfyui output is missing declared output '{output_name}'"
        )));
    };
    entries
        .iter()
        .map(|entry| {
            let entry = serde_json::from_value::<ViewRef>(entry.clone()).map_err(|e| {
                internal_error(format!(
                    "malformed comfyui output entry for '{output_name}': {e}"
                ))
            })?;
            validate_view_ref(&entry)?;
            Ok(entry)
        })
        .collect()
}

/// A structurally unexpected but non-transport failure (malformed response,
/// missing field, internal invariant violation).
fn internal_error(message: impl Into<String>) -> BackendError {
    BackendError {
        kind: FailureKind::Internal,
        message: message.into(),
        retry_hint: false,
    }
}

#[cfg(test)]
mod tests {
    use gpq_domain::{ContentHash, MediaKind};
    use serde_json::json;

    use super::*;

    fn sample_graph() -> Map<String, Value> {
        let Value::Object(graph) = json!({
            "3": { "class_type": "KSampler", "inputs": { "steps": 20, "seed": 0 } },
            "5": { "class_type": "LoadImage", "inputs": { "image": "placeholder" } },
        }) else {
            panic!("sample graph literal must be a JSON object");
        };
        graph
    }

    #[test]
    fn expected_media_kind_matches_each_comfyui_modality() {
        assert_eq!(expected_media_kind(Modality::Image), Some(MediaKind::Image));
        assert_eq!(expected_media_kind(Modality::Video), Some(MediaKind::Video));
        assert_eq!(expected_media_kind(Modality::Music), Some(MediaKind::Audio));
    }

    #[test]
    fn apply_parameters_overrides_addressed_node_input() {
        let mut graph = sample_graph();
        let parameters = json!({ "3.steps": 40 });
        let Ok(()) = apply_parameters(&mut graph, &parameters, None) else {
            panic!("valid override must apply");
        };
        assert_eq!(graph["3"]["inputs"]["steps"], json!(40));
    }

    #[test]
    fn apply_parameters_rejects_unknown_node_id() {
        let mut graph = sample_graph();
        let parameters = json!({ "99.steps": 1 });
        let Err(err) = apply_parameters(&mut graph, &parameters, None) else {
            panic!("override on unknown node id must fail");
        };
        assert_eq!(err.kind, FailureKind::InvalidInput);
    }

    #[test]
    fn apply_parameters_rejects_malformed_pointer() {
        let mut graph = sample_graph();
        let parameters = json!({ "no_dot_here": 1 });
        let Err(err) = apply_parameters(&mut graph, &parameters, None) else {
            panic!("pointer without a node/input separator must fail");
        };
        assert_eq!(err.kind, FailureKind::InvalidInput);
    }

    #[test]
    fn apply_parameters_places_seed_at_reserved_pointer() {
        let mut graph = sample_graph();
        let parameters = json!({ "$seed": "3.seed" });
        let Ok(()) = apply_parameters(&mut graph, &parameters, Some(42)) else {
            panic!("seed placement must apply");
        };
        assert_eq!(graph["3"]["inputs"]["seed"], json!(42));
    }

    #[test]
    fn apply_parameters_replaces_portable_placeholders() {
        let mut graph = sample_graph();
        graph["3"]["inputs"]["prompt"] = json!("$prompt");
        graph["3"]["inputs"]["width"] = json!("$width");
        let parameters = json!({ "$prompt": "a red panda", "$width": 1024 });
        let Ok(()) = apply_parameters(&mut graph, &parameters, None) else {
            panic!("portable placeholders must apply");
        };
        assert_eq!(graph["3"]["inputs"]["prompt"], json!("a red panda"));
        assert_eq!(graph["3"]["inputs"]["width"], json!(1024));
    }

    #[test]
    fn apply_parameters_rejects_missing_portable_placeholder() {
        let mut graph = sample_graph();
        let Err(error) = apply_parameters(&mut graph, &json!({ "$prompt": "unused" }), None) else {
            panic!("missing portable placeholder must fail");
        };
        assert_eq!(error.kind, FailureKind::InvalidInput);
    }

    #[test]
    fn apply_parameters_skips_seed_without_pointer() {
        let mut graph = sample_graph();
        let before = graph.clone();
        let Ok(()) = apply_parameters(&mut graph, &json!({}), Some(7)) else {
            panic!("missing seed pointer must not be an error");
        };
        assert_eq!(graph, before);
    }

    #[test]
    fn substitute_artifact_placeholders_replaces_matching_strings_anywhere() {
        let mut graph = sample_graph();
        let mut uploads = HashMap::new();
        uploads.insert(
            "placeholder".to_string(),
            "input/uploaded_0001.png".to_string(),
        );
        for value in graph.values_mut() {
            substitute_artifact_placeholders(value, &uploads);
        }
        assert_eq!(
            graph["5"]["inputs"]["image"],
            json!("input/uploaded_0001.png")
        );
        // Unrelated strings, like class_type, are left untouched.
        assert_eq!(graph["5"]["class_type"], json!("LoadImage"));
    }

    #[test]
    fn parse_event_reads_progress() {
        let frame = json!({ "type": "progress", "data": { "value": 5, "max": 20, "prompt_id": "p1", "node": "3" } }).to_string();
        assert_eq!(
            parse_event(&frame),
            Some(ComfyEvent::Progress {
                prompt_id: "p1".to_string(),
                node: Some("3".to_string()),
                value: 5,
                max: 20
            })
        );
    }

    #[test]
    fn parse_event_reads_executing() {
        let frame = json!({ "type": "executing", "data": { "node": "3", "display_node": "3", "prompt_id": "p1" } }).to_string();
        assert_eq!(
            parse_event(&frame),
            Some(ComfyEvent::Executing {
                prompt_id: "p1".to_string(),
                node: Some("3".to_string())
            })
        );
    }

    #[test]
    fn parse_event_reads_comfy_completion_marker() {
        let frame =
            json!({ "type": "executing", "data": { "node": null, "prompt_id": "p1" } }).to_string();
        assert_eq!(
            parse_event(&frame),
            Some(ComfyEvent::Executing {
                prompt_id: "p1".to_string(),
                node: None
            })
        );
    }

    #[test]
    fn parse_event_reads_executed_with_output() {
        let frame = json!({
            "type": "executed",
            "data": { "node": "9", "display_node": "9", "output": { "images": [{"filename": "a.png"}] }, "prompt_id": "p1" }
        })
        .to_string();
        let Some(ComfyEvent::Executed {
            prompt_id,
            node,
            output: Some(output),
        }) = parse_event(&frame)
        else {
            panic!("expected an Executed event with output");
        };
        assert_eq!(prompt_id, "p1");
        assert_eq!(node, "9");
        assert_eq!(output["images"][0]["filename"], json!("a.png"));
    }

    #[test]
    fn parse_event_reads_execution_error() {
        let frame = json!({
            "type": "execution_error",
            "data": { "prompt_id": "p1", "node_id": "3", "node_type": "KSampler",
                       "exception_type": "torch.OutOfMemoryError", "exception_message": "CUDA out of memory" }
        })
        .to_string();
        assert_eq!(
            parse_event(&frame),
            Some(ComfyEvent::ExecutionError {
                prompt_id: "p1".to_string(),
                exception_type: "torch.OutOfMemoryError".to_string(),
                exception_message: "CUDA out of memory".to_string(),
            })
        );
    }

    #[test]
    fn parse_event_ignores_unknown_type() {
        let frame = json!({ "type": "progress_state", "data": {} }).to_string();
        assert_eq!(parse_event(&frame), Some(ComfyEvent::Other));
    }

    #[test]
    fn parse_event_rejects_malformed_frame() {
        assert_eq!(parse_event("not json"), None);
        assert_eq!(parse_event(&json!({ "data": {} }).to_string()), None);
    }

    #[test]
    fn classify_execution_error_maps_out_of_memory() {
        assert_eq!(
            classify_execution_error(
                "RuntimeError",
                "CUDA out of memory. Tried to allocate 2 GiB"
            ),
            FailureKind::OutOfMemory
        );
    }

    #[test]
    fn classify_execution_error_maps_missing_model() {
        assert_eq!(
            classify_execution_error(
                "FileNotFoundError",
                "Model file not found: sdxl.safetensors"
            ),
            FailureKind::ModelUnavailable
        );
    }

    #[test]
    fn classify_execution_error_maps_missing_node() {
        assert_eq!(
            classify_execution_error(
                "InvalidNodeError",
                "Node type ImpactWildcard does not exist"
            ),
            FailureKind::UnsupportedCapability
        );
    }

    #[test]
    fn classify_execution_error_maps_value_not_in_list() {
        assert_eq!(
            classify_execution_error("ValueError", "Value not in list: ckpt_name"),
            FailureKind::UnsupportedCapability
        );
    }

    #[test]
    fn classify_execution_error_defaults_to_internal() {
        assert_eq!(
            classify_execution_error("ZeroDivisionError", "division by zero"),
            FailureKind::Internal
        );
    }

    #[test]
    fn classify_prompt_validation_error_maps_missing_class() {
        let payload = json!({ "error": { "type": "prompt_outputs_failed_validation", "details": "Node class_type ImpactWildcard does not exist" } });
        assert_eq!(
            classify_prompt_validation_error(&payload),
            FailureKind::UnsupportedCapability
        );
    }

    #[test]
    fn classify_prompt_validation_error_maps_missing_value_in_list() {
        let payload = json!({ "error": { "type": "prompt_outputs_failed_validation", "details": "ckpt_name: value not in list" } });
        assert_eq!(
            classify_prompt_validation_error(&payload),
            FailureKind::ModelUnavailable
        );
    }

    #[test]
    fn classify_prompt_validation_error_defaults_to_invalid_input() {
        let payload = json!({ "error": { "type": "no_prompt", "message": "no prompt" } });
        assert_eq!(
            classify_prompt_validation_error(&payload),
            FailureKind::InvalidInput
        );
    }

    #[test]
    fn package_name_from_python_module_extracts_custom_node_package() {
        assert_eq!(
            package_name_from_python_module("custom_nodes.ComfyUI-Impact-Pack.nodes"),
            Some("ComfyUI-Impact-Pack")
        );
        assert_eq!(package_name_from_python_module("nodes"), None);
    }

    #[test]
    fn custom_node_versions_prefers_known_package_version() {
        let mut object_info = BTreeMap::new();
        object_info.insert(
            "ImpactWildcard".to_string(),
            ObjectInfoEntry {
                python_module: "custom_nodes.ComfyUI-Impact-Pack.nodes".to_string(),
            },
        );
        object_info.insert(
            "KSampler".to_string(),
            ObjectInfoEntry {
                python_module: "nodes".to_string(),
            },
        );
        let mut package_versions = BTreeMap::new();
        package_versions.insert("ComfyUI-Impact-Pack".to_string(), "7.1.0".to_string());

        let nodes = custom_node_versions(&object_info, &package_versions, &BTreeMap::new());
        assert_eq!(nodes.get("ComfyUI-Impact-Pack"), Some(&"7.1.0".to_string()));
        assert!(!nodes.contains_key("KSampler"));
    }

    #[test]
    fn custom_node_versions_falls_back_to_unknown() {
        let mut object_info = BTreeMap::new();
        object_info.insert(
            "Foo".to_string(),
            ObjectInfoEntry {
                python_module: "custom_nodes.SomePack.nodes".to_string(),
            },
        );
        let nodes = custom_node_versions(&object_info, &BTreeMap::new(), &BTreeMap::new());
        assert_eq!(nodes.get("SomePack"), Some(&"unknown".to_string()));
    }

    #[test]
    fn custom_node_versions_prefer_operator_configuration() {
        let object_info = BTreeMap::from([(
            "Foo".to_string(),
            ObjectInfoEntry {
                python_module: "custom_nodes.SomePack.nodes".to_string(),
            },
        )]);
        let package_versions = BTreeMap::from([(String::from("SomePack"), String::from("1.0.0"))]);
        let configured_versions =
            BTreeMap::from([(String::from("SomePack"), String::from("2.0.0"))]);
        let nodes = custom_node_versions(&object_info, &package_versions, &configured_versions);
        assert_eq!(nodes.get("SomePack"), Some(&"2.0.0".to_string()));
    }

    /// `GET /system_stats` as emitted by upstream `ComfyUI` 0.34: the
    /// `comfy_package_versions` field is an array of package entries, and an
    /// entry whose installed version is unresolvable carries `null`.
    #[test]
    fn system_stats_parses_upstream_package_version_entries() {
        let raw = json!({
            "system": {
                "os": "darwin",
                "comfyui_version": "0.34.2",
                "comfy_package_versions": [
                    {"name": "comfyui-frontend-package", "installed": "1.49.6", "required": "1.49.6"},
                    {"name": "comfyui-embedded-docs", "installed": null, "required": "0.4.0"}
                ],
                "python_version": "3.13.12"
            },
            "devices": [{"name": "mps", "type": "mps", "index": null, "vram_total": 64, "vram_free": 32}]
        });
        let Ok(stats) = serde_json::from_value::<SystemStatsResponse>(raw) else {
            panic!("upstream system_stats shape must deserialize");
        };
        assert_eq!(stats.system.comfyui_version, "0.34.2");
        assert_eq!(
            stats.system.package_versions(),
            BTreeMap::from([("comfyui-frontend-package".to_string(), "1.49.6".to_string())])
        );
        assert_eq!(stats.devices[0].vram_free, Some(32));
    }

    #[test]
    fn extract_output_entries_reads_declared_images() {
        let output =
            json!({ "images": [{ "filename": "a.png", "subfolder": "", "type": "output" }] });
        let Ok(entries) = extract_output_entries(&output, "images") else {
            panic!("declared output must parse");
        };
        assert_eq!(
            entries,
            vec![ViewRef {
                filename: "a.png".to_string(),
                subfolder: String::new(),
                kind: "output".to_string()
            }]
        );
    }

    #[test]
    fn extract_output_entries_rejects_missing_output_name() {
        let output = json!({ "other": [] });
        let Err(err) = extract_output_entries(&output, "images") else {
            panic!("missing declared output name must fail");
        };
        assert_eq!(err.kind, FailureKind::Internal);
    }

    #[test]
    fn history_output_reads_nested_pointer() {
        let history = json!({ "p1": { "outputs": { "9": { "images": [] } } } });
        let Ok(output) = history_output(&history, "p1", "9") else {
            panic!("nested pointer must resolve");
        };
        assert_eq!(output, json!({ "images": [] }));
    }

    #[test]
    fn history_output_reads_opaque_node_ids_without_pointer_parsing() {
        let history = json!({
            "p1": { "outputs": { "node/one~two": { "images": [] } } }
        });
        let Ok(output) = history_output(&history, "p1", "node/one~two") else {
            panic!("opaque node ids must be looked up as object keys");
        };
        assert_eq!(output, json!({ "images": [] }));
    }

    #[test]
    fn custom_node_requirements_require_the_exact_version() {
        let required = BTreeMap::from([(String::from("SomePack"), String::from("1.0.0"))]);
        let installed = BTreeMap::from([(String::from("SomePack"), String::from("2.0.0"))]);
        let Err(error) = validate_custom_node_requirements(&required, &installed) else {
            panic!("a mismatched custom-node version must be rejected");
        };
        assert_eq!(error.kind, FailureKind::UnsupportedCapability);
    }

    #[test]
    fn unknown_custom_node_version_does_not_satisfy_an_exact_pin() {
        let required = BTreeMap::from([(String::from("SomePack"), String::from("unknown"))]);
        let installed = BTreeMap::from([(String::from("SomePack"), String::from("unknown"))]);
        let Err(error) = validate_custom_node_requirements(&required, &installed) else {
            panic!("an unknown custom-node version must be rejected");
        };
        assert_eq!(error.kind, FailureKind::UnsupportedCapability);
    }

    #[test]
    fn registered_output_entries_reject_path_traversal() {
        let output = json!({
            "images": [{ "filename": "nested/../../outside.png", "type": "output" }]
        });
        let Err(error) = extract_output_entries(&output, "images") else {
            panic!("output filenames must be basenames");
        };
        assert_eq!(error.kind, FailureKind::Internal);
    }

    #[test]
    fn history_output_rejects_missing_node() {
        let history = json!({ "p1": { "outputs": {} } });
        let Err(err) = history_output(&history, "p1", "9") else {
            panic!("missing node outputs must fail");
        };
        assert_eq!(err.kind, FailureKind::Internal);
    }

    #[test]
    fn revalidate_manifest_shape_uses_domain_types() {
        // Compile-time/shape sanity check that the manifest fields this
        // module reads line up with `gpq_domain::WorkflowManifest`.
        let manifest = WorkflowManifest {
            output_node: "9".to_string(),
            output_name: "images".to_string(),
            artifact_kind: MediaKind::Image,
            artifact_mime: "image/png".to_string(),
            required_models: vec![ContentHash::digest(b"model")],
            required_custom_nodes: BTreeMap::from([(
                "ComfyUI-Impact-Pack".to_string(),
                "7.1.0".to_string(),
            )]),
        };
        assert_eq!(manifest.output_node, "9");
        assert!(
            manifest
                .required_custom_nodes
                .contains_key("ComfyUI-Impact-Pack")
        );
    }

    #[tokio::test]
    async fn revalidate_uses_workflow_manifest_model_pins() {
        let Ok(temp_dir) = tempfile::tempdir() else {
            panic!("temporary directory must be available");
        };
        let model_path = temp_dir.path().join("model.safetensors");
        if let Err(error) = std::fs::write(&model_path, b"model-bytes") {
            panic!("model fixture must be writable: {error}");
        }
        let Ok(base_url) = Url::parse("http://127.0.0.1:8188") else {
            panic!("fixture URL must parse");
        };
        let pool = PoolConfig {
            key: "pool".to_owned(),
            backend: BackendKind::ComfyUi,
            executable: PathBuf::from("/bin/true"),
            args: Vec::new(),
            env: BTreeMap::new(),
            state_dir: temp_dir.path().to_path_buf(),
            startup_timeout: Duration::from_secs(1),
            base_url,
            slots: Some(1),
            model_paths: vec![model_path],
            expected_hashes: BTreeMap::new(),
            custom_node_versions: BTreeMap::new(),
        };
        let backend = ComfyBackend::new(&pool);
        let expected_hash = hash_model_fresh(&pool.model_paths[0])
            .unwrap_or_else(|error| panic!("model fixture must hash: {error}"));
        let manifest = WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "images".to_owned(),
            artifact_kind: MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![expected_hash],
            required_custom_nodes: BTreeMap::new(),
        };
        let request = ExecutionRequest {
            attempt_id: "attempt".to_owned(),
            modality: Modality::Image,
            model_sha256: None,
            model_path: None,
            comfy_prompt: None,
            workflow_graph: Some(json!({
                "1": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": "model.safetensors"}}
            })),
            workflow_manifest: Some(manifest.clone()),
            parameters: json!({}),
            inputs: Vec::new(),
            seed: None,
            stream_tokens: false,
            deadline: Duration::from_secs(1),
        };

        assert!(
            backend.revalidate(&manifest, &request).await.is_ok(),
            "workflow model hashes must come from the workflow manifest"
        );
    }

    #[tokio::test]
    async fn revalidate_checks_every_checkpoint_model_pin() {
        let Ok(temp_dir) = tempfile::tempdir() else {
            panic!("temporary directory must be available");
        };
        let first_path = temp_dir.path().join("first.safetensors");
        let second_path = temp_dir.path().join("second.safetensors");
        if let Err(error) = std::fs::write(&first_path, b"first-model") {
            panic!("first model fixture must be writable: {error}");
        }
        if let Err(error) = std::fs::write(&second_path, b"second-model") {
            panic!("second model fixture must be writable: {error}");
        }
        let Ok(base_url) = Url::parse("http://127.0.0.1:8188") else {
            panic!("fixture URL must parse");
        };
        let pool = PoolConfig {
            key: "pool".to_owned(),
            backend: BackendKind::ComfyUi,
            executable: PathBuf::from("/bin/true"),
            args: Vec::new(),
            env: BTreeMap::new(),
            state_dir: temp_dir.path().to_path_buf(),
            startup_timeout: Duration::from_secs(1),
            base_url,
            slots: Some(1),
            model_paths: vec![first_path.clone(), second_path.clone()],
            expected_hashes: BTreeMap::new(),
            custom_node_versions: BTreeMap::new(),
        };
        let backend = ComfyBackend::new(&pool);
        let first_hash = hash_model_fresh(&first_path)
            .unwrap_or_else(|error| panic!("first model fixture must hash: {error}"));
        let second_expected_hash = ContentHash::digest(b"different-second-model");
        let manifest = WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "images".to_owned(),
            artifact_kind: MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![first_hash, second_expected_hash],
            required_custom_nodes: BTreeMap::new(),
        };
        let request = ExecutionRequest {
            attempt_id: "attempt".to_owned(),
            modality: Modality::Image,
            model_sha256: None,
            model_path: None,
            comfy_prompt: None,
            workflow_graph: Some(json!({
                "1": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": "first.safetensors"}},
                "2": {"class_type": "CheckpointLoaderSimple", "inputs": {"ckpt_name": "second.safetensors"}}
            })),
            workflow_manifest: Some(manifest.clone()),
            parameters: json!({}),
            inputs: Vec::new(),
            seed: None,
            stream_tokens: false,
            deadline: Duration::from_secs(1),
        };

        let Err(error) = backend.revalidate(&manifest, &request).await else {
            panic!("a mismatched second checkpoint pin must be rejected");
        };
        assert_eq!(error.kind, FailureKind::ModelUnavailable);
    }

    #[test]
    fn checkpoint_names_read_every_ckpt_name_input() {
        let graph = json!({
            "4": { "class_type": "CheckpointLoaderSimple", "inputs": { "ckpt_name": "sdxl/base.safetensors" } },
            "5": { "class_type": "CheckpointLoaderSimple", "inputs": { "ckpt_name": "clip.safetensors" } },
        });
        assert_eq!(
            checkpoint_names(&graph),
            vec!["sdxl/base.safetensors", "clip.safetensors"]
        );
    }

    #[test]
    fn checkpoint_name_absent_without_a_loader_node() {
        let graph = json!({
            "5": { "class_type": "KSampler", "inputs": { "steps": 20 } },
        });
        assert!(checkpoint_names(&graph).is_empty());
    }

    #[test]
    fn resolve_checkpoint_path_matches_trailing_components() {
        let model_paths = vec![
            PathBuf::from("/models/checkpoints/sdxl/base.safetensors"),
            PathBuf::from("/models/checkpoints/other.safetensors"),
        ];
        assert_eq!(
            resolve_checkpoint_path(&model_paths, "sdxl/base.safetensors"),
            Some(Path::new("/models/checkpoints/sdxl/base.safetensors"))
        );
        assert_eq!(
            resolve_checkpoint_path(&model_paths, "sdxl\\base.safetensors"),
            Some(Path::new("/models/checkpoints/sdxl/base.safetensors"))
        );
    }

    #[test]
    fn resolve_checkpoint_path_rejects_unconfigured_name() {
        let model_paths = vec![PathBuf::from("/models/checkpoints/other.safetensors")];
        assert_eq!(
            resolve_checkpoint_path(&model_paths, "missing.safetensors"),
            None
        );
    }

    #[test]
    fn raw_prompt_parser_preserves_large_integer_values() {
        let payload = json!({
            "prompt": {
                "node": {
                    "class_type": "RawNode",
                    "inputs": {"seed": 18_446_744_073_709_551_615_u64, "link": ["other", 0]}
                }
            },
            "extra_data": {}
        });
        let parsed =
            parse_raw_prompt(&payload).unwrap_or_else(|error| panic!("valid raw prompt: {error}"));
        assert_eq!(
            parsed.graph["node"]["inputs"]["seed"],
            json!(18_446_744_073_709_551_615_u64)
        );
        assert_eq!(parsed.graph["node"]["inputs"]["link"][1], json!(0));
    }

    #[test]
    fn raw_output_plan_deduplicates_and_escapes_pointers() {
        let outputs = json!({
            "node/one~two": {
                "images": [
                    {"filename": "same.png"},
                    {"filename": "same.png"}
                ]
            }
        });
        let plans =
            plan_raw_outputs(&outputs).unwrap_or_else(|error| panic!("valid outputs: {error}"));
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].pointers,
            [
                "/node~1one~0two/images/0".to_owned(),
                "/node~1one~0two/images/1".to_owned()
            ]
        );
    }

    #[test]
    fn raw_output_plan_rejects_path_traversal() {
        let error = plan_raw_outputs(&json!({"node": [{"filename": "../secret"}]}))
            .err()
            .unwrap_or_else(|| panic!("path traversal must be rejected"));
        assert_eq!(error.kind, FailureKind::Internal);
    }
}
