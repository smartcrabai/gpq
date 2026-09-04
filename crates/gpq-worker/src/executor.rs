//! Executes one Attempt end to end on a Worker (ADR 0003, ADR 0005, ADR 0008).
//!
//! [`execute`] fetches and verifies input Artifacts, revalidates that the
//! Pool still has the pinned Model/Workflow capability before touching a
//! backend, runs the backend while forwarding progress/tokens to the
//! control session, enforces the lease's execution timeout, publishes
//! outputs, and reports exactly one terminal outcome
//! (`AttemptResult`/`AttemptFailure`/`CancelAcknowledged`). The Execution
//! Slot and the Attempt's scratch directory are released on every exit path.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use buffa::{EnumValue, MessageField};
use buffa_types::google::protobuf::Timestamp;
use chrono::Utc;
use futures::StreamExt;
use gpq_domain::{ArtifactManifest, ContentHash, FailureKind, Hasher, ManifestMismatch, Modality};
use gpq_proto::gpq::v1::{self as v1, ArtifactPlacement as ProtoArtifactPlacement};
use gpq_proto::gpq::worker::v1::{
    ArtifactChunk, AttemptFailure, AttemptOutput, AttemptProgress, AttemptResult, AttemptRunning,
    AttemptTokenDelta, CancelAcknowledged, FetchArtifactRequest, LeaseAssignment, LeaseInput,
    WorkerMessage, worker_message,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;

use crate::artifacts::LocalArtifactStore;
use crate::backend::{Backend, ExecutionEvent, ExecutionRequest, InputArtifact};
use crate::pool::{PoolAdvertisementData, PoolSupervisor, SlotLease};

/// Concrete gRPC transport shared by every client the Worker holds against
/// Remote (ADR 0004): a cheap-to-clone, backpressured HTTP/2 connection.
pub type Transport = connectrpc::client::SharedHttp2Connection;

/// Everything one spawned Attempt execution needs, independent of the
/// control session that spawned it.
pub struct ExecutionContext {
    /// Identity of the Attempt being executed.
    pub attempt_id: gpq_domain::AttemptId,
    /// The lease Remote assigned, carrying everything needed to execute it.
    pub lease: LeaseAssignment,
    /// The Device Pool key (`PoolAdvertisementData::pool_key`) this Attempt
    /// runs on.
    pub pool_key: String,
    /// The Execution Slot reserved for this Attempt; releases automatically
    /// on drop, at every exit path from [`execute`].
    #[expect(
        dead_code,
        reason = "held only for its Drop impl, which releases the Execution Slot back to the pool when ExecutionContext drops; the field itself is never read"
    )]
    pub slot: SlotLease,
    /// Handle to the backend adapter to run the Attempt on.
    pub backend: Arc<dyn Backend>,
    /// The Worker's Device Pool supervisor, for capability revalidation and
    /// model-path resolution.
    pub pools: Arc<PoolSupervisor>,
    /// Sink for `WorkerMessage`s destined for the control session's outbound
    /// stream.
    pub outbound: mpsc::Sender<WorkerMessage>,
    /// Client for `WorkerTransferService`, used to fetch input Artifacts
    /// that have no presigned download URL.
    pub transfer: gpq_proto::gpq::worker::v1::WorkerTransferServiceClient<Transport>,
    /// Shared HTTP client for presigned S3 GET/PUT.
    pub http: reqwest::Client,
    /// Worker-local output store.
    pub artifacts: Arc<LocalArtifactStore>,
    /// Scratch directory for this Attempt's inputs and outputs; removed on
    /// every exit path.
    pub attempt_dir: PathBuf,
    /// Cooperative cancellation, set by the control session on
    /// `CancelRequest` or lease expiry (ADR 0003).
    pub cancel: CancellationToken,
}

/// Removes the Attempt's scratch directory on every exit path.
struct DirGuard(PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Why [`run`] stopped short of a normal [`ExecutionOutcome`].
enum Stop {
    /// Cooperative cancellation completed (`CancelRequest` or lease
    /// expiry); Remote wants `CancelAcknowledged`, not a failure.
    Cancelled,
    /// A normalized, reportable failure.
    Failed {
        kind: FailureKind,
        message: String,
        retry_hint: bool,
    },
}

/// The pieces of a successful Attempt, assembled into `AttemptResult`.
struct ExecutionOutcome {
    output_text: String,
    outputs: Vec<AttemptOutput>,
    usage: Option<v1::Usage>,
}

/// Executes `ctx.lease` and reports exactly one terminal `WorkerMessage`.
///
/// The Execution Slot (`ctx.slot`) and the Attempt's scratch directory are
/// released unconditionally when this function returns, on every path.
pub async fn execute(ctx: ExecutionContext) {
    let _cleanup = DirGuard(ctx.attempt_dir.clone());
    if let Err(err) = tokio::fs::create_dir_all(&ctx.attempt_dir).await {
        report_failure(
            &ctx,
            FailureKind::Internal,
            format!("cannot create attempt directory: {err}"),
            true,
        )
        .await;
        return;
    }
    match run(&ctx).await {
        Ok(outcome) => report_success(&ctx, outcome).await,
        Err(Stop::Cancelled) => report_cancel_acknowledged(&ctx).await,
        Err(Stop::Failed {
            kind,
            message,
            retry_hint,
        }) => report_failure(&ctx, kind, message, retry_hint).await,
    }
}

async fn run(ctx: &ExecutionContext) -> Result<ExecutionOutcome, Stop> {
    let modality = modality_from_proto(ctx.lease.modality).map_err(internal)?;
    let inputs = fetch_inputs(ctx).await?;

    let model_hash = if ctx.lease.model_sha256.is_empty() {
        None
    } else {
        Some(
            ContentHash::from_str(&ctx.lease.model_sha256)
                .map_err(|err| internal(format!("invalid model hash on lease: {err}")))?,
        )
    };
    let workflow_manifest = ctx
        .lease
        .workflow_manifest
        .clone()
        .into_option()
        .map(|manifest| workflow_manifest_from_proto(&manifest))
        .transpose()
        .map_err(internal)?;

    let pools = ctx.pools.capabilities();
    let Some(pool) = pools.iter().find(|pool| pool.pool_key == ctx.pool_key) else {
        return Err(Stop::Failed {
            kind: FailureKind::Internal,
            message: format!("pool {} is no longer advertised", ctx.pool_key),
            retry_hint: true,
        });
    };
    revalidate_capabilities(pool, modality, model_hash, workflow_manifest.as_ref()).map_err(
        |(kind, message)| Stop::Failed {
            kind,
            message,
            retry_hint: false,
        },
    )?;

    let model_path = if modality == Modality::Llm {
        model_hash.and_then(|hash| ctx.pools.resolve_model_path(&ctx.pool_key, hash))
    } else {
        None
    };

    let workflow_graph = ctx
        .lease
        .workflow_graph
        .clone()
        .into_option()
        .map(|graph| struct_to_json(&graph))
        .transpose()
        .map_err(internal)?;
    let parameters = ctx
        .lease
        .parameters
        .clone()
        .into_option()
        .map(|params| struct_to_json(&params))
        .transpose()
        .map_err(internal)?
        .unwrap_or_else(|| serde_json::json!({}));

    let exec_timeout =
        execution_timeout_duration(ctx.lease.execution_timeout.clone().into_option(), modality);

    let request = ExecutionRequest {
        attempt_id: ctx.attempt_id.to_string(),
        modality,
        model_sha256: model_hash,
        model_path,
        workflow_graph,
        workflow_manifest,
        parameters,
        inputs,
        seed: Some(ctx.lease.seed),
        stream_tokens: ctx.lease.stream_tokens,
        deadline: exec_timeout,
    };

    send(
        ctx,
        AttemptRunning {
            attempt_id: ctx.attempt_id.to_string(),
            ..Default::default()
        }
        .into(),
    )
    .await;

    run_backend(ctx, request, exec_timeout).await
}

/// The Attempt's execution deadline duration, taken from the leased value
/// when present and parseable, or the modality's default otherwise (ADR
/// 0003): the timeout clock starts the moment execution actually begins, at
/// the same point `AttemptRunning` is reported. Pure so the fallback is
/// unit-testable without running a backend.
fn execution_timeout_duration(
    lease_execution_timeout: Option<buffa_types::google::protobuf::Duration>,
    modality: Modality,
) -> std::time::Duration {
    lease_execution_timeout
        .and_then(|duration| std::time::Duration::try_from(duration).ok())
        .unwrap_or_else(|| modality.default_execution_timeout())
}

/// Upper bound the executor waits, after cancelling on execution timeout,
/// for the backend's own cooperative-cancellation handshake (e.g.
/// `ComfyBackend`'s `/interrupt` grace timer in `backend/comfy.rs`) to
/// settle `execute_fut` before force-reclaiming memory (ADR 0003:
/// "cooperative cancellation followed by backend restart when needed").
/// Kept above every backend's own grace window so a well-behaved backend
/// always gets to finish unwinding on its own first; this constant is only
/// the backstop for a backend that never observes cancellation.
const TIMEOUT_RELEASE_GRACE: std::time::Duration = std::time::Duration::from_secs(45);

/// Runs the backend to completion, forwarding events and enforcing the
/// execution timeout, which starts now (ADR 0003) and is never retried.
async fn run_backend(
    ctx: &ExecutionContext,
    request: ExecutionRequest,
    exec_timeout: std::time::Duration,
) -> Result<ExecutionOutcome, Stop> {
    let (events_tx, mut events_rx) = mpsc::channel(64);
    let execute_fut = ctx.backend.execute(request, events_tx, ctx.cancel.clone());
    tokio::pin!(execute_fut);
    let sleep = tokio::time::sleep(exec_timeout);
    tokio::pin!(sleep);
    // Armed only once `sleep` fires; keeps ticking independently so the
    // loop below can still service `execute_fut` and `events_rx` while it
    // counts down.
    let release_grace = tokio::time::sleep(TIMEOUT_RELEASE_GRACE);
    tokio::pin!(release_grace);

    let mut output_text = String::new();
    let mut raw_outputs: Vec<(PathBuf, ArtifactManifest)> = Vec::new();
    let mut usage: Option<v1::Usage> = None;
    let mut timed_out = false;
    let mut release_armed = false;
    let mut released = false;
    let mut backend_result = None;

    while backend_result.is_none() {
        tokio::select! {
            maybe_event = events_rx.recv() => {
                if let Some(event) = maybe_event {
                    handle_event(ctx, event, &mut output_text, &mut raw_outputs, &mut usage).await;
                }
            }
            result = &mut execute_fut => {
                backend_result = Some(result);
            }
            () = &mut sleep, if !timed_out => {
                // Cancel and let `execute_fut` keep being polled by the
                // arm above so the backend's own cooperative-cancellation
                // handshake (e.g. ComfyUI's `/interrupt` grace timer) can
                // actually run. Never await the memory reclaim here: that
                // would stall this select! and starve `execute_fut` from
                // ever observing `cancel.cancelled()`.
                timed_out = true;
                ctx.cancel.cancel();
                release_grace
                    .as_mut()
                    .reset(tokio::time::Instant::now() + TIMEOUT_RELEASE_GRACE);
                release_armed = true;
            }
            () = &mut release_grace, if release_armed && !released => {
                // The backend still hasn't settled `execute_fut` on its
                // own after a full grace period: force the reclaim, but on
                // a detached task so this select! keeps driving
                // `execute_fut` instead of blocking on the HTTP call.
                released = true;
                let backend = Arc::clone(&ctx.backend);
                tokio::spawn(async move {
                    let _ = backend.release_memory().await;
                });
            }
        }
    }
    if timed_out && !released {
        // `execute_fut` settled within the grace period on its own;
        // reclaim memory now that nothing is left to drive.
        let _ = ctx.backend.release_memory().await;
    }
    while let Ok(event) = events_rx.try_recv() {
        handle_event(ctx, event, &mut output_text, &mut raw_outputs, &mut usage).await;
    }

    let Some(result) = backend_result else {
        return Err(internal(
            "executor loop exited without a backend result".to_owned(),
        ));
    };

    if timed_out {
        return Err(Stop::Failed {
            kind: FailureKind::ExecutionTimedOut,
            message: "attempt exceeded its execution timeout".to_owned(),
            retry_hint: false,
        });
    }
    if ctx.cancel.is_cancelled() {
        return Err(Stop::Cancelled);
    }
    if let Err(err) = result {
        if err.kind == FailureKind::Cancelled {
            return Err(Stop::Cancelled);
        }
        return Err(Stop::Failed {
            kind: err.kind,
            message: err.message,
            retry_hint: err.retry_hint,
        });
    }

    let mut outputs = Vec::with_capacity(raw_outputs.len());
    for (path, manifest) in raw_outputs {
        outputs.push(
            publish_output(ctx, &path, manifest)
                .await
                .map_err(|(kind, message)| Stop::Failed {
                    kind,
                    retry_hint: kind.is_retryable(),
                    message,
                })?,
        );
    }
    Ok(ExecutionOutcome {
        output_text,
        outputs,
        usage,
    })
}

async fn handle_event(
    ctx: &ExecutionContext,
    event: ExecutionEvent,
    output_text: &mut String,
    outputs: &mut Vec<(PathBuf, ArtifactManifest)>,
    usage: &mut Option<v1::Usage>,
) {
    match event {
        ExecutionEvent::Progress {
            fraction,
            stage,
            step,
            total_steps,
        } => {
            let progress = v1::Progress {
                fraction,
                stage,
                step,
                total_steps,
                observed_at: MessageField::some(Timestamp::from(Utc::now())),
                ..Default::default()
            };
            send(
                ctx,
                AttemptProgress {
                    attempt_id: ctx.attempt_id.to_string(),
                    progress: MessageField::some(progress),
                    ..Default::default()
                }
                .into(),
            )
            .await;
        }
        ExecutionEvent::Token { text } => {
            // ADR 0006: only forward the delta when the lease's caller asked
            // for streaming; the token still contributes to the final
            // output text either way.
            if ctx.lease.stream_tokens {
                send(
                    ctx,
                    AttemptTokenDelta {
                        attempt_id: ctx.attempt_id.to_string(),
                        text,
                        ..Default::default()
                    }
                    .into(),
                )
                .await;
            }
        }
        ExecutionEvent::Output { path, manifest } => outputs.push((path, manifest)),
        ExecutionEvent::Text { text } => *output_text = text,
        ExecutionEvent::Usage {
            prompt_tokens,
            completion_tokens,
            total_tokens,
        } => {
            *usage = Some(v1::Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                ..Default::default()
            });
        }
    }
}

async fn send(ctx: &ExecutionContext, message: worker_message::Message) {
    let _ = ctx
        .outbound
        .send(WorkerMessage {
            message: Some(message),
            ..Default::default()
        })
        .await;
}

async fn report_success(ctx: &ExecutionContext, outcome: ExecutionOutcome) {
    send(
        ctx,
        AttemptResult {
            attempt_id: ctx.attempt_id.to_string(),
            output_text: outcome.output_text,
            outputs: outcome.outputs,
            usage: outcome.usage.map(MessageField::some).unwrap_or_default(),
            ..Default::default()
        }
        .into(),
    )
    .await;
}

async fn report_failure(
    ctx: &ExecutionContext,
    kind: FailureKind,
    message: String,
    retry_hint: bool,
) {
    let failure = to_proto_failure(kind, message, retry_hint);
    send(
        ctx,
        AttemptFailure {
            attempt_id: ctx.attempt_id.to_string(),
            failure: MessageField::some(failure),
            ..Default::default()
        }
        .into(),
    )
    .await;
}

async fn report_cancel_acknowledged(ctx: &ExecutionContext) {
    send(
        ctx,
        CancelAcknowledged {
            attempt_id: ctx.attempt_id.to_string(),
            ..Default::default()
        }
        .into(),
    )
    .await;
}

fn internal(message: String) -> Stop {
    Stop::Failed {
        kind: FailureKind::Internal,
        message,
        retry_hint: false,
    }
}

/// Re-checks that the Pool's currently advertised capabilities still satisfy
/// the lease's pinned Model/Workflow Version, guarding against a race where
/// the Active Runtime changed again between `ensure_runtime` (before the
/// Slot was even acquired) and this task actually starting (ADR 0003,
/// ADR 0005). A mismatch here must not be retried on this Worker.
fn revalidate_capabilities(
    pool: &PoolAdvertisementData,
    modality: Modality,
    model_hash: Option<ContentHash>,
    workflow_manifest: Option<&gpq_domain::WorkflowManifest>,
) -> Result<(), (FailureKind, String)> {
    if modality == Modality::Llm
        && let Some(expected) = model_hash
        && pool.resident_model != Some(expected)
    {
        return Err((
            FailureKind::ModelUnavailable,
            format!(
                "pool {} no longer has model {expected} resident",
                pool.pool_key
            ),
        ));
    }
    if let Some(manifest) = workflow_manifest {
        for hash in &manifest.required_models {
            if !pool.models.contains(hash) {
                return Err((
                    FailureKind::ModelUnavailable,
                    format!("pool {} is missing required model {hash}", pool.pool_key),
                ));
            }
        }
        for (node, version) in &manifest.required_custom_nodes {
            if pool.custom_nodes.get(node) != Some(version) {
                return Err((
                    FailureKind::UnsupportedCapability,
                    format!(
                        "pool {} is missing custom node {node}@{version}",
                        pool.pool_key
                    ),
                ));
            }
        }
    }
    Ok(())
}

async fn fetch_inputs(ctx: &ExecutionContext) -> Result<Vec<InputArtifact>, Stop> {
    let mut inputs = Vec::with_capacity(ctx.lease.inputs.len());
    for lease_input in &ctx.lease.inputs {
        let manifest =
            manifest_from_proto(&lease_input.manifest).map_err(|message| Stop::Failed {
                kind: FailureKind::InvalidInput,
                message,
                retry_hint: false,
            })?;
        let path = ctx
            .attempt_dir
            .join(format!("input-{}", lease_input.artifact_id));
        download_input(ctx, lease_input, &path, &manifest)
            .await
            .map_err(|(kind, message)| Stop::Failed {
                retry_hint: kind.is_retryable(),
                kind,
                message,
            })?;
        inputs.push(InputArtifact {
            artifact_id: lease_input.artifact_id.clone(),
            path,
            manifest,
        });
    }
    Ok(inputs)
}

async fn download_input(
    ctx: &ExecutionContext,
    lease_input: &LeaseInput,
    path: &Path,
    manifest: &ArtifactManifest,
) -> Result<(), (FailureKind, String)> {
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|err| (FailureKind::Internal, err.to_string()))?;
    let mut hasher = Hasher::new();
    let mut total = 0_u64;

    if lease_input.download_url.is_empty() {
        let request = FetchArtifactRequest {
            artifact_id: lease_input.artifact_id.clone(),
            attempt_id: ctx.attempt_id.to_string(),
            offset: 0,
            ..Default::default()
        };
        let mut stream = ctx
            .transfer
            .fetch_artifact(request)
            .await
            .map_err(|err| (FailureKind::TransferFailed, err.to_string()))?;
        loop {
            let Some(chunk_msg) = stream
                .message::<ArtifactChunk>()
                .await
                .map_err(|err| (FailureKind::TransferFailed, err.to_string()))?
            else {
                break;
            };
            let chunk = chunk_msg.to_owned_message();
            hasher.update(&chunk.data);
            total += chunk.data.len() as u64;
            file.write_all(&chunk.data)
                .await
                .map_err(|err| (FailureKind::Internal, err.to_string()))?;
            if chunk.last {
                break;
            }
        }
    } else {
        let response = ctx
            .http
            .get(lease_input.download_url.as_str())
            .send()
            .await
            .map_err(|err| (FailureKind::TransferFailed, err.to_string()))?;
        let mut response = response
            .error_for_status()
            .map_err(|err| (FailureKind::TransferFailed, err.to_string()))?;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|err| (FailureKind::TransferFailed, err.to_string()))?
        {
            hasher.update(&chunk);
            total += chunk.len() as u64;
            file.write_all(&chunk)
                .await
                .map_err(|err| (FailureKind::Internal, err.to_string()))?;
        }
    }
    file.flush()
        .await
        .map_err(|err| (FailureKind::Internal, err.to_string()))?;
    verify_download(manifest, total, hasher)
        .map_err(|err| (FailureKind::TransferFailed, err.to_string()))
}

/// Verifies a fully-downloaded input's declared shape against its manifest
/// (ADR 0008). Extracted from the network path so it can be unit tested
/// without a live Worker or Remote.
fn verify_download(
    manifest: &ArtifactManifest,
    total_bytes: u64,
    hasher: Hasher,
) -> Result<(), ManifestMismatch> {
    manifest.verify(total_bytes, hasher.finish())
}

/// Which strategy publishes an output Artifact for a given placement (ADR
/// 0008): only Object Store and Worker-local placements are valid for
/// Attempt outputs; Inline Relay is input-only, and anything unset or
/// unrecognized is a protocol error the Worker must reject rather than
/// silently drop. Pure so the placement branch is unit-testable without an
/// upload or a local publish.
enum OutputPublishStrategy {
    ObjectStore,
    WorkerLocal,
}

fn output_publish_strategy(
    placement: EnumValue<ProtoArtifactPlacement>,
) -> Result<OutputPublishStrategy, (FailureKind, String)> {
    match placement {
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE) => {
            Ok(OutputPublishStrategy::ObjectStore)
        }
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL) => {
            Ok(OutputPublishStrategy::WorkerLocal)
        }
        other => Err((
            FailureKind::Internal,
            format!("unsupported output placement: {other:?}"),
        )),
    }
}

async fn publish_output(
    ctx: &ExecutionContext,
    path: &Path,
    manifest: ArtifactManifest,
) -> Result<AttemptOutput, (FailureKind, String)> {
    match output_publish_strategy(ctx.lease.output_placement)? {
        OutputPublishStrategy::ObjectStore => {
            upload_to_object_store(ctx, path, &manifest).await?;
            Ok(AttemptOutput {
                manifest: MessageField::some(manifest_to_proto(&manifest)),
                placement: EnumValue::Known(
                    ProtoArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE,
                ),
                object_key: ctx.lease.output_object_key.clone(),
                delivery_token: String::new(),
                ..Default::default()
            })
        }
        OutputPublishStrategy::WorkerLocal => {
            let handle = ctx
                .artifacts
                .publish(ctx.attempt_id, path, manifest)
                .await
                .map_err(|err| (FailureKind::Internal, err.to_string()))?;
            Ok(AttemptOutput {
                manifest: MessageField::some(manifest_to_proto(&handle.manifest)),
                placement: EnumValue::Known(
                    ProtoArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
                ),
                object_key: String::new(),
                delivery_token: handle.delivery_token(),
                ..Default::default()
            })
        }
    }
}

/// Running tally over a streamed output upload: bytes actually put on the
/// wire and their digest, computed incrementally so `upload_to_object_store`
/// never has to hold the whole output file in memory to verify it (ADR
/// 0003's multi-hour image/video/music execution budgets imply outputs that
/// can be multi-gigabyte).
#[derive(Default, Clone)]
struct UploadTally {
    bytes: u64,
    hasher: Hasher,
}

impl UploadTally {
    fn record(&mut self, chunk: &[u8]) {
        self.hasher.update(chunk);
        self.bytes += chunk.len() as u64;
    }
}

async fn upload_to_object_store(
    ctx: &ExecutionContext,
    path: &Path,
    manifest: &ArtifactManifest,
) -> Result<(), (FailureKind, String)> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|err| (FailureKind::Internal, err.to_string()))?;
    let tally = Arc::new(Mutex::new(UploadTally::default()));
    let stream_tally = Arc::clone(&tally);
    // Tees each chunk into the running hash/length tally as it is read off
    // disk and fed to the PUT body, so the digest is computed over exactly
    // what was sent without ever buffering the full file.
    let body = ReaderStream::new(file).map(move |chunk| {
        let chunk = chunk?;
        stream_tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record(&chunk);
        Ok::<_, std::io::Error>(chunk)
    });
    // The presigned PUT was signed for a fixed-length object: without an
    // explicit `Content-Length`, `wrap_stream` leaves the length unknown and
    // hyper falls back to `Transfer-Encoding: chunked`, which S3-compatible
    // stores reject. The declared size is authoritative here and the bytes
    // actually sent are verified against the manifest below.
    let response = ctx
        .http
        .put(ctx.lease.output_upload_url.as_str())
        .header(reqwest::header::CONTENT_LENGTH, manifest.size_bytes)
        .body(reqwest::Body::wrap_stream(body))
        .send()
        .await
        .map_err(|err| (FailureKind::TransferFailed, err.to_string()))?;
    response
        .error_for_status()
        .map_err(|err| (FailureKind::TransferFailed, err.to_string()))?;
    let sent = tally
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    manifest
        .verify(sent.bytes, sent.hasher.finish())
        .map_err(|err| (FailureKind::Internal, err.to_string()))
}

/// Decodes a protobuf `Struct` into JSON for the backend. `Struct` carries
/// every number as a double, so an integral value such as a `ComfyUI` link
/// output index `["1", 0]` or a `$width` parameter would otherwise reach the
/// backend as `0.0`; `ComfyUI` indexes node outputs with it and rejects a
/// float. Integral doubles that fit `i64` are restored to JSON integers.
fn struct_to_json(
    value: &buffa_types::google::protobuf::Struct,
) -> Result<serde_json::Value, String> {
    let mut json = serde_json::to_value(value)
        .map_err(|err| format!("workflow struct decode failed: {err}"))?;
    restore_integers(&mut json);
    Ok(json)
}

/// Largest magnitude a double represents exactly as an integer (2^53).
const EXACT_INTEGER_LIMIT: f64 = 9_007_199_254_740_992.0;

fn restore_integers(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Number(number) => {
            let Some(float) = number.as_f64() else {
                return;
            };
            if float.fract() == 0.0 && float.abs() <= EXACT_INTEGER_LIMIT {
                // Exact: `float` is integral and far inside the `i64` range.
                #[expect(clippy::cast_possible_truncation)]
                let integer = float as i64;
                *value = serde_json::Value::from(integer);
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(restore_integers),
        serde_json::Value::Object(map) => map.values_mut().for_each(restore_integers),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::String(_) => {}
    }
}

fn manifest_from_proto(manifest: &v1::ArtifactManifest) -> Result<ArtifactManifest, String> {
    let digest = ContentHash::from_str(&manifest.digest_sha256)
        .map_err(|err| format!("invalid artifact digest: {err}"))?;
    let kind = media_kind_from_proto(manifest.kind)?;
    Ok(ArtifactManifest {
        size_bytes: manifest.size_bytes,
        digest,
        kind,
        mime_type: manifest.mime_type.clone(),
    })
}

pub(crate) fn manifest_to_proto(manifest: &ArtifactManifest) -> v1::ArtifactManifest {
    v1::ArtifactManifest {
        size_bytes: manifest.size_bytes,
        digest_sha256: manifest.digest.to_hex(),
        kind: EnumValue::Known(media_kind_to_proto(manifest.kind)),
        mime_type: manifest.mime_type.clone(),
        ..Default::default()
    }
}

fn media_kind_from_proto(kind: EnumValue<v1::MediaKind>) -> Result<gpq_domain::MediaKind, String> {
    match kind {
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_IMAGE) => Ok(gpq_domain::MediaKind::Image),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_VIDEO) => Ok(gpq_domain::MediaKind::Video),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_AUDIO) => Ok(gpq_domain::MediaKind::Audio),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_TEXT) => Ok(gpq_domain::MediaKind::Text),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_BINARY) => Ok(gpq_domain::MediaKind::Binary),
        other => Err(format!("unknown media kind: {other:?}")),
    }
}

fn media_kind_to_proto(kind: gpq_domain::MediaKind) -> v1::MediaKind {
    match kind {
        gpq_domain::MediaKind::Image => v1::MediaKind::MEDIA_KIND_IMAGE,
        gpq_domain::MediaKind::Video => v1::MediaKind::MEDIA_KIND_VIDEO,
        gpq_domain::MediaKind::Audio => v1::MediaKind::MEDIA_KIND_AUDIO,
        gpq_domain::MediaKind::Text => v1::MediaKind::MEDIA_KIND_TEXT,
        gpq_domain::MediaKind::Binary => v1::MediaKind::MEDIA_KIND_BINARY,
    }
}

/// Converts the proto `Modality` on a lease to the domain type. Remote
/// derives modality after alias resolution, so a lease always carries a
/// concrete, known value (ADR 0006); anything else is a protocol error.
pub(crate) fn modality_from_proto(modality: EnumValue<v1::Modality>) -> Result<Modality, String> {
    match modality {
        EnumValue::Known(v1::Modality::MODALITY_LLM) => Ok(Modality::Llm),
        EnumValue::Known(v1::Modality::MODALITY_IMAGE) => Ok(Modality::Image),
        EnumValue::Known(v1::Modality::MODALITY_VIDEO) => Ok(Modality::Video),
        EnumValue::Known(v1::Modality::MODALITY_MUSIC) => Ok(Modality::Music),
        other => Err(format!("unknown modality: {other:?}")),
    }
}

/// Converts a domain `BackendKind` to its proto representation, used when
/// advertising Pool capabilities (ADR 0005).
pub(crate) fn backend_kind_to_proto(kind: gpq_domain::BackendKind) -> v1::BackendKind {
    match kind {
        gpq_domain::BackendKind::LlamaCpp => v1::BackendKind::BACKEND_KIND_LLAMA_CPP,
        gpq_domain::BackendKind::MlxDspark => v1::BackendKind::BACKEND_KIND_MLX_DSPARK,
        gpq_domain::BackendKind::ComfyUi => v1::BackendKind::BACKEND_KIND_COMFYUI,
    }
}

/// Converts a domain `FailureKind` to its proto representation. `WorkerLost`
/// and `LeaseExpired` are Remote-originated (a Worker never classifies its
/// own failure that way) and fall back to `Internal` defensively.
fn failure_kind_to_proto(kind: FailureKind) -> v1::FailureKind {
    match kind {
        FailureKind::InvalidInput => v1::FailureKind::FAILURE_KIND_INVALID_INPUT,
        FailureKind::UnsupportedCapability => v1::FailureKind::FAILURE_KIND_UNSUPPORTED_CAPABILITY,
        FailureKind::ModelUnavailable => v1::FailureKind::FAILURE_KIND_MODEL_UNAVAILABLE,
        FailureKind::OutOfMemory => v1::FailureKind::FAILURE_KIND_OUT_OF_MEMORY,
        FailureKind::BackendCrashed => v1::FailureKind::FAILURE_KIND_BACKEND_CRASHED,
        FailureKind::ExecutionTimedOut => v1::FailureKind::FAILURE_KIND_EXECUTION_TIMED_OUT,
        FailureKind::Cancelled => v1::FailureKind::FAILURE_KIND_CANCELLED,
        FailureKind::TransferFailed => v1::FailureKind::FAILURE_KIND_TRANSFER_FAILED,
        FailureKind::Internal | FailureKind::WorkerLost | FailureKind::LeaseExpired => {
            v1::FailureKind::FAILURE_KIND_INTERNAL
        }
    }
}

/// Builds a wire `Failure` from a normalized kind, diagnostic message, and
/// retry hint (ADR 0003). Shared by `AttemptFailure`, `LeaseRejected`, and
/// this module's own `AttemptFailure` reporting.
pub(crate) fn to_proto_failure(
    kind: FailureKind,
    message: String,
    retry_hint: bool,
) -> v1::Failure {
    v1::Failure {
        kind: EnumValue::Known(failure_kind_to_proto(kind)),
        message,
        worker_retry_hint: retry_hint,
        ..Default::default()
    }
}

fn workflow_manifest_from_proto(
    manifest: &v1::WorkflowManifest,
) -> Result<gpq_domain::WorkflowManifest, String> {
    let artifact_kind = media_kind_from_proto(manifest.artifact_kind)?;
    let mut required_models = Vec::with_capacity(manifest.required_model_sha256.len());
    for hash in &manifest.required_model_sha256 {
        required_models.push(
            ContentHash::from_str(hash)
                .map_err(|err| format!("invalid required model hash: {err}"))?,
        );
    }
    Ok(gpq_domain::WorkflowManifest {
        output_node: manifest.output_node.clone(),
        output_name: manifest.output_name.clone(),
        artifact_kind,
        artifact_mime: manifest.artifact_mime.clone(),
        required_models,
        required_custom_nodes: manifest
            .required_custom_nodes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A protobuf `Struct` round trip turns every number into a double; the
    /// backend must still see `ComfyUI` link indices and integer inputs as
    /// JSON integers while genuine fractions stay floats.
    #[test]
    fn struct_to_json_restores_integral_numbers() {
        let wire = serde_json::json!({
            "9": { "class_type": "SaveImage", "inputs": { "images": ["1", 0] } },
            "1": { "class_type": "EmptyImage", "inputs": { "width": 640, "color": 14_709_792 } },
            "3": { "class_type": "KSampler", "inputs": { "cfg": 7.5, "denoise": 1.0, "seed": -1 } },
        });
        let Ok(graph) = serde_json::from_value::<buffa_types::google::protobuf::Struct>(wire)
        else {
            panic!("graph literal must be a Struct");
        };
        let Ok(json) = struct_to_json(&graph) else {
            panic!("struct must decode");
        };
        assert_eq!(json["9"]["inputs"]["images"][1], serde_json::json!(0));
        assert!(json["9"]["inputs"]["images"][1].is_i64());
        assert!(json["1"]["inputs"]["width"].is_i64());
        assert!(json["1"]["inputs"]["color"].is_i64());
        assert_eq!(json["3"]["inputs"]["seed"], serde_json::json!(-1));
        assert_eq!(json["3"]["inputs"]["cfg"], serde_json::json!(7.5));
        assert!(json["3"]["inputs"]["denoise"].is_i64());
        assert_eq!(json["9"]["inputs"]["images"].to_string(), r#"["1",0]"#);
    }

    #[test]
    fn download_verification_accepts_matching_bytes() {
        let manifest = ArtifactManifest {
            size_bytes: 3,
            digest: ContentHash::digest(b"abc"),
            kind: gpq_domain::MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        };
        let mut hasher = Hasher::new();
        hasher.update(b"abc");
        assert_eq!(verify_download(&manifest, 3, hasher), Ok(()));
    }

    #[test]
    fn download_verification_rejects_size_mismatch() {
        let manifest = ArtifactManifest {
            size_bytes: 3,
            digest: ContentHash::digest(b"abc"),
            kind: gpq_domain::MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        };
        let mut hasher = Hasher::new();
        hasher.update(b"ab");
        assert_eq!(
            verify_download(&manifest, 2, hasher),
            Err(ManifestMismatch::Size {
                declared: 3,
                received: 2
            })
        );
    }

    #[test]
    fn download_verification_rejects_digest_mismatch() {
        let manifest = ArtifactManifest {
            size_bytes: 3,
            digest: ContentHash::digest(b"abc"),
            kind: gpq_domain::MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        };
        let mut hasher = Hasher::new();
        hasher.update(b"xyz");
        assert!(matches!(
            verify_download(&manifest, 3, hasher),
            Err(ManifestMismatch::Digest { .. })
        ));
    }

    #[test]
    fn revalidate_rejects_llm_when_resident_model_changed() {
        let pool = PoolAdvertisementData {
            pool_key: "gpu0".to_owned(),
            backend: gpq_domain::BackendKind::LlamaCpp,
            backend_version: "1.0".to_owned(),
            ready: true,
            unready_reason: None,
            slots_total: 4,
            slots_busy: Vec::new(),
            resident_model: Some(ContentHash::digest(b"other-model")),
            accelerator_memory_bytes: None,
            models: Vec::new(),
            custom_nodes: std::collections::BTreeMap::new(),
            probes: std::collections::BTreeMap::new(),
        };
        let requested = ContentHash::digest(b"requested-model");
        let result = revalidate_capabilities(&pool, Modality::Llm, Some(requested), None);
        assert!(matches!(result, Err((FailureKind::ModelUnavailable, _))));
    }

    #[test]
    fn revalidate_rejects_missing_custom_node() {
        let pool = PoolAdvertisementData {
            pool_key: "gpu0".to_owned(),
            backend: gpq_domain::BackendKind::ComfyUi,
            backend_version: "1.0".to_owned(),
            ready: true,
            unready_reason: None,
            slots_total: 1,
            slots_busy: Vec::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            models: Vec::new(),
            custom_nodes: std::collections::BTreeMap::new(),
            probes: std::collections::BTreeMap::new(),
        };
        let manifest = gpq_domain::WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "IMAGE".to_owned(),
            artifact_kind: gpq_domain::MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: Vec::new(),
            required_custom_nodes: std::collections::BTreeMap::from([(
                "comfy-node".to_owned(),
                "1.2.0".to_owned(),
            )]),
        };
        let result = revalidate_capabilities(&pool, Modality::Image, None, Some(&manifest));
        assert!(matches!(
            result,
            Err((FailureKind::UnsupportedCapability, _))
        ));
    }

    #[test]
    fn execution_timeout_uses_the_leased_value_when_valid() {
        let duration = buffa_types::google::protobuf::Duration {
            seconds: 120,
            ..Default::default()
        };
        assert_eq!(
            execution_timeout_duration(Some(duration), Modality::Llm),
            std::time::Duration::from_mins(2)
        );
    }

    #[test]
    fn execution_timeout_falls_back_to_the_modality_default_when_unset() {
        // ADR 0003: the timeout clock still needs a value even if a lease
        // somehow carries none.
        assert_eq!(
            execution_timeout_duration(None, Modality::Image),
            Modality::Image.default_execution_timeout()
        );
    }

    #[test]
    fn execution_timeout_falls_back_to_the_modality_default_when_negative() {
        let duration = buffa_types::google::protobuf::Duration {
            seconds: -5,
            ..Default::default()
        };
        assert_eq!(
            execution_timeout_duration(Some(duration), Modality::Music),
            Modality::Music.default_execution_timeout()
        );
    }

    #[test]
    fn output_publish_strategy_accepts_object_store_and_worker_local() {
        assert!(matches!(
            output_publish_strategy(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE
            )),
            Ok(OutputPublishStrategy::ObjectStore)
        ));
        assert!(matches!(
            output_publish_strategy(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL
            )),
            Ok(OutputPublishStrategy::WorkerLocal)
        ));
    }

    #[test]
    fn output_publish_strategy_rejects_inline_relay_and_unspecified() {
        // ADR 0008: Inline Relay is input-only; an Attempt output must
        // never silently land there.
        assert!(matches!(
            output_publish_strategy(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_INLINE_RELAY
            )),
            Err((FailureKind::Internal, _))
        ));
        assert!(matches!(
            output_publish_strategy(EnumValue::Known(
                ProtoArtifactPlacement::ARTIFACT_PLACEMENT_UNSPECIFIED
            )),
            Err((FailureKind::Internal, _))
        ));
    }

    #[test]
    fn revalidate_accepts_llm_when_resident_model_matches() {
        let hash = ContentHash::digest(b"resident-model");
        let pool = PoolAdvertisementData {
            pool_key: "gpu0".to_owned(),
            backend: gpq_domain::BackendKind::LlamaCpp,
            backend_version: "1.0".to_owned(),
            ready: true,
            unready_reason: None,
            slots_total: 4,
            slots_busy: Vec::new(),
            resident_model: Some(hash),
            accelerator_memory_bytes: None,
            models: Vec::new(),
            custom_nodes: std::collections::BTreeMap::new(),
            probes: std::collections::BTreeMap::new(),
        };
        assert!(revalidate_capabilities(&pool, Modality::Llm, Some(hash), None).is_ok());
    }

    #[test]
    fn revalidate_rejects_a_workflow_missing_a_required_model() {
        let pool = PoolAdvertisementData {
            pool_key: "gpu0".to_owned(),
            backend: gpq_domain::BackendKind::ComfyUi,
            backend_version: "1.0".to_owned(),
            ready: true,
            unready_reason: None,
            slots_total: 1,
            slots_busy: Vec::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            models: Vec::new(),
            custom_nodes: std::collections::BTreeMap::new(),
            probes: std::collections::BTreeMap::new(),
        };
        let manifest = gpq_domain::WorkflowManifest {
            output_node: "9".to_owned(),
            output_name: "IMAGE".to_owned(),
            artifact_kind: gpq_domain::MediaKind::Image,
            artifact_mime: "image/png".to_owned(),
            required_models: vec![ContentHash::digest(b"checkpoint")],
            required_custom_nodes: std::collections::BTreeMap::new(),
        };
        let result = revalidate_capabilities(&pool, Modality::Image, None, Some(&manifest));
        assert!(matches!(result, Err((FailureKind::ModelUnavailable, _))));
    }
}
