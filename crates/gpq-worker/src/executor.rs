//! Executes one typed, leased native generation end to end.
//!
//! Inputs are staged and verified before the native child is called, output
//! files are re-hashed before publication, and every path reports exactly one
//! terminal `WorkerMessage`. Cancellation is cooperative first, then the owning
//! native process tree is forcibly reclaimed after a bounded grace period.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use buffa::{EnumValue, Message, MessageField};
use buffa_types::google::protobuf::Timestamp;
use chrono::Utc;
use futures::StreamExt;
use gpq_domain::{
    ArtifactId, ArtifactManifest, AttemptId, ContentHash, FailureKind, GenerationParameters,
    Hasher, LoraSelection, MediaKind, ModelAssetRole, ModelFamily, ModelManifest, SamplingPreset,
    TRANSFER_CHUNK_BYTES,
};
use gpq_inference::{InferenceRequest, NativeOutput, OutputKind};
use gpq_proto::gpq::v1::{self as v1, ArtifactPlacement as ProtoArtifactPlacement};
use gpq_proto::gpq::worker::v1::{
    ArtifactChunk, AttemptFailure, AttemptOutput, AttemptProgress, AttemptResult, AttemptRunning,
    CancelAcknowledged, FetchArtifactRequest, LeaseAssignment, LeaseInput, WorkerMessage,
    lease_assignment, worker_message,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;

use crate::artifacts::LocalArtifactStore;
use crate::backend::{Backend, ExecutionEvent, ExecutionRequest};
use crate::pool::{PoolSupervisor, SlotLease};

pub type Transport = connectrpc::client::SharedHttp2Connection;

#[derive(Debug, Clone)]
pub(crate) struct DecodedLease {
    pub model_version: ContentHash,
    pub manifest: ModelManifest,
    pub parameters: GenerationParameters,
}

pub struct ExecutionContext {
    pub attempt_id: AttemptId,
    pub lease: LeaseAssignment,
    pub decoded: DecodedLease,
    pub pool_key: String,
    #[expect(dead_code, reason = "held for Drop to release the pool execution slot")]
    pub slot: SlotLease,
    pub backend: Arc<dyn Backend>,
    pub pools: Arc<PoolSupervisor>,
    pub outbound: mpsc::Sender<WorkerMessage>,
    pub transfer: gpq_proto::gpq::worker::v1::WorkerTransferServiceClient<Transport>,
    pub http: reqwest::Client,
    pub artifacts: Arc<LocalArtifactStore>,
    pub attempt_dir: PathBuf,
    pub cancel: CancellationToken,
}

struct DirGuard(PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

enum Stop {
    Cancelled,
    Failed {
        kind: FailureKind,
        message: String,
        retry_hint: bool,
    },
}

struct ExecutionOutcome {
    outputs: Vec<AttemptOutput>,
}

#[derive(Clone)]
struct InputArtifact {
    path: PathBuf,
    manifest: ArtifactManifest,
}

pub async fn execute(ctx: ExecutionContext) {
    let _cleanup = DirGuard(ctx.attempt_dir.clone());
    if let Err(error) = tokio::fs::create_dir_all(&ctx.attempt_dir).await {
        report_failure(
            &ctx,
            FailureKind::Internal,
            format!("cannot create attempt directory: {error}"),
            true,
        )
        .await;
        return;
    }
    if let Err(error) = tokio::fs::create_dir_all(ctx.attempt_dir.join("native-output")).await {
        report_failure(
            &ctx,
            FailureKind::Internal,
            format!("cannot create native output directory: {error}"),
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
    let computed_manifest_hash = ctx
        .decoded
        .manifest
        .canonical_hash()
        .map_err(|error| internal(format!("serializing pinned model manifest: {error}")))?;
    if computed_manifest_hash != ctx.decoded.model_version {
        return Err(Stop::Failed {
            kind: FailureKind::ModelUnavailable,
            message: "pinned model manifest hash changed after lease validation".to_owned(),
            retry_hint: false,
        });
    }
    let input_artifacts = fetch_inputs(ctx).await?;
    let inputs = input_artifacts
        .iter()
        .map(|(artifact_id, input)| (*artifact_id, input.path.clone()))
        .collect();
    let request = ctx
        .pools
        .build_inference_request(
            &ctx.pool_key,
            ctx.decoded.model_version,
            ctx.decoded.parameters.clone(),
            inputs,
            ctx.attempt_dir.join("native-output"),
            ctx.lease.seed,
        )
        .map_err(|error| Stop::Failed {
            kind: FailureKind::InvalidInput,
            message: format!("building trusted native request: {error:#}"),
            retry_hint: false,
        })?;
    let timeout = execution_timeout_duration(
        ctx.lease.execution_timeout.as_option().cloned(),
        ctx.decoded
            .manifest
            .family
            .modality()
            .default_execution_timeout(),
    )
    .map_err(|message| Stop::Failed {
        kind: FailureKind::InvalidInput,
        message,
        retry_hint: false,
    })?;

    send(
        ctx,
        AttemptRunning {
            attempt_id: ctx.attempt_id.to_string(),
            ..Default::default()
        }
        .into(),
    )
    .await;

    let native_outputs = run_backend(ctx, request, timeout).await?;
    if native_outputs.len() != 1 {
        return Err(Stop::Failed {
            kind: FailureKind::BackendCrashed,
            message: format!(
                "native generation returned {} output files; exactly one is required",
                native_outputs.len()
            ),
            retry_hint: true,
        });
    }
    let output = &native_outputs[0];
    validate_native_output(output, &ctx.attempt_dir.join("native-output")).map_err(|message| {
        Stop::Failed {
            kind: FailureKind::BackendCrashed,
            message,
            retry_hint: true,
        }
    })?;
    let manifest = manifest_for_output(output)
        .await
        .map_err(|(kind, message)| Stop::Failed {
            kind,
            message,
            retry_hint: kind.is_retryable(),
        })?;
    let published =
        publish_output(ctx, &output.path, manifest)
            .await
            .map_err(|(kind, message)| Stop::Failed {
                kind,
                message,
                retry_hint: kind.is_retryable(),
            })?;
    Ok(ExecutionOutcome {
        outputs: vec![published],
    })
}

async fn run_backend(
    ctx: &ExecutionContext,
    request: InferenceRequest,
    exec_timeout: Duration,
) -> Result<Vec<NativeOutput>, Stop> {
    let (events_tx, mut events_rx) = mpsc::channel(64);
    let execute_fut = ctx.backend.execute(
        ExecutionRequest {
            model_version: ctx.decoded.model_version,
            inference: request,
            deadline: exec_timeout,
        },
        events_tx,
        ctx.cancel.clone(),
    );
    tokio::pin!(execute_fut);
    let sleep = tokio::time::sleep(exec_timeout);
    tokio::pin!(sleep);
    let release_grace = tokio::time::sleep(Duration::from_secs(45));
    tokio::pin!(release_grace);

    let mut outputs = Vec::new();
    let mut timed_out = false;
    let mut cancellation_seen = ctx.cancel.is_cancelled();
    let mut release_armed = false;
    let mut reclaimed = false;
    let mut backend_result = None;
    if cancellation_seen {
        arm_release_grace(&mut release_armed, release_grace.as_mut());
    }

    while backend_result.is_none() {
        tokio::select! {
            maybe_event = events_rx.recv() => {
                if let Some(event) = maybe_event {
                    handle_event(ctx, event, &mut outputs).await;
                }
            }
            result = &mut execute_fut => backend_result = Some(result),
            () = ctx.cancel.cancelled(), if !cancellation_seen => {
                cancellation_seen = true;
                arm_release_grace(&mut release_armed, release_grace.as_mut());
            }
            () = &mut sleep, if !timed_out => {
                timed_out = true;
                ctx.cancel.cancel();
                cancellation_seen = true;
                arm_release_grace(&mut release_armed, release_grace.as_mut());
            }
            () = &mut release_grace, if release_armed && !reclaimed => {
                reclaimed = true;
                let pools = Arc::clone(&ctx.pools);
                let pool_key = ctx.pool_key.clone();
                let attempt_id = ctx.attempt_id;
                tokio::spawn(async move {
                    pools.force_reclaim(&pool_key, attempt_id).await;
                });
            }
        }
    }
    if timed_out && !reclaimed {
        match ctx.backend.release_memory().await {
            Ok(true) => {}
            Ok(false) | Err(_) => {
                ctx.pools.force_reclaim(&ctx.pool_key, ctx.attempt_id).await;
            }
        }
    }
    while let Ok(event) = events_rx.try_recv() {
        handle_event(ctx, event, &mut outputs).await;
    }
    let Some(result) = backend_result else {
        return Err(internal(
            "native executor exited without a result".to_owned(),
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
    if let Err(error) = result {
        if error.kind == FailureKind::Cancelled {
            return Err(Stop::Cancelled);
        }
        return Err(Stop::Failed {
            kind: error.kind,
            message: error.message,
            retry_hint: error.retry_hint,
        });
    }
    Ok(outputs)
}

fn arm_release_grace(release_armed: &mut bool, release_grace: Pin<&mut tokio::time::Sleep>) {
    if !*release_armed {
        release_grace.reset(tokio::time::Instant::now() + Duration::from_secs(45));
        *release_armed = true;
    }
}

async fn handle_event(
    ctx: &ExecutionContext,
    event: ExecutionEvent,
    outputs: &mut Vec<NativeOutput>,
) {
    match event {
        ExecutionEvent::Progress { fraction } => {
            let progress = v1::Progress {
                fraction: f64::from(fraction),
                stage: "native".to_owned(),
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
        ExecutionEvent::Output { output } => outputs.push(output),
    }
}

fn validate_native_output(output: &NativeOutput, output_dir: &Path) -> Result<(), String> {
    let output_path = std::fs::canonicalize(&output.path).map_err(|error| {
        format!(
            "native output {} is unavailable: {error}",
            output.path.display()
        )
    })?;
    let output_dir = std::fs::canonicalize(output_dir)
        .map_err(|error| format!("native output directory is unavailable: {error}"))?;
    if !output_path.starts_with(&output_dir) || !output_path.is_file() {
        return Err(
            "native output is outside its attempt directory or not a regular file".to_owned(),
        );
    }
    if output.width == 0 || output.height == 0 {
        return Err("native output has invalid dimensions".to_owned());
    }
    match output.kind {
        OutputKind::Image if output.mime_type == "image/png" => Ok(()),
        OutputKind::Video
            if output.mime_type == "video/webm"
                && output.frames.is_some_and(|frames| frames > 0)
                && output
                    .duration_seconds
                    .is_some_and(|duration| duration.is_finite() && duration > 0.0) =>
        {
            Ok(())
        }
        _ => Err("native output kind, MIME type, and metadata disagree".to_owned()),
    }
}

async fn manifest_for_output(
    output: &NativeOutput,
) -> Result<ArtifactManifest, (FailureKind, String)> {
    let mut file = tokio::fs::File::open(&output.path)
        .await
        .map_err(|error| (FailureKind::Internal, error.to_string()))?;
    let mut hasher = Hasher::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; TRANSFER_CHUNK_BYTES];
    loop {
        let count = file
            .read(&mut buffer)
            .await
            .map_err(|error| (FailureKind::Internal, error.to_string()))?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| (FailureKind::Internal, "output size overflow".to_owned()))?;
        hasher.update(&buffer[..count]);
    }
    let (kind, mime_type) = match output.kind {
        OutputKind::Image if output.mime_type == "image/png" => (MediaKind::Image, "image/png"),
        OutputKind::Video if output.mime_type == "video/webm" => (MediaKind::Video, "video/webm"),
        _ => {
            return Err((
                FailureKind::BackendCrashed,
                "native output media type is invalid".to_owned(),
            ));
        }
    };
    Ok(ArtifactManifest {
        size_bytes: bytes,
        digest: hasher.finish(),
        kind,
        mime_type: mime_type.to_owned(),
    })
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
    let message = WorkerMessage {
        message: Some(
            AttemptResult {
                attempt_id: ctx.attempt_id.to_string(),
                outputs: outcome.outputs,
                ..Default::default()
            }
            .into(),
        ),
        ..Default::default()
    };
    if message.encoded_len() > 4 * 1024 * 1024 {
        report_failure(
            ctx,
            FailureKind::InvalidInput,
            "native result exceeds the 4 MiB control transport limit".to_owned(),
            false,
        )
        .await;
        return;
    }
    let _ = ctx.outbound.send(message).await;
}

async fn report_failure(
    ctx: &ExecutionContext,
    kind: FailureKind,
    message: String,
    retry_hint: bool,
) {
    let failure = AttemptFailure {
        attempt_id: ctx.attempt_id.to_string(),
        failure: MessageField::some(to_proto_failure(kind, message, retry_hint)),
        ..Default::default()
    };
    send(ctx, failure.into()).await;
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

fn execution_timeout_duration(
    lease_timeout: Option<buffa_types::google::protobuf::Duration>,
    default: Duration,
) -> Result<Duration, String> {
    let timeout = lease_timeout
        .map(Duration::try_from)
        .transpose()
        .map_err(|error| format!("invalid leased execution timeout: {error}"))?
        .unwrap_or(default);
    if timeout.is_zero() {
        return Err("leased execution timeout must be positive".to_owned());
    }
    Ok(timeout)
}

async fn fetch_inputs(ctx: &ExecutionContext) -> Result<BTreeMap<ArtifactId, InputArtifact>, Stop> {
    let mut inputs: BTreeMap<ArtifactId, InputArtifact> = BTreeMap::new();
    for lease_input in &ctx.lease.inputs {
        let artifact_id = ArtifactId::from_str(&lease_input.artifact_id)
            .map_err(|error| invalid(format!("invalid input artifact id: {error}")))?;
        let manifest = manifest_from_proto(&lease_input.manifest).map_err(invalid)?;
        if let Some(previous) = inputs.get(&artifact_id) {
            if previous.manifest != manifest {
                return Err(invalid(format!(
                    "duplicate input {artifact_id} has conflicting manifests"
                )));
            }
            continue;
        }
        let path = ctx.attempt_dir.join(format!("input-{artifact_id}"));
        download_input(ctx, lease_input, artifact_id, &path, &manifest)
            .await
            .map_err(|(kind, message)| Stop::Failed {
                retry_hint: kind.is_retryable(),
                kind,
                message,
            })?;
        inputs.insert(artifact_id, InputArtifact { path, manifest });
    }
    let required = referenced_inputs(&ctx.decoded.parameters);
    if required
        .iter()
        .any(|artifact| !inputs.contains_key(artifact))
    {
        return Err(invalid(
            "a typed request references an artifact absent from the lease".to_owned(),
        ));
    }
    if inputs.keys().any(|artifact| !required.contains(artifact)) {
        return Err(invalid(
            "lease stages an artifact not referenced by typed parameters".to_owned(),
        ));
    }
    Ok(inputs)
}

fn referenced_inputs(parameters: &GenerationParameters) -> std::collections::BTreeSet<ArtifactId> {
    let mut ids = std::collections::BTreeSet::new();
    match parameters {
        GenerationParameters::Image(image) => ids.extend(image.references.iter().copied()),
        GenerationParameters::Video(video) => {
            ids.extend(video.first_frame);
            ids.extend(video.last_frame);
        }
    }
    ids
}

async fn download_input(
    ctx: &ExecutionContext,
    lease_input: &LeaseInput,
    artifact_id: ArtifactId,
    path: &Path,
    manifest: &ArtifactManifest,
) -> Result<(), (FailureKind, String)> {
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|error| (FailureKind::Internal, error.to_string()))?;
    let mut hasher = Hasher::new();
    let total = match lease_input.placement {
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_REMOTE_LOCAL) => {
            fetch_remote_local_input(ctx, lease_input, artifact_id, &mut file, &mut hasher).await?
        }
        EnumValue::Known(ProtoArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE) => {
            fetch_object_store_input(ctx, lease_input, &mut file, &mut hasher).await?
        }
        placement => {
            return Err((
                FailureKind::InvalidInput,
                format!("unsupported input placement: {placement:?}"),
            ));
        }
    };
    file.flush()
        .await
        .map_err(|error| (FailureKind::Internal, error.to_string()))?;
    manifest
        .verify(total, hasher.finish())
        .map_err(|error| (FailureKind::TransferFailed, error.to_string()))
}

async fn fetch_remote_local_input(
    ctx: &ExecutionContext,
    lease_input: &LeaseInput,
    artifact_id: ArtifactId,
    file: &mut tokio::fs::File,
    hasher: &mut Hasher,
) -> Result<u64, (FailureKind, String)> {
    if !lease_input.download_url.is_empty() {
        return Err((
            FailureKind::InvalidInput,
            "remote-local input must use the authenticated transfer RPC, not a URL".to_owned(),
        ));
    }
    let mut stream = ctx
        .transfer
        .fetch_artifact(FetchArtifactRequest {
            artifact_id: artifact_id.to_string(),
            attempt_id: ctx.attempt_id.to_string(),
            offset: 0,
            ..Default::default()
        })
        .await
        .map_err(|error| (FailureKind::TransferFailed, error.to_string()))?;
    let mut expected_offset = 0_u64;
    let mut saw_last = false;
    loop {
        let Some(message) = stream
            .message::<ArtifactChunk>()
            .await
            .map_err(|error| (FailureKind::TransferFailed, error.to_string()))?
        else {
            break;
        };
        let chunk = message.to_owned_message();
        if saw_last || chunk.offset != expected_offset || chunk.data.len() > TRANSFER_CHUNK_BYTES {
            return Err((
                FailureKind::TransferFailed,
                "remote-local artifact chunks have invalid offset, size, or final marker"
                    .to_owned(),
            ));
        }
        expected_offset = expected_offset
            .checked_add(chunk.data.len() as u64)
            .ok_or_else(|| {
                (
                    FailureKind::TransferFailed,
                    "artifact byte offset overflow".to_owned(),
                )
            })?;
        hasher.update(&chunk.data);
        file.write_all(&chunk.data)
            .await
            .map_err(|error| (FailureKind::Internal, error.to_string()))?;
        saw_last = chunk.last;
        if saw_last {
            break;
        }
    }
    if !saw_last {
        return Err((
            FailureKind::TransferFailed,
            "remote-local artifact stream ended before its final chunk".to_owned(),
        ));
    }
    Ok(expected_offset)
}

async fn fetch_object_store_input(
    ctx: &ExecutionContext,
    lease_input: &LeaseInput,
    file: &mut tokio::fs::File,
    hasher: &mut Hasher,
) -> Result<u64, (FailureKind, String)> {
    let url = url::Url::parse(&lease_input.download_url).map_err(|error| {
        (
            FailureKind::InvalidInput,
            format!("invalid presigned input URL: {error}"),
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err((
            FailureKind::InvalidInput,
            "presigned input URL must be HTTP(S) and contain no user credentials".to_owned(),
        ));
    }
    // This request intentionally carries no Worker authorization. The URL
    // is an external object-scoped presigned GET, never the Remote-local path.
    let mut response = ctx
        .http
        .get(url)
        .send()
        .await
        .map_err(|error| (FailureKind::TransferFailed, sanitized_http_error(error)))?
        .error_for_status()
        .map_err(|error| (FailureKind::TransferFailed, sanitized_http_error(error)))?;
    let mut total = 0_u64;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| (FailureKind::TransferFailed, sanitized_http_error(error)))?
    {
        total = total.checked_add(chunk.len() as u64).ok_or_else(|| {
            (
                FailureKind::TransferFailed,
                "artifact byte count overflow".to_owned(),
            )
        })?;
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|error| (FailureKind::Internal, error.to_string()))?;
    }
    Ok(total)
}

fn invalid(message: String) -> Stop {
    Stop::Failed {
        kind: FailureKind::InvalidInput,
        message,
        retry_hint: false,
    }
}

fn manifest_from_proto(manifest: &v1::ArtifactManifest) -> Result<ArtifactManifest, String> {
    let digest = ContentHash::from_str(&manifest.digest_sha256)
        .map_err(|error| format!("invalid artifact digest: {error}"))?;
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

fn media_kind_from_proto(kind: EnumValue<v1::MediaKind>) -> Result<MediaKind, String> {
    match kind {
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_IMAGE) => Ok(MediaKind::Image),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_VIDEO) => Ok(MediaKind::Video),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_AUDIO) => Ok(MediaKind::Audio),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_TEXT) => Ok(MediaKind::Text),
        EnumValue::Known(v1::MediaKind::MEDIA_KIND_BINARY) => Ok(MediaKind::Binary),
        other => Err(format!("unknown artifact media kind: {other:?}")),
    }
}

fn media_kind_to_proto(kind: MediaKind) -> v1::MediaKind {
    match kind {
        MediaKind::Image => v1::MediaKind::MEDIA_KIND_IMAGE,
        MediaKind::Video => v1::MediaKind::MEDIA_KIND_VIDEO,
        MediaKind::Audio => v1::MediaKind::MEDIA_KIND_AUDIO,
        MediaKind::Text => v1::MediaKind::MEDIA_KIND_TEXT,
        MediaKind::Binary => v1::MediaKind::MEDIA_KIND_BINARY,
    }
}

pub(crate) fn decode_lease(lease: &LeaseAssignment) -> Result<DecodedLease, String> {
    gpq_domain::validate_seed(lease.seed).map_err(|error| error.to_string())?;
    let model_version = ContentHash::from_str(&lease.model_sha256)
        .map_err(|error| format!("invalid model version hash: {error}"))?;
    let wire_manifest = lease
        .model_manifest
        .as_option()
        .ok_or_else(|| "lease omits its pinned model manifest".to_owned())?;
    let manifest = model_manifest_from_proto(wire_manifest)?;
    manifest
        .validate()
        .map_err(|error| format!("invalid pinned model manifest: {error}"))?;
    let canonical_hash = manifest
        .canonical_hash()
        .map_err(|error| format!("serializing pinned model manifest: {error}"))?;
    if canonical_hash != model_version {
        return Err("lease model hash does not match its canonical model manifest".to_owned());
    }
    let parameters = generation_parameters_from_proto(lease)?;
    parameters
        .validate_for_family(manifest.family)
        .map_err(|error| format!("invalid typed generation parameters: {error}"))?;
    Ok(DecodedLease {
        model_version,
        manifest,
        parameters,
    })
}

fn generation_parameters_from_proto(
    lease: &LeaseAssignment,
) -> Result<GenerationParameters, String> {
    match lease.generation_parameters.clone() {
        Some(lease_assignment::GenerationParameters::ImageParameters(image)) => {
            let references = image
                .reference_artifact_ids
                .iter()
                .map(|id| {
                    ArtifactId::from_str(id)
                        .map_err(|error| format!("invalid reference artifact id: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let loras = image
                .loras
                .iter()
                .map(lora_from_proto)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(GenerationParameters::Image(gpq_domain::ImageParameters {
                prompt: image.prompt,
                negative_prompt: image.negative_prompt,
                width: image.width,
                height: image.height,
                steps: image.steps,
                cfg_scale: image.cfg_scale,
                references,
                loras,
                sigmas: (!image.sigmas.is_empty()).then_some(image.sigmas),
            }))
        }
        Some(lease_assignment::GenerationParameters::VideoParameters(video)) => {
            Ok(GenerationParameters::Video(gpq_domain::VideoParameters {
                prompt: video.prompt,
                negative_prompt: video.negative_prompt,
                width: video.width,
                height: video.height,
                frames: video.frames,
                first_frame: optional_artifact_id(video.first_frame_artifact_id)?,
                last_frame: optional_artifact_id(video.last_frame_artifact_id)?,
            }))
        }
        None => Err("lease omits typed image/video parameters".to_owned()),
    }
}

fn optional_artifact_id(value: Option<String>) -> Result<Option<ArtifactId>, String> {
    value
        .map(|id| {
            ArtifactId::from_str(&id).map_err(|error| format!("invalid frame artifact id: {error}"))
        })
        .transpose()
}

fn lora_from_proto(wire: &v1::LoraSelection) -> Result<LoraSelection, String> {
    Ok(LoraSelection {
        sha256: ContentHash::from_str(&wire.sha256)
            .map_err(|error| format!("invalid LoRA content hash: {error}"))?,
        scale: wire.scale,
    })
}

fn model_manifest_from_proto(wire: &v1::ModelManifest) -> Result<ModelManifest, String> {
    let family = match wire.family {
        EnumValue::Known(v1::ModelFamily::MODEL_FAMILY_QWEN_IMAGE21) => ModelFamily::QwenImage21,
        EnumValue::Known(v1::ModelFamily::MODEL_FAMILY_MINIMAX_H3) => ModelFamily::MinimaxH3,
        other => return Err(format!("unknown model family: {other:?}")),
    };
    let mut assets = BTreeMap::new();
    for asset in &wire.assets {
        let role = match asset.role {
            EnumValue::Known(v1::ModelAssetRole::MODEL_ASSET_ROLE_DIFFUSION) => {
                ModelAssetRole::Diffusion
            }
            EnumValue::Known(v1::ModelAssetRole::MODEL_ASSET_ROLE_TEXT_ENCODER) => {
                ModelAssetRole::TextEncoder
            }
            EnumValue::Known(v1::ModelAssetRole::MODEL_ASSET_ROLE_VISION_ENCODER) => {
                ModelAssetRole::VisionEncoder
            }
            EnumValue::Known(v1::ModelAssetRole::MODEL_ASSET_ROLE_VAE) => ModelAssetRole::Vae,
            EnumValue::Known(v1::ModelAssetRole::MODEL_ASSET_ROLE_AUDIO_VAE) => {
                ModelAssetRole::AudioVae
            }
            other => return Err(format!("unknown model asset role: {other:?}")),
        };
        let hash = ContentHash::from_str(&asset.sha256)
            .map_err(|error| format!("invalid model asset hash: {error}"))?;
        if assets.insert(role, hash).is_some() {
            return Err(format!("model manifest repeats asset role {role:?}"));
        }
    }
    let default_loras = wire
        .default_loras
        .iter()
        .map(lora_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let sampling = wire
        .sampling
        .as_option()
        .ok_or_else(|| "model manifest omits its sampling preset".to_owned())?;
    Ok(ModelManifest {
        family,
        assets,
        default_loras,
        sampling: SamplingPreset {
            steps: sampling.steps,
            cfg_scale: sampling.cfg_scale,
            sigmas: sampling.sigmas.clone(),
        },
    })
}

pub(crate) fn model_manifest_to_proto(manifest: &ModelManifest) -> v1::ModelManifest {
    v1::ModelManifest {
        family: EnumValue::Known(match manifest.family {
            ModelFamily::QwenImage21 => v1::ModelFamily::MODEL_FAMILY_QWEN_IMAGE21,
            ModelFamily::MinimaxH3 => v1::ModelFamily::MODEL_FAMILY_MINIMAX_H3,
        }),
        assets: manifest
            .assets
            .iter()
            .map(|(role, hash)| v1::ModelAsset {
                role: EnumValue::Known(match role {
                    ModelAssetRole::Diffusion => v1::ModelAssetRole::MODEL_ASSET_ROLE_DIFFUSION,
                    ModelAssetRole::TextEncoder => {
                        v1::ModelAssetRole::MODEL_ASSET_ROLE_TEXT_ENCODER
                    }
                    ModelAssetRole::VisionEncoder => {
                        v1::ModelAssetRole::MODEL_ASSET_ROLE_VISION_ENCODER
                    }
                    ModelAssetRole::Vae => v1::ModelAssetRole::MODEL_ASSET_ROLE_VAE,
                    ModelAssetRole::AudioVae => v1::ModelAssetRole::MODEL_ASSET_ROLE_AUDIO_VAE,
                }),
                sha256: hash.to_hex(),
                ..Default::default()
            })
            .collect(),
        default_loras: manifest
            .default_loras
            .iter()
            .map(|lora| v1::LoraSelection {
                sha256: lora.sha256.to_hex(),
                scale: lora.scale,
                ..Default::default()
            })
            .collect(),
        sampling: MessageField::some(v1::SamplingPreset {
            steps: manifest.sampling.steps,
            cfg_scale: manifest.sampling.cfg_scale,
            sigmas: manifest.sampling.sigmas.clone(),
            ..Default::default()
        }),
        ..Default::default()
    }
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
            FailureKind::InvalidInput,
            format!("unsupported output placement: {other:?}"),
        )),
    }
}

enum OutputPublishStrategy {
    ObjectStore,
    WorkerLocal,
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
                .map_err(|error| (FailureKind::Internal, error.to_string()))?;
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
fn sanitized_http_error(error: reqwest::Error) -> String {
    error.without_url().to_string()
}

async fn upload_to_object_store(
    ctx: &ExecutionContext,
    path: &Path,
    manifest: &ArtifactManifest,
) -> Result<(), (FailureKind, String)> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|error| (FailureKind::Internal, error.to_string()))?;
    let tally = Arc::new(Mutex::new(UploadTally::default()));
    let stream_tally = Arc::clone(&tally);
    let body = ReaderStream::new(file).map(move |chunk| {
        let chunk = chunk?;
        stream_tally
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record(&chunk);
        Ok::<_, std::io::Error>(chunk)
    });
    let response = ctx
        .http
        .put(ctx.lease.output_upload_url.as_str())
        .header(reqwest::header::CONTENT_LENGTH, manifest.size_bytes)
        .body(reqwest::Body::wrap_stream(body))
        .send()
        .await
        .map_err(|error| (FailureKind::TransferFailed, sanitized_http_error(error)))?;
    response
        .error_for_status()
        .map_err(|error| (FailureKind::TransferFailed, sanitized_http_error(error)))?;
    let sent = tally
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    manifest
        .verify(sent.bytes, sent.hasher.finish())
        .map_err(|error| (FailureKind::Internal, error.to_string()))
}

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

#[cfg(test)]
mod tests {
    use super::{media_kind_from_proto, media_kind_to_proto, validate_native_output};
    use buffa::EnumValue;
    use gpq_domain::MediaKind;
    use gpq_inference::{NativeOutput, OutputKind};
    use gpq_proto::gpq::v1;

    #[test]
    fn artifact_media_kind_round_trips() {
        for (domain, wire) in [
            (MediaKind::Image, v1::MediaKind::MEDIA_KIND_IMAGE),
            (MediaKind::Video, v1::MediaKind::MEDIA_KIND_VIDEO),
            (MediaKind::Audio, v1::MediaKind::MEDIA_KIND_AUDIO),
            (MediaKind::Text, v1::MediaKind::MEDIA_KIND_TEXT),
            (MediaKind::Binary, v1::MediaKind::MEDIA_KIND_BINARY),
        ] {
            assert_eq!(media_kind_to_proto(domain), wire);
            assert_eq!(media_kind_from_proto(EnumValue::Known(wire)), Ok(domain));
        }
    }

    #[test]
    fn native_output_must_be_owned_by_attempt_directory() {
        let Ok(directory) = tempfile::tempdir() else {
            panic!("create test directory");
        };
        let output_dir = directory.path().join("native-output");
        assert!(std::fs::create_dir(&output_dir).is_ok());
        let inside = output_dir.join("generated.png");
        let outside = directory.path().join("foreign.png");
        assert!(std::fs::write(&inside, b"image").is_ok());
        assert!(std::fs::write(&outside, b"foreign").is_ok());
        let output = |path| NativeOutput {
            path,
            mime_type: "image/png".to_owned(),
            kind: OutputKind::Image,
            width: 32,
            height: 32,
            frames: None,
            duration_seconds: None,
        };

        assert!(validate_native_output(&output(inside), &output_dir).is_ok());
        assert!(validate_native_output(&output(outside), &output_dir).is_err());
    }
}
