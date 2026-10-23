//! Private loopback server for the native inference child role.
//!
//! This process receives only requests from its owning Worker parent. Model and
//! `LoRA` files are re-resolved against the child-loaded configuration and input
//! paths are confined to the Worker's staging tree before native code runs.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{Context, ensure};
use clap::Parser;
use gpq_domain::{ArtifactId, ContentHash, GenerationParameters, ModelAssetRole};
use gpq_inference::{InferenceRequest, NativeOutput, OutputKind, ResolvedLora, ResolvedModelFiles};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::config::{PoolConfig, WorkerConfig};
use crate::models::{self, ConfiguredExtraLora, ConfiguredModel};

const TOKEN_ENV: &str = "GPQ_INFER_TOKEN";
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "gpq infer", about = "Private native inference child role")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    pool: String,
    #[arg(long)]
    listen: Option<String>,
    #[arg(long, hide = true)]
    diagnose: bool,
}

struct State {
    pool: PoolConfig,
    models: Vec<ConfiguredModel>,
    extra_loras: BTreeMap<gpq_domain::ContentHash, ConfiguredExtraLora>,
    token: String,
    attempts_root: PathBuf,
    execution: Arc<Semaphore>,
    active: Arc<Mutex<Option<ActiveJob>>>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ready: bool,
    backend: String,
    backend_version: String,
    devices: Vec<gpq_inference::BackendDevice>,
    detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct ReleaseResponse {
    released: bool,
}

#[derive(Debug)]
struct ActiveJob {
    id: uuid::Uuid,
    cancelled: bool,
}

struct ActiveJobGuard {
    active: Arc<Mutex<Option<ActiveJob>>>,
    id: uuid::Uuid,
    _permit: OwnedSemaphorePermit,
}
fn lock_active(active: &Mutex<Option<ActiveJob>>) -> MutexGuard<'_, Option<ActiveJob>> {
    active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn register_active_job(
    active: &Arc<Mutex<Option<ActiveJob>>>,
    id: uuid::Uuid,
    permit: OwnedSemaphorePermit,
) -> anyhow::Result<ActiveJobGuard> {
    let mut current = lock_active(active);
    ensure!(
        current.is_none(),
        "inference slot already has an active job"
    );
    *current = Some(ActiveJob {
        id,
        cancelled: false,
    });
    Ok(ActiveJobGuard {
        active: Arc::clone(active),
        id,
        _permit: permit,
    })
}

fn is_job_cancelled(active: &Mutex<Option<ActiveJob>>, id: uuid::Uuid) -> bool {
    lock_active(active)
        .as_ref()
        .is_some_and(|job| job.id == id && job.cancelled)
}

fn mark_job_cancelled(active: &mut Option<ActiveJob>, id: uuid::Uuid) -> Option<bool> {
    let job = active.as_mut().filter(|job| job.id == id)?;
    let newly_cancelled = !job.cancelled;
    job.cancelled = true;
    Some(newly_cancelled)
}

impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        let mut active = lock_active(&self.active);
        if active.as_ref().is_some_and(|job| job.id == self.id) {
            *active = None;
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Event {
    Progress { fraction: f32 },
    Output { output: NativeOutput },
    Complete,
    Cancelled,
    Error { kind: &'static str, message: String },
}

#[derive(Debug, Deserialize)]
struct NativeExecutionRequest {
    job_id: uuid::Uuid,
    model_version: ContentHash,
    inference: InferenceRequest,
}

#[derive(Debug, Deserialize)]
struct CancelRequest {
    job_id: uuid::Uuid,
}

#[derive(Debug, Deserialize)]
struct HttpRequest {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

/// Runs the private infer role in the current executable.
///
/// # Errors
///
/// Returns an error if arguments or configuration are invalid, device diagnosis fails,
/// or the inference listener cannot accept a connection.
pub async fn run_infer(args: Vec<OsString>) -> anyhow::Result<()> {
    init_tracing();
    let args = match Args::try_parse_from(args) {
        Ok(args) => args,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            error.print()?;
            return Ok(());
        }
        Err(error) => return Err(error).context("parsing native inference arguments"),
    };
    if args.diagnose {
        ensure!(
            args.listen.is_none(),
            "--listen cannot be used for a diagnostic probe"
        );
        return run_accelerator_diagnostic(&args.config, &args.pool);
    }
    let listen = args
        .listen
        .context("--listen is required for the inference server")?;
    let address: SocketAddr = listen
        .parse()
        .context("parsing native inference listen address")?;
    ensure!(
        address.ip().is_loopback(),
        "inference listener must be loopback"
    );
    ensure!(
        address.port() != 0,
        "inference listener must use a fixed nonzero port"
    );

    let config = WorkerConfig::load(&args.config)?;
    let pool = config
        .pools
        .iter()
        .find(|pool| pool.key == args.pool)
        .cloned()
        .with_context(|| format!("pool `{}` is not configured", args.pool))?;
    ensure!(
        pool.listen == listen,
        "inference listen address does not match worker configuration"
    );
    let token = std::env::var(TOKEN_ENV).context("missing private native inference token")?;
    ensure!(
        token.len() >= 32,
        "private native inference token is too short"
    );
    let (models, extra_loras) = models::scan_models(&pool)?;
    let attempts_root = config.state_dir.join("attempts");
    tokio::fs::create_dir_all(&attempts_root).await?;
    let state = Arc::new(State {
        pool,
        models,
        extra_loras,
        token,
        attempts_root,
        execution: Arc::new(Semaphore::new(1)),
        active: Arc::new(Mutex::new(None)),
    });
    let listener = TcpListener::bind(address)
        .await
        .with_context(|| format!("binding private inference listener at {address}"))?;
    tracing::info!(pool = %state.pool.key, %address, "native inference child listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        if !peer.ip().is_loopback() {
            drop(stream);
            continue;
        }
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, state).await {
                tracing::debug!(%peer, error = %error, "native inference connection ended");
            }
        });
    }
}

fn run_accelerator_diagnostic(config_path: &Path, pool_key: &str) -> anyhow::Result<()> {
    let config = WorkerConfig::load(config_path)?;
    let pool = config
        .pools
        .iter()
        .find(|pool| pool.key == pool_key)
        .with_context(|| format!("pool `{pool_key}` is not configured"))?;
    let devices = gpq_inference::list_devices("").context("enumerating native devices")?;
    let device = configured_device(pool, &devices)?;
    println!(
        "configured device {} is available ({})",
        device.name, device.description
    );
    Ok(())
}

fn configured_device<'a>(
    pool: &PoolConfig,
    devices: &'a [gpq_inference::BackendDevice],
) -> anyhow::Result<&'a gpq_inference::BackendDevice> {
    gpq_inference::validate_device_assignments(
        &pool.backend,
        &pool.runtime.module_backends,
        pool.runtime.params_backend.as_deref(),
        pool.runtime.max_vram.as_deref(),
        devices,
    )
    .context("validating configured backend device assignments")?;
    let device_id = gpq_inference::resolve_device_id(&pool.backend, devices)
        .context("resolving configured native device")?;
    devices
        .iter()
        .find(|device| device.name == device_id)
        .with_context(|| format!("configured native device {device_id} was not enumerated"))
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();
}

async fn handle_connection(mut stream: TcpStream, state: Arc<State>) -> anyhow::Result<()> {
    let request = match read_request(&mut stream).await {
        Ok(request) => request,
        Err(error) => {
            write_json(
                &mut stream,
                400,
                &serde_json::json!({"error": error.to_string()}),
            )
            .await?;
            return Ok(());
        }
    };
    let authorization = request
        .headers
        .get("authorization")
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !constant_time_eq(authorization.as_bytes(), state.token.as_bytes()) {
        write_json(
            &mut stream,
            401,
            &serde_json::json!({"error":"unauthorized"}),
        )
        .await?;
        return Ok(());
    }

    match (request.method.as_str(), request.target.as_str()) {
        ("GET", "/health") => {
            let health = health(&state);
            write_json(&mut stream, 200, &health).await?;
        }
        ("POST", "/execute") => execute(&mut stream, &state, &request.body).await?,
        ("POST", "/cancel") => {
            let cancel = match serde_json::from_slice::<CancelRequest>(&request.body) {
                Ok(cancel) => cancel,
                Err(error) => {
                    write_json(
                        &mut stream,
                        400,
                        &serde_json::json!({"error":error.to_string()}),
                    )
                    .await?;
                    return Ok(());
                }
            };
            let cancel_requested = request_cancel(&state, cancel.job_id);
            write_json(
                &mut stream,
                200,
                &serde_json::json!({"cancel_requested":cancel_requested}),
            )
            .await?;
        }
        ("POST", "/release") => {
            let released =
                lock_active(&state.active).is_none() && state.execution.available_permits() == 1;
            write_json(&mut stream, 200, &ReleaseResponse { released }).await?;
        }
        _ => write_json(&mut stream, 404, &serde_json::json!({"error":"not found"})).await?,
    }
    Ok(())
}
fn request_cancel(state: &Arc<State>, id: uuid::Uuid) -> bool {
    let newly_cancelled = {
        let mut active = lock_active(&state.active);
        let Some(newly_cancelled) = mark_job_cancelled(&mut active, id) else {
            return false;
        };
        // Identity check, cancellation marker, and native cancellation share
        // the same lock as active-job transitions.
        let _ = gpq_inference::cancel_current();
        newly_cancelled
    };
    if newly_cancelled {
        let active = Arc::clone(&state.active);
        tokio::spawn(async move {
            loop {
                let should_retry = {
                    let active_job = lock_active(&active);
                    if active_job
                        .as_ref()
                        .is_some_and(|job| job.id == id && job.cancelled)
                    {
                        let _ = gpq_inference::cancel_current();
                        true
                    } else {
                        false
                    }
                };
                if !should_retry {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
    }
    true
}

fn health(state: &State) -> HealthResponse {
    let (devices, mut details) = match gpq_inference::list_devices("") {
        Ok(devices) => {
            let mut details = Vec::new();
            if let Err(error) = configured_device(&state.pool, &devices) {
                details.push(format!(
                    "configured backend device assignments unavailable: {error:#}"
                ));
            }
            (devices, details)
        }
        Err(error) => (Vec::new(), vec![error.to_string()]),
    };
    for model in &state.models {
        if let Err(error) = model.verify_assets() {
            details.push(format!("model {} changed: {error:#}", model.manifest_hash));
        }
    }
    for (hash, lora) in &state.extra_loras {
        if let Err(error) = lora.snapshot.verify() {
            details.push(format!("image LoRA {hash} changed: {error:#}"));
        }
    }
    let detail = (!details.is_empty()).then(|| details.join("; "));
    HealthResponse {
        ready: detail.is_none(),
        backend: state.pool.backend.clone(),
        backend_version: env!("CARGO_PKG_VERSION").to_owned(),
        devices,
        detail,
    }
}

async fn execute(stream: &mut TcpStream, state: &Arc<State>, body: &[u8]) -> anyhow::Result<()> {
    let control = match serde_json::from_slice::<NativeExecutionRequest>(body) {
        Ok(control) if !control.job_id.is_nil() => control,
        Ok(_) => {
            write_json(
                stream,
                400,
                &serde_json::json!({"error":"job_id must not be nil"}),
            )
            .await?;
            return Ok(());
        }
        Err(error) => {
            write_json(stream, 400, &serde_json::json!({"error":error.to_string()})).await?;
            return Ok(());
        }
    };
    let Ok(permit) = Arc::clone(&state.execution).try_acquire_owned() else {
        write_json(stream, 409, &serde_json::json!({"error":"pool is busy"})).await?;
        return Ok(());
    };
    let job_id = control.job_id;
    let job_guard = register_active_job(&state.active, job_id, permit)?;
    if let Err(error) = validate_request(state, control.model_version, &control.inference).await {
        write_json(
            stream,
            400,
            &serde_json::json!({"error":format!("{error:#}")}),
        )
        .await?;
        return Ok(());
    }
    stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n",
        )
        .await?;

    let (event_tx, event_rx) = mpsc::channel::<Result<Event, String>>(64);
    let inference_request = control.inference;
    tokio::task::spawn_blocking(move || {
        execute_blocking(job_guard, job_id, &inference_request, &event_tx);
    });
    drain_events(stream, state, job_id, event_rx).await
}

fn execute_blocking(
    job_guard: ActiveJobGuard,
    job_id: uuid::Uuid,
    inference_request: &InferenceRequest,
    event_tx: &mpsc::Sender<Result<Event, String>>,
) {
    if is_job_cancelled(&job_guard.active, job_id) {
        let _ = event_tx.blocking_send(Ok(Event::Cancelled));
        return;
    }
    let progress_tx = mpsc::Sender::clone(event_tx);
    let result = gpq_inference::generate(inference_request, move |fraction| {
        if fraction.is_finite() {
            let _ = progress_tx.blocking_send(Ok(Event::Progress { fraction }));
        }
    });
    // Cancellation requested before terminal delivery suppresses output
    // even when native generation won the completion race.
    let terminal: Result<Event, (&'static str, String)> =
        if is_job_cancelled(&job_guard.active, job_id) {
            Ok(Event::Cancelled)
        } else {
            match result {
                Ok(outputs) => {
                    let mut error = None;
                    for output in outputs {
                        if let Err(validation) =
                            validate_output(&output, &inference_request.output_dir)
                        {
                            error = Some(format!("native output rejected: {validation:#}"));
                            break;
                        }
                        if event_tx
                            .blocking_send(Ok(Event::Output { output }))
                            .is_err()
                        {
                            error = Some("native event receiver disconnected".to_owned());
                            break;
                        }
                    }
                    error.map_or(Ok(Event::Complete), |message| Err(("internal", message)))
                }
                Err(gpq_inference::InferenceError::InvalidRequest(message)) => {
                    Err(("invalid_input", message))
                }
                Err(gpq_inference::InferenceError::OutOfMemory(message)) => {
                    Err(("out_of_memory", message))
                }
                Err(gpq_inference::InferenceError::Cancelled(_)) => Ok(Event::Cancelled),
                Err(gpq_inference::InferenceError::NativeRuntime(message)) => {
                    Err(("backend_crashed", message))
                }
                Err(gpq_inference::InferenceError::Response(error)) => {
                    Err(("backend_crashed", error.to_string()))
                }
            }
        };
    match terminal {
        Ok(event) => {
            let _ = event_tx.blocking_send(Ok(event));
        }
        Err((kind, message)) => {
            let _ = event_tx.blocking_send(Ok(Event::Error { kind, message }));
        }
    }
    drop(job_guard);
}

async fn drain_events(
    stream: &mut TcpStream,
    state: &Arc<State>,
    job_id: uuid::Uuid,
    mut event_rx: mpsc::Receiver<Result<Event, String>>,
) -> anyhow::Result<()> {
    let mut client_disconnected = false;
    while let Some(event) = event_rx.recv().await {
        let event = event.map_err(|error| {
            anyhow::anyhow!("native inference event channel returned an error: {error}")
        })?;
        let mut bytes = serde_json::to_vec(&event)?;
        bytes.extend_from_slice(b"\n\n");
        if !client_disconnected {
            let write_result = async {
                stream.write_all(b"data: ").await?;
                stream.write_all(&bytes).await
            }
            .await;
            if let Err(error) = write_result {
                tracing::debug!(error = %error, "native inference client closed event stream; cancelling execution");
                client_disconnected = true;
                request_cancel(state, job_id);
            }
        }
        let terminal = matches!(
            event,
            Event::Complete | Event::Cancelled | Event::Error { .. }
        );
        if terminal && !client_disconnected {
            let _ = stream.shutdown().await;
        }
    }
    Ok(())
}

async fn validate_request(
    state: &State,
    model_version: ContentHash,
    request: &InferenceRequest,
) -> anyhow::Result<()> {
    let model = state
        .models
        .iter()
        .find(|model| {
            model.manifest_hash == model_version
                && model.manifest.family == request.model.family
                && serde_json::to_value(resolve_model(model))
                    .ok()
                    .zip(serde_json::to_value(&request.model).ok())
                    .is_some_and(|(expected, actual)| expected == actual)
        })
        .context("request model version and resolved assets do not match configured manifest")?;
    model
        .verify_assets()
        .context("verifying configured model asset snapshots")?;
    gpq_domain::validate_seed(request.seed)
        .context("generation seed exceeds the native signed 64-bit range")?;
    request
        .parameters
        .validate_for_family(model.manifest.family)
        .context("invalid generation parameters")?;
    let expected_runtime = crate::pool::runtime_options(&state.pool);
    ensure!(
        serde_json::to_value(&expected_runtime)? == serde_json::to_value(&request.runtime)?,
        "request runtime options differ from worker configuration"
    );
    for (hash, path) in &request.extra_loras {
        let configured = state
            .extra_loras
            .get(hash)
            .with_context(|| format!("request LoRA {hash} is not configured on this pool"))?;
        ensure!(
            configured.path.as_path() == path.as_path(),
            "request LoRA {hash} resolved to an unconfigured path"
        );
        configured.snapshot.verify()?;
    }
    validate_lora_selection(&request.parameters, &request.extra_loras)?;
    validate_shape(&state.pool, &model.manifest, &request.parameters)?;
    validate_staged_inputs(&state.attempts_root, &request.parameters, &request.inputs).await?;
    validate_staged_directory(&state.attempts_root, &request.output_dir).await?;
    Ok(())
}

fn resolve_model(model: &ConfiguredModel) -> ResolvedModelFiles {
    ResolvedModelFiles {
        family: model.manifest.family,
        diffusion_model: model.asset_paths[&ModelAssetRole::Diffusion].clone(),
        text_encoder: model.asset_paths[&ModelAssetRole::TextEncoder].clone(),
        vision_encoder: model
            .asset_paths
            .get(&ModelAssetRole::VisionEncoder)
            .cloned(),
        vae: model.asset_paths[&ModelAssetRole::Vae].clone(),
        audio_vae: model.asset_paths.get(&ModelAssetRole::AudioVae).cloned(),
        default_loras: model
            .default_lora_paths
            .iter()
            .map(|lora| ResolvedLora {
                sha256: lora.sha256,
                path: lora.path.clone(),
                scale: lora.scale,
            })
            .collect(),
        sampling: model.manifest.sampling.clone(),
    }
}

fn validate_lora_selection(
    parameters: &GenerationParameters,
    extra_loras: &BTreeMap<gpq_domain::ContentHash, PathBuf>,
) -> anyhow::Result<()> {
    match parameters {
        GenerationParameters::Image(image) => {
            for lora in &image.loras {
                ensure!(
                    extra_loras.contains_key(&lora.sha256),
                    "image LoRA {} is not present in the trusted extra-LoRA map",
                    lora.sha256
                );
            }
        }
        GenerationParameters::Video(_) => ensure!(
            extra_loras.is_empty(),
            "video request cannot carry image LoRAs"
        ),
    }
    Ok(())
}

fn validate_shape(
    pool: &PoolConfig,
    manifest: &gpq_domain::ModelManifest,
    parameters: &GenerationParameters,
) -> anyhow::Result<()> {
    match parameters {
        GenerationParameters::Image(image) => {
            let pixels = u64::from(image.width)
                .checked_mul(u64::from(image.height))
                .context("image pixel count overflow")?;
            let steps = image
                .steps
                .or_else(|| {
                    image.sigmas.as_ref().map(|sigmas| {
                        u32::try_from(sigmas.len().saturating_sub(1)).unwrap_or(u32::MAX)
                    })
                })
                .unwrap_or(manifest.sampling.steps);
            ensure!(
                pixels <= pool.shape.image_pixels,
                "image exceeds pool max_image_pixels"
            );
            ensure!(steps <= pool.shape.steps, "image exceeds pool max_steps");
        }
        GenerationParameters::Video(video) => {
            let pixels = u64::from(video.width)
                .checked_mul(u64::from(video.height))
                .and_then(|pixels| pixels.checked_mul(u64::from(video.frames)))
                .context("video pixel count overflow")?;
            ensure!(
                pixels <= pool.shape.video_pixels,
                "video exceeds pool max_video_pixels"
            );
            ensure!(
                video.frames <= pool.shape.video_frames,
                "video exceeds pool max_video_frames"
            );
        }
    }
    Ok(())
}

async fn validate_staged_inputs(
    root: &Path,
    parameters: &GenerationParameters,
    inputs: &BTreeMap<ArtifactId, PathBuf>,
) -> anyhow::Result<()> {
    let mut required = BTreeSet::new();
    match parameters {
        GenerationParameters::Image(image) => required.extend(image.references.iter().copied()),
        GenerationParameters::Video(video) => {
            required.extend(video.first_frame);
            required.extend(video.last_frame);
        }
    }
    ensure!(
        required
            .iter()
            .all(|artifact| inputs.contains_key(artifact)),
        "a referenced input artifact was not staged"
    );
    ensure!(
        inputs.keys().all(|artifact| required.contains(artifact)),
        "input map contains an unreferenced artifact"
    );
    for path in inputs.values() {
        validate_staged_file(root, path).await?;
    }
    Ok(())
}

async fn validate_staged_file(root: &Path, path: &Path) -> anyhow::Result<()> {
    let root = tokio::fs::canonicalize(root).await?;
    let path = tokio::fs::canonicalize(path).await?;
    ensure!(
        path.starts_with(root),
        "staged input escaped the worker attempts directory"
    );
    ensure!(
        tokio::fs::metadata(&path).await?.is_file(),
        "staged input is not a regular file"
    );
    Ok(())
}

async fn validate_staged_directory(root: &Path, path: &Path) -> anyhow::Result<()> {
    let root = tokio::fs::canonicalize(root).await?;
    let path = tokio::fs::canonicalize(path).await?;
    ensure!(
        path.starts_with(root),
        "native output directory escaped the worker attempts directory"
    );
    ensure!(
        tokio::fs::metadata(path).await?.is_dir(),
        "native output path is not a directory"
    );
    Ok(())
}

fn validate_output(output: &NativeOutput, output_dir: &Path) -> anyhow::Result<()> {
    let output_path = std::fs::canonicalize(&output.path)?;
    let output_dir = std::fs::canonicalize(output_dir)?;
    ensure!(
        output_path.starts_with(output_dir),
        "native output escaped its attempt directory"
    );
    ensure!(output_path.is_file(), "native output is not a regular file");
    ensure!(
        output.width > 0 && output.height > 0,
        "native output dimensions are invalid"
    );
    ensure!(
        output.duration_seconds.is_none_or(f64::is_finite),
        "native output duration is invalid"
    );
    let media_matches = match output.kind {
        OutputKind::Image => output.mime_type == "image/png",
        OutputKind::Video => {
            output.mime_type == "video/webm"
                && output.frames.is_some_and(|frames| frames > 0)
                && output
                    .duration_seconds
                    .is_some_and(|duration| duration > 0.0)
        }
    };
    ensure!(
        media_matches,
        "native output kind does not match media type or metadata"
    );
    Ok(())
}

async fn read_request(stream: &mut TcpStream) -> anyhow::Result<HttpRequest> {
    let mut bytes = Vec::with_capacity(4096);
    let header_end = loop {
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        ensure!(bytes.len() < MAX_HEADER_BYTES, "HTTP headers exceed limit");
        let mut chunk = [0_u8; 2048];
        let count = stream.read(&mut chunk).await?;
        ensure!(count > 0, "connection closed before HTTP headers");
        bytes.extend_from_slice(&chunk[..count]);
    };
    let header = std::str::from_utf8(&bytes[..header_end - 4])?;
    let mut lines = header.split("\r\n");
    let mut first = lines
        .next()
        .context("missing HTTP request line")?
        .split_whitespace();
    let method = first.next().context("missing HTTP method")?.to_owned();
    let target = first.next().context("missing HTTP target")?.to_owned();
    ensure!(first.next().is_some(), "malformed HTTP request line");
    let mut headers = BTreeMap::new();
    let mut content_length = 0_usize;
    for line in lines {
        let (name, value) = line.split_once(':').context("malformed HTTP header")?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_owned();
        if name == "content-length" {
            content_length = value.parse()?;
        }
        ensure!(
            name != "transfer-encoding",
            "chunked HTTP request bodies are unsupported"
        );
        headers.insert(name, value);
    }
    ensure!(
        content_length <= MAX_REQUEST_BYTES,
        "HTTP request body exceeds limit"
    );
    let mut body = bytes.split_off(header_end);
    ensure!(body.len() <= content_length, "invalid HTTP request framing");
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = vec![0_u8; remaining.min(8192)];
        let count = stream.read(&mut chunk).await?;
        ensure!(
            count > 0,
            "connection closed before HTTP request body completed"
        );
        body.extend_from_slice(&chunk[..count]);
    }
    Ok(HttpRequest {
        method,
        target,
        headers,
        body,
    })
}

async fn write_json<T: Serialize>(
    stream: &mut TcpStream,
    status: u16,
    value: &T,
) -> io::Result<()> {
    let body = serde_json::to_vec(value).map_err(io::Error::other)?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max {
        difference |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    difference == 0
}
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, mpsc as std_mpsc};

    use gpq_domain::{
        ContentHash, GenerationParameters, ImageParameters, ModelAssetRole, ModelFamily,
    };
    use gpq_inference::{
        BackendDevice, InferenceRequest, NativeOutput, OutputKind, RuntimeOptions,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Semaphore, oneshot};

    use super::{
        State, configured_device, handle_connection, is_job_cancelled, lock_active,
        register_active_job, resolve_model, validate_output,
    };
    use crate::config::{ModelConfig, PoolConfig, RuntimeConfig, ShapeLimits};

    #[test]
    fn configured_device_accepts_cpu_and_rejects_unavailable_accelerator() -> anyhow::Result<()> {
        let devices = [BackendDevice {
            name: "cpu".to_owned(),
            description: "CPU".to_owned(),
        }];
        let pool = PoolConfig {
            key: "test-pool".to_owned(),
            backend: "cpu".to_owned(),
            listen: "127.0.0.1:4001".to_owned(),
            state_dir: PathBuf::new(),
            startup_timeout: std::time::Duration::from_secs(1),
            runtime: RuntimeConfig::default(),
            shape: ShapeLimits::default(),
            models: Vec::new(),
            extra_image_loras: Vec::new(),
            expected_hashes: BTreeMap::new(),
            env: BTreeMap::new(),
        };
        let device = configured_device(&pool, &devices)?;
        assert_eq!(device.name, "cpu");

        let missing_pool = PoolConfig {
            backend: "cuda999".to_owned(),
            ..pool
        };
        let Err(error) = configured_device(&missing_pool, &devices) else {
            anyhow::bail!("unavailable configured accelerator must fail");
        };
        anyhow::ensure!(
            format!("{error:#}").contains("cuda999"),
            "unexpected probe error: {error:#}"
        );
        Ok(())
    }

    fn test_state(root: &Path) -> (Arc<State>, InferenceRequest, ContentHash) {
        let assets = [
            (ModelAssetRole::Diffusion, "diffusion.bin"),
            (ModelAssetRole::TextEncoder, "text.bin"),
            (ModelAssetRole::VisionEncoder, "vision.bin"),
            (ModelAssetRole::Vae, "vae.bin"),
        ]
        .into_iter()
        .map(|(role, name)| {
            let path = root.join(name);
            assert!(std::fs::write(&path, name.as_bytes()).is_ok());
            (role, path)
        })
        .collect();
        let pool = PoolConfig {
            key: "test-pool".to_owned(),
            backend: "cpu".to_owned(),
            listen: "127.0.0.1:4001".to_owned(),
            state_dir: root.join("state"),
            startup_timeout: std::time::Duration::from_secs(10),
            runtime: RuntimeConfig::default(),
            shape: ShapeLimits::default(),
            models: vec![ModelConfig {
                family: ModelFamily::QwenImage21,
                assets,
                default_loras: Vec::new(),
                sampling: None,
            }],
            extra_image_loras: Vec::new(),
            expected_hashes: BTreeMap::new(),
            env: BTreeMap::new(),
        };
        let Ok((models, extra_loras)) = crate::models::scan_models(&pool) else {
            panic!("scan configured test model");
        };
        let manifest_hash = models[0].manifest_hash;
        let attempts_root = root.join("attempts");
        let output_dir = attempts_root.join("output");
        assert!(std::fs::create_dir_all(&output_dir).is_ok());
        let request = InferenceRequest {
            model: resolve_model(&models[0]),
            parameters: GenerationParameters::Image(ImageParameters {
                prompt: "test image".to_owned(),
                negative_prompt: String::new(),
                width: 32,
                height: 32,
                steps: None,
                cfg_scale: None,
                references: Vec::new(),
                loras: Vec::new(),
                sigmas: None,
            }),
            inputs: BTreeMap::new(),
            extra_loras: BTreeMap::new(),
            output_dir,
            seed: 7,
            runtime: RuntimeOptions {
                backend: "cpu".to_owned(),
                module_backends: BTreeMap::new(),
                params_backend: None,
                max_vram: None,
                threads: 1,
                vae_tiling: false,
                model_args: Vec::new(),
            },
        };
        let state = Arc::new(State {
            pool,
            models,
            extra_loras,
            token: "test-private-inference-token".to_owned(),
            attempts_root,
            execution: Arc::new(Semaphore::new(1)),
            active: Arc::new(Mutex::new(None)),
        });
        (state, request, manifest_hash)
    }

    async fn post_json(state: Arc<State>, target: &str, value: serde_json::Value) -> Vec<u8> {
        let Ok(listener) = TcpListener::bind("127.0.0.1:0").await else {
            panic!("bind test listener");
        };
        let Ok(address) = listener.local_addr() else {
            panic!("read test listener address");
        };
        let server = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                panic!("accept test connection");
            };
            if let Err(error) = handle_connection(stream, state).await {
                panic!("handle test request: {error}");
            }
        });

        let Ok(mut client) = TcpStream::connect(address).await else {
            panic!("connect test client");
        };
        let body = value.to_string();
        let request = format!(
            "POST {target} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-private-inference-token\r\nContent-Length: {}\r\n\r\n{body}",
            body.len(),
        );
        if let Err(error) = client.write_all(request.as_bytes()).await {
            panic!("write test request: {error}");
        }
        let mut response = Vec::new();
        if let Err(error) = client.read_to_end(&mut response).await {
            panic!("read test response: {error}");
        }
        if let Err(error) = server.await {
            panic!("join test server: {error}");
        }
        response
    }

    fn response_body(response: &[u8]) -> &[u8] {
        let Some(boundary) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
            panic!("HTTP response has headers");
        };
        &response[boundary + 4..]
    }

    fn assert_idle(state: &State) {
        assert!(lock_active(&state.active).is_none());
        let Ok(permit) = Arc::clone(&state.execution).try_acquire_owned() else {
            panic!("validation rejection must release the execution slot");
        };
        drop(permit);
    }

    #[tokio::test]
    async fn execute_rejections_release_slot_and_active_job() {
        let Ok(directory) = tempfile::tempdir() else {
            panic!("create test directory");
        };
        let (state, request, manifest_hash) = test_state(directory.path());
        let wrong_hash = ContentHash::digest(b"wrong manifest");
        let mut wrong_owner = request.clone();
        wrong_owner.model.diffusion_model = PathBuf::from("/not-configured/diffusion.bin");
        let mut invalid_seed = request.clone();
        invalid_seed.seed = u64::MAX;
        let cases = [
            (wrong_hash, request.clone()),
            (manifest_hash, wrong_owner),
            (manifest_hash, invalid_seed),
        ];

        for (model_version, inference) in cases {
            let job_id = uuid::Uuid::now_v7();
            let response = post_json(
                Arc::clone(&state),
                "/execute",
                serde_json::json!({
                    "job_id": job_id,
                    "model_version": model_version,
                    "inference": inference,
                }),
            )
            .await;
            assert!(response.starts_with(b"HTTP/1.1 400 Bad Request"));
            assert_idle(&state);
        }
    }

    #[tokio::test]
    async fn stale_cancel_and_release_cannot_change_the_active_job() {
        let Ok(directory) = tempfile::tempdir() else {
            panic!("create test directory");
        };
        let (state, _, _) = test_state(directory.path());
        let stale_id = uuid::Uuid::now_v7();
        let Ok(old_permit) = Arc::clone(&state.execution).try_acquire_owned() else {
            panic!("acquire old job slot");
        };
        let Ok(old_guard) = register_active_job(&state.active, stale_id, old_permit) else {
            panic!("register old job");
        };
        drop(old_guard);
        let current_id = uuid::Uuid::now_v7();
        let Ok(permit) = Arc::clone(&state.execution).try_acquire_owned() else {
            panic!("acquire current job slot");
        };
        let Ok(job_guard) = register_active_job(&state.active, current_id, permit) else {
            panic!("register current job");
        };

        let response = post_json(
            Arc::clone(&state),
            "/cancel",
            serde_json::json!({"job_id": stale_id}),
        )
        .await;
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        let Ok(cancel_response) =
            serde_json::from_slice::<serde_json::Value>(response_body(&response))
        else {
            panic!("decode cancel response");
        };
        assert_eq!(
            cancel_response["cancel_requested"],
            serde_json::Value::Bool(false)
        );
        assert!(!is_job_cancelled(&state.active, current_id));

        let response = post_json(Arc::clone(&state), "/release", serde_json::json!({})).await;
        assert!(response.starts_with(b"HTTP/1.1 200 OK"));
        let Ok(release_response) =
            serde_json::from_slice::<serde_json::Value>(response_body(&response))
        else {
            panic!("decode release response");
        };
        assert_eq!(release_response["released"], serde_json::Value::Bool(false));
        assert!(
            lock_active(&state.active)
                .as_ref()
                .is_some_and(|job| job.id == current_id)
        );

        let response = post_json(
            Arc::clone(&state),
            "/cancel",
            serde_json::json!({"job_id": current_id}),
        )
        .await;
        let Ok(cancel_response) =
            serde_json::from_slice::<serde_json::Value>(response_body(&response))
        else {
            panic!("decode active cancel response");
        };
        assert_eq!(
            cancel_response["cancel_requested"],
            serde_json::Value::Bool(true)
        );
        assert!(is_job_cancelled(&state.active, current_id));
        drop(job_guard);
        assert_idle(&state);
    }

    #[test]
    fn output_validation_rejects_artifacts_outside_attempt_directory() {
        let Ok(directory) = tempfile::tempdir() else {
            panic!("create test directory");
        };
        let output_dir = directory.path().join("attempt");
        let outside = directory.path().join("outside.png");
        assert!(std::fs::create_dir(&output_dir).is_ok());
        assert!(std::fs::write(&outside, b"not published").is_ok());
        let output = NativeOutput {
            path: outside,
            mime_type: "image/png".to_owned(),
            kind: OutputKind::Image,
            width: 32,
            height: 32,
            frames: None,
            duration_seconds: None,
        };
        assert!(validate_output(&output, &output_dir).is_err());
    }

    #[tokio::test]
    async fn blocking_work_keeps_slot_after_http_wait_is_dropped() {
        let semaphore = Arc::new(Semaphore::new(1));
        let active = Arc::new(Mutex::new(None));
        let job_id = uuid::Uuid::now_v7();
        let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
            panic!("acquire test slot");
        };
        let Ok(job_guard) = register_active_job(&active, job_id, permit) else {
            panic!("register test job");
        };
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = std_mpsc::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let blocking = tokio::task::spawn_blocking(move || {
            assert!(
                started_tx.send(()).is_ok() && finish_rx.recv().is_ok(),
                "signal blocking work lifecycle"
            );
            drop(job_guard);
            let _ = done_tx.send(());
        });
        assert!(started_rx.await.is_ok(), "blocking work started");

        let http_wait = tokio::spawn(async move {
            if let Err(error) = blocking.await {
                panic!("join blocking work: {error}");
            }
        });
        http_wait.abort();
        assert!(semaphore.clone().try_acquire_owned().is_err());
        assert!(!is_job_cancelled(&active, job_id));
        assert!(
            lock_active(&active)
                .as_ref()
                .is_some_and(|job| job.id == job_id)
        );

        assert!(
            finish_tx.send(()).is_ok() && done_rx.await.is_ok(),
            "finish blocking work"
        );
        assert!(lock_active(&active).is_none());
        let Ok(permit) = semaphore.try_acquire_owned() else {
            panic!("completed blocking work releases the slot");
        };
        drop(permit);
    }
}
