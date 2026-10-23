use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString, c_char, c_void};
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};

pub use gpq_domain::{
    ArtifactId, ContentHash, GenerationParameters, ImageParameters, LoraSelection, ModelAssetRole,
    ModelFamily, ModelManifest, SamplingPreset, VideoParameters,
};
use gpq_domain::{MAX_PROMPT_BYTES, validate_seed};
use serde::{Deserialize, Serialize};
use thiserror::Error;
const MAX_IMAGE_DIMENSION: u32 = 16_384;
const MAX_IMAGE_PIXELS: u64 = 134_217_728;

#[derive(Debug, Error)]
pub enum InferenceError {
    #[error("invalid inference request: {0}")]
    InvalidRequest(String),
    #[error("native inference ran out of memory: {0}")]
    OutOfMemory(String),
    #[error("native inference was cancelled: {0}")]
    Cancelled(String),
    #[error("native inference failed: {0}")]
    NativeRuntime(String),
    #[error("native response could not be decoded: {0}")]
    Response(#[from] serde_json::Error),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedLora {
    pub sha256: ContentHash,
    pub path: PathBuf,
    pub scale: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolvedModelFiles {
    pub family: ModelFamily,
    pub diffusion_model: PathBuf,
    pub text_encoder: PathBuf,
    pub vision_encoder: Option<PathBuf>,
    pub vae: PathBuf,
    pub audio_vae: Option<PathBuf>,
    pub default_loras: Vec<ResolvedLora>,
    pub sampling: SamplingPreset,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeOptions {
    /// Default native device selector, e.g. `vulkan0`, `metal`/`metal0`, `MTL0`, or `cpu`.
    pub backend: String,
    /// Native module-specific overrides (`diffusion`, `te`, `clip_vision`, `vae`).
    #[serde(default)]
    pub module_backends: BTreeMap<String, String>,
    /// Native parameter residency: a device assignment, `disk`, or `None` for auto-fit.
    pub params_backend: Option<String>,
    /// Native GiB limit: scalar budget `24` or per-device, e.g. `vulkan0=20,metal0=32`.
    pub max_vram: Option<String>,
    pub threads: u32,
    pub vae_tiling: bool,
    #[serde(default)]
    pub model_args: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub model: ResolvedModelFiles,
    pub parameters: GenerationParameters,
    pub inputs: BTreeMap<ArtifactId, PathBuf>,
    pub extra_loras: BTreeMap<ContentHash, PathBuf>,
    pub output_dir: PathBuf,
    pub seed: u64,
    pub runtime: RuntimeOptions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    Image,
    Video,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeOutput {
    pub path: PathBuf,
    pub mime_type: String,
    pub kind: OutputKind,
    pub width: u32,
    pub height: u32,
    pub frames: Option<u32>,
    pub duration_seconds: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendDevice {
    /// The device identifier accepted by the native backend selector.
    pub name: String,
    /// Native GGML device description, including its physical-device identity.
    pub description: String,
}

const NATIVE_STATUS_OK: u32 = 0;
const NATIVE_STATUS_INVALID_REQUEST: u32 = 1;
const NATIVE_STATUS_OUT_OF_MEMORY: u32 = 2;
const NATIVE_STATUS_CANCELLED: u32 = 3;
fn native_failure(status: u32, message: String) -> InferenceError {
    match status {
        NATIVE_STATUS_INVALID_REQUEST => InferenceError::InvalidRequest(message),
        NATIVE_STATUS_OUT_OF_MEMORY => InferenceError::OutOfMemory(message),
        NATIVE_STATUS_CANCELLED => InferenceError::Cancelled(message),
        _ => InferenceError::NativeRuntime(message),
    }
}

type NativeProgress = unsafe extern "C" fn(f32, *mut c_void);

unsafe extern "C" {
    fn gpq_native_generate(
        request_json: *const c_char,
        progress: Option<NativeProgress>,
        progress_data: *mut c_void,
        output_json: *mut *mut c_char,
        error: *mut *mut c_char,
    ) -> u32;
    fn gpq_native_list_devices(output_json: *mut *mut c_char, error: *mut *mut c_char) -> u32;
    fn gpq_native_cancel_current() -> bool;
    fn gpq_native_free_string(value: *mut c_char);
}

struct NativeString(*mut c_char);

impl NativeString {
    fn into_string(self) -> Result<String, InferenceError> {
        if self.0.is_null() {
            return Err(InferenceError::NativeRuntime(
                "native function returned a null string".into(),
            ));
        }
        let value = unsafe { CStr::from_ptr(self.0) }
            .to_string_lossy()
            .into_owned();
        Ok(value)
    }
}

impl Drop for NativeString {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { gpq_native_free_string(self.0) };
        }
    }
}

struct ProgressState<F> {
    callback: F,
    panicked: bool,
}

unsafe extern "C" fn progress_trampoline<F: FnMut(f32)>(value: f32, data: *mut c_void) {
    if data.is_null() {
        return;
    }
    let state = unsafe { &mut *data.cast::<ProgressState<F>>() };
    if state.panicked {
        return;
    }
    if catch_unwind(AssertUnwindSafe(|| (state.callback)(value))).is_err() {
        state.panicked = true;
        let _ = unsafe { gpq_native_cancel_current() };
    }
}

/// Generates an image or an H3 audio-video result synchronously in the current process.
///
/// # Errors
///
/// Returns an error for invalid requests, native generation failures, progress callback
/// panics, request serialization failures, or native response decoding failures.
pub fn generate<F>(
    request: &InferenceRequest,
    progress: F,
) -> Result<Vec<NativeOutput>, InferenceError>
where
    F: FnMut(f32),
{
    let request = validate_and_resolve(request)?;
    let json = serde_json::to_string(&request)?;
    let json = CString::new(json)
        .map_err(|_| InferenceError::InvalidRequest("native request contains a NUL byte".into()))?;
    let mut output = std::ptr::null_mut();
    let mut error = std::ptr::null_mut();
    let mut state = ProgressState {
        callback: progress,
        panicked: false,
    };
    let status = unsafe {
        gpq_native_generate(
            json.as_ptr(),
            Some(progress_trampoline::<F>),
            (&raw mut state).cast(),
            &raw mut output,
            &raw mut error,
        )
    };
    let response = NativeString(output);
    let native_error = NativeString(error);
    if state.panicked {
        return Err(InferenceError::NativeRuntime(
            "progress callback panicked".into(),
        ));
    }
    if status != NATIVE_STATUS_OK {
        let message = native_error
            .into_string()
            .unwrap_or_else(|error| error.to_string());
        return Err(native_failure(status, message));
    }
    let response = response.into_string()?;
    Ok(serde_json::from_str(&response)?)
}

/// Returns native GGML device names and physical-device descriptions.
///
/// # Errors
///
/// Returns an error if native enumeration fails or its JSON response cannot be decoded.
pub fn list_devices(backend: &str) -> Result<Vec<BackendDevice>, InferenceError> {
    let mut output = std::ptr::null_mut();
    let mut error = std::ptr::null_mut();
    let status = unsafe { gpq_native_list_devices(&raw mut output, &raw mut error) };
    let response = NativeString(output);
    let native_error = NativeString(error);
    if status != NATIVE_STATUS_OK {
        let message = native_error
            .into_string()
            .unwrap_or_else(|error| error.to_string());
        return Err(native_failure(status, message));
    }
    let mut devices: Vec<BackendDevice> = serde_json::from_str(&response.into_string()?)?;
    if backend.is_empty() {
        return Ok(devices);
    }
    let exact_name = backend.chars().last().is_some_and(|ch| ch.is_ascii_digit());
    devices.retain(|device| {
        if exact_name {
            device.name.eq_ignore_ascii_case(backend)
        } else {
            device
                .name
                .get(..backend.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(backend))
        }
    });
    Ok(devices)
}

/// Requests interruption of the one active native generation, if any.
#[must_use]
pub fn cancel_current() -> bool {
    unsafe { gpq_native_cancel_current() }
}

#[derive(Serialize)]
struct WireRequest {
    family: &'static str,
    model: WireModel,
    runtime: WireRuntime,
    generation: WireGeneration,
    output_dir: PathBuf,
    seed: i64,
}

#[derive(Serialize)]
struct WireModel {
    diffusion_model: PathBuf,
    text_encoder: PathBuf,
    vision_encoder: Option<PathBuf>,
    vae: PathBuf,
    audio_vae: Option<PathBuf>,
    default_loras: Vec<WireLora>,
    steps: u32,
    cfg_scale: f32,
    sigmas: Vec<f32>,
}

#[derive(Clone, Serialize)]
struct WireLora {
    sha256: ContentHash,
    path: PathBuf,
    scale: f32,
}

#[derive(Serialize)]
struct WireRuntime {
    backend: String,
    params_backend: Option<String>,
    max_vram: Option<String>,
    threads: u32,
    vae_tiling: bool,
    model_args: String,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WireGeneration {
    Image {
        prompt: String,
        negative_prompt: String,
        width: u32,
        height: u32,
        references: Vec<PathBuf>,
        loras: Vec<WireLora>,
    },
    Video {
        prompt: String,
        negative_prompt: String,
        width: u32,
        height: u32,
        frames: u32,
        first_frame: Option<PathBuf>,
        last_frame: Option<PathBuf>,
    },
}

fn validate_and_resolve(request: &InferenceRequest) -> Result<WireRequest, InferenceError> {
    let seed = validate_seed(request.seed)
        .map_err(|error| InferenceError::InvalidRequest(error.to_string()))?;
    request
        .parameters
        .validate_for_family(request.model.family)
        .map_err(|error| InferenceError::InvalidRequest(error.to_string()))?;
    match (&request.model.family, &request.parameters) {
        (ModelFamily::QwenImage21, GenerationParameters::Image(_))
        | (ModelFamily::MinimaxH3, GenerationParameters::Video(_)) => {}
        _ => {
            return Err(InferenceError::InvalidRequest(
                "generation modality does not match the pinned model family".into(),
            ));
        }
    }

    let runtime = validate_runtime(request)?;
    let mut seen_loras = BTreeSet::new();
    let default_loras = validate_model_assets(request, &mut seen_loras)?;
    let (generation, steps, cfg_scale, sigmas) = match &request.parameters {
        GenerationParameters::Image(parameters) => {
            validate_image_generation(request, parameters, &mut seen_loras)?
        }
        GenerationParameters::Video(parameters) => validate_video_generation(request, parameters)?,
    };
    let wire_model = WireModel {
        diffusion_model: request.model.diffusion_model.clone(),
        text_encoder: request.model.text_encoder.clone(),
        vision_encoder: request.model.vision_encoder.clone(),
        vae: request.model.vae.clone(),
        audio_vae: request.model.audio_vae.clone(),
        default_loras,
        steps,
        cfg_scale,
        sigmas,
    };
    let model_args = request.runtime.model_args.join(",");
    fs::create_dir_all(&request.output_dir).map_err(|error| {
        InferenceError::InvalidRequest(format!(
            "cannot create output directory {}: {error}",
            request.output_dir.display()
        ))
    })?;

    Ok(WireRequest {
        family: match request.model.family {
            ModelFamily::QwenImage21 => "qwen_image_2_1",
            ModelFamily::MinimaxH3 => "minimax_h3",
        },
        model: wire_model,
        runtime: WireRuntime {
            backend: runtime.backend,
            params_backend: runtime.params_backend,
            max_vram: runtime.max_vram,
            threads: request.runtime.threads,
            vae_tiling: request.runtime.vae_tiling,
            model_args,
        },
        generation,
        output_dir: request.output_dir.clone(),
        seed,
    })
}

fn validate_runtime(
    request: &InferenceRequest,
) -> Result<RuntimeDeviceAssignments, InferenceError> {
    let devices = list_devices("")?;
    let runtime = resolve_runtime_device_assignments(
        &request.runtime.backend,
        &request.runtime.module_backends,
        request.runtime.params_backend.as_deref(),
        request.runtime.max_vram.as_deref(),
        &devices,
    )?;
    if request.runtime.threads == 0 || request.runtime.threads > i32::MAX as u32 {
        return invalid("thread count must be within 1..=i32::MAX");
    }
    for argument in &request.runtime.model_args {
        if argument.is_empty() || argument.contains('\0') || argument.contains(',') {
            return invalid("model_args entries must be nonempty and cannot contain NUL or comma");
        }
    }
    Ok(runtime)
}

fn validate_model_assets(
    request: &InferenceRequest,
    seen_loras: &mut BTreeSet<ContentHash>,
) -> Result<Vec<WireLora>, InferenceError> {
    for path in [
        Some(request.model.diffusion_model.as_path()),
        Some(request.model.text_encoder.as_path()),
        Some(request.model.vae.as_path()),
        request.model.vision_encoder.as_deref(),
        request.model.audio_vae.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        require_file(path, "model asset")?;
    }
    if request.model.family == ModelFamily::MinimaxH3 && request.model.audio_vae.is_none() {
        return invalid("MiniMax-H3 video requires an audio VAE");
    }
    let mut default_loras = Vec::with_capacity(request.model.default_loras.len());
    for lora in &request.model.default_loras {
        require_file(&lora.path, "default LoRA")?;
        if !lora.scale.is_finite() || !seen_loras.insert(lora.sha256) {
            return invalid("default LoRA scales must be finite and hashes unique");
        }
        default_loras.push(WireLora {
            sha256: lora.sha256,
            path: lora.path.clone(),
            scale: lora.scale,
        });
    }
    Ok(default_loras)
}

fn validate_image_generation(
    request: &InferenceRequest,
    parameters: &ImageParameters,
    seen_loras: &mut BTreeSet<ContentHash>,
) -> Result<(WireGeneration, u32, f32, Vec<f32>), InferenceError> {
    validate_prompt(&parameters.prompt, &parameters.negative_prompt)?;
    validate_dimensions(parameters.width, parameters.height)?;
    if parameters.references.len() > 10 {
        return invalid("an image request can contain at most ten references");
    }
    if !parameters.references.is_empty() && request.model.vision_encoder.is_none() {
        return invalid("Qwen Image editing requires its vision projector asset");
    }
    let references = resolve_images(&parameters.references, &request.inputs)?;
    let mut loras = Vec::with_capacity(parameters.loras.len());
    for selection in &parameters.loras {
        if !selection.scale.is_finite() || !seen_loras.insert(selection.sha256) {
            return invalid("request LoRA scales must be finite and hashes unique");
        }
        let path = request.extra_loras.get(&selection.sha256).ok_or_else(|| {
            InferenceError::InvalidRequest(format!(
                "no worker-resolved file exists for LoRA {}",
                selection.sha256
            ))
        })?;
        require_file(path, "request LoRA")?;
        loras.push(WireLora {
            sha256: selection.sha256,
            path: path.clone(),
            scale: selection.scale,
        });
    }
    let (steps, cfg_scale, sigmas) = resolve_sampling(
        &request.model.sampling,
        parameters.steps,
        parameters.cfg_scale,
        parameters.sigmas.as_deref(),
    )?;
    Ok((
        WireGeneration::Image {
            prompt: parameters.prompt.clone(),
            negative_prompt: parameters.negative_prompt.clone(),
            width: parameters.width,
            height: parameters.height,
            references,
            loras,
        },
        steps,
        cfg_scale,
        sigmas,
    ))
}

fn validate_video_generation(
    request: &InferenceRequest,
    parameters: &VideoParameters,
) -> Result<(WireGeneration, u32, f32, Vec<f32>), InferenceError> {
    parameters
        .validate()
        .map_err(|error| InferenceError::InvalidRequest(error.to_string()))?;
    validate_pixel_limit(parameters.width, parameters.height)?;
    if parameters.frames > i32::MAX as u32 {
        return invalid("video frame count exceeds the native API range");
    }
    let first_frame = parameters
        .first_frame
        .map(|id| resolve_image(id, &request.inputs))
        .transpose()?;
    let last_frame = parameters
        .last_frame
        .map(|id| resolve_image(id, &request.inputs))
        .transpose()?;
    if request.model.audio_vae.is_none() {
        return invalid("audio-video generation requires an audio VAE");
    }
    let (steps, cfg_scale, sigmas) = resolve_sampling(&request.model.sampling, None, None, None)?;
    Ok((
        WireGeneration::Video {
            prompt: parameters.prompt.clone(),
            negative_prompt: parameters.negative_prompt.clone(),
            width: parameters.width,
            height: parameters.height,
            frames: parameters.frames,
            first_frame,
            last_frame,
        },
        steps,
        cfg_scale,
        sigmas,
    ))
}

fn resolve_sampling(
    preset: &SamplingPreset,
    steps: Option<u32>,
    cfg_scale: Option<f32>,
    sigmas: Option<&[f32]>,
) -> Result<(u32, f32, Vec<f32>), InferenceError> {
    let sampling = preset
        .resolve(steps, cfg_scale, sigmas)
        .map_err(|error| InferenceError::InvalidRequest(error.to_string()))?;
    if sampling.steps > i32::MAX as u32 || sampling.sigmas.len() > i32::MAX as usize {
        return invalid("effective sampling values exceed the native API range");
    }
    Ok((sampling.steps, sampling.cfg_scale, sampling.sigmas.to_vec()))
}

fn backend_assignment(
    backend: &str,
    module_backends: &BTreeMap<String, String>,
    devices: &[BackendDevice],
) -> Result<String, InferenceError> {
    let default = resolve_device(backend, devices)?;
    if module_backends.is_empty() {
        return Ok(default.to_owned());
    }
    let mut assignment = format!("all={default}");
    for (module, device) in module_backends {
        validate_module_backend(module)?;
        assignment.push(',');
        assignment.push_str(module);
        assignment.push('=');
        assignment.push_str(resolve_device(device, devices)?);
    }
    Ok(assignment)
}

fn validate_module_backend(module: &str) -> Result<(), InferenceError> {
    if matches!(module, "diffusion" | "te" | "clip_vision" | "vae") {
        Ok(())
    } else {
        invalid(&format!(
            "unsupported native module backend selector: {module}"
        ))
    }
}

struct RuntimeDeviceAssignments {
    backend: String,
    params_backend: Option<String>,
    max_vram: Option<String>,
}

fn resolve_runtime_device_assignments(
    backend: &str,
    module_backends: &BTreeMap<String, String>,
    params_backend: Option<&str>,
    max_vram: Option<&str>,
    devices: &[BackendDevice],
) -> Result<RuntimeDeviceAssignments, InferenceError> {
    Ok(RuntimeDeviceAssignments {
        backend: backend_assignment(backend, module_backends, devices)?,
        params_backend: params_backend
            .map(|assignment| validate_params_backend(assignment, devices))
            .transpose()?,
        max_vram: max_vram
            .map(|assignment| validate_max_vram(assignment, devices))
            .transpose()?,
    })
}

/// Validates configured native device assignments against enumerated GGML device IDs.
///
/// # Errors
///
/// Returns `InvalidRequest` when an assignment is malformed or selects an unavailable device.
pub fn validate_device_assignments(
    backend: &str,
    module_backends: &BTreeMap<String, String>,
    params_backend: Option<&str>,
    max_vram: Option<&str>,
    devices: &[BackendDevice],
) -> Result<(), InferenceError> {
    resolve_runtime_device_assignments(backend, module_backends, params_backend, max_vram, devices)
        .map(drop)
}

/// Resolves a selector to a borrowed enumerated GGML device ID.
///
/// # Errors
///
/// Returns `InvalidRequest` when the selector is malformed or unavailable.
pub fn resolve_device_id<'a>(
    requested: &str,
    devices: &'a [BackendDevice],
) -> Result<&'a str, InferenceError> {
    resolve_device(requested, devices)
}

fn validate_params_backend(
    assignment: &str,
    devices: &[BackendDevice],
) -> Result<String, InferenceError> {
    if assignment.trim().is_empty() {
        return invalid("params_backend cannot be empty");
    }
    let mut normalized = Vec::with_capacity(assignment.matches(',').count() + 1);
    for item in assignment.split(',') {
        let (module, device) = item.split_once('=').unwrap_or(("", item));
        if !module.is_empty()
            && !matches!(
                module,
                "all" | "default" | "*" | "diffusion" | "te" | "clip_vision" | "vae"
            )
        {
            return invalid(&format!(
                "unsupported params_backend module selector: {module}"
            ));
        }
        let device = if device == "disk" {
            "disk"
        } else {
            resolve_device(device, devices)?
        };
        if module.is_empty() {
            normalized.push(device.to_owned());
        } else {
            normalized.push(format!("{module}={device}"));
        }
    }
    Ok(normalized.join(","))
}

fn validate_max_vram(
    assignment: &str,
    devices: &[BackendDevice],
) -> Result<String, InferenceError> {
    if assignment.trim().is_empty() {
        return invalid("max_vram cannot be empty");
    }
    if !assignment.contains('=') {
        if assignment.parse::<u32>().is_ok_and(|amount| amount > 0) {
            return Ok(assignment.to_owned());
        }
        return invalid("max_vram scalar GiB value must be a positive integer");
    }
    let mut selected = BTreeSet::new();
    let mut normalized = Vec::with_capacity(assignment.matches(',').count() + 1);
    for item in assignment.split(',') {
        let Some((device, amount)) = item.split_once('=') else {
            return invalid("max_vram entries must use device=GiB form");
        };
        let device = resolve_device(device, devices)?;
        if !selected.insert(device) {
            return invalid("max_vram cannot assign a device more than once");
        }
        let parsed = amount
            .parse::<f32>()
            .map_err(|_| InferenceError::InvalidRequest("max_vram GiB value is invalid".into()))?;
        if !parsed.is_finite() {
            return invalid("max_vram GiB values must be finite");
        }
        normalized.push(format!("{device}={amount}"));
    }
    Ok(normalized.join(","))
}

fn resolve_device<'a>(
    requested: &str,
    devices: &'a [BackendDevice],
) -> Result<&'a str, InferenceError> {
    if requested.is_empty() || requested.contains('&') {
        return invalid(&format!("selected native device {requested:?} is invalid"));
    }
    if let Some(device) = devices
        .iter()
        .find(|device| device.name.eq_ignore_ascii_case(requested))
    {
        return Ok(&device.name);
    }

    let alias =
        if requested.eq_ignore_ascii_case("metal") || requested.eq_ignore_ascii_case("metal0") {
            Some("MTL0")
        } else if requested.eq_ignore_ascii_case("vulkan") {
            Some("vulkan0")
        } else if requested.eq_ignore_ascii_case("cuda") {
            Some("cuda0")
        } else {
            None
        };
    alias
        .and_then(|alias| {
            devices
                .iter()
                .find(|device| device.name.eq_ignore_ascii_case(alias))
        })
        .map(|device| device.name.as_str())
        .ok_or_else(|| {
            InferenceError::InvalidRequest(format!(
                "selected native device {requested:?} is unavailable"
            ))
        })
}

fn validate_prompt(prompt: &str, negative_prompt: &str) -> Result<(), InferenceError> {
    if prompt.trim().is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        return invalid(&format!(
            "prompt must be nonempty and no longer than {MAX_PROMPT_BYTES} UTF-8 bytes"
        ));
    }
    if negative_prompt.len() > MAX_PROMPT_BYTES {
        return invalid(&format!(
            "negative prompt exceeds {MAX_PROMPT_BYTES} UTF-8 bytes"
        ));
    }
    Ok(())
}

fn validate_dimensions(width: u32, height: u32) -> Result<(), InferenceError> {
    if width == 0 || height == 0 || !width.is_multiple_of(32) || !height.is_multiple_of(32) {
        return invalid("image dimensions must be positive multiples of 32");
    }
    if width > MAX_IMAGE_DIMENSION || height > MAX_IMAGE_DIMENSION {
        return invalid("requested image dimensions exceed native memory-safe bounds");
    }
    validate_pixel_limit(width, height)
}

fn validate_pixel_limit(width: u32, height: u32) -> Result<(), InferenceError> {
    if u64::from(width) * u64::from(height) > MAX_IMAGE_PIXELS {
        return invalid("requested image dimensions exceed native memory-safe bounds");
    }
    Ok(())
}

fn resolve_images(
    ids: &[ArtifactId],
    inputs: &BTreeMap<ArtifactId, PathBuf>,
) -> Result<Vec<PathBuf>, InferenceError> {
    ids.iter().map(|id| resolve_image(*id, inputs)).collect()
}

fn resolve_image(
    id: ArtifactId,
    inputs: &BTreeMap<ArtifactId, PathBuf>,
) -> Result<PathBuf, InferenceError> {
    let path = inputs.get(&id).ok_or_else(|| {
        InferenceError::InvalidRequest(format!("worker did not stage referenced artifact {id}"))
    })?;
    require_file(path, "referenced image")?;
    Ok(path.clone())
}

fn require_file(path: &Path, role: &str) -> Result<(), InferenceError> {
    let metadata = fs::metadata(path).map_err(|error| {
        InferenceError::InvalidRequest(format!("cannot access {role} {}: {error}", path.display()))
    })?;
    if !metadata.is_file() {
        return invalid(&format!("{role} {} is not a regular file", path.display()));
    }
    Ok(())
}

fn invalid<T>(message: &str) -> Result<T, InferenceError> {
    Err(InferenceError::InvalidRequest(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_device_assignments_use_native_ids_and_reject_unavailable_aliases() {
        let devices = [
            BackendDevice {
                name: "MTL0".into(),
                description: "Apple M4 Max".into(),
            },
            BackendDevice {
                name: "cpu".into(),
                description: "CPU".into(),
            },
        ];
        let modules = BTreeMap::from([
            ("diffusion".into(), "mTl0".into()),
            ("te".into(), "CPU".into()),
        ]);
        let assignments = resolve_runtime_device_assignments(
            "metal",
            &modules,
            Some("all=metal0,te=cpu"),
            Some("metal0=32"),
            &devices,
        )
        .unwrap_or_else(|error| panic!("valid device assignments should resolve: {error}"));
        assert_eq!(assignments.backend, "all=MTL0,diffusion=MTL0,te=cpu");
        assert_eq!(
            assignments.params_backend.as_deref(),
            Some("all=MTL0,te=cpu")
        );
        assert_eq!(assignments.max_vram.as_deref(), Some("MTL0=32"));
        let disk_and_scalar = resolve_runtime_device_assignments(
            "cpu",
            &BTreeMap::new(),
            Some("disk"),
            Some("24"),
            &devices,
        )
        .unwrap_or_else(|error| panic!("disk and scalar VRAM assignments should resolve: {error}"));
        assert_eq!(disk_and_scalar.backend, "cpu");
        assert_eq!(disk_and_scalar.params_backend.as_deref(), Some("disk"));
        assert_eq!(disk_and_scalar.max_vram.as_deref(), Some("24"));
        let cpu_only = [BackendDevice {
            name: "cpu".into(),
            description: "CPU".into(),
        }];
        assert!(
            validate_device_assignments("metal0", &BTreeMap::new(), None, None, &cpu_only).is_err()
        );
    }

    #[test]
    fn video_limits_are_rejected_before_native_generation() {
        let parameters = VideoParameters {
            prompt: "scene".into(),
            negative_prompt: String::new(),
            width: 16_384,
            height: 32,
            frames: 5,
            first_frame: None,
            last_frame: None,
        };
        let request = InferenceRequest {
            model: ResolvedModelFiles {
                family: ModelFamily::MinimaxH3,
                diffusion_model: PathBuf::new(),
                text_encoder: PathBuf::new(),
                vision_encoder: None,
                vae: PathBuf::new(),
                audio_vae: None,
                default_loras: Vec::new(),
                sampling: ModelFamily::MinimaxH3.default_sampling_preset(),
            },
            parameters: GenerationParameters::Video(parameters.clone()),
            inputs: BTreeMap::new(),
            extra_loras: BTreeMap::new(),
            output_dir: PathBuf::new(),
            seed: 0,
            runtime: RuntimeOptions {
                backend: "cpu".into(),
                module_backends: BTreeMap::new(),
                params_backend: None,
                max_vram: None,
                threads: 1,
                vae_tiling: false,
                model_args: Vec::new(),
            },
        };
        let Err(error) = validate_video_generation(&request, &parameters) else {
            panic!("video dimensions unsupported by VP8 must not reach native generation");
        };
        assert!(matches!(
            error,
            InferenceError::InvalidRequest(message)
                if message == "video dimensions exceed the VP8 maximum axis"
        ));
        let excessive_pixels = VideoParameters {
            width: 16_352,
            height: 16_352,
            ..parameters
        };
        let Err(error) = validate_video_generation(&request, &excessive_pixels) else {
            panic!("unsafe per-frame pixel counts must not reach native generation");
        };
        assert!(matches!(
            error,
            InferenceError::InvalidRequest(message)
                if message == "requested image dimensions exceed native memory-safe bounds"
        ));
    }
}
