//! Generation-level value types.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::hash::ContentHash;
use crate::id::ArtifactId;
use crate::modality::Modality;
use crate::version::{ModelFamily, SamplingError, sigma_step_count};

const MAX_VP8_DIMENSION: u32 = 16_383;

/// Scheduling priority, zero through nine, nine being most urgent (ADR 0006).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct Priority(u8);

/// A priority outside the accepted range.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("priority {0} is outside 0..=9")]
pub struct PriorityOutOfRange(pub u16);

impl Priority {
    /// Lowest priority.
    pub const MIN: Self = Self(0);
    /// Highest priority.
    pub const MAX: Self = Self(9);
    /// Priority used when a Generation and its Tenant say nothing.
    pub const DEFAULT: Self = Self(5);

    /// Validates and wraps a raw priority.
    ///
    /// # Errors
    ///
    /// Returns [`PriorityOutOfRange`] when `value` exceeds nine.
    pub fn new(value: u8) -> Result<Self, PriorityOutOfRange> {
        if value <= Self::MAX.0 {
            Ok(Self(value))
        } else {
            Err(PriorityOutOfRange(u16::from(value)))
        }
    }

    /// Returns the raw priority.
    #[must_use]
    pub const fn get(&self) -> u8 {
        self.0
    }
}

impl Default for Priority {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TryFrom<u8> for Priority {
    type Error = PriorityOutOfRange;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<i32> for Priority {
    type Error = PriorityOutOfRange;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        u8::try_from(value)
            .map_err(|_| {
                PriorityOutOfRange(u16::try_from(value.unsigned_abs()).unwrap_or(u16::MAX))
            })
            .and_then(Self::new)
    }
}

impl From<Priority> for u8 {
    fn from(value: Priority) -> Self {
        value.0
    }
}

impl From<Priority> for i16 {
    fn from(value: Priority) -> Self {
        Self::from(value.0)
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Maximum UTF-8 prompt size accepted by the native request boundary.
pub const MAX_PROMPT_BYTES: usize = 16 * 1024;

/// Maximum number of reference images in an image request.
pub const MAX_IMAGE_REFERENCES: usize = 10;

/// Largest deterministic seed accepted by the native C API's signed 64-bit seed.
pub const MAX_NATIVE_SEED: u64 = i64::MAX as u64;

/// A caller seed that cannot be represented by the native C API.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
#[error("seed {0} exceeds the native signed 64-bit seed range")]
pub struct SeedOutOfRange(pub u64);

/// Checks the native C API seed range without reinterpreting high bits as random.
///
/// # Errors
///
/// Returns [`SeedOutOfRange`] when the value exceeds `i64::MAX`.
pub fn validate_seed(seed: u64) -> Result<i64, SeedOutOfRange> {
    i64::try_from(seed).map_err(|_| SeedOutOfRange(seed))
}

/// A single image generation or edit request.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct ImageParameters {
    /// Prompt describing the requested image.
    pub prompt: String,
    /// Optional negative prompt.
    #[serde(default)]
    pub negative_prompt: String,
    /// Requested output width in pixels.
    pub width: u32,
    /// Requested output height in pixels.
    pub height: u32,
    /// Optional override of the model's sampling steps.
    pub steps: Option<u32>,
    /// Optional override of the model's classifier-free guidance scale.
    pub cfg_scale: Option<f32>,
    /// Reference image Artifact IDs, in conditioning order.
    #[serde(default)]
    pub references: Vec<ArtifactId>,
    /// Additional image `LoRAs` applied after the model defaults.
    #[serde(default)]
    pub loras: Vec<crate::version::LoraSelection>,
    /// Optional model-specific sigma schedule override.
    pub sigmas: Option<Vec<f32>>,
}

impl ImageParameters {
    /// Validates prompt, shape, sampling, `LoRA`, and input-reference bounds.
    ///
    /// # Errors
    ///
    /// Returns the first malformed field or out-of-range request value.
    pub fn validate(&self) -> Result<(), ParameterValidationError> {
        validate_prompt(&self.prompt, false)?;
        validate_prompt(&self.negative_prompt, true)?;
        validate_dimensions(self.width, self.height)?;
        if self.steps == Some(0) {
            return Err(ParameterValidationError::InvalidSteps);
        }
        if self.cfg_scale.is_some_and(|scale| !scale.is_finite()) {
            return Err(ParameterValidationError::NonFiniteCfgScale);
        }
        if self.references.len() > MAX_IMAGE_REFERENCES {
            return Err(ParameterValidationError::TooManyReferences);
        }
        if self.loras.iter().any(|lora| !lora.scale.is_finite()) {
            return Err(ParameterValidationError::NonFiniteLoraScale);
        }
        if self
            .sigmas
            .as_deref()
            .is_some_and(|sigmas| sigma_step_count(sigmas).is_err())
        {
            return Err(ParameterValidationError::InvalidSigmas);
        }
        Ok(())
    }
}

/// A single synchronized-audio video generation request.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct VideoParameters {
    /// Prompt describing the requested video and audio.
    pub prompt: String,
    /// Optional negative prompt.
    #[serde(default)]
    pub negative_prompt: String,
    /// Requested output width in pixels.
    pub width: u32,
    /// Requested output height in pixels.
    pub height: u32,
    /// Requested frame count; H3 supports 17k+5 frames, with k >= 0.
    pub frames: u32,
    /// Optional first-frame conditioning Artifact.
    pub first_frame: Option<ArtifactId>,
    /// Optional last-frame conditioning Artifact; requires a first frame.
    pub last_frame: Option<ArtifactId>,
}

impl VideoParameters {
    /// Validates prompt, VP8 shape, supported frame count, and image conditioning.
    ///
    /// # Errors
    ///
    /// Returns the first malformed field or out-of-range request value.
    pub fn validate(&self) -> Result<(), ParameterValidationError> {
        validate_prompt(&self.prompt, false)?;
        validate_prompt(&self.negative_prompt, true)?;
        validate_dimensions(self.width, self.height)?;
        if self.width > MAX_VP8_DIMENSION || self.height > MAX_VP8_DIMENSION {
            return Err(ParameterValidationError::VideoDimensionLimit);
        }
        if self.frames < 5 || !(self.frames - 5).is_multiple_of(17) {
            return Err(ParameterValidationError::InvalidVideoFrames);
        }
        if self.last_frame.is_some() && self.first_frame.is_none() {
            return Err(ParameterValidationError::LastFrameRequiresFirst);
        }
        Ok(())
    }
}

/// Typed native generation parameters.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GenerationParameters {
    /// Still image generation or editing.
    Image(ImageParameters),
    /// Synchronized-audio video generation.
    Video(VideoParameters),
}

impl GenerationParameters {
    /// Validates typed parameters against the selected model family.
    ///
    /// # Errors
    ///
    /// Returns an error when the family and modality disagree or a parameter is invalid.
    pub fn validate_for_family(&self, family: ModelFamily) -> Result<(), ParameterValidationError> {
        match (family, self) {
            (ModelFamily::QwenImage21, Self::Image(image)) => image.validate(),
            (ModelFamily::MinimaxH3, Self::Video(video)) => video.validate(),
            _ => Err(ParameterValidationError::FamilyModalityMismatch),
        }
    }

    /// Returns the modality implied by the typed parameter variant.
    #[must_use]
    pub const fn modality(&self) -> Modality {
        match self {
            Self::Image(_) => Modality::Image,
            Self::Video(_) => Modality::Video,
        }
    }

    /// Returns distinct input Artifact IDs in their conditioning order.
    ///
    /// Image reference ordering is retained; duplicates are removed only for
    /// transfer staging. Video stages the first frame before the last frame.
    #[must_use]
    pub fn staging_artifact_ids(&self) -> Vec<ArtifactId> {
        let mut ids = Vec::new();
        let inputs: &[ArtifactId] = match self {
            Self::Image(image) => &image.references,
            Self::Video(video) => {
                if let Some(first) = video.first_frame {
                    ids.push(first);
                }
                if let Some(last) = video.last_frame
                    && !ids.contains(&last)
                {
                    ids.push(last);
                }
                return ids;
            }
        };
        ids.reserve(inputs.len());
        for id in inputs {
            if !ids.contains(id) {
                ids.push(*id);
            }
        }
        ids
    }
}

/// Invalid typed generation parameters.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum ParameterValidationError {
    /// A required prompt is empty or whitespace-only.
    #[error("prompt must not be empty")]
    EmptyPrompt,
    /// A prompt exceeds the native request size limit.
    #[error("prompt exceeds the native request size limit")]
    PromptTooLong,
    /// Width and height must be positive multiples of 32.
    #[error("width and height must be positive multiples of 32")]
    InvalidDimensions,
    /// Explicit sampling steps must be positive.
    #[error("steps must be positive")]
    InvalidSteps,
    /// Guidance scale must be finite.
    #[error("cfg_scale must be finite")]
    NonFiniteCfgScale,
    /// Requests are limited to ten reference image Artifacts.
    #[error("image requests support at most ten references")]
    TooManyReferences,
    /// `LoRA` scales must be finite.
    #[error("LoRA scales must be finite")]
    NonFiniteLoraScale,
    /// Sigma schedules must be finite and empty or contain at least two values.
    #[error("sigma schedule must be finite and contain at least two values when nonempty")]
    InvalidSigmas,
    /// An explicit step count does not match the selected sigma schedule.
    #[error("steps must match the selected sigma schedule length minus one")]
    SamplingStepsMismatch,
    /// Video frame counts must have the form 17k+5.
    #[error("video frame count must be at least 5 and satisfy 17k+5")]
    InvalidVideoFrames,
    /// Video dimensions must fit within the VP8 maximum axis.
    #[error("video dimensions exceed the VP8 maximum axis")]
    VideoDimensionLimit,
    /// Last-frame conditioning requires first-frame conditioning.
    #[error("last_frame requires first_frame")]
    LastFrameRequiresFirst,
    /// The model family does not support the selected generation modality.
    #[error("model family does not support the requested modality")]
    FamilyModalityMismatch,
}

impl From<SamplingError> for ParameterValidationError {
    fn from(error: SamplingError) -> Self {
        match error {
            SamplingError::InvalidSteps => Self::InvalidSteps,
            SamplingError::NonFiniteCfgScale => Self::NonFiniteCfgScale,
            SamplingError::NonFiniteSigma
            | SamplingError::InvalidSigmaSchedule
            | SamplingError::SigmaStepCountOverflow => Self::InvalidSigmas,
            SamplingError::StepsMismatch => Self::SamplingStepsMismatch,
        }
    }
}

fn validate_prompt(prompt: &str, may_be_empty: bool) -> Result<(), ParameterValidationError> {
    if prompt.len() > MAX_PROMPT_BYTES {
        return Err(ParameterValidationError::PromptTooLong);
    }
    if !may_be_empty && prompt.trim().is_empty() {
        return Err(ParameterValidationError::EmptyPrompt);
    }
    Ok(())
}

fn validate_dimensions(width: u32, height: u32) -> Result<(), ParameterValidationError> {
    if width == 0 || height == 0 || !width.is_multiple_of(32) || !height.is_multiple_of(32) {
        return Err(ParameterValidationError::InvalidDimensions);
    }
    Ok(())
}

/// The pinned native model target resolved at admission.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct ExecutionTarget {
    version: ContentHash,
    modality: Modality,
}

impl ExecutionTarget {
    /// Creates the pinned target with the modality derived from its model family.
    #[must_use]
    pub const fn for_model(version: ContentHash, family: ModelFamily) -> Self {
        Self {
            version,
            modality: family.modality(),
        }
    }

    /// The immutable manifest content hash.
    #[must_use]
    pub const fn version(&self) -> ContentHash {
        self.version
    }

    /// The modality derived from the model family.
    #[must_use]
    pub const fn modality(&self) -> Modality {
        self.modality
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image_parameters() -> ImageParameters {
        ImageParameters {
            prompt: "a sunset".to_owned(),
            negative_prompt: String::new(),
            width: 512,
            height: 512,
            steps: None,
            cfg_scale: None,
            references: Vec::new(),
            loras: Vec::new(),
            sigmas: None,
        }
    }

    #[test]
    fn priority_range_and_order_are_preserved() {
        assert_eq!(Priority::new(0), Ok(Priority::MIN));
        assert_eq!(Priority::new(9), Ok(Priority::MAX));
        assert_eq!(Priority::new(10), Err(PriorityOutOfRange(10)));
        assert!(Priority::try_from(-1_i32).is_err());
        let mut values = [Priority::MAX, Priority::MIN, Priority::DEFAULT];
        values.sort_unstable();
        assert_eq!(values, [Priority::MIN, Priority::DEFAULT, Priority::MAX]);
    }

    #[test]
    fn seed_conversion_accepts_the_native_boundary_and_rejects_overflow() {
        assert_eq!(validate_seed(0), Ok(0));
        assert_eq!(validate_seed(MAX_NATIVE_SEED), Ok(i64::MAX));
        assert_eq!(
            validate_seed(MAX_NATIVE_SEED + 1),
            Err(SeedOutOfRange(MAX_NATIVE_SEED + 1))
        );
    }

    #[test]
    fn target_pins_version_and_derives_modality_from_family() {
        let version = ContentHash::digest(b"manifest");
        let target = ExecutionTarget::for_model(version, ModelFamily::MinimaxH3);
        assert_eq!(target.version(), version);
        assert_eq!(target.modality(), Modality::Video);
    }

    #[test]
    fn image_request_validates_shape_and_sampling_and_bounds_references() {
        let mut image = image_parameters();
        assert_eq!(image.validate(), Ok(()));

        image.width = 513;
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::InvalidDimensions)
        );

        image.width = 512;
        image.cfg_scale = Some(f32::INFINITY);
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::NonFiniteCfgScale)
        );

        image.cfg_scale = None;
        image.references = (0..=MAX_IMAGE_REFERENCES)
            .map(|_| ArtifactId::new())
            .collect();
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::TooManyReferences)
        );
    }

    #[test]
    fn image_request_bounds_prompts_and_rejects_invalid_steps_and_lora_scales() {
        let mut image = image_parameters();
        image.prompt = "   ".to_owned();
        assert_eq!(image.validate(), Err(ParameterValidationError::EmptyPrompt));

        image.prompt = "x".repeat(MAX_PROMPT_BYTES + 1);
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::PromptTooLong)
        );

        image.prompt = "a landscape".to_owned();
        image.steps = Some(0);
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::InvalidSteps)
        );

        image.steps = None;
        image.loras = vec![crate::LoraSelection {
            sha256: ContentHash::digest(b"style"),
            scale: f32::NAN,
        }];
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::NonFiniteLoraScale)
        );
    }

    #[test]
    fn nonempty_sigma_schedule_requires_at_least_two_finite_values() {
        let mut image = image_parameters();
        image.sigmas = Some(vec![1.0]);
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::InvalidSigmas)
        );

        image.sigmas = Some(vec![1.0, f32::INFINITY, 0.0]);
        assert_eq!(
            image.validate(),
            Err(ParameterValidationError::InvalidSigmas)
        );
    }

    #[test]
    fn video_request_enforces_h3_frame_and_conditioning_rules() {
        let mut video = VideoParameters {
            prompt: "a moving scene".to_owned(),
            negative_prompt: String::new(),
            width: 512,
            height: 512,
            frames: 22,
            first_frame: Some(ArtifactId::new()),
            last_frame: Some(ArtifactId::new()),
        };
        assert_eq!(video.validate(), Ok(()));
        video.frames = 23;
        assert_eq!(
            video.validate(),
            Err(ParameterValidationError::InvalidVideoFrames)
        );

        video.frames = 22;
        video.first_frame = None;
        assert_eq!(
            video.validate(),
            Err(ParameterValidationError::LastFrameRequiresFirst)
        );
    }

    #[test]
    fn video_request_respects_vp8_axis_limit() {
        let mut video = VideoParameters {
            prompt: "a moving scene".to_owned(),
            negative_prompt: String::new(),
            width: 16_384,
            height: 32,
            frames: 5,
            first_frame: None,
            last_frame: None,
        };
        assert_eq!(
            video.validate(),
            Err(ParameterValidationError::VideoDimensionLimit)
        );

        video.width = 16_352;
        assert_eq!(video.validate(), Ok(()));

        video.width = 32;
        video.height = 16_384;
        assert_eq!(
            video.validate(),
            Err(ParameterValidationError::VideoDimensionLimit)
        );
    }

    #[test]
    fn family_validation_rejects_modality_mismatch() {
        let image = GenerationParameters::Image(image_parameters());
        assert_eq!(image.validate_for_family(ModelFamily::QwenImage21), Ok(()));
        assert_eq!(
            image.validate_for_family(ModelFamily::MinimaxH3),
            Err(ParameterValidationError::FamilyModalityMismatch)
        );
    }

    #[test]
    fn staging_deduplicates_without_reordering_image_references() {
        let first = ArtifactId::new();
        let second = ArtifactId::new();
        let image = GenerationParameters::Image(ImageParameters {
            references: vec![first, second, first],
            ..image_parameters()
        });
        assert_eq!(image.staging_artifact_ids(), vec![first, second]);
    }
}
