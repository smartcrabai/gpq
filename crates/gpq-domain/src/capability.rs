//! Native model, `LoRA`, and shape capability matching.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::generation::{GenerationParameters, ParameterValidationError};
use crate::hash::ContentHash;
use crate::id::{DevicePoolId, SlotId, TenantId, WorkerId};
use crate::modality::Modality;
use crate::version::{LoraSelection, ModelManifest, ModelManifestError};

/// The validated, model-specific shape of a generation request.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct GenerationShape {
    /// Requested output width in pixels.
    pub width: u32,
    /// Requested output height in pixels.
    pub height: u32,
    /// Effective sampling steps after applying the model preset.
    pub steps: Option<u32>,
    /// Requested video frame count; absent for image requests.
    pub frames: Option<u32>,
}

/// Per-slot native shape limits; absent values mean no configured limit.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct ShapeLimits {
    /// Maximum pixels in a still image.
    pub max_image_pixels: Option<u64>,
    /// Maximum total pixels in a video (width × height × frames).
    pub max_video_pixels: Option<u64>,
    /// Maximum video frames.
    pub max_video_frames: Option<u32>,
    /// Maximum sampling steps.
    pub max_steps: Option<u32>,
}

/// What a Generation needs from a native model slot.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Requirement {
    /// Owning Tenant.
    pub tenant_id: TenantId,
    /// Modality determined by the pinned model family.
    pub modality: Modality,
    /// Canonical hash of the immutable model manifest.
    pub version: ContentHash,
    /// Additional image `LoRA` assets required by this request.
    pub required_loras: BTreeSet<ContentHash>,
    /// Validated request shape and effective sampling steps.
    pub shape: GenerationShape,
}

/// A malformed model or request that cannot form a scheduling requirement.
#[derive(Debug, thiserror::Error)]
pub enum RequirementError {
    /// The model manifest is missing required runtime assets or has invalid sampling.
    #[error(transparent)]
    ModelManifest(#[from] ModelManifestError),
    /// The request parameters do not match the model family or violate validation.
    #[error(transparent)]
    Parameters(#[from] ParameterValidationError),
    /// The manifest could not be encoded as canonical JSON for its identity hash.
    #[error("model manifest cannot be serialized for canonical hashing: {0}")]
    ManifestHash(#[from] serde_json::Error),
    /// The requested model version is not the supplied manifest's canonical identity.
    #[error("model manifest hashes to {actual}, not requested version {requested}")]
    ManifestHashMismatch {
        /// Requested pinned content hash.
        requested: ContentHash,
        /// Canonical hash of the supplied manifest.
        actual: ContentHash,
    },
}

impl Requirement {
    /// Builds a scheduling requirement from the pinned model and typed parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest, model hash, or generation parameters are invalid.
    pub fn for_generation(
        tenant_id: TenantId,
        version: ContentHash,
        manifest: &ModelManifest,
        parameters: &GenerationParameters,
    ) -> Result<Self, RequirementError> {
        manifest.validate()?;
        parameters.validate_for_family(manifest.family)?;
        let actual = manifest.canonical_hash()?;
        if actual != version {
            return Err(RequirementError::ManifestHashMismatch {
                requested: version,
                actual,
            });
        }
        let shape = match parameters {
            GenerationParameters::Image(image) => {
                let sampling = manifest
                    .sampling
                    .resolve(image.steps, image.cfg_scale, image.sigmas.as_deref())
                    .map_err(ParameterValidationError::from)?;
                GenerationShape {
                    width: image.width,
                    height: image.height,
                    steps: Some(sampling.steps),
                    frames: None,
                }
            }
            GenerationParameters::Video(video) => {
                let sampling = manifest
                    .sampling
                    .resolve(None, None, None)
                    .map_err(ParameterValidationError::from)?;
                GenerationShape {
                    width: video.width,
                    height: video.height,
                    steps: Some(sampling.steps),
                    frames: Some(video.frames),
                }
            }
        };
        let required_loras = match parameters {
            GenerationParameters::Image(image) => image
                .loras
                .iter()
                .map(|selection: &LoraSelection| selection.sha256)
                .collect(),
            GenerationParameters::Video(_) => BTreeSet::new(),
        };
        Ok(Self {
            tenant_id,
            modality: manifest.modality(),
            version,
            required_loras,
            shape,
        })
    }
}

/// What one native execution slot advertises about itself.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct SlotCapability {
    /// Owning Tenant of the Worker.
    pub tenant_id: TenantId,
    /// The advertising Worker.
    pub worker_id: WorkerId,
    /// The Device Pool exposing this slot.
    pub pool_id: DevicePoolId,
    /// Slot identity.
    pub slot_id: SlotId,
    /// Observed accelerator/device selector, such as `metal` or `vulkan0`.
    pub accelerator: String,
    /// Observed native runtime version.
    pub accelerator_version: String,
    /// Available immutable model manifests keyed by their canonical hashes.
    pub model_manifests: BTreeMap<ContentHash, ModelManifest>,
    /// Separately available image `LoRA` assets keyed by content hash.
    pub image_loras: BTreeSet<ContentHash>,
    /// Model manifest currently resident in the pool, if any.
    pub resident_model: Option<ContentHash>,
    /// Accelerator memory reported by the backend, when known.
    pub accelerator_memory_bytes: Option<u64>,
    /// Configured shape limits for this pool.
    pub shape_limits: ShapeLimits,
}

/// Why a Slot cannot run a Generation.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, thiserror::Error)]
pub enum IncapableReason {
    /// The Slot belongs to a different Tenant.
    #[error("slot belongs to another tenant")]
    TenantMismatch,
    /// A pinned model manifest is not present on the host.
    #[error("model version {0} is not installed")]
    MissingModel(ContentHash),
    /// The advertised manifest's family does not match the required modality.
    #[error("model manifest provides {found}, but {required} is required")]
    ModelModalityMismatch {
        /// Modality required by the Generation.
        required: Modality,
        /// Modality provided by the manifest.
        found: Modality,
    },
    /// A selected image `LoRA` is not usable on this slot.
    #[error("image LoRA {0} is not installed")]
    MissingLora(ContentHash),
    /// The request shape cannot be accepted by the selected pool.
    #[error("request shape is invalid for native execution")]
    InvalidShape,
    /// The still image exceeds the pool's pixel limit.
    #[error("image has {requested} pixels, pool limit is {maximum}")]
    ImagePixelLimit {
        /// Requested pixels.
        requested: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// The video exceeds the pool's total-pixel limit.
    #[error("video has {requested} total pixels, pool limit is {maximum}")]
    VideoPixelLimit {
        /// Requested width × height × frames.
        requested: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// The video exceeds the pool's frame limit.
    #[error("video has {requested} frames, pool limit is {maximum}")]
    VideoFrameLimit {
        /// Requested frame count.
        requested: u32,
        /// Configured maximum.
        maximum: u32,
    },
    /// The request exceeds the pool's sampling-step limit.
    #[error("request uses {requested} steps, pool limit is {maximum}")]
    StepLimit {
        /// Requested sampling steps.
        requested: u32,
        /// Configured maximum.
        maximum: u32,
    },
}

impl SlotCapability {
    /// Decides whether this Slot may execute a Generation.
    ///
    /// Reported accelerator memory is informational; admission gates only on
    /// pinned assets, selected `LoRAs`, and configured shape limits.
    ///
    /// # Errors
    ///
    /// Returns the [`IncapableReason`] that excludes this Slot.
    pub fn admits(&self, requirement: &Requirement) -> Result<(), IncapableReason> {
        if self.tenant_id != requirement.tenant_id {
            return Err(IncapableReason::TenantMismatch);
        }
        let Some(manifest) = self.model_manifests.get(&requirement.version) else {
            return Err(IncapableReason::MissingModel(requirement.version));
        };
        if manifest.modality() != requirement.modality {
            return Err(IncapableReason::ModelModalityMismatch {
                required: requirement.modality,
                found: manifest.modality(),
            });
        }
        for lora in &requirement.required_loras {
            if !self.image_loras.contains(lora) {
                return Err(IncapableReason::MissingLora(*lora));
            }
        }
        self.admits_shape(requirement)?;
        Ok(())
    }

    fn admits_shape(&self, requirement: &Requirement) -> Result<(), IncapableReason> {
        let shape = requirement.shape;
        if shape.width == 0
            || shape.height == 0
            || !shape.width.is_multiple_of(32)
            || !shape.height.is_multiple_of(32)
            || shape.steps.is_none_or(|steps| steps == 0)
        {
            return Err(IncapableReason::InvalidShape);
        }
        let image_pixels = u64::from(shape.width) * u64::from(shape.height);
        match requirement.modality {
            Modality::Image => {
                if shape.frames.is_some() {
                    return Err(IncapableReason::InvalidShape);
                }
                if let Some(maximum) = self.shape_limits.max_image_pixels
                    && image_pixels > maximum
                {
                    return Err(IncapableReason::ImagePixelLimit {
                        requested: image_pixels,
                        maximum,
                    });
                }
            }
            Modality::Video => {
                let Some(frames) = shape
                    .frames
                    .filter(|frames| *frames >= 5 && (*frames - 5) % 17 == 0)
                else {
                    return Err(IncapableReason::InvalidShape);
                };
                if let Some(maximum) = self.shape_limits.max_video_frames
                    && frames > maximum
                {
                    return Err(IncapableReason::VideoFrameLimit {
                        requested: frames,
                        maximum,
                    });
                }
                let video_pixels = image_pixels.saturating_mul(u64::from(frames));
                if let Some(maximum) = self.shape_limits.max_video_pixels
                    && video_pixels > maximum
                {
                    return Err(IncapableReason::VideoPixelLimit {
                        requested: video_pixels,
                        maximum,
                    });
                }
            }
        }
        if let Some(maximum) = self.shape_limits.max_steps
            && let Some(steps) = shape.steps
            && steps > maximum
        {
            return Err(IncapableReason::StepLimit {
                requested: steps,
                maximum,
            });
        }
        Ok(())
    }

    /// Whether the Slot already holds the requirement's exact Model in memory.
    ///
    /// Cache-aware scheduling prefers these Slots.
    #[must_use]
    pub fn holds_resident_model(&self, requirement: &Requirement) -> bool {
        self.resident_model == Some(requirement.version)
    }
}

/// Whether any registered Slot is still a candidate for a requirement.
///
/// A Generation fails after a candidate-specific failure only when no
/// registered Slot remains capable.
#[must_use]
pub fn any_candidate_remains<'a>(
    slots: impl IntoIterator<Item = &'a SlotCapability>,
    requirement: &Requirement,
) -> bool {
    slots
        .into_iter()
        .any(|slot| slot.admits(requirement).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{ImageParameters, VideoParameters};
    use crate::version::{ModelAssetRole, ModelFamily};

    fn manifest(family: ModelFamily) -> ModelManifest {
        let roles: &[ModelAssetRole] = match family {
            ModelFamily::QwenImage21 => &[
                ModelAssetRole::Diffusion,
                ModelAssetRole::TextEncoder,
                ModelAssetRole::VisionEncoder,
                ModelAssetRole::Vae,
            ],
            ModelFamily::MinimaxH3 => &[
                ModelAssetRole::Diffusion,
                ModelAssetRole::TextEncoder,
                ModelAssetRole::Vae,
                ModelAssetRole::AudioVae,
            ],
        };
        ModelManifest {
            family,
            assets: roles
                .iter()
                .map(|role| (*role, ContentHash::digest(&[*role as u8])))
                .collect(),
            default_loras: Vec::new(),
            sampling: family.default_sampling_preset(),
        }
    }

    fn image_parameters(loras: Vec<LoraSelection>) -> GenerationParameters {
        GenerationParameters::Image(ImageParameters {
            prompt: "a landscape".to_owned(),
            negative_prompt: String::new(),
            width: 512,
            height: 512,
            steps: None,
            cfg_scale: None,
            references: Vec::new(),
            loras,
            sigmas: None,
        })
    }

    fn video_parameters() -> GenerationParameters {
        GenerationParameters::Video(VideoParameters {
            prompt: "a moving landscape".to_owned(),
            negative_prompt: String::new(),
            width: 512,
            height: 512,
            frames: 22,
            first_frame: None,
            last_frame: None,
        })
    }

    fn requirement(
        tenant_id: TenantId,
        manifest: &ModelManifest,
        parameters: &GenerationParameters,
    ) -> Requirement {
        let version = manifest
            .canonical_hash()
            .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"));
        Requirement::for_generation(tenant_id, version, manifest, parameters)
            .unwrap_or_else(|error| panic!("fixture request is valid: {error}"))
    }

    fn slot(tenant_id: TenantId) -> SlotCapability {
        SlotCapability {
            tenant_id,
            worker_id: WorkerId::new(),
            pool_id: DevicePoolId::new(),
            slot_id: SlotId::new(),
            accelerator: "vulkan0".to_owned(),
            accelerator_version: "native-test".to_owned(),
            model_manifests: BTreeMap::new(),
            image_loras: BTreeSet::new(),
            resident_model: None,
            accelerator_memory_bytes: None,
            shape_limits: ShapeLimits::default(),
        }
    }

    fn add_model(slot: &mut SlotCapability, model: ModelManifest) -> ContentHash {
        let version = model
            .canonical_hash()
            .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"));
        slot.model_manifests.insert(version, model);
        version
    }

    #[test]
    fn rejects_other_tenants_and_requires_the_exact_manifest_hash() {
        let tenant = TenantId::new();
        let manifest = manifest(ModelFamily::QwenImage21);
        let request = image_parameters(Vec::new());

        assert!(matches!(
            Requirement::for_generation(
                tenant,
                ContentHash::digest(b"wrong manifest hash"),
                &manifest,
                &request,
            ),
            Err(RequirementError::ManifestHashMismatch { .. })
        ));

        let requirement = requirement(tenant, &manifest, &request);
        let mut slot = slot(TenantId::new());
        add_model(&mut slot, manifest.clone());
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::TenantMismatch)
        );

        slot.tenant_id = tenant;
        slot.model_manifests.clear();
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::MissingModel(requirement.version))
        );
        add_model(&mut slot, manifest);
        assert_eq!(slot.admits(&requirement), Ok(()));
    }

    #[test]
    fn image_loras_must_be_separately_advertised() {
        let tenant = TenantId::new();
        let lora = ContentHash::digest(b"style");
        let manifest = manifest(ModelFamily::QwenImage21);
        let request = image_parameters(vec![LoraSelection {
            sha256: lora,
            scale: 0.7,
        }]);
        let requirement = requirement(tenant, &manifest, &request);
        let mut slot = slot(tenant);
        add_model(&mut slot, manifest);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::MissingLora(lora))
        );
        slot.image_loras.insert(lora);
        assert_eq!(slot.admits(&requirement), Ok(()));
    }

    #[test]
    fn image_shape_and_sampling_limits_exclude_unsupported_requests() {
        let tenant = TenantId::new();
        let manifest = manifest(ModelFamily::QwenImage21);
        let request = image_parameters(Vec::new());
        let requirement = requirement(tenant, &manifest, &request);
        let mut slot = slot(tenant);
        add_model(&mut slot, manifest);
        slot.shape_limits = ShapeLimits {
            max_image_pixels: Some(512 * 512),
            max_video_pixels: None,
            max_video_frames: None,
            max_steps: Some(40),
        };
        assert_eq!(slot.admits(&requirement), Ok(()));

        slot.shape_limits.max_image_pixels = Some(512 * 512 - 1);
        assert!(matches!(
            slot.admits(&requirement),
            Err(IncapableReason::ImagePixelLimit { .. })
        ));
        slot.shape_limits.max_image_pixels = None;
        slot.shape_limits.max_steps = Some(39);
        assert!(matches!(
            slot.admits(&requirement),
            Err(IncapableReason::StepLimit {
                requested: 40,
                maximum: 39,
            })
        ));
    }

    #[test]
    fn effective_schedule_steps_drive_image_admission_limits() {
        let tenant = TenantId::new();
        let mut qwen = manifest(ModelFamily::QwenImage21);
        qwen.sampling.steps = 8;
        let request = image_parameters(Vec::new());
        let qwen_requirement = requirement(tenant, &qwen, &request);
        assert_eq!(qwen_requirement.shape.steps, Some(8));

        let mut slot = slot(tenant);
        add_model(&mut slot, qwen.clone());
        slot.shape_limits.max_steps = Some(8);
        assert_eq!(slot.admits(&qwen_requirement), Ok(()));

        let mut request = image_parameters(Vec::new());
        let GenerationParameters::Image(image) = &mut request else {
            unreachable!();
        };
        image.sigmas = Some(vec![1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]);
        let long_schedule = requirement(tenant, &qwen, &request);
        assert_eq!(long_schedule.shape.steps, Some(10));
        slot.shape_limits.max_steps = Some(9);
        assert_eq!(
            slot.admits(&long_schedule),
            Err(IncapableReason::StepLimit {
                requested: 10,
                maximum: 9,
            })
        );
    }

    #[test]
    fn explicit_steps_must_match_selected_schedule_and_empty_sigmas_fall_back() {
        let tenant = TenantId::new();
        let qwen = manifest(ModelFamily::QwenImage21);
        let mut request = image_parameters(Vec::new());
        let GenerationParameters::Image(image) = &mut request else {
            unreachable!();
        };
        image.steps = Some(1);
        image.sigmas = Some(vec![1.0, 0.5, 0.0]);
        assert!(matches!(
            Requirement::for_generation(
                tenant,
                qwen.canonical_hash()
                    .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}")),
                &qwen,
                &request,
            ),
            Err(RequirementError::Parameters(
                ParameterValidationError::SamplingStepsMismatch
            ))
        ));

        let mut qwen = qwen;
        qwen.sampling.steps = 3;
        qwen.sampling.sigmas = vec![1.0, 0.75, 0.25, 0.0];
        let mut request = image_parameters(Vec::new());
        let GenerationParameters::Image(image) = &mut request else {
            unreachable!();
        };
        image.sigmas = Some(Vec::new());
        let requirement = requirement(tenant, &qwen, &request);
        assert_eq!(requirement.shape.steps, Some(3));

        let mut slot = slot(tenant);
        add_model(&mut slot, qwen);
        slot.shape_limits.max_steps = Some(3);
        assert_eq!(slot.admits(&requirement), Ok(()));
    }

    #[test]
    fn video_shape_limits_cover_total_pixels_and_frame_count() {
        let tenant = TenantId::new();
        let manifest = manifest(ModelFamily::MinimaxH3);
        let request = video_parameters();
        let requirement = requirement(tenant, &manifest, &request);
        let mut slot = slot(tenant);
        add_model(&mut slot, manifest);
        slot.shape_limits = ShapeLimits {
            max_image_pixels: None,
            max_video_pixels: Some(512 * 512 * 22),
            max_video_frames: Some(22),
            max_steps: Some(4),
        };
        assert_eq!(slot.admits(&requirement), Ok(()));
        slot.shape_limits.max_video_pixels = Some(512 * 512 * 22 - 1);
        assert!(matches!(
            slot.admits(&requirement),
            Err(IncapableReason::VideoPixelLimit { .. })
        ));
        slot.shape_limits.max_video_pixels = Some(512 * 512 * 22);
        slot.shape_limits.max_video_frames = Some(21);
        assert_eq!(
            slot.admits(&requirement),
            Err(IncapableReason::VideoFrameLimit {
                requested: 22,
                maximum: 21,
            })
        );
    }

    #[test]
    fn reported_memory_does_not_gate_admission() {
        let tenant = TenantId::new();
        let manifest = manifest(ModelFamily::QwenImage21);
        let request = image_parameters(Vec::new());
        let requirement = requirement(tenant, &manifest, &request);
        let mut slot = slot(tenant);
        add_model(&mut slot, manifest);

        for memory in [None, Some(0), Some(1024)] {
            slot.accelerator_memory_bytes = memory;
            assert_eq!(slot.admits(&requirement), Ok(()));
        }
    }
}
