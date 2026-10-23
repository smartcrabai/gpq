//! Immutable native model manifests and execution timeout composition.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::hash::ContentHash;
use crate::modality::Modality;

/// Execution limits a model may declare.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct ExecutionLimits {
    /// Replaces the modality default when present.
    pub execution_timeout: Option<Duration>,
    /// Accelerator memory the model is expected to need, in bytes.
    pub estimated_memory_bytes: Option<u64>,
}

/// The native model family determines its modality.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub enum ModelFamily {
    /// Qwen Image 2.1 image generation and editing.
    #[serde(rename = "qwen_image_2_1")]
    QwenImage21,
    /// `MiniMax` H3 image-conditioned video generation.
    #[serde(rename = "minimax_h3")]
    MinimaxH3,
}

impl ModelFamily {
    /// The modality this model family supports.
    #[must_use]
    pub const fn modality(self) -> Modality {
        match self {
            Self::QwenImage21 => Modality::Image,
            Self::MinimaxH3 => Modality::Video,
        }
    }

    /// The model-family default sampler.
    #[must_use]
    pub fn default_sampling_preset(self) -> SamplingPreset {
        match self {
            Self::QwenImage21 => SamplingPreset {
                steps: 40,
                cfg_scale: 6.0,
                sigmas: Vec::new(),
            },
            Self::MinimaxH3 => SamplingPreset {
                steps: 4,
                cfg_scale: 1.0,
                sigmas: vec![1.0, 0.965_517, 0.888_889, 0.679_245, 0.0],
            },
        }
    }
}

/// A hashed asset needed by a native model runtime.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAssetRole {
    /// Diffusion model weights.
    Diffusion,
    /// Text encoder weights.
    TextEncoder,
    /// Vision encoder/projector weights for image conditioning.
    VisionEncoder,
    /// Image/video VAE weights.
    Vae,
    /// Audio VAE weights for synchronized video audio.
    AudioVae,
}

/// A selected image `LoRA` and its runtime scale.
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
pub struct LoraSelection {
    /// Content hash of the `LoRA` asset.
    pub sha256: ContentHash,
    /// Runtime multiplier; must be finite.
    pub scale: f32,
}

/// Sampling parameters pinned by a model manifest.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct SamplingPreset {
    /// Number of diffusion steps.
    pub steps: u32,
    /// Classifier-free guidance scale.
    pub cfg_scale: f32,
    /// Optional model-specific sigma schedule.
    pub sigmas: Vec<f32>,
}

/// Sampling values resolved for one native generation request.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct ResolvedSampling<'a> {
    /// Actual number of diffusion iterations.
    pub steps: u32,
    /// Effective classifier-free guidance scale.
    pub cfg_scale: f32,
    /// Selected custom schedule; empty means the runtime default schedule.
    pub sigmas: &'a [f32],
}

/// A sampling preset or override cannot be resolved.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum SamplingError {
    /// Sampling steps must be positive.
    #[error("sampling steps must be positive")]
    InvalidSteps,
    /// Guidance scale must be finite.
    #[error("sampling cfg_scale must be finite")]
    NonFiniteCfgScale,
    /// Sigma values must be finite.
    #[error("sampling sigmas must be finite")]
    NonFiniteSigma,
    /// A nonempty sigma schedule must contain at least two values.
    #[error("a nonempty sigma schedule must contain at least two values")]
    InvalidSigmaSchedule,
    /// The sigma schedule's step count cannot be represented by `u32`.
    #[error("sigma schedule step count exceeds u32")]
    SigmaStepCountOverflow,
    /// Declared steps must match the sigma schedule's step count.
    #[error("sampling steps must match sigma schedule length minus one")]
    StepsMismatch,
}

impl SamplingPreset {
    /// Resolves request overrides using the model's preset and actual schedule length.
    ///
    /// Empty request sigma lists mean no override. A selected nonempty schedule
    /// determines the iteration count and must agree with an explicit step count.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid preset or override values, or inconsistent
    /// step counts and sigma schedules.
    pub fn resolve<'a>(
        &'a self,
        steps: Option<u32>,
        cfg_scale: Option<f32>,
        sigmas: Option<&'a [f32]>,
    ) -> Result<ResolvedSampling<'a>, SamplingError> {
        self.validate()?;
        if steps == Some(0) {
            return Err(SamplingError::InvalidSteps);
        }
        if cfg_scale.is_some_and(|scale| !scale.is_finite()) {
            return Err(SamplingError::NonFiniteCfgScale);
        }

        let sigmas = sigmas
            .filter(|schedule| !schedule.is_empty())
            .unwrap_or(&self.sigmas);
        let scheduled_steps = sigma_step_count(sigmas)?;
        let steps = match scheduled_steps {
            Some(scheduled_steps) => {
                if steps.is_some_and(|requested| requested != scheduled_steps) {
                    return Err(SamplingError::StepsMismatch);
                }
                scheduled_steps
            }
            None => steps.unwrap_or(self.steps),
        };
        Ok(ResolvedSampling {
            steps,
            cfg_scale: cfg_scale.unwrap_or(self.cfg_scale),
            sigmas,
        })
    }

    fn validate(&self) -> Result<(), SamplingError> {
        if self.steps == 0 {
            return Err(SamplingError::InvalidSteps);
        }
        if !self.cfg_scale.is_finite() {
            return Err(SamplingError::NonFiniteCfgScale);
        }
        if sigma_step_count(&self.sigmas)?.is_some_and(|steps| steps != self.steps) {
            return Err(SamplingError::StepsMismatch);
        }
        Ok(())
    }
}

pub(crate) fn sigma_step_count(sigmas: &[f32]) -> Result<Option<u32>, SamplingError> {
    if sigmas.iter().any(|sigma| !sigma.is_finite()) {
        return Err(SamplingError::NonFiniteSigma);
    }
    if sigmas.is_empty() {
        return Ok(None);
    }
    if sigmas.len() < 2 {
        return Err(SamplingError::InvalidSigmaSchedule);
    }
    u32::try_from(sigmas.len() - 1)
        .map(Some)
        .map_err(|_| SamplingError::SigmaStepCountOverflow)
}

impl From<SamplingError> for ModelManifestError {
    fn from(error: SamplingError) -> Self {
        match error {
            SamplingError::InvalidSteps => Self::InvalidSamplingSteps,
            SamplingError::NonFiniteCfgScale => Self::NonFiniteSamplingScale,
            SamplingError::NonFiniteSigma => Self::NonFiniteSigma,
            SamplingError::InvalidSigmaSchedule => Self::InvalidSigmaSchedule,
            SamplingError::SigmaStepCountOverflow => Self::SigmaStepCountOverflow,
            SamplingError::StepsMismatch => Self::SamplingStepsMismatch,
        }
    }
}

/// Immutable model assets and default sampling parameters.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct ModelManifest {
    /// Native model family.
    pub family: ModelFamily,
    /// Required assets by role, identified by content hash.
    pub assets: BTreeMap<ModelAssetRole, ContentHash>,
    /// Immutable default `LoRAs`, in application order.
    pub default_loras: Vec<LoraSelection>,
    /// Sampling preset used unless typed request fields override it.
    pub sampling: SamplingPreset,
}

/// A model manifest that cannot be used by a native runtime.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum ModelManifestError {
    /// A required hashed asset is absent.
    #[error("model manifest is missing required {0:?} asset")]
    MissingAsset(ModelAssetRole),
    /// Sampling steps must be positive.
    #[error("sampling steps must be positive")]
    InvalidSamplingSteps,
    /// Sampling guidance must be finite.
    #[error("sampling cfg_scale must be finite")]
    NonFiniteSamplingScale,
    /// Sampling sigmas must be finite.
    #[error("sampling sigmas must be finite")]
    NonFiniteSigma,
    /// Default `LoRA` scales must be finite.
    #[error("default LoRA scales must be finite")]
    NonFiniteLoraScale,
    /// A nonempty sigma schedule must contain at least two values.
    #[error("a nonempty sigma schedule must contain at least two values")]
    InvalidSigmaSchedule,
    /// The sigma schedule step count cannot be represented by `u32`.
    #[error("sigma schedule step count exceeds u32")]
    SigmaStepCountOverflow,
    /// Declared steps must match the sigma schedule's step count.
    #[error("sampling steps must match sigma schedule length minus one")]
    SamplingStepsMismatch,
}

impl ModelManifest {
    /// The modality determined by this manifest's model family.
    #[must_use]
    pub const fn modality(&self) -> Modality {
        self.family.modality()
    }

    /// Validates the assets and sampling values required for native execution.
    ///
    /// # Errors
    ///
    /// Returns the first missing asset or inconsistent sampling value.
    pub fn validate(&self) -> Result<(), ModelManifestError> {
        let required_assets: &[ModelAssetRole] = match self.family {
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
        for role in required_assets {
            if !self.assets.contains_key(role) {
                return Err(ModelManifestError::MissingAsset(*role));
            }
        }
        self.sampling.validate()?;
        if self
            .default_loras
            .iter()
            .any(|lora| !lora.scale.is_finite())
        {
            return Err(ModelManifestError::NonFiniteLoraScale);
        }
        Ok(())
    }

    /// Hashes the canonical JSON representation of this manifest.
    ///
    /// Manifest asset roles are a `BTreeMap`, and default `LoRA` order is retained
    /// by its vector, so semantically identical manifests have the same hash.
    ///
    /// # Errors
    ///
    /// Returns a JSON error if a non-finite float cannot be serialized.
    pub fn canonical_hash(&self) -> Result<ContentHash, serde_json::Error> {
        serde_json::to_vec(self).map(|bytes| ContentHash::digest(&bytes))
    }
}

/// Resolves the execution timeout of an Attempt.
///
/// A model may lower or replace the modality default, and a Generation may only
/// shorten the result. The Tenant ceiling applies last and can only shorten it.
#[must_use]
pub fn resolve_execution_timeout(
    modality: Modality,
    version: ExecutionLimits,
    requested: Option<Duration>,
    tenant_ceiling: Duration,
) -> Duration {
    let base = version
        .execution_timeout
        .unwrap_or_else(|| modality.default_execution_timeout());
    let shortened = requested.map_or(base, |requested| requested.min(base));
    shortened.min(tenant_ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: Duration = Duration::from_hours(1);
    const DAY: Duration = Duration::from_hours(24);

    #[test]
    fn sampler_resolution_uses_actual_schedule_length_and_empty_override_falls_back() {
        let qwen = ModelFamily::QwenImage21;
        assert_eq!(qwen.modality(), Modality::Image);
        let qwen = qwen.default_sampling_preset();
        let default = qwen
            .resolve(None, None, None)
            .unwrap_or_else(|error| panic!("Qwen preset is valid: {error}"));
        assert_eq!(default.steps, 40);
        assert_eq!(default.cfg_scale, 6.0);
        assert_eq!(default.sigmas, Vec::<f32>::new());

        let h3 = ModelFamily::MinimaxH3.default_sampling_preset();
        assert_eq!(ModelFamily::MinimaxH3.modality(), Modality::Video);
        let resolved = h3
            .resolve(None, None, Some(&[]))
            .unwrap_or_else(|error| panic!("empty request schedule uses H3 preset: {error}"));
        assert_eq!(resolved.steps, 4);
        assert_eq!(resolved.cfg_scale, 1.0);
        assert_eq!(resolved.sigmas, h3.sigmas.as_slice());

        assert_eq!(
            h3.resolve(Some(3), None, None),
            Err(SamplingError::StepsMismatch)
        );
    }

    #[test]
    fn manifest_hash_is_stable_and_lora_order_is_identity() {
        let mut assets = BTreeMap::new();
        assets.insert(ModelAssetRole::Diffusion, ContentHash::digest(b"d"));
        assets.insert(ModelAssetRole::TextEncoder, ContentHash::digest(b"t"));
        assets.insert(ModelAssetRole::VisionEncoder, ContentHash::digest(b"v"));
        assets.insert(ModelAssetRole::Vae, ContentHash::digest(b"vae"));
        let manifest = ModelManifest {
            family: ModelFamily::QwenImage21,
            assets,
            default_loras: vec![
                LoraSelection {
                    sha256: ContentHash::digest(b"first"),
                    scale: 0.5,
                },
                LoraSelection {
                    sha256: ContentHash::digest(b"second"),
                    scale: 1.0,
                },
            ],
            sampling: ModelFamily::QwenImage21.default_sampling_preset(),
        };
        let first = manifest
            .canonical_hash()
            .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"));

        let mut reversed_asset_order = manifest.clone();
        reversed_asset_order.assets = manifest
            .assets
            .iter()
            .rev()
            .map(|(&role, &hash)| (role, hash))
            .collect();
        assert_eq!(
            reversed_asset_order
                .canonical_hash()
                .unwrap_or_else(|error| {
                    panic!("reordered fixture manifest has canonical JSON: {error}")
                }),
            first
        );

        let mut reordered = manifest.clone();
        reordered.default_loras.reverse();
        assert_ne!(
            reordered.canonical_hash().unwrap_or_else(|error| {
                panic!("reordered fixture manifest has canonical JSON: {error}")
            }),
            first
        );
    }

    #[test]
    fn manifests_validate_required_assets_sampling_consistency_and_finite_numbers() {
        let manifest = ModelManifest {
            family: ModelFamily::MinimaxH3,
            assets: BTreeMap::new(),
            default_loras: Vec::new(),
            sampling: ModelFamily::MinimaxH3.default_sampling_preset(),
        };
        assert_eq!(
            manifest.validate(),
            Err(ModelManifestError::MissingAsset(ModelAssetRole::Diffusion))
        );

        let mut manifest = manifest;
        for role in [
            ModelAssetRole::Diffusion,
            ModelAssetRole::TextEncoder,
            ModelAssetRole::Vae,
            ModelAssetRole::AudioVae,
        ] {
            manifest.assets.insert(role, ContentHash::digest(b"asset"));
        }
        manifest.sampling.cfg_scale = f32::NAN;
        assert_eq!(
            manifest.validate(),
            Err(ModelManifestError::NonFiniteSamplingScale)
        );

        manifest.sampling.cfg_scale = 1.0;
        let canonical_hash = manifest
            .canonical_hash()
            .unwrap_or_else(|error| panic!("fixture manifest has canonical JSON: {error}"));
        manifest.sampling.steps = 5;
        assert_ne!(
            manifest.canonical_hash().unwrap_or_else(|error| {
                panic!("fixture manifest identity remains serializable: {error}")
            }),
            canonical_hash
        );
        assert_eq!(
            manifest.validate(),
            Err(ModelManifestError::SamplingStepsMismatch)
        );
    }

    #[test]
    fn timeout_composition_preserves_model_request_and_tenant_caps() {
        assert_eq!(
            resolve_execution_timeout(Modality::Image, ExecutionLimits::default(), None, DAY),
            Duration::from_hours(2)
        );
        let limits = ExecutionLimits {
            execution_timeout: Some(HOUR),
            estimated_memory_bytes: None,
        };
        assert_eq!(
            resolve_execution_timeout(Modality::Video, limits, None, DAY),
            HOUR
        );
        assert_eq!(
            resolve_execution_timeout(Modality::Video, limits, Some(Duration::from_mins(1)), DAY),
            Duration::from_mins(1)
        );
        assert_eq!(
            resolve_execution_timeout(Modality::Video, limits, Some(DAY), HOUR),
            HOUR
        );
    }
}
