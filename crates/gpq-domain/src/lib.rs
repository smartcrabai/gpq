//! Domain vocabulary of the GPU Generation Queue.
//!
//! This crate holds the language of `CONTEXT.md` and the invariants decided in
//! `docs/adr`: identifiers, lifecycle state machines, failure classification,
//! retry and lease policy, execution timeouts, capability matching, and the
//! GPU-utilization-first scheduling order. It is pure logic: no database, no
//! transport, no filesystem.

pub mod artifact;
pub mod capability;
pub mod failure;
pub mod generation;
pub mod hash;
pub mod id;
pub mod lease;
pub mod modality;
pub mod schedule;
pub mod state;
pub mod tenant;
pub mod version;

pub use artifact::{
    ArtifactManifest, ArtifactPlacement, ManifestMismatch, MediaKind, OUTPUT_ARTIFACT_TTL,
    TRANSFER_CHUNK_BYTES,
};
pub use capability::{
    GenerationShape, IncapableReason, Requirement, RequirementError, ShapeLimits, SlotCapability,
    any_candidate_remains,
};
pub use failure::{FailureKind, MAX_ATTEMPTS, RetryDecision};
pub use generation::{
    ExecutionTarget, GenerationParameters, ImageParameters, MAX_IMAGE_REFERENCES, MAX_NATIVE_SEED,
    MAX_PROMPT_BYTES, ParameterValidationError, Priority, SeedOutOfRange, VideoParameters,
    validate_seed,
};
pub use hash::{ContentHash, ContentHashError, Hasher};
pub use id::{ArtifactId, AttemptId, DevicePoolId, GenerationId, SlotId, TenantId, WorkerId};
pub use lease::{HEARTBEAT_INTERVAL, LEASE_TTL, lease_expiry_from};
pub use modality::Modality;
pub use schedule::{Candidate, SlotContext, select_batch, select_next};
pub use state::{ArtifactState, AttemptState, GenerationState, TransitionError};
pub use tenant::TenantSettings;
pub use version::{
    ExecutionLimits, LoraSelection, ModelAssetRole, ModelFamily, ModelManifest, ModelManifestError,
    ResolvedSampling, SamplingError, SamplingPreset, resolve_execution_timeout,
};

#[cfg(test)]
mod tests {
    use super::*;

    /// JSONB columns and model manifests store stable serde values.
    fn assert_serde_matches<T>(value: T, expected: &str)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug + Copy,
    {
        let Ok(json) = serde_json::to_string(&value) else {
            panic!("{expected} must serialize");
        };
        assert_eq!(json, format!("\"{expected}\""));
        let Ok(parsed) = serde_json::from_str::<T>(&json) else {
            panic!("{expected} must deserialize");
        };
        assert_eq!(parsed, value);
    }

    #[test]
    fn serde_names_match_stable_names() {
        for state in GenerationState::all() {
            assert_serde_matches(*state, state.as_str());
        }
        for state in AttemptState::all() {
            assert_serde_matches(*state, state.as_str());
        }
        for state in ArtifactState::all() {
            assert_serde_matches(*state, state.as_str());
        }
        for kind in FailureKind::all() {
            assert_serde_matches(*kind, kind.as_str());
        }
        for modality in Modality::all() {
            assert_serde_matches(*modality, modality.as_str());
        }
        for kind in MediaKind::all() {
            assert_serde_matches(*kind, kind.as_str());
        }
        for placement in ArtifactPlacement::all() {
            assert_serde_matches(*placement, placement.as_str());
        }
        assert_serde_matches(ModelFamily::QwenImage21, "qwen_image_2_1");
        assert_serde_matches(ModelFamily::MinimaxH3, "minimax_h3");
        assert_serde_matches(ModelAssetRole::AudioVae, "audio_vae");
    }
}
