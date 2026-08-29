//! Modalities and their default execution timeouts.
//!
//! ADR 0007 unifies the Generation envelope across modalities but not their
//! parameters; ADR 0003 sets modality-specific execution timeout defaults.

use std::time::Duration;

use crate::state::state_enum;

state_enum! {
    /// The kind of result a Generation produces.
    ///
    /// Modality is derived after alias resolution, never sent by callers (ADR 0006).
    Modality {
        /// Text generation on an LLM runtime such as llama.cpp or mlx-dspark.
        Llm => "llm",
        /// Still image generation on a `ComfyUI` runtime.
        Image => "image",
        /// Video generation on a `ComfyUI` runtime.
        Video => "video",
        /// Music or audio generation on a `ComfyUI` runtime.
        Music => "music",
    }
}

impl Modality {
    /// Default Attempt execution timeout (ADR 0003).
    #[must_use]
    pub const fn default_execution_timeout(&self) -> Duration {
        match self {
            Self::Llm => Duration::from_mins(30),
            Self::Image => Duration::from_hours(2),
            Self::Video | Self::Music => Duration::from_hours(24),
        }
    }

    /// The canonical backend kind for this modality; compatible runtime kinds
    /// may also satisfy it (ADR 0005).
    #[must_use]
    pub const fn backend_kind(&self) -> BackendKind {
        match self {
            Self::Llm => BackendKind::LlamaCpp,
            Self::Image | Self::Video | Self::Music => BackendKind::ComfyUi,
        }
    }
}

state_enum! {
    /// The runtime kind occupying a Device Pool.
    ///
    /// A Pool hosts at most one Active Runtime and switches exclusively between
    /// kinds rather than colocating them (ADR 0005).
    BackendKind {
        /// A managed `llama-server` process.
        LlamaCpp => "llama_cpp",
        /// A managed `mlx-dspark serve` process.
        MlxDspark => "mlx_dspark",
        /// A managed `ComfyUI` process.
        ComfyUi => "comfyui",
    }
}

impl BackendKind {
    /// Default number of Execution Slots exposed by a fresh runtime.
    ///
    /// llama.cpp defaults to four; mlx-dspark and `ComfyUI` default to one.
    #[must_use]
    pub const fn default_slots(&self) -> u32 {
        match self {
            Self::LlamaCpp => 4,
            Self::MlxDspark | Self::ComfyUi => 1,
        }
    }

    /// Whether this runtime can execute work requiring `required`.
    ///
    /// llama.cpp and mlx-dspark are interchangeable LLM runtimes at the
    /// scheduler boundary; both serve the same pinned-model contract.
    #[must_use]
    pub const fn satisfies(self, required: Self) -> bool {
        matches!(
            (self, required),
            (
                Self::LlamaCpp | Self::MlxDspark,
                Self::LlamaCpp | Self::MlxDspark
            ) | (Self::ComfyUi, Self::ComfyUi)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_timeouts_follow_adr_0003() {
        assert_eq!(
            Modality::Llm.default_execution_timeout(),
            Duration::from_mins(30)
        );
        assert_eq!(
            Modality::Image.default_execution_timeout(),
            Duration::from_hours(2)
        );
        assert_eq!(
            Modality::Video.default_execution_timeout(),
            Duration::from_hours(24)
        );
        assert_eq!(
            Modality::Music.default_execution_timeout(),
            Duration::from_hours(24)
        );
    }

    #[test]
    fn modalities_map_to_backends() {
        assert_eq!(Modality::Llm.backend_kind(), BackendKind::LlamaCpp);
        for modality in [Modality::Image, Modality::Video, Modality::Music] {
            assert_eq!(modality.backend_kind(), BackendKind::ComfyUi);
        }
    }

    #[test]
    fn mlx_dspark_is_an_llm_compatible_single_slot_runtime() {
        assert_eq!(BackendKind::MlxDspark.default_slots(), 1);
        assert!(BackendKind::MlxDspark.satisfies(BackendKind::LlamaCpp));
        assert!(BackendKind::LlamaCpp.satisfies(BackendKind::MlxDspark));
        assert!(!BackendKind::MlxDspark.satisfies(BackendKind::ComfyUi));
    }

    #[test]
    fn names_round_trip() {
        for modality in Modality::all() {
            assert_eq!(modality.as_str().parse::<Modality>(), Ok(*modality));
        }
        for kind in BackendKind::all() {
            assert_eq!(kind.as_str().parse::<BackendKind>(), Ok(*kind));
        }
    }
}
