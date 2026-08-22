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
        /// Text generation on a llama.cpp runtime.
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

    /// The backend kind that executes this modality (ADR 0005).
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
    /// kinds rather than colocating both (ADR 0005).
    BackendKind {
        /// A managed `llama-server` process.
        LlamaCpp => "llama_cpp",
        /// A managed `ComfyUI` process.
        ComfyUi => "comfyui",
    }
}

impl BackendKind {
    /// Default number of Execution Slots exposed by a fresh runtime.
    ///
    /// llama.cpp exposes several Slots through continuous batching; `ComfyUI`
    /// normally exposes one (ADR 0005).
    #[must_use]
    pub const fn default_slots(&self) -> u32 {
        match self {
            Self::LlamaCpp => 4,
            Self::ComfyUi => 1,
        }
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
    fn names_round_trip() {
        for modality in Modality::all() {
            assert_eq!(modality.as_str().parse::<Modality>(), Ok(*modality));
        }
        for kind in BackendKind::all() {
            assert_eq!(kind.as_str().parse::<BackendKind>(), Ok(*kind));
        }
    }
}
