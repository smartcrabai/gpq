//! Supported native generation modalities and their default execution timeouts.

use std::time::Duration;

use crate::state::state_enum;

state_enum! {
    /// The kind of result a Generation produces.
    Modality {
        /// Still image generation.
        Image => "image",
        /// Video generation with synchronized audio.
        Video => "video",
    }
}

impl Modality {
    /// Default Attempt execution timeout (ADR 0003).
    #[must_use]
    pub const fn default_execution_timeout(&self) -> Duration {
        match self {
            Self::Image => Duration::from_hours(2),
            Self::Video => Duration::from_hours(24),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_modalities_keep_their_timeout_defaults() {
        assert_eq!(
            Modality::Image.default_execution_timeout(),
            Duration::from_hours(2)
        );
        assert_eq!(
            Modality::Video.default_execution_timeout(),
            Duration::from_hours(24)
        );
    }

    #[test]
    fn stable_names_round_trip() {
        for modality in Modality::all() {
            assert_eq!(modality.as_str().parse::<Modality>(), Ok(*modality));
        }
    }
}
