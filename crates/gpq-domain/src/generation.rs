//! Generation-level value types.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::hash::ContentHash;
use crate::modality::{BackendKind, Modality};
use crate::state::state_enum;

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

state_enum! {
    /// Whether the caller holds a connection open for the whole Generation.
    ///
    /// ADR 0003: Remote records this so that, on startup, nonterminal synchronous
    /// `OpenAI` Generations are cancelled before Worker sessions are accepted, while
    /// durable Native Generations keep their leases and resume.
    CallerKind {
        /// `OpenAI`-compatible HTTP or SSE request; dies with its connection.
        Synchronous => "synchronous",
        /// Native API submission; survives disconnection and Remote restart.
        Durable => "durable",
    }
}

/// The pinned execution target resolved at admission (ADR 0012).
///
/// A Generation names exactly one Model or Workflow alias; admission resolves it
/// to an immutable version hash that every Attempt must reuse.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum ExecutionTarget {
    /// A llama.cpp Model Version.
    Model {
        /// Content hash of the exact model material.
        version: ContentHash,
    },
    /// A `ComfyUI` Workflow Version.
    Workflow {
        /// Content hash of the immutable graph and output manifest.
        version: ContentHash,
    },
}

impl ExecutionTarget {
    /// The version hash regardless of target kind.
    #[must_use]
    pub const fn version(&self) -> ContentHash {
        match self {
            Self::Model { version } | Self::Workflow { version } => *version,
        }
    }

    /// The runtime kind required to execute this target.
    #[must_use]
    pub const fn backend_kind(&self) -> BackendKind {
        match self {
            Self::Model { .. } => BackendKind::LlamaCpp,
            Self::Workflow { .. } => BackendKind::ComfyUi,
        }
    }

    /// Whether the target is consistent with a derived modality.
    #[must_use]
    pub fn matches_modality(&self, modality: Modality) -> bool {
        self.backend_kind() == modality.backend_kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_range_is_enforced() {
        assert_eq!(Priority::new(0), Ok(Priority::MIN));
        assert_eq!(Priority::new(9), Ok(Priority::MAX));
        assert_eq!(Priority::new(10), Err(PriorityOutOfRange(10)));
        assert!(Priority::try_from(-1_i32).is_err());
    }

    #[test]
    fn higher_priority_sorts_last() {
        let mut values = [Priority::MAX, Priority::MIN, Priority::DEFAULT];
        values.sort_unstable();
        assert_eq!(values, [Priority::MIN, Priority::DEFAULT, Priority::MAX]);
    }

    #[test]
    fn targets_bind_to_backends() {
        let model = ExecutionTarget::Model {
            version: ContentHash::digest(b"m"),
        };
        let workflow = ExecutionTarget::Workflow {
            version: ContentHash::digest(b"w"),
        };
        assert!(model.matches_modality(Modality::Llm));
        assert!(!model.matches_modality(Modality::Image));
        assert!(workflow.matches_modality(Modality::Video));
    }
}
