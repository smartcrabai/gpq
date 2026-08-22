//! Tenant-configurable policy.
//!
//! ADR 0002 makes `maximum_queue_age` (default 30 minutes) the starvation guard
//! for the utilization-first scheduler. ADR 0006 exposes queue age, capacity,
//! Artifact limits, timeout ceilings, and default priority through the
//! Master-Key-authenticated Tenant service.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::generation::Priority;

/// Mutable per-Tenant limits.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct TenantSettings {
    /// Age after which a queued Generation is scheduled ahead of cache-friendly
    /// work, preventing indefinite starvation (ADR 0002).
    pub maximum_queue_age: Duration,
    /// Maximum number of simultaneously nonterminal Generations.
    pub max_queued_generations: u32,
    /// Largest accepted input Artifact.
    pub max_input_artifact_bytes: u64,
    /// Largest accepted output Artifact.
    pub max_output_artifact_bytes: u64,
    /// Upper bound applied to any resolved execution timeout.
    pub execution_timeout_ceiling: Duration,
    /// Priority assigned when a Generation does not specify one.
    pub default_priority: Priority,
}

impl TenantSettings {
    /// Default queue age guard (ADR 0002).
    pub const DEFAULT_MAXIMUM_QUEUE_AGE: Duration = Duration::from_mins(30);

    /// Whether a Generation queued for `age` counts as overdue.
    #[must_use]
    pub fn is_overdue(&self, age: Duration) -> bool {
        age >= self.maximum_queue_age
    }
}

impl Default for TenantSettings {
    fn default() -> Self {
        Self {
            maximum_queue_age: Self::DEFAULT_MAXIMUM_QUEUE_AGE,
            max_queued_generations: 1000,
            max_input_artifact_bytes: 256 * 1024 * 1024,
            max_output_artifact_bytes: 2 * 1024 * 1024 * 1024,
            // 24h: the longest modality default (video, music) is admissible
            // unless an operator lowers the ceiling.
            execution_timeout_ceiling: Duration::from_hours(24),
            default_priority: Priority::DEFAULT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_queue_age_is_thirty_minutes() {
        assert_eq!(
            TenantSettings::default().maximum_queue_age,
            Duration::from_mins(30)
        );
    }

    #[test]
    fn overdue_is_inclusive_of_the_boundary() {
        let settings = TenantSettings::default();
        assert!(!settings.is_overdue(Duration::from_secs(1799)));
        assert!(settings.is_overdue(Duration::from_mins(30)));
    }
}
