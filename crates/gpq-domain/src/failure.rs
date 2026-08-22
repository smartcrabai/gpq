//! Failure classification and the authoritative retry policy.
//!
//! ADR 0003: Workers normalize backend failures to a closed enum plus a retry
//! hint; Remote applies the retry policy from the enum rather than parsing
//! backend text. Automatic retries cover Worker loss, lease expiry, backend
//! crashes, transient transfers, and candidate-specific runtime OOM, and stop
//! after three Attempts.

use crate::state::state_enum;

/// Maximum number of Attempts per Generation, including the first (ADR 0003).
pub const MAX_ATTEMPTS: u32 = 3;

state_enum! {
    /// The normalized cause of a failed Attempt.
    FailureKind {
        /// Request payload, prompt, or Artifact is unusable.
        InvalidInput => "invalid_input",
        /// The backend cannot do what the Generation asks for.
        UnsupportedCapability => "unsupported_capability",
        /// The pinned Model Version is not present on the executing Worker.
        ModelUnavailable => "model_unavailable",
        /// The runtime ran out of accelerator memory for this candidate.
        OutOfMemory => "out_of_memory",
        /// The managed backend process died.
        BackendCrashed => "backend_crashed",
        /// Execution exceeded the resolved execution timeout.
        ExecutionTimedOut => "execution_timed_out",
        /// Cooperative cancellation completed.
        Cancelled => "cancelled",
        /// An Artifact transfer failed transiently.
        TransferFailed => "transfer_failed",
        /// Anything else; diagnostic detail lives in the raw error text.
        Internal => "internal",
        /// Remote observed the Worker session disappear (Remote-originated).
        WorkerLost => "worker_lost",
        /// The Attempt's lease expired without heartbeats (Remote-originated).
        LeaseExpired => "lease_expired",
    }
}

impl FailureKind {
    /// Whether this cause is eligible for an automatic retry at all.
    ///
    /// The Worker's retry hint never widens this set; it is diagnostic only.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::OutOfMemory
                | Self::BackendCrashed
                | Self::TransferFailed
                | Self::WorkerLost
                | Self::LeaseExpired
        )
    }

    /// Whether the failure invalidates the executing Slot's claimed capability.
    ///
    /// A runtime OOM proves the Slot cannot host this Model or Workflow, so that
    /// Slot is marked incapable and the retry must look elsewhere (ADR 0003).
    #[must_use]
    pub const fn invalidates_slot_capability(&self) -> bool {
        matches!(self, Self::OutOfMemory)
    }

    /// Whether the failure means no candidate could ever run the Generation.
    #[must_use]
    pub const fn is_capability_failure(&self) -> bool {
        matches!(
            self,
            Self::UnsupportedCapability | Self::ModelUnavailable | Self::OutOfMemory
        )
    }

    /// Decides what Remote does after an Attempt fails with this cause.
    ///
    /// `attempts_used` counts Attempts already created for the Generation,
    /// including the one that just failed. `eligible_candidates_remain` answers
    /// whether any registered Slot is still considered capable; a Generation
    /// fails for VRAM insufficiency only after every candidate is known
    /// incapable (ADR 0003).
    #[must_use]
    pub fn retry_decision(
        &self,
        attempts_used: u32,
        eligible_candidates_remain: bool,
    ) -> RetryDecision {
        if !self.is_retryable() {
            return RetryDecision::Fail;
        }
        if attempts_used >= MAX_ATTEMPTS {
            return RetryDecision::Fail;
        }
        if self.invalidates_slot_capability() && !eligible_candidates_remain {
            return RetryDecision::Fail;
        }
        RetryDecision::Requeue
    }
}

/// What Remote does with a Generation after an Attempt fails.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RetryDecision {
    /// Return the Generation to `Queued` so another Slot creates a new Attempt.
    Requeue,
    /// Settle the Generation as `Failed`.
    Fail,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_failures_never_retry() {
        for kind in [
            FailureKind::InvalidInput,
            FailureKind::UnsupportedCapability,
            FailureKind::ModelUnavailable,
            FailureKind::ExecutionTimedOut,
            FailureKind::Cancelled,
            FailureKind::Internal,
        ] {
            assert_eq!(kind.retry_decision(1, true), RetryDecision::Fail, "{kind}");
        }
    }

    #[test]
    fn transient_failures_retry_up_to_three_attempts() {
        let kind = FailureKind::BackendCrashed;
        assert_eq!(kind.retry_decision(1, true), RetryDecision::Requeue);
        assert_eq!(kind.retry_decision(2, true), RetryDecision::Requeue);
        assert_eq!(kind.retry_decision(3, true), RetryDecision::Fail);
    }

    #[test]
    fn oom_retries_only_while_a_candidate_remains() {
        let kind = FailureKind::OutOfMemory;
        assert_eq!(kind.retry_decision(1, true), RetryDecision::Requeue);
        assert_eq!(kind.retry_decision(1, false), RetryDecision::Fail);
        assert!(kind.invalidates_slot_capability());
    }

    #[test]
    fn lease_loss_is_retryable() {
        assert!(FailureKind::LeaseExpired.is_retryable());
        assert!(FailureKind::WorkerLost.is_retryable());
        assert!(!FailureKind::LeaseExpired.invalidates_slot_capability());
    }

    #[test]
    fn is_capability_failure_covers_every_kind() {
        // Only these three mean no candidate could ever run the Generation;
        // an exhaustive match forces this test to be updated if a new
        // `FailureKind` variant is added.
        for kind in FailureKind::all() {
            let expected = match kind {
                FailureKind::UnsupportedCapability
                | FailureKind::ModelUnavailable
                | FailureKind::OutOfMemory => true,
                FailureKind::InvalidInput
                | FailureKind::BackendCrashed
                | FailureKind::ExecutionTimedOut
                | FailureKind::Cancelled
                | FailureKind::TransferFailed
                | FailureKind::Internal
                | FailureKind::WorkerLost
                | FailureKind::LeaseExpired => false,
            };
            assert_eq!(kind.is_capability_failure(), expected, "{kind}");
        }
    }

    #[test]
    fn names_round_trip() {
        for kind in FailureKind::all() {
            assert_eq!(kind.as_str().parse::<FailureKind>(), Ok(*kind));
        }
    }
}
