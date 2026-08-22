//! Lifecycle state machines.
//!
//! ADR 0003 fixes the Generation and Attempt state sets and their monotonicity:
//! terminal states are final, retries create new Attempts instead of reviving
//! old ones, and cancellation races result commitment through a compare-and-set.
//! ADR 0008 fixes the independent Artifact lifecycle. ADR 0017 stores all three
//! as text with database check constraints, so every state has a stable name.

/// A rejected state transition.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("illegal transition {from} -> {to}")]
pub struct TransitionError {
    /// The state the record is in.
    pub from: &'static str,
    /// The state that was requested.
    pub to: &'static str,
}

/// Failure to parse a persisted state name.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("unknown state {0:?}")]
pub struct UnknownState(pub String);

macro_rules! state_enum {
    (
        $(#[$meta:meta])*
        $name:ident {
            $(
                $(#[$vmeta:meta])*
                $variant:ident => $text:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            Debug,
            ::serde::Serialize,
            ::serde::Deserialize
        )]
        pub enum $name {
            $(
                $(#[$vmeta])*
                #[serde(rename = $text)]
                $variant
            ),+
        }

        impl $name {
            /// The stable name stored in `PostgreSQL` and carried on the wire.
            #[must_use]
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }

            /// Every state, in declaration order.
            #[must_use]
            pub const fn all() -> &'static [Self] {
                &[$(Self::$variant),+]
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::core::str::FromStr for $name {
            type Err = $crate::state::UnknownState;

            fn from_str(s: &str) -> ::core::result::Result<Self, Self::Err> {
                match s {
                    $($text => Ok(Self::$variant),)+
                    other => Err($crate::state::UnknownState(other.to_owned())),
                }
            }
        }
    };
}

pub(crate) use state_enum;

state_enum! {
    /// Generation lifecycle (ADR 0003).
    GenerationState {
        Queued => "queued",
        Running => "running",
        Cancelling => "cancelling",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
        Expired => "expired",
    }
}

state_enum! {
    /// Attempt lifecycle (ADR 0003).
    AttemptState {
        Leased => "leased",
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
        LeaseExpired => "lease_expired",
    }
}

state_enum! {
    /// Artifact lifecycle (ADR 0008), independent of its Generation.
    ArtifactState {
        Pending => "pending",
        Available => "available",
        Delivering => "delivering",
        Consumed => "consumed",
        Expired => "expired",
        Lost => "lost",
    }
}

impl GenerationState {
    /// Whether the Generation can no longer change state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Expired
        )
    }

    /// Whether a Worker may still be executing an Attempt for this Generation.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        matches!(self, Self::Running | Self::Cancelling)
    }

    /// Whether the scheduler may lease this Generation.
    #[must_use]
    pub const fn is_leasable(&self) -> bool {
        matches!(self, Self::Queued)
    }

    /// Legal successors.
    ///
    /// `Running -> Queued` is the single backward edge: a retryable Attempt
    /// failure re-queues the Generation so another Slot can lease a new Attempt
    /// (ADR 0003). Terminal states have no successors.
    #[must_use]
    pub const fn can_transition_to(&self, to: Self) -> bool {
        matches!(
            (self, to),
            (
                Self::Queued,
                Self::Running | Self::Cancelled | Self::Failed | Self::Expired
            ) | (
                Self::Running,
                Self::Queued
                    | Self::Cancelling
                    | Self::Succeeded
                    | Self::Failed
                    | Self::Cancelled
                    | Self::Expired,
            ) | (
                Self::Cancelling,
                Self::Cancelled | Self::Succeeded | Self::Failed,
            )
        )
    }

    /// Applies a transition, rejecting illegal and terminal-state changes.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] when the edge does not exist, which includes
    /// every edge out of a terminal state.
    pub fn transition(self, to: Self) -> Result<Self, TransitionError> {
        if self.can_transition_to(to) {
            Ok(to)
        } else {
            Err(TransitionError {
                from: self.as_str(),
                to: to.as_str(),
            })
        }
    }
}

impl AttemptState {
    /// Whether the Attempt can no longer change state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::LeaseExpired
        )
    }

    /// Whether the Attempt still holds a lease that heartbeats keep alive.
    #[must_use]
    pub const fn is_live(&self) -> bool {
        matches!(self, Self::Leased | Self::Running)
    }

    /// Legal successors.
    #[must_use]
    pub const fn can_transition_to(&self, to: Self) -> bool {
        matches!(
            (self, to),
            (
                Self::Leased,
                Self::Running
                    | Self::Succeeded
                    | Self::Failed
                    | Self::Cancelled
                    | Self::LeaseExpired,
            ) | (
                Self::Running,
                Self::Succeeded | Self::Failed | Self::Cancelled | Self::LeaseExpired,
            )
        )
    }

    /// Applies a transition, rejecting illegal and terminal-state changes.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] when the edge does not exist, which includes
    /// every edge out of a terminal state.
    pub fn transition(self, to: Self) -> Result<Self, TransitionError> {
        if self.can_transition_to(to) {
            Ok(to)
        } else {
            Err(TransitionError {
                from: self.as_str(),
                to: to.as_str(),
            })
        }
    }
}

impl ArtifactState {
    /// Whether the Artifact can no longer change state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Consumed | Self::Expired | Self::Lost)
    }

    /// Whether the bytes can still be downloaded.
    #[must_use]
    pub const fn is_downloadable(&self) -> bool {
        matches!(self, Self::Available)
    }

    /// Legal successors.
    #[must_use]
    pub const fn can_transition_to(&self, to: Self) -> bool {
        matches!(
            (self, to),
            (Self::Pending, Self::Available | Self::Expired | Self::Lost)
                | (
                    Self::Available,
                    Self::Delivering | Self::Expired | Self::Lost
                )
                | (
                    Self::Delivering,
                    Self::Consumed | Self::Expired | Self::Lost
                )
        )
    }

    /// Applies a transition, rejecting illegal and terminal-state changes.
    ///
    /// # Errors
    ///
    /// Returns [`TransitionError`] when the edge does not exist, which includes
    /// every edge out of a terminal state.
    pub fn transition(self, to: Self) -> Result<Self, TransitionError> {
        if self.can_transition_to(to) {
            Ok(to)
        } else {
            Err(TransitionError {
                from: self.as_str(),
                to: to.as_str(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_transition_matrix_is_exhaustive() {
        // ADR 0003's complete GenerationState edge set. `Running -> Queued`
        // is the single backward edge (a retryable Attempt failure requeues
        // the Generation); terminal states have no outgoing edges. Any
        // change to `can_transition_to`'s `matches!` pattern that adds or
        // removes an edge must be reflected here first.
        let legal: &[(GenerationState, GenerationState)] = &[
            (GenerationState::Queued, GenerationState::Running),
            (GenerationState::Queued, GenerationState::Cancelled),
            (GenerationState::Queued, GenerationState::Failed),
            (GenerationState::Queued, GenerationState::Expired),
            (GenerationState::Running, GenerationState::Queued),
            (GenerationState::Running, GenerationState::Cancelling),
            (GenerationState::Running, GenerationState::Succeeded),
            (GenerationState::Running, GenerationState::Failed),
            (GenerationState::Running, GenerationState::Cancelled),
            (GenerationState::Running, GenerationState::Expired),
            (GenerationState::Cancelling, GenerationState::Cancelled),
            (GenerationState::Cancelling, GenerationState::Succeeded),
            (GenerationState::Cancelling, GenerationState::Failed),
        ];
        for &from in GenerationState::all() {
            for &to in GenerationState::all() {
                let expected = legal.contains(&(from, to));
                assert_eq!(
                    from.can_transition_to(to),
                    expected,
                    "{from} -> {to} expected {expected}"
                );
            }
        }
    }

    #[test]
    fn terminal_attempt_states_are_final() {
        for state in AttemptState::all() {
            if !state.is_terminal() {
                continue;
            }
            for target in AttemptState::all() {
                assert!(!state.can_transition_to(*target));
            }
        }
    }

    #[test]
    fn retryable_failure_requeues_generation() {
        assert_eq!(
            GenerationState::Running.transition(GenerationState::Queued),
            Ok(GenerationState::Queued)
        );
        assert_eq!(
            GenerationState::Succeeded.transition(GenerationState::Queued),
            Err(TransitionError {
                from: "succeeded",
                to: "queued",
            })
        );
    }

    #[test]
    fn artifact_lifecycle_is_one_shot() {
        assert!(ArtifactState::Delivering.can_transition_to(ArtifactState::Consumed));
        assert!(!ArtifactState::Consumed.can_transition_to(ArtifactState::Available));
        assert!(!ArtifactState::Pending.can_transition_to(ArtifactState::Delivering));
    }

    #[test]
    fn state_names_round_trip() {
        for state in GenerationState::all() {
            assert_eq!(state.as_str().parse::<GenerationState>(), Ok(*state));
        }
        for state in AttemptState::all() {
            assert_eq!(state.as_str().parse::<AttemptState>(), Ok(*state));
        }
        for state in ArtifactState::all() {
            assert_eq!(state.as_str().parse::<ArtifactState>(), Ok(*state));
        }
    }

    #[test]
    fn attempt_transition_reports_from_and_to() {
        assert_eq!(
            AttemptState::Leased.transition(AttemptState::Running),
            Ok(AttemptState::Running)
        );
        assert_eq!(
            AttemptState::Succeeded.transition(AttemptState::Running),
            Err(TransitionError {
                from: "succeeded",
                to: "running",
            })
        );
    }

    #[test]
    fn artifact_transition_reports_from_and_to() {
        assert_eq!(
            ArtifactState::Pending.transition(ArtifactState::Available),
            Ok(ArtifactState::Available)
        );
        assert_eq!(
            ArtifactState::Consumed.transition(ArtifactState::Available),
            Err(TransitionError {
                from: "consumed",
                to: "available",
            })
        );
    }
}
