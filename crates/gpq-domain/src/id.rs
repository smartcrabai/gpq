//! Typed identifiers.
//!
//! ADR 0017 fixes `UUIDv7` as the identifier type for every stored record, so the
//! newtypes below wrap [`Uuid`] and generate v7 values (time-ordered, which the
//! scheduler's `created_at` ordering and index locality both benefit from).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! typed_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a fresh time-ordered identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wraps an identifier already persisted elsewhere.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Returns the underlying UUID.
            #[must_use]
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(Uuid::parse_str(s)?))
            }
        }
    };
}

typed_id! {
    /// The isolated customer boundary that owns Generations and Workers.
    TenantId
}
typed_id! {
    /// A tenant's request for one AI-generated result.
    GenerationId
}
typed_id! {
    /// One execution of a Generation by a Worker.
    AttemptId
}
typed_id! {
    /// A tenant-scoped local service executing Generations.
    WorkerId
}
typed_id! {
    /// A non-overlapping set of GPUs controlled as one exclusive resource.
    DevicePoolId
}
typed_id! {
    /// One concurrent execution permit exposed by an Active Runtime.
    SlotId
}
typed_id! {
    /// A transient large binary input or output of a Generation.
    ArtifactId
}
typed_id! {
    /// Exact model material identified by a content hash.
    ModelVersionId
}
typed_id! {
    /// An immutable Workflow graph and output contract.
    WorkflowVersionId
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_time_ordered() {
        let first = GenerationId::new();
        let second = GenerationId::new();
        assert_eq!(first.as_uuid().get_version_num(), 7);
        assert!(first < second, "UUIDv7 must sort by creation time");
    }

    #[test]
    fn round_trips_through_text() {
        let id = TenantId::new();
        let Ok(parsed) = id.to_string().parse::<TenantId>() else {
            panic!("a rendered identifier must parse back");
        };
        assert_eq!(id, parsed);
    }

    fn assert_rejects_malformed<T>()
    where
        T: FromStr<Err = uuid::Error> + fmt::Debug + PartialEq,
    {
        for input in ["", "not-a-uuid", "123e4567-e89b-12d3-a456"] {
            let Err(expected) = Uuid::parse_str(input) else {
                panic!("test input {input:?} must be malformed");
            };
            assert_eq!(input.parse::<T>(), Err(expected), "input {input:?}");
        }
    }

    #[test]
    fn malformed_text_fails_to_parse_for_every_id_type() {
        assert_rejects_malformed::<TenantId>();
        assert_rejects_malformed::<GenerationId>();
        assert_rejects_malformed::<AttemptId>();
        assert_rejects_malformed::<WorkerId>();
        assert_rejects_malformed::<DevicePoolId>();
        assert_rejects_malformed::<SlotId>();
        assert_rejects_malformed::<ArtifactId>();
        assert_rejects_malformed::<ModelVersionId>();
        assert_rejects_malformed::<WorkflowVersionId>();
    }
}
