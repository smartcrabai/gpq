//! Native Generation, Catalog, and Tenant services (ADR 0006).
//!
//! Every method here authenticates the Tenant Master Key from the
//! `Authorization: Bearer` header (ADR 0006, ADR 0009) before touching any
//! Tenant-scoped data, and converts between the domain vocabulary
//! (`gpq_domain`) and the wire vocabulary (`gpq_proto::gpq::v1`) - the two
//! deliberately diverge (e.g. the domain splits a Workflow's execution
//! limits out of its manifest; the wire message does not), so the
//! conversions below are the single place that reconciles them.

pub mod catalog;
pub mod generation;
pub mod tenant;

pub use catalog::CatalogApi;
pub use generation::GenerationApi;
pub use tenant::TenantApi;

use std::time::Duration;

use buffa::{EnumValue, MessageField};
use buffa_types::google::protobuf::{Duration as ProtoDuration, Timestamp as ProtoTimestamp};
use chrono::{DateTime, Utc};
use connectrpc::{ConnectError, ErrorCode, RequestContext};
use gpq_domain::TenantId;
use gpq_proto::gpq::v1 as pb;

use crate::state::AppState;

/// Authenticates the Tenant Master Key carried as an `Authorization: Bearer`
/// header (ADR 0006, ADR 0009). Every Native RPC calls this before touching
/// any Tenant-scoped data.
pub(crate) fn authenticate(
    state: &AppState,
    ctx: &RequestContext,
) -> impl Future<Output = Result<TenantId, ConnectError>> + Send + 'static {
    let db = state.db.clone();
    let token = crate::auth::bearer_token(ctx.headers()).map(str::to_owned);
    async move {
        let token = token
            .ok_or_else(|| ConnectError::new(ErrorCode::Unauthenticated, "missing bearer token"))?;
        match db.authenticate_master_key(&token).await {
            Ok(Some(tenant_id)) => Ok(tenant_id),
            Ok(None) => Err(ConnectError::new(
                ErrorCode::Unauthenticated,
                "invalid master key",
            )),
            Err(err) => Err(ConnectError::new(ErrorCode::Internal, err.to_string())),
        }
    }
}

/// Converts a domain [`Modality`](gpq_domain::Modality) to its wire enum.
pub(crate) fn modality_to_proto(modality: gpq_domain::Modality) -> EnumValue<pb::Modality> {
    use gpq_domain::Modality as D;
    EnumValue::Known(match modality {
        D::Llm => pb::Modality::MODALITY_LLM,
        D::Image => pb::Modality::MODALITY_IMAGE,
        D::Video => pb::Modality::MODALITY_VIDEO,
        D::Music => pb::Modality::MODALITY_MUSIC,
    })
}

/// Converts a wire `Modality` to its domain enum. `None` for the zero value
/// or an unrecognized wire value.
pub(crate) fn modality_from_proto(value: EnumValue<pb::Modality>) -> Option<gpq_domain::Modality> {
    use gpq_domain::Modality as D;
    match value.as_known()? {
        pb::Modality::MODALITY_LLM => Some(D::Llm),
        pb::Modality::MODALITY_IMAGE => Some(D::Image),
        pb::Modality::MODALITY_VIDEO => Some(D::Video),
        pb::Modality::MODALITY_MUSIC => Some(D::Music),
        pb::Modality::MODALITY_UNSPECIFIED => None,
    }
}

/// Converts a domain [`GenerationState`](gpq_domain::GenerationState) to its
/// wire enum.
pub(crate) fn generation_state_to_proto(
    state: gpq_domain::GenerationState,
) -> EnumValue<pb::GenerationState> {
    use gpq_domain::GenerationState as D;
    EnumValue::Known(match state {
        D::Queued => pb::GenerationState::GENERATION_STATE_QUEUED,
        D::Running => pb::GenerationState::GENERATION_STATE_RUNNING,
        D::Cancelling => pb::GenerationState::GENERATION_STATE_CANCELLING,
        D::Succeeded => pb::GenerationState::GENERATION_STATE_SUCCEEDED,
        D::Failed => pb::GenerationState::GENERATION_STATE_FAILED,
        D::Cancelled => pb::GenerationState::GENERATION_STATE_CANCELLED,
        D::Expired => pb::GenerationState::GENERATION_STATE_EXPIRED,
    })
}

/// Converts a wire `GenerationState` to its domain enum. `None` for the zero
/// value or an unrecognized wire value.
pub(crate) fn generation_state_from_proto(
    value: EnumValue<pb::GenerationState>,
) -> Option<gpq_domain::GenerationState> {
    use gpq_domain::GenerationState as D;
    match value.as_known()? {
        pb::GenerationState::GENERATION_STATE_QUEUED => Some(D::Queued),
        pb::GenerationState::GENERATION_STATE_RUNNING => Some(D::Running),
        pb::GenerationState::GENERATION_STATE_CANCELLING => Some(D::Cancelling),
        pb::GenerationState::GENERATION_STATE_SUCCEEDED => Some(D::Succeeded),
        pb::GenerationState::GENERATION_STATE_FAILED => Some(D::Failed),
        pb::GenerationState::GENERATION_STATE_CANCELLED => Some(D::Cancelled),
        pb::GenerationState::GENERATION_STATE_EXPIRED => Some(D::Expired),
        pb::GenerationState::GENERATION_STATE_UNSPECIFIED => None,
    }
}

/// Converts a domain [`FailureKind`](gpq_domain::FailureKind) to its wire
/// enum.
pub(crate) fn failure_kind_to_proto(kind: gpq_domain::FailureKind) -> EnumValue<pb::FailureKind> {
    use gpq_domain::FailureKind as D;
    EnumValue::Known(match kind {
        D::InvalidInput => pb::FailureKind::FAILURE_KIND_INVALID_INPUT,
        D::UnsupportedCapability => pb::FailureKind::FAILURE_KIND_UNSUPPORTED_CAPABILITY,
        D::ModelUnavailable => pb::FailureKind::FAILURE_KIND_MODEL_UNAVAILABLE,
        D::OutOfMemory => pb::FailureKind::FAILURE_KIND_OUT_OF_MEMORY,
        D::BackendCrashed => pb::FailureKind::FAILURE_KIND_BACKEND_CRASHED,
        D::ExecutionTimedOut => pb::FailureKind::FAILURE_KIND_EXECUTION_TIMED_OUT,
        D::Cancelled => pb::FailureKind::FAILURE_KIND_CANCELLED,
        D::TransferFailed => pb::FailureKind::FAILURE_KIND_TRANSFER_FAILED,
        D::Internal => pb::FailureKind::FAILURE_KIND_INTERNAL,
        D::WorkerLost => pb::FailureKind::FAILURE_KIND_WORKER_LOST,
        D::LeaseExpired => pb::FailureKind::FAILURE_KIND_LEASE_EXPIRED,
    })
}

/// Converts a domain [`MediaKind`](gpq_domain::MediaKind) to its wire enum.
pub(crate) fn media_kind_to_proto(kind: gpq_domain::MediaKind) -> EnumValue<pb::MediaKind> {
    use gpq_domain::MediaKind as D;
    EnumValue::Known(match kind {
        D::Image => pb::MediaKind::MEDIA_KIND_IMAGE,
        D::Video => pb::MediaKind::MEDIA_KIND_VIDEO,
        D::Audio => pb::MediaKind::MEDIA_KIND_AUDIO,
        D::Text => pb::MediaKind::MEDIA_KIND_TEXT,
        D::Binary => pb::MediaKind::MEDIA_KIND_BINARY,
    })
}

/// Converts a wire `MediaKind` to its domain enum. `None` for the zero value
/// or an unrecognized wire value.
pub(crate) fn media_kind_from_proto(
    value: EnumValue<pb::MediaKind>,
) -> Option<gpq_domain::MediaKind> {
    use gpq_domain::MediaKind as D;
    match value.as_known()? {
        pb::MediaKind::MEDIA_KIND_IMAGE => Some(D::Image),
        pb::MediaKind::MEDIA_KIND_VIDEO => Some(D::Video),
        pb::MediaKind::MEDIA_KIND_AUDIO => Some(D::Audio),
        pb::MediaKind::MEDIA_KIND_TEXT => Some(D::Text),
        pb::MediaKind::MEDIA_KIND_BINARY => Some(D::Binary),
        pb::MediaKind::MEDIA_KIND_UNSPECIFIED => None,
    }
}

/// Converts a domain [`ArtifactPlacement`](gpq_domain::ArtifactPlacement) to
/// its wire enum.
pub(crate) fn artifact_placement_to_proto(
    placement: gpq_domain::ArtifactPlacement,
) -> EnumValue<pb::ArtifactPlacement> {
    use gpq_domain::ArtifactPlacement as D;
    EnumValue::Known(match placement {
        D::ObjectStore => pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE,
        D::WorkerLocal => pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        D::InlineRelay => pb::ArtifactPlacement::ARTIFACT_PLACEMENT_INLINE_RELAY,
    })
}

/// Converts a wire `ArtifactPlacement` to its domain enum. `None` for the
/// zero value or an unrecognized wire value.
pub(crate) fn artifact_placement_from_proto(
    value: EnumValue<pb::ArtifactPlacement>,
) -> Option<gpq_domain::ArtifactPlacement> {
    use gpq_domain::ArtifactPlacement as D;
    match value.as_known()? {
        pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE => Some(D::ObjectStore),
        pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL => Some(D::WorkerLocal),
        pb::ArtifactPlacement::ARTIFACT_PLACEMENT_INLINE_RELAY => Some(D::InlineRelay),
        pb::ArtifactPlacement::ARTIFACT_PLACEMENT_UNSPECIFIED => None,
    }
}

/// Converts a domain [`ArtifactState`](gpq_domain::ArtifactState) to its wire
/// enum.
pub(crate) fn artifact_state_to_proto(
    state: gpq_domain::ArtifactState,
) -> EnumValue<pb::ArtifactState> {
    use gpq_domain::ArtifactState as D;
    EnumValue::Known(match state {
        D::Pending => pb::ArtifactState::ARTIFACT_STATE_PENDING,
        D::Available => pb::ArtifactState::ARTIFACT_STATE_AVAILABLE,
        D::Delivering => pb::ArtifactState::ARTIFACT_STATE_DELIVERING,
        D::Consumed => pb::ArtifactState::ARTIFACT_STATE_CONSUMED,
        D::Expired => pb::ArtifactState::ARTIFACT_STATE_EXPIRED,
        D::Lost => pb::ArtifactState::ARTIFACT_STATE_LOST,
    })
}

/// Converts a domain [`BackendKind`](gpq_domain::BackendKind) to its wire
/// enum.
pub(crate) fn backend_kind_to_proto(kind: gpq_domain::BackendKind) -> EnumValue<pb::BackendKind> {
    use gpq_domain::BackendKind as D;
    EnumValue::Known(match kind {
        D::LlamaCpp => pb::BackendKind::BACKEND_KIND_LLAMA_CPP,
        D::MlxDspark => pb::BackendKind::BACKEND_KIND_MLX_DSPARK,
        D::ComfyUi => pb::BackendKind::BACKEND_KIND_COMFYUI,
    })
}

/// Converts a `std::time::Duration` to a set wire `Duration` field.
pub(crate) fn duration_to_proto(duration: Duration) -> MessageField<ProtoDuration> {
    MessageField::some(duration.into())
}

/// Converts a wire `Duration` field to a `std::time::Duration`. `None` when
/// unset or when the wire value cannot be represented as a (non-negative)
/// `std::time::Duration`.
pub(crate) fn duration_from_proto(field: MessageField<ProtoDuration>) -> Option<Duration> {
    Duration::try_from(field.into_option()?).ok()
}

/// Converts a `chrono::DateTime<Utc>` to a set wire `Timestamp` field.
pub(crate) fn timestamp_to_proto(instant: DateTime<Utc>) -> MessageField<ProtoTimestamp> {
    MessageField::some(instant.into())
}

/// Converts a wire `uint32` priority (0..=9) to a domain [`Priority`].
pub(crate) fn priority_from_wire(value: u32) -> Result<gpq_domain::Priority, String> {
    let raw = u8::try_from(value).map_err(|_err| format!("priority {value} is outside 0..=9"))?;
    gpq_domain::Priority::new(raw).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use buffa::EnumValue;
    use gpq_domain::{
        ArtifactPlacement, BackendKind, FailureKind, GenerationState, MediaKind, Modality,
    };

    use super::*;

    #[test]
    fn modality_round_trips_through_proto() {
        for modality in [
            Modality::Llm,
            Modality::Image,
            Modality::Video,
            Modality::Music,
        ] {
            let proto = modality_to_proto(modality);
            assert_eq!(modality_from_proto(proto), Some(modality));
        }
    }

    #[test]
    fn modality_unspecified_has_no_domain_value() {
        assert_eq!(
            modality_from_proto(EnumValue::Known(pb::Modality::MODALITY_UNSPECIFIED)),
            None
        );
    }

    #[test]
    fn modality_unknown_wire_value_has_no_domain_value() {
        assert_eq!(modality_from_proto(EnumValue::Unknown(99)), None);
    }

    #[test]
    fn generation_state_round_trips_through_proto() {
        for state in GenerationState::all().iter().copied() {
            let proto = generation_state_to_proto(state);
            assert_eq!(generation_state_from_proto(proto), Some(state));
        }
    }

    #[test]
    fn media_kind_round_trips_through_proto() {
        for kind in [
            MediaKind::Image,
            MediaKind::Video,
            MediaKind::Audio,
            MediaKind::Text,
            MediaKind::Binary,
        ] {
            let proto = media_kind_to_proto(kind);
            assert_eq!(media_kind_from_proto(proto), Some(kind));
        }
    }

    #[test]
    fn artifact_placement_round_trips_through_proto() {
        for placement in [
            ArtifactPlacement::ObjectStore,
            ArtifactPlacement::WorkerLocal,
            ArtifactPlacement::InlineRelay,
        ] {
            let proto = artifact_placement_to_proto(placement);
            assert_eq!(artifact_placement_from_proto(proto), Some(placement));
        }
    }

    #[test]
    fn failure_kind_maps_every_domain_variant() {
        // One-directional (wire never sends a Failure the domain didn't
        // produce), but every domain variant must still map to a distinct,
        // known wire value.
        let mut seen = std::collections::HashSet::new();
        for kind in [
            FailureKind::InvalidInput,
            FailureKind::UnsupportedCapability,
            FailureKind::ModelUnavailable,
            FailureKind::OutOfMemory,
            FailureKind::BackendCrashed,
            FailureKind::ExecutionTimedOut,
            FailureKind::Cancelled,
            FailureKind::TransferFailed,
            FailureKind::Internal,
            FailureKind::WorkerLost,
            FailureKind::LeaseExpired,
        ] {
            let proto = failure_kind_to_proto(kind);
            assert!(proto.is_known());
            assert!(seen.insert(proto.to_i32()));
        }
    }

    #[test]
    fn backend_kind_maps_every_domain_variant() {
        let mut seen = std::collections::BTreeSet::new();
        for kind in [
            BackendKind::LlamaCpp,
            BackendKind::MlxDspark,
            BackendKind::ComfyUi,
        ] {
            let proto = backend_kind_to_proto(kind);
            assert!(proto.is_known());
            assert!(seen.insert(proto.to_i32()));
        }
    }

    #[test]
    fn duration_round_trips_through_proto() {
        let duration = Duration::from_secs(3_661);
        let proto = duration_to_proto(duration);
        assert_eq!(duration_from_proto(proto), Some(duration));
    }

    #[test]
    fn duration_from_proto_is_none_when_unset() {
        assert_eq!(duration_from_proto(MessageField::none()), None);
    }

    #[test]
    fn timestamp_to_proto_sets_the_field() {
        let instant = DateTime::<Utc>::from_timestamp(1_700_000_000, 0)
            .unwrap_or_else(|| panic!("valid unix timestamp"));
        let proto = timestamp_to_proto(instant);
        let Some(wire) = proto.into_option() else {
            panic!("timestamp_to_proto must set the field");
        };
        let Ok(round_tripped) = DateTime::<Utc>::try_from(wire) else {
            panic!("timestamp_to_proto must produce a representable wire timestamp");
        };
        assert_eq!(round_tripped, instant);
    }

    #[test]
    fn priority_from_wire_accepts_the_full_range() {
        for value in 0..=9u32 {
            let Ok(priority) = priority_from_wire(value) else {
                panic!("priority {value} should be in range");
            };
            assert_eq!(u32::from(priority.get()), value);
        }
    }

    #[test]
    fn priority_from_wire_rejects_out_of_range() {
        assert!(priority_from_wire(10).is_err());
        assert!(priority_from_wire(u32::from(u16::MAX)).is_err());
    }
}
