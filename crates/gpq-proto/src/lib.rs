//! Generated Protobuf message types and Connect/gRPC service stubs.
//!
//! Native Generation, Catalog, and Tenant services speak the Connect protocol
//! with binary or JSON codecs; Worker enrollment, the bidirectional control
//! Session, and Artifact transfer speak gRPC (ADR 0004).

#![allow(
    clippy::pedantic,
    missing_docs,
    refining_impl_trait_internal,
    refining_impl_trait_reachable,
    reason = "generated code"
)]

include!(concat!(env!("OUT_DIR"), "/_gpq_proto.rs"));

/// Protocol version reported in the Worker handshake.
///
/// Protobuf evolution preserves compatibility within one major version; a major
/// mismatch fails explicitly (ADR 0004).
pub const PROTOCOL_MAJOR: u32 = 1;

/// Minor protocol version, incremented for backward-compatible additions.
pub const PROTOCOL_MINOR: u32 = 0;

/// Whether a Worker reporting `major` may join this Remote.
#[must_use]
pub fn protocol_compatible(major: u32) -> bool {
    major == PROTOCOL_MAJOR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_matching_major_versions_join() {
        assert!(protocol_compatible(PROTOCOL_MAJOR));
        assert!(!protocol_compatible(PROTOCOL_MAJOR + 1));
        assert!(!protocol_compatible(0));
    }
}
