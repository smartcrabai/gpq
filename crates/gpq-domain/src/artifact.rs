//! Artifact manifests and placement.
//!
//! ADR 0008: Artifacts are transient. Their immutable manifest (size, SHA-256
//! digest, media kind, MIME type) is fixed before transfer or result
//! commitment, Worker-local delivery uses bounded one-MiB chunks with declared
//! size and digest validation, and unclaimed outputs expire one hour after
//! completion.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::hash::ContentHash;
use crate::state::state_enum;

/// Chunk size of Worker-local Artifact transfers (ADR 0008).
pub const TRANSFER_CHUNK_BYTES: usize = 1024 * 1024;

/// How long a completed but unclaimed output Artifact survives (ADR 0008).
pub const OUTPUT_ARTIFACT_TTL: Duration = Duration::from_hours(1);

state_enum! {
    /// Coarse content classification of an Artifact.
    MediaKind {
        /// Still image bytes.
        Image => "image",
        /// Video container bytes.
        Video => "video",
        /// Audio or music bytes.
        Audio => "audio",
        /// Text payload too large or too structured for a row.
        Text => "text",
        /// Anything else the backend produces.
        Binary => "binary",
    }
}

state_enum! {
    /// Where an Artifact's bytes live.
    ///
    /// ADR 0008: Native input Artifacts use S3-compatible ephemeral placement so
    /// queued work can move between Workers; outputs choose Worker-local relay or
    /// the same ephemeral placement; synchronous `OpenAI` image inputs may be relayed
    /// inline while the request stays connected.
    ArtifactPlacement {
        /// Object storage reachable by Remote and Workers through presigned URLs.
        ObjectStore => "object_store",
        /// The producing Worker's crash-recoverable state directory.
        WorkerLocal => "worker_local",
        /// Bytes carried inline through the connected request; never persisted.
        InlineRelay => "inline_relay",
    }
}

impl ArtifactPlacement {
    /// Whether the placement needs configured S3 credentials on Remote.
    #[must_use]
    pub const fn requires_object_store(&self) -> bool {
        matches!(self, Self::ObjectStore)
    }
}

/// The immutable description of an Artifact, fixed before any transfer.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ArtifactManifest {
    /// Exact byte length.
    pub size_bytes: u64,
    /// SHA-256 digest of the bytes.
    pub digest: ContentHash,
    /// Coarse content classification.
    pub kind: MediaKind,
    /// MIME type.
    pub mime_type: String,
}

/// A manifest mismatch discovered while validating transferred bytes.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ManifestMismatch {
    /// Transferred length differed from the declared length.
    #[error("artifact size mismatch: declared {declared}, received {received}")]
    Size {
        /// Declared length.
        declared: u64,
        /// Observed length.
        received: u64,
    },
    /// Transferred bytes hashed differently than declared.
    #[error("artifact digest mismatch: declared {declared}, received {received}")]
    Digest {
        /// Declared digest.
        declared: ContentHash,
        /// Observed digest.
        received: ContentHash,
    },
}

impl ArtifactManifest {
    /// Verifies transferred bytes against the manifest.
    ///
    /// # Errors
    ///
    /// Returns [`ManifestMismatch`] when the transferred length or digest
    /// differs from the immutable manifest.
    pub fn verify(&self, received_bytes: u64, digest: ContentHash) -> Result<(), ManifestMismatch> {
        if received_bytes != self.size_bytes {
            return Err(ManifestMismatch::Size {
                declared: self.size_bytes,
                received: received_bytes,
            });
        }
        if digest != self.digest {
            return Err(ManifestMismatch::Digest {
                declared: self.digest,
                received: digest,
            });
        }
        Ok(())
    }

    /// Number of one-MiB chunks a Worker-local transfer will produce.
    #[must_use]
    pub const fn chunk_count(&self) -> u64 {
        let chunk = TRANSFER_CHUNK_BYTES as u64;
        self.size_bytes.div_ceil(chunk)
    }

    /// Whether the Artifact fits inside a Tenant's size limit.
    #[must_use]
    pub const fn fits_within(&self, limit_bytes: u64) -> bool {
        self.size_bytes <= limit_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(size: u64, data: &[u8]) -> ArtifactManifest {
        ArtifactManifest {
            size_bytes: size,
            digest: ContentHash::digest(data),
            kind: MediaKind::Image,
            mime_type: "image/png".to_owned(),
        }
    }

    #[test]
    fn verification_accepts_matching_bytes() {
        let manifest = manifest(3, b"abc");
        assert_eq!(manifest.verify(3, ContentHash::digest(b"abc")), Ok(()));
    }

    #[test]
    fn verification_rejects_short_transfer() {
        let manifest = manifest(3, b"abc");
        assert_eq!(
            manifest.verify(2, ContentHash::digest(b"ab")),
            Err(ManifestMismatch::Size {
                declared: 3,
                received: 2
            })
        );
    }

    #[test]
    fn verification_rejects_wrong_digest() {
        let manifest = manifest(3, b"abc");
        assert_eq!(
            manifest.verify(3, ContentHash::digest(b"xyz")),
            Err(ManifestMismatch::Digest {
                declared: ContentHash::digest(b"abc"),
                received: ContentHash::digest(b"xyz"),
            })
        );
    }

    #[test]
    fn chunk_count_uses_one_mib_chunks() {
        assert_eq!(manifest(0, b"").chunk_count(), 0);
        assert_eq!(manifest(1, b"a").chunk_count(), 1);
        assert_eq!(manifest(1024 * 1024, b"a").chunk_count(), 1);
        assert_eq!(manifest(1024 * 1024 + 1, b"a").chunk_count(), 2);
    }

    #[test]
    fn fits_within_admits_at_and_below_limit_rejects_above() {
        assert!(manifest(9, b"").fits_within(10));
        assert!(manifest(10, b"").fits_within(10));
        assert!(!manifest(11, b"").fits_within(10));
    }

    #[test]
    fn only_object_store_needs_s3() {
        assert!(ArtifactPlacement::ObjectStore.requires_object_store());
        assert!(!ArtifactPlacement::WorkerLocal.requires_object_store());
        assert!(!ArtifactPlacement::InlineRelay.requires_object_store());
    }

    #[test]
    fn names_round_trip() {
        for kind in MediaKind::all() {
            assert_eq!(kind.as_str().parse::<MediaKind>(), Ok(*kind));
        }
        for placement in ArtifactPlacement::all() {
            assert_eq!(
                placement.as_str().parse::<ArtifactPlacement>(),
                Ok(*placement)
            );
        }
    }
}
