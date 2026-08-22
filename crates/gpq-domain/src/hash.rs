//! Content hashing.
//!
//! Model Versions, Workflow Versions, and Artifact Manifests are all identified
//! by a SHA-256 content hash (ADR 0005, ADR 0008, ADR 0012). A fixed-size
//! newtype keeps those identities allocation-free and comparison-cheap.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

/// A SHA-256 digest of immutable content.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash([u8; 32]);

/// Failure to parse a hex-encoded [`ContentHash`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ContentHashError {
    /// The text was not 64 hexadecimal characters.
    #[error("content hash must be 64 hex characters, got {0}")]
    Length(usize),
    /// The text contained a non-hexadecimal character.
    #[error("content hash is not valid hex")]
    Encoding,
}

impl ContentHash {
    /// Wraps raw digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Digests a byte slice.
    #[must_use]
    pub fn digest(data: &[u8]) -> Self {
        Self(Sha256::digest(data).into())
    }

    /// Returns the raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the lowercase hex encoding used in SQL, protobuf, and logs.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentHash({self})")
    }
}

impl FromStr for ContentHash {
    type Err = ContentHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(ContentHashError::Length(s.len()));
        }
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(s, &mut bytes).map_err(|_| ContentHashError::Encoding)?;
        Ok(Self(bytes))
    }
}

impl Serialize for ContentHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// Incremental SHA-256 hasher for streamed Artifacts and model files.
#[derive(Default, Clone)]
pub struct Hasher(Sha256);

impl Hasher {
    /// Creates an empty hasher.
    #[must_use]
    pub fn new() -> Self {
        Self(Sha256::new())
    }

    /// Feeds a chunk.
    pub fn update(&mut self, chunk: &[u8]) {
        self.0.update(chunk);
    }

    /// Finalizes the digest.
    #[must_use]
    pub fn finish(self) -> ContentHash {
        ContentHash(self.0.finalize().into())
    }
}

impl fmt::Debug for Hasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Hasher(..)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_matches_known_vector() {
        let hash = ContentHash::digest(b"abc");
        assert_eq!(
            hash.to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn streaming_matches_one_shot() {
        let mut hasher = Hasher::new();
        hasher.update(b"ab");
        hasher.update(b"c");
        assert_eq!(hasher.finish(), ContentHash::digest(b"abc"));
    }

    #[test]
    fn rejects_malformed_hex() {
        assert_eq!(
            "dead".parse::<ContentHash>(),
            Err(ContentHashError::Length(4))
        );
        assert_eq!(
            "z".repeat(64).parse::<ContentHash>(),
            Err(ContentHashError::Encoding)
        );
    }

    #[test]
    fn round_trips_through_hex() {
        let hash = ContentHash::digest(b"model weights");
        assert_eq!(hash.to_hex().parse::<ContentHash>(), Ok(hash));
    }
}
