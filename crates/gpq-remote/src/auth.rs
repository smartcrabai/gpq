//! Credential hashing and bearer-token parsing (ADR 0009).
//!
//! Tenant Master Keys and Worker Credentials are stored only as keyed hashes,
//! never in plaintext, so a leaked database backup cannot be used to
//! impersonate a Tenant or Worker. Both public API families authenticate with
//! `Authorization: Bearer <secret>` (ADR 0006).

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Computes and verifies HMAC-SHA256 digests of credentials under one
/// operator-supplied key (ADR 0009: `GPQ_CREDENTIAL_KEY`).
#[derive(Clone)]
pub struct KeyedHasher {
    key: [u8; 32],
}

impl KeyedHasher {
    /// Creates a hasher keyed by the operator-supplied credential key.
    #[must_use]
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Digests `secret` under the configured key.
    #[must_use]
    pub fn hash(&self, secret: &str) -> Vec<u8> {
        let mut mac = self.mac();
        mac.update(secret.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }

    fn mac(&self) -> HmacSha256 {
        match HmacSha256::new_from_slice(&self.key) {
            Ok(mac) => mac,
            // HMAC accepts a key of any length (RFC 2104 §2): a 32-byte key
            // can never trigger the `InvalidLength` error.
            Err(_) => unreachable!("HMAC-SHA256 accepts keys of any length"),
        }
    }
}

/// Generates a fresh random secret with a human-readable prefix, e.g.
/// `gpq_mk_<43 base64url chars>` for a Tenant Master Key.
#[must_use]
pub fn generate_secret(prefix: &str) -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    format!("{prefix}_{}", URL_SAFE_NO_PAD.encode(bytes))
}

/// Extracts the bearer token from an `Authorization` header, accepting the
/// `Bearer` scheme case-insensitively and rejecting every other scheme.
#[must_use]
pub fn bearer_token(headers: &http::HeaderMap) -> Option<&str> {
    let value = headers.get(http::header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    scheme.eq_ignore_ascii_case("bearer").then(|| token.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hasher() -> KeyedHasher {
        KeyedHasher::new([7u8; 32])
    }

    #[test]
    fn same_secret_hashes_equal() {
        let hasher = hasher();
        assert_eq!(hasher.hash("secret-a"), hasher.hash("secret-a"));
    }

    #[test]
    fn different_secrets_hash_differently() {
        let hasher = hasher();
        assert_ne!(hasher.hash("secret-a"), hasher.hash("secret-b"));
    }

    #[test]
    fn generated_secrets_carry_their_prefix_and_are_unique() {
        let a = generate_secret("gpq_mk");
        let b = generate_secret("gpq_mk");
        assert!(a.starts_with("gpq_mk_"));
        assert_ne!(a, b);
    }

    fn headers_with_auth(value: &str) -> http::HeaderMap {
        let mut headers = http::HeaderMap::new();
        let Ok(header_value) = http::HeaderValue::from_str(value) else {
            panic!("test header value must be a valid HeaderValue")
        };
        headers.insert(http::header::AUTHORIZATION, header_value);
        headers
    }

    #[test]
    fn bearer_token_parses_the_standard_scheme() {
        let headers = headers_with_auth("Bearer x");
        assert_eq!(bearer_token(&headers), Some("x"));
    }

    #[test]
    fn bearer_token_parses_case_insensitively() {
        let headers = headers_with_auth("bearer X");
        assert_eq!(bearer_token(&headers), Some("X"));
    }

    #[test]
    fn bearer_token_rejects_other_schemes() {
        let headers = headers_with_auth("Basic dXNlcjpwYXNz");
        assert_eq!(bearer_token(&headers), None);
    }

    #[test]
    fn bearer_token_is_none_without_a_header() {
        let headers = http::HeaderMap::new();
        assert_eq!(bearer_token(&headers), None);
    }
}
