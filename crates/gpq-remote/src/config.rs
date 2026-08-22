//! Runtime configuration for `gpq-remote serve`, loaded from environment
//! variables.
//!
//! ADR 0016: `serve` receives only the forced-RLS application credential —
//! schema migration uses a separate, operator-supplied connection string for
//! the `migrate` subcommand. ADR 0019: no TLS configuration lives here; TLS
//! terminates at the ingress. ADR 0008: S3 configuration is optional and
//! never affects Remote readiness, so its absence yields `None` here rather
//! than an error, while a half-supplied S3 configuration is a startup error.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, bail};
use url::Url;

/// Environment-derived configuration for the `serve` command.
#[derive(Clone)]
pub struct RemoteConfig {
    /// `PostgreSQL` connection string for the forced-RLS `gpq_app` role.
    pub database_url: String,
    /// Address the Axum/ConnectRPC/gRPC listener binds to.
    pub bind_addr: SocketAddr,
    /// Keyed-hash key for Master Keys and Worker Credentials (ADR 0009).
    pub credential_key: [u8; 32],
    /// Base URL used to build Artifact download links returned to callers.
    pub public_base_url: Url,
    /// Optional S3-compatible object store. Absence disables Native input
    /// Artifacts and S3 output placement but never blocks readiness (ADR 0008).
    pub object_store: Option<ObjectStoreConfig>,
}

/// S3-compatible object store connection details.
#[derive(Clone)]
pub struct ObjectStoreConfig {
    /// Bucket holding ephemeral input and output Artifacts.
    pub bucket: String,
    /// Region passed to the S3 client.
    pub region: String,
    /// Non-AWS S3-compatible endpoint override, if any.
    pub endpoint: Option<String>,
    /// Lifetime of presigned URLs issued to leased Workers (ADR 0008: 15
    /// minutes by default).
    pub presign_ttl: Duration,
}

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8080";
/// ADR 0008: Remote issues object-scoped 15-minute presigned URLs.
const DEFAULT_PRESIGN_TTL_SECS: u64 = 15 * 60;

impl RemoteConfig {
    /// Loads configuration from the process environment.
    ///
    /// # Errors
    /// Returns an error if a required variable is missing or malformed, or if
    /// only one of `GPQ_S3_BUCKET`/`GPQ_S3_REGION` is set.
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url =
            std::env::var("GPQ_DATABASE_URL").context("GPQ_DATABASE_URL is required")?;
        let bind_addr = std::env::var("GPQ_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string())
            .parse()
            .context("GPQ_BIND_ADDR is not a valid socket address")?;
        let credential_key_hex =
            std::env::var("GPQ_CREDENTIAL_KEY").context("GPQ_CREDENTIAL_KEY is required")?;
        let credential_key = parse_credential_key(&credential_key_hex)?;
        let public_base_url = std::env::var("GPQ_PUBLIC_BASE_URL")
            .context("GPQ_PUBLIC_BASE_URL is required")?
            .parse::<Url>()
            .context("GPQ_PUBLIC_BASE_URL is not a valid URL")?;
        let object_store = resolve_object_store(
            std::env::var("GPQ_S3_BUCKET").ok(),
            std::env::var("GPQ_S3_REGION").ok(),
            std::env::var("GPQ_S3_ENDPOINT").ok(),
            std::env::var("GPQ_S3_PRESIGN_TTL_SECS").ok(),
        )?;

        Ok(Self {
            database_url,
            bind_addr,
            credential_key,
            public_base_url,
            object_store,
        })
    }
}

/// Parses the hex-encoded 32-byte keyed-hash key (ADR 0009).
fn parse_credential_key(hex_str: &str) -> anyhow::Result<[u8; 32]> {
    let bytes = hex::decode(hex_str.trim()).context("GPQ_CREDENTIAL_KEY is not valid hex")?;
    let len = bytes.len();
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("GPQ_CREDENTIAL_KEY must decode to 32 bytes, got {len}"))
}

/// Builds the optional object store config, or fails fast on partial
/// configuration (ADR 0008: absence is fine, half-configuration is not).
fn resolve_object_store(
    bucket: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    presign_ttl_secs: Option<String>,
) -> anyhow::Result<Option<ObjectStoreConfig>> {
    let presign_ttl = match presign_ttl_secs {
        Some(secs) => Duration::from_secs(
            secs.parse()
                .context("GPQ_S3_PRESIGN_TTL_SECS is not a valid number of seconds")?,
        ),
        None => Duration::from_secs(DEFAULT_PRESIGN_TTL_SECS),
    };
    match (bucket, region) {
        (None, None) => Ok(None),
        (Some(bucket), Some(region)) => Ok(Some(ObjectStoreConfig {
            bucket,
            region,
            endpoint,
            presign_ttl,
        })),
        (Some(_), None) => bail!("GPQ_S3_REGION is required when GPQ_S3_BUCKET is set"),
        (None, Some(_)) => bail!("GPQ_S3_BUCKET is required when GPQ_S3_REGION is set"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_key_parses_valid_hex() {
        let hex_str = "00".repeat(32);
        let Ok(key) = parse_credential_key(&hex_str) else {
            panic!("expected 64 zero hex chars to parse")
        };
        assert_eq!(key, [0u8; 32]);
    }

    #[test]
    fn credential_key_rejects_wrong_length() {
        let hex_str = "00".repeat(16);
        assert!(parse_credential_key(&hex_str).is_err());
    }

    #[test]
    fn credential_key_rejects_invalid_hex() {
        assert!(parse_credential_key("not hex at all").is_err());
    }

    #[test]
    fn object_store_absent_is_none() {
        let Ok(None) = resolve_object_store(None, None, None, None) else {
            panic!("expected no S3 configuration to resolve to None")
        };
    }

    #[test]
    fn object_store_full_configuration_resolves() {
        let Ok(Some(config)) = resolve_object_store(
            Some("bucket".to_string()),
            Some("us-east-1".to_string()),
            Some("https://example.invalid".to_string()),
            Some("60".to_string()),
        ) else {
            panic!("expected full S3 configuration to resolve to Some")
        };
        assert_eq!(config.bucket, "bucket");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.presign_ttl, Duration::from_mins(1));
    }

    #[test]
    fn object_store_default_presign_ttl_is_fifteen_minutes() {
        let Ok(Some(config)) = resolve_object_store(
            Some("bucket".to_string()),
            Some("us-east-1".to_string()),
            None,
            None,
        ) else {
            panic!("expected full S3 configuration to resolve to Some")
        };
        assert_eq!(config.presign_ttl, Duration::from_mins(15));
    }

    #[test]
    fn object_store_partial_configuration_errors() {
        assert!(resolve_object_store(Some("bucket".to_string()), None, None, None).is_err());
        assert!(resolve_object_store(None, Some("us-east-1".to_string()), None, None).is_err());
    }
}
