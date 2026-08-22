//! Fixture for ADR 0008's S3-compatible object-store path: a
//! `testcontainers`-managed `MinIO` container plus the `aws-sdk-s3` client
//! and bucket tests use to inspect object storage directly, alongside the
//! Remote under test.
//!
//! Reused (read-only, via its own `ObjectStoreFixture::start()` instance) by
//! any suite that needs real S3-compatible object storage; each caller gets
//! its own container, the same way each test binary gets its own
//! `PostgreSQL` container from `e2e_support`.

use anyhow::Context;
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use tokio::sync::Mutex as TokioMutex;

/// Lifetime of presigned URLs issued by a Remote configured against this
/// fixture (`GPQ_S3_PRESIGN_TTL_SECS`): short enough that a test can assert
/// the expiry timestamp without waiting for it, long enough that a slow CI
/// run never legitimately outlives it mid-test.
pub const PRESIGN_TTL_SECS: u64 = 120;

const MINIO_ROOT_USER: &str = "minioadmin";
const MINIO_ROOT_PASSWORD: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "gpq-artifacts-test";

/// A running `MinIO` container plus the `aws-sdk-s3` client and bucket tests
/// use to inspect object storage directly (ADR 0008: only Remote holds S3
/// credentials in production, but tests legitimately reach in to assert on
/// bytes Remote itself never exposes any other way).
pub struct ObjectStoreFixture {
    container: TokioMutex<Option<ContainerAsync<MinIO>>>,
    endpoint: String,
    /// Direct S3 client for test assertions and fixture setup.
    pub client: aws_sdk_s3::Client,
    /// The bucket this fixture created and configured Remote to use.
    pub bucket: String,
}

impl ObjectStoreFixture {
    /// Starts `MinIO` and creates its bucket.
    ///
    /// # Errors
    /// Returns an error if the container fails to start, the mapped API
    /// port cannot be read, or the bucket cannot be created.
    pub async fn start() -> anyhow::Result<Self> {
        let container = MinIO::default()
            .start()
            .await
            .context("starting the MinIO testcontainer")?;
        let host = container
            .get_host()
            .await
            .context("reading the MinIO container host")?;
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .context("reading the MinIO container's mapped API port")?;
        let endpoint = format!("http://{host}:{port}");

        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_config::Region::new(REGION))
            .endpoint_url(&endpoint)
            .credentials_provider(Credentials::new(
                MINIO_ROOT_USER,
                MINIO_ROOT_PASSWORD,
                None,
                None,
                "objectstore-test-fixture",
            ))
            .force_path_style(true)
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);
        client
            .create_bucket()
            .bucket(BUCKET)
            .send()
            .await
            .context("creating the test bucket in MinIO")?;

        Ok(Self {
            container: TokioMutex::new(Some(container)),
            endpoint,
            client,
            bucket: BUCKET.to_owned(),
        })
    }

    /// Environment `gpq-remote serve` needs to enable object storage: the
    /// `GPQ_S3_*` settings plus the `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
    /// pair `aws-config`'s default credential chain resolves them from
    /// (ADR 0008). Ready to extend `HarnessOptions::extra_remote_env` with.
    #[must_use]
    pub fn extra_remote_env(&self) -> Vec<(String, String)> {
        vec![
            ("GPQ_S3_BUCKET".to_owned(), self.bucket.clone()),
            ("GPQ_S3_REGION".to_owned(), REGION.to_owned()),
            ("GPQ_S3_ENDPOINT".to_owned(), self.endpoint.clone()),
            (
                "GPQ_S3_PRESIGN_TTL_SECS".to_owned(),
                PRESIGN_TTL_SECS.to_string(),
            ),
            ("AWS_ACCESS_KEY_ID".to_owned(), MINIO_ROOT_USER.to_owned()),
            (
                "AWS_SECRET_ACCESS_KEY".to_owned(),
                MINIO_ROOT_PASSWORD.to_owned(),
            ),
        ]
    }

    /// Removes the `MinIO` container through `testcontainers`' own API,
    /// mirroring `e2e_support::reap_shared_container`: a container held in a
    /// `static` for the process's lifetime is otherwise never dropped.
    ///
    /// # Errors
    /// Returns an error if the container is still running but cannot be
    /// removed.
    pub async fn teardown(&self) -> anyhow::Result<()> {
        let taken = self.container.lock().await.take();
        if let Some(container) = taken {
            container
                .rm()
                .await
                .context("removing the MinIO container")?;
        }
        Ok(())
    }
}
