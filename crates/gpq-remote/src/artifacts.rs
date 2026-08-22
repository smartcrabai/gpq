//! Artifact object storage, in-memory relay buffers, and the one-shot output
//! download route (ADR 0006, ADR 0008).
//!
//! Object storage is optional: without `RemoteConfig::object_store`,
//! [`ArtifactService::object_store_available`] is `false` and every S3-backed
//! method returns a descriptive error instead of touching a client, but
//! readiness, text-only work, synchronous `OpenAI` image relay, and
//! Worker-local outputs are unaffected (ADR 0008).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context as _, bail};
use aws_sdk_s3::presigning::PresigningConfig;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::{DateTime, Utc};
use gpq_domain::{ArtifactId, ArtifactPlacement, ArtifactState, TenantId, WorkerId};
use gpq_proto::gpq::worker::v1::__buffa::oneof::remote_message::Message as RemoteMessageKind;
use gpq_proto::gpq::worker::v1::{DeliverRequest, DiscardOutput, RemoteMessage};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;
use uuid::Uuid;

use crate::config::RemoteConfig;
use crate::db::artifacts as db_artifacts;
use crate::db::artifacts::{ArtifactDirection, ArtifactRow};
use crate::registry::SendOutcome;
use crate::state::AppState;

/// Hard ceiling on how many bytes of a single Artifact this process buffers
/// in memory (inline-relay inputs, and the resumable Worker-local delivery
/// channel's internal accounting). It mirrors the `tenants.max_output_artifact_bytes`
/// default from the initial migration as a defensive limit: the real
/// Tenant-configured limit is enforced at admission, before bytes ever reach
/// here; this only stops a single buffered Artifact from exhausting Remote's
/// memory (ADR 0008).
pub const LOCAL_BUFFER_CEILING_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// How many chunks a live Worker-local delivery may have in flight before the
/// producing Worker's stream is backpressured.
const DELIVERY_CHANNEL_CAPACITY: usize = 4;

/// How many times a Worker-local download retries after an internal stream
/// break while the external client is still connected (ADR 0008).
const MAX_RESUME_ATTEMPTS: u32 = 5;

/// Base delay before retrying a control-channel send that reported
/// [`SendOutcome::Backpressured`], scaled by attempt number. Retries share
/// [`MAX_RESUME_ATTEMPTS`] with internal stream breaks — one resume budget
/// per delivery attempt, not a separate ceiling per failure kind.
const BACKPRESSURE_RETRY_DELAY: Duration = Duration::from_millis(50);

/// How long `stream_worker_local` waits for the next [`DeliveryChunk`] once
/// a Worker has accepted a `DeliverRequest`, before treating a
/// live-but-silent Worker as failed. Reset on every received chunk, so it
/// bounds inter-chunk silence rather than total transfer duration — a slow
/// but steadily progressing transfer never trips it (ADR 0008: an accepted
/// delivery that never produces a byte must not hold the Artifact
/// `delivering` forever).
const DELIVERY_CHUNK_TIMEOUT: Duration = Duration::from_secs(30);

/// One chunk of a live Worker-local Artifact delivery, relayed from the
/// producing Worker's `WorkerTransferService::DeliverArtifact` RPC to the
/// downloading HTTP client.
#[derive(Debug, Clone)]
pub enum DeliveryChunk {
    /// Raw bytes at the next expected offset.
    Data(Vec<u8>),
    /// The Worker finished sending; the caller already validated the digest.
    Complete,
    /// The Worker (or Remote) aborted the delivery.
    Failed(String),
}

/// Artifact object storage, in-memory relay buffers, and download routing.
#[derive(Clone)]
pub struct ArtifactService {
    inner: Arc<Inner>,
}

struct Inner {
    client: Option<aws_sdk_s3::Client>,
    bucket: String,
    presign_ttl: Duration,
    local: Mutex<HashMap<ArtifactId, Vec<u8>>>,
    deliveries: Mutex<HashMap<ArtifactId, mpsc::Sender<DeliveryChunk>>>,
}

fn as_u64(len: usize) -> u64 {
    u64::try_from(len).unwrap_or(u64::MAX)
}

/// Grows a buffered length by `additional` bytes, rejecting overflow and
/// growth past `ceiling`. Kept pure and separate from [`ArtifactService::put_local`]
/// so the bound-enforcement logic is cheaply unit-testable without allocating
/// a real multi-gigabyte buffer.
fn checked_grow(current: u64, additional: u64, ceiling: u64) -> anyhow::Result<u64> {
    let new_len = current
        .checked_add(additional)
        .context("buffer length overflow")?;
    if new_len > ceiling {
        bail!("growing to {new_len} bytes exceeds the {ceiling}-byte buffering ceiling");
    }
    Ok(new_len)
}

impl ArtifactService {
    /// Builds the service. Without a configured object store this still
    /// succeeds — [`Self::object_store_available`] reports `false` and every
    /// S3-backed method fails explicitly instead of panicking (ADR 0008).
    ///
    /// # Errors
    ///
    /// Currently infallible: it never returns `Err`. It stays `async fn
    /// -> Result` because credential-chain resolution through `aws_config`
    /// is loaded here, and because it shares a signature with the rest of
    /// this type's S3-backed constructors.
    pub async fn new(config: &RemoteConfig) -> anyhow::Result<Self> {
        let (client, bucket, presign_ttl) = match &config.object_store {
            None => (None, String::new(), Duration::from_mins(15)),
            Some(object_store) => {
                let region = aws_config::Region::new(object_store.region.clone());
                let mut loader =
                    aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);
                if let Some(endpoint) = &object_store.endpoint {
                    loader = loader.endpoint_url(endpoint.clone());
                }
                let shared_config = loader.load().await;
                let mut builder = aws_sdk_s3::config::Builder::from(&shared_config);
                if object_store.endpoint.is_some() {
                    // Path-style addressing for S3-compatible endpoints that don't
                    // support virtual-hosted-style bucket subdomains.
                    builder = builder.force_path_style(true);
                }
                let client = aws_sdk_s3::Client::from_conf(builder.build());
                (
                    Some(client),
                    object_store.bucket.clone(),
                    object_store.presign_ttl,
                )
            }
        };
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                bucket,
                presign_ttl,
                local: Mutex::new(HashMap::new()),
                deliveries: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Whether S3-compatible object storage is configured.
    #[must_use]
    pub fn object_store_available(&self) -> bool {
        self.inner.client.is_some()
    }

    fn client(&self) -> anyhow::Result<&aws_sdk_s3::Client> {
        self.inner
            .client
            .as_ref()
            .context("object storage is not configured for this Remote (ADR 0008)")
    }

    /// Presigns a `PUT` for uploading an Artifact matching `manifest`.
    ///
    /// # Errors
    ///
    /// Returns an error if object storage is not configured for this
    /// Remote, if `manifest.size_bytes` does not fit an S3 `content-length`
    /// (`i64`), if the configured presign TTL is invalid, if S3 rejects the
    /// presign request (network failure or a non-2xx response), or if the
    /// resulting presigned URI does not parse as a [`Url`].
    pub async fn presign_put(
        &self,
        key: &str,
        manifest: &gpq_domain::ArtifactManifest,
    ) -> anyhow::Result<(Url, DateTime<Utc>)> {
        let client = self.client()?;
        let size = i64::try_from(manifest.size_bytes)
            .context("artifact size does not fit an S3 content-length")?;
        let presigning =
            PresigningConfig::expires_in(self.inner.presign_ttl).context("invalid presign TTL")?;
        let presigned = client
            .put_object()
            .bucket(self.inner.bucket.as_str())
            .key(key)
            .content_length(size)
            .content_type(manifest.mime_type.as_str())
            .presigned(presigning)
            .await
            .context("failed to presign artifact upload")?;
        let url = Url::parse(presigned.uri()).context("presigned PUT URL was not a valid URL")?;
        let ttl =
            chrono::Duration::from_std(self.inner.presign_ttl).unwrap_or(chrono::Duration::zero());
        Ok((url, Utc::now() + ttl))
    }

    /// Presigns a `PUT` with no pinned size or content type, for an output
    /// Artifact whose real [`gpq_domain::ArtifactManifest`] is not known
    /// until the Attempt reports it (ADR 0003/0008): unlike [`Self::presign_put`],
    /// this never binds a `content-length` into the signature, so the actual
    /// upload can be any size. The real manifest (size, digest, kind, MIME
    /// type) is recorded afterward by `db::artifacts::record_output` from
    /// what the Worker reports.
    ///
    /// # Errors
    ///
    /// Returns an error if object storage is not configured for this
    /// Remote, if the configured presign TTL is invalid, if S3 rejects the
    /// presign request (network failure or a non-2xx response), or if the
    /// resulting presigned URI does not parse as a [`Url`].
    pub async fn presign_put_unsized(&self, key: &str) -> anyhow::Result<(Url, DateTime<Utc>)> {
        let client = self.client()?;
        let presigning =
            PresigningConfig::expires_in(self.inner.presign_ttl).context("invalid presign TTL")?;
        let presigned = client
            .put_object()
            .bucket(self.inner.bucket.as_str())
            .key(key)
            .presigned(presigning)
            .await
            .context("failed to presign artifact upload")?;
        let url = Url::parse(presigned.uri()).context("presigned PUT URL was not a valid URL")?;
        let ttl =
            chrono::Duration::from_std(self.inner.presign_ttl).unwrap_or(chrono::Duration::zero());
        Ok((url, Utc::now() + ttl))
    }

    /// Presigns a `GET` for downloading an object-store Artifact.
    ///
    /// # Errors
    ///
    /// Returns an error if object storage is not configured for this
    /// Remote, if the configured presign TTL is invalid, if S3 rejects the
    /// presign request (network failure or a non-2xx response), or if the
    /// resulting presigned URI does not parse as a [`Url`].
    pub async fn presign_get(&self, key: &str) -> anyhow::Result<(Url, DateTime<Utc>)> {
        let client = self.client()?;
        let presigning =
            PresigningConfig::expires_in(self.inner.presign_ttl).context("invalid presign TTL")?;
        let presigned = client
            .get_object()
            .bucket(self.inner.bucket.as_str())
            .key(key)
            .presigned(presigning)
            .await
            .context("failed to presign artifact download")?;
        let url = Url::parse(presigned.uri()).context("presigned GET URL was not a valid URL")?;
        let ttl =
            chrono::Duration::from_std(self.inner.presign_ttl).unwrap_or(chrono::Duration::zero());
        Ok((url, Utc::now() + ttl))
    }

    /// Deletes an object-store Artifact, including any multipart upload left
    /// dangling for the same key (ADR 0008: "S3 transfers use multipart
    /// operations"). Both the object delete and the multipart cleanup are
    /// always attempted, even if one fails: a transient error deleting the
    /// object must not skip aborting a dangling multipart upload (or vice
    /// versa), which would otherwise bill indefinitely.
    ///
    /// # Errors
    ///
    /// Returns an error if object storage is not configured for this
    /// Remote, or if either the object delete or the dangling-multipart
    /// cleanup fails (network failure, a non-2xx S3 response, or an
    /// individual multipart abort failing). Both operations are always
    /// attempted rather than short-circuited: if only one side fails, its
    /// error is returned; if both fail, the object-delete error is returned
    /// with the multipart-cleanup error appended as additional context.
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let client = self.client()?;
        let delete_result = client
            .delete_object()
            .bucket(self.inner.bucket.as_str())
            .key(key)
            .send()
            .await
            .context("failed to delete artifact object");
        let cleanup_result = self.abort_dangling_multipart_uploads(client, key).await;

        match (delete_result, cleanup_result) {
            (Ok(_), Ok(())) => Ok(()),
            (Err(err), Ok(())) | (Ok(_), Err(err)) => Err(err),
            (Err(delete_err), Err(cleanup_err)) => {
                Err(delete_err.context(format!("also: {cleanup_err}")))
            }
        }
    }

    /// Aborts every multipart upload left dangling for `key`, aggregating
    /// per-upload failures into one error rather than stopping at the first
    /// (ADR 0008: "S3 transfers use multipart operations").
    async fn abort_dangling_multipart_uploads(
        &self,
        client: &aws_sdk_s3::Client,
        key: &str,
    ) -> anyhow::Result<()> {
        let uploads = client
            .list_multipart_uploads()
            .bucket(self.inner.bucket.as_str())
            .prefix(key)
            .send()
            .await
            .context("failed to list dangling multipart uploads")?;
        let mut errors = Vec::new();
        for upload in uploads.uploads() {
            if upload.key() != Some(key) {
                continue;
            }
            let Some(upload_id) = upload.upload_id() else {
                continue;
            };
            if let Err(err) = client
                .abort_multipart_upload()
                .bucket(self.inner.bucket.as_str())
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
            {
                errors.push(format!("upload {upload_id}: {err}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            bail!(
                "failed to abort {} dangling multipart upload(s): {}",
                errors.len(),
                errors.join("; ")
            );
        }
    }

    fn lock_local(&self) -> std::sync::MutexGuard<'_, HashMap<ArtifactId, Vec<u8>>> {
        self.inner
            .local
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_deliveries(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<ArtifactId, mpsc::Sender<DeliveryChunk>>> {
        self.inner
            .deliveries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Appends `chunk` at `offset` to an Artifact's in-memory buffer (inline
    /// relay input bytes or a small worker-local transfer), returning the
    /// buffer's new length. Rejects an out-of-order `offset` and rejects
    /// growth past [`LOCAL_BUFFER_CEILING_BYTES`].
    ///
    /// # Errors
    ///
    /// Returns an error if `offset` does not equal the buffer's current
    /// length (an out-of-order chunk), or if appending `chunk` would
    /// overflow a `u64` length or grow the buffer past
    /// [`LOCAL_BUFFER_CEILING_BYTES`].
    pub fn put_local(
        &self,
        artifact: ArtifactId,
        offset: u64,
        chunk: &[u8],
    ) -> anyhow::Result<u64> {
        let mut local = self.lock_local();
        let buffer = local.entry(artifact).or_default();
        let current = as_u64(buffer.len());
        if offset != current {
            bail!(
                "out-of-order chunk for artifact {artifact}: expected offset {current}, got {offset}"
            );
        }
        let new_len = checked_grow(current, as_u64(chunk.len()), LOCAL_BUFFER_CEILING_BYTES)
            .with_context(|| format!("artifact {artifact} cannot buffer this chunk"))?;
        buffer.extend_from_slice(chunk);
        Ok(new_len)
    }

    /// Removes and returns an Artifact's buffered bytes, if any.
    #[must_use]
    pub fn take_local(&self, artifact: ArtifactId) -> Option<Vec<u8>> {
        self.lock_local().remove(&artifact)
    }

    /// Returns an Artifact's currently buffered length, without consuming it.
    #[must_use]
    pub fn len_local(&self, artifact: ArtifactId) -> Option<u64> {
        self.lock_local()
            .get(&artifact)
            .map(|buffer| as_u64(buffer.len()))
    }

    /// Drops an Artifact's buffered bytes without returning them, e.g. once
    /// they are known to be unneeded (Generation terminated, download
    /// abandoned).
    pub fn discard_local(&self, artifact: ArtifactId) {
        self.lock_local().remove(&artifact);
    }

    /// Registers interest in a Worker-local delivery, replacing any previous
    /// subscription for the same Artifact. Only one downloader is ever active
    /// per Artifact — [`db::artifacts::begin_delivery`](crate::db::artifacts::begin_delivery)
    /// enforces that before this is called.
    #[must_use]
    pub fn subscribe_delivery(&self, artifact: ArtifactId) -> mpsc::Receiver<DeliveryChunk> {
        let (tx, rx) = mpsc::channel(DELIVERY_CHANNEL_CAPACITY);
        self.lock_deliveries().insert(artifact, tx);
        rx
    }

    /// Forwards one chunk from the producing Worker to the subscribed
    /// downloader. Returns `false` when nobody is listening, so the Worker
    /// transfer RPC can reject the chunk instead of silently dropping it.
    pub async fn push_delivery(&self, artifact: ArtifactId, chunk: DeliveryChunk) -> bool {
        let sender = self.lock_deliveries().get(&artifact).cloned();
        match sender {
            Some(tx) => tx.send(chunk).await.is_ok(),
            None => false,
        }
    }

    /// Ends a delivery subscription, e.g. after the client disconnects or the
    /// transfer completes or fails permanently.
    pub fn end_delivery(&self, artifact: ArtifactId) {
        self.lock_deliveries().remove(&artifact);
    }
}

/// Relative path of the one-shot output download route for an Artifact.
/// Native snapshots (`GetGeneration`/`ListGenerations`/`WatchGeneration`)
/// report this joined against `RemoteConfig::public_base_url` as
/// `ArtifactRef.download_path`.
#[must_use]
pub fn download_path(artifact: ArtifactId) -> String {
    format!("/v1/artifacts/{artifact}")
}

/// Absolute download URL for an Artifact, built from the configured public
/// base URL.
///
/// # Errors
///
/// Returns an error if `base` cannot be joined with the Artifact's download
/// path (e.g. `base` is not a valid base for relative-URL joins).
pub fn download_url(base: &Url, artifact: ArtifactId) -> anyhow::Result<Url> {
    base.join(&download_path(artifact))
        .context("public_base_url could not be joined with the artifact download path")
}

/// The one-shot output download route's outcome for a given Artifact lookup
/// (ADR 0008). Kept as a pure function so it is unit-testable without a
/// running router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadOutcome {
    /// No downloadable output Artifact exists for this id.
    NotFound,
    /// Stream the Worker-local bytes directly.
    Deliver,
    /// Redirect to a presigned object-store URL.
    Redirect,
    /// Consumed, expired, or lost: `410 Gone`.
    Gone,
    /// Already being delivered: `409 Conflict`.
    Conflict,
    /// The producing Worker is offline: `503`, retryable.
    WorkerOffline,
}

/// Classifies a download attempt from the Artifact's current row and whether
/// its producing Worker (if any) is online.
#[must_use]
pub fn classify_download(row: Option<&ArtifactRow>, worker_online: bool) -> DownloadOutcome {
    let Some(row) = row else {
        return DownloadOutcome::NotFound;
    };
    if row.direction != ArtifactDirection::Output {
        return DownloadOutcome::NotFound;
    }
    match row.state {
        ArtifactState::Consumed | ArtifactState::Expired | ArtifactState::Lost => {
            DownloadOutcome::Gone
        }
        ArtifactState::Delivering => DownloadOutcome::Conflict,
        ArtifactState::Pending => DownloadOutcome::NotFound,
        ArtifactState::Available => match row.placement {
            ArtifactPlacement::WorkerLocal => {
                if worker_online {
                    DownloadOutcome::Deliver
                } else {
                    DownloadOutcome::WorkerOffline
                }
            }
            ArtifactPlacement::ObjectStore => DownloadOutcome::Redirect,
            // Outputs never use inline relay (ADR 0008 limits that placement
            // to synchronous input bytes); treat it defensively as gone.
            ArtifactPlacement::InlineRelay => DownloadOutcome::Gone,
        },
    }
}

/// Serves the one-shot output download route, `GET /v1/artifacts/{artifact_id}`
/// (ADR 0006 keeps Artifact download on Axum alongside `OpenAI` and health
/// routes).
#[must_use = "the router is inert until mounted onto the application"]
pub fn download_router(state: AppState) -> axum::Router {
    axum::Router::new()
        .route("/v1/artifacts/{artifact_id}", get(download_artifact))
        .with_state(state)
}

async fn download_artifact(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(artifact_id): Path<Uuid>,
) -> Response {
    let Some(token) = crate::auth::bearer_token(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let tenant = match state.db.authenticate_master_key(token).await {
        Ok(Some(tenant)) => tenant,
        Ok(None) => return StatusCode::UNAUTHORIZED.into_response(),
        Err(err) => {
            tracing::error!(%err, "master key authentication failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let artifact = ArtifactId::from_uuid(artifact_id);

    let mut conn = match state.db.begin_tenant(tenant).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::error!(%err, "failed to open tenant-scoped transaction");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let row = match db_artifacts::get(&mut conn, tenant, artifact).await {
        Ok(row) => row,
        Err(err) => {
            tracing::error!(%err, "artifact lookup failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let worker_online = row
        .as_ref()
        .and_then(|row| row.worker_id)
        .is_some_and(|worker| state.workers.is_online(worker));

    let outcome = classify_download(row.as_ref(), worker_online);
    match outcome {
        DownloadOutcome::NotFound => StatusCode::NOT_FOUND.into_response(),
        DownloadOutcome::Gone => StatusCode::GONE.into_response(),
        DownloadOutcome::Conflict => StatusCode::CONFLICT.into_response(),
        DownloadOutcome::WorkerOffline => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        DownloadOutcome::Redirect => {
            let Some(row) = row else {
                return StatusCode::NOT_FOUND.into_response();
            };
            redirect_to_object_store(&state, conn, tenant, artifact, row).await
        }
        DownloadOutcome::Deliver => {
            let Some(row) = row else {
                return StatusCode::NOT_FOUND.into_response();
            };
            deliver_worker_local(&state, conn, tenant, artifact, row).await
        }
    }
}

async fn redirect_to_object_store(
    state: &AppState,
    mut conn: sqlx::Transaction<'static, sqlx::Postgres>,
    tenant: TenantId,
    artifact: ArtifactId,
    row: ArtifactRow,
) -> Response {
    match db_artifacts::begin_delivery(&mut conn, tenant, artifact).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::CONFLICT.into_response(),
        Err(err) => {
            tracing::error!(%err, "begin_delivery failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    let Some(object_key) = row.object_key.clone() else {
        tracing::error!(%artifact, "object-store artifact is missing its object_key");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let presigned = match state.artifacts.presign_get(&object_key).await {
        Ok((url, _)) => url,
        Err(err) => {
            tracing::error!(%err, "presign_get failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    if let Err(err) = db_artifacts::mark_consumed(&mut conn, tenant, artifact).await {
        tracing::error!(%err, "failed to mark object-store artifact consumed");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    if let Err(err) = conn.commit().await {
        tracing::error!(%err, "failed to commit object-store download transaction");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let mut response = StatusCode::FOUND.into_response();
    if let Ok(value) = header::HeaderValue::from_str(presigned.as_str()) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

async fn deliver_worker_local(
    state: &AppState,
    mut conn: sqlx::Transaction<'static, sqlx::Postgres>,
    tenant: TenantId,
    artifact: ArtifactId,
    row: ArtifactRow,
) -> Response {
    let Some(worker) = row.worker_id else {
        tracing::error!(%artifact, "worker-local artifact is missing its worker_id");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    match db_artifacts::begin_delivery(&mut conn, tenant, artifact).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::CONFLICT.into_response(),
        Err(err) => {
            tracing::error!(%err, "begin_delivery failed");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }
    if let Err(err) = conn.commit().await {
        tracing::error!(%err, "failed to commit begin-delivery transaction");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    let delivery_token = row.delivery_token.clone().unwrap_or_default();
    let (body_tx, body_rx) =
        mpsc::channel::<Result<Bytes, std::io::Error>>(DELIVERY_CHANNEL_CAPACITY);
    tokio::spawn(stream_worker_local(
        state.clone(),
        tenant,
        artifact,
        worker,
        delivery_token,
        row.committed_offset,
        body_tx,
    ));

    let mut response = Response::new(Body::from_stream(ReceiverStream::new(body_rx)));
    if let Ok(value) = header::HeaderValue::from_str(&row.manifest.mime_type) {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, row.manifest.size_bytes.into());
    response
}

fn deliver_request(artifact: ArtifactId, delivery_token: &str, offset: u64) -> RemoteMessage {
    RemoteMessage {
        message: Some(RemoteMessageKind::from(DeliverRequest {
            artifact_id: artifact.to_string(),
            delivery_token: delivery_token.to_owned(),
            offset,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn discard_output_request(
    artifact: ArtifactId,
    delivery_token: &str,
    reason: &str,
) -> RemoteMessage {
    RemoteMessage {
        message: Some(RemoteMessageKind::from(DiscardOutput {
            artifact_id: artifact.to_string(),
            delivery_token: delivery_token.to_owned(),
            reason: reason.to_owned(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// Ends a delivery, telling the producing Worker to discard the output and
/// marking the Artifact `expired` (ADR 0008 keeps the row but deletes the
/// bytes on client disconnect). Also used when a Worker-local delivery gives
/// up after exhausting its backpressure retry budget: the Worker is not
/// confirmed gone, so this reports `expired` rather than the stronger `lost`
/// claim [`mark_delivery_lost`] makes.
async fn discard_and_expire(
    state: &AppState,
    tenant: TenantId,
    artifact: ArtifactId,
    worker: WorkerId,
    delivery_token: &str,
    reason: &str,
) {
    state.artifacts.end_delivery(artifact);
    let _ = state.workers.send(
        worker,
        discard_output_request(artifact, delivery_token, reason),
    );
    match state.db.begin_tenant(tenant).await {
        Ok(mut conn) => {
            if let Err(err) =
                db_artifacts::set_state(&mut conn, tenant, artifact, ArtifactState::Expired).await
            {
                tracing::error!(%err, "failed to expire discarded artifact");
            } else if let Err(err) = conn.commit().await {
                tracing::error!(%err, "failed to commit expired-artifact transaction");
            }
        }
        Err(err) => tracing::error!(%err, "failed to open tenant-scoped transaction for discard"),
    }
}

/// Shared identifying context for one Worker-local delivery attempt, so
/// [`request_worker_delivery`] and [`drain_worker_chunks`] each take a
/// handful of arguments instead of the same five `state`/`tenant`/
/// `artifact`/`worker`/`delivery_token` values individually.
struct DeliveryContext<'a> {
    state: &'a AppState,
    tenant: TenantId,
    artifact: ArtifactId,
    worker: WorkerId,
    delivery_token: &'a str,
}

/// Outcome of asking the producing Worker to (re)start delivery at a given
/// offset (see [`request_worker_delivery`]): whether
/// [`stream_worker_local`] should move on to draining chunks, or stop
/// because the failure — and its client-facing error — has already been
/// handled.
enum DeliveryRequestOutcome {
    Ready,
    Stop,
}

/// Sends a `DeliverRequest` for `artifact` at `offset`, retrying with
/// backoff while the Worker's control channel is backpressured, sharing
/// [`MAX_RESUME_ATTEMPTS`] with [`drain_worker_chunks`]'s own retry budget
/// — one resume budget per delivery attempt, not a separate ceiling per
/// failure kind. Split out of [`stream_worker_local`] so the
/// request/backpressure phase and the chunk-draining phase are each under
/// the line-count lint on their own; behavior is unchanged.
async fn request_worker_delivery(
    ctx: &DeliveryContext<'_>,
    offset: u64,
    attempts: &mut u32,
    tx: &mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> DeliveryRequestOutcome {
    loop {
        match ctx.state.workers.send(
            ctx.worker,
            deliver_request(ctx.artifact, ctx.delivery_token, offset),
        ) {
            SendOutcome::Delivered => return DeliveryRequestOutcome::Ready,
            SendOutcome::Offline => {
                ctx.state.artifacts.end_delivery(ctx.artifact);
                mark_delivery_lost(
                    ctx.state,
                    ctx.tenant,
                    ctx.artifact,
                    "producing worker is offline",
                )
                .await;
                let _ = tx
                    .send(Err(std::io::Error::other("producing worker went offline")))
                    .await;
                return DeliveryRequestOutcome::Stop;
            }
            SendOutcome::Backpressured => {
                *attempts += 1;
                if resume_attempts_exhausted(*attempts) {
                    discard_and_expire(
                        ctx.state,
                        ctx.tenant,
                        ctx.artifact,
                        ctx.worker,
                        ctx.delivery_token,
                        "producing worker's control channel stayed backpressured across the retry budget",
                    )
                    .await;
                    let _ = tx
                        .send(Err(std::io::Error::other(
                            "producing worker is backpressured; the artifact was not delivered",
                        )))
                        .await;
                    return DeliveryRequestOutcome::Stop;
                }
                tracing::warn!(
                    artifact = %ctx.artifact,
                    attempt = *attempts,
                    "worker control channel backpressured while requesting delivery, retrying"
                );
                tokio::time::sleep(BACKPRESSURE_RETRY_DELAY * *attempts).await;
            }
        }
    }
}

/// Outcome of draining chunks for one accepted `DeliverRequest`: either the
/// transfer is fully done (delivered, or a terminal failure already
/// reported to the client), or the Worker-local delivery needs a fresh
/// `DeliverRequest` at the now-updated offset.
enum ChunkDrainOutcome {
    Done,
    ResumeDelivery,
}

/// Persists the newly committed delivery `offset` for `artifact` after a
/// chunk was successfully relayed to the client, logging (never
/// propagating) any database failure: a failure to persist here only risks
/// re-delivering already-sent bytes on the next resume, which the
/// downloading client tolerates by design (ADR 0008), so it must not abort
/// an otherwise-successful chunk relay.
async fn persist_delivery_offset(ctx: &DeliveryContext<'_>, offset: u64) {
    if let Ok(mut conn) = ctx.state.db.begin_tenant(ctx.tenant).await {
        if let Err(err) =
            db_artifacts::commit_offset(&mut conn, ctx.tenant, ctx.artifact, offset).await
        {
            tracing::error!(%err, "failed to persist delivery offset");
        } else if let Err(err) = conn.commit().await {
            tracing::error!(%err, "failed to commit delivery-offset transaction");
        }
    }
}

/// Relays [`DeliveryChunk`]s from the producing Worker to the downloading
/// HTTP client until the transfer completes, fails terminally, or the
/// Worker goes quiet for [`DELIVERY_CHUNK_TIMEOUT`]. See
/// [`request_worker_delivery`] for why this is split out of
/// [`stream_worker_local`]; behavior is unchanged.
async fn drain_worker_chunks(
    ctx: &DeliveryContext<'_>,
    offset: &mut u64,
    attempts: &mut u32,
    rx: &mut mpsc::Receiver<DeliveryChunk>,
    tx: &mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> ChunkDrainOutcome {
    loop {
        let chunk = tokio::select! {
            chunk = rx.recv() => chunk,
            () = tokio::time::sleep(DELIVERY_CHUNK_TIMEOUT) => {
                ctx.state.artifacts.end_delivery(ctx.artifact);
                mark_delivery_lost(
                    ctx.state,
                    ctx.tenant,
                    ctx.artifact,
                    "delivery timed out waiting for the next chunk",
                )
                .await;
                let _ = tx
                    .send(Err(std::io::Error::other(
                        "producing worker accepted delivery but sent nothing for too long",
                    )))
                    .await;
                return ChunkDrainOutcome::Done;
            }
        };
        match chunk {
            Some(DeliveryChunk::Data(bytes)) => {
                *offset = offset.saturating_add(as_u64(bytes.len()));
                if tx.send(Ok(Bytes::from(bytes))).await.is_err() {
                    discard_and_expire(
                        ctx.state,
                        ctx.tenant,
                        ctx.artifact,
                        ctx.worker,
                        ctx.delivery_token,
                        "client disconnected",
                    )
                    .await;
                    return ChunkDrainOutcome::Done;
                }
                persist_delivery_offset(ctx, *offset).await;
            }
            Some(DeliveryChunk::Complete) => {
                ctx.state.artifacts.end_delivery(ctx.artifact);
                if let Ok(mut conn) = ctx.state.db.begin_tenant(ctx.tenant).await {
                    if let Err(err) =
                        db_artifacts::mark_consumed(&mut conn, ctx.tenant, ctx.artifact).await
                    {
                        tracing::error!(%err, "failed to mark delivered artifact consumed");
                    } else if let Err(err) = conn.commit().await {
                        tracing::error!(%err, "failed to commit consumed-artifact transaction");
                    }
                }
                return ChunkDrainOutcome::Done;
            }
            Some(DeliveryChunk::Failed(reason)) => {
                ctx.state.artifacts.end_delivery(ctx.artifact);
                *attempts += 1;
                if resume_attempts_exhausted(*attempts) {
                    mark_delivery_lost(ctx.state, ctx.tenant, ctx.artifact, &reason).await;
                    let _ = tx
                        .send(Err(std::io::Error::other(format!(
                            "artifact delivery failed repeatedly: {reason}"
                        ))))
                        .await;
                    return ChunkDrainOutcome::Done;
                }
                tracing::warn!(
                    artifact = %ctx.artifact,
                    %reason,
                    attempt = *attempts,
                    "worker-local delivery chunk failed, retrying"
                );
                return ChunkDrainOutcome::ResumeDelivery;
            }
            None => {
                ctx.state.artifacts.end_delivery(ctx.artifact);
                *attempts += 1;
                if resume_attempts_exhausted(*attempts) {
                    mark_delivery_lost(
                        ctx.state,
                        ctx.tenant,
                        ctx.artifact,
                        "delivery subscription closed",
                    )
                    .await;
                    let _ = tx
                        .send(Err(std::io::Error::other(
                            "artifact delivery failed repeatedly",
                        )))
                        .await;
                    return ChunkDrainOutcome::Done;
                }
                return ChunkDrainOutcome::ResumeDelivery;
            }
        }
    }
}

/// Streams a Worker-local output to the HTTP client, resuming with a fresh
/// `DeliverRequest` at the last committed offset if the internal Worker
/// stream breaks while the client is still connected (ADR 0008). A
/// backpressured control-channel send is retried with backoff rather than
/// treated as the Worker being offline, and an accepted `DeliverRequest`
/// that produces no chunk within [`DELIVERY_CHUNK_TIMEOUT`] is torn down
/// deterministically instead of leaking the task and stranding the
/// Artifact `delivering` forever.
async fn stream_worker_local(
    state: AppState,
    tenant: TenantId,
    artifact: ArtifactId,
    worker: WorkerId,
    delivery_token: String,
    start_offset: u64,
    tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let ctx = DeliveryContext {
        state: &state,
        tenant,
        artifact,
        worker,
        delivery_token: &delivery_token,
    };
    let mut offset = start_offset;
    let mut attempts = 0u32;
    loop {
        let mut rx = state.artifacts.subscribe_delivery(artifact);

        let ready = request_worker_delivery(&ctx, offset, &mut attempts, &tx).await;
        if matches!(ready, DeliveryRequestOutcome::Stop) {
            return;
        }

        let drained = drain_worker_chunks(&ctx, &mut offset, &mut attempts, &mut rx, &tx).await;
        if matches!(drained, ChunkDrainOutcome::Done) {
            return;
        }
    }
}

/// Whether a Worker-local delivery has exhausted its resume attempts and
/// must be marked `lost` rather than retried again. Kept pure and separate
/// from [`stream_worker_local`] so the resume-count threshold is
/// unit-testable without a live delivery stream.
#[must_use]
fn resume_attempts_exhausted(attempts: u32) -> bool {
    attempts >= MAX_RESUME_ATTEMPTS
}

/// Marks a stuck `delivering` Artifact `lost`: the producing Worker's
/// session ended (or its delivery stream failed repeatedly) before the
/// bytes were ever delivered, and `Delivering` has no legal edge back to
/// `Available` — the only way out is a terminal state (ADR 0008). Logged,
/// not propagated: the caller already reports the delivery failure to the
/// downloading client through its own response stream.
async fn mark_delivery_lost(
    state: &AppState,
    tenant: TenantId,
    artifact: ArtifactId,
    reason: &str,
) {
    let mut conn = match state.db.begin_tenant(tenant).await {
        Ok(conn) => conn,
        Err(err) => {
            tracing::error!(%err, %artifact, "failed to open tenant-scoped transaction to mark artifact lost");
            return;
        }
    };
    if let Err(err) = db_artifacts::mark_lost(&mut conn, tenant, artifact).await {
        tracing::error!(%err, %artifact, "failed to mark undeliverable artifact lost");
        return;
    }
    if let Err(err) = conn.commit().await {
        tracing::error!(%err, %artifact, "failed to commit lost-artifact transaction");
        return;
    }
    tracing::warn!(%artifact, %reason, "marked worker-local artifact lost after undeliverable transfer");
}

#[cfg(test)]
mod tests {
    use gpq_domain::{ArtifactManifest, ContentHash, MediaKind};

    use super::*;

    fn manifest() -> ArtifactManifest {
        ArtifactManifest {
            size_bytes: 3,
            digest: ContentHash::digest(b"abc"),
            kind: MediaKind::Binary,
            mime_type: "application/octet-stream".to_owned(),
        }
    }

    fn row(
        state: ArtifactState,
        placement: ArtifactPlacement,
        direction: ArtifactDirection,
    ) -> ArtifactRow {
        ArtifactRow {
            id: ArtifactId::new(),
            direction,
            state,
            placement,
            manifest: manifest(),
            object_key: (placement == ArtifactPlacement::ObjectStore).then(|| "key".to_owned()),
            worker_id: (placement == ArtifactPlacement::WorkerLocal).then(WorkerId::new),
            delivery_token: None,
            committed_offset: 0,
        }
    }

    fn service_without_object_store() -> ArtifactService {
        ArtifactService {
            inner: Arc::new(Inner {
                client: None,
                bucket: String::new(),
                presign_ttl: Duration::from_mins(15),
                local: Mutex::new(HashMap::new()),
                deliveries: Mutex::new(HashMap::new()),
            }),
        }
    }

    #[test]
    fn download_maps_terminal_states_to_gone() {
        for state in [
            ArtifactState::Consumed,
            ArtifactState::Expired,
            ArtifactState::Lost,
        ] {
            let row = row(
                state,
                ArtifactPlacement::ObjectStore,
                ArtifactDirection::Output,
            );
            assert_eq!(classify_download(Some(&row), true), DownloadOutcome::Gone);
        }
    }

    #[test]
    fn download_maps_delivering_to_conflict() {
        let row = row(
            ArtifactState::Delivering,
            ArtifactPlacement::ObjectStore,
            ArtifactDirection::Output,
        );
        assert_eq!(
            classify_download(Some(&row), true),
            DownloadOutcome::Conflict
        );
    }

    #[test]
    fn download_maps_missing_artifact_to_not_found() {
        assert_eq!(classify_download(None, true), DownloadOutcome::NotFound);
    }

    #[test]
    fn download_maps_input_direction_to_not_found() {
        let row = row(
            ArtifactState::Available,
            ArtifactPlacement::ObjectStore,
            ArtifactDirection::Input,
        );
        assert_eq!(
            classify_download(Some(&row), true),
            DownloadOutcome::NotFound
        );
    }

    #[test]
    fn download_maps_offline_worker_local_to_worker_offline() {
        let row = row(
            ArtifactState::Available,
            ArtifactPlacement::WorkerLocal,
            ArtifactDirection::Output,
        );
        assert_eq!(
            classify_download(Some(&row), false),
            DownloadOutcome::WorkerOffline
        );
        assert_eq!(
            classify_download(Some(&row), true),
            DownloadOutcome::Deliver
        );
    }

    #[test]
    fn download_maps_object_store_available_to_redirect() {
        let row = row(
            ArtifactState::Available,
            ArtifactPlacement::ObjectStore,
            ArtifactDirection::Output,
        );
        assert_eq!(
            classify_download(Some(&row), true),
            DownloadOutcome::Redirect
        );
    }

    #[test]
    fn put_local_rejects_offset_gaps() {
        let service = service_without_object_store();
        let artifact = ArtifactId::new();
        assert_eq!(service.put_local(artifact, 0, b"ab").unwrap_or(0), 2);
        assert!(service.put_local(artifact, 5, b"cd").is_err());
        assert_eq!(service.put_local(artifact, 2, b"cd").unwrap_or(0), 4);
        assert_eq!(service.take_local(artifact), Some(b"abcd".to_vec()));
    }

    #[test]
    fn checked_grow_rejects_overflow_and_ceiling_breach() {
        assert_eq!(checked_grow(2, 3, 10).unwrap_or(0), 5);
        assert!(checked_grow(8, 3, 10).is_err());
        assert!(checked_grow(u64::MAX, 1, u64::MAX).is_err());
    }

    #[test]
    fn discard_local_drops_buffered_bytes() {
        let service = service_without_object_store();
        let artifact = ArtifactId::new();
        let _ = service.put_local(artifact, 0, b"abc");
        service.discard_local(artifact);
        assert_eq!(service.len_local(artifact), None);
        assert_eq!(service.take_local(artifact), None);
    }

    #[test]
    fn len_local_reports_length_without_consuming_the_buffer() {
        // Mirrors `WorkerTransferService::fetch_artifact`'s flow: `len_local`
        // is checked before `take_local` consumes the buffer for the
        // response, so it must not itself remove the bytes (ADR 0008).
        let service = service_without_object_store();
        let artifact = ArtifactId::new();
        let _ = service.put_local(artifact, 0, b"hello");
        assert_eq!(service.len_local(artifact), Some(5));
        assert_eq!(service.len_local(artifact), Some(5));
        assert_eq!(service.take_local(artifact), Some(b"hello".to_vec()));
        assert_eq!(service.len_local(artifact), None);
        assert_eq!(service.take_local(artifact), None);
    }

    #[test]
    fn resume_attempts_exhausted_at_the_configured_ceiling() {
        assert!(!resume_attempts_exhausted(MAX_RESUME_ATTEMPTS - 1));
        assert!(resume_attempts_exhausted(MAX_RESUME_ATTEMPTS));
        assert!(resume_attempts_exhausted(MAX_RESUME_ATTEMPTS + 1));
    }

    #[test]
    fn download_url_builds_an_absolute_url_from_the_public_base() {
        let Ok(base) = Url::parse("https://gpq.example.invalid/") else {
            panic!("expected a valid base url")
        };
        let artifact = ArtifactId::new();
        let Ok(url) = download_url(&base, artifact) else {
            panic!("expected download_url to join successfully")
        };
        assert_eq!(
            url.as_str(),
            format!("https://gpq.example.invalid/v1/artifacts/{artifact}")
        );
    }

    #[tokio::test]
    async fn push_delivery_reports_no_subscriber() {
        let service = service_without_object_store();
        let artifact = ArtifactId::new();
        assert!(
            !service
                .push_delivery(artifact, DeliveryChunk::Complete)
                .await
        );
        let mut rx = service.subscribe_delivery(artifact);
        assert!(
            service
                .push_delivery(artifact, DeliveryChunk::Data(vec![1, 2, 3]))
                .await
        );
        service.end_delivery(artifact);
        assert!(
            !service
                .push_delivery(artifact, DeliveryChunk::Complete)
                .await
        );
        assert!(
            matches!(rx.recv().await, Some(DeliveryChunk::Data(bytes)) if bytes == vec![1, 2, 3])
        );
    }

    #[test]
    fn object_store_unavailable_is_reported() {
        // Constructed synchronously to avoid depending on a Tokio runtime here.
        let service = ArtifactService {
            inner: Arc::new(Inner {
                client: None,
                bucket: String::new(),
                presign_ttl: Duration::from_mins(15),
                local: Mutex::new(HashMap::new()),
                deliveries: Mutex::new(HashMap::new()),
            }),
        };
        assert!(!service.object_store_available());
    }
}
