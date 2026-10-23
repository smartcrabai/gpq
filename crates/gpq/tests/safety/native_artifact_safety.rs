//! Consumer-level regressions for artifact admission, transfer, expiry, and cleanup.

use super::e2e_support;
use super::e2e_support::{Harness, SyntheticWorker};
use super::{harness, harness_without_object_store, object_store};

use std::time::Duration;

use anyhow::Context;
use chrono::{Duration as ChronoDuration, Utc};
use connectrpc::client::{CallOptions, ClientConfig, Http2Connection, SharedHttp2Connection};
use connectrpc::{ErrorCode, Protocol};
use futures::StreamExt;
use gpq_domain::{ContentHash, ModelFamily, TRANSFER_CHUNK_BYTES};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use gpq_proto::gpq::worker::v1::__buffa::oneof::remote_message;
use sqlx::PgPool;
use uuid::Uuid;

fn manifest_for(bytes: &[u8]) -> pb::ArtifactManifest {
    pb::ArtifactManifest {
        size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        digest_sha256: ContentHash::digest(bytes).to_hex(),
        kind: pb::MediaKind::MEDIA_KIND_BINARY.into(),
        mime_type: "application/octet-stream".to_owned(),
        ..Default::default()
    }
}

fn submit_with_reference(alias: &str, artifact: Uuid) -> pb::SubmitRequest {
    let mut request = e2e_support::image_request(alias, "use this reference");
    if let Some(pb::submit_request::GenerationParameters::ImageParameters(parameters)) =
        request.generation_parameters.as_mut()
    {
        parameters.reference_artifact_ids = vec![artifact.to_string()];
    }
    request
}

async fn create_input(
    harness: &Harness,
    bytes: &[u8],
) -> anyhow::Result<pb::CreateInputArtifactResponse> {
    harness
        .generation_client(&harness.tenant1.master_key)
        .create_input_artifact(pb::CreateInputArtifactRequest {
            manifest: manifest_for(bytes).into(),
            ..Default::default()
        })
        .await
        .map(connectrpc::UnaryResponse::into_owned)
        .map_err(|error| anyhow::anyhow!("CreateInputArtifact failed: {error}"))
}

async fn upload_remote_local(
    harness: &Harness,
    upload_url: &str,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let response = harness
        .http
        .put(upload_url)
        .bearer_auth(&harness.tenant1.master_key)
        .body(bytes.to_vec())
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "remote-local PUT failed: {}",
        response.status()
    );
    Ok(())
}

async fn upload_presigned(harness: &Harness, upload_url: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let response = harness
        .http
        .put(upload_url)
        .header("content-type", "application/octet-stream")
        .body(bytes.to_vec())
        .send()
        .await?;
    anyhow::ensure!(
        response.status().is_success(),
        "presigned PUT failed: {}",
        response.status()
    );
    Ok(())
}

async fn verify_remote_local_claim_and_expiry(harness: &Harness) -> anyhow::Result<()> {
    let worker =
        SyntheticWorker::spawn(harness, "artifact-ready-local", ModelFamily::QwenImage21).await?;
    let bytes = b"verified input bytes";
    let pending = create_input(harness, bytes).await?;
    let artifact_id: Uuid = pending.artifact_id.parse()?;

    let rejected = harness
        .submit(submit_with_reference(&worker.alias, artifact_id))
        .await;
    anyhow::ensure!(
        rejected.is_err_and(|error| error.code == ErrorCode::InvalidArgument),
        "a pending remote-local input entered the queue"
    );

    upload_remote_local(harness, &pending.upload_url, bytes).await?;
    harness
        .submit(submit_with_reference(&worker.alias, artifact_id))
        .await
        .context("verified upload could not be submitted")?;
    let reused = harness
        .submit(submit_with_reference(&worker.alias, artifact_id))
        .await;
    anyhow::ensure!(
        reused.is_err_and(|error| error.code == ErrorCode::InvalidArgument),
        "one input Artifact was claimed by more than one Generation"
    );

    let expired = create_input(harness, bytes).await?;
    let expired_id: Uuid = expired.artifact_id.parse()?;
    upload_remote_local(harness, &expired.upload_url, bytes).await?;
    sqlx::query(
        "UPDATE artifacts SET expires_at = now() - interval '1 minute' \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(harness.tenant1.id)
    .bind(expired_id)
    .execute(harness.admin_pool())
    .await?;
    let expired_submit = harness
        .submit(submit_with_reference(&worker.alias, expired_id))
        .await;
    anyhow::ensure!(
        expired_submit.is_err_and(|error| error.code == ErrorCode::InvalidArgument),
        "an expired input Artifact entered the queue"
    );
    let (generation_id, ttl_elapsed): (Option<Uuid>, bool) = sqlx::query_as(
        "SELECT generation_id, expires_at IS NOT NULL AND expires_at <= now() \
         FROM artifacts WHERE tenant_id = $1 AND id = $2",
    )
    .bind(harness.tenant1.id)
    .bind(expired_id)
    .fetch_one(harness.admin_pool())
    .await?;
    anyhow::ensure!(
        generation_id.is_none(),
        "failed admission linked the expired input"
    );
    anyhow::ensure!(ttl_elapsed, "failed admission cleared the input TTL");
    Ok(())
}

async fn verify_cancelled_remote_local_input_cleanup(harness: &Harness) -> anyhow::Result<()> {
    let cancel_worker =
        SyntheticWorker::spawn(harness, "artifact-cancel-local", ModelFamily::QwenImage21).await?;
    let cancel_alias = cancel_worker.alias.clone();
    let cancel_name = cancel_worker.name.clone();
    let cancel_worker_id: Uuid = cancel_worker.worker_id.parse()?;
    let mut cancel_worker = cancel_worker;
    cancel_worker.disconnect();
    e2e_support::wait_until(
        || async { harness.worker_is_online(&cancel_name, false).await },
        Duration::from_secs(10),
    )
    .await
    .context("the local cancellation worker did not disconnect")?;
    sqlx::query("UPDATE device_pools SET ready = false WHERE tenant_id = $1 AND worker_id = $2")
        .bind(harness.tenant1.id)
        .bind(cancel_worker_id)
        .execute(harness.admin_pool())
        .await?;
    let local_input = create_input(harness, b"cancel local bytes").await?;
    let local_artifact: Uuid = local_input.artifact_id.parse()?;
    upload_remote_local(harness, &local_input.upload_url, b"cancel local bytes").await?;
    let queued = harness
        .submit(submit_with_reference(&cancel_alias, local_artifact))
        .await
        .context("failed to queue a local-input generation")?;
    anyhow::ensure!(
        queued.state == pb::GenerationState::GENERATION_STATE_QUEUED,
        "local generation was not queued"
    );
    let cancelled = harness
        .generation_client(&harness.tenant1.master_key)
        .cancel_generation(pb::CancelGenerationRequest {
            generation_id: queued.generation_id,
            ..Default::default()
        })
        .await
        .map_err(|error| anyhow::anyhow!("CancelGeneration failed: {error}"))?
        .into_owned();
    anyhow::ensure!(
        cancelled.generation.into_option().is_some_and(
            |generation| generation.state == pb::GenerationState::GENERATION_STATE_CANCELLED
        ),
        "local queued generation did not become cancelled"
    );
    let content: Option<i32> = sqlx::query_scalar(
        "SELECT octet_length(content) FROM artifacts WHERE tenant_id = $1 AND id = $2",
    )
    .bind(harness.tenant1.id)
    .bind(local_artifact)
    .fetch_optional(harness.admin_pool())
    .await?;
    anyhow::ensure!(
        content.is_none(),
        "queued cancellation retained remote-local input bytes or metadata"
    );
    Ok(())
}

#[test]
fn remote_local_claim_requires_uploaded_unexpired_bytes_and_preserves_single_use()
-> anyhow::Result<()> {
    let harness = harness_without_object_store();
    e2e_support::block_on(async {
        verify_remote_local_claim_and_expiry(&harness).await?;
        verify_cancelled_remote_local_input_cleanup(&harness).await
    })
}

async fn verify_object_store_upload_readiness(harness: &Harness) -> anyhow::Result<()> {
    let worker =
        SyntheticWorker::spawn(harness, "artifact-ready-s3", ModelFamily::QwenImage21).await?;
    let bytes = b"uploaded S3 input";
    let pending = create_input(harness, bytes).await?;
    let pending_id: Uuid = pending.artifact_id.parse()?;
    let before_upload = harness
        .submit(submit_with_reference(&worker.alias, pending_id))
        .await;
    anyhow::ensure!(
        before_upload.is_err_and(|error| error.code == ErrorCode::InvalidArgument),
        "an object-store input without an uploaded object entered the queue"
    );
    upload_presigned(harness, &pending.upload_url, bytes).await?;
    harness
        .submit(submit_with_reference(&worker.alias, pending_id))
        .await
        .context("a completed presigned upload was not ready for Submit")?;
    Ok(())
}

async fn verify_object_store_cancellation_cleanup(
    harness: &Harness,
    store: &super::ObjectStoreUse,
) -> anyhow::Result<()> {
    let cancelled_worker =
        SyntheticWorker::spawn(harness, "artifact-cancel-s3", ModelFamily::QwenImage21).await?;
    let cancelled_alias = cancelled_worker.alias.clone();
    let cancelled_name = cancelled_worker.name.clone();
    let cancelled_worker_id: Uuid = cancelled_worker.worker_id.parse()?;
    let mut cancelled_worker = cancelled_worker;
    cancelled_worker.disconnect();
    e2e_support::wait_until(
        || async { harness.worker_is_online(&cancelled_name, false).await },
        Duration::from_secs(10),
    )
    .await
    .context("the cancellation worker did not disconnect")?;
    sqlx::query("UPDATE device_pools SET ready = false WHERE tenant_id = $1 AND worker_id = $2")
        .bind(harness.tenant1.id)
        .bind(cancelled_worker_id)
        .execute(harness.admin_pool())
        .await?;

    let queued_input = create_input(harness, b"cancel this input").await?;
    let queued_artifact: Uuid = queued_input.artifact_id.parse()?;
    let queued_key: String =
        sqlx::query_scalar("SELECT object_key FROM artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(harness.tenant1.id)
            .bind(queued_artifact)
            .fetch_one(harness.admin_pool())
            .await?;
    upload_presigned(harness, &queued_input.upload_url, b"cancel this input").await?;
    let queued = harness
        .submit(submit_with_reference(&cancelled_alias, queued_artifact))
        .await
        .context("failed to queue the generation for cancellation")?;
    anyhow::ensure!(
        queued.state == pb::GenerationState::GENERATION_STATE_QUEUED,
        "generation was not queued"
    );
    let cancelled = harness
        .generation_client(&harness.tenant1.master_key)
        .cancel_generation(pb::CancelGenerationRequest {
            generation_id: queued.generation_id.clone(),
            ..Default::default()
        })
        .await
        .map_err(|error| anyhow::anyhow!("CancelGeneration failed: {error}"))?
        .into_owned();
    anyhow::ensure!(
        cancelled.generation.into_option().is_some_and(
            |generation| generation.state == pb::GenerationState::GENERATION_STATE_CANCELLED
        ),
        "queued generation did not become cancelled"
    );
    let metadata: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(harness.tenant1.id)
            .bind(queued_artifact)
            .fetch_optional(harness.admin_pool())
            .await?;
    anyhow::ensure!(
        metadata.is_none(),
        "queued cancellation retained input metadata"
    );
    anyhow::ensure!(
        store
            .client
            .head_object()
            .bucket(&store.bucket)
            .key(&queued_key)
            .send()
            .await
            .is_err(),
        "queued cancellation retained input bytes in object storage"
    );
    Ok(())
}

async fn verify_unclaimed_object_store_input_expiry(
    harness: &Harness,
    store: &super::ObjectStoreUse,
) -> anyhow::Result<()> {
    let unclaimed = create_input(harness, b"unclaimed S3 input").await?;
    let unclaimed_id: Uuid = unclaimed.artifact_id.parse()?;
    let unclaimed_key: String =
        sqlx::query_scalar("SELECT object_key FROM artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(harness.tenant1.id)
            .bind(unclaimed_id)
            .fetch_one(harness.admin_pool())
            .await?;
    upload_presigned(harness, &unclaimed.upload_url, b"unclaimed S3 input").await?;
    let has_deadline: bool = sqlx::query_scalar(
        "SELECT expires_at IS NOT NULL AND expires_at > now() FROM artifacts \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(harness.tenant1.id)
    .bind(unclaimed_id)
    .fetch_one(harness.admin_pool())
    .await?;
    anyhow::ensure!(
        has_deadline,
        "unclaimed object-store input has no live database TTL"
    );
    sqlx::query(
        "UPDATE artifacts SET expires_at = now() - interval '1 minute' \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(harness.tenant1.id)
    .bind(unclaimed_id)
    .execute(harness.admin_pool())
    .await?;
    e2e_support::wait_until(
        || async {
            let state: String =
                sqlx::query_scalar("SELECT state FROM artifacts WHERE tenant_id = $1 AND id = $2")
                    .bind(harness.tenant1.id)
                    .bind(unclaimed_id)
                    .fetch_one(harness.admin_pool())
                    .await?;
            let object_missing = store
                .client
                .head_object()
                .bucket(&store.bucket)
                .key(&unclaimed_key)
                .send()
                .await
                .is_err();
            Ok((state == "expired" && object_missing).then_some(()))
        },
        Duration::from_secs(20),
    )
    .await
    .context("expired unclaimed input was not swept from the database and object store")?;
    Ok(())
}

async fn verify_expired_output_download_does_not_sweep(harness: &Harness) -> anyhow::Result<()> {
    let generation =
        insert_generation(harness.admin_pool(), harness.tenant1.id, "expired-download").await?;
    let expired_output = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO artifacts \
         (tenant_id, id, generation_id, direction, state, placement, size_bytes, \
          digest_sha256, kind, mime_type, object_key, available_at, expires_at) \
         VALUES ($1, $2, $3, 'output', 'available', 'object_store', 3, $4, \
                 'binary', 'application/octet-stream', $5, now(), now() - interval '1 minute')",
    )
    .bind(harness.tenant1.id)
    .bind(expired_output)
    .bind(generation)
    .bind(ContentHash::digest(b"abc").to_hex())
    .bind(format!("tenants/{}/expired-output", harness.tenant1.id))
    .execute(harness.admin_pool())
    .await?;
    let available_before: String =
        sqlx::query_scalar("SELECT state FROM artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(harness.tenant1.id)
            .bind(expired_output)
            .fetch_one(harness.admin_pool())
            .await?;
    anyhow::ensure!(
        available_before == "available",
        "expiry sweep ran before download acquisition"
    );
    let download = harness
        .http
        .get(harness.url(&format!("/v1/artifacts/{expired_output}")))
        .bearer_auth(&harness.tenant1.master_key)
        .send()
        .await?;
    anyhow::ensure!(
        download.status() == reqwest::StatusCode::GONE,
        "expired output returned {}",
        download.status()
    );
    let state_after: String =
        sqlx::query_scalar("SELECT state FROM artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(harness.tenant1.id)
            .bind(expired_output)
            .fetch_one(harness.admin_pool())
            .await?;
    anyhow::ensure!(
        state_after == "available",
        "download acquisition relied on the expiry sweep"
    );
    Ok(())
}

#[test]
fn object_store_inputs_require_head_readiness_and_expire_or_clean_up_at_terminal_paths()
-> anyhow::Result<()> {
    let harness = harness();
    let store = object_store();
    e2e_support::block_on(async {
        verify_object_store_upload_readiness(&harness).await?;
        verify_object_store_cancellation_cleanup(&harness, &store).await?;
        verify_unclaimed_object_store_input_expiry(&harness, &store).await?;
        verify_expired_output_download_does_not_sweep(&harness).await
    })
}

async fn insert_generation(pool: &PgPool, tenant: Uuid, name: &str) -> anyhow::Result<Uuid> {
    let id = Uuid::now_v7();
    let version_sha256 = ContentHash::digest(name.as_bytes()).to_hex();
    sqlx::query(
        "INSERT INTO generations \
         (tenant_id, id, state, modality, alias, version_sha256, priority, execution_timeout, \
          output_placement, attempt_count, accepted_attempt_id, failure_kind, failure_message, \
          created_at, updated_at) \
         VALUES ($1, $2, 'queued', 'image', 'test-alias', $3, 5, interval '30 minutes', \
                 'worker_local', 0, NULL, NULL, '', now(), now())",
    )
    .bind(tenant)
    .bind(id)
    .bind(version_sha256)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_attempt_input(
    harness: &Harness,
    tenant: Uuid,
    worker: Uuid,
    pool: Uuid,
    name: &str,
    bytes: &[u8],
) -> anyhow::Result<(Uuid, Uuid)> {
    let db = harness.admin_pool();
    let generation_id = insert_generation(db, tenant, name).await?;
    sqlx::query(
        "UPDATE generations SET state = 'running', attempt_count = 1 \
         WHERE tenant_id = $1 AND id = $2",
    )
    .bind(tenant)
    .bind(generation_id)
    .execute(db)
    .await?;

    let attempt_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO attempts \
         (tenant_id, id, generation_id, attempt_number, state, worker_id, pool_id, slot_key, \
          lease_expires_at) \
         VALUES ($1, $2, $3, 1, 'running', $4, $5, 'slot-0', $6)",
    )
    .bind(tenant)
    .bind(attempt_id)
    .bind(generation_id)
    .bind(worker)
    .bind(pool)
    .bind(Utc::now() + ChronoDuration::minutes(30))
    .execute(db)
    .await?;

    let artifact_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO artifacts \
         (tenant_id, id, generation_id, direction, state, placement, size_bytes, \
          digest_sha256, kind, mime_type, content, available_at) \
         VALUES ($1, $2, $3, 'input', 'available', 'remote_local', $4, $5, \
                 'binary', 'application/octet-stream', $6, now())",
    )
    .bind(tenant)
    .bind(artifact_id)
    .bind(generation_id)
    .bind(i64::try_from(bytes.len())?)
    .bind(ContentHash::digest(bytes).to_hex())
    .bind(bytes)
    .execute(db)
    .await?;
    Ok((attempt_id, artifact_id))
}

async fn transfer_client(
    uri: &http::Uri,
    credential: &str,
) -> anyhow::Result<wpb::WorkerTransferServiceClient<SharedHttp2Connection>> {
    let connection = Http2Connection::builder()
        .initial_stream_window_size(64 * 1024)
        .initial_connection_window_size(64 * 1024)
        .connect_plaintext(uri.clone())
        .await?;
    let config = ClientConfig::new(uri.clone())
        .with_protocol(Protocol::Grpc)
        .with_default_header("authorization", format!("Bearer {credential}"));
    Ok(wpb::WorkerTransferServiceClient::new(
        connection.shared(16),
        config,
    ))
}

async fn expect_fetch_denied(
    uri: &http::Uri,
    credential: &str,
    attempt: Uuid,
    artifact: Uuid,
) -> anyhow::Result<()> {
    let client = transfer_client(uri, credential).await?;
    let result = client
        .fetch_artifact_with_options(
            wpb::FetchArtifactRequest {
                attempt_id: attempt.to_string(),
                artifact_id: artifact.to_string(),
                ..Default::default()
            },
            CallOptions::default(),
        )
        .await;
    anyhow::ensure!(
        result.is_err(),
        "unauthorized FetchArtifact request succeeded"
    );
    Ok(())
}

async fn verify_fetch_stream_stops_after_lease_expiry(
    harness: &Harness,
    client: &wpb::WorkerTransferServiceClient<SharedHttp2Connection>,
    attempt: Uuid,
    artifact: Uuid,
    payload_size: u64,
) -> anyhow::Result<()> {
    let mut stream = client
        .fetch_artifact_with_options(
            wpb::FetchArtifactRequest {
                attempt_id: attempt.to_string(),
                artifact_id: artifact.to_string(),
                ..Default::default()
            },
            CallOptions::default(),
        )
        .await
        .context("assigned worker could not fetch the input")?;
    let first = stream
        .message::<wpb::ArtifactChunk>()
        .await?
        .context("input stream ended before its first chunk")?
        .to_owned_message();
    anyhow::ensure!(
        first.data.len() == TRANSFER_CHUNK_BYTES && !first.last,
        "unexpected first chunk"
    );
    sqlx::query("UPDATE attempts SET lease_expires_at = now() - interval '1 second' WHERE tenant_id = $1 AND id = $2")
        .bind(harness.tenant1.id)
        .bind(attempt)
        .execute(harness.admin_pool())
        .await?;
    let mut received = first.data.len();
    let expiry_error = loop {
        match stream.message::<wpb::ArtifactChunk>().await {
            Ok(Some(chunk)) => {
                let chunk = chunk.to_owned_message();
                anyhow::ensure!(
                    chunk.offset == u64::try_from(received)?,
                    "input stream resumed from the wrong offset after lease expiry"
                );
                anyhow::ensure!(
                    !chunk.last,
                    "input stream completed after the database lease expired"
                );
                received += chunk.data.len();
            }
            Ok(None) => break None,
            Err(error) => break Some(error),
        }
    };
    anyhow::ensure!(
        expiry_error.is_some_and(|error| error.code == ErrorCode::FailedPrecondition),
        "input stream did not fail authorization after the database lease expired"
    );
    anyhow::ensure!(
        u64::try_from(received)? < payload_size,
        "input stream delivered the complete artifact after lease expiry"
    );
    Ok(())
}

async fn verify_fetch_stream_stops_after_attempt_settlement(
    harness: &Harness,
    client: &wpb::WorkerTransferServiceClient<SharedHttp2Connection>,
    attempt: Uuid,
    artifact: Uuid,
    payload_size: u64,
) -> anyhow::Result<()> {
    let mut stream = client
        .fetch_artifact_with_options(
            wpb::FetchArtifactRequest {
                attempt_id: attempt.to_string(),
                artifact_id: artifact.to_string(),
                ..Default::default()
            },
            CallOptions::default(),
        )
        .await
        .context("assigned worker could not start the second input stream")?;
    let first = stream
        .message::<wpb::ArtifactChunk>()
        .await?
        .context("second input stream ended before its first chunk")?
        .to_owned_message();
    anyhow::ensure!(
        first.data.len() == TRANSFER_CHUNK_BYTES && !first.last,
        "unexpected second first chunk"
    );
    sqlx::query("UPDATE attempts SET state = 'failed' WHERE tenant_id = $1 AND id = $2")
        .bind(harness.tenant1.id)
        .bind(attempt)
        .execute(harness.admin_pool())
        .await?;
    let mut received = first.data.len();
    let settlement_error = loop {
        match stream.message::<wpb::ArtifactChunk>().await {
            Ok(Some(chunk)) => {
                let chunk = chunk.to_owned_message();
                anyhow::ensure!(
                    chunk.offset == u64::try_from(received)?,
                    "input stream resumed from the wrong offset after settlement"
                );
                anyhow::ensure!(
                    !chunk.last,
                    "input stream completed after the attempt settled"
                );
                received += chunk.data.len();
            }
            Ok(None) => break None,
            Err(error) => break Some(error),
        }
    };
    anyhow::ensure!(
        settlement_error.is_some_and(|error| error.code == ErrorCode::FailedPrecondition),
        "input stream did not fail authorization after the attempt settled"
    );
    anyhow::ensure!(
        u64::try_from(received)? < payload_size,
        "input stream delivered the complete artifact after settlement"
    );
    Ok(())
}

#[test]
fn fetch_reauthorizes_every_chunk_after_expiry_settlement_and_worker_or_tenant_mismatch()
-> anyhow::Result<()> {
    let harness = harness_without_object_store();
    e2e_support::block_on(async {
        let uri: http::Uri = harness.url("").parse()?;
        let (owner_id, owner_credential) = e2e_support::synthetic::enroll(
            &uri,
            &harness.tenant1.master_key,
            &format!("artifact-fetch-owner-{}", Uuid::now_v7()),
        )
        .await?;
        let (wrong_worker_id, wrong_credential) = e2e_support::synthetic::enroll(
            &uri,
            &harness.tenant1.master_key,
            &format!("artifact-fetch-other-{}", Uuid::now_v7()),
        )
        .await?;
        let (foreign_id, foreign_credential) = e2e_support::synthetic::enroll(
            &uri,
            &harness.tenant2.master_key,
            &format!("artifact-fetch-foreign-{}", Uuid::now_v7()),
        )
        .await?;
        let owner_id: Uuid = owner_id.parse()?;
        let _wrong_worker_id: Uuid = wrong_worker_id.parse()?;
        let _foreign_id: Uuid = foreign_id.parse()?;
        let pool_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO device_pools \
             (tenant_id, id, worker_id, pool_key, accelerator, total_slots, claimed_slots) \
             VALUES ($1, $2, $3, 'artifact-fetch', 'metal', 1, 0)",
        )
        .bind(harness.tenant1.id)
        .bind(pool_id)
        .bind(owner_id)
        .execute(harness.admin_pool())
        .await?;

        let payload = vec![0x5a; TRANSFER_CHUNK_BYTES * 16 + 17];
        let payload_size = u64::try_from(payload.len())?;
        let (attempt, artifact) = insert_attempt_input(
            &harness,
            harness.tenant1.id,
            owner_id,
            pool_id,
            "fetch-expiry",
            &payload,
        )
        .await?;
        expect_fetch_denied(&uri, &wrong_credential, attempt, artifact).await?;
        expect_fetch_denied(&uri, &foreign_credential, attempt, artifact).await?;

        let client = transfer_client(&uri, &owner_credential).await?;
        verify_fetch_stream_stops_after_lease_expiry(
            &harness,
            &client,
            attempt,
            artifact,
            payload_size,
        )
        .await?;
        let (settled_attempt, settled_artifact) = insert_attempt_input(
            &harness,
            harness.tenant1.id,
            owner_id,
            pool_id,
            "fetch-settlement",
            &payload,
        )
        .await?;
        verify_fetch_stream_stops_after_attempt_settlement(
            &harness,
            &client,
            settled_attempt,
            settled_artifact,
            payload_size,
        )
        .await?;
        Ok(())
    })
}

type WorkerSession = connectrpc::client::BidiStream<
    hyper::body::Incoming,
    wpb::WorkerMessage,
    <wpb::RemoteMessage as buffa::HasMessageView>::View<'static>,
>;

async fn online_worker(
    harness: &Harness,
    name: &str,
) -> anyhow::Result<(Uuid, String, http::Uri, WorkerSession)> {
    let uri: http::Uri = harness.url("").parse()?;
    let (worker_id, credential) =
        e2e_support::synthetic::enroll(&uri, &harness.tenant1.master_key, name).await?;
    let connection = Http2Connection::connect_plaintext(uri.clone()).await?;
    let config = ClientConfig::new(uri.clone())
        .with_protocol(Protocol::Grpc)
        .with_default_header("authorization", format!("Bearer {credential}"));
    let client = wpb::WorkerSessionServiceClient::new(connection.shared(16), config);
    let mut session = client.session().await?;
    session
        .send(wpb::WorkerMessage {
            message: Some(
                wpb::Handshake {
                    protocol_major: gpq_proto::PROTOCOL_MAJOR,
                    protocol_minor: gpq_proto::PROTOCOL_MINOR,
                    worker_version: "artifact-safety-test".to_owned(),
                    host_descriptor: "artifact-safety-test".to_owned(),
                    ..Default::default()
                }
                .into(),
            ),
            ..Default::default()
        })
        .await?;
    let acknowledgement = session
        .message::<wpb::RemoteMessage>()
        .await?
        .context("worker session closed before handshake acknowledgement")?
        .to_owned_message();
    anyhow::ensure!(
        matches!(
            acknowledgement.message,
            Some(remote_message::Message::HandshakeAck(_))
        ),
        "worker session did not acknowledge the handshake"
    );
    e2e_support::wait_until(
        || async { harness.worker_is_online(name, true).await },
        Duration::from_secs(10),
    )
    .await
    .context("test worker was not registered online")?;
    Ok((worker_id.parse()?, credential, uri, session))
}

async fn next_deliver(session: &mut WorkerSession) -> anyhow::Result<wpb::DeliverRequest> {
    loop {
        let message = session
            .message::<wpb::RemoteMessage>()
            .await?
            .context("worker control session closed before delivery request")?
            .to_owned_message();
        if let Some(remote_message::Message::Deliver(request)) = message.message {
            return Ok(*request);
        }
    }
}

async fn insert_worker_output(
    harness: &Harness,
    tenant: Uuid,
    worker: Uuid,
    name: &str,
    manifest: &pb::ArtifactManifest,
) -> anyhow::Result<Uuid> {
    let generation = insert_generation(harness.admin_pool(), tenant, name).await?;
    let artifact = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO artifacts \
         (tenant_id, id, generation_id, direction, state, placement, size_bytes, \
          digest_sha256, kind, mime_type, worker_id, delivery_token, available_at, expires_at) \
         VALUES ($1, $2, $3, 'output', 'available', 'worker_local', $4, $5, \
                 'binary', 'application/octet-stream', $6, $7, now(), now() + interval '1 hour')",
    )
    .bind(tenant)
    .bind(artifact)
    .bind(generation)
    .bind(i64::try_from(manifest.size_bytes)?)
    .bind(&manifest.digest_sha256)
    .bind(worker)
    .bind(Uuid::now_v7().to_string())
    .execute(harness.admin_pool())
    .await?;
    Ok(artifact)
}

async fn send_delivery_chunk(
    client: &wpb::WorkerTransferServiceClient<SharedHttp2Connection>,
    request: &wpb::DeliverRequest,
    manifest: &pb::ArtifactManifest,
    offset: u64,
    data: Vec<u8>,
    last: bool,
) -> Result<wpb::DeliverArtifactResponse, connectrpc::ConnectError> {
    let requests = vec![
        wpb::DeliverArtifactRequest {
            message: Some(
                wpb::DeliverArtifactStart {
                    artifact_id: request.artifact_id.clone(),
                    delivery_token: request.delivery_token.clone(),
                    manifest: manifest.clone().into(),
                    offset,
                    ..Default::default()
                }
                .into(),
            ),
            ..Default::default()
        },
        wpb::DeliverArtifactRequest {
            message: Some(
                wpb::ArtifactChunk {
                    offset,
                    data,
                    last,
                    ..Default::default()
                }
                .into(),
            ),
            ..Default::default()
        },
    ];
    client
        .deliver_artifact(connectrpc::stream_iter(requests))
        .await
        .map(connectrpc::UnaryResponse::into_owned)
}

async fn assert_artifact_state(
    harness: &Harness,
    artifact: Uuid,
    expected: &str,
) -> anyhow::Result<()> {
    let state: String =
        sqlx::query_scalar("SELECT state FROM artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(harness.tenant1.id)
            .bind(artifact)
            .fetch_one(harness.admin_pool())
            .await?;
    anyhow::ensure!(
        state == expected,
        "artifact state was {state}, expected {expected}"
    );
    Ok(())
}

async fn verify_resumed_output_rejects_corrupt_prefix(
    harness: &Harness,
    worker: Uuid,
    client: &wpb::WorkerTransferServiceClient<SharedHttp2Connection>,
    session: &mut WorkerSession,
    manifest: &pb::ArtifactManifest,
) -> anyhow::Result<()> {
    let expected = b"abc";
    let wrong_prefix_artifact = insert_worker_output(
        harness,
        harness.tenant1.id,
        worker,
        "wrong-resume-prefix",
        manifest,
    )
    .await?;
    let download_url = harness.url(&format!("/v1/artifacts/{wrong_prefix_artifact}"));
    let response = harness
        .http
        .get(download_url.as_str())
        .bearer_auth(&harness.tenant1.master_key)
        .send()
        .await?;
    anyhow::ensure!(
        response.status().as_u16() == 200 && response.url().as_str() == download_url.as_str(),
        "worker-local download was not HTTP 200 at its requested origin/path: {} {}",
        response.status(),
        response.url()
    );
    let first_request = next_deliver(session).await?;
    let first_rpc =
        send_delivery_chunk(client, &first_request, manifest, 0, b"x".to_vec(), false).await;
    anyhow::ensure!(
        first_rpc.is_err(),
        "incomplete first delivery stream was not interrupted"
    );
    let resumed = next_deliver(session).await?;
    anyhow::ensure!(
        resumed.offset == 1,
        "internal resume did not continue at the committed byte"
    );
    let resumed_rpc =
        send_delivery_chunk(client, &resumed, manifest, 1, b"bc".to_vec(), true).await?;
    anyhow::ensure!(resumed_rpc.accepted, "resumed transfer was not received");
    let delivered = response.bytes().await;
    anyhow::ensure!(
        !matches!(delivered, Ok(bytes) if bytes.len() == expected.len()),
        "wrong prefix followed by a valid suffix completed the declared HTTP body"
    );
    assert_artifact_state(harness, wrong_prefix_artifact, "lost").await
}

async fn verify_oversized_output_chunk_is_not_forwarded(
    harness: &Harness,
    worker: Uuid,
    client: &wpb::WorkerTransferServiceClient<SharedHttp2Connection>,
    session: &mut WorkerSession,
    manifest: &pb::ArtifactManifest,
) -> anyhow::Result<()> {
    let oversized_artifact = insert_worker_output(
        harness,
        harness.tenant1.id,
        worker,
        "oversized-delivery",
        manifest,
    )
    .await?;
    let download_url = harness.url(&format!("/v1/artifacts/{oversized_artifact}"));
    let response = harness
        .http
        .get(download_url.as_str())
        .bearer_auth(&harness.tenant1.master_key)
        .send()
        .await?;
    anyhow::ensure!(
        response.status().as_u16() == 200 && response.url().as_str() == download_url.as_str(),
        "oversized-chunk download was not HTTP 200 at its requested origin/path: {} {}",
        response.status(),
        response.url()
    );
    let mut body = response.bytes_stream();
    let request = next_deliver(session).await?;
    let oversized = vec![0x77; TRANSFER_CHUNK_BYTES + 1];
    let oversized_rpc = send_delivery_chunk(client, &request, manifest, 0, oversized, true).await;
    anyhow::ensure!(
        oversized_rpc.is_err(),
        "oversized chunk was accepted by the transfer RPC"
    );
    let first_body_item = body.next().await;
    anyhow::ensure!(
        !matches!(first_body_item, Some(Ok(bytes)) if !bytes.is_empty()),
        "oversized chunk bytes were forwarded to the downloading client"
    );
    assert_artifact_state(harness, oversized_artifact, "lost").await
}

async fn verify_empty_output_rejects_incorrect_digest(
    harness: &Harness,
    worker: Uuid,
    client: &wpb::WorkerTransferServiceClient<SharedHttp2Connection>,
    session: &mut WorkerSession,
) -> anyhow::Result<()> {
    let mut empty_manifest = manifest_for(&[]);
    empty_manifest.digest_sha256 = ContentHash::digest(b"not-empty").to_hex();
    let empty_artifact = insert_worker_output(
        harness,
        harness.tenant1.id,
        worker,
        "empty-invalid-digest",
        &empty_manifest,
    )
    .await?;
    let download_url = harness.url(&format!("/v1/artifacts/{empty_artifact}"));
    let response_url = download_url.clone();
    let http = harness.http.clone();
    let master_key = harness.tenant1.master_key.clone();
    let download =
        tokio::spawn(async move { http.get(download_url).bearer_auth(master_key).send().await });
    let request = next_deliver(session).await?;
    let empty_rpc =
        send_delivery_chunk(client, &request, &empty_manifest, 0, Vec::new(), true).await?;
    anyhow::ensure!(
        !empty_rpc.accepted,
        "empty artifact with an incorrect manifest digest was accepted"
    );
    let response = download.await??;
    anyhow::ensure!(
        response.status().as_u16() == 502 && response.url().as_str() == response_url.as_str(),
        "empty artifact with an incorrect digest was not rejected at its requested URL: {} {}",
        response.status(),
        response.url()
    );
    assert_artifact_state(harness, empty_artifact, "lost").await
}

#[test]
fn worker_local_resume_hashes_all_chunks_and_rejects_oversized_chunks_before_forwarding()
-> anyhow::Result<()> {
    let harness = harness_without_object_store();
    e2e_support::block_on(async {
        let name = format!("artifact-delivery-{}", Uuid::now_v7());
        let (worker, credential, uri, mut session) = online_worker(&harness, &name).await?;
        let client = transfer_client(&uri, &credential).await?;
        let manifest = manifest_for(b"abc");
        verify_resumed_output_rejects_corrupt_prefix(
            &harness,
            worker,
            &client,
            &mut session,
            &manifest,
        )
        .await?;
        verify_oversized_output_chunk_is_not_forwarded(
            &harness,
            worker,
            &client,
            &mut session,
            &manifest,
        )
        .await?;
        verify_empty_output_rejects_incorrect_digest(&harness, worker, &client, &mut session)
            .await?;
        Ok(())
    })
}
