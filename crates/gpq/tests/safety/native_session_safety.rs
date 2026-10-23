//! Worker-session regressions for attempt ownership, lease expiry, credential
//! revocation, cancellation acknowledgements, and accepted local output delivery.

use std::time::Duration;

use super::e2e_support;
use super::e2e_support::{Harness, SyntheticWorker, harness, image_request, wait_until};
use anyhow::Context;
use gpq_domain::{ContentHash, ModelFamily};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use uuid::Uuid;

async fn lease(
    worker: &mut SyntheticWorker,
    generation_id: &str,
    start: bool,
) -> anyhow::Result<wpb::LeaseAssignment> {
    let assignment = worker
        .recv_lease(Duration::from_secs(30))
        .await
        .context("synthetic worker did not receive its lease")?;
    anyhow::ensure!(assignment.generation_id == generation_id);
    if start {
        worker.start_attempt(&assignment.attempt_id).await?;
    }
    Ok(assignment)
}

async fn wait_for_generation(
    h: &Harness,
    generation_id: &str,
    expected: pb::GenerationState,
) -> anyhow::Result<pb::Generation> {
    wait_until(
        || async {
            let generation = h.get_generation(generation_id).await?;
            Ok((generation.state == expected).then_some(generation))
        },
        Duration::from_secs(15),
    )
    .await
}

fn worker_local_result(
    attempt_id: &str,
    token: &str,
    bytes: &[u8],
) -> anyhow::Result<wpb::AttemptResult> {
    Ok(wpb::AttemptResult {
        attempt_id: attempt_id.to_owned(),
        outputs: vec![wpb::AttemptOutput {
            manifest: pb::ArtifactManifest {
                size_bytes: u64::try_from(bytes.len())
                    .context("worker-local test artifact exceeds u64 size")?,
                digest_sha256: ContentHash::digest(bytes).to_hex(),
                kind: pb::MediaKind::MEDIA_KIND_IMAGE.into(),
                mime_type: "image/png".to_owned(),
                ..Default::default()
            }
            .into(),
            placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
            delivery_token: token.to_owned(),
            ..Default::default()
        }],
        ..Default::default()
    })
}

async fn pool_free_slots(h: &Harness, worker_id: Uuid) -> anyhow::Result<i32> {
    Ok(sqlx::query_scalar(
        "SELECT free_slots FROM device_pools WHERE tenant_id = $1 AND worker_id = $2",
    )
    .bind(h.tenant1.id)
    .bind(worker_id)
    .fetch_one(h.admin_pool())
    .await?)
}

/// A second enrolled worker cannot change another worker's attempt by naming
/// its UUID, including through oversized-result failure handling.
#[test]
fn foreign_worker_controls_and_oversized_result_leave_the_owned_attempt_unchanged()
-> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut owner =
            SyntheticWorker::spawn(&h, "native-session-owner", ModelFamily::QwenImage21).await?;
        let mut foreign =
            SyntheticWorker::spawn(&h, "native-session-foreign", ModelFamily::QwenImage21).await?;
        let generation = h
            .submit(image_request(&owner.alias, "owner-only attempt"))
            .await?;
        let generation_id = generation.generation_id.clone();
        let generation_uuid = Uuid::parse_str(&generation_id)?;
        let assignment = lease(&mut owner, &generation_id, false).await?;
        let attempt_id = assignment.attempt_id.clone();

        foreign.start_attempt(&attempt_id).await?;
        foreign
            .fail_attempt(&attempt_id, pb::FailureKind::FAILURE_KIND_INVALID_INPUT)
            .await?;
        foreign
            .send(wpb::LeaseRejected {
                attempt_id: attempt_id.clone(),
                failure: pb::Failure {
                    kind: pb::FailureKind::FAILURE_KIND_UNSUPPORTED_CAPABILITY.into(),
                    message: "foreign rejection".to_owned(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            })
            .await?;
        foreign.acknowledge_cancel(&attempt_id).await?;
        foreign
            .send(wpb::AttemptResult {
                attempt_id: attempt_id.clone(),
                outputs: vec![wpb::AttemptOutput {
                    manifest: pb::ArtifactManifest {
                        size_bytes: u64::MAX,
                        digest_sha256: ContentHash::digest(b"oversized").to_hex(),
                        kind: pb::MediaKind::MEDIA_KIND_IMAGE.into(),
                        mime_type: "image/png".to_owned(),
                        ..Default::default()
                    }
                    .into(),
                    placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
                    delivery_token: "foreign-oversized-output".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        foreign.heartbeat(std::slice::from_ref(&attempt_id)).await?;
        let cancellation = foreign
            .recv_cancel_request(Duration::from_secs(10))
            .await
            .context("foreign heartbeat did not complete the preceding control messages")?;
        anyhow::ensure!(cancellation.attempt_id == attempt_id);

        let generation_row = h
            .generation_row(h.tenant1.id, generation_uuid)
            .await?
            .context("generation disappeared")?;
        let attempt_row = h
            .attempt_rows(h.tenant1.id, generation_uuid)
            .await?
            .into_iter()
            .next()
            .context("attempt disappeared")?;
        anyhow::ensure!(generation_row.state == "running");
        anyhow::ensure!(generation_row.attempt_count == 1);
        anyhow::ensure!(attempt_row.state == "leased");
        anyhow::ensure!(pool_free_slots(&h, Uuid::parse_str(&owner.worker_id)?).await? == 0);
        Ok(())
    })
}

/// Heartbeats cannot revive an expired timestamp while the expiry sweep has
/// not yet settled the attempt.
#[test]
fn expired_heartbeat_is_not_renewed() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "native-session-expired", ModelFamily::QwenImage21).await?;
        let generation = h
            .submit(image_request(&worker.alias, "expired heartbeat"))
            .await?;
        let generation_uuid = Uuid::parse_str(&generation.generation_id)?;
        let assignment = lease(&mut worker, &generation.generation_id, true).await?;
        let attempt_uuid = Uuid::parse_str(&assignment.attempt_id)?;
        let before = wait_until(
            || async {
                let attempt = h
                    .attempt_rows(h.tenant1.id, generation_uuid)
                    .await?
                    .into_iter()
                    .next()
                    .context("attempt disappeared")?;
                Ok((attempt.state == "running").then_some(attempt))
            },
            Duration::from_secs(15),
        )
        .await?;
        anyhow::ensure!(before.state == "running");
        let (expired_at,): (chrono::DateTime<chrono::Utc>,) = sqlx::query_as(
            "UPDATE attempts SET lease_expires_at = now() - interval '10 milliseconds' \
             WHERE tenant_id = $1 AND id = $2 RETURNING lease_expires_at",
        )
        .bind(h.tenant1.id)
        .bind(attempt_uuid)
        .fetch_one(h.admin_pool())
        .await?;

        worker
            .heartbeat(std::slice::from_ref(&assignment.attempt_id))
            .await?;
        worker
            .recv_cancel_request(Duration::from_secs(10))
            .await
            .context("expired attempt was not reported as unrenewed")?;
        let (db_now,): (chrono::DateTime<chrono::Utc>,) = sqlx::query_as("SELECT now()")
            .fetch_one(h.admin_pool())
            .await?;
        let attempt = h
            .attempt_rows(h.tenant1.id, generation_uuid)
            .await?
            .into_iter()
            .next()
            .context("attempt disappeared")?;
        anyhow::ensure!(attempt.lease_expires_at == expired_at);
        anyhow::ensure!(attempt.lease_expires_at <= db_now);
        Ok(())
    })
}

/// An unsolicited acknowledgement is an atomic no-op unless the Generation is
/// already `cancelling`.
#[test]
fn unsolicited_cancel_ack_preserves_running_generation_and_slot() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker = SyntheticWorker::spawn(
            &h,
            "native-session-unsolicited-cancel",
            ModelFamily::QwenImage21,
        )
        .await?;
        let generation = h
            .submit(image_request(&worker.alias, "unsolicited cancel"))
            .await?;
        let generation_uuid = Uuid::parse_str(&generation.generation_id)?;
        let assignment = lease(&mut worker, &generation.generation_id, false).await?;

        worker.acknowledge_cancel(&assignment.attempt_id).await?;
        worker
            .heartbeat(std::slice::from_ref(&assignment.attempt_id))
            .await?;
        anyhow::ensure!(
            worker
                .recv_cancel_request(Duration::from_millis(500))
                .await
                .is_err(),
            "unsolicited acknowledgement released the live lease"
        );

        let generation_row = h
            .generation_row(h.tenant1.id, generation_uuid)
            .await?
            .context("generation disappeared")?;
        let attempt_row = h
            .attempt_rows(h.tenant1.id, generation_uuid)
            .await?
            .into_iter()
            .next()
            .context("attempt disappeared")?;
        anyhow::ensure!(generation_row.state == "running");
        anyhow::ensure!(attempt_row.state == "leased");
        anyhow::ensure!(pool_free_slots(&h, Uuid::parse_str(&worker.worker_id)?).await? == 0);
        Ok(())
    })
}

/// Revoking a credential ends future authority on an established session even
/// when the worker sends its result before its next heartbeat.
#[test]
fn revoked_worker_cannot_commit_result_without_a_prior_heartbeat() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker = SyntheticWorker::spawn(
            &h,
            "native-session-revoked-result",
            ModelFamily::QwenImage21,
        )
        .await?;
        let generation = h
            .submit(image_request(&worker.alias, "revoked result"))
            .await?;
        let generation_uuid = Uuid::parse_str(&generation.generation_id)?;
        let assignment = lease(&mut worker, &generation.generation_id, true).await?;
        h.revoke_worker(Uuid::parse_str(&worker.worker_id)?).await?;

        worker
            .send(worker_local_result(
                &assignment.attempt_id,
                "revoked-result-token",
                b"must not be accepted",
            )?)
            .await?;
        // This is deliberately after the result. Stream ordering makes the
        // session close a barrier proving the preceding result was processed.
        worker
            .heartbeat(std::slice::from_ref(&assignment.attempt_id))
            .await?;
        anyhow::ensure!(
            worker
                .recv_cancel_request(Duration::from_secs(5))
                .await
                .is_err(),
            "revoked worker session remained open"
        );

        let generation_row = h
            .generation_row(h.tenant1.id, generation_uuid)
            .await?
            .context("generation disappeared")?;
        let attempt_row = h
            .attempt_rows(h.tenant1.id, generation_uuid)
            .await?
            .into_iter()
            .next()
            .context("attempt disappeared")?;
        let outputs: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM artifacts WHERE tenant_id = $1 AND generation_id = $2 \
             AND direction = 'output'",
        )
        .bind(h.tenant1.id)
        .bind(generation_uuid)
        .fetch_one(h.admin_pool())
        .await?;
        anyhow::ensure!(generation_row.state == "running");
        anyhow::ensure!(attempt_row.state == "running");
        anyhow::ensure!(outputs == 0);
        Ok(())
    })
}

/// A replay of the accepted result preserves its worker-local delivery token,
/// so the accepted bytes remain downloadable.
#[test]
fn duplicate_accepted_result_preserves_worker_local_downloadability() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker = SyntheticWorker::spawn(
            &h,
            "native-session-duplicate-result",
            ModelFamily::QwenImage21,
        )
        .await?;
        let generation = h
            .submit(image_request(&worker.alias, "duplicate result"))
            .await?;
        let assignment = lease(&mut worker, &generation.generation_id, true).await?;
        let bytes = b"accepted worker-local image bytes";
        let token = "accepted-output-delivery-token";
        let result = worker_local_result(&assignment.attempt_id, token, bytes)?;
        worker
            .send(worker_local_result(&assignment.attempt_id, token, bytes)?)
            .await?;
        let succeeded = wait_for_generation(
            &h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
        )
        .await?;
        anyhow::ensure!(succeeded.output_artifacts.len() == 1);
        worker.send(result).await?;
        anyhow::ensure!(
            worker
                .recv_discard(Duration::from_secs(2), token)
                .await
                .is_err(),
            "duplicate result discarded the accepted delivery token"
        );

        let download_path = succeeded.output_artifacts[0].download_path.clone();
        let download_url = url::Url::parse(&download_path)?;
        let service_url = url::Url::parse(&h.url("/"))?;
        let expected_path = format!(
            "/v1/artifacts/{}",
            succeeded.output_artifacts[0].artifact_id
        );
        anyhow::ensure!(
            download_url.origin() == service_url.origin()
                && download_url.path() == expected_path
                && download_url.query().is_none()
                && download_url.fragment().is_none(),
            "download URL does not identify this Remote's artifact endpoint: {download_path}"
        );
        let http = h.http.clone();
        let key = h.tenant1.master_key.clone();
        let mut download = tokio::spawn(async move {
            let response = http.get(download_url).bearer_auth(key).send().await?;
            let status = response.status();
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, body))
        });
        let request = tokio::select! {
            request = worker.recv_deliver(Duration::from_secs(15), token) => {
                request.context("accepted worker-local output was not requested")?
            }
            result = &mut download => {
                let (status, _) = result??;
                anyhow::bail!("artifact download completed before worker delivery request (HTTP {status})");
            }
        };
        let accepted = worker
            .deliver_output(
                &request,
                bytes,
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        anyhow::ensure!(
            accepted.accepted,
            "output delivery rejected: {}",
            accepted.reason
        );
        let (status, body) = download.await??;
        anyhow::ensure!(
            status == reqwest::StatusCode::OK,
            "artifact download returned HTTP {status}"
        );
        anyhow::ensure!(body.as_ref() == bytes);
        Ok(())
    })
}
