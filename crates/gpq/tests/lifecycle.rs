//! Worker-lifecycle end-to-end tests: at-least-once execution across Worker
//! loss, heartbeat-extended leases, and result-commitment races (ADR 0003),
//! plus Worker Credential and control-protocol guards (ADR 0004, ADR 0009).
//!
//! Compiles its own copy of `e2e_support` into this binary, so the suite has an
//! independent PostgreSQL container, Remote, and real `gpq worker` pair.
//! Per-test access guards keep shared services alive through each case;
//! teardown waits for active cases and later access builds a fresh harness.
//!
//! Each test's doc comment names the ADR invariant it defends.

#[expect(
    dead_code,
    reason = "shared harness compiled into every integration-test binary; each suite \
              uses the subset it needs"
)]
mod e2e_support;

use std::time::Duration;

use anyhow::Context;
use connectrpc::ErrorCode;
use e2e_support::{Harness, SyntheticWorker, harness, image_request, wait_until};
use gpq_domain::ModelFamily;
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use uuid::Uuid;

/// Receives the next lease for `worker`, asserts it is for `generation_id`, and
/// reports the Attempt as running.
async fn lease_and_start(
    worker: &mut SyntheticWorker,
    generation_id: &str,
) -> anyhow::Result<wpb::LeaseAssignment> {
    let lease = worker
        .recv_lease(Duration::from_secs(30))
        .await
        .context("the synthetic worker never received a LeaseAssignment")?;
    anyhow::ensure!(
        lease.generation_id == generation_id,
        "leased generation {} != submitted {generation_id}",
        lease.generation_id
    );
    worker.start_attempt(&lease.attempt_id).await?;
    Ok(lease)
}

/// Waits until the Attempt numbered `attempt_number` of `generation` reports
/// `lease_expired`.
async fn wait_for_lease_expiry(
    h: &Harness,
    generation: Uuid,
    attempt_number: i32,
    timeout: Duration,
) -> anyhow::Result<e2e_support::AttemptRow> {
    wait_until(
        || async {
            let attempts = h.attempt_rows(h.tenant1.id, generation).await?;
            Ok(attempts.into_iter().find(|attempt| {
                attempt.attempt_number == attempt_number && attempt.state == "lease_expired"
            }))
        },
        timeout,
    )
    .await
    .context("the attempt never lease-expired")
}

/// Killing a Worker mid-Attempt lets its lease lapse without a renewing
/// heartbeat; the expiry sweep marks the Attempt `lease_expired` and returns
/// the Generation to `queued`, and the same Worker, reconnecting with its
/// unchanged Worker Credential, leases a second Attempt that succeeds. This is
/// ADR 0003's at-least-once execution across Worker loss.
#[test]
fn worker_loss_lease_expiry_and_retry_succeeds_on_reconnect() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "lifecycle-loss", ModelFamily::QwenImage21).await?;
        let generation = h.submit(image_request(&worker.alias, "slow")).await?;
        let generation_id = generation.generation_id.clone();
        let generation_uuid = Uuid::parse_str(&generation_id)?;

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        worker.disconnect();

        let expired =
            wait_for_lease_expiry(&h, generation_uuid, 1, Duration::from_secs(75)).await?;
        anyhow::ensure!(
            expired.failure_kind.as_deref() == Some("lease_expired"),
            "failure_kind: {:?}",
            expired.failure_kind
        );

        worker
            .reconnect(&h)
            .await
            .context("reconnecting the worker with its original credential")?;
        let second = lease_and_start(&mut worker, &generation_id).await?;
        anyhow::ensure!(
            second.attempt_id != lease.attempt_id,
            "the same attempt was re-leased"
        );
        worker
            .complete_attempt(
                &second.attempt_id,
                b"recovered bytes",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;

        let succeeded = wait_until(
            || async {
                let row = h.generation_row(h.tenant1.id, generation_uuid).await?;
                Ok(row.filter(|row| row.state == "succeeded"))
            },
            Duration::from_secs(40),
        )
        .await
        .context("the generation never succeeded after the worker reconnected")?;
        anyhow::ensure!(
            succeeded.attempt_count == 2,
            "attempt_count: {}",
            succeeded.attempt_count
        );

        let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
        anyhow::ensure!(attempts.len() == 2, "attempts: {attempts:?}");
        anyhow::ensure!(
            attempts[0].state == "lease_expired",
            "attempt 1 state: {}",
            attempts[0].state
        );
        anyhow::ensure!(
            attempts[1].state == "succeeded",
            "attempt 2 state: {}",
            attempts[1].state
        );
        Ok(())
    })
}

/// An Attempt whose execution exceeds the 45-second lease TTL stays alive
/// because the Worker heartbeats (ADR 0003): `lease_expires_at` advances past
/// its initially leased value, the Attempt never lapses into `lease_expired`,
/// and the Generation still succeeds with exactly one Attempt.
#[test]
fn heartbeat_renews_lease_past_the_ttl_without_expiry() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "lifecycle-heartbeat", ModelFamily::QwenImage21).await?;
        let generation = h
            .submit(image_request(&worker.alias, "slow but alive"))
            .await?;
        let generation_id = generation.generation_id.clone();
        let generation_uuid = Uuid::parse_str(&generation_id)?;

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        let first_expiry = wait_until(
            || async {
                let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
                Ok(attempts.first().map(|attempt| attempt.lease_expires_at))
            },
            Duration::from_secs(30),
        )
        .await
        .context("the attempt was never leased")?;

        // Heartbeat every 10 seconds for longer than one TTL, then check that
        // the lease moved forward and never lapsed.
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_secs(10)).await;
            worker
                .heartbeat(std::slice::from_ref(&lease.attempt_id))
                .await?;
        }
        let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
        let attempt = attempts.first().context("the attempt disappeared")?;
        anyhow::ensure!(
            attempt.state != "lease_expired",
            "lease expired despite active heartbeats"
        );
        anyhow::ensure!(
            attempt.lease_expires_at > first_expiry,
            "lease_expires_at never advanced past {first_expiry}"
        );

        worker
            .complete_attempt(
                &lease.attempt_id,
                b"slow but alive bytes",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        let succeeded = wait_until(
            || async {
                let row = h.generation_row(h.tenant1.id, generation_uuid).await?;
                Ok(row.filter(|row| row.state == "succeeded"))
            },
            Duration::from_secs(60),
        )
        .await
        .context("the generation never succeeded despite a heartbeat-extended lease")?;
        anyhow::ensure!(
            succeeded.attempt_count == 1,
            "attempt_count: {}",
            succeeded.attempt_count
        );
        Ok(())
    })
}

/// After a Generation is cancelled, a late `AttemptResult` racing the
/// cancellation is rejected by Remote's terminal compare-and-set, and the
/// Worker is told to discard its output over the same control Session (ADR
/// 0003): the Generation and its Attempt stay `cancelled`. This also proves
/// cancel propagation: `CancelGeneration` reaches the Worker as a
/// `CancelRequest` and the Attempt settles `cancelled` on acknowledgement.
#[test]
fn late_result_after_cancellation_is_rejected_and_discarded() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "lifecycle-race", ModelFamily::QwenImage21).await?;
        let generation = h.submit(image_request(&worker.alias, "cancel me")).await?;
        let generation_id = generation.generation_id.clone();
        let generation_uuid = Uuid::parse_str(&generation_id)?;

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        wait_until(
            || async {
                let row = h.generation_row(h.tenant1.id, generation_uuid).await?;
                Ok(row.filter(|row| row.state == "running").map(|_| ()))
            },
            Duration::from_secs(30),
        )
        .await
        .context("the generation never reached running before cancelling it")?;

        h.generation_client(&h.tenant1.master_key)
            .cancel_generation(pb::CancelGenerationRequest {
                generation_id: generation_id.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("CancelGeneration failed: {err}"))?;

        let cancel = worker
            .recv_cancel_request(Duration::from_secs(30))
            .await
            .context("the synthetic worker never received the CancelRequest")?;
        anyhow::ensure!(
            cancel.attempt_id == lease.attempt_id,
            "cancel named another attempt"
        );
        worker.acknowledge_cancel(&lease.attempt_id).await?;

        let cancelled = wait_until(
            || async {
                let row = h.generation_row(h.tenant1.id, generation_uuid).await?;
                Ok(row.filter(|row| row.state == "cancelled"))
            },
            Duration::from_secs(30),
        )
        .await
        .context("the generation never reached cancelled from CancelAcknowledged")?;
        anyhow::ensure!(cancelled.attempt_count == 1);

        let token = worker
            .complete_attempt(
                &lease.attempt_id,
                b"late success that must never land",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        let discard = worker
            .recv_discard(Duration::from_secs(30), &token)
            .await
            .context("Remote never told the synthetic worker to discard its late output")?;
        anyhow::ensure!(discard.delivery_token == token);

        let after = h
            .generation_row(h.tenant1.id, generation_uuid)
            .await?
            .context("the generation disappeared")?;
        anyhow::ensure!(
            after.state == "cancelled",
            "state flipped to {}",
            after.state
        );
        anyhow::ensure!(
            after.attempt_count == 1,
            "attempt_count: {}",
            after.attempt_count
        );

        let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
        anyhow::ensure!(attempts.len() == 1, "attempts: {attempts:?}");
        anyhow::ensure!(
            attempts[0].state == "cancelled",
            "attempt: {:?}",
            attempts[0]
        );
        Ok(())
    })
}

/// A stale `AttemptResult` for an Attempt whose lease already lapsed is
/// rejected the same way (ADR 0003: "results committed under an expired lease
/// are rejected"). The synthetic Worker never heartbeats, so the lease lapses
/// on its own, and Remote still tells it to discard the late output over the
/// control Session even though the Generation itself only requeued.
#[test]
fn stale_result_after_lease_expiry_is_rejected_and_discarded() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "lifecycle-expiry", ModelFamily::QwenImage21).await?;
        let generation = h
            .submit(image_request(&worker.alias, "never heartbeats"))
            .await?;
        let generation_id = generation.generation_id.clone();
        let generation_uuid = Uuid::parse_str(&generation_id)?;

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        let expired =
            wait_for_lease_expiry(&h, generation_uuid, 1, Duration::from_secs(75)).await?;
        anyhow::ensure!(
            expired.failure_kind.as_deref() == Some("lease_expired"),
            "failure_kind: {:?}",
            expired.failure_kind
        );

        let token = worker
            .complete_attempt(
                &lease.attempt_id,
                b"stale success under an expired lease",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        let discard = worker
            .recv_discard(Duration::from_secs(30), &token)
            .await
            .context("Remote never discarded the stale result committed under an expired lease")?;
        anyhow::ensure!(discard.delivery_token == token);

        let first_attempt = h
            .attempt_rows(h.tenant1.id, generation_uuid)
            .await?
            .into_iter()
            .find(|attempt| attempt.attempt_number == 1)
            .context("the first attempt disappeared")?;
        anyhow::ensure!(
            first_attempt.state == "lease_expired",
            "attempt: {first_attempt:?}"
        );
        Ok(())
    })
}

/// A `Handshake` reporting an incompatible protocol major is rejected
/// explicitly with `FailedPrecondition` (ADR 0004).
#[test]
fn session_handshake_rejects_incompatible_protocol_major() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let err = SyntheticWorker::spawn_with_protocol_major(
            &h,
            "lifecycle-protocol-mismatch",
            gpq_proto::PROTOCOL_MAJOR + 1,
        )
        .await?;
        anyhow::ensure!(
            err.code == ErrorCode::FailedPrecondition,
            "unexpected error code: {:?}",
            err.code
        );
        Ok(())
    })
}

/// Revoking a Worker Credential (ADR 0009) stops that Worker from
/// re-establishing its control Session, and a Generation aimed at the Model it
/// alone served stays `queued` because no online Worker can run it (ADR 0004,
/// ADR 0009). The real `gpq worker` process is revoked the same way: after a
/// restart it never comes `online`. It gets an isolated harness so parallel
/// cases retain their own real Worker.
#[test]
fn revoked_worker_credential_blocks_reconnection_and_scheduling() -> anyhow::Result<()> {
    let h = e2e_support::block_on(Harness::build_with(e2e_support::HarnessOptions::default()))?;
    let result = e2e_support::block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "lifecycle-revoked", ModelFamily::QwenImage21).await?;
        let worker_uuid: Uuid = worker.worker_id.parse()?;
        h.revoke_worker(worker_uuid)
            .await
            .context("revoking the synthetic worker's credential")?;
        worker.disconnect();

        let reconnect = worker.reconnect(&h).await;
        anyhow::ensure!(reconnect.is_err(), "a revoked credential reconnected");

        let generation = h
            .submit(image_request(&worker.alias, "should stay queued"))
            .await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let state = h.get_generation(&generation.generation_id).await?.state;
        anyhow::ensure!(
            state == pb::GenerationState::GENERATION_STATE_QUEUED,
            "a generation was scheduled onto a revoked worker: {state:?}"
        );

        // The real `gpq worker` process: revoke, restart, and watch it never
        // come back online.
        let real_name = h.worker_name().to_owned();
        h.revoke_worker_credential()
            .await
            .context("revoking the real worker's credential")?;
        anyhow::ensure!(
            h.kill_worker().await?,
            "expected a running gpq worker to kill"
        );
        h.restart_worker()
            .context("restarting the worker after revoking its credential")?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            anyhow::ensure!(
                h.worker_is_online(&real_name, true).await?.is_none(),
                "the revoked worker reconnected and went online"
            );
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    });
    let teardown = e2e_support::block_on(h.teardown());
    result?;
    teardown
}

/// Requests teardown; active tests retain their harness until they finish.
#[test]
fn harness_teardown_waits_for_active_uses() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(e2e_support::teardown_harness(h))
}
