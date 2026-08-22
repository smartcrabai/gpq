//! Worker-lifecycle end-to-end tests: at-least-once execution across Worker
//! loss, heartbeat-extended leases, and result-commitment races (ADR 0003),
//! plus Worker Credential and control-protocol guards (ADR 0004, ADR 0009).
//!
//! Compiles its own copy of `e2e_support` into this binary (`mod
//! e2e_support` below), so this suite gets its own `PostgreSQL` container
//! and its own Remote/Worker pair, independent of `e2e.rs`. Torn down by
//! `zzz_teardown_harness`, named to sort last so
//! `cargo test -p gpq-remote --test lifecycle -- --test-threads=1` runs it
//! after every other test regardless of pass/fail — required here because
//! every test shares the one lazily-built `Harness` and several mutate its
//! single real Worker.
//!
//! Each test's doc comment names the ADR invariant it defends.

#[expect(
    dead_code,
    reason = "shared harness compiled into every integration-test binary; each suite \
              uses the subset it needs"
)]
mod e2e_support;
mod lifecycle_support;

use std::time::Duration;

use anyhow::Context;
use e2e_support::fake_llama::FakeMode;
use e2e_support::{Harness, harness, wait_until};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use lifecycle_support::{SyntheticLlmWorker, SyntheticWorker};

/// Killing the Worker mid-Attempt lets its lease lapse without a renewing
/// heartbeat; the expiry sweep marks the Attempt `lease_expired` and
/// returns the Generation to `queued`, and a Worker restarted with the same
/// config and credential directory leases a second Attempt that succeeds —
/// ADR 0003's at-least-once execution across Worker loss.
#[test]
fn worker_loss_lease_expiry_and_retry_succeeds_on_restart() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake
            .set_mode(FakeMode::reply("slow reply").with_delay(Duration::from_secs(5)));

        let generation = h.native_submit_model(&h.model_alias).await?;
        let generation_uuid = uuid::Uuid::parse_str(&generation.generation_id)?;

        wait_until(
            || async {
                let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
                Ok((!attempts.is_empty()).then_some(()))
            },
            Duration::from_secs(30),
        )
        .await
        .context("the first attempt was never leased")?;

        anyhow::ensure!(
            h.kill_worker().await?,
            "expected a running gpq-worker to kill"
        );

        let expired = wait_until(
            || async {
                let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
                Ok(attempts.into_iter().find(|attempt| {
                    attempt.attempt_number == 1 && attempt.state == "lease_expired"
                }))
            },
            Duration::from_secs(75),
        )
        .await
        .context("the first attempt never lease-expired after worker loss")?;
        anyhow::ensure!(
            expired.failure_kind.as_deref() == Some("lease_expired"),
            "failure_kind: {:?}",
            expired.failure_kind
        );

        wait_until(
            || async {
                let row = h.generation_row(h.tenant1.id, generation_uuid).await?;
                Ok(row.filter(|row| row.state == "queued" || row.state == "running"))
            },
            Duration::from_secs(30),
        )
        .await
        .context("the generation never returned to queued/running after lease expiry")?;

        h.restart_worker()
            .context("restarting the worker after killing it")?;

        let succeeded = wait_until(
            || async {
                let row = h.generation_row(h.tenant1.id, generation_uuid).await?;
                Ok(row.filter(|row| row.state == "succeeded"))
            },
            Duration::from_secs(40),
        )
        .await
        .context("the generation never succeeded after the worker restarted")?;
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

        h.fake.set_mode(FakeMode::reply("recovered"));
        Ok(())
    })
}

/// An Attempt whose execution exceeds the Worker's 45-second lease TTL
/// stays alive because the Worker heartbeats every 10 seconds (ADR 0003):
/// `lease_expires_at` advances at least once, the Attempt never lapses
/// into `lease_expired`, and the Generation still succeeds with exactly
/// one Attempt.
#[test]
fn heartbeat_renews_lease_past_the_ttl_without_expiry() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake
            .set_mode(FakeMode::reply("slow but alive").with_delay(Duration::from_secs(50)));

        let generation = h.native_submit_model(&h.model_alias).await?;
        let generation_uuid = uuid::Uuid::parse_str(&generation.generation_id)?;

        let first_expiry = wait_until(
            || async {
                let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
                Ok(attempts.first().map(|attempt| attempt.lease_expires_at))
            },
            Duration::from_secs(30),
        )
        .await
        .context("the attempt was never leased")?;

        let renewed = wait_until(
            || async {
                let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
                let Some(attempt) = attempts.first() else {
                    return Ok(None);
                };
                anyhow::ensure!(
                    attempt.state != "lease_expired",
                    "lease expired despite active heartbeats"
                );
                Ok((attempt.lease_expires_at > first_expiry).then_some(attempt.lease_expires_at))
            },
            Duration::from_secs(35),
        )
        .await
        .context("lease_expires_at never advanced past its initially leased value")?;
        anyhow::ensure!(renewed > first_expiry);

        let succeeded = wait_until(
            || async {
                let row = h.generation_row(h.tenant1.id, generation_uuid).await?;
                Ok(row.filter(|row| row.state == "succeeded"))
            },
            Duration::from_mins(1),
        )
        .await
        .context("the generation never succeeded despite a heartbeat-extended lease")?;
        anyhow::ensure!(
            succeeded.attempt_count == 1,
            "attempt_count: {}",
            succeeded.attempt_count
        );

        let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
        anyhow::ensure!(attempts.len() == 1, "attempts: {attempts:?}");
        anyhow::ensure!(
            attempts[0].state == "succeeded",
            "attempt: {:?}",
            attempts[0]
        );

        h.fake.set_mode(FakeMode::reply("recovered"));
        Ok(())
    })
}

/// After a Generation is cancelled, a late `AttemptResult` racing the
/// cancellation is rejected by Remote's terminal compare-and-set, and the
/// Worker is told to discard its output over the same control Session
/// (ADR 0003): the Generation and its Attempt stay `cancelled`. Driven
/// with the generated `WorkerSessionServiceClient` directly, impersonating
/// a Worker at the protocol layer, because `crates/gpq-worker/src/backend/
/// llama.rs`'s cooperative cancellation makes a real Worker's late success
/// unreachable — cancellation always wins the real race.
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one protocol narrative: lease, run, cancel, then commit a late result and read \
              the Discard reply off the same stream; splitting it would break the ordering \
              the assertions depend on"
)]
fn late_result_after_cancellation_is_rejected_and_discarded() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut synthetic = SyntheticLlmWorker::spawn(h, "lifecycle-synthetic-race").await?;

        let generation = h.native_submit_model(&synthetic.alias).await?;
        let generation_uuid = uuid::Uuid::parse_str(&generation.generation_id)?;

        let lease = synthetic
            .worker
            .recv_lease(Duration::from_secs(30))
            .await
            .context("the synthetic worker never received a LeaseAssignment")?;
        anyhow::ensure!(lease.generation_id == generation.generation_id);

        synthetic
            .worker
            .send(wpb::AttemptRunning {
                attempt_id: lease.attempt_id.clone(),
                ..Default::default()
            })
            .await?;

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
                generation_id: generation.generation_id.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("CancelGeneration failed: {err}"))?;

        let cancel = synthetic
            .worker
            .recv_cancel_request(Duration::from_secs(30))
            .await
            .context("the synthetic worker never received the CancelRequest")?;
        anyhow::ensure!(cancel.attempt_id == lease.attempt_id);

        synthetic
            .worker
            .send(wpb::CancelAcknowledged {
                attempt_id: lease.attempt_id.clone(),
                ..Default::default()
            })
            .await?;

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

        synthetic
            .worker
            .send(wpb::AttemptResult {
                attempt_id: lease.attempt_id.clone(),
                output_text: "late success that must never land".to_owned(),
                outputs: vec![wpb::AttemptOutput {
                    manifest: pb::ArtifactManifest {
                        size_bytes: 4,
                        digest_sha256: "0".repeat(64),
                        kind: pb::MediaKind::MEDIA_KIND_TEXT.into(),
                        mime_type: "text/plain".to_owned(),
                        ..Default::default()
                    }
                    .into(),
                    placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
                    delivery_token: "synthetic-race-token".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;

        let discard = synthetic
            .worker
            .recv_discard(Duration::from_secs(30), "synthetic-race-token")
            .await
            .context("Remote never told the synthetic worker to discard its late output")?;
        anyhow::ensure!(discard.delivery_token == "synthetic-race-token");

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
/// rejected the same way (ADR 0003: "results committed under an expired
/// lease are rejected") — the synthetic Worker never heartbeats, letting
/// the lease expire on its own, and Remote still tells it to discard the
/// late output over the control Session even though the Generation itself
/// only requeued rather than reaching a terminal state.
#[test]
fn stale_result_after_lease_expiry_is_rejected_and_discarded() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut synthetic = SyntheticLlmWorker::spawn(h, "lifecycle-synthetic-expiry").await?;

        let generation = h.native_submit_model(&synthetic.alias).await?;
        let generation_uuid = uuid::Uuid::parse_str(&generation.generation_id)?;

        let lease = synthetic
            .worker
            .recv_lease(Duration::from_secs(30))
            .await
            .context("the synthetic worker never received a LeaseAssignment")?;

        // Never heartbeat: the lease lapses on its own after LEASE_TTL.
        let expired = wait_until(
            || async {
                let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
                Ok(attempts.into_iter().find(|attempt| {
                    attempt.attempt_number == 1 && attempt.state == "lease_expired"
                }))
            },
            Duration::from_secs(75),
        )
        .await
        .context("the attempt never lease-expired without heartbeats")?;
        anyhow::ensure!(
            expired.failure_kind.as_deref() == Some("lease_expired"),
            "failure_kind: {:?}",
            expired.failure_kind
        );

        synthetic
            .worker
            .send(wpb::AttemptResult {
                attempt_id: lease.attempt_id.clone(),
                output_text: "stale success under an expired lease".to_owned(),
                outputs: vec![wpb::AttemptOutput {
                    manifest: pb::ArtifactManifest {
                        size_bytes: 4,
                        digest_sha256: "1".repeat(64),
                        kind: pb::MediaKind::MEDIA_KIND_TEXT.into(),
                        mime_type: "text/plain".to_owned(),
                        ..Default::default()
                    }
                    .into(),
                    placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
                    delivery_token: "synthetic-expired-token".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;

        let discard = synthetic
            .worker
            .recv_discard(Duration::from_secs(30), "synthetic-expired-token")
            .await
            .context("Remote never discarded the stale result committed under an expired lease")?;
        anyhow::ensure!(discard.delivery_token == "synthetic-expired-token");

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
/// explicitly with `FailedPrecondition` (ADR 0004) — driven with the
/// generated `WorkerSessionServiceClient` directly rather than the Worker
/// binary, since `gpq-worker` itself never reports a mismatched major.
#[test]
fn session_handshake_rejects_incompatible_protocol_major() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let err = SyntheticWorker::connect_with_bad_protocol_major(
            h,
            "lifecycle-synthetic-protocol-mismatch",
            gpq_proto::PROTOCOL_MAJOR + 1,
        )
        .await?;
        anyhow::ensure!(
            err.code == connectrpc::ErrorCode::FailedPrecondition,
            "unexpected error code: {:?}",
            err.code
        );
        Ok(())
    })
}

/// Revoking the enrolled Worker's Credential (ADR 0009) makes it fail to
/// re-establish its control Session on the next connection attempt — the
/// harness's real Worker never comes back `online` in `ListWorkers` — and
/// the Model it alone served becomes unavailable, since a Tenant Master Key
/// never grants Worker protocol authority (ADR 0004, ADR 0009). Runs last
/// among the substantive tests (before `zzz_teardown_harness`, sorted by
/// the `zy_` prefix) because it permanently disables the shared harness's
/// real Worker.
#[test]
fn zy_revoked_worker_credential_blocks_reconnection_and_scheduling() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        h.fake.set_mode(FakeMode::reply("before revocation"));

        let warmup = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&serde_json::json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "warmup"}],
                "stream": false,
            }))
            .send()
            .await?;
        anyhow::ensure!(
            warmup.status().is_success(),
            "warmup status {}",
            warmup.status()
        );

        h.revoke_worker_credential()
            .await
            .context("revoking the harness worker's credential")?;
        anyhow::ensure!(
            h.kill_worker().await?,
            "expected a running gpq-worker to kill"
        );
        h.restart_worker()
            .context("restarting the worker after revoking its credential")?;

        // gpq-worker never treats an Unauthenticated session as fatal — it
        // retries with backoff forever (crates/gpq-worker/src/session.rs) —
        // so the only observable proof of revocation is that it never comes
        // back online, checked repeatedly over a window rather than once.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let response = h
                .catalog_client(&h.tenant1.master_key)
                .list_workers(pb::ListWorkersRequest::default())
                .await
                .map_err(|err| anyhow::anyhow!("ListWorkers failed: {err}"))?
                .into_owned();
            if let Some(worker) = response
                .workers
                .iter()
                .find(|worker| !worker.name.starts_with(lifecycle_support::NAME_PREFIX))
            {
                anyhow::ensure!(
                    !worker.online,
                    "the revoked worker reconnected and went online"
                );
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let after_revoke = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&serde_json::json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "should fail"}],
                "stream": false,
            }))
            .send()
            .await?;
        anyhow::ensure!(
            after_revoke.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "status {}",
            after_revoke.status()
        );
        let body: serde_json::Value = after_revoke.json().await?;
        anyhow::ensure!(
            body["error"]["code"] == "model_not_available",
            "body: {body}"
        );

        Ok(())
    })
}

/// Tears the shared `Harness` down: kills the `gpq-remote`/`gpq-worker`
/// child processes, drops the per-run database and login role, and removes
/// the run's temp directory. Named to sort after every other test above so
/// it runs last under `--test-threads=1`, whether or not earlier tests
/// failed.
#[test]
fn zzz_teardown_harness() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(Harness::teardown(h))
}
