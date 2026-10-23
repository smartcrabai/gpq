//! End-to-end tests driving the real `gpq remote serve` and `gpq worker run`
//! processes over their real wire protocols (Connect, gRPC, and plain HTTP).
//!
//! Tests share a PostgreSQL 18 container; a per-test access guard keeps the
//! real Remote and Worker alive during each case. Teardown waits for active
//! cases, and subsequent access builds a fresh harness.
//!
//! Generations execute on [`SyntheticWorker`]s, one per test, each advertising
//! a private Model Version. Each test's doc comment names the ADR invariant it
//! defends.

#[expect(
    dead_code,
    reason = "shared harness compiled into every integration-test binary; each suite \
              uses the subset it needs"
)]
mod e2e_support;
#[path = "safety/native_session_safety.rs"]
mod native_session_safety;

use std::time::Duration;

use anyhow::Context;
use connectrpc::ErrorCode;
use e2e_support::{Harness, HarnessOptions, SyntheticWorker, harness, image_request, wait_until};
use gpq_domain::{ContentHash, ModelFamily};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;

/// Polls until the Generation `generation_id` reaches `state`.
async fn wait_for_state(
    h: &Harness,
    generation_id: &str,
    state: pb::GenerationState,
    timeout: Duration,
) -> anyhow::Result<pb::Generation> {
    wait_until(
        || async {
            let generation = h.get_generation(generation_id).await?;
            Ok((generation.state == state).then_some(generation))
        },
        timeout,
    )
    .await
    .with_context(|| format!("generation {generation_id} never reached {state:?}"))
}

/// Receives the next lease for `worker`, asserts it is for `generation_id`,
/// and reports the Attempt as running.
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

/// Delivers a completed worker-local output through the real artifact endpoint
/// and verifies its one-shot, tenant-scoped consumption behavior.
async fn serve_and_verify_image_output(
    h: &Harness,
    worker: &mut SyntheticWorker,
    generation_id: &str,
    token: &str,
    succeeded: &pb::Generation,
    bytes: &[u8],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        succeeded.output_artifacts.len() == 1,
        "output artifacts: {:?}",
        succeeded.output_artifacts
    );
    let artifact = &succeeded.output_artifacts[0];
    let artifact_id = artifact.artifact_id.clone();
    let download_path = artifact.download_path.clone();
    let service_url = url::Url::parse(&h.url("/"))?;
    let artifact_url = url::Url::parse(&download_path)?;
    let expected_path = format!("/v1/artifacts/{}", artifact.artifact_id);
    anyhow::ensure!(
        artifact_url.origin() == service_url.origin()
            && artifact_url.username().is_empty()
            && artifact_url.password().is_none()
            && artifact_url.path() == expected_path
            && artifact_url.query().is_none()
            && artifact_url.fragment().is_none(),
        "download URL does not identify this Remote's artifact endpoint: {download_path}"
    );
    anyhow::ensure!(
        !format!("{succeeded:?}").contains(&*h.tmp_root().to_string_lossy()),
        "a filesystem path leaked into the Generation payload"
    );

    // Start the client GET before asking the Worker to deliver its output.
    let download = {
        let http = h.http.clone();
        let url = artifact_url.clone();
        let key = h.tenant1.master_key.clone();
        tokio::spawn(async move {
            let response = http.get(url).bearer_auth(key).send().await?;
            let status = response.status();
            Ok::<_, reqwest::Error>((status, response.bytes().await?))
        })
    };
    let deliver = worker
        .recv_deliver(Duration::from_secs(30), token)
        .await
        .context("Remote never asked the worker to deliver its output")?;
    let accepted = worker
        .deliver_output(
            &deliver,
            bytes,
            pb::MediaKind::MEDIA_KIND_IMAGE,
            "image/png",
        )
        .await?;
    anyhow::ensure!(accepted.accepted, "delivery rejected: {}", accepted.reason);

    let (status, body) = download.await??;
    anyhow::ensure!(status.is_success(), "download status: {status}");
    anyhow::ensure!(body.as_ref() == bytes, "downloaded bytes differ");
    verify_image_artifact_consumption(h, generation_id, artifact_id, artifact_url).await
}

async fn verify_image_artifact_consumption(
    h: &Harness,
    generation_id: &str,
    artifact_id: String,
    artifact_url: url::Url,
) -> anyhow::Result<()> {
    // The response can reach its declared length just before consumption commits.
    wait_until(
        || async {
            let current = h.get_generation(generation_id).await?;
            Ok(current
                .output_artifacts
                .iter()
                .any(|output| {
                    output.artifact_id == artifact_id
                        && output.state == pb::ArtifactState::ARTIFACT_STATE_CONSUMED
                })
                .then_some(()))
        },
        Duration::from_secs(15),
    )
    .await?;
    let consumed = h
        .http
        .get(artifact_url.clone())
        .bearer_auth(&h.tenant1.master_key)
        .send()
        .await?;
    anyhow::ensure!(
        consumed.status() == reqwest::StatusCode::GONE,
        "artifact remained downloadable after its one-shot delivery: {}",
        consumed.status()
    );

    // Another Tenant's Master Key cannot read this Tenant's output by its URL.
    let foreign = h
        .http
        .get(artifact_url)
        .bearer_auth(&h.tenant2.master_key)
        .send()
        .await?;
    anyhow::ensure!(
        foreign.status().is_client_error(),
        "tenant 2 read tenant 1's output: {}",
        foreign.status()
    );
    Ok(())
}

/// Model Version, is aliased, then disconnects (ADR 0002 capability filtering).
async fn alias_without_online_worker(h: &Harness, name: &str) -> anyhow::Result<String> {
    let mut worker = SyntheticWorker::spawn(h, name, ModelFamily::QwenImage21).await?;
    let alias = worker.alias.clone();
    worker.disconnect();
    wait_until(|| h.worker_is_online(name, false), Duration::from_secs(30))
        .await
        .context("the synthetic worker never went offline after disconnecting")?;
    Ok(alias)
}

/// Counts every nonterminal (queued, running, or cancelling) Generation for
/// `master_key`'s Tenant, paging through `ListGenerations` per state.
async fn count_nonterminal(h: &Harness, master_key: &str) -> anyhow::Result<u32> {
    let mut total = 0_u32;
    for state in [
        pb::GenerationState::GENERATION_STATE_QUEUED,
        pb::GenerationState::GENERATION_STATE_RUNNING,
        pb::GenerationState::GENERATION_STATE_CANCELLING,
    ] {
        let mut page_token = String::new();
        loop {
            let response = h
                .generation_client(master_key)
                .list_generations(pb::ListGenerationsRequest {
                    page_size: 200,
                    page_token: page_token.clone(),
                    state: state.into(),
                    ..Default::default()
                })
                .await
                .map_err(|err| anyhow::anyhow!("ListGenerations failed: {err}"))?
                .into_owned();
            total += u32::try_from(response.generations.len())?;
            if response.next_page_token.is_empty() {
                break;
            }
            page_token = response.next_page_token;
        }
    }
    Ok(total)
}

/// `GET /healthz` and `GET /readyz` answer `200` with no object storage
/// configured (ADR 0008: S3 is optional and never affects readiness).
#[test]
fn healthz_and_readyz_ok_without_object_storage() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let healthz = h.http.get(h.url("/healthz")).send().await?;
        anyhow::ensure!(
            healthz.status().is_success(),
            "healthz: {}",
            healthz.status()
        );

        let readyz = h.http.get(h.url("/readyz")).send().await?;
        anyhow::ensure!(readyz.status().is_success(), "readyz: {}", readyz.status());
        Ok(())
    })
}

/// `CatalogService.ListModels` rejects an unknown Tenant Master Key with
/// `Unauthenticated` and, for a valid key, lists Model aliases only: the
/// alias a Worker registered is visible to its own Tenant (ADR 0006, ADR
/// 0012).
#[test]
fn catalog_requires_master_key_and_lists_aliases() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let rejected = h
            .catalog_client("not-a-tenant-master-key")
            .list_models(pb::ListModelsRequest::default())
            .await;
        let Err(err) = rejected else {
            anyhow::bail!("ListModels accepted an unknown Master Key");
        };
        anyhow::ensure!(
            err.code == ErrorCode::Unauthenticated,
            "unexpected error code: {:?}",
            err.code
        );

        let worker = SyntheticWorker::spawn(&h, "e2e-catalog", ModelFamily::QwenImage21).await?;
        let models = h
            .catalog_client(&h.tenant1.master_key)
            .list_models(pb::ListModelsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("ListModels failed: {err}"))?
            .into_owned();
        anyhow::ensure!(
            models
                .aliases
                .iter()
                .any(|alias| alias.alias == worker.alias),
            "expected {} among the listed aliases",
            worker.alias
        );
        Ok(())
    })
}

/// A durable image `Submit` reaches `running` and then `succeeded` through a
/// Worker that leases exactly the typed `ImageParameters` submitted (ADR 0006,
/// ADR 0003). The Worker's worker-local output is delivered through the
/// download path Remote returns, which a plain HTTP client can fetch with the
/// Tenant Master Key (ADR 0006, ADR 0008), and the Generation's events persist.
/// The public payloads carry no filesystem paths (ADR 0008).
#[test]
fn image_generation_leases_typed_parameters_and_serves_output_to_clients() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker = SyntheticWorker::spawn(&h, "e2e-image", ModelFamily::QwenImage21).await?;
        let generation = h.submit(image_request(&worker.alias, "a red fox")).await?;
        anyhow::ensure!(
            generation.modality == pb::Modality::MODALITY_IMAGE,
            "modality: {:?}",
            generation.modality
        );
        anyhow::ensure!(
            generation.version_sha256 == worker.version_sha256,
            "generation pinned to {} instead of {}",
            generation.version_sha256,
            worker.version_sha256
        );
        let generation_id = generation.generation_id.clone();
        let generation_uuid = uuid::Uuid::parse_str(&generation_id)?;

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        match &lease.generation_parameters {
            Some(wpb::lease_assignment::GenerationParameters::ImageParameters(params)) => {
                anyhow::ensure!(
                    params.prompt == "a red fox",
                    "leased prompt: {}",
                    params.prompt
                );
            }
            other => anyhow::bail!("lease did not carry image parameters: {other:?}"),
        }
        anyhow::ensure!(lease.seed == 42, "leased seed: {}", lease.seed);
        wait_for_state(
            &h,
            &generation_id,
            pb::GenerationState::GENERATION_STATE_RUNNING,
            Duration::from_secs(15),
        )
        .await?;

        let bytes = b"\x89PNG synthetic image bytes".to_vec();
        let token = worker
            .complete_attempt(
                &lease.attempt_id,
                &bytes,
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        let succeeded = wait_for_state(
            &h,
            &generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(20),
        )
        .await?;
        serve_and_verify_image_output(&h, &mut worker, &generation_id, &token, &succeeded, &bytes)
            .await?;

        let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
        anyhow::ensure!(attempts.len() == 1, "attempts: {attempts:?}");
        anyhow::ensure!(
            attempts[0].state == "succeeded",
            "attempt: {:?}",
            attempts[0]
        );
        let events = h.event_kinds(h.tenant1.id, generation_uuid).await?;
        anyhow::ensure!(
            events.contains(&"attempt_created".to_owned()),
            "events: {events:?}"
        );
        anyhow::ensure!(
            events.contains(&"state_changed".to_owned()),
            "events: {events:?}"
        );
        Ok(())
    })
}

/// An unknown Model alias is rejected at admission with `NotFound` (ADR 0006),
/// and an alias no online Worker serves admits the Generation but leaves it
/// `queued` rather than leasing it to a Worker that cannot run it (ADR 0002).
#[test]
fn unknown_alias_is_not_found_and_unserved_alias_stays_queued() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let rejected = h.submit(image_request("does-not-exist", "hi")).await;
        let Err(err) = rejected else {
            anyhow::bail!("an unknown alias was admitted");
        };
        anyhow::ensure!(
            err.code == ErrorCode::NotFound,
            "unexpected code: {:?}",
            err.code
        );

        let alias = alias_without_online_worker(&h, "e2e-unserved").await?;
        let generation = h.submit(image_request(&alias, "nobody home")).await?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let still_queued = h.get_generation(&generation.generation_id).await?;
        anyhow::ensure!(
            still_queued.state == pb::GenerationState::GENERATION_STATE_QUEUED,
            "expected queued, got {:?}",
            still_queued.state
        );
        Ok(())
    })
}

/// A Generation whose Attempt fails with a backend crash is retried up to the
/// three-Attempt ceiling, after which the Generation fails (ADR 0003).
#[test]
fn backend_failure_exhausts_retries_and_fails_generation() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "e2e-failing", ModelFamily::QwenImage21).await?;
        let generation = h
            .submit(image_request(&worker.alias, "this will fail"))
            .await?;
        let generation_id = generation.generation_id.clone();
        let generation_uuid = uuid::Uuid::parse_str(&generation_id)?;

        for _ in 0..3 {
            let lease = lease_and_start(&mut worker, &generation_id).await?;
            worker
                .fail_attempt(
                    &lease.attempt_id,
                    pb::FailureKind::FAILURE_KIND_BACKEND_CRASHED,
                )
                .await?;
        }

        let failed = wait_for_state(
            &h,
            &generation_id,
            pb::GenerationState::GENERATION_STATE_FAILED,
            Duration::from_secs(30),
        )
        .await?;
        anyhow::ensure!(
            failed.attempt_count == 3,
            "attempt_count: {}",
            failed.attempt_count
        );

        let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
        anyhow::ensure!(attempts.len() == 3, "attempts: {attempts:?}");
        for (index, attempt) in attempts.iter().enumerate() {
            anyhow::ensure!(
                attempt.attempt_number == i32::try_from(index)? + 1,
                "attempt: {attempt:?}"
            );
            anyhow::ensure!(attempt.state == "failed", "attempt: {attempt:?}");
            anyhow::ensure!(
                attempt.failure_kind.as_deref() == Some("backend_crashed"),
                "attempt: {attempt:?}"
            );
        }
        Ok(())
    })
}

/// `WatchGeneration` yields the current snapshot first, then live events
/// through the terminal `stateChanged`, so a client that subscribes before
/// the Worker finishes still observes completion (ADR 0006).
#[test]
fn watch_generation_streams_snapshot_then_live_events() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker = SyntheticWorker::spawn(&h, "e2e-watch", ModelFamily::QwenImage21).await?;
        let generation = h.submit(image_request(&worker.alias, "watched")).await?;
        let generation_id = generation.generation_id.clone();
        let mut stream = h.watch_generation(&generation_id).await?;

        let first = stream
            .message::<pb::GenerationEvent>()
            .await
            .map_err(|err| anyhow::anyhow!("watch stream error: {err}"))?
            .context("stream ended before any event")?
            .to_owned_message();
        anyhow::ensure!(
            matches!(first.event, Some(pb::generation_event::Event::Snapshot(_))),
            "first event was not a snapshot: {first:?}"
        );

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        worker
            .complete_attempt(
                &lease.attempt_id,
                b"watched bytes",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;

        let mut saw_terminal = false;
        for _ in 0..64_u32 {
            let Some(item) = stream
                .message::<pb::GenerationEvent>()
                .await
                .map_err(|err| anyhow::anyhow!("watch stream error: {err}"))?
            else {
                break;
            };
            let event = item.to_owned_message();
            if let Some(pb::generation_event::Event::StateChanged(changed)) = event.event
                && changed.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED
            {
                saw_terminal = true;
                break;
            }
        }
        anyhow::ensure!(saw_terminal, "never observed a terminal stateChanged event");
        Ok(())
    })
}

/// A second Tenant's Master Key cannot read the first Tenant's Generation
/// (`NotFound`), sees none of its Model aliases, and cannot see the first
/// Tenant's Worker (ADR 0001, ADR 0011).
#[test]
fn cross_tenant_isolation_over_the_api() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let worker = SyntheticWorker::spawn(&h, "e2e-isolation", ModelFamily::QwenImage21).await?;
        let generation = h.submit(image_request(&worker.alias, "tenant one")).await?;

        let err = h
            .generation_client(&h.tenant2.master_key)
            .get_generation(pb::GetGenerationRequest {
                generation_id: generation.generation_id.clone(),
                ..Default::default()
            })
            .await
            .err()
            .context("tenant 2 unexpectedly read tenant 1's generation")?;
        anyhow::ensure!(
            err.code == ErrorCode::NotFound,
            "unexpected code: {:?}",
            err.code
        );

        let models = h
            .catalog_client(&h.tenant2.master_key)
            .list_models(pb::ListModelsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("tenant 2 list_models failed: {err}"))?
            .into_owned();
        anyhow::ensure!(
            models
                .aliases
                .iter()
                .all(|alias| alias.alias != worker.alias),
            "tenant 2 sees tenant 1's alias {}",
            worker.alias
        );

        let workers = h
            .catalog_client(&h.tenant2.master_key)
            .list_workers(pb::ListWorkersRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("tenant 2 list_workers failed: {err}"))?
            .into_owned();
        anyhow::ensure!(
            workers
                .workers
                .iter()
                .all(|listed_worker| listed_worker.worker_id != worker.worker_id),
            "tenant 2 sees tenant 1's worker {}: {:?}",
            worker.worker_id,
            workers.workers
        );
        Ok(())
    })
}

/// A `remote_local` input is uploaded through the URL `CreateInputArtifact`
/// returns, which is a client-usable absolute URL with no filesystem path in
/// it. The bytes are stored only after the Tenant Master Key authenticates the
/// upload, and a Worker fetches them over `FetchArtifact` during its attempt.
/// The input row is removed when the Generation reaches a terminal state
/// (ADR 0008).
#[test]
fn remote_local_input_uploads_by_url_and_is_removed_at_terminal() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker = SyntheticWorker::spawn(&h, "e2e-input", ModelFamily::QwenImage21).await?;
        let bytes = b"reference image bytes uploaded by the client".to_vec();
        let (artifact_id, upload_url) = create_input(&h, &bytes).await?;
        anyhow::ensure!(
            upload_url.starts_with(&h.url("/v1/artifacts/")),
            "upload url is not a client URL on this Remote: {upload_url}"
        );
        anyhow::ensure!(
            !upload_url.contains(&*h.tmp_root().to_string_lossy()),
            "a filesystem path leaked into the upload URL"
        );
        upload_bytes(&h, &upload_url, &h.tenant1.master_key, &bytes).await?;

        let mut request = image_request(&worker.alias, "use the reference");
        if let Some(pb::submit_request::GenerationParameters::ImageParameters(params)) =
            request.generation_parameters.as_mut()
        {
            params.reference_artifact_ids = vec![artifact_id.to_string()];
        }
        let generation = h.submit(request).await?;
        let generation_id = generation.generation_id.clone();

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        let input = lease
            .inputs
            .iter()
            .find(|input| input.artifact_id == artifact_id.to_string())
            .context("the lease does not name the reference input")?;
        anyhow::ensure!(
            input.download_url.is_empty(),
            "a remote_local input leaked a download URL"
        );
        let fetched = worker
            .fetch_input(&lease.attempt_id, &artifact_id.to_string())
            .await?;
        anyhow::ensure!(fetched == bytes, "fetched input differs from the upload");

        worker
            .complete_attempt(
                &lease.attempt_id,
                b"from the reference",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        wait_for_state(
            &h,
            &generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(20),
        )
        .await?;
        let remaining = h
            .generation_row(h.tenant1.id, uuid::Uuid::parse_str(&generation_id)?)
            .await?;
        anyhow::ensure!(remaining.is_some(), "generation row vanished");
        anyhow::ensure!(
            e2e_support::db::artifact_state(h.admin_pool(), h.tenant1.id, artifact_id)
                .await?
                .is_none(),
            "the remote_local input survived its Generation's terminal state"
        );
        Ok(())
    })
}

/// A `remote_local` upload whose bytes disagree with the declared manifest is
/// rejected (`400`) for both a wrong size and a wrong SHA-256, an
/// unauthenticated upload is rejected, and none of them changes the input's
/// state or stored bytes. A correct upload then succeeds (ADR 0008).
#[test]
fn remote_local_upload_rejects_mismatches_without_state_change() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let bytes = b"exactly these bytes".to_vec();
        let (artifact_id, upload_url) = create_input(&h, &bytes).await?;

        let short = bytes[..bytes.len() - 1].to_vec();
        let status = put_bytes(&h, &upload_url, Some(&h.tenant1.master_key), short).await?;
        anyhow::ensure!(
            status == reqwest::StatusCode::BAD_REQUEST,
            "short upload: {status}"
        );

        let mut wrong_digest = bytes.clone();
        wrong_digest[0] ^= 0xff;
        let status = put_bytes(&h, &upload_url, Some(&h.tenant1.master_key), wrong_digest).await?;
        anyhow::ensure!(
            status == reqwest::StatusCode::BAD_REQUEST,
            "digest mismatch: {status}"
        );

        let status = put_bytes(&h, &upload_url, None, bytes.clone()).await?;
        anyhow::ensure!(
            status == reqwest::StatusCode::UNAUTHORIZED,
            "unauthenticated upload: {status}"
        );

        let pending = e2e_support::db::artifact_state(h.admin_pool(), h.tenant1.id, artifact_id)
            .await?
            .context("the input row disappeared after rejected uploads")?;
        anyhow::ensure!(
            pending == ("pending".to_owned(), None),
            "rejected uploads changed the input: {pending:?}"
        );

        upload_bytes(&h, &upload_url, &h.tenant1.master_key, &bytes).await?;
        let available = e2e_support::db::artifact_state(h.admin_pool(), h.tenant1.id, artifact_id)
            .await?
            .context("the input row disappeared after a valid upload")?;
        anyhow::ensure!(
            available.0 == "available",
            "valid upload did not make the input available: {available:?}"
        );
        Ok(())
    })
}

/// A Tenant cannot touch another Tenant's `remote_local` input, and a Worker
/// cannot fetch an input that does not belong to its own live Attempt (ADR
/// 0008, ADR 0009, ADR 0011). A Worker enrolled in tenant 2 gets nothing for
/// a tenant 1 input, and the tenant 1 Worker still fetches its own input.
#[test]
fn remote_local_inputs_are_scoped_to_their_tenant_and_attempt() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let bytes = b"tenant one private reference".to_vec();
        let (artifact_id, upload_url) = create_input(&h, &bytes).await?;

        // Another Tenant's Master Key cannot upload to this pending input.
        let status = put_bytes(&h, &upload_url, Some(&h.tenant2.master_key), bytes.clone()).await?;
        anyhow::ensure!(status.is_client_error(), "cross-tenant upload: {status}");
        let state = e2e_support::db::artifact_state(h.admin_pool(), h.tenant1.id, artifact_id)
            .await?
            .context("the input row disappeared")?;
        anyhow::ensure!(
            state == ("pending".to_owned(), None),
            "state changed: {state:?}"
        );

        upload_bytes(&h, &upload_url, &h.tenant1.master_key, &bytes).await?;

        let mut worker = SyntheticWorker::spawn(&h, "e2e-scoped", ModelFamily::QwenImage21).await?;
        let mut request = image_request(&worker.alias, "scoped");
        if let Some(pb::submit_request::GenerationParameters::ImageParameters(params)) =
            request.generation_parameters.as_mut()
        {
            params.reference_artifact_ids = vec![artifact_id.to_string()];
        }
        let generation = h.submit(request).await?;
        let lease = lease_and_start(&mut worker, &generation.generation_id).await?;

        let outsider =
            SyntheticWorker::enrolled_in(&h, &h.tenant2.master_key, "e2e-outsider").await?;
        anyhow::ensure!(
            outsider
                .fetch_input(&lease.attempt_id, &artifact_id.to_string())
                .await
                .is_err(),
            "a tenant 2 worker fetched a tenant 1 input"
        );

        let owned = worker
            .fetch_input(&lease.attempt_id, &artifact_id.to_string())
            .await?;
        anyhow::ensure!(
            owned == bytes,
            "the owning worker could not fetch its input"
        );

        // A live Worker credential names only its own Attempts: a stale or
        // invented Attempt id for the same input must fetch nothing.
        let foreign_attempt = uuid::Uuid::now_v7().to_string();
        anyhow::ensure!(
            worker
                .fetch_input(&foreign_attempt, &artifact_id.to_string())
                .await
                .is_err(),
            "a worker fetched an input under an attempt it does not hold"
        );

        worker
            .complete_attempt(
                &lease.attempt_id,
                b"scoped result",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        wait_for_state(
            &h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(20),
        )
        .await?;
        Ok(())
    })
}

/// `CreateInputArtifact` without object storage returns a `remote_local`
/// upload URL on this Remote, and a native `Submit` requesting object-store
/// output placement fails admission with `FailedPrecondition` (ADR 0008).
#[test]
fn object_store_output_placement_is_refused_without_object_storage() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let worker = SyntheticWorker::spawn(&h, "e2e-no-s3", ModelFamily::QwenImage21).await?;
        let mut request = image_request(&worker.alias, "no s3");
        request.output_placement = pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE.into();
        let Err(err) = h.submit(request).await else {
            anyhow::bail!("object-store output was admitted without object storage");
        };
        anyhow::ensure!(
            err.code == ErrorCode::FailedPrecondition,
            "unexpected code: {:?}",
            err.code
        );
        Ok(())
    })
}

/// Lowering `max_queued_generations` to the Tenant's current nonterminal count
/// makes the very next admission fail `ResourceExhausted`, proving the setting
/// actually reaches admission (ADR 0002, ADR 0006).
#[test]
fn lowered_max_queued_generations_rejects_the_next_admission() -> anyhow::Result<()> {
    let h = e2e_support::block_on(Harness::build_with(HarnessOptions::default()))?;
    let result = e2e_support::block_on(async {
        let alias = alias_without_online_worker(&h, "e2e-capacity").await?;
        h.submit(image_request(&alias, "filler")).await?;

        let nonterminal = wait_until(
            || async {
                let count = count_nonterminal(&h, &h.tenant1.master_key).await?;
                Ok((count >= 1).then_some(count))
            },
            Duration::from_secs(5),
        )
        .await
        .context("the filler admission never became nonterminal")?;

        let original = tenant_settings(&h, &h.tenant1.master_key).await?;
        let mut tightened = original.clone();
        tightened.max_queued_generations = nonterminal;
        update_settings(
            &h,
            &h.tenant1.master_key,
            tightened,
            "max_queued_generations",
        )
        .await?;

        let rejected = h.submit(image_request(&alias, "one too many")).await;
        update_settings(&h, &h.tenant1.master_key, original, "").await?;

        let Err(err) = rejected else {
            anyhow::bail!("admission succeeded above max_queued_generations");
        };
        anyhow::ensure!(
            err.code == ErrorCode::ResourceExhausted,
            "unexpected code: {:?}",
            err.code
        );
        Ok(())
    });
    let teardown = e2e_support::block_on(h.teardown());
    result?;
    teardown
}

/// After `DeleteModelAlias`, the alias is no longer listed, while a Generation
/// admitted through it before deletion still runs to completion on the Model
/// Version it was pinned to: alias deletion never mutates Model Versions or
/// past Generations (ADR 0012).
#[test]
fn delete_model_alias_removes_listing_without_mutating_pinned_generations() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut worker = SyntheticWorker::spawn(&h, "e2e-alias", ModelFamily::QwenImage21).await?;
        let generation = h.submit(image_request(&worker.alias, "pinned")).await?;
        let generation_id = generation.generation_id.clone();

        h.catalog_client(&h.tenant1.master_key)
            .delete_model_alias(pb::DeleteAliasRequest {
                alias: worker.alias.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("DeleteModelAlias failed: {err}"))?;

        let models = h
            .catalog_client(&h.tenant1.master_key)
            .list_models(pb::ListModelsRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("ListModels failed: {err}"))?
            .into_owned();
        anyhow::ensure!(
            models
                .aliases
                .iter()
                .all(|alias| alias.alias != worker.alias),
            "deleted alias still listed"
        );

        let lease = lease_and_start(&mut worker, &generation_id).await?;
        worker
            .complete_attempt(
                &lease.attempt_id,
                b"pinned result",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        let succeeded = wait_for_state(
            &h,
            &generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(20),
        )
        .await?;
        anyhow::ensure!(
            succeeded.version_sha256 == worker.version_sha256,
            "version_sha256 changed after alias deletion"
        );
        Ok(())
    })
}

/// `GetTenantSettings` returns exactly the defaults `0001_initial.sql`'s
/// `tenants` table columns default to (ADR 0002, ADR 0006). Uses Tenant 2,
/// which no other test mutates, so this stays read-only.
#[test]
fn get_tenant_settings_returns_migration_defaults() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let settings = tenant_settings(&h, &h.tenant2.master_key).await?;
        let queue_age = Duration::try_from(
            settings
                .maximum_queue_age
                .clone()
                .into_option()
                .context("settings missing maximum_queue_age")?,
        )
        .map_err(|err| anyhow::anyhow!("invalid maximum_queue_age: {err}"))?;
        anyhow::ensure!(
            queue_age == Duration::from_mins(30),
            "queue_age: {queue_age:?}"
        );
        anyhow::ensure!(
            settings.max_queued_generations == 1000,
            "max_queued_generations: {}",
            settings.max_queued_generations
        );
        anyhow::ensure!(
            settings.max_input_artifact_bytes == 256 * 1024 * 1024,
            "max_input_artifact_bytes: {}",
            settings.max_input_artifact_bytes
        );
        anyhow::ensure!(
            settings.max_output_artifact_bytes == 2 * 1024 * 1024 * 1024,
            "max_output_artifact_bytes: {}",
            settings.max_output_artifact_bytes
        );
        anyhow::ensure!(
            settings.default_priority == 5,
            "default_priority: {}",
            settings.default_priority
        );
        Ok(())
    })
}

/// `UpdateTenantSettings` changes only the fields named in `update_mask`,
/// leaving every other field at its prior value, and rejects a
/// `default_priority` outside `0..=9` (ADR 0006). It uses an isolated harness
/// so temporary settings cannot affect parallel tests.
#[test]
fn update_tenant_settings_honours_partial_mask_and_rejects_out_of_range() -> anyhow::Result<()> {
    let h = e2e_support::block_on(Harness::build_with(HarnessOptions::default()))?;
    let result = e2e_support::block_on(async {
        let original = tenant_settings(&h, &h.tenant2.master_key).await?;

        let mut touched = original.clone();
        touched.default_priority = 7;
        let updated =
            update_settings(&h, &h.tenant2.master_key, touched, "default_priority").await?;
        anyhow::ensure!(
            updated.default_priority == 7,
            "default_priority: {}",
            updated.default_priority
        );
        anyhow::ensure!(
            updated.max_queued_generations == original.max_queued_generations,
            "an unmasked field changed"
        );
        anyhow::ensure!(
            updated.max_input_artifact_bytes == original.max_input_artifact_bytes,
            "an unmasked field changed"
        );

        let mut out_of_range = updated.clone();
        out_of_range.default_priority = 10;
        let rejected = tenant_client(&h, &h.tenant2.master_key)?
            .update_tenant_settings(pb::UpdateTenantSettingsRequest {
                settings: out_of_range.into(),
                update_mask: vec!["default_priority".to_owned()],
                ..Default::default()
            })
            .await;
        anyhow::ensure!(
            rejected.is_err(),
            "expected an out-of-range default_priority to be rejected"
        );

        update_settings(&h, &h.tenant2.master_key, original, "").await?;
        Ok(())
    });
    let teardown = e2e_support::block_on(h.teardown());
    result?;
    teardown
}

/// Reads `GetTenantSettings` for the Tenant that owns `master_key`.
async fn tenant_settings(h: &Harness, master_key: &str) -> anyhow::Result<pb::TenantSettings> {
    let response = tenant_client(h, master_key)?
        .get_tenant_settings(pb::GetTenantSettingsRequest::default())
        .await
        .map_err(|err| anyhow::anyhow!("GetTenantSettings failed: {err}"))?
        .into_owned();
    response
        .settings
        .into_option()
        .context("response missing settings")
}

/// Applies `settings` under `update_mask`; an empty mask writes every field.
async fn update_settings(
    h: &Harness,
    master_key: &str,
    settings: pb::TenantSettings,
    mask: &str,
) -> anyhow::Result<pb::TenantSettings> {
    let update_mask = if mask.is_empty() {
        Vec::new()
    } else {
        vec![mask.to_owned()]
    };
    let response = tenant_client(h, master_key)?
        .update_tenant_settings(pb::UpdateTenantSettingsRequest {
            settings: settings.into(),
            update_mask,
            ..Default::default()
        })
        .await
        .map_err(|err| anyhow::anyhow!("UpdateTenantSettings failed: {err}"))?
        .into_owned();
    response
        .settings
        .into_option()
        .context("UpdateTenantSettings response missing settings")
}

/// Builds a `TenantService` client for `master_key`, using the harness's base
/// URL and the same transport the other clients use.
fn tenant_client(
    h: &Harness,
    master_key: &str,
) -> anyhow::Result<pb::TenantServiceClient<connectrpc::client::HttpClient>> {
    let uri: http::Uri = h
        .url("")
        .parse()
        .context("the harness base url must parse as a URI")?;
    let transport = connectrpc::client::HttpClient::plaintext_http2_only();
    let config = connectrpc::client::ClientConfig::new(uri)
        .with_protocol(connectrpc::Protocol::Connect)
        .with_codec_format(connectrpc::CodecFormat::Json)
        .with_default_header("authorization", format!("Bearer {master_key}"));
    Ok(pb::TenantServiceClient::new(transport, config))
}

/// Creates a `remote_local` input for `bytes` as Tenant 1, returning its id
/// and the upload URL `CreateInputArtifact` issued.
async fn create_input(h: &Harness, bytes: &[u8]) -> anyhow::Result<(uuid::Uuid, String)> {
    let response = h
        .generation_client(&h.tenant1.master_key)
        .create_input_artifact(pb::CreateInputArtifactRequest {
            manifest: pb::ArtifactManifest {
                size_bytes: u64::try_from(bytes.len())?,
                digest_sha256: ContentHash::digest(bytes).to_hex(),
                kind: pb::MediaKind::MEDIA_KIND_IMAGE.into(),
                mime_type: "image/png".to_owned(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .await
        .map_err(|err| anyhow::anyhow!("CreateInputArtifact failed: {err}"))?
        .into_owned();
    Ok((response.artifact_id.parse()?, response.upload_url))
}

/// Sends `body` to `upload_url`, authenticated with `master_key` when given,
/// and returns the response status.
async fn put_bytes(
    h: &Harness,
    upload_url: &str,
    master_key: Option<&str>,
    body: Vec<u8>,
) -> anyhow::Result<reqwest::StatusCode> {
    let mut request = h
        .http
        .put(upload_url)
        .header("content-type", "application/octet-stream")
        .body(body);
    if let Some(key) = master_key {
        request = request.bearer_auth(key);
    }
    Ok(request.send().await?.status())
}

/// Uploads `bytes` to `upload_url` with `master_key`, requiring success.
async fn upload_bytes(
    h: &Harness,
    upload_url: &str,
    master_key: &str,
    bytes: &[u8],
) -> anyhow::Result<()> {
    let status = put_bytes(h, upload_url, Some(master_key), bytes.to_vec()).await?;
    anyhow::ensure!(status.is_success(), "upload failed: {status}");
    Ok(())
}

/// Requests teardown of the shared harness without interrupting active tests.
#[test]
fn harness_teardown_waits_for_active_uses() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(e2e_support::teardown_harness(h))
}
