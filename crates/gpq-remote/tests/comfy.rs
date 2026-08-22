//! End-to-end tests for the `ComfyUI` image modality (ADR 0005, ADR 0007,
//! ADR 0018), worker-local output Artifact delivery (ADR 0008), and
//! object-store output placement (ADR 0006, ADR 0008).
//!
//! This binary boots its own `gpq-remote`/`gpq-worker` pair with a `ComfyUI`
//! `Pool` pointed at [`comfy_support::FakeComfy`], and its own `MinIO` fixture
//! for the object-store scenario, sharing nothing with `e2e.rs` or the
//! sibling `objectstore.rs`/`lifecycle.rs` suites beyond the read-only
//! `e2e_support`/`objectstore_support` harness code.
//!
//! Every test's doc comment names the ADR invariant it defends.

mod comfy_support;
#[path = "e2e_support/mod.rs"]
#[expect(
    dead_code,
    reason = "shared harness compiled into every integration-test binary; each suite \
              uses the subset it needs"
)]
mod e2e_support;
#[path = "objectstore_support/mod.rs"]
mod objectstore_support;

use std::collections::BTreeMap;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::Context;
use buffa_types::google::protobuf::Struct;
use comfy_support::FakeComfy;
use e2e_support::{Harness, HarnessOptions, PoolKind, wait_until};
use gpq_proto::gpq::v1 as pb;
use objectstore_support::ObjectStoreFixture;
use uuid::Uuid;

struct Fixtures {
    harness: Harness,
    fake: FakeComfy,
    object_store: ObjectStoreFixture,
}

static FIXTURES: LazyLock<Fixtures> = LazyLock::new(|| e2e_support::block_on(build_fixtures()));

async fn build_fixtures() -> Fixtures {
    match try_build_fixtures().await {
        Ok(fixtures) => fixtures,
        Err(err) => panic!("failed to build the comfy e2e fixtures: {err:?}"),
    }
}

async fn try_build_fixtures() -> anyhow::Result<Fixtures> {
    let fake = FakeComfy::spawn()
        .await
        .context("starting the fake ComfyUI")?;
    let object_store = ObjectStoreFixture::start()
        .await
        .context("starting the MinIO fixture")?;
    let harness = Harness::build_with(HarnessOptions {
        extra_remote_env: object_store.extra_remote_env(),
        pool_kind: PoolKind::ComfyUi,
        pool_base_url: Some(fake.base_url().to_owned()),
    })
    .await
    .context("building the ComfyUI harness")?;
    Ok(Fixtures {
        harness,
        fake,
        object_store,
    })
}

fn harness() -> &'static Harness {
    &FIXTURES.harness
}

fn fake() -> &'static FakeComfy {
    &FIXTURES.fake
}

fn object_store() -> &'static ObjectStoreFixture {
    &FIXTURES.object_store
}

/// Registers a Workflow Version for `graph` (always declaring
/// [`comfy_support::OUTPUT_NODE`]/[`comfy_support::OUTPUT_NAME`] as an image
/// output) with `required_custom_nodes`, aliases it under a name derived
/// from `prefix`, and returns the registered version plus that alias.
async fn register_workflow(
    h: &Harness,
    prefix: &str,
    graph: serde_json::Value,
    required_custom_nodes: BTreeMap<String, String>,
) -> anyhow::Result<(pb::WorkflowVersion, String)> {
    let graph_struct: Struct = serde_json::from_value(graph)
        .context("building the workflow graph as a protobuf Struct")?;
    let manifest = pb::WorkflowManifest {
        output_node: comfy_support::OUTPUT_NODE.to_owned(),
        output_name: comfy_support::OUTPUT_NAME.to_owned(),
        artifact_kind: pb::MediaKind::MEDIA_KIND_IMAGE.into(),
        artifact_mime: "image/png".to_owned(),
        required_custom_nodes: required_custom_nodes.into_iter().collect(),
        ..Default::default()
    };
    let request = pb::RegisterWorkflowVersionRequest {
        graph: graph_struct.into(),
        manifest: manifest.into(),
        modality: pb::Modality::MODALITY_IMAGE.into(),
        ..Default::default()
    };
    let response = h
        .catalog_client(&h.tenant1.master_key)
        .register_workflow_version(request)
        .await
        .map_err(|err| anyhow::anyhow!("RegisterWorkflowVersion failed: {err}"))?
        .into_owned();
    let version = response
        .version
        .into_option()
        .context("RegisterWorkflowVersion response missing the registered version")?;

    let alias = format!("{prefix}-{}", &version.content_sha256[..8]);
    h.catalog_client(&h.tenant1.master_key)
        .set_workflow_alias(pb::SetWorkflowAliasRequest {
            alias: alias.clone(),
            content_sha256: version.content_sha256.clone(),
            ..Default::default()
        })
        .await
        .map_err(|err| anyhow::anyhow!("SetWorkflowAlias failed: {err}"))?;
    Ok((version, alias))
}

/// Submits a durable Native Generation against a Workflow alias with an
/// explicit output placement, as Tenant 1.
async fn submit_workflow(
    h: &Harness,
    alias: &str,
    placement: pb::ArtifactPlacement,
) -> anyhow::Result<pb::Generation> {
    let request = pb::SubmitRequest {
        target: Some(pb::submit_request::Target::WorkflowAlias(alias.to_owned())),
        output_placement: placement.into(),
        ..Default::default()
    };
    let response = h
        .submit_client(&h.tenant1.master_key)
        .submit(request)
        .await
        .map_err(|err| anyhow::anyhow!("native Submit failed: {err}"))?
        .into_owned();
    response
        .generation
        .into_option()
        .context("Submit response missing generation")
}

async fn wait_for_state(
    h: &Harness,
    generation_id: &str,
    state: pb::GenerationState,
    timeout: Duration,
) -> anyhow::Result<pb::Generation> {
    wait_until(
        || async {
            let generation = h.native_get_generation(generation_id).await?;
            Ok((generation.state == state).then_some(generation))
        },
        timeout,
    )
    .await
}

/// `Submit` on a Workflow alias derives modality `image` from the pinned
/// Workflow Version, pins that Version's hash, and succeeds with an output
/// Artifact whose manifest digest equals the SHA-256 of the bytes the fake
/// served on `/view` (ADR 0007, ADR 0012).
#[test]
fn submit_image_workflow_derives_modality_and_hashes_output_artifact() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let (version, alias) = register_workflow(
            h,
            "img-success",
            comfy_support::success_graph(),
            BTreeMap::new(),
        )
        .await?;
        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;
        anyhow::ensure!(
            generation.modality == pb::Modality::MODALITY_IMAGE,
            "modality: {:?}",
            generation.modality
        );

        let succeeded = wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(15),
        )
        .await?;
        anyhow::ensure!(
            succeeded.version_sha256 == version.content_sha256,
            "version_sha256: {} != {}",
            succeeded.version_sha256,
            version.content_sha256
        );

        let artifact = succeeded
            .output_artifacts
            .first()
            .context("expected one output artifact")?
            .clone();
        let manifest = artifact
            .manifest
            .into_option()
            .context("artifact missing a manifest")?;
        anyhow::ensure!(
            manifest.digest_sha256 == comfy_support::image_digest_hex(),
            "digest_sha256: {} != {}",
            manifest.digest_sha256,
            comfy_support::image_digest_hex()
        );
        anyhow::ensure!(
            manifest.size_bytes == comfy_support::IMAGE_BYTES.len() as u64,
            "size_bytes: {}",
            manifest.size_bytes
        );
        Ok(())
    })
}

/// `WatchGeneration` reports progress events sourced from the fake's
/// `progress` WebSocket frames (ADR 0006).
#[test]
fn watch_generation_streams_comfy_progress_events() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let (_version, alias) = register_workflow(
            h,
            "img-progress",
            comfy_support::success_graph(),
            BTreeMap::new(),
        )
        .await?;
        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;
        let mut stream = h.native_watch_generation(&generation.generation_id).await?;

        let mut saw_progress = false;
        let mut saw_terminal = false;
        for _ in 0..256_u32 {
            let Some(item) = stream
                .message::<pb::GenerationEvent>()
                .await
                .map_err(|err| anyhow::anyhow!("watch stream error: {err}"))?
            else {
                break;
            };
            match item.to_owned_message().event {
                Some(pb::generation_event::Event::Progress(progress)) => {
                    if progress.fraction > 0.0 {
                        saw_progress = true;
                    }
                }
                Some(pb::generation_event::Event::StateChanged(changed))
                    if changed.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED =>
                {
                    saw_terminal = true;
                    break;
                }
                _ => {}
            }
        }
        anyhow::ensure!(saw_progress, "never observed a nonzero progress event");
        anyhow::ensure!(saw_terminal, "never observed the terminal succeeded event");
        Ok(())
    })
}

/// A Workflow whose manifest requires a custom node the fake never reports
/// in `/object_info` is never leased to any Slot — the scheduler's
/// capability match excludes it before an Attempt is ever created — so it
/// creates no Attempt at all, and `RegisterWorkflowVersion` already reports
/// it unavailable (ADR 0018, ADR 0003: "Capability mismatches discovered
/// before execution do not create Attempts").
#[test]
fn workflow_missing_custom_node_creates_no_attempt() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let required = BTreeMap::from([("ComfyUI-Missing-Pack".to_owned(), "1.0.0".to_owned())]);
        let (version, alias) = register_workflow(
            h,
            "img-missing-node",
            comfy_support::success_graph(),
            required,
        )
        .await?;
        anyhow::ensure!(
            !version.available,
            "expected the version to be unavailable: {version:?}"
        );

        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;
        let generation_uuid = Uuid::parse_str(&generation.generation_id)?;

        tokio::time::sleep(Duration::from_secs(3)).await;
        let row = h
            .generation_row(h.tenant1.id, generation_uuid)
            .await?
            .context("generation row missing")?;
        anyhow::ensure!(row.state == "queued", "state: {}", row.state);
        let attempts = h.attempt_rows(h.tenant1.id, generation_uuid).await?;
        anyhow::ensure!(attempts.is_empty(), "attempts: {attempts:?}");
        Ok(())
    })
}

/// A Workflow whose graph fails before it can produce output settles its
/// Generation `failed` after exactly one Attempt, with the `FailureKind`
/// matching how the fake rejected it: a graph naming a checkpoint the fake
/// does not resolve fails its Attempt as `ModelUnavailable`, which is
/// neither retryable nor requires another candidate to exhaust (ADR 0012,
/// ADR 0003); an `execution_error` carrying an out-of-memory exception
/// fails its Attempt as `OutOfMemory`, which marks the executing Pool
/// incapable of that Workflow Version, so with the harness's single —
/// now incapable — Pool as the only candidate, the Generation fails
/// outright rather than retrying forever (ADR 0003).
#[test]
fn unexecutable_workflow_graphs_fail_the_attempt_and_generation() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        for (prefix, graph, failure_kind) in [
            (
                "img-model-unavailable",
                comfy_support::model_unavailable_graph(),
                pb::FailureKind::FAILURE_KIND_MODEL_UNAVAILABLE,
            ),
            (
                "img-oom",
                comfy_support::oom_graph(),
                pb::FailureKind::FAILURE_KIND_OUT_OF_MEMORY,
            ),
        ] {
            let (_version, alias) = register_workflow(h, prefix, graph, BTreeMap::new()).await?;
            let generation = submit_workflow(
                h,
                &alias,
                pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
            )
            .await?;

            let failed = wait_for_state(
                h,
                &generation.generation_id,
                pb::GenerationState::GENERATION_STATE_FAILED,
                Duration::from_secs(15),
            )
            .await?;
            let failure = failed
                .failure
                .into_option()
                .context("expected a Failure on the generation")?;
            anyhow::ensure!(failure.kind == failure_kind, "failure: {failure:?}");
            anyhow::ensure!(
                failed.attempt_count == 1,
                "attempt_count: {}",
                failed.attempt_count
            );
        }
        Ok(())
    })
}

/// Cancelling a running image Generation reaches the Worker as `POST
/// /interrupt`; the fake observes it and reports `execution_interrupted`,
/// and the Generation settles `cancelled` (ADR 0003, ADR 0006).
#[test]
fn interrupt_cancels_running_image_generation() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let (_version, alias) =
            register_workflow(h, "img-hang", comfy_support::hang_graph(), BTreeMap::new()).await?;
        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;

        wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_RUNNING,
            Duration::from_secs(10),
        )
        .await?;

        let interrupts_before = fake().interrupted_count();
        h.generation_client(&h.tenant1.master_key)
            .cancel_generation(pb::CancelGenerationRequest {
                generation_id: generation.generation_id.clone(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("CancelGeneration failed: {err}"))?;

        wait_until(
            || async { Ok((fake().interrupted_count() > interrupts_before).then_some(())) },
            Duration::from_secs(15),
        )
        .await
        .context("fake ComfyUI never observed the interrupt")?;

        wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_CANCELLED,
            Duration::from_secs(20),
        )
        .await?;
        Ok(())
    })
}

/// A Worker-local output Artifact streams the exact bytes the fake served
/// on `/view`, then ends `consumed`: a second download attempt is `410
/// Gone` (ADR 0008's one-shot delivery contract).
#[test]
fn worker_local_artifact_delivers_once_then_gone() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let (_version, alias) = register_workflow(
            h,
            "img-delivery",
            comfy_support::success_graph(),
            BTreeMap::new(),
        )
        .await?;
        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;
        let succeeded = wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(15),
        )
        .await?;
        let artifact_id = succeeded
            .output_artifacts
            .first()
            .context("expected one output artifact")?
            .artifact_id
            .clone();

        let first = h
            .http
            .get(h.url(&format!("/v1/artifacts/{artifact_id}")))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(first.status().is_success(), "status {}", first.status());
        let bytes = first.bytes().await?;
        anyhow::ensure!(
            bytes.as_ref() == comfy_support::IMAGE_BYTES,
            "downloaded bytes did not match the bytes the fake served"
        );

        // `mark_consumed` commits asynchronously, slightly after the last
        // byte reaches this client, so poll rather than assume it already
        // landed the instant `.bytes()` resolves.
        wait_until(
            || async {
                let second = h
                    .http
                    .get(h.url(&format!("/v1/artifacts/{artifact_id}")))
                    .bearer_auth(&h.tenant1.master_key)
                    .send()
                    .await?;
                Ok((second.status() == reqwest::StatusCode::GONE).then_some(()))
            },
            Duration::from_secs(5),
        )
        .await
        .context("expected 410 Gone on redownload once delivery finished consuming")?;
        Ok(())
    })
}

/// A download started while an earlier download of the same Artifact is
/// still in flight is rejected `409 Conflict` rather than racing it (ADR
/// 0008).
#[test]
fn worker_local_artifact_download_conflicts_while_in_flight() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let (_version, alias) = register_workflow(
            h,
            "img-conflict",
            comfy_support::success_graph(),
            BTreeMap::new(),
        )
        .await?;
        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;
        let succeeded = wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(15),
        )
        .await?;
        let artifact_id = succeeded
            .output_artifacts
            .first()
            .context("expected one output artifact")?
            .artifact_id
            .clone();

        // `send()` resolves once Remote answers the response head, which it
        // does only after marking the Artifact `delivering` — so by the
        // time this returns, a concurrent second request must already
        // observe the conflict, with no sleep or race needed.
        let first = h
            .http
            .get(h.url(&format!("/v1/artifacts/{artifact_id}")))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(first.status().is_success(), "status {}", first.status());

        let second = h
            .http
            .get(h.url(&format!("/v1/artifacts/{artifact_id}")))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(
            second.status() == reqwest::StatusCode::CONFLICT,
            "status {}",
            second.status()
        );

        let bytes = first.bytes().await?;
        anyhow::ensure!(
            bytes.as_ref() == comfy_support::IMAGE_BYTES,
            "the in-flight first download's bytes did not match"
        );
        Ok(())
    })
}

/// A download whose producing Worker is offline answers `503`, a
/// retryable outcome distinct from every terminal Artifact state (ADR
/// 0008).
#[test]
fn worker_local_artifact_download_offline_worker_is_retryable() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let (_version, alias) = register_workflow(
            h,
            "img-offline",
            comfy_support::success_graph(),
            BTreeMap::new(),
        )
        .await?;
        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;
        let succeeded = wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(15),
        )
        .await?;
        let artifact_id = succeeded
            .output_artifacts
            .first()
            .context("expected one output artifact")?
            .artifact_id
            .clone();

        anyhow::ensure!(h.kill_worker().await?, "expected a running worker to kill");

        // Wait for Remote to actually observe the disconnect before
        // downloading: `GET /v1/artifacts/{id}` on a `WorkerLocal` output
        // transitions state via `begin_delivery` the instant it still
        // believes the Worker is online, so polling too early can race a
        // stale "online" read into a terminal `Lost` instead of a stable,
        // idempotent `WorkerOffline` classification.
        wait_until(
            || async {
                let response = h
                    .catalog_client(&h.tenant1.master_key)
                    .list_workers(pb::ListWorkersRequest::default())
                    .await
                    .map_err(|err| anyhow::anyhow!("ListWorkers failed: {err}"))?
                    .into_owned();
                let offline = response.workers.first().is_none_or(|worker| !worker.online);
                Ok(offline.then_some(()))
            },
            Duration::from_secs(10),
        )
        .await
        .context("Remote never observed the worker go offline")?;

        let offline_result = wait_until(
            || async {
                let response = h
                    .http
                    .get(h.url(&format!("/v1/artifacts/{artifact_id}")))
                    .bearer_auth(&h.tenant1.master_key)
                    .send()
                    .await?;
                Ok((response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE).then_some(()))
            },
            Duration::from_secs(15),
        )
        .await;

        // Restore the harness for every later test regardless of outcome.
        h.restart_worker().context("restarting the worker")?;
        wait_until(|| h.pool_is_ready(true), Duration::from_secs(30))
            .await
            .context("the Pool never became ready again after restart")?;

        offline_result.context("expected 503 while the producing worker was offline")?;
        Ok(())
    })
}

/// An image Generation placed in object storage lands its output object in
/// the bucket, `GET /v1/artifacts/{id}` redirects to a presigned URL
/// serving the exact bytes, and the Artifact ends `consumed` (ADR 0006,
/// ADR 0008).
#[test]
fn object_store_output_placement_lands_in_bucket_and_redirects() -> anyhow::Result<()> {
    let h = harness();
    let store = object_store();
    e2e_support::block_on(async {
        let (_version, alias) = register_workflow(
            h,
            "img-objectstore",
            comfy_support::success_graph(),
            BTreeMap::new(),
        )
        .await?;
        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE,
        )
        .await?;
        let succeeded = wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(15),
        )
        .await?;
        let artifact = succeeded
            .output_artifacts
            .first()
            .context("expected one output artifact")?
            .clone();
        let manifest = artifact
            .manifest
            .into_option()
            .context("artifact missing a manifest")?;
        anyhow::ensure!(
            manifest.digest_sha256 == comfy_support::image_digest_hex(),
            "digest_sha256: {} != {}",
            manifest.digest_sha256,
            comfy_support::image_digest_hex()
        );

        let listed = store
            .client
            .list_objects_v2()
            .bucket(&store.bucket)
            .send()
            .await
            .context("listing the MinIO bucket")?;
        let expected_size = i64::try_from(comfy_support::IMAGE_BYTES.len())
            .context("image byte length does not fit an i64")?;
        let landed = listed
            .contents()
            .iter()
            .any(|object| object.size() == Some(expected_size));
        anyhow::ensure!(
            landed,
            "expected the output object in the bucket: {listed:?}"
        );

        let no_redirect = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let response = no_redirect
            .get(h.url(&format!("/v1/artifacts/{}", artifact.artifact_id)))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::FOUND,
            "status {}",
            response.status()
        );
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .context("missing Location header")?
            .to_str()?
            .to_owned();

        let presigned = reqwest::Client::new().get(&location).send().await?;
        anyhow::ensure!(
            presigned.status().is_success(),
            "presigned GET status {}",
            presigned.status()
        );
        let bytes = presigned.bytes().await?;
        anyhow::ensure!(
            bytes.as_ref() == comfy_support::IMAGE_BYTES,
            "bytes served through the presigned URL did not match"
        );

        let second = h
            .http
            .get(h.url(&format!("/v1/artifacts/{}", artifact.artifact_id)))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(
            second.status() == reqwest::StatusCode::GONE,
            "expected 410 Gone once the object-store artifact is consumed, got {}",
            second.status()
        );
        Ok(())
    })
}

/// Tears down the comfy harness: stops the `gpq-remote`/`gpq-worker` child
/// processes, drops the per-run database, and reaps both the `PostgreSQL`
/// and `MinIO` containers. Named to sort after every `*_...` test above so it
/// runs last under `--test-threads=1`, whether or not earlier tests failed.
#[test]
fn zzz_teardown_comfy_harness() -> anyhow::Result<()> {
    e2e_support::block_on(async {
        harness().teardown().await?;
        object_store().teardown().await
    })
}
