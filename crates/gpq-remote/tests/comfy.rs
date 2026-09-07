//! End-to-end tests for `ComfyUI` registered image Workflows and raw prompts
//! (ADR 0005, ADR 0007, ADR 0018), worker-local output Artifact delivery (ADR
//! 0008), and object-store output placement (ADR 0006, ADR 0008).
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
use base64::Engine as _;
use buffa_types::google::protobuf::Struct;
use comfy_support::FakeComfy;
use e2e_support::{Harness, HarnessOptions, PoolKind, wait_until};
use futures::StreamExt;
use gpq_proto::gpq::v1 as pb;
use objectstore_support::ObjectStoreFixture;
use sha2::{Digest, Sha256};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
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
    register_workflow_with_models(h, prefix, graph, required_custom_nodes, Vec::new()).await
}

async fn register_workflow_with_models(
    h: &Harness,
    prefix: &str,
    graph: serde_json::Value,
    required_custom_nodes: BTreeMap<String, String>,
    required_models: Vec<String>,
) -> anyhow::Result<(pb::WorkflowVersion, String)> {
    let graph_struct: Struct = serde_json::from_value(graph)
        .context("building the workflow graph as a protobuf Struct")?;
    let manifest = pb::WorkflowManifest {
        output_node: comfy_support::OUTPUT_NODE.to_owned(),
        output_name: comfy_support::OUTPUT_NAME.to_owned(),
        artifact_kind: pb::MediaKind::MEDIA_KIND_IMAGE.into(),
        artifact_mime: "image/png".to_owned(),
        required_model_sha256: required_models,
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

/// A Workflow Version's required model hash is admitted and revalidated
/// through the real Native Catalog/Submit APIs before `ComfyUI` executes it
/// (ADR 0007, ADR 0012).
#[test]
fn submit_workflow_with_manifest_model_pin_succeeds() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let model_hash = gpq_domain::ContentHash::digest(e2e_support::FAKE_MODEL_BYTES).to_hex();
        let (version, alias) = register_workflow_with_models(
            h,
            "img-model-pin",
            comfy_support::success_graph(),
            BTreeMap::new(),
            vec![model_hash],
        )
        .await?;
        anyhow::ensure!(
            version.available,
            "model-pinned version unavailable: {version:?}"
        );

        let generation = submit_workflow(
            h,
            &alias,
            pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL,
        )
        .await?;
        let succeeded = match wait_for_state(
            h,
            &generation.generation_id,
            pb::GenerationState::GENERATION_STATE_SUCCEEDED,
            Duration::from_secs(15),
        )
        .await
        {
            Ok(generation) => generation,
            Err(error) => {
                let current = h.native_get_generation(&generation.generation_id).await?;
                anyhow::bail!(
                    "model-pinned generation did not succeed: {error}; current={current:?}"
                )
            }
        };
        anyhow::ensure!(
            succeeded.version_sha256 == version.content_sha256,
            "version_sha256: {} != {}",
            succeeded.version_sha256,
            version.content_sha256
        );
        Ok(())
    })
}

/// `POST /v1/images/generations` accepts the `OpenAI` image shape used by
/// `@ai-sdk/openai-compatible`, binds portable prompt/size placeholders into
/// the Workflow graph, and returns the generated bytes as `data[].b64_json`.
#[test]
fn openai_image_generation_returns_sdk_compatible_base64_json() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let mut graph = comfy_support::success_graph();
        graph[comfy_support::OUTPUT_NODE]["inputs"]["prompt"] = serde_json::json!("$prompt");
        graph[comfy_support::OUTPUT_NODE]["inputs"]["width"] = serde_json::json!("$width");
        graph[comfy_support::OUTPUT_NODE]["inputs"]["height"] = serde_json::json!("$height");
        let (_version, alias) =
            register_workflow(h, "openai-image", graph, BTreeMap::new()).await?;

        let response = h
            .http
            .post(h.url("/v1/images/generations"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&serde_json::json!({
                "model": alias,
                "prompt": "a red panda astronaut",
                "n": 1,
                "size": "1024x768",
                "response_format": "b64_json"
            }))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "status {}: {}",
            response.status(),
            response.text().await?
        );
        let body: serde_json::Value = response.json().await?;
        let data = body["data"]
            .as_array()
            .context("image response missing data array")?;
        anyhow::ensure!(data.len() == 1, "body: {body}");
        let encoded = data[0]["b64_json"]
            .as_str()
            .context("image response missing b64_json")?;
        let decoded = base64::engine::general_purpose::STANDARD.decode(encoded)?;
        anyhow::ensure!(decoded == comfy_support::IMAGE_BYTES, "wrong image bytes");

        let submitted = fake()
            .last_prompt_graph()
            .context("fake ComfyUI did not receive a graph")?;
        let inputs = &submitted[comfy_support::OUTPUT_NODE]["inputs"];
        anyhow::ensure!(
            inputs["prompt"] == "a red panda astronaut",
            "graph: {submitted}"
        );
        anyhow::ensure!(
            inputs["width"].as_f64() == Some(1024.0),
            "graph: {submitted}"
        );
        anyhow::ensure!(
            inputs["height"].as_f64() == Some(768.0),
            "graph: {submitted}"
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

/// A Workflow requiring the exact version declared in the Worker TOML is
/// advertised as available and executes through the real public APIs (ADR
/// 0007, ADR 0018).
#[test]
fn workflow_with_configured_custom_node_succeeds() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        wait_until(|| h.worker_is_online(true), Duration::from_secs(30))
            .await
            .context("the Worker was not online before custom-node admission")?;
        wait_until(|| h.pool_is_ready(true), Duration::from_secs(30))
            .await
            .context("the Pool was not ready before custom-node admission")?;
        let required = BTreeMap::from([("SomePack".to_owned(), "1.0.0".to_owned())]);
        let (version, alias) = register_workflow(
            h,
            "img-custom-node",
            comfy_support::success_graph(),
            required,
        )
        .await?;
        anyhow::ensure!(
            version.available,
            "configured custom-node version was not advertised: {version:?}"
        );

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
        anyhow::ensure!(succeeded.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED);
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
        wait_until(|| h.worker_is_online(true), Duration::from_secs(30))
            .await
            .context("Remote never observed the restarted worker online")?;
        wait_until(|| h.pool_is_ready(true), Duration::from_secs(30))
            .await
            .context("the Pool never became ready again after restart")?;

        offline_result.context("expected 503 while the producing worker was offline")?;
        Ok(())
    })
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "covers authentication, tenant isolation, validation, and prompt-id conflict behavior"
)]
fn comfy_api_auth_and_client_isolation() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let unauthenticated = h.http.get(h.url("/prompt")).send().await?;
        anyhow::ensure!(
            unauthenticated.status() == reqwest::StatusCode::UNAUTHORIZED,
            "unauthenticated prompt status: {}",
            unauthenticated.status()
        );
        let unauthenticated_ws = h.http.get(h.url("/ws?clientId=auth-test")).send().await?;
        anyhow::ensure!(
            unauthenticated_ws.status() == reqwest::StatusCode::UNAUTHORIZED,
            "unauthenticated WebSocket status: {}",
            unauthenticated_ws.status()
        );

        let client_id = "same-client";
        let graph = serde_json::json!({
            "raw": {"class_type": "RawNode", "inputs": {}}
        });
        let prompt_id = Uuid::now_v7().to_string();
        let connect = |master_key: String| async move {
            let ws_url = h
                .url(&format!("/ws?clientId={client_id}"))
                .replacen("http://", "ws://", 1);
            let mut request = ws_url.into_client_request()?;
            request.headers_mut().insert(
                http::header::AUTHORIZATION,
                format!("Bearer {master_key}").parse()?,
            );
            let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
            let status = tokio::time::timeout(Duration::from_secs(5), socket.next())
                .await?
                .context("WebSocket closed before status")??;
            anyhow::ensure!(matches!(status, Message::Text(_)), "missing status frame");
            Ok::<_, anyhow::Error>(socket)
        };
        let mut tenant1_ws = connect(h.tenant1.master_key.clone()).await?;
        let mut tenant2_ws = connect(h.tenant2.master_key.clone()).await?;
        let body = serde_json::json!({
            "prompt": graph,
            "client_id": client_id,
            "prompt_id": prompt_id,
            "extra_data": {"tenant": "one"}
        });
        let accepted = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&body)
            .send()
            .await?;
        anyhow::ensure!(
            accepted.status().is_success(),
            "tenant 1 prompt: {}",
            accepted.status()
        );

        for _ in 0..15 {
            if let Ok(Some(Ok(Message::Text(text)))) =
                tokio::time::timeout(Duration::from_millis(100), tenant2_ws.next()).await
            {
                let frame: serde_json::Value = serde_json::from_str(&text)?;
                anyhow::ensure!(
                    !(frame["type"] == "execution_success"
                        && frame["data"]["prompt_id"] == prompt_id),
                    "tenant 2 received tenant 1 event: {frame}"
                );
            }
        }
        let mut tenant1_succeeded = false;
        for _ in 0..300 {
            let Ok(Some(frame)) =
                tokio::time::timeout(Duration::from_millis(100), tenant1_ws.next()).await
            else {
                continue;
            };
            let Message::Text(text) = frame? else {
                continue;
            };
            let frame: serde_json::Value = serde_json::from_str(&text)?;
            if frame["type"] == "execution_success" && frame["data"]["prompt_id"] == prompt_id {
                tenant1_succeeded = true;
                break;
            }
        }
        anyhow::ensure!(
            tenant1_succeeded,
            "tenant 1 did not receive its terminal event"
        );

        let conflict = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&body)
            .send()
            .await?;
        anyhow::ensure!(
            conflict.status() == reqwest::StatusCode::CONFLICT,
            "same-tenant duplicate status: {}",
            conflict.status()
        );
        let tenant2_accepted = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant2.master_key)
            .json(&body)
            .send()
            .await?;
        anyhow::ensure!(
            tenant2_accepted.status().is_success(),
            "same prompt id in another tenant: {}",
            tenant2_accepted.status()
        );

        let invalid = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&serde_json::json!({}))
            .send()
            .await?;
        anyhow::ensure!(
            invalid.status() == reqwest::StatusCode::BAD_REQUEST,
            "invalid prompt: {}",
            invalid.status()
        );
        let oversized = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant1.master_key)
            .body(vec![b'x'; 2 * 1024 * 1024 + 1])
            .send()
            .await?;
        anyhow::ensure!(
            oversized.status() == reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            "oversized prompt: {}",
            oversized.status()
        );
        Ok(())
    })
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "covers durable admission, restart recovery, and asynchronous failure events"
)]
fn comfy_api_durable_admission_and_async_failure() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        anyhow::ensure!(h.kill_worker().await?, "expected a running worker to stop");
        wait_until(
            || async {
                let response = h
                    .catalog_client(&h.tenant1.master_key)
                    .list_workers(pb::ListWorkersRequest::default())
                    .await
                    .map_err(|error| anyhow::anyhow!("ListWorkers failed: {error}"))?
                    .into_owned();
                Ok((response.workers.first().is_none_or(|worker| !worker.online)).then_some(()))
            },
            Duration::from_secs(10),
        )
        .await?;
        let queued_prompt_id = Uuid::now_v7().to_string();
        let queued_response = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&serde_json::json!({
                "prompt": {"queued": {"class_type": "RawNode", "inputs": {}}},
                "client_id": "durable-client",
                "prompt_id": queued_prompt_id
            }))
            .send()
            .await?;
        anyhow::ensure!(
            queued_response.status().is_success(),
            "durable POST: {}",
            queued_response.status()
        );
        let queued_history = h
            .http
            .get(h.url(&format!("/history/{queued_prompt_id}")))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        anyhow::ensure!(
            queued_history == serde_json::json!({}),
            "queued history was visible: {queued_history}"
        );

        h.restart_worker()?;
        wait_until(|| h.pool_is_ready(true), Duration::from_secs(30)).await?;
        let queued_done = wait_until(
            || async {
                let value = h
                    .http
                    .get(h.url(&format!("/history/{queued_prompt_id}")))
                    .bearer_auth(&h.tenant1.master_key)
                    .send()
                    .await?
                    .json::<serde_json::Value>()
                    .await?;
                Ok(value
                    .get(&queued_prompt_id)
                    .filter(|entry| entry["status"]["completed"] == true)
                    .cloned())
            },
            Duration::from_secs(30),
        )
        .await?;
        anyhow::ensure!(queued_done["status"]["status_str"] == "success");

        let failure_client = "failure-client";
        let failure_prompt_id = Uuid::now_v7().to_string();
        let ws_url = h
            .url(&format!("/ws?clientId={failure_client}"))
            .replacen("http://", "ws://", 1);
        let mut ws_request = ws_url.into_client_request()?;
        ws_request.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {}", h.tenant1.master_key).parse()?,
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(ws_request).await?;
        let status = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await?
            .context("failure WebSocket closed before status")??;
        anyhow::ensure!(
            matches!(status, Message::Text(_)),
            "failure WebSocket missing status"
        );
        let failure_response = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&serde_json::json!({
                "prompt": {"oom": {"class_type": comfy_support::OOM_NODE_CLASS, "inputs": {}}},
                "client_id": failure_client,
                "prompt_id": failure_prompt_id
            }))
            .send()
            .await?;
        anyhow::ensure!(
            failure_response.status().is_success(),
            "failure POST: {}",
            failure_response.status()
        );
        let mut saw_error = false;
        let mut saw_success = false;
        for _ in 0..300 {
            let Ok(frame) = tokio::time::timeout(Duration::from_millis(100), ws.next()).await
            else {
                continue;
            };
            let Some(frame) = frame else { break };
            let Message::Text(text) = frame? else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_str(&text)?;
            if value["data"]["prompt_id"] == failure_prompt_id {
                saw_error |= value["type"] == "execution_error";
                saw_success |= value["type"] == "execution_success";
            }
            if saw_error {
                break;
            }
        }
        anyhow::ensure!(saw_error, "raw failure did not reach execution_error");
        anyhow::ensure!(!saw_success, "raw failure emitted execution_success");
        let failed_history = wait_until(
            || async {
                let value = h
                    .http
                    .get(h.url(&format!("/history/{failure_prompt_id}")))
                    .bearer_auth(&h.tenant1.master_key)
                    .send()
                    .await?
                    .json::<serde_json::Value>()
                    .await?;
                Ok(value
                    .get(&failure_prompt_id)
                    .filter(|entry| entry["status"]["status_str"] == "error")
                    .cloned())
            },
            Duration::from_secs(30),
        )
        .await?;
        anyhow::ensure!(failed_history["outputs"] == serde_json::json!({}));
        Ok(())
    })
}
#[test]
#[expect(
    clippy::too_many_lines,
    reason = "covers raw graph preservation, mixed output metadata, deduplicated artifacts, and downloads"
)]
fn comfy_api_round_trip_preserves_mixed_outputs() -> anyhow::Result<()> {
    let h = harness();
    e2e_support::block_on(async {
        let client_id = "raw-client";
        let prompt_id = Uuid::now_v7().to_string();
        let graph = serde_json::json!({
            "node/one~two": {
                "class_type": "RawImageNode",
                "inputs": {"seed": 18_446_744_073_709_551_615_u64}
            },
            "audio/key~name": {
                "class_type": "RawAudioNode",
                "inputs": {"link": ["node/one~two", 0]}
            }
        });
        let ws_url = h
            .url(&format!("/ws?clientId={client_id}"))
            .replacen("http://", "ws://", 1);
        let mut ws_request = ws_url.into_client_request()?;
        ws_request.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {}", h.tenant1.master_key).parse()?,
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(ws_request).await?;
        let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await?
            .context("Comfy WebSocket closed before status")??;
        let Message::Text(first) = first else {
            anyhow::bail!("Comfy WebSocket did not send a text status frame")
        };
        let first: serde_json::Value = serde_json::from_str(&first)?;
        anyhow::ensure!(first["type"] == "status", "first frame: {first}");
        anyhow::ensure!(first["data"]["sid"] == client_id, "status sid: {first}");

        let response = h
            .http
            .post(h.url("/prompt"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&serde_json::json!({
                "prompt": graph.clone(),
                "client_id": client_id,
                "prompt_id": prompt_id,
                "extra_data": {
                    "custom": {"keep": true},
                    "auth_token_comfy_org": "must-not-be-public"
                },
                "partial_execution_targets": ["node/one~two"]
            }))
            .send()
            .await?;
        anyhow::ensure!(
            response.status().is_success(),
            "POST /prompt: {}",
            response.status()
        );
        let accepted: serde_json::Value = response.json().await?;
        anyhow::ensure!(accepted["prompt_id"] == prompt_id, "accepted: {accepted}");
        drop(accepted);

        let expected_graph = graph.clone();
        let observed_graph = wait_until(
            || {
                let expected_graph = expected_graph.clone();
                async move {
                    Ok(fake()
                        .last_prompt_graph()
                        .filter(|observed| observed == &expected_graph))
                }
            },
            Duration::from_secs(5),
        )
        .await
        .context("fake ComfyUI did not receive the raw prompt")?;
        anyhow::ensure!(
            observed_graph["node/one~two"]["inputs"]["seed"]
                == serde_json::json!(18_446_744_073_709_551_615_u64),
            "raw seed was changed: {observed_graph}"
        );
        anyhow::ensure!(
            observed_graph["audio/key~name"]["inputs"]["link"][1] == serde_json::json!(0),
            "raw link index was changed: {observed_graph}"
        );

        let mut saw_success = false;
        let mut saw_executed = false;
        for _ in 0..300 {
            let Ok(frame) = tokio::time::timeout(Duration::from_millis(100), ws.next()).await
            else {
                continue;
            };
            let Some(frame) = frame else { break };
            let frame = frame?;
            let Message::Text(text) = frame else { continue };
            let value: serde_json::Value = serde_json::from_str(&text)?;
            if value["type"] == "executed"
                && value["data"]["prompt_id"] == prompt_id
                && value["data"]["node"] == "node/one~two"
            {
                saw_executed = true;
            }
            if value["type"] == "execution_success" && value["data"]["prompt_id"] == prompt_id {
                saw_success = true;
                break;
            }
        }
        let debug_history = h
            .http
            .get(h.url(&format!("/history/{prompt_id}")))
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        eprintln!(
            "DEBUG history after websocket: {} {}",
            debug_history.status(),
            debug_history.text().await?
        );
        anyhow::ensure!(
            saw_success,
            "raw Comfy WebSocket never reached execution_success"
        );
        anyhow::ensure!(
            saw_executed,
            "raw Comfy WebSocket omitted the completed output node"
        );

        let history = wait_until(
            || async {
                let response = h
                    .http
                    .get(h.url(&format!("/history/{prompt_id}")))
                    .bearer_auth(&h.tenant1.master_key)
                    .send()
                    .await?;
                let value: serde_json::Value = response.json().await?;
                Ok(value
                    .get(&prompt_id)
                    .filter(|entry| entry["status"]["completed"] == true)
                    .cloned())
            },
            Duration::from_secs(30),
        )
        .await?;
        anyhow::ensure!(
            history["prompt"][3]["custom"]["keep"] == true,
            "custom extra_data was not retained: {history}"
        );
        anyhow::ensure!(
            history["prompt"][3].get("auth_token_comfy_org").is_none(),
            "Comfy credential leaked into history: {history}"
        );
        anyhow::ensure!(
            history["prompt"][2] == observed_graph,
            "history graph changed: {} != {observed_graph}",
            history["prompt"][2]
        );
        anyhow::ensure!(
            history["prompt"][1] == client_id,
            "history client_id changed: {history}"
        );

        let image = &history["outputs"]["node/one~two"]["images"][0];
        let video = &history["outputs"]["node/one~two"]["images"][1];
        let audio = &history["outputs"]["audio/key~name"]["audio/custom"][0];
        let duplicate = &history["outputs"]["audio/key~name"]["audio/custom"][1];
        let image_filename = image["filename"].as_str().context("image filename")?;
        let video_filename = video["filename"].as_str().context("video filename")?;
        let audio_filename = audio["filename"].as_str().context("audio filename")?;
        anyhow::ensure!(
            duplicate["filename"].as_str() == Some(image_filename),
            "duplicate file reference did not share a filename: {history}"
        );
        anyhow::ensure!(image["label"] == "image", "image custom metadata lost");
        anyhow::ensure!(video["label"] == "video", "video custom metadata lost");
        anyhow::ensure!(audio["label"] == "audio", "audio custom metadata lost");

        for (filename, expected, mime) in [
            (image_filename, comfy_support::IMAGE_BYTES, "image/png"),
            (video_filename, comfy_support::VIDEO_BYTES, "video/mp4"),
            (audio_filename, comfy_support::AUDIO_BYTES, "audio/flac"),
        ] {
            let response = h
                .http
                .get(h.url("/view"))
                .query(&[
                    ("filename", filename),
                    ("subfolder", ""),
                    ("type", "output"),
                ])
                .bearer_auth(&h.tenant1.master_key)
                .send()
                .await?;
            anyhow::ensure!(
                response.status().is_success(),
                "view {filename}: {}",
                response.status()
            );
            anyhow::ensure!(
                response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    == Some(mime),
                "view {filename} MIME mismatch"
            );
            let bytes = response.bytes().await?;
            anyhow::ensure!(bytes.as_ref() == expected, "view {filename} bytes mismatch");
            anyhow::ensure!(
                hex::encode(Sha256::digest(&bytes)) == hex::encode(Sha256::digest(expected)),
                "view {filename} digest mismatch"
            );
            wait_until(
                || async {
                    let second = h
                        .http
                        .get(h.url("/view"))
                        .query(&[("filename", filename), ("type", "output")])
                        .bearer_auth(&h.tenant1.master_key)
                        .send()
                        .await?;
                    Ok((second.status() == reqwest::StatusCode::GONE).then_some(()))
                },
                Duration::from_secs(5),
            )
            .await
            .with_context(|| format!("second view {filename} did not become Gone"))?;
        }
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
