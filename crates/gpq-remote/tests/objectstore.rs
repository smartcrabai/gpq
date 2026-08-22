//! End-to-end coverage of ADR 0008's S3-compatible object-store path: a real
//! `gpq-remote`/`gpq-worker` pair from `e2e_support` wired to a real `MinIO`
//! container from `objectstore_support`.
//!
//! Every test shares one `testcontainers`-managed `PostgreSQL` container
//! (from `e2e_support`) and one `testcontainers`-managed `MinIO` container
//! (from `objectstore_support`), both started lazily on first use. A second,
//! independent Remote/Worker pair with no `GPQ_S3_*` configuration at all
//! covers ADR 0008's "S3 is optional" tests. `zzz_teardown_harnesses` — named
//! to sort last — tears down both Harnesses and the `MinIO` container so
//! `cargo test -p gpq-remote --test objectstore -- --test-threads=1` leaves
//! nothing running behind it.
//!
//! Each test's doc comment names the ADR invariant it defends.

#[expect(
    dead_code,
    reason = "shared harness compiled into every integration-test binary; each suite \
              uses the subset it needs"
)]
mod e2e_support;
mod objectstore_support;

use std::sync::LazyLock;
use std::time::Duration;

use anyhow::Context;
use buffa_types::google::protobuf::Struct;
use chrono::{DateTime, Utc};
use e2e_support::fake_llama::FakeMode;
use e2e_support::{Harness, HarnessOptions, block_on, wait_until};
use gpq_domain::hash::ContentHash;
use gpq_proto::gpq::v1 as pb;
use objectstore_support::ObjectStoreFixture;
use serde_json::json;
use uuid::Uuid;

/// The default `max_input_artifact_bytes` a freshly created Tenant gets
/// (`crates/gpq-remote/src/native/tenant.rs`), used to synthesize a manifest
/// that exceeds it without allocating that many real bytes.
const DEFAULT_MAX_INPUT_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;

static WITH_OBJECT_STORE: LazyLock<(Harness, ObjectStoreFixture)> = LazyLock::new(|| {
    block_on(async {
        let store = match ObjectStoreFixture::start().await {
            Ok(store) => store,
            Err(err) => panic!("failed to start the MinIO test fixture: {err:?}"),
        };
        let options = HarnessOptions {
            extra_remote_env: store.extra_remote_env(),
            ..HarnessOptions::default()
        };
        let harness = match Harness::build_with(options).await {
            Ok(harness) => harness,
            Err(err) => panic!("failed to build the object-store gpq-remote harness: {err:?}"),
        };
        (harness, store)
    })
});

static WITHOUT_OBJECT_STORE: LazyLock<Harness> = LazyLock::new(|| {
    block_on(async {
        match Harness::build_with(HarnessOptions::default()).await {
            Ok(harness) => harness,
            Err(err) => panic!("failed to build the no-object-store gpq-remote harness: {err:?}"),
        }
    })
});

fn harness() -> &'static Harness {
    &WITH_OBJECT_STORE.0
}

fn object_store() -> &'static ObjectStoreFixture {
    &WITH_OBJECT_STORE.1
}

fn harness_without_object_store() -> &'static Harness {
    &WITHOUT_OBJECT_STORE
}

/// Builds the opaque OpenAI-shaped `parameters` a Native `Submit` against
/// the llama.cpp Model alias needs (ADR 0007 keeps this payload opaque).
fn chat_parameters(message: &str) -> anyhow::Result<Struct> {
    Ok(serde_json::from_value(serde_json::json!({
        "messages": [{"role": "user", "content": message}],
    }))?)
}

/// A wire `ArtifactManifest` describing `bytes` as an opaque binary blob.
fn manifest_for(bytes: &[u8]) -> pb::ArtifactManifest {
    pb::ArtifactManifest {
        size_bytes: bytes.len() as u64,
        digest_sha256: ContentHash::digest(bytes).to_hex(),
        kind: pb::MediaKind::MEDIA_KIND_BINARY.into(),
        mime_type: "application/octet-stream".to_owned(),
        ..Default::default()
    }
}

/// Creates an object-store input Artifact for `bytes` on `h` (as Tenant 1),
/// uploads them through the returned presigned URL, and returns the
/// Artifact's id and its object-store key.
async fn create_and_upload_input(h: &Harness, bytes: &[u8]) -> anyhow::Result<(Uuid, String)> {
    let response = h
        .generation_client(&h.tenant1.master_key)
        .create_input_artifact(pb::CreateInputArtifactRequest {
            manifest: manifest_for(bytes).into(),
            ..Default::default()
        })
        .await
        .map_err(|err| anyhow::anyhow!("CreateInputArtifact failed: {err}"))?
        .into_owned();

    let put = h
        .http
        .put(&response.upload_url)
        .header("content-type", "application/octet-stream")
        .body(bytes.to_vec())
        .send()
        .await?;
    anyhow::ensure!(
        put.status().is_success(),
        "presigned PUT failed: {}",
        put.status()
    );

    let artifact_id: Uuid = response.artifact_id.parse()?;
    let object_key: String =
        sqlx::query_scalar("SELECT object_key FROM artifacts WHERE tenant_id = $1 AND id = $2")
            .bind(h.tenant1.id)
            .bind(artifact_id)
            .fetch_one(h.admin_pool())
            .await
            .context("reading the created input artifact's object_key")?;

    Ok((artifact_id, object_key))
}

/// `CreateInputArtifact` presigns a `PUT` whose expiry matches
/// `GPQ_S3_PRESIGN_TTL_SECS`; uploading through it stores exactly the
/// declared bytes at the Artifact's durable object-store key, immediately
/// retrievable directly from the bucket with no further Remote round-trip
/// (ADR 0008: "Native input Artifacts use S3-compatible ephemeral
/// placement").
#[test]
fn create_input_artifact_presigns_upload_and_is_downloadable() -> anyhow::Result<()> {
    let h = harness();
    let store = object_store();
    block_on(async {
        let bytes = b"artifact upload payload for ADR 0008".to_vec();
        let before = Utc::now();
        let response = h
            .generation_client(&h.tenant1.master_key)
            .create_input_artifact(pb::CreateInputArtifactRequest {
                manifest: manifest_for(&bytes).into(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("CreateInputArtifact failed: {err}"))?
            .into_owned();

        let expires_at: DateTime<Utc> = response
            .upload_url_expires_at
            .into_option()
            .context("response missing upload_url_expires_at")?
            .try_into()
            .map_err(|err| anyhow::anyhow!("invalid upload_url_expires_at: {err}"))?;
        let expected =
            before + chrono::Duration::seconds(objectstore_support::PRESIGN_TTL_SECS.cast_signed());
        let drift_secs = (expires_at - expected).num_seconds().abs();
        anyhow::ensure!(
            drift_secs <= 10,
            "expiry {expires_at} too far from the expected {expected} (GPQ_S3_PRESIGN_TTL_SECS={})",
            objectstore_support::PRESIGN_TTL_SECS
        );

        let put = h
            .http
            .put(&response.upload_url)
            .header("content-type", "application/octet-stream")
            .body(bytes.clone())
            .send()
            .await?;
        anyhow::ensure!(
            put.status().is_success(),
            "presigned PUT failed: {}",
            put.status()
        );

        let artifact_id: Uuid = response.artifact_id.parse()?;
        let (object_key, placement): (String, String) = sqlx::query_as(
            "SELECT object_key, placement FROM artifacts WHERE tenant_id = $1 AND id = $2",
        )
        .bind(h.tenant1.id)
        .bind(artifact_id)
        .fetch_one(h.admin_pool())
        .await?;
        anyhow::ensure!(
            placement == "object_store",
            "unexpected placement: {placement}"
        );

        let stored = store
            .client
            .get_object()
            .bucket(&store.bucket)
            .key(&object_key)
            .send()
            .await
            .context("downloading the uploaded artifact directly from the bucket")?;
        let stored_bytes = stored.body.collect().await?.into_bytes();
        anyhow::ensure!(
            stored_bytes.as_ref() == bytes.as_slice(),
            "stored bytes differ from the uploaded bytes"
        );
        Ok(())
    })
}

/// `CreateInputArtifact` rejects a manifest declaring more bytes than the
/// Tenant's `max_input_artifact_bytes` before ever presigning an upload
/// (ADR 0006: Tenant-configurable Artifact limits).
#[test]
fn create_input_artifact_rejects_oversized_manifest() -> anyhow::Result<()> {
    let h = harness();
    block_on(async {
        let oversized_manifest = pb::ArtifactManifest {
            size_bytes: DEFAULT_MAX_INPUT_ARTIFACT_BYTES + 1,
            digest_sha256: ContentHash::digest(b"oversized").to_hex(),
            kind: pb::MediaKind::MEDIA_KIND_BINARY.into(),
            mime_type: "application/octet-stream".to_owned(),
            ..Default::default()
        };
        let result = h
            .generation_client(&h.tenant1.master_key)
            .create_input_artifact(pb::CreateInputArtifactRequest {
                manifest: oversized_manifest.into(),
                ..Default::default()
            })
            .await;
        let Err(err) = result else {
            anyhow::bail!("an oversized manifest was not rejected");
        };
        anyhow::ensure!(
            err.code == connectrpc::ErrorCode::InvalidArgument,
            "unexpected error code: {:?}",
            err.code
        );
        Ok(())
    })
}

/// A Native `Submit` naming an object-store input Artifact reaches the
/// Worker, which downloads it directly from object storage via the
/// presigned `download_url` in its `LeaseInput` rather than
/// `WorkerTransferService::FetchArtifact` — that RPC explicitly rejects any
/// non-inline-relay input (`crates/gpq-remote/src/transfer.rs`), so a
/// successful Generation is only possible if the Worker used the URL (ADR
/// 0008: "Native input Artifacts use S3-compatible ephemeral placement so
/// queued work can move between Workers").
#[test]
fn native_submit_with_object_store_input_reaches_worker_via_presigned_url() -> anyhow::Result<()> {
    let h = harness();
    block_on(async {
        h.fake
            .set_mode(FakeMode::reply("used the object store input"));

        let bytes = b"reference material fetched straight from object storage".to_vec();
        let (artifact_id, _object_key) = create_and_upload_input(h, &bytes).await?;

        let request = pb::SubmitRequest {
            target: Some(pb::submit_request::Target::ModelAlias(
                h.model_alias.clone(),
            )),
            parameters: chat_parameters("hi")?.into(),
            input_artifact_ids: vec![artifact_id.to_string()],
            output_placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
            ..Default::default()
        };
        let generation = h
            .submit_client(&h.tenant1.master_key)
            .submit(request)
            .await
            .map_err(|err| anyhow::anyhow!("native Submit failed: {err}"))?
            .into_owned()
            .generation
            .into_option()
            .context("Submit response missing generation")?;

        let succeeded = wait_until(
            || async {
                let g = h.native_get_generation(&generation.generation_id).await?;
                Ok((g.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED).then_some(g))
            },
            Duration::from_secs(15),
        )
        .await
        .context("generation with an object-store input never succeeded")?;
        anyhow::ensure!(
            succeeded
                .output_text
                .contains("used the object store input"),
            "unexpected output_text: {}",
            succeeded.output_text
        );
        Ok(())
    })
}

/// Input Artifacts are deleted from both `PostgreSQL` and the bucket the
/// moment their Generation reaches a terminal state, not lazily and not by
/// the expiry sweep (ADR 0008: "Inputs are deleted when the Generation
/// terminates").
#[test]
fn input_artifact_deleted_on_generation_terminal_state() -> anyhow::Result<()> {
    let h = harness();
    let store = object_store();
    block_on(async {
        h.fake.set_mode(FakeMode::reply("terminal cleanup check"));

        let bytes = b"input purged once the generation terminates".to_vec();
        let (artifact_id, object_key) = create_and_upload_input(h, &bytes).await?;

        let request = pb::SubmitRequest {
            target: Some(pb::submit_request::Target::ModelAlias(
                h.model_alias.clone(),
            )),
            parameters: chat_parameters("hi")?.into(),
            input_artifact_ids: vec![artifact_id.to_string()],
            output_placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
            ..Default::default()
        };
        let generation = h
            .submit_client(&h.tenant1.master_key)
            .submit(request)
            .await
            .map_err(|err| anyhow::anyhow!("native Submit failed: {err}"))?
            .into_owned()
            .generation
            .into_option()
            .context("Submit response missing generation")?;

        wait_until(
            || async {
                let g = h.native_get_generation(&generation.generation_id).await?;
                Ok((g.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED).then_some(()))
            },
            Duration::from_secs(15),
        )
        .await
        .context("generation never succeeded")?;

        wait_until(
            || async {
                let exists: Option<i64> =
                    sqlx::query_scalar("SELECT 1 FROM artifacts WHERE tenant_id = $1 AND id = $2")
                        .bind(h.tenant1.id)
                        .bind(artifact_id)
                        .fetch_optional(h.admin_pool())
                        .await?;
                Ok(exists.is_none().then_some(()))
            },
            Duration::from_secs(10),
        )
        .await
        .context("input artifact row was not deleted at the generation's terminal state")?;

        wait_until(
            || async {
                let missing = store
                    .client
                    .head_object()
                    .bucket(&store.bucket)
                    .key(&object_key)
                    .send()
                    .await
                    .is_err();
                Ok(missing.then_some(()))
            },
            Duration::from_secs(10),
        )
        .await
        .context("input artifact bytes were not deleted from the bucket")?;
        Ok(())
    })
}

/// The generic object-store expiry sweep transitions a past-due Artifact to
/// `expired` and deletes its bucket object (ADR 0008: "unclaimed outputs
/// expire one hour after completion"). `db::artifacts::expire_due` treats
/// every object-store-placed Artifact identically regardless of input/output
/// direction, so an input Artifact with its `expires_at` backdated through
/// the admin pool — standing in for what `record_output` would have set —
/// exercises exactly the same sweep path a real unclaimed output would,
/// without sleeping an hour.
#[test]
fn expiry_sweep_transitions_and_deletes_past_due_object_store_artifact() -> anyhow::Result<()> {
    let h = harness();
    let store = object_store();
    block_on(async {
        let bytes = b"never claimed before it expires".to_vec();
        let (artifact_id, object_key) = create_and_upload_input(h, &bytes).await?;

        sqlx::query(
            "UPDATE artifacts SET expires_at = now() - interval '1 minute' \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(h.tenant1.id)
        .bind(artifact_id)
        .execute(h.admin_pool())
        .await
        .context("backdating the artifact's expires_at")?;

        wait_until(
            || async {
                let state: String = sqlx::query_scalar(
                    "SELECT state FROM artifacts WHERE tenant_id = $1 AND id = $2",
                )
                .bind(h.tenant1.id)
                .bind(artifact_id)
                .fetch_one(h.admin_pool())
                .await?;
                Ok((state == "expired").then_some(()))
            },
            Duration::from_secs(10),
        )
        .await
        .context("past-due artifact was never swept to expired")?;

        wait_until(
            || async {
                let missing = store
                    .client
                    .head_object()
                    .bucket(&store.bucket)
                    .key(&object_key)
                    .send()
                    .await
                    .is_err();
                Ok(missing.then_some(()))
            },
            Duration::from_secs(10),
        )
        .await
        .context("expired artifact's bytes were never deleted from the bucket")?;
        Ok(())
    })
}

/// Without S3 configured, `CreateInputArtifact` fails `FailedPrecondition`
/// and a Native `Submit` requesting object-store output placement fails
/// admission the same explicit way, while ordinary text-only chat
/// completions and `/readyz` are unaffected (ADR 0008: "S3 configuration is
/// optional and does not affect Remote readiness").
#[test]
fn object_storage_absent_fails_explicitly_without_blocking_readiness() -> anyhow::Result<()> {
    let h = harness_without_object_store();
    block_on(async {
        let readyz = h.http.get(h.url("/readyz")).send().await?;
        anyhow::ensure!(readyz.status().is_success(), "readyz: {}", readyz.status());

        let bytes = b"unreachable without object storage".to_vec();
        let create_result = h
            .generation_client(&h.tenant1.master_key)
            .create_input_artifact(pb::CreateInputArtifactRequest {
                manifest: manifest_for(&bytes).into(),
                ..Default::default()
            })
            .await;
        let Err(create_err) = create_result else {
            anyhow::bail!("CreateInputArtifact succeeded without object storage configured");
        };
        anyhow::ensure!(
            create_err.code == connectrpc::ErrorCode::FailedPrecondition,
            "unexpected error code: {:?}",
            create_err.code
        );

        let submit_result = h
            .submit_client(&h.tenant1.master_key)
            .submit(pb::SubmitRequest {
                target: Some(pb::submit_request::Target::ModelAlias(
                    h.model_alias.clone(),
                )),
                parameters: chat_parameters("hi")?.into(),
                output_placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE.into(),
                ..Default::default()
            })
            .await;
        let Err(submit_err) = submit_result else {
            anyhow::bail!(
                "Submit with object-store output placement succeeded without object storage configured"
            );
        };
        anyhow::ensure!(
            submit_err.code == connectrpc::ErrorCode::FailedPrecondition,
            "unexpected error code: {:?}",
            submit_err.code
        );

        h.fake
            .set_mode(FakeMode::reply("text-only chat still works"));
        let chat = h
            .http
            .post(h.url("/v1/chat/completions"))
            .bearer_auth(&h.tenant1.master_key)
            .json(&json!({
                "model": h.model_alias,
                "messages": [{"role": "user", "content": "hi"}],
                "stream": false,
            }))
            .send()
            .await?;
        anyhow::ensure!(
            chat.status().is_success(),
            "chat completion failed without object storage configured: {}",
            chat.status()
        );
        let body: serde_json::Value = chat.json().await?;
        anyhow::ensure!(
            body["choices"][0]["message"]["content"] == "text-only chat still works",
            "body: {body}"
        );
        Ok(())
    })
}

/// Tears down both Harnesses and the `MinIO` container. Named to sort last.
#[test]
fn zzz_teardown_harnesses() -> anyhow::Result<()> {
    block_on(async {
        Harness::teardown(harness_without_object_store()).await?;
        Harness::teardown(harness()).await?;
        object_store().teardown().await?;
        Ok(())
    })
}
