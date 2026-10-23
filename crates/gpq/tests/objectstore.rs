//! End-to-end coverage of ADR 0008's S3-compatible object-store path: a real
//! `gpq remote serve` and real `gpq worker` pair from `e2e_support` wired to a
//! real `MinIO` container from `objectstore_support`, with a protocol-level
//! `SyntheticWorker` standing in for execution.
//!
//! PostgreSQL and `MinIO` fixtures are shared while test access guards are
//! active. Teardown waits for current cases, and later access creates fresh
//! Remote/Worker and `MinIO` fixtures.
//!
//! Each test's doc comment names the ADR invariant it defends.

#[expect(
    dead_code,
    reason = "shared harness compiled into every integration-test binary; each suite \
              uses the subset it needs"
)]
mod e2e_support;
#[path = "safety/native_artifact_safety.rs"]
mod native_artifact_safety;
mod objectstore_support;

use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use connectrpc::ErrorCode;
use e2e_support::{
    Harness, HarnessOptions, SyntheticWorker, block_on, image_request, run_background, wait_until,
};
use gpq_domain::{ContentHash, ModelFamily};
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use objectstore_support::ObjectStoreFixture;
use uuid::Uuid;

/// The default `max_input_artifact_bytes` a freshly created Tenant gets
/// (`crates/gpq-remote/src/native/tenant.rs`), used to synthesize a manifest
/// that exceeds it without allocating that many real bytes.
const DEFAULT_MAX_INPUT_ARTIFACT_BYTES: u64 = 256 * 1024 * 1024;

struct RegistryState<T> {
    current: Option<Arc<FixtureEntry<T>>>,
    building: bool,
}

struct Registry<T> {
    state: Mutex<RegistryState<T>>,
    changed: Condvar,
}
type WithObjectStore = (Harness, ObjectStoreFixture);

struct FixtureEntry<T> {
    resources: T,
    active_uses: AtomicUsize,
    closing: AtomicBool,
    cleanup_started: AtomicBool,
    idle: tokio::sync::Notify,
    cleanup: fn(Arc<FixtureEntry<T>>),
}

struct FixtureUse<T>(Arc<FixtureEntry<T>>);

impl<T> Drop for FixtureUse<T> {
    fn drop(&mut self) {
        if self.0.active_uses.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
            if RETIRE_FIXTURES.load(Ordering::Acquire) {
                (self.0.cleanup)(self.0.clone());
            }
        }
    }
}

struct WithObjectStoreHarness(FixtureUse<WithObjectStore>);
struct ObjectStoreUse(FixtureUse<WithObjectStore>);
struct WithoutObjectStoreHarness(FixtureUse<Harness>);

impl Deref for WithObjectStoreHarness {
    type Target = Harness;

    fn deref(&self) -> &Self::Target {
        &self.0.0.resources.0
    }
}

impl Deref for ObjectStoreUse {
    type Target = ObjectStoreFixture;

    fn deref(&self) -> &Self::Target {
        &self.0.0.resources.1
    }
}

impl Deref for WithoutObjectStoreHarness {
    type Target = Harness;

    fn deref(&self) -> &Self::Target {
        &self.0.0.resources
    }
}

static WITH_OBJECT_STORE: LazyLock<Registry<WithObjectStore>> = LazyLock::new(|| Registry {
    state: Mutex::new(RegistryState {
        current: None,
        building: false,
    }),
    changed: Condvar::new(),
});
static WITHOUT_OBJECT_STORE: LazyLock<Registry<Harness>> = LazyLock::new(|| Registry {
    state: Mutex::new(RegistryState {
        current: None,
        building: false,
    }),
    changed: Condvar::new(),
});
static RETIRE_FIXTURES: AtomicBool = AtomicBool::new(false);

fn acquire<T: Send + Sync + 'static>(
    registry: &'static Registry<T>,
    cleanup: fn(Arc<FixtureEntry<T>>),
    initialize: impl FnOnce() -> T,
) -> FixtureUse<T> {
    let mut state = registry
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    loop {
        if let Some(entry) = state.current.as_ref()
            && !entry.closing.load(Ordering::Acquire)
        {
            entry.active_uses.fetch_add(1, Ordering::Relaxed);
            return FixtureUse(entry.clone());
        }
        if state.current.is_some() || state.building {
            state = registry
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
            continue;
        }

        state.building = true;
        drop(state);
        let outcome = catch_unwind(AssertUnwindSafe(initialize));
        let mut state = registry
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        state.building = false;
        match outcome {
            Ok(resources) => {
                let entry = Arc::new(FixtureEntry {
                    resources,
                    active_uses: AtomicUsize::new(1),
                    closing: AtomicBool::new(false),
                    cleanup_started: AtomicBool::new(false),
                    idle: tokio::sync::Notify::new(),
                    cleanup,
                });
                state.current = Some(entry.clone());
                registry.changed.notify_all();
                return FixtureUse(entry);
            }
            Err(payload) => {
                registry.changed.notify_all();
                drop(state);
                resume_unwind(payload);
            }
        }
    }
}

fn build_with_object_store() -> WithObjectStore {
    block_on(async {
        let store = match ObjectStoreFixture::start().await {
            Ok(store) => store,
            Err(err) => panic!("failed to start the MinIO test fixture: {err:?}"),
        };
        let options = HarnessOptions {
            extra_remote_env: store.extra_remote_env(),
        };
        let harness = match Harness::build_with(options).await {
            Ok(harness) => harness,
            Err(err) => panic!("failed to build the object-store gpq remote harness: {err:?}"),
        };
        (harness, store)
    })
}

fn harness() -> WithObjectStoreHarness {
    WithObjectStoreHarness(acquire(
        &WITH_OBJECT_STORE,
        schedule_with_object_store_cleanup,
        build_with_object_store,
    ))
}

fn object_store() -> ObjectStoreUse {
    ObjectStoreUse(acquire(
        &WITH_OBJECT_STORE,
        schedule_with_object_store_cleanup,
        build_with_object_store,
    ))
}

fn harness_without_object_store() -> WithoutObjectStoreHarness {
    WithoutObjectStoreHarness(acquire(
        &WITHOUT_OBJECT_STORE,
        schedule_without_object_store_cleanup,
        || {
            block_on(async {
                match Harness::build_with(HarnessOptions::default()).await {
                    Ok(harness) => harness,
                    Err(err) => {
                        panic!("failed to build the no-object-store gpq remote harness: {err:?}")
                    }
                }
            })
        },
    ))
}

fn schedule_with_object_store_cleanup(entry: Arc<FixtureEntry<WithObjectStore>>) {
    let should_spawn = {
        let registry = WITH_OBJECT_STORE
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !registry
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
            || entry.active_uses.load(Ordering::Acquire) != 0
        {
            return;
        }
        entry.closing.store(true, Ordering::Release);
        WITH_OBJECT_STORE.changed.notify_all();
        entry
            .cleanup_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    };
    if should_spawn {
        run_background(async move {
            if let Err(error) = cleanup_with_object_store(entry).await {
                eprintln!("failed to clean up object-store test fixtures: {error:#}");
            }
        });
    }
}

fn schedule_without_object_store_cleanup(entry: Arc<FixtureEntry<Harness>>) {
    let should_spawn = {
        let registry = WITHOUT_OBJECT_STORE
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !registry
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
            || entry.active_uses.load(Ordering::Acquire) != 0
        {
            return;
        }
        entry.closing.store(true, Ordering::Release);
        WITHOUT_OBJECT_STORE.changed.notify_all();
        entry
            .cleanup_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    };
    if should_spawn {
        run_background(async move {
            if let Err(error) = cleanup_without_object_store(entry).await {
                eprintln!("failed to clean up no-object-store harness: {error:#}");
            }
        });
    }
}

async fn wait_until_unused<T: Sync>(entry: &FixtureEntry<T>) {
    loop {
        let notified = entry.idle.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if entry.active_uses.load(Ordering::Acquire) == 0 {
            return;
        }
        notified.await;
    }
}

async fn cleanup_with_object_store(
    entry: Arc<FixtureEntry<WithObjectStore>>,
) -> anyhow::Result<()> {
    wait_until_unused(&entry).await;
    let harness_result = entry.resources.0.teardown().await;
    let store_result = entry.resources.1.teardown().await;
    clear_registry(&WITH_OBJECT_STORE, &entry);
    harness_result?;
    store_result
}

async fn cleanup_without_object_store(entry: Arc<FixtureEntry<Harness>>) -> anyhow::Result<()> {
    wait_until_unused(&entry).await;
    let result = entry.resources.teardown().await;
    clear_registry(&WITHOUT_OBJECT_STORE, &entry);
    result
}

fn clear_registry<T>(registry: &'static Registry<T>, entry: &Arc<FixtureEntry<T>>) {
    let mut state = registry
        .state
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if state
        .current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, entry))
    {
        state.current = None;
    }
    registry.changed.notify_all();
}

async fn teardown_with_object_store(
    harness: WithObjectStoreHarness,
    store: ObjectStoreUse,
) -> anyhow::Result<()> {
    RETIRE_FIXTURES.store(true, Ordering::Release);
    let entry = harness.0.0.clone();
    {
        let state = WITH_OBJECT_STORE
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            entry.closing.store(true, Ordering::Release);
            entry.cleanup_started.store(true, Ordering::Release);
        }
    }
    drop(store);
    drop(harness);
    cleanup_with_object_store(entry).await
}

async fn teardown_without_object_store(harness: WithoutObjectStoreHarness) -> anyhow::Result<()> {
    RETIRE_FIXTURES.store(true, Ordering::Release);
    let entry = harness.0.0.clone();
    {
        let state = WITHOUT_OBJECT_STORE
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if state
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            entry.closing.store(true, Ordering::Release);
            entry.cleanup_started.store(true, Ordering::Release);
        }
    }
    drop(harness);
    cleanup_without_object_store(entry).await
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

/// Submits an image Generation on `worker`'s alias that references
/// `artifact_id` as its only reference input.
async fn submit_with_reference(
    h: &Harness,
    worker: &SyntheticWorker,
    artifact_id: Uuid,
) -> anyhow::Result<pb::Generation> {
    let mut request = image_request(&worker.alias, "use the reference");
    if let Some(pb::submit_request::GenerationParameters::ImageParameters(params)) =
        request.generation_parameters.as_mut()
    {
        params.reference_artifact_ids = vec![artifact_id.to_string()];
    }
    h.submit(request)
        .await
        .map_err(|err| anyhow::anyhow!("native Submit failed: {err}"))
}

/// Polls until the Generation `generation_id` reaches `SUCCEEDED`.
async fn wait_succeeded(h: &Harness, generation_id: &str) -> anyhow::Result<()> {
    wait_until(
        || async {
            let g = h.get_generation(generation_id).await?;
            Ok((g.state == pb::GenerationState::GENERATION_STATE_SUCCEEDED).then_some(()))
        },
        Duration::from_secs(20),
    )
    .await
    .context("the generation never succeeded")
}

/// `CreateInputArtifact` presigns a `PUT` whose expiry matches
/// `GPQ_S3_PRESIGN_TTL_SECS`; uploading through it stores exactly the declared
/// bytes at the Artifact's durable object-store key, immediately retrievable
/// directly from the bucket with no further Remote round-trip (ADR 0008:
/// "Native input Artifacts use S3-compatible ephemeral placement").
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

/// A Native `Submit` with object-store output placement gives the Worker a
/// per-attempt presigned upload target. After the Worker reports that object,
/// the authenticated artifact endpoint redirects to its bytes exactly once.
#[test]
fn native_submit_with_object_store_output_redirects_once_to_uploaded_bytes() -> anyhow::Result<()> {
    let h = harness();
    block_on(async {
        let mut worker = SyntheticWorker::spawn(&h, "os-output", ModelFamily::QwenImage21).await?;
        let mut request = image_request(&worker.alias, "store generated output");
        request.output_placement = pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE.into();
        let submitted = h.submit(request).await?;
        let generation_id = submitted.generation_id.clone();

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
        let bytes = b"native object-store output bytes";
        let upload = h
            .http
            .put(&lease.output_upload_url)
            .header("content-type", "application/octet-stream")
            .body(bytes.to_vec())
            .send()
            .await?;
        anyhow::ensure!(
            upload.status().is_success(),
            "presigned output PUT failed: {}",
            upload.status()
        );

        worker
            .send(wpb::AttemptResult {
                attempt_id: lease.attempt_id.clone(),
                outputs: vec![wpb::AttemptOutput {
                    manifest: manifest_for(bytes).into(),
                    placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE.into(),
                    object_key: lease.output_object_key.clone(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        wait_succeeded(&h, &generation_id).await?;

        let succeeded = h.get_generation(&generation_id).await?;
        anyhow::ensure!(
            succeeded.output_artifacts.len() == 1,
            "output artifacts: {:?}",
            succeeded.output_artifacts
        );
        let artifact = &succeeded.output_artifacts[0];

        let no_redirect = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let response = no_redirect
            .get(artifact.download_path.as_str())
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == reqwest::StatusCode::FOUND,
            "artifact endpoint did not redirect: {}",
            response.status()
        );
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .context("artifact redirect has no Location header")?
            .to_str()?;
        let presigned_url = url::Url::parse(location)?;
        let downloaded = h
            .http
            .get(presigned_url)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        anyhow::ensure!(
            downloaded.as_ref() == bytes,
            "downloaded object-store output differs from the uploaded bytes"
        );

        let consumed = no_redirect
            .get(artifact.download_path.as_str())
            .bearer_auth(&h.tenant1.master_key)
            .send()
            .await?;
        anyhow::ensure!(
            consumed.status() == reqwest::StatusCode::GONE,
            "artifact remained downloadable after its one-shot redirect: {}",
            consumed.status()
        );
        Ok(())
    })
}

/// `CreateInputArtifact` rejects a manifest declaring more bytes than the
/// Tenant's `max_input_artifact_bytes` before ever presigning an upload (ADR
/// 0006: Tenant-configurable Artifact limits).
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
            err.code == ErrorCode::InvalidArgument,
            "unexpected error code: {:?}",
            err.code
        );
        Ok(())
    })
}

/// A Native `Submit` naming an object-store input Artifact reaches the Worker
/// with the input's presigned `download_url` and no bytes routed through
/// Remote (ADR 0008: "Native input Artifacts use S3-compatible ephemeral
/// placement so queued work can move between Workers"). The Worker's own
/// download of that URL returns exactly the uploaded bytes.
#[test]
fn native_submit_with_object_store_input_reaches_worker_via_presigned_url() -> anyhow::Result<()> {
    let h = harness();
    block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "os-presigned", ModelFamily::QwenImage21).await?;
        let bytes = b"reference material fetched straight from object storage".to_vec();
        let (artifact_id, _object_key) = create_and_upload_input(&h, &bytes).await?;

        let generation = submit_with_reference(&h, &worker, artifact_id).await?;
        let generation_id = generation.generation_id.clone();
        let lease = worker
            .recv_lease(Duration::from_secs(30))
            .await
            .context("the synthetic worker never received a LeaseAssignment")?;
        let input = lease
            .inputs
            .iter()
            .find(|input| input.artifact_id == artifact_id.to_string())
            .context("the lease does not name the object-store input")?;
        anyhow::ensure!(
            input.placement.as_known()
                == Some(pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE),
            "unexpected input placement: {:?}",
            input.placement
        );
        anyhow::ensure!(!input.download_url.is_empty(), "no presigned download url");

        let downloaded = h
            .http
            .get(&input.download_url)
            .send()
            .await?
            .error_for_status()
            .context("the presigned download url was rejected")?
            .bytes()
            .await?;
        anyhow::ensure!(
            downloaded.as_ref() == bytes.as_slice(),
            "downloaded input differs"
        );

        worker.start_attempt(&lease.attempt_id).await?;
        worker
            .complete_attempt(
                &lease.attempt_id,
                b"used the object store input",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        wait_succeeded(&h, &generation_id).await
    })
}

/// Input Artifacts are deleted from both `PostgreSQL` and the bucket the moment
/// their Generation reaches a terminal state, not lazily and not by the expiry
/// sweep (ADR 0008: "Inputs are deleted when the Generation terminates").
#[test]
fn input_artifact_deleted_on_generation_terminal_state() -> anyhow::Result<()> {
    let h = harness();
    let store = object_store();
    block_on(async {
        let mut worker =
            SyntheticWorker::spawn(&h, "os-terminal", ModelFamily::QwenImage21).await?;
        let bytes = b"input purged once the generation terminates".to_vec();
        let (artifact_id, object_key) = create_and_upload_input(&h, &bytes).await?;

        let generation = submit_with_reference(&h, &worker, artifact_id).await?;
        let lease = worker
            .recv_lease(Duration::from_secs(30))
            .await
            .context("the synthetic worker never received a LeaseAssignment")?;
        worker.start_attempt(&lease.attempt_id).await?;
        worker
            .complete_attempt(
                &lease.attempt_id,
                b"terminal cleanup check",
                pb::MediaKind::MEDIA_KIND_IMAGE,
                "image/png",
            )
            .await?;
        wait_succeeded(&h, &generation.generation_id).await?;

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
/// `expired` and deletes its bucket object (ADR 0008: "unclaimed outputs expire
/// one hour after completion"). `db::artifacts::expire_due` treats every
/// object-store-placed Artifact identically regardless of direction, so an
/// input Artifact with `expires_at` backdated through the admin pool exercises
/// exactly the sweep path a real unclaimed output would, without sleeping an
/// hour.
#[test]
fn expiry_sweep_transitions_and_deletes_past_due_object_store_artifact() -> anyhow::Result<()> {
    let h = harness();
    let store = object_store();
    block_on(async {
        let bytes = b"never claimed before it expires".to_vec();
        let (artifact_id, object_key) = create_and_upload_input(&h, &bytes).await?;

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

/// Without object storage configured, `CreateInputArtifact` falls back to a
/// `remote_local` input whose upload URL is on this Remote, a native `Submit`
/// requesting object-store output placement fails admission with
/// `FailedPrecondition`, and readiness is unaffected (ADR 0008: "S3
/// configuration is optional and does not affect Remote readiness").
#[test]
fn object_storage_absent_falls_back_to_remote_local_and_refuses_object_outputs()
-> anyhow::Result<()> {
    let h = harness_without_object_store();
    block_on(async {
        let readyz = h.http.get(h.url("/readyz")).send().await?;
        anyhow::ensure!(readyz.status().is_success(), "readyz: {}", readyz.status());

        let bytes = b"stored by remote itself".to_vec();
        let response = h
            .generation_client(&h.tenant1.master_key)
            .create_input_artifact(pb::CreateInputArtifactRequest {
                manifest: manifest_for(&bytes).into(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("CreateInputArtifact failed without S3: {err}"))?
            .into_owned();
        anyhow::ensure!(
            response.upload_url.starts_with(&h.url("/v1/artifacts/")),
            "upload url is not on this Remote: {}",
            response.upload_url
        );

        let worker = SyntheticWorker::spawn(&h, "os-absent", ModelFamily::QwenImage21).await?;
        let mut request = image_request(&worker.alias, "no s3");
        request.output_placement = pb::ArtifactPlacement::ARTIFACT_PLACEMENT_OBJECT_STORE.into();
        let Err(err) = h.submit(request).await else {
            anyhow::bail!("object-store output was admitted without object storage configured");
        };
        anyhow::ensure!(
            err.code == ErrorCode::FailedPrecondition,
            "unexpected error code: {:?}",
            err.code
        );
        Ok(())
    })
}

/// Requests cleanup; active test guards keep all three fixtures alive.
#[test]
fn fixture_teardown_waits_for_active_uses() -> anyhow::Result<()> {
    let without = harness_without_object_store();
    let with = harness();
    let store = object_store();
    block_on(async {
        teardown_without_object_store(without).await?;
        teardown_with_object_store(with, store).await
    })
}
