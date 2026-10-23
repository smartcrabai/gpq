//! Shared end-to-end test harness: lazily starts a testcontainers-managed
//! PostgreSQL 18 container and shares a real Remote/Worker fixture while test
//! access guards are active. Teardown retires that generation after active
//! cases finish; later access builds a valid new fixture.
//!
//! Generations need an accelerator and model weights to execute, which a CI
//! host does not have. Tests that lease, run, fail, or fetch for a Generation
//! therefore drive a [`SyntheticWorker`] at the protocol layer instead. The real
//! Worker process still proves enrollment, session registration, revocation,
//! and restart against the real Remote.
//!
//! Coverage caveat: the Worker runs as a child process, and instrumented
//! binaries only flush their counters on a normal exit, while these suites
//! stop the Worker with signals. `cargo llvm-cov` therefore reports the
//! Worker's files at their unit-test coverage only. Judge Worker coverage by
//! the behavior these tests assert, not by that number.

pub mod cli;
pub mod db;
#[path = "../shared/container.rs"]
pub(crate) mod shared_container;
pub mod synthetic;

pub use db::{AttemptRow, GenerationRow};

use shared_container::{ContainerLease, maintenance_url, reap_shared_container};
pub use synthetic::SyntheticWorker;

use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, PoisonError};

use anyhow::{Context, bail};
use connectrpc::client::{ClientConfig, HttpClient};
use connectrpc::{CodecFormat, Protocol};
use gpq_proto::gpq::v1 as pb;
use rand::Rng;
use sqlx::PgPool;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::runtime::Runtime;
use uuid::Uuid;

const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Polls `attempt` until it resolves `Some`, or fails after `timeout`.
pub async fn wait_until<T, F, Fut>(mut attempt: F, timeout: Duration) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = attempt().await? {
            return Ok(value);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out after {timeout:?} waiting for a condition to become true");
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn random_hex(len_bytes: usize) -> String {
    let mut buf = vec![0_u8; len_bytes];
    rand::rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

fn free_port() -> anyhow::Result<u16> {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").context("binding an ephemeral port")?;
    Ok(listener.local_addr()?.port())
}

fn database_url_for(base: &url::Url, db_name: &str) -> url::Url {
    let mut url = base.clone();
    url.set_path(&format!("/{db_name}"));
    url
}

fn database_url_with_credentials(
    base: &url::Url,
    user: &str,
    password: &str,
) -> anyhow::Result<url::Url> {
    let mut url = base.clone();
    url.set_username(user)
        .map_err(|()| anyhow::anyhow!("failed to set the connection string username"))?;
    url.set_password(Some(password))
        .map_err(|()| anyhow::anyhow!("failed to set the connection string password"))?;
    Ok(url)
}

/// A `SubmitRequest` for an image Generation against `alias`, with the
/// deterministic seed the harness uses everywhere (below `i64::MAX`).
#[must_use]
pub fn image_request(alias: &str, prompt: &str) -> pb::SubmitRequest {
    pb::SubmitRequest {
        target: Some(pb::submit_request::Target::ModelAlias(alias.to_owned())),
        generation_parameters: Some(pb::submit_request::GenerationParameters::ImageParameters(
            Box::new(pb::ImageParameters {
                prompt: prompt.to_owned(),
                width: 512,
                height: 512,
                ..Default::default()
            }),
        )),
        output_placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
        seed: Some(42),
        ..Default::default()
    }
}

fn worker_config_toml(
    worker_name: &str,
    remote_base: &str,
    worker_state_dir: &Path,
    pool_state_dir: &Path,
    listen: &str,
) -> String {
    format!(
        "name = \"{worker_name}\"\n\
         remote_url = \"{remote_base}\"\n\
         state_dir = \"{worker_state_dir}\"\n\
         \n\
         [[pools]]\n\
         key = \"cpu0\"\n\
         backend = \"cpu\"\n\
         listen = \"{listen}\"\n\
         state_dir = \"{pool_state_dir}\"\n\
         startup_timeout_secs = 20\n",
        worker_state_dir = worker_state_dir.display(),
        pool_state_dir = pool_state_dir.display(),
    )
}

fn write_credential_file(dir: &Path, credential: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    // Match the destination name used by the installed systemd unit's
    // `LoadCredential=gpq-worker:...` directive.
    let path = dir.join("gpq-worker");
    std::fs::write(&path, credential)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

async fn wait_for_readyz(http: &reqwest::Client, remote_base: &str) -> anyhow::Result<()> {
    wait_until(
        || async {
            let ok = http
                .get(format!("{remote_base}/readyz"))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            Ok(ok.then_some(()))
        },
        Duration::from_secs(20),
    )
    .await
    .context("gpq remote never became ready")
}

/// Sends SIGTERM and gives the process `grace` to exit on its own (so
/// `gpq worker` can run its own graceful shutdown), falling back to SIGKILL.
async fn terminate_gracefully(child: &mut Child, grace: Duration) {
    let Some(pid) = child.id() else {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return;
    };
    let _ = Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .await;
    if tokio::time::timeout(grace, child.wait()).await.is_err() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

/// One Tenant's identity and live Master Key.
pub struct TenantFixture {
    pub id: Uuid,
    pub master_key: String,
}

/// One migrated database, a running Remote and real Worker, and two Tenants.
/// Tests in a binary share one generation until all access guards release it.
pub struct Harness {
    admin_pool: PgPool,
    maintenance_pool: PgPool,
    db_name: String,
    app_role: String,
    remote_base: String,
    remote_uri: http::Uri,
    native_transport: HttpClient,
    remote_child: Mutex<Option<Child>>,
    worker_child: Mutex<Option<Child>>,
    /// Everything needed to respawn `gpq worker run` exactly as it was first
    /// started.
    worker_spawn: WorkerSpawn,
    /// The `gpq` binary and the schema-owner environment its administration
    /// subcommands need (ADR 0016).
    admin: AdminCli,
    /// The real Worker's identity, for administration commands.
    worker_id: Uuid,
    worker_name: String,
    tmp_root: PathBuf,
    pub http: reqwest::Client,
    pub tenant1: TenantFixture,
    pub tenant2: TenantFixture,
    container_lease: tokio::sync::Mutex<Option<ContainerLease>>,
}

/// The `gpq` binary plus the schema-owner environment its administration
/// subcommands run with.
struct AdminCli {
    binary: PathBuf,
    env: Vec<(String, String)>,
}

/// The argv and environment the harness used to start `gpq worker run`.
struct WorkerSpawn {
    binary: PathBuf,
    config_path: PathBuf,
    credentials_dir: PathBuf,
}

/// Starts `gpq worker run` from a recorded spawn spec.
fn spawn_worker(spec: &WorkerSpawn) -> anyhow::Result<Child> {
    Command::new(&spec.binary)
        .arg("worker")
        .arg("run")
        .arg("--config")
        .arg(&spec.config_path)
        .env("CREDENTIALS_DIRECTORY", &spec.credentials_dir)
        .kill_on_drop(true)
        .spawn()
        .context("spawning gpq worker run")
}

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|err| panic!("failed to build the shared e2e test runtime: {err}"))
});

fn runtime() -> &'static Runtime {
    &RUNTIME
}

struct HarnessEntry {
    harness: Harness,
    active_uses: AtomicUsize,
    closing: AtomicBool,
    cleanup_started: AtomicBool,
    idle: tokio::sync::Notify,
}

struct HarnessRegistry {
    current: Option<Arc<HarnessEntry>>,
    building: bool,
}

static HARNESS_REGISTRY: LazyLock<(Mutex<HarnessRegistry>, Condvar)> = LazyLock::new(|| {
    (
        Mutex::new(HarnessRegistry {
            current: None,
            building: false,
        }),
        Condvar::new(),
    )
});
static RETIRE_HARNESSES: AtomicBool = AtomicBool::new(false);

/// One test's use of the shared Remote and Worker fixture.
pub struct HarnessAccess(Arc<HarnessEntry>);

impl Deref for HarnessAccess {
    type Target = Harness;

    fn deref(&self) -> &Self::Target {
        &self.0.harness
    }
}

impl Drop for HarnessAccess {
    fn drop(&mut self) {
        let last_use = self.0.active_uses.fetch_sub(1, Ordering::AcqRel) == 1;
        if last_use {
            self.0.idle.notify_waiters();
            schedule_retired_harness_cleanup(&self.0);
        }
    }
}

/// The shared harness, lazily built and kept alive while any test is using it.
pub fn harness() -> HarnessAccess {
    let (slot, changed) = &*HARNESS_REGISTRY;
    let mut registry = slot.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        if let Some(entry) = registry.current.as_ref()
            && !entry.closing.load(Ordering::Acquire)
        {
            entry.active_uses.fetch_add(1, Ordering::Relaxed);
            return HarnessAccess(entry.clone());
        }
        if registry.current.is_some() || registry.building {
            registry = changed
                .wait(registry)
                .unwrap_or_else(PoisonError::into_inner);
            continue;
        }

        registry.building = true;
        drop(registry);
        let built = catch_unwind(AssertUnwindSafe(|| runtime().block_on(Harness::build())));
        let mut registry = slot.lock().unwrap_or_else(PoisonError::into_inner);
        registry.building = false;
        match built {
            Ok(harness) => {
                let entry = Arc::new(HarnessEntry {
                    harness,
                    active_uses: AtomicUsize::new(1),
                    closing: AtomicBool::new(false),
                    cleanup_started: AtomicBool::new(false),
                    idle: tokio::sync::Notify::new(),
                });
                registry.current = Some(entry.clone());
                changed.notify_all();
                return HarnessAccess(entry);
            }
            Err(payload) => {
                changed.notify_all();
                drop(registry);
                resume_unwind(payload);
            }
        }
    }
}

/// Runs `fut` to completion on the shared runtime.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    runtime().block_on(fut)
}

/// Closes a test suite's shared harness after all active tests have released it.
///
/// # Errors
/// Returns an error if the harness or its container cannot be removed.
pub async fn teardown_harness(access: HarnessAccess) -> anyhow::Result<()> {
    RETIRE_HARNESSES.store(true, Ordering::Release);
    let entry = access.0.clone();
    {
        let (slot, _) = &*HARNESS_REGISTRY;
        let registry = slot.lock().unwrap_or_else(PoisonError::into_inner);
        if registry
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            entry.closing.store(true, Ordering::Release);
            entry.cleanup_started.store(true, Ordering::Release);
        }
    }
    drop(access);
    cleanup_harness_entry(entry).await
}

fn schedule_retired_harness_cleanup(entry: &Arc<HarnessEntry>) {
    if !RETIRE_HARNESSES.load(Ordering::Acquire) {
        return;
    }
    let should_spawn = {
        let (slot, changed) = &*HARNESS_REGISTRY;
        let registry = slot.lock().unwrap_or_else(PoisonError::into_inner);
        if !registry
            .current
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, entry))
            || entry.active_uses.load(Ordering::Acquire) != 0
        {
            return;
        }
        entry.closing.store(true, Ordering::Release);
        changed.notify_all();
        entry
            .cleanup_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    };
    if should_spawn {
        let entry = entry.clone();
        run_background(async move {
            if let Err(error) = cleanup_harness_entry(entry).await {
                eprintln!("failed to clean up retired e2e harness: {error:#}");
            }
        });
    }
}

async fn cleanup_harness_entry(entry: Arc<HarnessEntry>) -> anyhow::Result<()> {
    entry.wait_until_unused().await;
    let result = entry.harness.teardown().await;
    let (slot, changed) = &*HARNESS_REGISTRY;
    let mut registry = slot.lock().unwrap_or_else(PoisonError::into_inner);
    if registry
        .current
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &entry))
    {
        registry.current = None;
    }
    changed.notify_all();
    result
}

impl HarnessEntry {
    async fn wait_until_unused(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active_uses.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) fn run_background(task: impl Future<Output = ()> + Send + 'static) {
    if tokio::runtime::Handle::try_current().is_ok() {
        let _task = runtime().spawn(task);
    } else {
        runtime().block_on(task);
    }
}
impl Harness {
    /// Absolute URL of `path` on the running `gpq remote serve`.
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.remote_base)
    }

    /// The name the real Worker enrolled under.
    #[must_use]
    pub fn worker_name(&self) -> &str {
        &self.worker_name
    }

    /// The scratch directory this run uses for Worker state and credentials.
    #[must_use]
    pub fn tmp_root(&self) -> &Path {
        &self.tmp_root
    }

    fn client_config(&self, master_key: &str) -> ClientConfig {
        ClientConfig::new(self.remote_uri.clone())
            .with_protocol(Protocol::Connect)
            .with_codec_format(CodecFormat::Json)
            .with_default_header("authorization", format!("Bearer {master_key}"))
    }

    /// A `GenerationService` client authenticated with `master_key`.
    #[must_use]
    pub fn generation_client(&self, master_key: &str) -> pb::GenerationServiceClient<HttpClient> {
        pb::GenerationServiceClient::new(
            self.native_transport.clone(),
            self.client_config(master_key),
        )
    }

    /// A `CatalogService` client authenticated with `master_key`.
    #[must_use]
    pub fn catalog_client(&self, master_key: &str) -> pb::CatalogServiceClient<HttpClient> {
        pb::CatalogServiceClient::new(
            self.native_transport.clone(),
            self.client_config(master_key),
        )
    }

    /// A `GenerationService` client carrying a fresh tenant-scoped
    /// idempotency key (ADR 0006), so each call is a distinct submission.
    #[must_use]
    pub fn submit_client(&self, master_key: &str) -> pb::GenerationServiceClient<HttpClient> {
        pb::GenerationServiceClient::new(
            self.native_transport.clone(),
            self.client_config(master_key)
                .with_default_header("idempotency-key", Uuid::now_v7().to_string()),
        )
    }

    /// Submits a durable Generation as Tenant 1.
    ///
    /// # Errors
    /// Returns the `ConnectError` admission rejected the request with.
    pub async fn submit(
        &self,
        request: pb::SubmitRequest,
    ) -> Result<pb::Generation, connectrpc::ConnectError> {
        let response = self
            .submit_client(&self.tenant1.master_key)
            .submit(request)
            .await?
            .into_owned();
        response.generation.into_option().ok_or_else(|| {
            connectrpc::ConnectError::new(connectrpc::ErrorCode::Internal, "no generation")
        })
    }

    /// Reads a Generation through `GetGeneration`, as Tenant 1.
    pub async fn get_generation(&self, generation_id: &str) -> anyhow::Result<pb::Generation> {
        let response = self
            .generation_client(&self.tenant1.master_key)
            .get_generation(pb::GetGenerationRequest {
                generation_id: generation_id.to_owned(),
                ..Default::default()
            })
            .await
            .map_err(|err| anyhow::anyhow!("GetGeneration failed: {err}"))?
            .into_owned();
        response
            .generation
            .into_option()
            .context("GetGeneration response missing generation")
    }

    /// Opens a `WatchGeneration` stream, as Tenant 1.
    pub async fn watch_generation(
        &self,
        generation_id: &str,
    ) -> anyhow::Result<
        connectrpc::client::ServerStream<
            <HttpClient as connectrpc::client::ClientTransport>::ResponseBody,
            pb::GenerationEventView<'static>,
        >,
    > {
        self.generation_client(&self.tenant1.master_key)
            .watch_generation_with_options(
                pb::WatchGenerationRequest {
                    generation_id: generation_id.to_owned(),
                    ..Default::default()
                },
                connectrpc::client::CallOptions::default(),
            )
            .await
            .map_err(|err| anyhow::anyhow!("WatchGeneration failed: {err}"))
    }

    /// The schema-owner pool, for suites that need to assert on tables this
    /// module has no helper for. It bypasses RLS: fine for assertions and
    /// fixture setup, never a substitute for exercising isolation through the
    /// wire API.
    pub const fn admin_pool(&self) -> &PgPool {
        &self.admin_pool
    }

    pub async fn generation_row(
        &self,
        tenant_id: Uuid,
        id: Uuid,
    ) -> anyhow::Result<Option<GenerationRow>> {
        db::generation_row(&self.admin_pool, tenant_id, id).await
    }

    pub async fn latest_generation_row(
        &self,
        tenant_id: Uuid,
        state: &str,
    ) -> anyhow::Result<Option<GenerationRow>> {
        db::latest_generation_row(&self.admin_pool, tenant_id, state).await
    }

    pub async fn attempt_rows(
        &self,
        tenant_id: Uuid,
        generation_id: Uuid,
    ) -> anyhow::Result<Vec<AttemptRow>> {
        db::attempt_rows(&self.admin_pool, tenant_id, generation_id).await
    }

    pub async fn event_kinds(
        &self,
        tenant_id: Uuid,
        generation_id: Uuid,
    ) -> anyhow::Result<Vec<String>> {
        db::event_kinds(&self.admin_pool, tenant_id, generation_id).await
    }

    /// Whether the Worker named `worker_name` is `online == expected` in
    /// tenant 1's `ListWorkers`.
    ///
    /// # Errors
    /// Returns an error if the public `ListWorkers` RPC fails.
    pub async fn worker_is_online(
        &self,
        worker_name: &str,
        expected: bool,
    ) -> anyhow::Result<Option<()>> {
        let response = self
            .catalog_client(&self.tenant1.master_key)
            .list_workers(pb::ListWorkersRequest::default())
            .await
            .map_err(|err| anyhow::anyhow!("ListWorkers failed: {err}"))?
            .into_owned();
        let online = response
            .workers
            .iter()
            .find(|worker| worker.name == worker_name)
            .is_some_and(|worker| worker.online);
        Ok((online == expected).then_some(()))
    }

    /// Revokes a Worker Credential through `gpq remote worker revoke` (ADR
    /// 0009: credentials are revocable, and revocation is an administration
    /// command, not an API call).
    ///
    /// # Errors
    /// Returns an error if the command fails.
    pub async fn revoke_worker(&self, worker: Uuid) -> anyhow::Result<()> {
        let env: Vec<(&str, &str)> = self
            .admin
            .env
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        cli::worker_revoke(&self.admin.binary, &env, self.tenant1.id, worker).await
    }

    /// Revokes the real Worker's credential.
    ///
    /// # Errors
    /// Returns an error if the command fails.
    pub async fn revoke_worker_credential(&self) -> anyhow::Result<()> {
        self.revoke_worker(self.worker_id).await
    }

    /// Stops the running `gpq worker run`, simulating Worker loss (ADR 0003).
    ///
    /// Returns `false` when no Worker was running.
    ///
    /// # Errors
    /// Returns an error if the child cannot be signalled.
    pub async fn kill_worker(&self) -> anyhow::Result<bool> {
        let child = self
            .worker_child
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        match child {
            Some(mut child) => {
                child.kill().await.context("killing the gpq worker child")?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Starts `gpq worker run` again with the exact configuration and
    /// credential directory the harness first used.
    ///
    /// # Errors
    /// Returns an error if a Worker is already running or the spawn fails.
    pub fn restart_worker(&self) -> anyhow::Result<()> {
        let child = spawn_worker(&self.worker_spawn)?;
        let mut slot = self
            .worker_child
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slot.is_some() {
            bail!("a gpq worker child is already running; kill it before restarting");
        }
        *slot = Some(child);
        Ok(())
    }

    /// Tears the harness down: gracefully stops both child processes, drops
    /// the per-run database and login role, removes the run's temp directory,
    /// and finally hands the `PostgreSQL` container back to `testcontainers`.
    ///
    /// # Errors
    /// Returns an error if the container cannot be removed.
    pub async fn teardown(&self) -> anyhow::Result<()> {
        let worker_child = self
            .worker_child
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(mut child) = worker_child {
            terminate_gracefully(&mut child, Duration::from_secs(10)).await;
        }

        let remote_child = self
            .remote_child
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(mut child) = remote_child {
            terminate_gracefully(&mut child, Duration::from_secs(10)).await;
        }

        self.admin_pool.close().await;
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.db_name
        )))
        .execute(&self.maintenance_pool)
        .await;
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"DROP ROLE IF EXISTS "{}""#,
            self.app_role
        )))
        .execute(&self.maintenance_pool)
        .await;
        self.maintenance_pool.close().await;

        let _ = std::fs::remove_dir_all(&self.tmp_root);

        let lease = self.container_lease.lock().await.take();
        if let Some(lease) = lease {
            lease.release().await?;
        }
        reap_shared_container().await
    }

    async fn build() -> Harness {
        match Self::build_with(HarnessOptions::default()).await {
            Ok(harness) => harness,
            Err(err) => panic!("failed to build the gpq e2e test harness: {err:?}"),
        }
    }

    /// Builds a harness with the given options, so suites other than `e2e.rs`
    /// can reuse this whole spawn sequence.
    ///
    /// # Errors
    /// Returns an error if any step of the sequence fails.
    pub async fn build_with(options: HarnessOptions) -> anyhow::Result<Harness> {
        let gpq_bin = PathBuf::from(env!("CARGO_BIN_EXE_gpq"));
        let container_lease = maintenance_url().await?;
        match Self::build_with_lease(options, gpq_bin, &container_lease).await {
            Ok(harness) => {
                *harness.container_lease.lock().await = Some(container_lease);
                Ok(harness)
            }
            Err(error) => match container_lease.release().await {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(error.context(format!(
                    "container lease cleanup also failed: {cleanup_error}"
                ))),
            },
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "linear one-shot setup sequence; splitting it would scatter the setup narrative \
                  across helpers each called exactly once"
    )]
    async fn build_with_lease(
        options: HarnessOptions,
        gpq_bin: PathBuf,
        container_lease: &ContainerLease,
    ) -> anyhow::Result<Harness> {
        let maintenance_url_str = container_lease.maintenance_url().to_owned();
        let maintenance_url: url::Url = maintenance_url_str
            .parse()
            .context("parsing the testcontainers PostgreSQL maintenance URL")?;
        let suffix = Uuid::now_v7().simple().to_string();
        let db_name = format!("gpq_test_{suffix}");
        let app_role = format!("gpq_app_{suffix}");
        let app_password = random_hex(16);

        let maintenance_pool = PgPool::connect(&maintenance_url_str)
            .await
            .context("connecting to the maintenance database")?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"CREATE DATABASE "{db_name}""#
        )))
        .execute(&maintenance_pool)
        .await
        .context("creating the per-run test database")?;

        let owner_url = database_url_for(&maintenance_url, &db_name);
        cli::migrate(&gpq_bin, owner_url.as_str())
            .await
            .context("running gpq remote migrate")?;

        let admin_pool = PgPool::connect(owner_url.as_str())
            .await
            .context("connecting as the schema owner")?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"CREATE ROLE "{app_role}" LOGIN PASSWORD '{app_password}' IN ROLE gpq_app"#
        )))
        .execute(&admin_pool)
        .await
        .context("creating the forced-RLS serving login role")?;
        let app_url = database_url_with_credentials(&owner_url, &app_role, &app_password)?;

        let credential_key_hex = random_hex(32);
        let remote_port = free_port()?;
        let remote_bind = format!("127.0.0.1:{remote_port}");
        let remote_base = format!("http://{remote_bind}");
        let remote_uri: http::Uri = remote_base.parse().context("parsing the remote base URI")?;

        let owner_url_str = owner_url.to_string();
        let admin_env: [(&str, &str); 3] = [
            ("GPQ_DATABASE_URL", owner_url_str.as_str()),
            ("GPQ_CREDENTIAL_KEY", credential_key_hex.as_str()),
            ("GPQ_PUBLIC_BASE_URL", remote_base.as_str()),
        ];
        let admin_env_owned: Vec<(String, String)> = admin_env
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        let tenant1_id = cli::tenant_create(&gpq_bin, &admin_env, "e2e-tenant-1").await?;
        let tenant1_key = cli::tenant_key_rotate(&gpq_bin, &admin_env, tenant1_id).await?;
        let tenant2_id = cli::tenant_create(&gpq_bin, &admin_env, "e2e-tenant-2").await?;
        let tenant2_key = cli::tenant_key_rotate(&gpq_bin, &admin_env, tenant2_id).await?;

        let remote_child = Command::new(&gpq_bin)
            .arg("remote")
            .arg("serve")
            .env("GPQ_DATABASE_URL", app_url.as_str())
            .env("GPQ_CREDENTIAL_KEY", &credential_key_hex)
            .env("GPQ_BIND_ADDR", &remote_bind)
            .env("GPQ_PUBLIC_BASE_URL", &remote_base)
            .envs(options.extra_remote_env.iter().map(|(k, v)| (k, v)))
            .kill_on_drop(true)
            .spawn()
            .context("spawning gpq remote serve")?;

        let http = reqwest::Client::new();
        wait_for_readyz(&http, &remote_base).await?;

        let tmp_root = std::env::temp_dir().join(format!("gpq-e2e-{suffix}"));
        let worker_state_dir = tmp_root.join("worker-state");
        let pool_state_dir = tmp_root.join("pool0-state");
        let credentials_dir = tmp_root.join("credentials");
        for dir in [
            &tmp_root,
            &worker_state_dir,
            &pool_state_dir,
            &credentials_dir,
        ] {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }

        let worker_name = format!("e2e-worker-{suffix}");
        let listen_port = free_port()?;
        let listen = format!("127.0.0.1:{listen_port}");
        let (worker_id, worker_credential) =
            synthetic::enroll(&remote_uri, &tenant1_key.secret, &worker_name).await?;
        write_credential_file(&credentials_dir, &worker_credential)?;

        let config_path = tmp_root.join("worker.toml");
        std::fs::write(
            &config_path,
            worker_config_toml(
                &worker_name,
                &remote_base,
                &worker_state_dir,
                &pool_state_dir,
                &listen,
            ),
        )
        .context("writing the worker TOML config")?;

        let worker_spawn = WorkerSpawn {
            binary: gpq_bin.clone(),
            config_path,
            credentials_dir,
        };
        let worker_child = spawn_worker(&worker_spawn)?;

        let native_transport = HttpClient::plaintext_http2_only();
        let harness = Harness {
            admin_pool,
            maintenance_pool,
            db_name,
            app_role,
            remote_base,
            remote_uri,
            native_transport,
            remote_child: Mutex::new(Some(remote_child)),
            worker_child: Mutex::new(Some(worker_child)),
            worker_spawn,
            admin: AdminCli {
                binary: gpq_bin,
                env: admin_env_owned,
            },
            worker_id: worker_id
                .parse()
                .context("parsing the enrolled Worker id returned by enrollment")?,
            worker_name: worker_name.clone(),
            tmp_root,
            http,
            tenant1: TenantFixture {
                id: tenant1_id,
                master_key: tenant1_key.secret,
            },
            tenant2: TenantFixture {
                id: tenant2_id,
                master_key: tenant2_key.secret,
            },
            container_lease: tokio::sync::Mutex::new(None),
        };

        // The real Worker registers its session even though its Pool cannot
        // execute anything on this host, so waiting for `online` is enough.
        wait_until(
            || harness.worker_is_online(&worker_name, true),
            Duration::from_secs(30),
        )
        .await
        .context("waiting for the real gpq worker to come online")?;

        Ok(harness)
    }
}

/// Options for [`Harness::build_with`].
#[derive(Clone, Debug, Default)]
pub struct HarnessOptions {
    /// Extra environment for `gpq remote serve`, e.g. the `GPQ_S3_*` object
    /// storage settings (ADR 0008 keeps those optional).
    pub extra_remote_env: Vec<(String, String)>,
}
