//! Shared end-to-end test harness: starts one shared `testcontainers`-managed
//! `PostgreSQL` 18 container for this test binary, boots real
//! `gpq-remote`/`gpq-worker` binaries against a throwaway database inside it
//! and a fake `llama-server`, then hands every test the wire clients and
//! database handles it needs.
//!
//! Coverage caveat: the Worker runs as a child process, and instrumented
//! binaries only flush their counters on a normal exit, while these suites
//! stop the Worker with signals (including deliberately killing it to test
//! Worker loss). `cargo llvm-cov` therefore reports `gpq-worker`'s files at
//! their unit-test coverage only, no matter how thoroughly a suite drives the
//! real Worker end to end. Judge Worker coverage by the behavior these tests
//! assert, not by that number.

pub mod cli;
pub mod db;
pub mod fake_llama;
#[path = "../shared/container.rs"]
mod shared_container;

pub use db::{AttemptRow, GenerationRow};
use shared_container::{maintenance_url, reap_shared_container};

use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, bail};
use buffa_types::google::protobuf::Struct;
use chrono::{DateTime, Utc};
use connectrpc::client::{CallOptions, ClientConfig, ClientTransport, HttpClient};
use connectrpc::{CodecFormat, Protocol};
use fake_llama::FakeLlama;
use gpq_proto::gpq::v1 as pb;
use gpq_proto::gpq::worker::v1 as wpb;
use rand::Rng;
use sqlx::PgPool;
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

/// Reads a `reqwest` SSE response into its decoded `data:` JSON payloads,
/// stopping at (and excluding) the `[DONE]` sentinel.
pub async fn collect_sse_json(
    response: reqwest::Response,
) -> anyhow::Result<Vec<serde_json::Value>> {
    use futures::StreamExt as _;
    let mut stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut events = Vec::new();
    while let Some(chunk) = stream.next().await {
        buffer.push_str(&String::from_utf8_lossy(&chunk?));
        while let Some(pos) = buffer.find("\n\n") {
            let frame: String = buffer.drain(..pos + 2).collect();
            let data = frame
                .lines()
                .filter_map(|line| line.strip_prefix("data:"))
                .map(str::trim)
                .collect::<Vec<_>>()
                .join("\n");
            if data.is_empty() {
                continue;
            }
            if data == "[DONE]" {
                return Ok(events);
            }
            events.push(serde_json::from_str(&data)?);
        }
    }
    Ok(events)
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

fn chat_parameters(user_message: &str) -> anyhow::Result<Struct> {
    let value = serde_json::json!({"messages": [{"role": "user", "content": user_message}]});
    serde_json::from_value(value).context("building chat parameters as a protobuf Struct")
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

/// Which backend kind a harness Pool advertises (ADR 0005: a Pool hosts one
/// Active Runtime kind at a time).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PoolKind {
    /// A llama.cpp Pool, the default the OpenAI-compatible tests need.
    #[default]
    LlamaCpp,
    /// A `ComfyUI` Pool, for the image, video, and music modalities.
    ComfyUi,
}

impl PoolKind {
    /// The value the Worker TOML expects.
    const fn as_str(self) -> &'static str {
        match self {
            Self::LlamaCpp => "llama_cpp",
            Self::ComfyUi => "comfyui",
        }
    }
}

/// How to build a harness, so suites beyond `e2e.rs` can reuse the whole
/// spawn sequence (container, migrations, serving role, Remote, Worker) with
/// the one or two things they need changed.
#[derive(Clone, Debug, Default)]
pub struct HarnessOptions {
    /// Extra environment for `gpq-remote serve`, e.g. the `GPQ_S3_*` object
    /// storage settings (ADR 0008 keeps those optional).
    pub extra_remote_env: Vec<(String, String)>,
    /// The Pool's backend kind.
    pub pool_kind: PoolKind,
    /// Loopback base URL of the backend the Pool talks to. `None` points the
    /// Pool at the harness's own fake llama-server.
    pub pool_base_url: Option<String>,
}

fn worker_config_toml(
    worker_name: &str,
    remote_base: &str,
    worker_state_dir: &Path,
    pool_state_dir: &Path,
    pool_kind: PoolKind,
    pool_base_url: &str,
    model_path: &Path,
) -> String {
    format!(
        "name = \"{worker_name}\"\n\
         remote_url = \"{remote_base}\"\n\
         state_dir = \"{worker_state_dir}\"\n\
         \n\
         [[pools]]\n\
         key = \"pool0\"\n\
         backend = \"{backend}\"\n\
         executable = \"/bin/sleep\"\n\
         args = [\"600\"]\n\
         state_dir = \"{pool_state_dir}\"\n\
         startup_timeout_secs = 20\n\
         base_url = \"{pool_base_url}\"\n\
         model_paths = [\"{model_path}\"]\n",
        worker_state_dir = worker_state_dir.display(),
        pool_state_dir = pool_state_dir.display(),
        backend = pool_kind.as_str(),
        model_path = model_path.display(),
    )
}

fn write_credential_file(dir: &Path, worker_name: &str, credential: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(worker_name);
    std::fs::write(&path, credential)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

async fn enroll_worker(
    remote_uri: &http::Uri,
    master_key: &str,
    worker_name: &str,
) -> anyhow::Result<(String, String)> {
    let connection = connectrpc::client::Http2Connection::connect_plaintext(remote_uri.clone())
        .await
        .context("connecting to remote for worker enrollment")?;
    let transport = connection.shared(16);
    let client_config = ClientConfig::new(remote_uri.clone())
        .with_protocol(Protocol::Grpc)
        .with_default_header("authorization", format!("Bearer {master_key}"));
    let client = wpb::WorkerEnrollmentServiceClient::new(transport, client_config);
    let request = wpb::EnrollRequest {
        worker_name: worker_name.to_owned(),
        host_descriptor: "gpq-remote-e2e-harness".to_owned(),
        protocol_major: gpq_proto::PROTOCOL_MAJOR,
        protocol_minor: gpq_proto::PROTOCOL_MINOR,
        worker_version: "e2e-test".to_owned(),
        ..Default::default()
    };
    let response = client
        .enroll(request)
        .await
        .map_err(|err| anyhow::anyhow!("Enroll RPC failed: {err}"))?
        .into_owned();
    Ok((response.worker_id, response.worker_credential))
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
    .context("gpq-remote never became ready")
}

/// Sends SIGTERM and gives the process `grace` to exit on its own (so
/// `gpq-worker` can run its own graceful shutdown, which terminates the
/// managed backend process tree it owns — ADR 0005 — rather than leaking
/// it), falling back to SIGKILL if it does not.
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

/// The shared, process-lifetime end-to-end fixture: a migrated database, a
/// running `gpq-remote serve`, a running `gpq-worker run` with one llama.cpp
/// Pool pointed at [`FakeLlama`], two enrolled Tenants, and one active Model
/// alias.
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
    /// Everything needed to respawn `gpq-worker run` exactly as it was first
    /// started, so a suite can exercise Worker loss and recovery (ADR 0003).
    worker_spawn: WorkerSpawn,
    /// `gpq-remote` binary and the schema-owner environment its
    /// administration subcommands need (ADR 0016).
    admin: AdminCli,
    /// The enrolled Worker's identity, for administration commands.
    worker_id: Uuid,
    tmp_root: PathBuf,
    pool_state_dir: PathBuf,
    worker_name: String,
    pub http: reqwest::Client,
    pub http2: reqwest::Client,
    pub fake: FakeLlama,
    pub tenant1: TenantFixture,
    pub tenant2: TenantFixture,
    pub model_alias: String,
}

/// The `gpq-remote` binary plus the schema-owner environment its
/// administration subcommands run with.
struct AdminCli {
    binary: PathBuf,
    env: Vec<(String, String)>,
}

/// The argv and environment the harness used to start `gpq-worker run`.
struct WorkerSpawn {
    binary: PathBuf,
    config_path: PathBuf,
    credentials_dir: PathBuf,
}

/// Starts `gpq-worker run` from a recorded spawn spec.
fn spawn_worker(spec: &WorkerSpawn) -> anyhow::Result<Child> {
    Command::new(&spec.binary)
        .arg("run")
        .arg("--config")
        .arg(&spec.config_path)
        .env("CREDENTIALS_DIRECTORY", &spec.credentials_dir)
        .kill_on_drop(true)
        .spawn()
        .context("spawning gpq-worker run")
}

/// Resolves the `gpq-worker` binary the harness runs as a child process.
///
/// Cargo only exposes `CARGO_BIN_EXE_*` for binaries of the crate under test,
/// and it does not build another package's binaries just because an
/// integration test wants them, so the sibling binary is missing whenever the
/// suite runs without a prior `cargo build --workspace --bins` — including
/// under `cargo llvm-cov`, which uses its own target directory. Build it on
/// demand into the same directory instead of failing the whole suite.
async fn ensure_worker_binary(bin_dir: &Path) -> anyhow::Result<PathBuf> {
    let worker_bin = bin_dir.join(if cfg!(windows) {
        "gpq-worker.exe"
    } else {
        "gpq-worker"
    });
    if worker_bin.is_file() {
        return Ok(worker_bin);
    }

    let Some(target_dir) = bin_dir.parent() else {
        bail!("{} has no parent target directory", bin_dir.display());
    };
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .args(["build", "--package", "gpq-worker", "--bin", "gpq-worker"])
        .arg("--target-dir")
        .arg(target_dir)
        .status()
        .await
        .context("running cargo build for the gpq-worker binary")?;
    if !status.success() {
        bail!("cargo build --package gpq-worker failed with {status}");
    }
    if !worker_bin.is_file() {
        bail!(
            "gpq-worker binary still missing at {} after building it",
            worker_bin.display()
        );
    }
    Ok(worker_bin)
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

static HARNESS: LazyLock<Harness> = LazyLock::new(|| runtime().block_on(Harness::build()));

/// The shared harness, built lazily on first use.
pub fn harness() -> &'static Harness {
    &HARNESS
}

/// Runs `fut` to completion on the shared runtime.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    runtime().block_on(fut)
}

impl Harness {
    /// Absolute URL of `path` on the running `gpq-remote serve`.
    #[must_use]
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.remote_base)
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
    /// idempotency key. ADR 0006 requires one on every Native `Submit`, and
    /// a fresh key per call keeps each helper invocation a distinct
    /// submission rather than a replay of the previous one.
    #[must_use]
    pub fn submit_client(&self, master_key: &str) -> pb::GenerationServiceClient<HttpClient> {
        pb::GenerationServiceClient::new(
            self.native_transport.clone(),
            self.client_config(master_key)
                .with_default_header("idempotency-key", Uuid::now_v7().to_string()),
        )
    }

    /// Submits a durable Native Generation against a Model alias, as
    /// Tenant 1.
    pub async fn native_submit_model(&self, alias: &str) -> anyhow::Result<pb::Generation> {
        let parameters = chat_parameters("say hi")?;
        let request = pb::SubmitRequest {
            target: Some(pb::submit_request::Target::ModelAlias(alias.to_owned())),
            parameters: parameters.into(),
            output_placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
            ..Default::default()
        };
        let response = self
            .submit_client(&self.tenant1.master_key)
            .submit(request)
            .await
            .map_err(|err| anyhow::anyhow!("native Submit failed: {err}"))?
            .into_owned();
        response
            .generation
            .into_option()
            .context("Submit response missing generation")
    }

    /// Submits a durable Native Generation against a Workflow alias, as
    /// Tenant 1.
    pub async fn native_submit_workflow(&self, alias: &str) -> anyhow::Result<pb::Generation> {
        let request = pb::SubmitRequest {
            target: Some(pb::submit_request::Target::WorkflowAlias(alias.to_owned())),
            output_placement: pb::ArtifactPlacement::ARTIFACT_PLACEMENT_WORKER_LOCAL.into(),
            ..Default::default()
        };
        let response = self
            .submit_client(&self.tenant1.master_key)
            .submit(request)
            .await
            .map_err(|err| anyhow::anyhow!("native Submit failed: {err}"))?
            .into_owned();
        response
            .generation
            .into_option()
            .context("Submit response missing generation")
    }

    /// Reads a Generation through `GetGeneration`, as Tenant 1.
    pub async fn native_get_generation(
        &self,
        generation_id: &str,
    ) -> anyhow::Result<pb::Generation> {
        let request = pb::GetGenerationRequest {
            generation_id: generation_id.to_owned(),
            ..Default::default()
        };
        let response = self
            .generation_client(&self.tenant1.master_key)
            .get_generation(request)
            .await
            .map_err(|err| anyhow::anyhow!("native GetGeneration failed: {err}"))?
            .into_owned();
        response
            .generation
            .into_option()
            .context("GetGeneration response missing generation")
    }

    /// Opens a `WatchGeneration` stream, as Tenant 1.
    pub async fn native_watch_generation(
        &self,
        generation_id: &str,
    ) -> anyhow::Result<
        connectrpc::client::ServerStream<
            <HttpClient as ClientTransport>::ResponseBody,
            pb::GenerationEventView<'static>,
        >,
    > {
        let request = pb::WatchGenerationRequest {
            generation_id: generation_id.to_owned(),
            ..Default::default()
        };
        self.generation_client(&self.tenant1.master_key)
            .watch_generation_with_options(request, CallOptions::default())
            .await
            .map_err(|err| anyhow::anyhow!("native WatchGeneration failed: {err}"))
    }

    /// Registers a Workflow Version and alias for which no `ComfyUI` Pool
    /// is ever online, returning the alias name.
    pub async fn register_workflow_alias_without_worker(&self) -> anyhow::Result<String> {
        let graph: Struct =
            serde_json::from_value(serde_json::json!({"1": {"class_type": "SaveImage"}}))?;
        let manifest = pb::WorkflowManifest {
            output_node: "1".to_owned(),
            output_name: "IMAGE".to_owned(),
            artifact_kind: pb::MediaKind::MEDIA_KIND_IMAGE.into(),
            artifact_mime: "image/png".to_owned(),
            ..Default::default()
        };
        let request = pb::RegisterWorkflowVersionRequest {
            graph: graph.into(),
            manifest: manifest.into(),
            modality: pb::Modality::MODALITY_IMAGE.into(),
            ..Default::default()
        };
        let response = self
            .catalog_client(&self.tenant1.master_key)
            .register_workflow_version(request)
            .await
            .map_err(|err| anyhow::anyhow!("RegisterWorkflowVersion failed: {err}"))?
            .into_owned();
        let version = response
            .version
            .into_option()
            .context("missing registered workflow version")?;
        let alias = format!("img-workflow-{}", &version.content_sha256[..8]);
        let set_request = pb::SetWorkflowAliasRequest {
            alias: alias.clone(),
            content_sha256: version.content_sha256,
            ..Default::default()
        };
        self.catalog_client(&self.tenant1.master_key)
            .set_workflow_alias(set_request)
            .await
            .map_err(|err| anyhow::anyhow!("SetWorkflowAlias failed: {err}"))?;
        Ok(alias)
    }

    /// Registers a synthetic Model Version no online Worker advertises,
    /// aliases it, and returns the alias name (ADR 0006's `model_not_available`
    /// path: a known alias with no capable online Worker).
    pub async fn register_unavailable_model_alias(&self) -> anyhow::Result<String> {
        let content_sha256 = random_hex(32);
        sqlx::query(
            "INSERT INTO model_versions (tenant_id, id, content_sha256, modality) \
             VALUES ($1, $2, $3, 'llm')",
        )
        .bind(self.tenant1.id)
        .bind(Uuid::now_v7())
        .bind(&content_sha256)
        .execute(&self.admin_pool)
        .await
        .context("inserting a synthetic, unadvertised model version")?;

        let alias = format!("unavailable-{}", &content_sha256[..8]);
        let request = pb::SetModelAliasRequest {
            alias: alias.clone(),
            content_sha256,
            ..Default::default()
        };
        self.catalog_client(&self.tenant1.master_key)
            .set_model_alias(request)
            .await
            .map_err(|err| anyhow::anyhow!("SetModelAlias failed: {err}"))?;
        Ok(alias)
    }

    pub async fn generation_row(
        &self,
        tenant_id: Uuid,
        id: Uuid,
    ) -> anyhow::Result<Option<GenerationRow>> {
        db::generation_row(&self.admin_pool, tenant_id, id).await
    }

    /// The schema-owner pool, for suites that need to assert on tables this
    /// module has no helper for.
    ///
    /// It bypasses RLS (see `db`'s module docs): fine for assertions and
    /// fixture setup, never a substitute for exercising isolation through the
    /// wire API.
    pub const fn admin_pool(&self) -> &PgPool {
        &self.admin_pool
    }

    pub async fn latest_generation_row(
        &self,
        tenant_id: Uuid,
        state: &str,
    ) -> anyhow::Result<Option<GenerationRow>> {
        db::latest_generation_row(&self.admin_pool, tenant_id, state).await
    }

    pub async fn generation_row_created_after(
        &self,
        tenant_id: Uuid,
        after: DateTime<Utc>,
    ) -> anyhow::Result<Option<GenerationRow>> {
        db::generation_row_created_after(&self.admin_pool, tenant_id, after).await
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

    pub async fn db_now(&self) -> anyhow::Result<DateTime<Utc>> {
        db::db_now(&self.admin_pool).await
    }

    /// Whether the Worker's single Pool currently reports `ready == expected`.
    pub async fn pool_is_ready(&self, expected: bool) -> anyhow::Result<Option<()>> {
        let ready = db::pool_ready(&self.admin_pool, self.tenant1.id, &self.worker_name).await?;
        Ok((ready == Some(expected)).then_some(()))
    }

    /// The OS pid of the fake backend's currently managed process (a plain
    /// `sleep`), read from the Pool's durable process-identity record.
    /// `None` before the Worker has spawned it yet.
    pub async fn managed_backend_pid(&self) -> anyhow::Result<Option<u32>> {
        let identity_path = self.pool_state_dir.join("process-identity.json");
        let Ok(raw) = tokio::fs::read_to_string(&identity_path).await else {
            return Ok(None);
        };
        let identity: serde_json::Value = serde_json::from_str(&raw)?;
        let pid = identity["pid"]
            .as_u64()
            .context("process-identity.json missing a numeric pid")?;
        Ok(Some(u32::try_from(pid)?))
    }

    /// Kills the fake backend's managed process (a plain `sleep`), the way
    /// a real backend crash would present to the Worker's supervisor (ADR
    /// 0005). Returns the killed pid, so a caller can confirm the Worker
    /// replaced it with a genuinely different process rather than merely
    /// observing a pool that was never actually disturbed.
    pub async fn kill_managed_backend_process(&self) -> anyhow::Result<u32> {
        let pid = wait_until(|| self.managed_backend_pid(), Duration::from_secs(10))
            .await
            .context("waiting for the Pool's managed-process identity file")?;
        let status = Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .status()
            .await
            .with_context(|| format!("running kill -9 {pid}"))?;
        anyhow::ensure!(status.success(), "kill -9 {pid} exited with {status}");
        Ok(pid)
    }

    /// Revokes this harness's Worker Credential through
    /// `gpq-remote worker revoke` (ADR 0009: credentials are revocable, and
    /// revocation is an administration command, not an API call).
    ///
    /// # Errors
    /// Returns an error if the command fails.
    pub async fn revoke_worker_credential(&self) -> anyhow::Result<()> {
        let env: Vec<(&str, &str)> = self
            .admin
            .env
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        cli::worker_revoke(&self.admin.binary, &env, self.tenant1.id, self.worker_id).await
    }

    /// Stops the running `gpq-worker`, simulating Worker loss (ADR 0003).
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
                child.kill().await.context("killing the gpq-worker child")?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Starts `gpq-worker run` again with the exact configuration and
    /// credential directory the harness first used, so a recovered Worker
    /// resumes against the same enrollment (ADR 0009).
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
            bail!("a gpq-worker child is already running; kill it before restarting");
        }
        *slot = Some(child);
        Ok(())
    }

    /// Tears the harness down: gracefully stops both child processes (so
    /// the Worker gets a chance to terminate the managed backend process
    /// tree it owns, rather than leaking it), drops the per-run database
    /// and login role, removes the run's temp directory, and finally hands
    /// the `PostgreSQL` container back to `testcontainers`.
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

        // Last, once nothing is connected to it any more: hand the container
        // back to `testcontainers` for removal.
        reap_shared_container().await
    }

    async fn build() -> Harness {
        match Self::build_with(HarnessOptions::default()).await {
            Ok(harness) => harness,
            Err(err) => panic!("failed to build the gpq-remote e2e test harness: {err:?}"),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "linear one-shot setup sequence; splitting it would scatter the setup narrative \
                  across helpers each called exactly once"
    )]
    /// Builds a harness with the given options, so suites other than
    /// `e2e.rs` can reuse this whole spawn sequence.
    ///
    /// # Errors
    /// Returns an error if any step of the sequence fails.
    pub async fn build_with(options: HarnessOptions) -> anyhow::Result<Harness> {
        let remote_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_gpq-remote"));
        let Some(bin_dir) = remote_bin.parent() else {
            bail!("CARGO_BIN_EXE_gpq-remote has no parent directory");
        };
        let worker_bin = ensure_worker_binary(bin_dir).await?;

        let maintenance_url_str = maintenance_url().await?;
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
        cli::migrate(&remote_bin, owner_url.as_str())
            .await
            .context("running gpq-remote migrate")?;

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
        let tenant1_id = cli::tenant_create(&remote_bin, &admin_env, "e2e-tenant-1").await?;
        let tenant1_key = cli::tenant_key_rotate(&remote_bin, &admin_env, tenant1_id).await?;
        let tenant2_id = cli::tenant_create(&remote_bin, &admin_env, "e2e-tenant-2").await?;
        let tenant2_key = cli::tenant_key_rotate(&remote_bin, &admin_env, tenant2_id).await?;

        let remote_child = Command::new(&remote_bin)
            .arg("serve")
            .env("GPQ_DATABASE_URL", app_url.as_str())
            .env("GPQ_CREDENTIAL_KEY", &credential_key_hex)
            .env("GPQ_BIND_ADDR", &remote_bind)
            .env("GPQ_PUBLIC_BASE_URL", &remote_base)
            .envs(options.extra_remote_env.iter().map(|(k, v)| (k, v)))
            .kill_on_drop(true)
            .spawn()
            .context("spawning gpq-remote serve")?;

        let http = reqwest::Client::new();
        let http2 = reqwest::Client::builder()
            .http2_prior_knowledge()
            .build()
            .context("building the HTTP/2 prior-knowledge reqwest client")?;
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
        let model_path = tmp_root.join("fake-model.bin");
        std::fs::write(&model_path, b"fake model bytes for the gpq e2e harness")
            .context("writing the fake model file")?;

        let fake_port = free_port()?;
        let fake = FakeLlama::spawn(fake_port, model_path.clone())
            .await
            .context("starting the fake llama-server")?;

        let worker_name = format!("e2e-worker-{suffix}");
        let (worker_id, worker_credential) =
            enroll_worker(&remote_uri, &tenant1_key.secret, &worker_name).await?;
        write_credential_file(&credentials_dir, &worker_name, &worker_credential)?;

        let config_path = tmp_root.join("worker.toml");
        std::fs::write(
            &config_path,
            worker_config_toml(
                &worker_name,
                &remote_base,
                &worker_state_dir,
                &pool_state_dir,
                options.pool_kind,
                options
                    .pool_base_url
                    .as_deref()
                    .unwrap_or(&format!("http://127.0.0.1:{fake_port}")),
                &model_path,
            ),
        )
        .context("writing the worker TOML config")?;

        let worker_spawn = WorkerSpawn {
            binary: worker_bin,
            config_path,
            credentials_dir,
        };
        let worker_child = spawn_worker(&worker_spawn)?;

        let native_transport = HttpClient::plaintext_http2_only();
        let catalog_config = ClientConfig::new(remote_uri.clone())
            .with_protocol(Protocol::Connect)
            .with_codec_format(CodecFormat::Json)
            .with_default_header("authorization", format!("Bearer {}", tenant1_key.secret));
        let catalog_for_wait =
            pb::CatalogServiceClient::new(native_transport.clone(), catalog_config);

        // Wait for the Worker to advertise a ready Pool. Only llama.cpp Pools
        // get a Model alias here: Remote registers Model Versions from
        // llama.cpp capability reports only (ADR 0012 pins Workflow Versions
        // through the Catalog API instead), so a ComfyUI harness maps its own
        // Workflow alias in the test that needs one.
        let advertised_model_sha256 = wait_until(
            || async {
                let response = catalog_for_wait
                    .list_workers(pb::ListWorkersRequest::default())
                    .await
                    .map_err(|err| anyhow::anyhow!("ListWorkers failed: {err}"))?
                    .into_owned();
                let Some(worker) = response.workers.into_iter().find(|w| w.name == worker_name)
                else {
                    return Ok(None);
                };
                if !worker.online {
                    return Ok(None);
                }
                if !worker.pools.iter().any(|pool| pool.total_slots > 0) {
                    return Ok(None);
                }
                match options.pool_kind {
                    PoolKind::LlamaCpp => Ok(worker.model_sha256.first().cloned()),
                    // Nothing to resolve, but the Pool is ready.
                    PoolKind::ComfyUi => Ok(Some(String::new())),
                }
            },
            Duration::from_secs(30),
        )
        .await
        .context("waiting for the Worker to come online with a ready Pool")?;

        let model_alias = match options.pool_kind {
            PoolKind::LlamaCpp => {
                let alias = "chat-model".to_owned();
                let set_alias_config = ClientConfig::new(remote_uri.clone())
                    .with_protocol(Protocol::Connect)
                    .with_codec_format(CodecFormat::Json)
                    .with_default_header("authorization", format!("Bearer {}", tenant1_key.secret));
                pb::CatalogServiceClient::new(native_transport.clone(), set_alias_config)
                    .set_model_alias(pb::SetModelAliasRequest {
                        alias: alias.clone(),
                        content_sha256: advertised_model_sha256,
                        ..Default::default()
                    })
                    .await
                    .map_err(|err| anyhow::anyhow!("SetModelAlias failed: {err}"))?;
                alias
            }
            PoolKind::ComfyUi => String::new(),
        };

        Ok(Harness {
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
                binary: remote_bin,
                env: admin_env_owned,
            },
            worker_id: worker_id
                .parse()
                .context("parsing the enrolled Worker id returned by enrollment")?,
            tmp_root,
            pool_state_dir,
            worker_name,
            http,
            http2,
            fake,
            tenant1: TenantFixture {
                id: tenant1_id,
                master_key: tenant1_key.secret,
            },
            tenant2: TenantFixture {
                id: tenant2_id,
                master_key: tenant2_key.secret,
            },
            model_alias,
        })
    }
}
