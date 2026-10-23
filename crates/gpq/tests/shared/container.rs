//! Shared bootstrap for PostgreSQL 18 testcontainers used by each integration
//! test binary. A container starts lazily and is shared while fixture leases
//! are active; after a cleanup request, a later fixture can start a fresh
//! container. Included as a private submodule of `support` and `e2e_support`,
//! so each test binary owns an independent container lifecycle.

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use sqlx::{Connection, PgConnection};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use tokio::runtime::Runtime;
use tokio::sync::{Mutex, OnceCell};

/// Process-wide state for this test binary's PostgreSQL container.
static CONTAINER: Mutex<ContainerState> = Mutex::const_new(ContainerState { shared: None });
static REAP_REQUESTED: AtomicBool = AtomicBool::new(false);
static CLEANUP_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap_or_else(|error| panic!("failed to create container cleanup runtime: {error}"))
});

struct ContainerState {
    shared: Option<SharedContainer>,
}

/// A started container and the state shared by its active fixture leases.
struct SharedContainer {
    container: ContainerAsync<PostgresImage>,
    /// Maintenance connection string, using the image's default
    /// `postgres`/`postgres` credentials and `postgres` database.
    maintenance_url: String,
    leases: Arc<LeaseTracker>,
    roles_initialized: Arc<OnceCell<()>>,
}

struct LeaseTracker {
    active: AtomicUsize,
}

/// Keeps the shared container alive for the lifetime of one test fixture.
pub struct ContainerLease {
    maintenance_url: String,
    leases: Arc<LeaseTracker>,
    explicit_release: bool,
}

impl ContainerLease {
    /// The maintenance connection string for this fixture's container.
    #[must_use]
    pub fn maintenance_url(&self) -> &str {
        &self.maintenance_url
    }

    /// Releases this fixture's lease and removes the container if cleanup was requested.
    ///
    /// # Errors
    /// Returns an error if the requested container removal fails.
    pub async fn release(mut self) -> anyhow::Result<()> {
        self.explicit_release = true;
        drop(self);
        cleanup_requested_container().await
    }
}

impl Drop for ContainerLease {
    fn drop(&mut self) {
        let last_lease = self.leases.active.fetch_sub(1, Ordering::AcqRel) == 1;
        // Implicit drops can run as a test runtime exits, so finish requested cleanup here.
        if last_lease && !self.explicit_release && REAP_REQUESTED.load(Ordering::Acquire) {
            let result = thread::Builder::new()
                .name("gpq-test-container-cleanup".to_owned())
                .spawn(|| CLEANUP_RUNTIME.block_on(cleanup_requested_container()));
            match result {
                Ok(task) => match task.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        eprintln!("failed to remove shared PostgreSQL test container: {error:#}");
                    }
                    Err(_) => eprintln!("shared PostgreSQL test cleanup thread panicked"),
                },
                Err(error) => {
                    eprintln!("failed to start shared PostgreSQL test cleanup thread: {error}");
                }
            }
        }
    }
}

/// Uses `TESTCONTAINERS_HOST_OVERRIDE` when set; otherwise preserves the host
/// reported by testcontainers.
///
/// # Errors
/// Returns an error when the override is set to an empty or invalid host.
pub fn resolve_testcontainers_host(default_host: url::Host) -> anyhow::Result<url::Host> {
    let host = match std::env::var("TESTCONTAINERS_HOST_OVERRIDE") {
        Ok(host) => host,
        Err(std::env::VarError::NotPresent) => return Ok(default_host),
        Err(error) => anyhow::bail!("reading TESTCONTAINERS_HOST_OVERRIDE: {error}"),
    };
    anyhow::ensure!(
        !host.is_empty(),
        "TESTCONTAINERS_HOST_OVERRIDE must not be empty"
    );
    url::Host::parse(&host)
        .map_err(|error| anyhow::anyhow!("invalid TESTCONTAINERS_HOST_OVERRIDE {host:?}: {error}"))
}

/// Starts or reuses PostgreSQL and returns a lease that keeps the container
/// alive until its fixture has finished. The shared roles are initialized once
/// per container before any database migration can need them.
pub async fn maintenance_url() -> anyhow::Result<ContainerLease> {
    let (lease, roles_initialized) = {
        let mut state = CONTAINER.lock().await;
        if state.shared.is_none() {
            let container = PostgresImage::default()
                .with_tag("18-alpine")
                .start()
                .await?;
            let host = resolve_testcontainers_host(container.get_host().await?)?;
            let port = container.get_host_port_ipv4(5432).await?;
            let maintenance_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
            state.shared = Some(SharedContainer {
                container,
                maintenance_url,
                leases: Arc::new(LeaseTracker {
                    active: AtomicUsize::new(0),
                }),
                roles_initialized: Arc::new(OnceCell::new()),
            });
        }

        let shared = state.shared.as_ref().ok_or_else(|| {
            anyhow::anyhow!("container state is missing after successful initialization")
        })?;
        shared.leases.active.fetch_add(1, Ordering::Relaxed);
        (
            ContainerLease {
                maintenance_url: shared.maintenance_url.clone(),
                leases: shared.leases.clone(),
                explicit_release: false,
            },
            shared.roles_initialized.clone(),
        )
    };

    let roles_initialized = roles_initialized
        .get_or_try_init(|| async {
            let mut connection = PgConnection::connect(lease.maintenance_url()).await?;
            sqlx::raw_sql(
                "DO $$ BEGIN \
                    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gpq_admin') THEN \
                        CREATE ROLE gpq_admin NOLOGIN; \
                    END IF; \
                    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gpq_app') THEN \
                        CREATE ROLE gpq_app NOLOGIN NOBYPASSRLS; \
                    END IF; \
                END $$;",
            )
            .execute(&mut connection)
            .await?;
            connection.close().await?;
            Ok::<(), anyhow::Error>(())
        })
        .await;

    if let Err(error) = roles_initialized {
        return match lease.release().await {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(error.context(format!(
                "container lease cleanup also failed: {cleanup_error}"
            ))),
        };
    }

    Ok(lease)
}

/// Requests removal of the shared container. Active fixtures keep it alive;
/// the last fixture to release its lease performs the removal. The request
/// also covers containers started by fixtures that run later.
///
/// # Errors
/// Returns an error if an idle container cannot be removed.
pub async fn reap_shared_container() -> anyhow::Result<()> {
    REAP_REQUESTED.store(true, Ordering::Release);
    cleanup_requested_container().await
}

async fn cleanup_requested_container() -> anyhow::Result<()> {
    let shared = {
        let mut state = CONTAINER.lock().await;
        if !REAP_REQUESTED.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(shared) = state.shared.as_ref() else {
            return Ok(());
        };
        if shared.leases.active.load(Ordering::Acquire) != 0 {
            return Ok(());
        }
        state.shared.take().ok_or_else(|| {
            anyhow::anyhow!("shared container disappeared while holding the cleanup lock")
        })?
    };
    shared.container.rm().await?;
    Ok(())
}
