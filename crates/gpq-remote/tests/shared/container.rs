//! Shared bootstrap for the one `testcontainers`-managed `PostgreSQL` 18
//! container each end-to-end/integration test binary in this crate starts
//! lazily on first use and reuses for every test that binary runs. Included
//! (via `#[path]`) as a private submodule of both `support` and
//! `e2e_support`, so each test binary that pulls in either gets its own
//! independent copy of the container bootstrap below.

use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use tokio::sync::Mutex;

/// This test binary's single shared `PostgreSQL` 18 container, held for the
/// whole process's lifetime.
///
/// Held in an `Option` rather than a write-once cell so [`reap_shared_container`]
/// can take it back out and hand it to `testcontainers`' own removal API: a
/// top-level `static` is never dropped, so `ContainerAsync`'s `Drop` impl —
/// the only cleanup `testcontainers` performs — would otherwise never run.
/// Taking it also makes reaping safe under any test order, because
/// [`maintenance_url`] simply starts a fresh container if the slot is empty.
static CONTAINER: Mutex<Option<SharedContainer>> = Mutex::const_new(None);

/// A started container plus the connection string that reaches it.
struct SharedContainer {
    container: ContainerAsync<PostgresImage>,
    /// Maintenance connection string, using the image's default
    /// `postgres`/`postgres` credentials and `postgres` database.
    maintenance_url: String,
}

/// Starts the shared container on first call and returns its maintenance
/// connection string; later calls reuse the already-running container.
pub async fn maintenance_url() -> anyhow::Result<String> {
    let mut slot = CONTAINER.lock().await;
    if let Some(shared) = slot.as_ref() {
        return Ok(shared.maintenance_url.clone());
    }
    let container = PostgresImage::default()
        .with_tag("18-alpine")
        .start()
        .await?;
    let host = container.get_host().await?;
    let port = container.get_host_port_ipv4(5432).await?;
    let maintenance_url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    *slot = Some(SharedContainer {
        container,
        maintenance_url: maintenance_url.clone(),
    });
    Ok(maintenance_url)
}

/// Removes the shared container through `testcontainers`' own API. Callers
/// must ensure nothing is still using it first — e.g. any open connections,
/// or (for the `e2e`/`lifecycle`/`comfy`/`objectstore` harnesses) the child
/// `gpq-remote`/`gpq-worker` processes.
///
/// # Errors
/// Returns an error if the container is still running but cannot be removed.
pub async fn reap_shared_container() -> anyhow::Result<()> {
    let taken = CONTAINER.lock().await.take();
    if let Some(shared) = taken {
        shared.container.rm().await?;
    }
    Ok(())
}
