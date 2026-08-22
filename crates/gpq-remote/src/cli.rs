//! Command-line entry point for `gpq-remote`.
//!
//! ADR 0016: schema migration and network-facing serving are separate
//! subcommands using separate `PostgreSQL` credentials. ADR 0009: Tenant and
//! Worker lifecycle/credential administration are local commands, distinct
//! from the always-on `serve` process. ADR 0019: `serve` speaks plaintext
//! HTTP/1.1 and h2c; TLS terminates at an ingress in front of it.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use gpq_domain::{TenantId, WorkerId};
use uuid::Uuid;

use crate::artifacts::ArtifactService;
use crate::config::RemoteConfig;
use crate::db::Db;
use crate::events::EventHub;
use crate::registry::WorkerRegistry;
use crate::state::AppState;

/// GPU Generation Queue coordinator.
#[derive(Parser)]
#[command(name = "gpq-remote", about = "GPU Generation Queue coordinator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs embedded schema migrations with the schema-owner credential
    /// (ADR 0016). Reads `GPQ_DATABASE_URL` directly, expected to name the
    /// schema-owner connection string rather than the forced-RLS role
    /// `serve` uses.
    Migrate,
    /// Serves OpenAI-compatible, Native, and Worker RPC traffic.
    Serve,
    /// Tenant lifecycle and credential administration (ADR 0009).
    Tenant {
        #[command(subcommand)]
        command: TenantCommand,
    },
    /// Worker administration.
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
}

#[derive(Subcommand)]
enum TenantCommand {
    /// Creates a Tenant with default settings.
    Create {
        #[arg(long)]
        name: String,
    },
    /// Lists every Tenant, including soft-deleted ones.
    List,
    /// Soft-deletes a Tenant.
    Delete {
        #[arg(long)]
        id: Uuid,
    },
    /// Tenant Master Key administration.
    Key {
        #[command(subcommand)]
        command: TenantKeyCommand,
    },
}

#[derive(Subcommand)]
enum TenantKeyCommand {
    /// Issues a new Tenant Master Key and prints the secret to stdout
    /// exactly once — it is never recoverable afterward (ADR 0009).
    Rotate {
        #[arg(long)]
        tenant: Uuid,
        #[arg(long, default_value = "")]
        label: String,
        /// Optional expiry, in days from now.
        #[arg(long)]
        expires_in_days: Option<i64>,
    },
    /// Revokes a Tenant Master Key. Idempotent.
    Revoke {
        #[arg(long)]
        tenant: Uuid,
        #[arg(long)]
        key_id: Uuid,
    },
    /// Lists a Tenant's Master Keys (never their secrets).
    List {
        #[arg(long)]
        tenant: Uuid,
    },
}

#[derive(Subcommand)]
enum WorkerCommand {
    /// Lists a Tenant's Workers.
    List {
        #[arg(long)]
        tenant: Uuid,
    },
    /// Revokes a Worker's credential. Idempotent.
    Revoke {
        #[arg(long)]
        tenant: Uuid,
        #[arg(long)]
        worker: Uuid,
    },
}

/// Parses CLI arguments and dispatches to the selected subcommand.
///
/// # Errors
///
/// Returns an error if telemetry initialization fails, or if the dispatched
/// subcommand fails: `migrate` when `GPQ_DATABASE_URL` is unset, `PostgreSQL`
/// is unreachable, or a migration fails to apply; `serve` when required
/// environment configuration is missing or invalid, the database is
/// unreachable or its schema does not match what `serve` expects, the
/// Artifact store cannot be initialized, the listen address cannot be
/// bound, or the HTTP server itself errors; `tenant`/`worker` when required
/// environment configuration is missing, the database is unreachable, or
/// the underlying Tenant/Worker/credential operation fails.
pub async fn run() -> anyhow::Result<()> {
    let _telemetry =
        crate::telemetry::init("gpq-remote").context("failed to initialize telemetry")?;
    let cli = Cli::parse();
    match cli.command {
        Command::Migrate => run_migrate().await,
        Command::Serve => run_serve().await,
        Command::Tenant { command } => run_tenant(command).await,
        Command::Worker { command } => run_worker(command).await,
    }
}

/// Runs embedded migrations against `GPQ_DATABASE_URL` (ADR 0016: the
/// schema-owner credential, not the forced-RLS one `serve` uses).
async fn run_migrate() -> anyhow::Result<()> {
    let database_url = std::env::var("GPQ_DATABASE_URL").context(
        "GPQ_DATABASE_URL is required (the schema-owner connection string for `migrate`)",
    )?;
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .context("failed to connect to PostgreSQL")?;
    crate::db::MIGRATOR
        .run(&pool)
        .await
        .context("failed to run migrations")?;
    println!("migrations applied");
    Ok(())
}

/// Ceiling on how long `serve` waits, after a shutdown signal, for in-flight
/// HTTP/Connect traffic to drain before forcing the process to exit anyway.
/// Axum's graceful shutdown otherwise waits indefinitely, and a Worker's
/// `WorkerSessionService::Session` control stream is an indefinite
/// bidirectional stream that only ends when the Worker disconnects or
/// observes `AppState::shutdown` — without this ceiling, every SIGTERM or
/// SIGINT received while a Worker is attached would end in a hard kill
/// instead of a bounded, clean drain.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounded wait for the `expiry` background loop to notice
/// `AppState::shutdown` and exit on its own between sweep ticks, so an
/// in-flight per-tenant sweep transaction can commit instead of being cut
/// off mid-transaction. `abort` remains the fallback if it does not.
const BACKGROUND_TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Serves OpenAI-compatible, Native, and Worker RPC traffic until a shutdown
/// signal arrives.
async fn run_serve() -> anyhow::Result<()> {
    let config = Arc::new(RemoteConfig::from_env()?);
    let db = Db::connect(&config).await?;
    db.assert_schema_current()
        .await
        .context("refusing to serve against an unexpected schema version")?;
    let artifacts = ArtifactService::new(&config).await;
    let shutdown = tokio_util::sync::CancellationToken::new();

    // `crate::scheduler::spawn` needs a full `AppState` to reach the database,
    // events, and Worker registry, but its own return value is what fills
    // `AppState.scheduler` — bootstrap with an inert handle nothing outside
    // this function ever observes, then rebuild the real, shared state with
    // the handle `spawn` returns.
    let bootstrap = AppState {
        config,
        db,
        events: EventHub::default(),
        workers: WorkerRegistry::default(),
        artifacts,
        scheduler: crate::scheduler::SchedulerHandle::inert(),
        shutdown: shutdown.clone(),
    };

    // ADR 0003: cancel every nonterminal synchronous OpenAI Generation before
    // accepting Worker sessions, so a lost client connection cannot leave
    // invisible work running across a restart.
    crate::expiry::cancel_synchronous_on_startup(&bootstrap)
        .await
        .context("failed to cancel nonterminal synchronous Generations on startup")?;

    let (scheduler, scheduler_task) = crate::scheduler::spawn(bootstrap.clone());
    let state = AppState {
        scheduler,
        ..bootstrap
    };
    let mut expiry_task = crate::expiry::spawn(state.clone());

    let listener = tokio::net::TcpListener::bind(state.config.bind_addr)
        .await
        .with_context(|| format!("failed to bind {}", state.config.bind_addr))?;
    tracing::info!(addr = %state.config.bind_addr, "gpq-remote listening");

    let app = crate::http::router(state);
    let serve = std::future::IntoFuture::into_future(
        axum::serve(listener, app).with_graceful_shutdown(shutdown_signal(shutdown.clone())),
    );
    tokio::pin!(serve);

    // The ceiling must bound the *drain*, not the server's whole lifetime:
    // arming it before the signal would make `serve` exit on its own after
    // `SHUTDOWN_DRAIN_TIMEOUT` of perfectly healthy operation.
    let drain_deadline = async {
        shutdown.cancelled().await;
        tokio::time::sleep(SHUTDOWN_DRAIN_TIMEOUT).await;
    };
    tokio::select! {
        result = &mut serve => result.context("HTTP server error")?,
        () = drain_deadline => {
            tracing::warn!(
                timeout = ?SHUTDOWN_DRAIN_TIMEOUT,
                "graceful shutdown drain deadline exceeded with connections still open; forcing shutdown"
            );
        }
    }

    if tokio::time::timeout(BACKGROUND_TASK_SHUTDOWN_GRACE, &mut expiry_task)
        .await
        .is_err()
    {
        tracing::warn!(
            timeout = ?BACKGROUND_TASK_SHUTDOWN_GRACE,
            "expiry loop did not exit within the shutdown grace period; aborting it"
        );
        expiry_task.abort();
    }
    // The scheduler ticker (`scheduler.rs`) has no cooperative shutdown of
    // its own to observe `AppState::shutdown`, so it is always hard-aborted
    // here, same as before this function grew a bounded drain.
    scheduler_task.abort();
    Ok(())
}

/// Resolves on SIGINT/SIGTERM, cancelling `shutdown` so every consumer of
/// `AppState::shutdown` (Worker `Session` streams, the `expiry` sweep loop)
/// starts winding down at the same moment `serve` begins its own graceful
/// drain.
async fn shutdown_signal(shutdown: tokio_util::sync::CancellationToken) {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        else {
            return;
        };
        signal.recv().await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    tracing::info!("shutdown signal received; cancelling background work and draining connections");
    shutdown.cancel();
}

async fn run_tenant(command: TenantCommand) -> anyhow::Result<()> {
    let config = RemoteConfig::from_env()?;
    let db = Db::connect(&config).await?;
    match command {
        TenantCommand::Create { name } => {
            let mut tx = db
                .begin()
                .await
                .context("failed to open an administration transaction")?;
            let id = crate::db::tenants::create_tenant(&mut tx, &name)
                .await
                .context("failed to create Tenant")?;
            tx.commit().await.context("failed to commit")?;
            println!("{id}");
        }
        TenantCommand::List => {
            let mut tx = db
                .begin()
                .await
                .context("failed to open an administration transaction")?;
            let tenants = crate::db::tenants::list_tenants(&mut tx)
                .await
                .context("failed to list Tenants")?;
            tx.commit().await.context("failed to commit")?;
            for tenant in tenants {
                let status = if tenant.deleted_at.is_some() {
                    "deleted"
                } else {
                    "active"
                };
                println!(
                    "{}\t{}\t{}\t{status}",
                    tenant.id, tenant.name, tenant.created_at
                );
            }
        }
        TenantCommand::Delete { id } => {
            let tenant = TenantId::from_uuid(id);
            let mut tx = db
                .begin()
                .await
                .context("failed to open an administration transaction")?;
            crate::db::tenants::delete_tenant(&mut tx, tenant)
                .await
                .context("failed to delete Tenant")?;
            tx.commit().await.context("failed to commit")?;
            println!("tenant {tenant} deleted");
        }
        TenantCommand::Key { command } => run_tenant_key(&db, command).await?,
    }
    Ok(())
}

async fn run_tenant_key(db: &Db, command: TenantKeyCommand) -> anyhow::Result<()> {
    match command {
        TenantKeyCommand::Rotate {
            tenant,
            label,
            expires_in_days,
        } => {
            let tenant = TenantId::from_uuid(tenant);
            let secret = crate::auth::generate_secret("gpq_mk");
            let digest = db.hasher().hash(&secret);
            let expires_at =
                expires_in_days.map(|days| chrono::Utc::now() + chrono::Duration::days(days));
            let mut tx = db
                .begin_tenant(tenant)
                .await
                .context("failed to open a Tenant transaction")?;
            let key_id =
                crate::db::tenants::insert_master_key(&mut tx, tenant, &digest, &label, expires_at)
                    .await
                    .context("failed to insert the Master Key")?;
            tx.commit().await.context("failed to commit")?;
            println!("key id: {key_id}");
            // Printed once, to stdout only: this is the only time the secret
            // is ever recoverable (ADR 0009). Never logged.
            println!("{secret}");
        }
        TenantKeyCommand::Revoke { tenant, key_id } => {
            let tenant = TenantId::from_uuid(tenant);
            let mut tx = db
                .begin_tenant(tenant)
                .await
                .context("failed to open a Tenant transaction")?;
            crate::db::tenants::revoke_master_key(&mut tx, tenant, key_id)
                .await
                .context("failed to revoke the Master Key")?;
            tx.commit().await.context("failed to commit")?;
            println!("key {key_id} revoked");
        }
        TenantKeyCommand::List { tenant } => {
            let tenant = TenantId::from_uuid(tenant);
            let mut tx = db
                .begin_tenant(tenant)
                .await
                .context("failed to open a Tenant transaction")?;
            let keys = crate::db::tenants::list_master_keys(&mut tx, tenant)
                .await
                .context("failed to list Master Keys")?;
            tx.commit().await.context("failed to commit")?;
            for key in keys {
                let status = if key.revoked_at.is_some() {
                    "revoked"
                } else {
                    "live"
                };
                let expires = key
                    .expires_at
                    .map_or_else(|| "never".to_owned(), |at| at.to_rfc3339());
                println!(
                    "{}\t{}\t{}\t{status}\texpires={expires}",
                    key.id, key.label, key.created_at
                );
            }
        }
    }
    Ok(())
}

async fn run_worker(command: WorkerCommand) -> anyhow::Result<()> {
    let config = RemoteConfig::from_env()?;
    let db = Db::connect(&config).await?;
    match command {
        WorkerCommand::List { tenant } => {
            let tenant = TenantId::from_uuid(tenant);
            let mut tx = db
                .begin_tenant(tenant)
                .await
                .context("failed to open a Tenant transaction")?;
            let workers = crate::db::workers::list_workers(&mut tx, tenant)
                .await
                .context("failed to list Workers")?;
            tx.commit().await.context("failed to commit")?;
            for worker in workers {
                let status = if worker.revoked_at.is_some() {
                    "revoked"
                } else {
                    "active"
                };
                let last_seen = worker
                    .last_seen_at
                    .map_or_else(|| "never".to_string(), |seen| seen.to_string());
                println!(
                    "{}\t{}\t{}\t{last_seen}\t{status}",
                    worker.id(),
                    worker.name,
                    worker.worker_version
                );
            }
        }
        WorkerCommand::Revoke { tenant, worker } => {
            let tenant = TenantId::from_uuid(tenant);
            let worker_id = WorkerId::from_uuid(worker);
            let mut tx = db
                .begin_tenant(tenant)
                .await
                .context("failed to open a Tenant transaction")?;
            crate::db::workers::revoke_worker(&mut tx, tenant, worker_id)
                .await
                .context("failed to revoke the Worker")?;
            tx.commit().await.context("failed to commit")?;
            println!("worker {worker_id} revoked");
        }
    }
    Ok(())
}
