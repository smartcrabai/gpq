//! `gpq-worker`: a tenant-owned host agent that supervises llama.cpp,
//! mlx-dspark, and `ComfyUI` subprocesses and executes Attempts leased from
//! `gpq-remote` (CONTEXT.md, ADR 0005).
//!
//! `run` is the single foreground implementation (ADR 0020); `enroll`,
//! `diagnose`, and `service install|uninstall|start|stop` are the only other
//! entry points, matching ADR 0009's command surface.

mod artifacts;
mod backend;
mod config;
mod credential;
mod diagnose;
mod executor;
mod models;
mod pool;
mod process;
mod service;
mod session;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, ensure};
use clap::{Parser, Subcommand};
use config::WorkerConfig;
use credential::CredentialStore;

/// Command-line interface for `gpq-worker`.
#[derive(Debug, Parser)]
#[command(name = "gpq-worker", version, about = "GPU Generation Queue Worker")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Path to a Worker TOML configuration file, shared by every subcommand that
/// needs one.
#[derive(Debug, clap::Args)]
struct ConfigArgs {
    /// Path to the Worker TOML configuration file.
    #[arg(long, short = 'c')]
    config: PathBuf,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// One-time enrollment authenticated by the Tenant Master Key.
    ///
    /// The Master Key is read from standard input only; it is never accepted
    /// as a command-line argument or environment variable (ADR 0009). On
    /// success the returned Worker Credential is stored in this platform's
    /// secret store and the assigned Worker id is printed.
    Enroll(ConfigArgs),
    /// Runs the Worker in the foreground.
    ///
    /// This is the single implementation every OS service wrapper invokes
    /// (ADR 0020): systemd on Linux, launchd (and Homebrew services) on
    /// macOS, and the native Windows Service on Windows all just start this
    /// same command.
    Run(ConfigArgs),
    /// Checks executables, state directories, model paths, backend
    /// reachability, credential storage, and Remote reachability without
    /// executing any work. Exits non-zero if any check fails.
    Diagnose(ConfigArgs),
    /// Installs, starts, stops, or removes the OS service wrapper around
    /// `gpq-worker run` (ADR 0020).
    #[command(subcommand)]
    Service(ServiceCommand),
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    /// Installs the OS service wrapper around `gpq-worker run`.
    ///
    /// On macOS this writes the same launchd agent plist that Homebrew's
    /// `brew services start gpq-worker` loads, so both entry points manage
    /// one underlying unit rather than two.
    #[command(
        long_about = "Installs the OS service wrapper around `gpq-worker run`.\n\n\
        On macOS this writes the same launchd agent plist that Homebrew's \
        `brew services start gpq-worker` loads, so both entry points manage \
        one underlying unit rather than two."
    )]
    Install(ConfigArgs),
    /// Stops (if running) and removes the OS service wrapper, then deletes
    /// this Worker's stored credential (ADR 0009: a revocable credential
    /// must not outlive the service that held it).
    Uninstall(ConfigArgs),
    /// Starts the installed OS service.
    Start,
    /// Stops the installed OS service.
    Stop,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    dispatch(cli.command).await
}

async fn dispatch(command: Command) -> anyhow::Result<()> {
    match command {
        Command::Enroll(args) => enroll(&args.config).await,
        Command::Run(args) => run_worker(&args.config).await,
        Command::Diagnose(args) => diagnose_cmd(&args.config).await,
        Command::Service(cmd) => service_cmd(cmd).await,
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .init();
}

/// Enrolls this Worker with Remote using a Tenant Master Key read from
/// standard input, then stores the returned Worker Credential (ADR 0009).
async fn enroll(config_path: &Path) -> anyhow::Result<()> {
    let config = WorkerConfig::load(config_path)?;
    let master_key = read_master_key_from_stdin()?;

    let response = enroll_with_remote(&config, &master_key)
        .await
        .context("enrolling with remote")?;

    let store = CredentialStore::new(&config.name, &config.state_dir);
    store
        .store(&response.worker_credential)
        .context("storing worker credential")?;

    println!(
        "enrolled worker `{}` (id {}) for tenant {}",
        config.name, response.worker_id, response.tenant_id
    );
    Ok(())
}

/// Reads the Tenant Master Key from standard input. Never accepted any other
/// way (ADR 0009).
fn read_master_key_from_stdin() -> anyhow::Result<String> {
    use std::io::Write as _;

    eprint!("Tenant Master Key: ");
    std::io::stderr()
        .flush()
        .context("flushing the master key prompt")?;

    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("reading the Tenant Master Key from stdin")?;
    let key = line.trim().to_owned();
    ensure!(!key.is_empty(), "no Tenant Master Key provided on stdin");
    Ok(key)
}

async fn enroll_with_remote(
    config: &WorkerConfig,
    master_key: &str,
) -> anyhow::Result<gpq_proto::gpq::worker::v1::EnrollResponse> {
    let uri: http::Uri = config
        .remote_url
        .as_str()
        .parse()
        .with_context(|| format!("parsing remote_url `{}` as a URI", config.remote_url))?;

    let connection = connectrpc::client::Http2Connection::connect_plaintext(uri.clone())
        .await
        .with_context(|| format!("connecting to remote at {uri}"))?;
    let transport = connection.shared(16);
    let client_config = connectrpc::client::ClientConfig::new(uri)
        .with_protocol(connectrpc::Protocol::Grpc)
        .with_default_header("authorization", format!("Bearer {master_key}"));
    let client =
        gpq_proto::gpq::worker::v1::WorkerEnrollmentServiceClient::new(transport, client_config);

    let request = gpq_proto::gpq::worker::v1::EnrollRequest {
        worker_name: config.name.clone(),
        host_descriptor: host_descriptor(),
        protocol_major: gpq_proto::PROTOCOL_MAJOR,
        protocol_minor: gpq_proto::PROTOCOL_MINOR,
        worker_version: env!("CARGO_PKG_VERSION").to_owned(),
        ..Default::default()
    };

    let response = client
        .enroll(request)
        .await
        .context("calling the Enroll RPC")?;
    Ok(response.into_owned())
}

/// A stable host identity: OS, architecture, and hostname (ADR 0004).
fn host_descriptor() -> String {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let hostname = hostname().unwrap_or_else(|| "unknown".to_owned());
    format!("{os}/{arch}/{hostname}")
}

#[cfg(unix)]
fn hostname() -> Option<String> {
    let mut buf = vec![0_u8; 256];
    // SAFETY: `buf` is valid for `buf.len()` writable bytes; `gethostname`
    // writes at most that many bytes including its NUL terminator.
    let result = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if result != 0 {
        return None;
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    buf.truncate(end);
    String::from_utf8(buf).ok()
}

#[cfg(windows)]
fn hostname() -> Option<String> {
    std::env::var("COMPUTERNAME").ok()
}

/// Loads configuration and the stored credential, starts every configured
/// Device Pool, and runs the Worker Session until a shutdown signal arrives
/// or the connection irrecoverably fails.
async fn run_worker(config_path: &Path) -> anyhow::Result<()> {
    let config = WorkerConfig::load(config_path)?;
    let store = CredentialStore::new(&config.name, &config.state_dir);
    let Some(credential) = store.load().context("loading worker credential")? else {
        anyhow::bail!(
            "worker `{}` is not enrolled; run `gpq-worker enroll --config {}` first",
            config.name,
            config_path.display()
        );
    };

    let pools = pool::PoolSupervisor::start(&config)
        .await
        .context("starting device pools")?;
    let pools = Arc::new(pools);
    let config = Arc::new(config);

    let shutdown = tokio_util::sync::CancellationToken::new();
    let watcher_token = shutdown.clone();
    let signal_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        tracing::info!("shutdown signal received, cancelling running attempts");
        watcher_token.cancel();
    });

    let result = session::run(config, credential, Arc::clone(&pools), shutdown.clone()).await;

    shutdown.cancel();
    signal_task.abort();
    pools.shutdown().await;

    result
}

/// Waits for SIGINT (all platforms) or SIGTERM (Unix), whichever arrives
/// first, so `run` can shut down gracefully under every OS service manager
/// (ADR 0020).
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(err) => {
                tracing::warn!(%err, "failed to install SIGTERM handler, watching SIGINT only");
                if let Err(err) = tokio::signal::ctrl_c().await {
                    tracing::warn!(%err, "failed to watch for SIGINT");
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::warn!(%err, "failed to watch for Ctrl-C");
        }
    }
}

/// Runs every diagnostic check and prints a human-readable report, exiting
/// non-zero (via an `Err`) when any check failed.
async fn diagnose_cmd(config_path: &Path) -> anyhow::Result<()> {
    let config = WorkerConfig::load(config_path)?;
    let report = diagnose::run(&config).await?;
    print!("{}", report.render());
    ensure!(report.all_ok(), "one or more diagnostic checks failed");
    Ok(())
}

async fn service_cmd(command: ServiceCommand) -> anyhow::Result<()> {
    match command {
        ServiceCommand::Install(args) => {
            let config = WorkerConfig::load(&args.config)?;
            let binary =
                std::env::current_exe().context("locating the current gpq-worker executable")?;
            let config_path = std::fs::canonicalize(&args.config)
                .with_context(|| format!("resolving {}", args.config.display()))?;
            let credential_file =
                CredentialStore::new(&config.name, &config.state_dir).fallback_path();
            service::install(&binary, &config_path, &credential_file)
                .await
                .context("installing the OS service")?;
            println!("installed gpq-worker as an OS service");
            Ok(())
        }
        ServiceCommand::Uninstall(args) => {
            let config = WorkerConfig::load(&args.config)?;
            // The Worker Credential must never outlive the service that
            // held it (ADR 0009), so credential deletion is attempted even
            // if removing the OS service wrapper failed (unit already
            // removed manually, a permission error, an already-stopped
            // launchd agent, ...); neither failure is allowed to hide the
            // other.
            let service_result = service::uninstall()
                .await
                .context("uninstalling the OS service");
            let credential_result = CredentialStore::new(&config.name, &config.state_dir)
                .delete()
                .context("deleting the stored worker credential");
            combine_uninstall_results(service_result, credential_result)?;
            println!("uninstalled gpq-worker OS service and removed its stored credential");
            Ok(())
        }
        ServiceCommand::Start => service::start().await.context("starting the OS service"),
        ServiceCommand::Stop => service::stop().await.context("stopping the OS service"),
    }
}

/// Combines the outcome of removing the OS service wrapper with the outcome
/// of deleting the stored Worker Credential (ADR 0009: a revocable
/// credential must not outlive the service that held it). Both operations
/// are always attempted regardless of one another; if both fail, the first
/// failure is never allowed to hide the second.
fn combine_uninstall_results(
    service_result: anyhow::Result<()>,
    credential_result: anyhow::Result<()>,
) -> anyhow::Result<()> {
    match (service_result, credential_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(service_err), Ok(())) => Err(service_err),
        (Ok(()), Err(credential_err)) => Err(credential_err),
        (Err(service_err), Err(credential_err)) => anyhow::bail!(
            "uninstalling the OS service failed ({service_err:#}); \
             deleting the stored worker credential also failed ({credential_err:#})"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::combine_uninstall_results;

    #[test]
    fn combine_uninstall_results_ok_when_both_succeed() {
        let Ok(()) = combine_uninstall_results(Ok(()), Ok(())) else {
            panic!("expected two successes to combine into Ok");
        };
    }

    #[test]
    fn combine_uninstall_results_surfaces_service_failure_alone() {
        let Err(err) =
            combine_uninstall_results(Err(anyhow::anyhow!("systemctl disable failed")), Ok(()))
        else {
            panic!("expected the service failure to surface");
        };
        assert_eq!(err.to_string(), "systemctl disable failed");
    }

    #[test]
    fn combine_uninstall_results_surfaces_credential_failure_alone() {
        let Err(err) = combine_uninstall_results(Ok(()), Err(anyhow::anyhow!("keyring locked")))
        else {
            panic!("expected the credential failure to surface");
        };
        assert_eq!(err.to_string(), "keyring locked");
    }

    #[test]
    fn combine_uninstall_results_reports_both_failures_when_neither_step_succeeds() {
        let Err(err) = combine_uninstall_results(
            Err(anyhow::anyhow!("systemctl disable failed")),
            Err(anyhow::anyhow!("keyring locked")),
        ) else {
            panic!("expected both failures to combine into one error");
        };
        let message = err.to_string();
        assert!(message.contains("systemctl disable failed"));
        assert!(message.contains("keyring locked"));
    }
}
