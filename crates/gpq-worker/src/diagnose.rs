//! Worker self-diagnosis (`gpq-worker diagnose`).
//!
//! Checks, per Device Pool (ADR 0005): the backend executable exists and is
//! executable, its state directory is writable, configured model paths are
//! present with matching SHA-256 hashes, and the backend answers its
//! required endpoint probes on its loopback address. Also checks Worker
//! Credential presence and storage backend (ADR 0009) and Remote
//! reachability (ADR 0004). The credential's value is never printed.

use std::time::Duration;

use crate::backend;
use crate::config::{PoolConfig, WorkerConfig};
use crate::credential::CredentialStore;
use crate::models::hash_model_fresh;
use crate::process::{self, Ownership};

/// The outcome of one diagnostic check.
#[derive(Debug, Clone)]
pub enum Outcome {
    /// The check passed; the string is a short human-readable detail.
    Ok(String),
    /// The check failed; the string explains why, without secret material.
    Failed(String),
}

impl Outcome {
    /// Whether this outcome represents success.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok(_))
    }
}

/// One labeled diagnostic check and its outcome.
#[derive(Debug, Clone)]
pub struct CheckResult {
    /// Human-readable name of the thing being checked.
    pub label: String,
    /// Whether it passed.
    pub outcome: Outcome,
}

/// The full set of checks performed by `gpq-worker diagnose`.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// Every check performed, in the order they ran.
    pub checks: Vec<CheckResult>,
}

impl Report {
    fn push(&mut self, label: impl Into<String>, outcome: Outcome) {
        self.checks.push(CheckResult {
            label: label.into(),
            outcome,
        });
    }

    /// Whether every check passed.
    #[must_use]
    pub fn all_ok(&self) -> bool {
        self.checks.iter().all(|check| check.outcome.is_ok())
    }

    /// Renders the report as human-readable lines. Never includes the
    /// credential value (ADR 0009).
    #[must_use]
    pub fn render(&self) -> String {
        use std::fmt::Write as _;

        let mut out = String::new();
        for check in &self.checks {
            match &check.outcome {
                Outcome::Ok(detail) => {
                    let _ = writeln!(out, "[ok]   {}: {detail}", check.label);
                }
                Outcome::Failed(detail) => {
                    let _ = writeln!(out, "[fail] {}: {detail}", check.label);
                }
            }
        }
        out
    }
}

/// Runs every diagnostic check against `config`.
///
/// # Errors
///
/// Returns an error only if a check itself cannot even be attempted (never
/// for an ordinary failed check, which is recorded in the returned
/// [`Report`] instead).
pub async fn run(config: &WorkerConfig) -> anyhow::Result<Report> {
    let mut report = Report::default();

    check_credential(config, &mut report);
    check_remote(config, &mut report).await;

    for pool in &config.pools {
        check_executable(pool, &mut report);
        check_state_dir_writable(pool, &mut report).await;
        check_models(pool, &mut report);
        check_managed_process(pool, &mut report).await;
        check_backend(pool, &mut report).await;
    }

    Ok(report)
}

fn check_credential(config: &WorkerConfig, report: &mut Report) {
    let store = CredentialStore::new(&config.name, &config.state_dir);
    let label = "credential";
    match store.load() {
        Ok(Some(_credential)) => report.push(
            label,
            Outcome::Ok(format!(
                "enrolled, stored via {}",
                CredentialStore::describe()
            )),
        ),
        Ok(None) => report.push(
            label,
            Outcome::Failed("not enrolled; run `gpq-worker enroll`".to_owned()),
        ),
        Err(err) => report.push(label, Outcome::Failed(format!("{err:#}"))),
    }
}

async fn check_remote(config: &WorkerConfig, report: &mut Report) {
    let label = "remote reachability";
    match remote_reachable(&config.remote_url).await {
        Ok(detail) => report.push(label, Outcome::Ok(detail)),
        Err(detail) => report.push(label, Outcome::Failed(detail)),
    }
}

async fn remote_reachable(remote_url: &url::Url) -> Result<String, String> {
    let host = remote_url
        .host_str()
        .ok_or_else(|| "remote_url has no host".to_owned())?;
    let port = remote_url
        .port_or_known_default()
        .ok_or_else(|| "remote_url has no resolvable port".to_owned())?;
    let addr = format!("{host}:{port}");
    match tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    {
        Ok(Ok(_stream)) => Ok(format!("connected to {addr}")),
        Ok(Err(err)) => Err(format!("connecting to {addr}: {err}")),
        Err(_) => Err(format!("timed out connecting to {addr}")),
    }
}

fn check_executable(pool: &PoolConfig, report: &mut Report) {
    let label = format!("pool `{}` executable", pool.key);
    match std::fs::metadata(&pool.executable) {
        Ok(metadata) if is_executable(&metadata) => {
            report.push(label, Outcome::Ok(pool.executable.display().to_string()));
        }
        Ok(_) => report.push(
            label,
            Outcome::Failed(format!("{} is not executable", pool.executable.display())),
        ),
        Err(err) => report.push(
            label,
            Outcome::Failed(format!("{}: {err}", pool.executable.display())),
        ),
    }
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    metadata.is_file()
}

async fn check_state_dir_writable(pool: &PoolConfig, report: &mut Report) {
    let label = format!("pool `{}` state dir", pool.key);
    let probe = pool.state_dir.join(".gpq-worker-diagnose-probe");
    match tokio::fs::write(&probe, b"ok").await {
        Ok(()) => {
            if let Err(err) = tokio::fs::remove_file(&probe).await {
                report.push(
                    label,
                    Outcome::Failed(format!("wrote probe file but could not remove it: {err}")),
                );
                return;
            }
            report.push(label, Outcome::Ok(pool.state_dir.display().to_string()));
        }
        Err(err) => report.push(
            label,
            Outcome::Failed(format!("{}: {err}", pool.state_dir.display())),
        ),
    }
}

fn check_models(pool: &PoolConfig, report: &mut Report) {
    for model_path in &pool.model_paths {
        let label = format!("pool `{}` model {}", pool.key, model_path.display());
        if !model_path.is_file() && !model_path.is_dir() {
            report.push(label, Outcome::Failed("path not found".to_owned()));
            continue;
        }
        let Some(expected) = pool.expected_hashes.get(&model_path.display().to_string()) else {
            report.push(
                label,
                Outcome::Ok("present (no expected hash configured)".to_owned()),
            );
            continue;
        };
        match hash_model_fresh(model_path) {
            Ok(actual) if actual.to_hex() == *expected => {
                report.push(label, Outcome::Ok(format!("sha256 {actual} matches")));
            }
            Ok(actual) => report.push(
                label,
                Outcome::Failed(format!(
                    "sha256 mismatch: expected {expected}, computed {actual}"
                )),
            ),
            Err(err) => report.push(label, Outcome::Failed(format!("{err:#}"))),
        }
    }
}

async fn check_backend(pool: &PoolConfig, report: &mut Report) {
    let label = format!("pool `{}` backend", pool.key);
    match backend::build(pool).probe().await {
        Ok(capabilities) => {
            let probes = capabilities
                .probes
                .iter()
                .map(|(name, ok)| format!("{name}={ok}"))
                .collect::<Vec<_>>()
                .join(", ");
            report.push(
                label,
                Outcome::Ok(format!("version {} ({probes})", capabilities.version)),
            );
        }
        Err(err) => report.push(label, Outcome::Failed(err.message)),
    }
}

/// Reports the durably-identified managed process this Worker previously
/// recorded for `pool`, if any, and whether it is still the process that
/// owns that identity (ADR 0005: children are identified durably by PID,
/// start time, and executable identity, never by PID alone). Only reads the
/// persisted identity file; never spawns anything (`diagnose` does no work).
async fn check_managed_process(pool: &PoolConfig, report: &mut Report) {
    let label = format!("pool `{}` managed process", pool.key);
    let identity_file = pool.state_dir.join(crate::pool::PROCESS_IDENTITY_FILE);
    let identity = match process::read_identity_file(&identity_file).await {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            report.push(
                label,
                Outcome::Ok("no managed process recorded yet".to_owned()),
            );
            return;
        }
        Err(err) => {
            report.push(label, Outcome::Failed(format!("{err:#}")));
            return;
        }
    };
    match process::verify_ownership(&identity).await {
        Ok(Ownership::Owned) => report.push(
            label,
            Outcome::Ok(format!(
                "pid {} ({}) is running and owned by this worker",
                identity.pid,
                identity.executable.display()
            )),
        ),
        Ok(Ownership::Foreign) => report.push(
            label,
            Outcome::Failed(format!(
                "pid {} has been reused by an unrelated process since this worker last recorded it (ADR 0005: never adopted)",
                identity.pid
            )),
        ),
        Ok(Ownership::Gone) => report.push(
            label,
            Outcome::Ok(format!(
                "pid {} ({}) recorded but no longer running",
                identity.pid,
                identity.executable.display()
            )),
        ),
        Err(err) => report.push(label, Outcome::Failed(format!("{err:#}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, Report};

    #[test]
    fn all_ok_is_true_only_when_every_check_passed() {
        let mut report = Report::default();
        assert!(report.all_ok(), "an empty report has no failing checks");

        report.push("a", Outcome::Ok("fine".to_owned()));
        assert!(report.all_ok());

        report.push("b", Outcome::Failed("broken".to_owned()));
        assert!(!report.all_ok());
    }

    #[test]
    fn render_marks_each_outcome() {
        let mut report = Report::default();
        report.push("a", Outcome::Ok("fine".to_owned()));
        report.push("b", Outcome::Failed("broken".to_owned()));

        let rendered = report.render();
        assert!(rendered.contains("[ok]   a: fine"));
        assert!(rendered.contains("[fail] b: broken"));
    }
}
