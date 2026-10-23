//! Worker self-diagnosis (`gpq worker diagnose`).
//!
//! Checks the single executable, configured asset hashes, accelerator
//! availability, managed-child ownership, state directories, credential, and
//! Remote reachability. Never prints any credential or the child bearer token.

use std::ffi::OsString;
use std::path::Path;
use std::time::Duration;

use crate::config::{PoolConfig, WorkerConfig};
use crate::credential::CredentialStore;
use crate::models;
use crate::process::{self, ManagedProcess, Ownership};

const DIAGNOSTIC_PROCESS_GRACE: Duration = Duration::from_secs(10);
const DIAGNOSTIC_PROCESS_IDENTITY_FILE: &str = ".gpq-diagnose-process-identity.json";

#[derive(Debug, Clone)]
pub enum Outcome {
    Ok(String),
    Failed(String),
}

impl Outcome {
    #[must_use]
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok(_))
    }
}

#[derive(Debug, Clone)]
pub struct CheckResult {
    pub label: String,
    pub outcome: Outcome,
}

#[derive(Debug, Clone, Default)]
pub struct Report {
    pub checks: Vec<CheckResult>,
}

impl Report {
    fn push(&mut self, label: impl Into<String>, outcome: Outcome) {
        self.checks.push(CheckResult {
            label: label.into(),
            outcome,
        });
    }

    #[must_use]
    pub fn all_ok(&self) -> bool {
        self.checks.iter().all(|check| check.outcome.is_ok())
    }

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

pub async fn run(config: &WorkerConfig, config_path: &Path) -> anyhow::Result<Report> {
    let mut report = Report::default();
    check_credential(config, &mut report);
    check_executable(&mut report);
    check_remote(config, &mut report).await;
    for pool in &config.pools {
        check_state_dir_writable(pool, &mut report).await;
        check_models(pool, &mut report);
        check_managed_process(pool, &mut report).await;
        check_accelerator(pool, config_path, &mut report).await;
    }
    Ok(report)
}

fn check_credential(config: &WorkerConfig, report: &mut Report) {
    let store = CredentialStore::new(&config.name, &config.state_dir, config.credential_storage);
    match store.load() {
        Ok(Some(_)) => report.push(
            "credential",
            Outcome::Ok(format!(
                "enrolled, stored via {}",
                CredentialStore::describe(config.credential_storage)
            )),
        ),
        Ok(None) => report.push(
            "credential",
            Outcome::Failed("not enrolled; run `gpq worker enroll`".to_owned()),
        ),
        Err(error) => report.push("credential", Outcome::Failed(format!("{error:#}"))),
    }
}

fn check_executable(report: &mut Report) {
    let label = "gpq executable";
    match std::env::current_exe() {
        Ok(path) => match std::fs::metadata(&path) {
            Ok(metadata) if is_executable(&metadata) => {
                report.push(label, Outcome::Ok(path.display().to_string()));
            }
            Ok(_) => report.push(
                label,
                Outcome::Failed(format!("{} is not executable", path.display())),
            ),
            Err(error) => report.push(label, Outcome::Failed(error.to_string())),
        },
        Err(error) => report.push(label, Outcome::Failed(error.to_string())),
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

async fn check_remote(config: &WorkerConfig, report: &mut Report) {
    match remote_reachable(&config.remote_url).await {
        Ok(detail) => report.push("remote reachability", Outcome::Ok(detail)),
        Err(detail) => report.push("remote reachability", Outcome::Failed(detail)),
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
        Ok(Ok(_)) => Ok(format!("connected to {addr}")),
        Ok(Err(error)) => Err(format!("connecting to {addr}: {error}")),
        Err(_) => Err(format!("timed out connecting to {addr}")),
    }
}

async fn check_state_dir_writable(pool: &PoolConfig, report: &mut Report) {
    let label = format!("pool `{}` state dir", pool.key);
    let probe = pool.state_dir.join(".gpq-diagnose-probe");
    match tokio::fs::write(&probe, b"ok").await {
        Ok(()) => match tokio::fs::remove_file(&probe).await {
            Ok(()) => report.push(label, Outcome::Ok(pool.state_dir.display().to_string())),
            Err(error) => report.push(
                label,
                Outcome::Failed(format!("wrote probe file but could not remove it: {error}")),
            ),
        },
        Err(error) => report.push(
            label,
            Outcome::Failed(format!("{}: {error}", pool.state_dir.display())),
        ),
    }
}

fn check_models(pool: &PoolConfig, report: &mut Report) {
    let label = format!("pool `{}` native model assets", pool.key);
    match models::scan_models(pool) {
        Ok((models, image_loras)) => report.push(
            label,
            Outcome::Ok(format!(
                "{} immutable model bundle(s), {} image LoRA(s) hashed",
                models.len(),
                image_loras.len()
            )),
        ),
        Err(error) => report.push(label, Outcome::Failed(format!("{error:#}"))),
    }
}

async fn check_accelerator(pool: &PoolConfig, config_path: &Path, report: &mut Report) {
    let label = format!("pool `{}` native accelerator configuration", pool.key);
    let outcome = match run_accelerator_probe(pool, config_path).await {
        Ok(detail) => Outcome::Ok(detail),
        Err(detail) => Outcome::Failed(detail),
    };
    report.push(label, outcome);
}

async fn run_accelerator_probe(pool: &PoolConfig, config_path: &Path) -> Result<String, String> {
    let executable =
        std::env::current_exe().map_err(|error| format!("resolving gpq executable: {error}"))?;
    let args = [
        OsString::from("infer"),
        OsString::from("--config"),
        config_path.as_os_str().to_os_string(),
        OsString::from("--pool"),
        pool.key.clone().into(),
        OsString::from("--diagnose"),
    ];
    let process = ManagedProcess::spawn(
        &executable,
        &args,
        &pool.env,
        &pool.state_dir,
        DIAGNOSTIC_PROCESS_IDENTITY_FILE,
    )
    .await
    .map_err(|error| format!("spawning isolated device probe: {error:#}"))?;
    let mut process = process::ProcessGuard::new(process, DIAGNOSTIC_PROCESS_GRACE);
    #[cfg(windows)]
    if let Some(error) = process.startup_error().map(str::to_owned) {
        let cleanup = stop_diagnostic_process(process).await;
        return Err(format!(
            "cannot supervise isolated device probe with a Windows Job Object: {error}; cleanup: {}",
            cleanup.map_or_else(
                |error| format!("failed: {error}"),
                |()| "complete".to_owned()
            )
        ));
    }
    let stdout = process.take_stdout().map(|mut stdout| {
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt as _;
            let mut output = String::new();
            stdout.read_to_string(&mut output).await.map(|_| output)
        })
    });
    let stderr = process.take_stderr().map(|mut stderr| {
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt as _;
            let mut output = String::new();
            stderr.read_to_string(&mut output).await.map(|_| output)
        })
    });
    let (failure, exited) =
        match tokio::time::timeout(pool.startup_timeout, process.wait_exit()).await {
            Ok(Ok(status)) if status.success() => (None, true),
            Ok(Ok(status)) => (
                Some(format!("isolated device probe exited with {status}")),
                true,
            ),
            Ok(Err(error)) => (
                Some(format!("waiting for isolated device probe: {error:#}")),
                false,
            ),
            Err(_) => (
                Some(format!(
                    "isolated device probe exceeded {:?}",
                    pool.startup_timeout
                )),
                false,
            ),
        };
    let cleanup = if exited {
        drop(process.disarm());
        None
    } else {
        Some(stop_diagnostic_process(process).await)
    };
    let output = if cleanup.as_ref().is_none_or(Result::is_ok) {
        collect_child_output(stdout, stderr)
            .await
            .map_err(|error| format!("reading isolated device probe output: {error:#}"))?
    } else {
        String::new()
    };
    match failure {
        None => Ok(output.trim().to_owned()),
        Some(failure) => Err(format!(
            "{failure}; cleanup: {}; {output}",
            cleanup.unwrap_or(Ok(())).map_or_else(
                |error| format!("failed: {error}"),
                |()| "complete".to_owned()
            )
        )),
    }
}

async fn stop_diagnostic_process(mut process: process::ProcessGuard) -> Result<(), String> {
    match process.terminate_tree(DIAGNOSTIC_PROCESS_GRACE).await {
        Ok(()) => {
            drop(process.disarm());
            Ok(())
        }
        Err(error) => {
            match tokio::time::timeout(DIAGNOSTIC_PROCESS_GRACE, process.wait_exit()).await {
                Ok(Ok(_)) => {
                    drop(process.disarm());
                    Err(format!(
                        "{error:#}; child exited, but process-tree cleanup was not verified"
                    ))
                }
                Ok(Err(wait_error)) => Err(format!(
                    "{error:#}; waiting for child exit failed: {wait_error:#}"
                )),
                Err(_) => Err(format!("{error:#}; child did not exit after termination")),
            }
        }
    }
}

async fn collect_child_output(
    stdout: Option<tokio::task::JoinHandle<std::io::Result<String>>>,
    stderr: Option<tokio::task::JoinHandle<std::io::Result<String>>>,
) -> anyhow::Result<String> {
    let stdout = match stdout {
        Some(stdout) => stdout.await??,
        None => String::new(),
    };
    let stderr = match stderr {
        Some(stderr) => stderr.await??,
        None => String::new(),
    };
    Ok(if stderr.trim().is_empty() {
        stdout
    } else if stdout.trim().is_empty() {
        stderr
    } else {
        format!("{stdout}\n{stderr}")
    })
}

async fn check_managed_process(pool: &PoolConfig, report: &mut Report) {
    let label = format!("pool `{}` managed native child", pool.key);
    let identity_file = pool.state_dir.join(crate::pool::PROCESS_IDENTITY_FILE);
    let identity = match process::read_identity_file(&identity_file).await {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            report.push(
                label,
                Outcome::Ok("no managed child recorded yet".to_owned()),
            );
            return;
        }
        Err(error) => {
            report.push(label, Outcome::Failed(format!("{error:#}")));
            return;
        }
    };
    match process::verify_ownership(&identity).await {
        Ok(Ownership::Owned) => report.push(
            label,
            Outcome::Ok(format!(
                "pid {} is running and owned by this worker",
                identity.pid
            )),
        ),
        Ok(Ownership::Foreign) => report.push(
            label,
            Outcome::Failed(format!(
                "pid {} was reused by an unrelated process",
                identity.pid
            )),
        ),
        Ok(Ownership::Gone) => report.push(
            label,
            Outcome::Ok(format!("pid {} is no longer running", identity.pid)),
        ),
        Err(error) => report.push(label, Outcome::Failed(format!("{error:#}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::{Outcome, Report};

    #[test]
    fn report_renders_failures_without_credentials() {
        let report = Report {
            checks: vec![super::CheckResult {
                label: "credential".to_owned(),
                outcome: Outcome::Failed("not enrolled".to_owned()),
            }],
        };
        assert!(!report.all_ok());
        assert_eq!(report.render(), "[fail] credential: not enrolled\n");
    }
}
