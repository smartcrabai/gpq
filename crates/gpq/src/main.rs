//! `gpq`: the single GPU Generation Queue executable.
//!
//! Each role is a subcommand that delegates to its library entry point:
//! `gpq remote` runs the coordinator, `gpq worker` runs the tenant-owned host
//! agent, and `gpq infer` runs the per-Device-Pool inference process that a
//! Worker spawns from this same executable (ADR 0020).

use std::ffi::OsString;

use clap::{Args, Parser, Subcommand};

/// Top-level command-line interface for the `gpq` executable.
#[derive(Debug, Parser)]
#[command(name = "gpq", version, about = "GPU Generation Queue")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Coordinator: schema migration, serving, and Tenant/Worker administration.
    #[command(disable_help_flag = true, disable_version_flag = true)]
    Remote(Forwarded),
    /// Tenant-owned Worker: enrollment, the foreground `run` loop, diagnostics,
    /// and OS service management.
    #[command(disable_help_flag = true, disable_version_flag = true)]
    Worker(Forwarded),
    /// Per-Device-Pool inference process spawned by `gpq worker run`; not for
    /// direct use.
    #[command(hide = true, disable_help_flag = true, disable_version_flag = true)]
    Infer(Forwarded),
}

/// Tokens after a role name, passed unchanged to that role's own parser so its
/// help, version, and validation output is the role's own.
#[derive(Debug, Args)]
struct Forwarded {
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        num_args = 0..,
        value_name = "ARGS"
    )]
    args: Vec<OsString>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Remote(forwarded) => gpq_remote::run_cli(role_argv("gpq remote", forwarded)).await,
        Command::Worker(forwarded) => gpq_worker::run_cli(role_argv("gpq worker", forwarded)).await,
        Command::Infer(forwarded) => gpq_worker::run_infer(role_argv("gpq infer", forwarded)).await,
    }
}

/// Builds the argv a role parser expects: the role's display name followed by
/// the subcommand tokens.
fn role_argv(display_name: &str, forwarded: Forwarded) -> Vec<OsString> {
    std::iter::once(OsString::from(display_name))
        .chain(forwarded.args)
        .collect()
}
