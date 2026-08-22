//! Runs `gpq-remote` administration subcommands (`migrate`, `tenant
//! create`, `tenant key rotate`, `worker revoke`) as real child processes,
//! proving the CLI path itself (ADR 0016, ADR 0009) rather than
//! reimplementing its logic.

use std::path::Path;
use std::process::Output;

use anyhow::{Context, bail};

/// One `tenant key rotate` result: the Tenant Master Key secret,
/// recoverable only at this moment (ADR 0009).
pub struct RotatedKey {
    pub secret: String,
}

async fn run(bin: &Path, args: &[&str], env: &[(&str, &str)]) -> anyhow::Result<Output> {
    let mut command = tokio::process::Command::new(bin);
    command.args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    command.kill_on_drop(true);
    let output = command
        .output()
        .await
        .with_context(|| format!("spawning {} {:?}", bin.display(), args))?;
    if !output.status.success() {
        bail!(
            "{} {:?} exited with {}: stdout={} stderr={}",
            bin.display(),
            args,
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(output)
}

/// Runs `gpq-remote migrate` (ADR 0016: the schema-owner credential).
pub async fn migrate(bin: &Path, database_url: &str) -> anyhow::Result<()> {
    run(bin, &["migrate"], &[("GPQ_DATABASE_URL", database_url)]).await?;
    Ok(())
}

/// Runs `gpq-remote tenant create --name <name>`, returning the printed
/// Tenant id.
pub async fn tenant_create(
    bin: &Path,
    admin_env: &[(&str, &str)],
    name: &str,
) -> anyhow::Result<uuid::Uuid> {
    let output = run(bin, &["tenant", "create", "--name", name], admin_env).await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let id_line = stdout
        .lines()
        .next_back()
        .context("`tenant create` printed no output")?;
    uuid::Uuid::parse_str(id_line.trim())
        .with_context(|| format!("parsing tenant id from {id_line:?}"))
}

/// Runs `gpq-remote tenant key rotate --tenant <id>`, returning the printed
/// key id and secret.
pub async fn tenant_key_rotate(
    bin: &Path,
    admin_env: &[(&str, &str)],
    tenant_id: uuid::Uuid,
) -> anyhow::Result<RotatedKey> {
    let tenant_arg = tenant_id.to_string();
    let output = run(
        bin,
        &[
            "tenant",
            "key",
            "rotate",
            "--tenant",
            &tenant_arg,
            "--label",
            "e2e",
        ],
        admin_env,
    )
    .await?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines();
    let key_id_line = lines
        .next()
        .context("`tenant key rotate` printed no key id line")?;
    let _key_id = key_id_line
        .strip_prefix("key id: ")
        .context("`tenant key rotate` first line missing `key id: ` prefix")?
        .trim();
    let secret = lines
        .next_back()
        .context("`tenant key rotate` printed no secret line")?
        .trim()
        .to_owned();
    Ok(RotatedKey { secret })
}

/// Runs `gpq-remote worker revoke --tenant <id> --worker <id>`, revoking a
/// Worker Credential (ADR 0009: Worker Credentials are revocable).
///
/// # Errors
/// Returns an error if the command fails.
pub async fn worker_revoke(
    bin: &Path,
    admin_env: &[(&str, &str)],
    tenant: uuid::Uuid,
    worker: uuid::Uuid,
) -> anyhow::Result<()> {
    let tenant = tenant.to_string();
    let worker = worker.to_string();
    run(
        bin,
        &["worker", "revoke", "--tenant", &tenant, "--worker", &worker],
        admin_env,
    )
    .await
    .map(|_| ())
}
