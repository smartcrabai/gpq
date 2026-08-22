//! OS service management wrapping `gpq-worker run` (ADR 0020).
//!
//! `gpq-worker run` is the single foreground implementation; install,
//! uninstall, start, and stop only manage systemd on Linux, launchd (and, by
//! extension, Homebrew services) on macOS, and a native Windows Service on
//! Windows. Unit/plist rendering is pure and unit-tested; the side-effecting
//! install path additionally writes the unit file and invokes the platform
//! service manager.

use std::path::Path;

use anyhow::{Context, bail};

/// systemd unit name / Windows service name.
pub const SERVICE_NAME: &str = "gpq-worker";
/// launchd label, matching the Keychain/Credential Manager service identity
/// used by [`crate::credential`] (ADR 0009).
pub const LAUNCHD_LABEL: &str = "ai.smartcrab.gpq-worker";

/// Renders the systemd unit file for `gpq-worker run` (ADR 0020).
///
/// `binary` and `config` are quoted as systemd command-line words
/// (`systemd.syntax(7)`): `ExecStart=` splits its value on whitespace the
/// same way a shell would, so an unquoted path containing a space would
/// either fail to start the service or silently execute a truncated
/// command.
///
/// `credential_file` is loaded by systemd itself via `LoadCredential=` and
/// exposed to the running process read-only at
/// `$CREDENTIALS_DIRECTORY/gpq-worker` (ADR 0009); the credential never
/// appears on this command line. Unlike `ExecStart=`, `LoadCredential=`
/// only ever splits its value on the first `:` (`load-fragment.c`'s
/// `config_parse_load_credential`), so the path half is taken verbatim,
/// including any whitespace, and must NOT be quoted here — systemd never
/// unquotes this directive, so quoting would bake literal quote characters
/// into the credential path instead of protecting it.
#[must_use]
pub fn render_systemd_unit(binary: &Path, config: &Path, credential_file: &Path) -> String {
    format!(
        "[Unit]\n\
         Description=GPU Generation Queue Worker\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={binary} run --config {config}\n\
         LoadCredential={SERVICE_NAME}:{credential_file}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        binary = quote_systemd_word(&binary.display().to_string()),
        config = quote_systemd_word(&config.display().to_string()),
        credential_file = credential_file.display(),
    )
}

/// Quotes `value` as one systemd command-line word (`systemd.syntax(7)`):
/// wrapped in double quotes with embedded backslashes and double quotes
/// escaped, so a value containing whitespace survives the shell-like word
/// splitting systemd applies to directives such as `ExecStart=`.
fn quote_systemd_word(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for ch in value.chars() {
        if matches!(ch, '\\' | '"') {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    quoted
}

/// Renders the launchd agent plist for `gpq-worker run` (ADR 0020).
///
/// Homebrew's `brew services start gpq-worker` loads this same plist under
/// `~/Library/LaunchAgents`; `service install`/`uninstall` and Homebrew
/// services manage one underlying launchd unit, never two.
#[must_use]
pub fn render_launchd_plist(binary: &Path, config: &Path) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \t<key>Label</key>\n\
         \t<string>{LAUNCHD_LABEL}</string>\n\
         \t<key>ProgramArguments</key>\n\
         \t<array>\n\
         \t\t<string>{binary}</string>\n\
         \t\t<string>run</string>\n\
         \t\t<string>--config</string>\n\
         \t\t<string>{config}</string>\n\
         \t</array>\n\
         \t<key>RunAtLoad</key>\n\
         \t<true/>\n\
         \t<key>KeepAlive</key>\n\
         \t<true/>\n\
         </dict>\n\
         </plist>\n",
        binary = binary.display(),
        config = config.display(),
    )
}

/// Argument vector for `sc.exe create` (ADR 0020). Each element is passed as
/// a separate argv entry (no shell involved), matching `sc.exe`'s unusual
/// `key= value` token convention.
///
/// The `binPath=` value itself is a single Windows command line that
/// `sc.exe` stores verbatim as the service's `ImagePath`, which the Service
/// Control Manager later hands to `CreateProcess` unmodified. The executable
/// and config paths are quoted the way `CreateProcess`/`CommandLineToArgvW`
/// require: an unquoted executable path containing a space — the Worker's
/// natural install location, `C:\Program Files\...`, always has one — is
/// otherwise the classic Unquoted Service Path weakness, letting an
/// attacker-planted `C:\Program.exe` run instead.
#[must_use]
pub fn windows_create_args(binary: &Path, config: &Path) -> Vec<String> {
    let bin_path = format!(
        "{} run --config {}",
        quote_windows_arg(&binary.display().to_string()),
        quote_windows_arg(&config.display().to_string()),
    );
    vec![
        "create".to_owned(),
        SERVICE_NAME.to_owned(),
        "binPath=".to_owned(),
        bin_path,
        "start=".to_owned(),
        "auto".to_owned(),
        "DisplayName=".to_owned(),
        "GPU Generation Queue Worker".to_owned(),
    ]
}

/// Quotes `value` as one Windows command-line argument, following the same
/// backslash/quote escaping `CommandLineToArgvW` expects, so a value
/// containing whitespace is parsed back as a single argument by both
/// `sc.exe` (locating `ImagePath`'s executable) and `gpq-worker.exe` itself
/// (parsing its own `--config` argument).
fn quote_windows_arg(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let mut backslashes: usize = 1;
            while chars.peek() == Some(&'\\') {
                backslashes += 1;
                chars.next();
            }
            let run = if matches!(chars.peek(), Some('"') | None) {
                backslashes * 2
            } else {
                backslashes
            };
            for _ in 0..run {
                quoted.push('\\');
            }
        } else if ch == '"' {
            quoted.push('\\');
            quoted.push('"');
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('"');
    quoted
}

/// Path of the installed systemd unit.
#[must_use]
pub fn systemd_unit_path() -> std::path::PathBuf {
    std::path::PathBuf::from("/etc/systemd/system/gpq-worker.service")
}

/// Path of the installed launchd agent plist, under the current user's home.
///
/// # Errors
///
/// Returns an error if `HOME` is not set.
pub fn launchd_plist_path() -> anyhow::Result<std::path::PathBuf> {
    let home = std::env::var_os("HOME").context("HOME is not set")?;
    Ok(std::path::PathBuf::from(home)
        .join("Library/LaunchAgents")
        .join(format!("{LAUNCHD_LABEL}.plist")))
}

/// Installs `gpq-worker run --config <config>` as an OS-managed service.
///
/// `credential_file` is only consulted on Linux, where it becomes the
/// systemd `LoadCredential=` source; other platforms ignore it because they
/// use a native secret store instead (ADR 0009).
///
/// Dispatch is by [`std::env::consts::OS`] rather than `#[cfg]` so every
/// platform's rendering and command-invocation logic stays compiled (and
/// unit-testable) on every host, matching how `gpq-worker` actually ships:
/// one cross-compiled binary per target, decided at build time by the
/// target triple, not by which OS happens to be building it.
///
/// # Errors
///
/// Returns an error if the current OS has no supported service manager, or
/// if writing the unit/plist or invoking the service manager fails.
pub async fn install(binary: &Path, config: &Path, credential_file: &Path) -> anyhow::Result<()> {
    match std::env::consts::OS {
        "linux" => install_linux(binary, config, credential_file).await,
        "macos" => install_macos(binary, config).await,
        "windows" => install_windows(binary, config).await,
        other => bail!("no supported service manager for platform `{other}`"),
    }
}

/// Removes the installed service, stopping it first if running.
///
/// # Errors
///
/// Returns an error if the current OS has no supported service manager, or
/// if removing the unit/plist or invoking the service manager fails.
pub async fn uninstall() -> anyhow::Result<()> {
    match std::env::consts::OS {
        "linux" => uninstall_linux().await,
        "macos" => uninstall_macos().await,
        "windows" => uninstall_windows().await,
        other => bail!("no supported service manager for platform `{other}`"),
    }
}

/// Starts the installed service.
///
/// # Errors
///
/// Returns an error if the current OS has no supported service manager, or
/// if invoking the service manager fails.
pub async fn start() -> anyhow::Result<()> {
    run_service_command("start").await
}

/// Stops the installed service.
///
/// # Errors
///
/// Returns an error if the current OS has no supported service manager, or
/// if invoking the service manager fails.
pub async fn stop() -> anyhow::Result<()> {
    run_service_command("stop").await
}

async fn run_service_command(verb: &str) -> anyhow::Result<()> {
    match std::env::consts::OS {
        "linux" => run_command("systemctl", &[verb, SERVICE_NAME]).await,
        "macos" => run_command("launchctl", &[verb, LAUNCHD_LABEL]).await,
        "windows" => run_command("sc.exe", &[verb, SERVICE_NAME]).await,
        other => bail!("no supported service manager for platform `{other}`"),
    }
}

async fn install_linux(binary: &Path, config: &Path, credential_file: &Path) -> anyhow::Result<()> {
    let unit = render_systemd_unit(binary, config, credential_file);
    let path = systemd_unit_path();
    tokio::fs::write(&path, unit)
        .await
        .with_context(|| format!("writing {}", path.display()))?;
    run_command("systemctl", &["daemon-reload"]).await?;
    run_command("systemctl", &["enable", SERVICE_NAME]).await
}

async fn uninstall_linux() -> anyhow::Result<()> {
    run_command("systemctl", &["disable", "--now", SERVICE_NAME]).await?;
    let path = systemd_unit_path();
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("removing {}", path.display())),
    }?;
    run_command("systemctl", &["daemon-reload"]).await
}

async fn install_macos(binary: &Path, config: &Path) -> anyhow::Result<()> {
    let plist = render_launchd_plist(binary, config);
    let path = launchd_plist_path()?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    tokio::fs::write(&path, plist)
        .await
        .with_context(|| format!("writing {}", path.display()))?;
    run_command("launchctl", &["load", "-w", &path.to_string_lossy()]).await
}

async fn uninstall_macos() -> anyhow::Result<()> {
    let path = launchd_plist_path()?;
    run_command("launchctl", &["unload", "-w", &path.to_string_lossy()]).await?;
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("removing {}", path.display())),
    }
}

async fn install_windows(binary: &Path, config: &Path) -> anyhow::Result<()> {
    let args = windows_create_args(binary, config);
    run_command_owned("sc.exe", args).await
}

async fn uninstall_windows() -> anyhow::Result<()> {
    run_command("sc.exe", &["delete", SERVICE_NAME]).await
}

async fn run_command(program: &str, args: &[&str]) -> anyhow::Result<()> {
    run_command_owned(program, args.iter().map(|a| (*a).to_owned()).collect()).await
}

async fn run_command_owned(program: &str, args: Vec<String>) -> anyhow::Result<()> {
    let status = tokio::process::Command::new(program)
        .args(&args)
        .status()
        .await
        .with_context(|| format!("spawning {program}"))?;
    if !status.success() {
        bail!("{program} {args:?} exited with {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{render_launchd_plist, render_systemd_unit, windows_create_args};

    #[test]
    fn systemd_unit_wires_exec_and_credential() {
        let unit = render_systemd_unit(
            Path::new("/usr/local/bin/gpq-worker"),
            Path::new("/etc/gpq-worker/worker.toml"),
            Path::new("/etc/gpq-worker/credential"),
        );
        assert!(unit.contains(
            "ExecStart=\"/usr/local/bin/gpq-worker\" run --config \"/etc/gpq-worker/worker.toml\""
        ));
        assert!(unit.contains("LoadCredential=gpq-worker:/etc/gpq-worker/credential"));
        assert!(unit.contains("[Install]"));
    }

    #[test]
    fn systemd_unit_quotes_paths_containing_spaces() {
        let unit = render_systemd_unit(
            Path::new("/usr/local/bin/gpq-worker"),
            Path::new("/home/jane doe/worker.toml"),
            Path::new("/home/jane doe/credential"),
        );
        assert!(unit.contains(
            "ExecStart=\"/usr/local/bin/gpq-worker\" run --config \"/home/jane doe/worker.toml\""
        ));
        // `LoadCredential=` splits its value only on the first `:` and never
        // unquotes it, so the path half must stay literal even though it
        // contains a space; quoting it would corrupt the stored path.
        assert!(unit.contains("LoadCredential=gpq-worker:/home/jane doe/credential"));
    }

    #[test]
    fn systemd_unit_escapes_embedded_quote_and_backslash() {
        let config_with_special_chars = "/etc/gpq-worker/quo\"te\\path.toml";
        let unit = render_systemd_unit(
            Path::new("/usr/local/bin/gpq-worker"),
            Path::new(config_with_special_chars),
            Path::new("/etc/gpq-worker/credential"),
        );
        assert!(unit.contains(
            "ExecStart=\"/usr/local/bin/gpq-worker\" run --config \"/etc/gpq-worker/quo\\\"te\\\\path.toml\""
        ));
    }

    #[test]
    fn launchd_plist_wires_program_arguments() {
        let plist = render_launchd_plist(
            Path::new("/opt/homebrew/bin/gpq-worker"),
            Path::new("/etc/gpq-worker/worker.toml"),
        );
        assert!(plist.contains("<string>ai.smartcrab.gpq-worker</string>"));
        assert!(plist.contains("<string>/opt/homebrew/bin/gpq-worker</string>"));
        assert!(plist.contains("<string>--config</string>"));
        assert!(plist.contains("<string>/etc/gpq-worker/worker.toml</string>"));
    }

    #[test]
    fn windows_args_use_key_value_tokens() {
        let args = windows_create_args(
            Path::new(r"C:\gpq\gpq-worker.exe"),
            Path::new(r"C:\ProgramData\gpq\worker.toml"),
        );
        assert_eq!(args[0], "create");
        assert_eq!(args[1], "gpq-worker");
        assert_eq!(args[2], "binPath=");
        assert_eq!(
            args[3],
            r#""C:\gpq\gpq-worker.exe" run --config "C:\ProgramData\gpq\worker.toml""#
        );
        assert_eq!(args[4], "start=");
        assert_eq!(args[5], "auto");
    }

    #[test]
    fn windows_binpath_quotes_program_files_style_path() {
        let args = windows_create_args(
            Path::new(r"C:\Program Files\gpq\gpq-worker.exe"),
            Path::new(r"C:\ProgramData\gpq\worker.toml"),
        );
        assert_eq!(
            args[3],
            r#""C:\Program Files\gpq\gpq-worker.exe" run --config "C:\ProgramData\gpq\worker.toml""#
        );
    }
}
