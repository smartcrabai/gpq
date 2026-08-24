//! Worker Credential persistence (ADR 0009).
//!
//! The Worker Credential is a revocable machine credential distinct from the
//! Tenant Master Key. It is stored in platform-appropriate secret storage:
//! Keychain on macOS, Credential Manager on Windows, systemd credentials
//! (read-only, `$CREDENTIALS_DIRECTORY/gpq-worker`) for Linux services managed
//! by systemd, and otherwise an owner-only mode-0600 file under the Worker's
//! state directory. The credential is never written to configuration,
//! command arguments, environment variables, logs, or diagnostics, and unsafe
//! file permissions or ownership stop startup rather than being silently
//! accepted.

use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

/// Reverse-DNS service identity used for platform secret stores, matching the
/// launchd label used elsewhere (ADR 0020).
#[cfg(any(target_os = "macos", target_os = "windows"))]
const SERVICE_NAME: &str = "ai.smartcrab.gpq-worker";

/// Environment variable systemd sets to the directory holding
/// `LoadCredential=` entries. Its presence means credential storage for this
/// process is systemd-managed and read-only.
const SYSTEMD_CREDENTIALS_DIR_VAR: &str = "CREDENTIALS_DIRECTORY";
/// Platform-appropriate persistence for one Worker's revocable credential.
pub struct CredentialStore {
    worker_name: String,
    state_dir: PathBuf,
}

impl CredentialStore {
    /// Creates a store scoped to `worker_name`, falling back to a file under
    /// `state_dir` on platforms without a native secret store.
    #[must_use]
    pub fn new(worker_name: &str, state_dir: &Path) -> Self {
        Self {
            worker_name: worker_name.to_owned(),
            state_dir: state_dir.to_owned(),
        }
    }

    /// Path of the owner-only fallback credential file under `state_dir`,
    /// used only on non-systemd, non-macOS Unix — and referenced by
    /// `service::install` as the source for systemd's `LoadCredential=`.
    #[must_use]
    pub fn fallback_path(&self) -> PathBuf {
        self.state_dir.join("worker-credential")
    }

    /// Persists `credential` to this platform's secret store.
    ///
    /// Refuses to run while systemd owns the credential directory for this
    /// process: that directory is populated by `LoadCredential=` and is
    /// read-only from here, so enrollment must run outside the service unit.
    ///
    /// # Errors
    ///
    /// Returns an error if systemd owns the credential directory, or if the
    /// platform secret store (or fallback file write) fails.
    pub fn store(&self, credential: &str) -> anyhow::Result<()> {
        if std::env::var_os(SYSTEMD_CREDENTIALS_DIR_VAR).is_some() {
            bail!(
                "credential directory for `{}` is managed by systemd (LoadCredential=); \
                 run `gpq-worker enroll` outside the service unit",
                self.worker_name
            );
        }
        self.store_platform(credential)
    }

    #[cfg(target_os = "macos")]
    fn store_platform(&self, credential: &str) -> anyhow::Result<()> {
        keyring::Entry::new(SERVICE_NAME, &self.worker_name)
            .context("opening macOS Keychain entry")?
            .set_password(credential)
            .context("writing macOS Keychain entry")
    }

    #[cfg(target_os = "windows")]
    fn store_platform(&self, credential: &str) -> anyhow::Result<()> {
        keyring::Entry::new(SERVICE_NAME, &self.worker_name)
            .context("opening Windows Credential Manager entry")?
            .set_password(credential)
            .context("writing Windows Credential Manager entry")
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    fn store_platform(&self, credential: &str) -> anyhow::Result<()> {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;

        std::fs::create_dir_all(&self.state_dir)
            .with_context(|| format!("creating state dir {}", self.state_dir.display()))?;
        let path = self.fallback_path();
        let tmp_path = path.with_extension("tmp");
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp_path)
                .with_context(|| format!("creating {}", tmp_path.display()))?;
            file.write_all(credential.as_bytes())
                .with_context(|| format!("writing {}", tmp_path.display()))?;
        }
        std::fs::rename(&tmp_path, &path).with_context(|| format!("installing {}", path.display()))
    }

    /// Loads the stored credential, returning `None` when enrollment has not
    /// run yet.
    ///
    /// Refuses (returns `Err`, stopping startup) a fallback file with unsafe
    /// permissions or an owner other than the current user (ADR 0009).
    ///
    /// # Errors
    ///
    /// Returns an error if a stored credential exists but cannot be read, or
    /// if a fallback file's permissions or ownership are unsafe.
    pub fn load(&self) -> anyhow::Result<Option<String>> {
        if let Some(dir) = std::env::var_os(SYSTEMD_CREDENTIALS_DIR_VAR) {
            let path = Path::new(&dir).join(crate::service::SERVICE_NAME);
            return match std::fs::read_to_string(&path) {
                Ok(contents) => Ok(Some(contents.trim_end_matches(['\n', '\r']).to_owned())),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err)
                    .with_context(|| format!("reading systemd credential {}", path.display())),
            };
        }
        self.load_platform()
    }

    #[cfg(any(target_os = "macos", target_os = "windows"))]
    fn load_platform(&self) -> anyhow::Result<Option<String>> {
        let entry = keyring::Entry::new(SERVICE_NAME, &self.worker_name)
            .context("opening platform secret store entry")?;
        match entry.get_password() {
            Ok(password) => Ok(Some(password)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(err) => Err(err).context("reading platform secret store entry"),
        }
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    fn load_platform(&self) -> anyhow::Result<Option<String>> {
        load_unix_file_credential(&self.fallback_path())
    }

    /// Deletes the stored credential, if any. Used by `service uninstall` and
    /// manual revocation; never errors when nothing was stored.
    ///
    /// # Errors
    ///
    /// Returns an error if the platform secret store (or fallback file
    /// removal) fails for a reason other than the entry already being
    /// absent.
    pub fn delete(&self) -> anyhow::Result<()> {
        self.delete_platform()
    }

    #[cfg(any(target_os = "macos", target_os = "windows"))]
    fn delete_platform(&self) -> anyhow::Result<()> {
        let entry = keyring::Entry::new(SERVICE_NAME, &self.worker_name)
            .context("opening platform secret store entry")?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(err) => Err(err).context("deleting platform secret store entry"),
        }
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    fn delete_platform(&self) -> anyhow::Result<()> {
        match std::fs::remove_file(self.fallback_path()) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).context("deleting fallback credential file"),
        }
    }

    /// Human-readable backend name for `diagnose` output. Never includes the
    /// credential itself.
    #[must_use]
    pub fn describe() -> &'static str {
        if std::env::var_os(SYSTEMD_CREDENTIALS_DIR_VAR).is_some() {
            return "systemd LoadCredential (read-only)";
        }
        Self::describe_platform()
    }

    #[cfg(target_os = "macos")]
    const fn describe_platform() -> &'static str {
        "macOS Keychain"
    }

    #[cfg(target_os = "windows")]
    const fn describe_platform() -> &'static str {
        "Windows Credential Manager"
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    const fn describe_platform() -> &'static str {
        "owner-only file (mode 0600)"
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn load_unix_file_credential(path: &Path) -> anyhow::Result<Option<String>> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("reading {}", path.display()));
        }
    };

    let mode = metadata.mode() & 0o777;
    if mode != 0o600 {
        bail!(
            "refusing to load credential {}: expected mode 0600, found {mode:03o}",
            path.display()
        );
    }

    let current_uid = current_uid();
    if metadata.uid() != current_uid {
        bail!(
            "refusing to load credential {}: owned by uid {}, current user is uid {current_uid}",
            path.display(),
            metadata.uid()
        );
    }

    let contents =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(contents.trim_end_matches(['\n', '\r']).to_owned()))
}

/// Returns the current process's real user id.
#[cfg(all(unix, not(target_os = "macos")))]
fn current_uid() -> u32 {
    // SAFETY: `getuid(2)` takes no arguments, has no preconditions, and
    // cannot fail.
    unsafe { libc::getuid() }
}

#[cfg(all(test, unix, not(target_os = "macos")))]
mod tests {
    use std::io::Write as _;

    use super::load_unix_file_credential;

    fn write_with_mode(path: &std::path::Path, contents: &str, mode: u32) {
        use std::os::unix::fs::OpenOptionsExt as _;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(mode)
            .open(path)
            .unwrap_or_else(|err| panic!("create {}: {err}", path.display()));
        file.write_all(contents.as_bytes())
            .unwrap_or_else(|err| panic!("write {}: {err}", path.display()));
    }

    #[test]
    #[cfg(unix)]
    fn accepts_owner_only_file() {
        let dir = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let path = dir.path().join("cred");
        write_with_mode(&path, "s3cret", 0o600);

        let Ok(Some(loaded)) = load_unix_file_credential(&path) else {
            panic!("expected a 0600 owner-owned file to load");
        };
        assert_eq!(loaded, "s3cret");
    }

    #[test]
    #[cfg(unix)]
    fn missing_file_is_none() {
        let dir = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let path = dir.path().join("cred");

        let Ok(None) = load_unix_file_credential(&path) else {
            panic!("expected a missing file to yield None");
        };
    }

    #[test]
    #[cfg(unix)]
    fn rejects_group_readable_file() {
        let dir = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let path = dir.path().join("cred");
        write_with_mode(&path, "s3cret", 0o640);

        let Err(err) = load_unix_file_credential(&path) else {
            panic!("expected a 0640 file to be refused");
        };
        assert!(err.to_string().contains("expected mode 0600"));
    }

    #[test]
    #[cfg(unix)]
    fn rejects_world_readable_file() {
        let dir = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let path = dir.path().join("cred");
        write_with_mode(&path, "s3cret", 0o644);

        let Err(err) = load_unix_file_credential(&path) else {
            panic!("expected a 0644 file to be refused");
        };
        assert!(err.to_string().contains("expected mode 0600"));
    }

    // Ownership mismatch cannot be exercised without a second uid available
    // to the test process, so only the permission-bit check is unit-tested
    // here; the uid comparison is exercised by inspection (see
    // `load_unix_file_credential`) and covered by `accepts_owner_only_file`
    // implicitly succeeding (the file is owned by the current test process).
}
