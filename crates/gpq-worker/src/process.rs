//! Managed subprocess supervision and durable process identity (ADR 0005).
//!
//! Every backend process a Worker starts runs inside a dedicated Unix process
//! group (via the stable `process_group(0)` API, so the child becomes the
//! leader of its own group) or a Windows Job Object, so the whole process
//! tree can be torn down as one unit even if the backend spawns helpers.
//!
//! A durable identity record (PID, process start marker, and executable path
//! plus size/mtime) is written next to the Pool's state so a Worker restart
//! can tell a process it previously owned from an unrelated process that has
//! since reused the same PID. ADR 0005 requires that a Worker never kill a
//! process by PID alone and never adopt an old process; [`verify_ownership`]
//! is the single decision point that enforces both rules.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};

static IDENTITY_TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Durable identity of a managed child process, persisted beside its Pool's
/// state directory so it survives a Worker restart (ADR 0005).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    /// Operating system process id at spawn time.
    pub pid: u32,
    /// Opaque, platform-specific process start marker compared only for exact
    /// equality: on Unix the `ps -o lstart=` text, on Windows the process
    /// creation `FILETIME` encoded as a decimal `u64`.
    pub start_marker: String,
    /// Absolute path of the executable that was launched.
    pub executable: PathBuf,
    /// Executable file size at spawn time, part of the identity check.
    pub executable_size: u64,
    /// Executable file modification time (Unix seconds) at spawn time.
    pub executable_mtime_unix: i64,
}

/// The outcome of comparing a recorded [`ProcessIdentity`] against the live
/// process table (ADR 0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ownership {
    /// A live process occupies the PID and every identity field matches.
    Owned,
    /// A live process occupies the PID but at least one identity field
    /// differs: the PID has been reused by an unrelated process.
    Foreign,
    /// No process occupies the PID anymore.
    Gone,
}

/// One backend process this Worker spawned and supervises.
pub struct ManagedProcess {
    child: Child,
    identity: ProcessIdentity,
    identity_file: PathBuf,
    #[cfg(windows)]
    job: Option<windows_job::JobHandle>,
    #[cfg(windows)]
    job_assignment_error: Option<String>,
}

impl ManagedProcess {
    /// Spawns `executable` into its own process group/Job Object and returns
    /// the owned handle before any further asynchronous identity setup.
    pub async fn spawn(
        executable: &Path,
        args: &[OsString],
        env: &std::collections::BTreeMap<String, String>,
        state_dir: &Path,
        identity_file_name: &str,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(state_dir).await?;
        let mut command = Command::new(executable);
        command
            .env_clear()
            .args(args)
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        #[cfg(unix)]
        {
            // A new, empty process group led by the child itself, so the
            // whole tree it spawns can be signalled by group id (ADR 0005).
            command.process_group(0);
        }
        let child = command
            .spawn()
            .map_err(|source| anyhow::anyhow!("spawning {}: {source}", executable.display()))?;
        let Some(pid) = child.id() else {
            anyhow::bail!(
                "spawned process for {} exited immediately",
                executable.display()
            );
        };
        // Assign to the Job Object immediately after spawn, before the child
        // has a chance to spawn grandchildren outside supervision.
        #[cfg(windows)]
        let (job, job_assignment_error) = match windows_job::create_and_assign(&child) {
            Ok(job) => (Some(job), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let identity = ProcessIdentity {
            pid,
            start_marker: String::new(),
            executable: executable.to_path_buf(),
            executable_size: 0,
            executable_mtime_unix: 0,
        };
        Ok(Self {
            child,
            identity,
            identity_file: state_dir.join(identity_file_name),
            #[cfg(windows)]
            job,
            #[cfg(windows)]
            job_assignment_error,
        })
    }

    /// Installs the durable identity fields after they have been persisted.
    pub(crate) fn set_identity(&mut self, identity: ProcessIdentity) {
        debug_assert_eq!(self.identity.pid, identity.pid);
        self.identity = identity;
    }
    #[cfg(windows)]
    pub(crate) fn startup_error(&self) -> Option<&str> {
        self.job_assignment_error.as_deref()
    }

    /// The operating system process id.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.identity.pid
    }

    /// Takes the child's stdout, if it has not already been taken.
    pub fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    /// Takes the child's stderr, if it has not already been taken.
    pub fn take_stderr(&mut self) -> Option<tokio::process::ChildStderr> {
        self.child.stderr.take()
    }

    /// Returns the exit status without blocking if the process has already
    /// exited.
    pub fn try_wait(&mut self) -> anyhow::Result<Option<std::process::ExitStatus>> {
        Ok(self.child.try_wait()?)
    }

    /// Waits for the process to exit.
    pub async fn wait_exit(&mut self) -> anyhow::Result<std::process::ExitStatus> {
        Ok(self.child.wait().await?)
    }

    /// Terminates the whole process tree and returns only after the owned
    /// group or Job Object has no remaining processes.
    pub async fn terminate_tree(&mut self, graceful: Duration) -> anyhow::Result<()> {
        #[cfg(unix)]
        unix_ps::terminate_group(self.identity.pid, graceful, Some(&mut self.child)).await?;
        #[cfg(windows)]
        match &self.job {
            Some(job) => job.terminate_and_wait(Duration::from_secs(5)).await?,
            None => {
                if self.child.try_wait()?.is_none() {
                    self.child
                        .start_kill()
                        .context("killing managed child without an assigned Job Object")?;
                }
                anyhow::bail!(
                    "cannot verify process tree cleanup without an assigned Job Object: {}",
                    self.job_assignment_error
                        .as_deref()
                        .unwrap_or("unknown Job Object assignment failure")
                );
            }
        }
        tokio::time::timeout(Duration::from_secs(5), self.wait_exit())
            .await
            .context("waiting for managed process leader to exit")??;
        let _ = tokio::fs::remove_file(&self.identity_file).await;
        Ok(())
    }
}

pub(crate) async fn record_identity_file(
    pid: u32,
    executable: &Path,
    identity_file: &Path,
) -> anyhow::Result<ProcessIdentity> {
    let start_marker = read_start_marker(pid).await?.ok_or_else(|| {
        anyhow::anyhow!("process {pid} exited before its identity could be recorded")
    })?;
    let metadata = tokio::fs::metadata(executable).await?;
    let identity = ProcessIdentity {
        pid,
        start_marker,
        executable: executable.to_path_buf(),
        executable_size: metadata.len(),
        executable_mtime_unix: mtime_unix(&metadata),
    };
    write_identity_file(identity_file, &identity).await?;
    Ok(identity)
}

/// Owns a freshly [`ManagedProcess::spawn`]ed process until [`Self::disarm`]
/// hands it to long-lived Pool state, terminating the whole process tree on
/// drop otherwise.
///
/// `ManagedProcess::spawn` deliberately sets `kill_on_drop(false)` (ADR
/// 0005: an orderly `terminate_tree` — SIGTERM then SIGKILL on Unix, the Job
/// Object on Windows — is what ends a managed process, not the OS reaping an
/// orphan when a bare `Child` handle merely goes out of scope). The guard
/// covers the synchronous handoff from spawn into `PoolState::process`;
/// durable identity setup then continues while Pool state owns the handle.
pub(crate) struct ProcessGuard {
    process: Option<ManagedProcess>,
    grace: Duration,
}

impl ProcessGuard {
    /// Guards `process`, terminating it with `grace` between SIGTERM and
    /// SIGKILL (Unix) or via the Job Object (Windows) if the guard is
    /// dropped before [`Self::disarm`].
    #[must_use]
    pub(crate) fn new(process: ManagedProcess, grace: Duration) -> Self {
        Self {
            process: Some(process),
            grace,
        }
    }

    /// Hands ownership of the guarded process to the caller, disarming the
    /// drop-time termination.
    #[must_use]
    pub(crate) fn disarm(mut self) -> ManagedProcess {
        self.process.take().unwrap_or_else(|| {
            unreachable!("ProcessGuard::process is set until disarm consumes it")
        })
    }
}

impl std::ops::Deref for ProcessGuard {
    type Target = ManagedProcess;

    fn deref(&self) -> &ManagedProcess {
        self.process.as_ref().unwrap_or_else(|| {
            unreachable!("ProcessGuard::process is set until disarm consumes it")
        })
    }
}

impl std::ops::DerefMut for ProcessGuard {
    fn deref_mut(&mut self) -> &mut ManagedProcess {
        self.process.as_mut().unwrap_or_else(|| {
            unreachable!("ProcessGuard::process is set until disarm consumes it")
        })
    }
}

impl Drop for ProcessGuard {
    /// Terminates the still-owned process tree so a failed startup can never
    /// abandon a live child to the OS. `terminate_tree` is async but `Drop`
    /// is not: when this thread has an ambient Tokio runtime (true for
    /// every `ProcessGuard` `start_pool_process` creates, since it always
    /// runs inside an async task) the termination is spawned onto it. The
    /// no-runtime branch is a defensive fallback that is not expected to
    /// run in this codebase; rather than silently abandoning the process it
    /// deterministically builds a throwaway current-thread runtime and
    /// blocks on the same `terminate_tree` used everywhere else, so a leak
    /// never happens quietly.
    fn drop(&mut self) {
        let Some(mut process) = self.process.take() else {
            return;
        };
        let grace = self.grace;
        let pid = process.pid();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(err) = process.terminate_tree(grace).await {
                    tracing::warn!(pid, error = %err, "failed to terminate an abandoned managed process");
                }
            });
            return;
        }
        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                if let Err(err) = runtime.block_on(process.terminate_tree(grace)) {
                    tracing::warn!(
                        pid,
                        error = %err,
                        "failed to terminate an abandoned managed process outside a Tokio runtime"
                    );
                }
            }
            Err(err) => {
                tracing::error!(
                    pid,
                    error = %err,
                    "no Tokio runtime available to terminate an abandoned managed process; it will leak"
                );
            }
        }
    }
}

/// Reads a durable identity file if it exists.
pub async fn read_identity_file(path: &Path) -> anyhow::Result<Option<ProcessIdentity>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Writes a durable identity file atomically (unique temp file + synchronous rename commit).
pub async fn write_identity_file(path: &Path, identity: &ProcessIdentity) -> anyhow::Result<()> {
    let Some(parent) = path.parent() else {
        anyhow::bail!(
            "identity file path {} has no parent directory",
            path.display()
        );
    };
    tokio::fs::create_dir_all(parent).await?;
    let sequence = IDENTITY_TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_path = parent.join(format!(
        ".{}.tmp-{}-{sequence}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("identity"),
        std::process::id()
    ));
    let body = serde_json::to_vec_pretty(identity)?;
    tokio::fs::write(&temp_path, body).await?;
    std::fs::rename(&temp_path, path)?;
    Ok(())
}

/// Checks whether `identity`'s PID still names the exact process it recorded.
///
/// Never returns [`Ownership::Owned`] on a PID match alone: the start marker
/// and executable identity (path, size, mtime) must also match, so a Worker
/// never kills or adopts a process that merely reused an old PID (ADR 0005).
pub async fn verify_ownership(identity: &ProcessIdentity) -> anyhow::Result<Ownership> {
    let Some(live_marker) = read_start_marker(identity.pid).await? else {
        return Ok(Ownership::Gone);
    };
    if live_marker != identity.start_marker {
        return Ok(Ownership::Foreign);
    }
    // Corroborate the persisted identity with the full live command line:
    // short OS executable names can be truncated, while the complete command
    // line must still contain the executable this Worker launched.
    let live_command_line = read_command_line(identity.pid).await?;
    if !command_line_corroborates(identity, live_command_line.as_deref()) {
        return Ok(Ownership::Foreign);
    }
    match tokio::fs::metadata(&identity.executable).await {
        Ok(metadata) => {
            if metadata.len() != identity.executable_size
                || mtime_unix(&metadata) != identity.executable_mtime_unix
            {
                return Ok(Ownership::Foreign);
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Ownership::Foreign),
        Err(err) => return Err(err.into()),
    }
    Ok(Ownership::Owned)
}

/// Whether a live command line still corroborates `identity`.
///
/// An unavailable command line is inconclusive, not disqualifying: the start
/// marker and executable identity already carry the proof. A command line that
/// mentions the executable the Worker spawned corroborates it even when the OS
/// reports an interpreter as the image (ADR 0005).
fn command_line_corroborates(identity: &ProcessIdentity, command_line: Option<&str>) -> bool {
    let (Some(command_line), Some(executable)) = (command_line, identity.executable.to_str())
    else {
        return true;
    };
    command_line.contains(executable)
}

/// Loads the identity file at `identity_file` and, if it proves ownership of
/// a live process, terminates it before Worker startup makes its Pool ready
/// (ADR 0005: kill verified stale children, never adopt, never kill blind).
pub async fn kill_stale(identity_file: &Path, graceful: Duration) -> anyhow::Result<Ownership> {
    let Some(identity) = read_identity_file(identity_file).await? else {
        return Ok(Ownership::Gone);
    };
    let ownership = verify_ownership(&identity).await?;
    if ownership == Ownership::Owned {
        terminate_pid_tree(identity.pid, graceful).await?;
    }
    if ownership != Ownership::Foreign {
        let _ = tokio::fs::remove_file(identity_file).await;
    }
    Ok(ownership)
}

fn mtime_unix(metadata: &std::fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |duration| {
            duration.as_secs().min(i64::MAX as u64).cast_signed()
        })
}

#[cfg(unix)]
async fn read_start_marker(pid: u32) -> anyhow::Result<Option<String>> {
    unix_ps::start_marker(pid).await
}

#[cfg(windows)]
async fn read_start_marker(pid: u32) -> anyhow::Result<Option<String>> {
    windows_job::start_marker(pid)
}

#[cfg(unix)]
async fn read_command_line(pid: u32) -> anyhow::Result<Option<String>> {
    unix_ps::command_line(pid).await
}

#[cfg(windows)]
async fn read_command_line(pid: u32) -> anyhow::Result<Option<String>> {
    windows_job::command_line(pid)
}

#[cfg(unix)]
async fn terminate_pid_tree(pid: u32, graceful: Duration) -> anyhow::Result<()> {
    unix_ps::terminate_group(pid, graceful, None).await
}

#[cfg(windows)]
async fn terminate_pid_tree(pid: u32, _graceful: Duration) -> anyhow::Result<()> {
    windows_job::terminate_pid(pid)
}

/// Unix process-table queries and group signalling. Process-table identity
/// checks use `ps`; group signalling uses libc so negative process-group IDs
/// are never parsed as external utility options.
#[cfg(unix)]
mod unix_ps {
    use std::time::Duration;

    use anyhow::Context;
    use tokio::process::{Child, Command};

    /// The `ps -o lstart=` text for `pid`, or `None` if no such process exists.
    pub async fn start_marker(pid: u32) -> anyhow::Result<Option<String>> {
        let output = Command::new("ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .output()
            .await?;
        if !output.status.success() {
            return Ok(None);
        }
        let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if text.is_empty() {
            Ok(None)
        } else {
            Ok(Some(text))
        }
    }

    /// The full `ps -o args=` command line for `pid`, if the process exists.
    pub async fn command_line(pid: u32) -> anyhow::Result<Option<String>> {
        let output = Command::new("ps")
            .args(["-o", "args=", "-p", &pid.to_string()])
            .output()
            .await?;
        if !output.status.success() {
            return Ok(None);
        }
        let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if text.is_empty() {
            Ok(None)
        } else {
            Ok(Some(text))
        }
    }

    /// Sends SIGTERM to the whole process group identified by
    /// `process_group_id`, then SIGKILL if any member is still alive after
    /// `graceful`.
    pub async fn terminate_group(
        process_group_id: u32,
        graceful: Duration,
        mut child: Option<&mut Child>,
    ) -> anyhow::Result<()> {
        if !signal_group(process_group_id, libc::SIGTERM)? {
            return Ok(());
        }
        tokio::time::sleep(graceful).await;
        if let Some(child) = child.as_deref_mut() {
            // Darwin reports EPERM for a process group containing only
            // zombies; reap our owned leader before probing the group.
            let _ = child
                .try_wait()
                .context("reaping managed process leader after SIGTERM")?;
        }
        if group_alive(process_group_id)? {
            // The process group may exit between the liveness probe and this
            // signal; ESRCH is therefore a successful teardown race.
            let _ = signal_group(process_group_id, libc::SIGKILL)?;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(child) = child.as_deref_mut() {
                    let _ = child
                        .try_wait()
                        .context("reaping managed process leader after SIGKILL")?;
                }
                if !group_alive(process_group_id)? {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "managed process group {process_group_id} remained alive after SIGKILL"
                    );
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        Ok(())
    }

    fn signal_group(process_group_id: u32, signal: libc::c_int) -> anyhow::Result<bool> {
        let group_id =
            libc::pid_t::try_from(process_group_id).context("process group ID exceeds pid_t")?;
        anyhow::ensure!(group_id > 0, "invalid process group ID {group_id}");
        // SAFETY: a positive PGID derived from the spawned child is passed as
        // a negative pid, which targets only that dedicated process group.
        if unsafe { libc::kill(-group_id, signal) } == 0 {
            return Ok(true);
        }

        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        Err(error)
            .with_context(|| format!("sending signal {signal} to managed process group {group_id}"))
    }

    fn group_alive(process_group_id: u32) -> anyhow::Result<bool> {
        signal_group(process_group_id, 0)
    }
}

/// Windows Job Object management and process-table queries.
#[cfg(windows)]
mod windows_job {
    use std::time::Duration;

    use anyhow::Context;
    use tokio::process::Child;
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
        QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_TERMINATE,
        QueryFullProcessImageNameW, TerminateProcess,
    };

    /// An owned Job Object handle configured to kill every member process
    /// when the handle closes (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`), which
    /// also acts as the crash safety net: if this Worker process dies without
    /// calling [`JobHandle::terminate`], Windows closes the handle for it and
    /// the whole tree goes down with it.
    pub struct JobHandle(HANDLE);

    // SAFETY: a Windows HANDLE is an opaque kernel object id, not
    // thread-affine state; std's own process handles are `Send`/`Sync` too.
    unsafe impl Send for JobHandle {}
    unsafe impl Sync for JobHandle {}

    impl JobHandle {
        /// Immediately terminates every process assigned to this job.
        pub fn terminate(&self) -> anyhow::Result<()> {
            // SAFETY: `self.0` is a valid job handle owned by this value for
            // its whole lifetime.
            let ok = unsafe { TerminateJobObject(self.0, 1) };
            if ok == 0 {
                anyhow::bail!(
                    "TerminateJobObject failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            Ok(())
        }
        /// Terminates the job and waits until every assigned process has exited.
        pub async fn terminate_and_wait(&self, timeout: Duration) -> anyhow::Result<()> {
            if self.active_processes()? == 0 {
                return Ok(());
            }
            self.terminate()?;
            tokio::time::timeout(timeout, async {
                loop {
                    if self.active_processes()? == 0 {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .context("waiting for managed Windows Job Object processes to exit")??;
            Ok(())
        }

        fn active_processes(&self) -> anyhow::Result<u32> {
            // SAFETY: zero is a valid initial bit pattern for this output structure.
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                unsafe { std::mem::zeroed() };
            let length = u32::try_from(std::mem::size_of_val(&accounting))
                .context("Windows Job Object accounting structure is too large")?;
            let mut returned = 0;
            // SAFETY: `self.0` is a valid owned job handle and the output buffer
            // and size describe the initialized local structure.
            let ok = unsafe {
                QueryInformationJobObject(
                    self.0,
                    JobObjectBasicAccountingInformation,
                    std::ptr::addr_of_mut!(accounting).cast(),
                    length,
                    &mut returned,
                )
            };
            if ok == 0 {
                anyhow::bail!(
                    "QueryInformationJobObject failed: {}",
                    std::io::Error::last_os_error()
                );
            }
            anyhow::ensure!(
                returned >= length,
                "QueryInformationJobObject returned a truncated accounting structure"
            );
            Ok(accounting.ActiveProcesses)
        }
    }

    impl Drop for JobHandle {
        fn drop(&mut self) {
            // SAFETY: `self.0` is a valid handle owned exclusively by this
            // value; closing it is the required cleanup for `CreateJobObjectW`.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    /// Creates a Job Object configured to kill every member process when its
    /// last handle closes, and assigns the freshly spawned child to it.
    pub fn create_and_assign(child: &Child) -> anyhow::Result<JobHandle> {
        // SAFETY: null attributes/name create an anonymous, unnamed job
        // object; the returned handle is checked for failure below.
        let job: HANDLE = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if job.is_null() {
            anyhow::bail!(
                "CreateJobObjectW failed: {}",
                std::io::Error::last_os_error()
            );
        }
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        let info_size = u32::try_from(std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
            .unwrap_or(u32::MAX);
        // SAFETY: `info` is a valid, fully-initialized limit structure whose
        // size exactly matches `info_size`.
        let ok = unsafe {
            SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                std::ptr::addr_of!(info).cast(),
                info_size,
            )
        };
        if ok == 0 {
            let err = std::io::Error::last_os_error();
            // SAFETY: `job` was just validated non-null above.
            unsafe {
                CloseHandle(job);
            }
            anyhow::bail!("SetInformationJobObject failed: {err}");
        }
        let Some(process_handle) = child.raw_handle() else {
            // SAFETY: `job` was just validated non-null above.
            unsafe {
                CloseHandle(job);
            }
            anyhow::bail!("child process handle is no longer available (already reaped)");
        };
        let process_handle = process_handle as HANDLE;
        // SAFETY: `process_handle` is the live handle tokio holds for this
        // child for as long as `child` is alive, which outlives this call.
        let ok = unsafe { AssignProcessToJobObject(job, process_handle) };
        if ok == 0 {
            let err = std::io::Error::last_os_error();
            // SAFETY: `job` was just validated non-null above.
            unsafe {
                CloseHandle(job);
            }
            anyhow::bail!("AssignProcessToJobObject failed: {err}");
        }
        Ok(JobHandle(job))
    }

    fn filetime_to_u64(ft: FILETIME) -> u64 {
        (u64::from(ft.dwHighDateTime) << 32) | u64::from(ft.dwLowDateTime)
    }

    fn open_for_query(pid: u32) -> Option<HANDLE> {
        // SAFETY: opening a process by pid with a read-only access mask; the
        // returned handle is closed by every caller before returning.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() { None } else { Some(handle) }
    }

    /// The process creation time of `pid`, encoded as a decimal `FILETIME`.
    pub fn start_marker(pid: u32) -> anyhow::Result<Option<String>> {
        let Some(handle) = open_for_query(pid) else {
            return Ok(None);
        };
        // SAFETY: a zeroed `FILETIME` (two `u32` fields) is a valid bit
        // pattern; these are overwritten by `GetProcessTimes` below.
        let mut creation: FILETIME = unsafe { std::mem::zeroed() };
        let mut exit: FILETIME = unsafe { std::mem::zeroed() };
        let mut kernel: FILETIME = unsafe { std::mem::zeroed() };
        let mut user: FILETIME = unsafe { std::mem::zeroed() };
        // SAFETY: all four output pointers reference valid, live locals.
        let ok =
            unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
        // SAFETY: `handle` was just validated non-null above.
        unsafe {
            CloseHandle(handle);
        }
        if ok == 0 {
            return Ok(None);
        }
        Ok(Some(filetime_to_u64(creation).to_string()))
    }

    /// The executable image path Windows reports for `pid`, if the process is
    /// still alive and queryable. Windows exposes the resolved image rather
    /// than a command line here, which is enough to corroborate ownership the
    /// same way the Unix command-line check does.
    pub fn command_line(pid: u32) -> anyhow::Result<Option<String>> {
        let Some(handle) = open_for_query(pid) else {
            return Ok(None);
        };
        let mut buffer = [0u16; 1024];
        let mut size = u32::try_from(buffer.len()).unwrap_or(0);
        // SAFETY: `buffer` and `size` describe a valid, correctly sized
        // wide-character output buffer.
        let ok = unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut size) };
        // SAFETY: `handle` was just validated non-null above.
        unsafe {
            CloseHandle(handle);
        }
        if ok == 0 {
            return Ok(None);
        }
        let text = String::from_utf16_lossy(&buffer[..size as usize]);
        Ok(Some(text))
    }

    /// Terminates the single process named by `pid`, used only to clean up a
    /// stale process recovered from a previous run's identity file (where no
    /// live Job Object handle is available). A live [`ManagedProcess`] tree
    /// is torn down via [`JobHandle::terminate`] instead, which also takes
    /// down every descendant.
    pub fn terminate_pid(pid: u32) -> anyhow::Result<()> {
        // SAFETY: opening a process by pid to request termination; the
        // handle is closed before returning in every branch.
        let handle = unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) };
        if handle.is_null() {
            // Already gone.
            return Ok(());
        }
        // SAFETY: `handle` was just validated non-null above.
        let ok = unsafe { TerminateProcess(handle, 1) };
        unsafe {
            CloseHandle(handle);
        }
        if ok == 0 {
            anyhow::bail!(
                "TerminateProcess failed: {}",
                std::io::Error::last_os_error()
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::path::Path;
    use std::path::PathBuf;
    #[cfg(unix)]
    use std::time::Duration;

    #[cfg(unix)]
    use super::{ManagedProcess, ProcessGuard, record_identity_file};
    use super::{
        Ownership, ProcessIdentity, command_line_corroborates, read_identity_file,
        write_identity_file,
    };
    #[cfg(unix)]
    #[tokio::test]
    async fn managed_process_terminates_its_group_without_a_path_kill_helper() {
        const FIXTURE: &str = "GPQ_PROCESS_TEARDOWN_FIXTURE";
        if std::env::var_os(FIXTURE).is_some() {
            run_process_tree_fixture().await;
            return;
        }

        let dir = tempfile::tempdir()
            .unwrap_or_else(|error| panic!("creating fixture directory: {error}"));
        let path_dir = dir.path().join("path");
        std::fs::create_dir(&path_dir)
            .unwrap_or_else(|error| panic!("creating fixture PATH: {error}"));
        let path = std::env::var_os("PATH").unwrap_or_default();
        let ps = std::env::split_paths(&path)
            .map(|entry| entry.join("ps"))
            .find(|candidate| candidate.is_file())
            .unwrap_or_else(|| panic!("could not find ps for the isolated process fixture"));
        std::os::unix::fs::symlink(ps, path_dir.join("ps"))
            .unwrap_or_else(|error| panic!("installing fixture ps symlink: {error}"));

        let executable = std::env::current_exe()
            .unwrap_or_else(|error| panic!("locating test executable: {error}"));
        let output = std::process::Command::new(executable)
            .args([
                "--exact",
                "process::tests::managed_process_terminates_its_group_without_a_path_kill_helper",
                "--nocapture",
            ])
            .env("PATH", &path_dir)
            .env("TMPDIR", std::env::temp_dir())
            .env(FIXTURE, "1")
            .output()
            .unwrap_or_else(|error| panic!("running isolated process fixture: {error}"));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("1 passed;"),
            "isolated process fixture did not pass: stdout={stdout}\nstderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(unix)]
    async fn run_process_tree_fixture() {
        let dir = tempfile::tempdir()
            .unwrap_or_else(|error| panic!("creating process fixture state: {error}"));
        let child_pid_file = dir.path().join("descendant.pid");
        let args = [
            OsString::from("-c"),
            OsString::from(
                "/bin/sleep 600 & child=$!; trap 'wait \"$child\"; exit 0' TERM; \
                 printf '%s\\n' \"$child\" > \"$1\"; wait \"$child\"",
            ),
            OsString::from("managed-process-fixture"),
            child_pid_file.as_os_str().to_os_string(),
        ];
        let mut env = BTreeMap::new();
        env.insert(
            "PATH".to_string(),
            std::env::var("PATH").unwrap_or_default(),
        );
        let process = ManagedProcess::spawn(
            Path::new("/bin/sh"),
            &args,
            &env,
            dir.path(),
            "identity.json",
        )
        .await
        .unwrap_or_else(|error| panic!("spawning managed process fixture: {error:#}"));
        let mut process = ProcessGuard::new(process, Duration::from_millis(50));
        let identity = record_identity_file(
            process.pid(),
            Path::new("/bin/sh"),
            &dir.path().join("identity.json"),
        )
        .await
        .unwrap_or_else(|error| panic!("recording managed process identity: {error:#}"));
        process.set_identity(identity);
        let child_pid = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(contents) = tokio::fs::read_to_string(&child_pid_file).await
                    && let Ok(pid) = contents.trim().parse::<u32>()
                {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|error| panic!("waiting for fixture descendant PID: {error}"));

        process
            .terminate_tree(Duration::from_millis(50))
            .await
            .unwrap_or_else(|error| panic!("terminating managed process fixture: {error:#}"));
        let _process = process.disarm();
        assert_fixture_descendant_gone(child_pid).await;
    }

    #[cfg(unix)]
    async fn assert_fixture_descendant_gone(pid: u32) {
        let pid = libc::pid_t::try_from(pid)
            .unwrap_or_else(|error| panic!("fixture PID does not fit pid_t: {error}"));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            // SAFETY: signal zero only probes the specific fixture descendant.
            if unsafe { libc::kill(pid, 0) } == -1 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    return;
                }
                panic!("probing fixture descendant {pid}: {error}");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "managed process descendant {pid} remained alive after teardown"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn sample_identity() -> ProcessIdentity {
        ProcessIdentity {
            pid: 4242,
            start_marker: "Wed Aug 20 10:00:00 2026".to_string(),
            executable: PathBuf::from("/opt/gpq/gpq"),
            executable_size: 123_456,
            executable_mtime_unix: 1_755_000_000,
        }
    }

    #[tokio::test]
    async fn identity_file_round_trips() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let path = dir.path().join("nested").join("identity.json");
        let identity = sample_identity();

        let Ok(()) = write_identity_file(&path, &identity).await else {
            panic!("write identity")
        };
        let Ok(Some(loaded)) = read_identity_file(&path).await else {
            panic!("expected identity file to round-trip");
        };

        assert_eq!(loaded, identity);
    }

    #[tokio::test]
    async fn missing_identity_file_reads_as_none() {
        let Ok(dir) = tempfile::tempdir() else {
            panic!("tempdir")
        };
        let path = dir.path().join("absent.json");

        let Ok(loaded) = read_identity_file(&path).await else {
            panic!("read missing identity")
        };

        assert_eq!(loaded, None);
    }

    /// Pure decision-table check for the ownership comparison rules, without
    /// touching the real process table (ADR 0005: PID match alone is never
    /// enough).
    fn decide(
        recorded: &ProcessIdentity,
        live_marker: Option<&str>,
        live_size: Option<u64>,
    ) -> Ownership {
        let Some(live_marker) = live_marker else {
            return Ownership::Gone;
        };
        if live_marker != recorded.start_marker {
            return Ownership::Foreign;
        }
        match live_size {
            Some(size) if size == recorded.executable_size => Ownership::Owned,
            _ => Ownership::Foreign,
        }
    }

    #[test]
    fn command_line_corroboration_checks_the_native_worker_executable() {
        let identity = sample_identity();
        let executable = identity
            .executable
            .to_str()
            .unwrap_or_else(|| panic!("sample executable path must be UTF-8"));

        assert!(
            command_line_corroborates(&identity, Some(&format!("{executable} infer --pool gpu0"))),
            "the managed child command line must retain its native executable",
        );
        assert!(
            command_line_corroborates(&identity, None),
            "an unavailable command line is inconclusive, not disqualifying"
        );
        assert!(!command_line_corroborates(
            &identity,
            Some("/usr/bin/unrelated --flag")
        ));
    }

    #[test]
    fn ownership_table_requires_every_field_to_match() {
        let identity = sample_identity();

        assert_eq!(decide(&identity, None, None), Ownership::Gone);
        assert_eq!(
            decide(
                &identity,
                Some("different marker"),
                Some(identity.executable_size)
            ),
            Ownership::Foreign
        );
        assert_eq!(
            decide(&identity, Some(&identity.start_marker), Some(999)),
            Ownership::Foreign
        );
        assert_eq!(
            decide(
                &identity,
                Some(&identity.start_marker),
                Some(identity.executable_size)
            ),
            Ownership::Owned
        );
    }
}
