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

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};

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
    job: windows_job::JobHandle,
}

impl ManagedProcess {
    /// Spawns `executable` into its own process group/Job Object, records its
    /// durable identity atomically under `state_dir`, and returns the handle.
    pub async fn spawn(
        executable: &Path,
        args: &[String],
        env: &std::collections::BTreeMap<String, String>,
        state_dir: &Path,
        identity_file_name: &str,
    ) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(state_dir).await?;
        let mut command = Command::new(executable);
        command
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
        let job = windows_job::create_and_assign(&child)?;
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
        let identity_file = state_dir.join(identity_file_name);
        write_identity_file(&identity_file, &identity).await?;
        Ok(Self {
            child,
            identity,
            identity_file,
            #[cfg(windows)]
            job,
        })
    }

    /// The operating system process id.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.identity.pid
    }

    /// The durable identity recorded for this process.
    #[must_use]
    pub fn identity(&self) -> &ProcessIdentity {
        &self.identity
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

    /// Terminates the whole process tree: SIGTERM followed by SIGKILL if the
    /// group has not exited within `graceful` (Unix), or an immediate
    /// `TerminateJobObject` (Windows, where there is no portable polite
    /// signal every backend would honor).
    pub async fn terminate_tree(&mut self, graceful: Duration) -> anyhow::Result<()> {
        #[cfg(unix)]
        unix_ps::terminate_group(self.identity.pid, graceful).await?;
        #[cfg(windows)]
        {
            let _ = graceful;
            self.job.terminate()?;
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), self.wait_exit()).await;
        let _ = tokio::fs::remove_file(&self.identity_file).await;
        Ok(())
    }
}

/// Owns a freshly [`ManagedProcess::spawn`]ed process until [`Self::disarm`]
/// hands it to long-lived Pool state, terminating the whole process tree on
/// drop otherwise.
///
/// `ManagedProcess::spawn` deliberately sets `kill_on_drop(false)` (ADR
/// 0005: an orderly `terminate_tree` — SIGTERM then SIGKILL on Unix, the Job
/// Object on Windows — is what ends a managed process, not the OS reaping an
/// orphan when a bare `Child` handle merely goes out of scope). That makes
/// every return between a successful spawn and storing the process in
/// `PoolState::process` responsible for terminating it explicitly, and a
/// future early return that forgets to is a silent GPU-holding leak. This
/// guard removes that responsibility from every call site: hold it across
/// the fallible startup sequence and call [`Self::disarm`] only once the
/// process is durably owned elsewhere.
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

/// Writes a durable identity file atomically (temp file + rename).
pub async fn write_identity_file(path: &Path, identity: &ProcessIdentity) -> anyhow::Result<()> {
    let Some(parent) = path.parent() else {
        anyhow::bail!(
            "identity file path {} has no parent directory",
            path.display()
        );
    };
    tokio::fs::create_dir_all(parent).await?;
    let temp_path = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("identity"),
        std::process::id()
    ));
    let body = serde_json::to_vec_pretty(identity)?;
    tokio::fs::write(&temp_path, body).await?;
    tokio::fs::rename(&temp_path, path).await?;
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
    // Corroborate with the live command line rather than the OS-reported
    // executable name: an interpreter-launched backend (ComfyUI is
    // `python main.py`) reports the interpreter, and `ps -o comm=` truncates,
    // so requiring equality would classify every such child as foreign and
    // leave its Pool unready forever. A command line that still mentions the
    // executable we spawned, together with the exact start marker below, is
    // what proves ownership (ADR 0005).
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
    unix_ps::terminate_group(pid, graceful).await
}

#[cfg(windows)]
async fn terminate_pid_tree(pid: u32, _graceful: Duration) -> anyhow::Result<()> {
    windows_job::terminate_pid(pid)
}

/// Unix process-table queries and group signalling, implemented by shelling
/// out to the POSIX-mandated `ps` and `kill` utilities rather than raw FFI.
#[cfg(unix)]
mod unix_ps {
    use std::time::Duration;

    use tokio::process::Command;

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

    /// Sends SIGTERM to the whole process group led by `pid`, then SIGKILL if
    /// any member is still alive after `graceful`.
    pub async fn terminate_group(pid: u32, graceful: Duration) -> anyhow::Result<()> {
        signal_group(pid, "-TERM").await?;
        tokio::time::sleep(graceful).await;
        if group_alive(pid).await? {
            signal_group(pid, "-KILL").await?;
        }
        Ok(())
    }

    async fn signal_group(pid: u32, signal: &str) -> anyhow::Result<()> {
        // A negative pid targets every process in that process group.
        let target = format!("-{pid}");
        let status = Command::new("kill")
            .args([signal, &target])
            .status()
            .await?;
        // Exit code 1 from `kill` means "no such process", which is fine: the
        // group may have already exited on its own.
        if !status.success() && status.code() != Some(1) {
            anyhow::bail!("kill {signal} {target} failed with {status}");
        }
        Ok(())
    }

    async fn group_alive(pid: u32) -> anyhow::Result<bool> {
        let target = format!("-{pid}");
        let status = Command::new("kill").args(["-0", &target]).status().await?;
        Ok(status.success())
    }
}

/// Windows Job Object management and process-table queries.
#[cfg(windows)]
mod windows_job {
    use tokio::process::Child;
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject,
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
    use std::path::PathBuf;

    use super::{
        Ownership, ProcessIdentity, command_line_corroborates, read_identity_file,
        write_identity_file,
    };

    fn sample_identity() -> ProcessIdentity {
        ProcessIdentity {
            pid: 4242,
            start_marker: "Wed Aug 20 10:00:00 2026".to_string(),
            executable: PathBuf::from("/opt/gpq/llama-server"),
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
    fn command_line_corroboration_accepts_interpreter_launched_backends() {
        let identity = sample_identity();
        let executable = identity
            .executable
            .to_str()
            .unwrap_or_else(|| panic!("sample executable path must be UTF-8"));

        assert!(
            command_line_corroborates(
                &identity,
                Some(&format!("python3 {executable} --port 8188"))
            ),
            "ComfyUI runs as `python main.py`, so the image name is the interpreter"
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
