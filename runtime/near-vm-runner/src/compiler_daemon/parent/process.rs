//! Lifecycle and IPC for one compiler-daemon worker process.

use crate::compiler_daemon::DAEMON_STARTUP_TIMEOUT;
#[cfg(feature = "test_features")]
use crate::compiler_daemon::protocol::TestAction;
use crate::compiler_daemon::protocol::{
    COMPILER_DAEMON_MEMORY_LIMIT_ENV, COMPILER_DAEMON_STACK_SIZE_ENV, COMPILER_DAEMON_THREADS_ENV,
    CompileRequest, DaemonStartup, DaemonStatus, IsolationStatus, MemoryLimitStatus, WorkerConfig,
    read_compile_response, read_frame, write_frame,
};
use crate::compiler_daemon::watchdog::{
    ProcessControl, ProcessWatchdog, TerminationReason, WatchdogError,
};
use crate::compiler_daemon::worker_failure::WorkerFailure;
use crate::wasmtime_runner::compiler_compatibility_hash;
#[cfg(target_os = "linux")]
use libc::{SCHED_OTHER, sched_param, sched_setscheduler};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult};
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::thread::{Builder, JoinHandle};
use std::time::{Duration, Instant};

static EXPECTED_COMPILER_COMPATIBILITY_HASH: OnceLock<Result<u64, String>> = OnceLock::new();
const PROCESS_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(1);
type CompileResult = Result<Vec<u8>, String>;

/// Parent-side handle to a spawned worker subprocess.
pub(super) struct DaemonProcess {
    control: Arc<ProcessControl>,
    stdin: ChildStdin,
    stdout: ChildStdout,
    teardown: Option<ProcessTeardown>,
    watchdog: ProcessWatchdog,
    status: Option<DaemonStatus>,
}

pub(super) struct SpawnFailure {
    pub(super) failure: WorkerFailure,
    pub(super) teardown: Option<ProcessTeardown>,
}

/// Sole owner of final process cleanup, including reservation-release notification.
/// Failed spawns return this to the coordinator before any teardown begins.
pub(super) struct ProcessTeardown {
    control: Arc<ProcessControl>,
    stderr_thread: Option<JoinHandle<()>>,
}

impl DaemonProcess {
    pub(super) fn spawn(binary: &Path, config: WorkerConfig) -> Result<Self, SpawnFailure> {
        // Do not inherit environment-based allocator, proxy, logging, or
        // compiler configuration from neard. The variables below are the
        // explicit process-level configuration contract for the worker.
        let mut command = Command::new(binary);
        command
            .arg("compile-wasm")
            .env_clear()
            .env(COMPILER_DAEMON_THREADS_ENV, config.threads.to_string())
            .env(COMPILER_DAEMON_STACK_SIZE_ENV, config.thread_stack_size_bytes.to_string())
            .env(COMPILER_DAEMON_MEMORY_LIMIT_ENV, config.memory_limit_bytes.to_string())
            .current_dir("/")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // Normalize the OS thread scheduling priority for spawned processed,
        // rather than inheriting the parent's priority.
        //
        // Changing the scheduling policy does not change the nice value,
        // so the child retains neard's baseline nice value.
        #[cfg(target_os = "linux")]
        // SAFETY: `sched_setscheduler` is async-signal-safe and the closure does
        // not access any state shared with the parent process.
        unsafe {
            command.pre_exec(|| {
                let param = sched_param { sched_priority: 0 };
                if sched_setscheduler(0, SCHED_OTHER, &param) == -1 {
                    return Err(IoError::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().map_err(|err| SpawnFailure {
            failure: WorkerFailure::Spawn(err.to_string()),
            teardown: None,
        })?;
        let stdin = child.stdin.take().expect("stdio configured as piped");
        let stdout = child.stdout.take().expect("stdio configured as piped");
        let child_stderr = child.stderr.take().expect("stdio configured as piped");
        let worker_id = child.id();
        let control = Arc::new(ProcessControl::new(child));
        let stderr_thread = match Builder::new()
            .name("compiler-daemon-stderr".to_owned())
            .spawn(move || relay_stderr(child_stderr, worker_id))
        {
            Ok(thread) => Some(thread),
            Err(err) => {
                return Err(SpawnFailure {
                    failure: WorkerFailure::Spawn(err.to_string()),
                    teardown: Some(ProcessTeardown { control, stderr_thread: None }),
                });
            }
        };
        let watchdog = match ProcessWatchdog::spawn(Arc::clone(&control)) {
            Ok(watchdog) => watchdog,
            Err(err) => {
                return Err(SpawnFailure {
                    failure: WorkerFailure::Spawn(err.to_string()),
                    teardown: Some(ProcessTeardown { control, stderr_thread }),
                });
            }
        };
        let teardown = Some(ProcessTeardown { control: Arc::clone(&control), stderr_thread });
        let mut process = Self { control, stdin, stdout, teardown, watchdog, status: None };
        match process.wait_for_startup(config) {
            Ok(status) => {
                process.status = Some(status);
                Ok(process)
            }
            Err(failure) => {
                process.watchdog.shutdown();
                let teardown = process.teardown.take();
                Err(SpawnFailure { failure, teardown })
            }
        }
    }

    fn wait_for_startup(&mut self, config: WorkerConfig) -> Result<DaemonStatus, WorkerFailure> {
        self.watchdog.arm(DAEMON_STARTUP_TIMEOUT, "startup");
        let result = read_frame(&mut self.stdout)
            .map_err(|err| format!("failed to read startup response: {err}"))
            .map(|bytes| -> Result<DaemonStatus, WorkerFailure> {
                // A complete response carries an explicit startup or
                // configuration result. Keep those errors distinct from
                // transport failures classified from the child exit.
                let startup: DaemonStartup = borsh::from_slice(&bytes).map_err(|err| {
                    WorkerFailure::Startup(format!("failed to deserialize startup response: {err}"))
                })?;
                match startup {
                    DaemonStartup::Ready(status) => {
                        validate_daemon_status(status, config).map_err(WorkerFailure::Startup)
                    }
                    DaemonStartup::Err(err) => Err(WorkerFailure::Startup(err)),
                }
            });
        match self.watchdog.finish(result) {
            Ok(result) => result,
            Err(WatchdogError::Timeout { phase, timeout }) => {
                Err(self.finish_failed_ipc(String::new(), Some((phase, timeout))))
            }
            Err(WatchdogError::Operation(err)) => {
                // EOF can become visible before the corresponding wait status.
                // Use the same bounded exit-classification grace period as
                // compilation IPC rather than losing a startup OOM classification.
                Err(self.finish_failed_ipc(err, None))
            }
        }
    }

    /// Send a compilation request and read the response. Returns:
    /// - `Ok(Ok(bytes))` -- compilation succeeded
    /// - `Ok(Err(msg))` -- daemon reported a compilation error (not retryable)
    /// - `Err(failure)` -- typed worker/process failure (retryable here)
    pub(super) fn compile_raw(
        &mut self,
        request: &CompileRequest<'_>,
    ) -> Result<CompileResult, WorkerFailure> {
        let request_bytes = borsh::to_vec(request).map_err(|e| WorkerFailure::Protocol {
            error: format!("failed to serialize request: {e}"),
            cleanup_status: None,
        })?;
        // The test-only `Timeout` action exercises watchdog recovery from an
        // unresponsive worker. Normal compilation requests have no deadline
        // right now, since we decided a hanging node is preferable to crashing
        // or committing a potentially nondeterministic error.
        let timeout = compilation_request_timeout(request);
        if let Some(timeout) = timeout {
            self.watchdog.arm(timeout, "compilation request");
        }
        let result = write_frame(&mut self.stdin, &request_bytes)
            .map_err(|e| format!("failed to send to compiler daemon: {e}"))
            .and_then(|()| {
                read_compile_response(&mut self.stdout)
                    .map_err(|e| format!("failed to read from compiler daemon: {e}"))
            });
        match timeout {
            Some(_) => match self.watchdog.finish(result) {
                Ok(result) => Ok(result),
                Err(WatchdogError::Timeout { phase, timeout }) => {
                    Err(self.finish_failed_ipc(String::new(), Some((phase, timeout))))
                }
                Err(WatchdogError::Operation(err)) => Err(self.finish_failed_ipc(err, None)),
            },
            None => result.map_err(|err| self.finish_failed_ipc(err, None)),
        }
    }

    /// Classify the original IPC failure before any cleanup signal can obscure
    /// an already available child exit status.
    fn finish_failed_ipc(
        &mut self,
        protocol_error: String,
        timeout: Option<(&'static str, Duration)>,
    ) -> WorkerFailure {
        self.watchdog.shutdown();

        let mut natural_status = self.control.try_status().ok().flatten();
        if let Some((phase, timeout)) = timeout {
            debug_assert_eq!(
                self.control.termination_reason(),
                Some(TerminationReason::WatchdogTimeout { phase, timeout })
            );
            let _ = self.control.wait_for_exit(PROCESS_TEARDOWN_TIMEOUT);
            return WorkerFailure::WatchdogTimeout { phase, timeout };
        }

        if natural_status.is_none() {
            // EOF and wait status become observable through different kernel
            // interfaces. Give a naturally exiting peer a short grace period
            // before deciding that cleanup must terminate a still-live child.
            natural_status = self.control.wait_for_exit(Duration::from_millis(100)).ok().flatten();
        }
        if self.control.termination_reason() == Some(TerminationReason::SchedulerEviction) {
            return WorkerFailure::Evicted;
        }
        if let Some(status) = natural_status {
            #[cfg(unix)]
            if status.code() == Some(crate::compiler_daemon::WORKER_MEMORY_EXHAUSTED_EXIT_CODE) {
                return WorkerFailure::LocalMemoryExhaustion;
            }
            return WorkerFailure::Crash { status, protocol_error };
        }

        // The peer broke the protocol while still alive. Record cleanup as a
        // parent action before killing it, and never report the cleanup signal
        // as the original failure cause.
        let _ = self.control.terminate(TerminationReason::ProtocolCleanup);
        let cleanup_status = self.control.wait_for_exit(PROCESS_TEARDOWN_TIMEOUT).ok().flatten();
        WorkerFailure::Protocol { error: protocol_error, cleanup_status }
    }

    pub(super) fn retire(
        mut self,
        reason: TerminationReason,
        on_reaped: impl FnOnce() + Send + 'static,
    ) {
        self.watchdog.shutdown();
        self.teardown.take().expect("process teardown already taken").finish(reason, on_reaped);
    }

    pub(super) fn status(&self) -> &DaemonStatus {
        self.status.as_ref().expect("daemon startup status unavailable")
    }

    pub(super) fn control(&self) -> Arc<ProcessControl> {
        Arc::clone(&self.control)
    }

    pub(super) fn is_alive(&self) -> bool {
        matches!(self.control.try_status(), Ok(None))
    }
}

fn validate_daemon_status(
    status: DaemonStatus,
    expected_config: WorkerConfig,
) -> Result<DaemonStatus, String> {
    let expected_hash = EXPECTED_COMPILER_COMPATIBILITY_HASH
        .get_or_init(|| {
            compiler_compatibility_hash()
                .map_err(|err| format!("failed to create local compatibility engine: {err}"))
        })
        .clone()?;
    if status.compiler_compatibility_hash != expected_hash {
        return Err(format!(
            "compiler compatibility mismatch: daemon reported {}, expected {expected_hash}",
            status.compiler_compatibility_hash
        ));
    }
    if status.worker_config != expected_config {
        return Err(format!(
            "compiler daemon configuration mismatch: daemon reported {} threads with {} byte stacks and a {} byte memory limit, expected {} threads with {} byte stacks and a {} byte memory limit",
            status.worker_config.threads,
            status.worker_config.thread_stack_size_bytes,
            status.worker_config.memory_limit_bytes,
            expected_config.threads,
            expected_config.thread_stack_size_bytes,
            expected_config.memory_limit_bytes,
        ));
    }
    #[cfg(unix)]
    if status.memory_limit
        != (MemoryLimitStatus::Enforced { memory_limit_bytes: expected_config.memory_limit_bytes })
    {
        return Err(format!(
            "compiler daemon did not enforce the requested {} byte memory limit: {:?}",
            expected_config.memory_limit_bytes, status.memory_limit
        ));
    }
    #[cfg(not(unix))]
    if status.memory_limit != MemoryLimitStatus::Unavailable {
        return Err(format!(
            "unexpected compiler daemon memory limit status: {:?}",
            status.memory_limit
        ));
    }
    #[cfg(target_os = "linux")]
    if !matches!(status.isolation, IsolationStatus::LinuxLandlock { abi: 1.. }) {
        return Err(format!(
            "compiler daemon did not enable landlock isolation: {:?}; ensure the kernel is at least 5.13, CONFIG_SECURITY_LANDLOCK is enabled, landlock is in the active LSM list, and the container seccomp profile allows landlock syscalls, or disable enable_compiler_daemon",
            status.isolation
        ));
    }
    #[cfg(not(target_os = "linux"))]
    if status.isolation != IsolationStatus::Unavailable {
        return Err(format!("unexpected compiler daemon isolation: {:?}", status.isolation));
    }
    Ok(status)
}

fn compilation_request_timeout(_request: &CompileRequest<'_>) -> Option<Duration> {
    #[cfg(feature = "test_features")]
    if _request.test_action == Some(TestAction::Timeout) {
        return Some(Duration::from_millis(100));
    }
    None
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        self.watchdog.shutdown();
        if let Some(teardown) = self.teardown.take() {
            teardown.finish(TerminationReason::ProcessDrop, || {});
        }
    }
}

impl ProcessTeardown {
    pub(super) fn finish(
        self,
        reason: TerminationReason,
        on_reaped: impl FnOnce() + Send + 'static,
    ) {
        let _ = self.control.terminate(reason);
        self.wait_for_exit(PROCESS_TEARDOWN_TIMEOUT, on_reaped);
    }

    fn wait_for_exit(self, timeout: Duration, on_reaped: impl FnOnce() + Send + 'static) {
        let Self { control, stderr_thread } = self;
        let reaped = control.wait_for_exit(timeout).ok().flatten().is_some();
        let mut on_reaped = Some(on_reaped);
        if reaped {
            on_reaped.take().unwrap()();
            if stderr_thread.as_ref().is_none_or(JoinHandle::is_finished) {
                if let Some(thread) = stderr_thread {
                    let _ = thread.join();
                }
                return;
            }
        }

        // One owner completes both process cleanup and reservation release.
        // Neither blocking reap nor a slow stderr relay can hold up the caller.
        let worker_id = control.id();
        let reaper = control.take_reaper();
        if Builder::new()
            .name("compiler-daemon-reaper".to_owned())
            .spawn(move || {
                match reaper.reap() {
                    Ok(_) => {
                        if let Some(on_reaped) = on_reaped {
                            on_reaped();
                        }
                    }
                    Err(err) => tracing::warn!(
                        target: "vm", worker_id, %err,
                        "failed to reap compiler daemon; retaining its reservation"
                    ),
                }
                if let Some(thread) = stderr_thread {
                    let _ = thread.join();
                }
            })
            .is_err()
        {
            // Do not fall back to an unbounded wait on the compilation caller.
            tracing::warn!(target: "vm", worker_id, "failed to start compiler daemon reaper; retaining any unreaped reservation");
        }
    }
}

/// Drain worker stderr so it cannot block on a full pipe.
///
/// Limit the data sent to neard's structured logs per time interval, discarding
/// excess output, to avoid unbounded memory usage on neard.
fn relay_stderr(mut child_stderr: ChildStderr, worker_id: u32) {
    let stderr_relay_interval = Duration::from_secs(60);
    let stderr_relay_limit_bytes = bytesize::kib(256u64);

    let mut buffer = [0; 4096];
    let mut interval_start = Instant::now();
    let mut relayed = 0;
    let mut rate_limit_reported = false;

    loop {
        let count = match read_retrying_on_interrupt(&mut child_stderr, &mut buffer) {
            Ok(0) => return,
            Ok(count) => count as u64,
            Err(err) => {
                tracing::warn!(target: "vm", worker_id, %err, "failed to read compiler daemon stderr");
                return;
            }
        };
        if interval_start.elapsed() >= stderr_relay_interval {
            interval_start = Instant::now();
            relayed = 0;
            rate_limit_reported = false;
        }

        let relay_count = count.min(stderr_relay_limit_bytes.saturating_sub(relayed));
        if relay_count > 0 {
            let output = String::from_utf8_lossy(&buffer[..relay_count as usize]);
            tracing::warn!(target: "vm", worker_id, stderr = %output, "compiler daemon stderr");
            relayed += relay_count;
        }
        if relay_count < count && !rate_limit_reported {
            tracing::warn!(target: "vm", worker_id, "compiler daemon stderr rate limit exceeded");
            rate_limit_reported = true;
        }
    }
}

fn read_retrying_on_interrupt(reader: &mut impl Read, buffer: &mut [u8]) -> IoResult<usize> {
    loop {
        match reader.read(buffer) {
            Err(err) if err.kind() == ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{read_retrying_on_interrupt, validate_daemon_status};
    use crate::compiler_daemon::protocol::{
        DaemonStatus, IsolationStatus, MemoryLimitStatus, WorkerConfig,
    };
    use crate::wasmtime_runner::compiler_compatibility_hash;
    use std::io::{Cursor, Error, ErrorKind, Read, Result};

    struct InterruptedOnce {
        interrupted: bool,
        input: Cursor<&'static [u8]>,
    }

    impl Read for InterruptedOnce {
        fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(Error::from(ErrorKind::Interrupted));
            }
            self.input.read(buffer)
        }
    }

    #[test]
    fn stderr_read_retries_when_interrupted() {
        let mut input =
            InterruptedOnce { interrupted: false, input: Cursor::new(b"daemon output") };
        let mut buffer = [0; 32];

        let count = read_retrying_on_interrupt(&mut input, &mut buffer).unwrap();

        assert_eq!(&buffer[..count], b"daemon output");
    }

    #[cfg(unix)]
    mod unix_tests {
        use super::super::{DaemonProcess, ProcessTeardown};
        use crate::compiler_daemon::WORKER_MEMORY_EXHAUSTED_EXIT_CODE;
        use crate::compiler_daemon::protocol::WorkerConfig;
        use crate::compiler_daemon::watchdog::{
            ProcessControl, ProcessWatchdog, TerminationReason,
        };
        use crate::compiler_daemon::worker_failure::WorkerFailure;
        use std::process::{Command, Stdio};
        use std::sync::Arc;
        use std::sync::mpsc::{TryRecvError, channel};
        use std::thread::spawn;
        use std::time::Duration;

        #[test]
        fn transport_failure_observes_delayed_memory_exhaustion_exit() {
            // Close stdout first so EOF is observable before the exit status,
            // reproducing the kernel-interface race from worker startup.
            let mut child = Command::new("sh")
                .args([
                    "-c",
                    &format!("exec 1>&-; sleep 0.02; exit {WORKER_MEMORY_EXHAUSTED_EXIT_CODE}"),
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let stdin = child.stdin.take().unwrap();
            let stdout = child.stdout.take().unwrap();
            let control = Arc::new(ProcessControl::new(child));
            let watchdog = ProcessWatchdog::spawn(Arc::clone(&control)).unwrap();
            let teardown =
                Some(ProcessTeardown { control: Arc::clone(&control), stderr_thread: None });
            let mut process =
                DaemonProcess { control, stdin, stdout, teardown, watchdog, status: None };
            let config =
                WorkerConfig { threads: 1, thread_stack_size_bytes: 1, memory_limit_bytes: 1 };

            let failure = process.wait_for_startup(config).unwrap_err();

            assert!(matches!(failure, WorkerFailure::LocalMemoryExhaustion));
        }

        #[test]
        fn detached_reap_keeps_status_nonblocking_and_notifies_only_after_exit() {
            // Hold stdin outside Child so wait() cannot close it. The child
            // cannot exit until the test releases it, regardless of scheduling.
            let mut child = Command::new("sh")
                .args(["-c", "read line; exit 0"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let stdin = child.stdin.take().unwrap();
            let control = Arc::new(ProcessControl::new(child));
            let teardown = ProcessTeardown { control: Arc::clone(&control), stderr_thread: None };
            let (reaped_tx, reaped_rx) = channel();
            let (returned_tx, returned_rx) = channel();
            let caller = spawn(move || {
                // Enter the post-termination wait directly to model a child
                // whose exit is delayed even after a termination request.
                teardown.wait_for_exit(Duration::ZERO, move || reaped_tx.send(()).unwrap());
                returned_tx.send(()).unwrap();
            });
            let timeout = Duration::from_secs(5);
            returned_rx.recv_timeout(timeout).expect("teardown caller remained blocked");
            assert_eq!(reaped_rx.try_recv(), Err(TryRecvError::Empty));

            // Child ownership has already transferred before teardown returns.
            // Status inspection must not contend with the reaper's wait().
            let (status_tx, status_rx) = channel();
            let observer_control = Arc::clone(&control);
            let observer = spawn(move || {
                status_tx.send(observer_control.try_status().unwrap()).unwrap();
            });
            assert_eq!(status_rx.recv_timeout(timeout).unwrap(), None);
            drop(stdin);
            reaped_rx.recv_timeout(timeout).expect("reap did not release the reservation");
            assert!(control.try_status().unwrap().unwrap().success());
            caller.join().unwrap();
            observer.join().unwrap();
        }

        #[test]
        fn already_reaped_worker_notifies_synchronously() {
            let child = Command::new("sh").args(["-c", "exit 0"]).spawn().unwrap();
            let control = Arc::new(ProcessControl::new(child));
            assert!(control.wait_for_exit(Duration::from_secs(5)).unwrap().is_some());
            let teardown = ProcessTeardown { control, stderr_thread: None };
            let (tx, rx) = channel();
            teardown.finish(TerminationReason::ProcessDrop, move || tx.send(()).unwrap());
            rx.try_recv().expect("confirmed reap did not release the reservation synchronously");
            assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        }
    }

    #[test]
    fn rejects_unacknowledged_worker_memory_limit() {
        let config =
            WorkerConfig { threads: 1, thread_stack_size_bytes: 1024, memory_limit_bytes: 4096 };
        #[cfg(unix)]
        let memory_limit = MemoryLimitStatus::Unavailable;
        #[cfg(not(unix))]
        let memory_limit = MemoryLimitStatus::Enforced { memory_limit_bytes: 4096 };
        let status = DaemonStatus {
            compiler_compatibility_hash: compiler_compatibility_hash().unwrap(),
            isolation: IsolationStatus::Unavailable,
            memory_limit,
            worker_config: config,
        };

        let err = validate_daemon_status(status, config).unwrap_err();
        #[cfg(unix)]
        assert!(err.contains("did not enforce the requested 4096 byte memory limit"), "{err}");
        #[cfg(not(unix))]
        assert!(err.contains("unexpected compiler daemon memory limit status"), "{err}");
    }
}
