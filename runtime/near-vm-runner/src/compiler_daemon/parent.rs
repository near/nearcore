//! Parent-side client for the out-of-process compiler daemon.
//!
//! A pool of worker subprocesses serves compilations in parallel, so
//! independent compilations shards run concurrently with independet memory
//! limits . The pool spawns workers lazily up to a configured maximum and
//! blocks callers when all workers are busy.
//!
//! Worker checkout is priority-ordered: when a worker frees up, the most urgent
//! waiting caller is served first (see [`CompilePriority`]).

use super::protocol::{CompileRequest, CompileResponse, DaemonStartup, read_frame, write_frame};
use crate::compile_priority::CompilePriority;
use crate::compiler_daemon::{
    DEFAULT_RAYON_THREADS_PER_WORKER, DEFAULT_TOTAL_MEMORY_BUDGET_BYTES, MAX_POOL_SIZE,
    MAX_SPAWN_ATTEMPTS, MIN_WORKER_MEMORY_LIMIT_BYTES,
};
use crate::logic::errors::{CompilationError, VMRunnerError};
use near_parameters::vm::LimitConfig;
use parking_lot::{Condvar, Mutex};
use std::array::from_fn;
use std::io::{Error as IoError, ErrorKind, Read, Write, stderr};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::OnceLock;
use std::thread::{Builder, JoinHandle, available_parallelism};
use std::time::{Duration, Instant};

static DAEMON_BINARY: OnceLock<PathBuf> = OnceLock::new();
static DAEMON_POOL_SIZE: OnceLock<usize> = OnceLock::new();
static DAEMON_POOL: OnceLock<DaemonPool> = OnceLock::new();

/// Set the path to the binary that should be spawned as the compiler daemon.
///
/// Only works once, subsequent calls are ignored.
pub fn set_daemon_binary(path: PathBuf) {
    if DAEMON_BINARY.set(path).is_err() {
        tracing::warn!("set_daemon_binary called more than once, ignoring");
    }
}

/// Configure the maximum number of compiler-daemon worker subprocesses.
/// Must be called before the first compilation; later calls are ignored.
/// If never called, defaults to the smaller of CPU parallelism and
/// `DEFAULT_TOTAL_MEMORY_BUDGET_BYTES` divided by the per-worker memory budget,
/// clamped to `[1, MAX_POOL_SIZE]`.
pub fn set_daemon_pool_size(size: usize) {
    if DAEMON_POOL_SIZE.set(size).is_err() {
        tracing::warn!("set_daemon_pool_size called more than once, ignoring");
    }
}

/// Returns true if a daemon binary has been configured via `set_daemon_binary`.
pub fn is_daemon_configured() -> bool {
    DAEMON_BINARY.get().is_some()
}

type CompileResult = Result<Vec<u8>, String>;

struct DaemonProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    stderr_thread: Option<JoinHandle<()>>,
}

impl DaemonProcess {
    fn spawn(binary: &Path) -> std::io::Result<Self> {
        // The compiler is fully configured through IPC. In particular it must
        // not inherit environment-based allocator, proxy, logging, or compiler
        // configuration from neard.
        let mut command = Command::new(binary);
        command
            .env_clear()
            // Rayon determines its global compilation pool size from this
            // variable.
            // TODO: make this configurable
            .env("RAYON_NUM_THREADS", DEFAULT_RAYON_THREADS_PER_WORKER.to_string())
            .current_dir("/")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let child_stderr = child.stderr.take().unwrap();
        let stderr_thread = match Builder::new()
            .name("compiler-daemon-stderr".to_owned())
            .spawn(move || relay_stderr(child_stderr))
        {
            Ok(thread) => Some(thread),
            Err(err) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(err);
            }
        };
        if let Err(err) = wait_for_startup(&mut stdout) {
            let _ = child.kill();
            let _ = child.wait();
            if let Some(stderr_thread) = stderr_thread {
                let _ = stderr_thread.join();
            }
            return Err(err);
        }
        Ok(Self { child, stdin, stdout, stderr_thread })
    }

    /// Send a compilation request and read the response. Returns:
    /// - `Ok(Ok(bytes))` -- compilation succeeded
    /// - `Ok(Err(msg))` -- daemon reported a compilation error (not retryable)
    /// - `Err(msg)` -- IPC failure, daemon likely crashed (retryable)
    fn compile_raw(&mut self, request: &CompileRequest) -> Result<CompileResult, String> {
        let request_bytes =
            borsh::to_vec(request).map_err(|e| format!("failed to serialize request: {e}"))?;
        write_frame(&mut self.stdin, &request_bytes)
            .map_err(|e| format!("failed to send to compiler daemon: {e}"))?;
        let response_bytes = read_frame(&mut self.stdout)
            .map_err(|e| format!("failed to read from compiler daemon: {e}"))?;
        let response: CompileResponse = borsh::from_slice(&response_bytes)
            .map_err(|e| format!("failed to deserialize response: {e}"))?;
        match response {
            CompileResponse::Ok(bytes) => Ok(Ok(bytes)),
            CompileResponse::Err(msg) => Ok(Err(msg)),
        }
    }

    fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

fn wait_for_startup(stdout: &mut ChildStdout) -> std::io::Result<()> {
    let bytes = read_frame(stdout)?;
    let startup: DaemonStartup = borsh::from_slice(&bytes)
        .map_err(|err| IoError::new(ErrorKind::InvalidData, err.to_string()))?;
    match startup {
        DaemonStartup::Ready => Ok(()),
        DaemonStartup::Err(err) => Err(IoError::new(ErrorKind::PermissionDenied, err)),
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(stderr_thread) = self.stderr_thread.take() {
            let _ = stderr_thread.join();
        }
    }
}

/// Drain worker stderr so it cannot block on a full pipe.
///
/// Limit the data sent to neard via stderr per time interval, discarding excess
/// output, to avoid unbounded memory usage on neard.
fn relay_stderr(mut child_stderr: ChildStderr) {
    let stderr_relay_interval = Duration::from_secs(60);
    let stderr_relay_limit_bytes = bytesize::kib(256u64);

    let mut buffer = [0; 4096];
    let mut interval_start = Instant::now();
    let mut relayed = 0;
    let mut rate_limit_reported = false;

    loop {
        let count = match child_stderr.read(&mut buffer) {
            Ok(0) | Err(_) => return,
            Ok(count) => count as u64,
        };
        if interval_start.elapsed() >= stderr_relay_interval {
            interval_start = Instant::now();
            relayed = 0;
            rate_limit_reported = false;
        }

        let relay_count = count.min(stderr_relay_limit_bytes.saturating_sub(relayed));
        if relay_count > 0 {
            let mut parent_stderr = stderr().lock();
            if parent_stderr.write_all(&buffer[..relay_count as usize]).is_err() {
                return;
            }
            let _ = parent_stderr.flush();
            relayed += relay_count;
        }
        if relay_count < count && !rate_limit_reported {
            tracing::warn!("compiler daemon stderr rate limit exceeded");
            rate_limit_reported = true;
        }
    }
}

struct PoolInner {
    /// Workers that are spawned and currently idle, ready to be checked out.
    idle: Vec<DaemonProcess>,
    /// Number of workers currently "live": idle + checked-out + being-spawned.
    /// This is the permit count; invariant: `idle.len() <= live <= max_workers`.
    live: usize,
    /// Number of callers blocked waiting for a worker, per priority class.
    waiters: [usize; CompilePriority::COUNT],
    /// Maximum `live` ever reached. Diagnostic witness that parallelism
    /// occurred (used by tests).
    #[cfg(feature = "test_features")]
    high_water: usize,
}

struct DaemonPool {
    binary: PathBuf,
    max_workers: usize,
    inner: Mutex<PoolInner>,
    /// One wait queue per priority class; index by `CompilePriority::index`.
    avail: [Condvar; CompilePriority::COUNT],
}

impl DaemonPool {
    /// Block until a worker is available.
    fn checkout(&self, priority: CompilePriority) -> Result<DaemonProcess, String> {
        let idx = priority.index();
        let mut inner = self.inner.lock();
        // Register before inspecting capacity so a newly arriving lower-priority
        // caller cannot steal a worker from an already-waiting higher-priority
        // caller while the latter is waking up.
        inner.waiters[idx] += 1;

        loop {
            if !priority_may_checkout(priority, &inner.waiters) {
                self.avail[idx].wait(&mut inner);
                continue;
            }

            // 1. Reuse an idle worker, draining any that have died.
            while let Some(mut worker) = inner.idle.pop() {
                if worker.is_alive() {
                    inner.waiters[idx] -= 1;
                    if !inner.idle.is_empty() {
                        self.wake_one(&inner);
                    }
                    return Ok(worker);
                }
                // Dead idle worker: release its permit and let it drop (reaped).
                inner.live -= 1;
            }

            // 2. No idle worker: spawn one if we have headroom. Reserve the
            //    permit first, then spawn WITHOUT holding the lock (fork/exec
            //    can block and must not stall other callers).
            if inner.live < self.max_workers {
                inner.live += 1;
                inner.waiters[idx] -= 1;
                #[cfg(feature = "test_features")]
                {
                    inner.high_water = inner.high_water.max(inner.live);
                }
                if inner.live < self.max_workers {
                    self.wake_one(&inner);
                }
                drop(inner);
                return match DaemonProcess::spawn(&self.binary) {
                    Ok(worker) => Ok(worker),
                    Err(e) => {
                        let mut inner = self.inner.lock();
                        inner.live -= 1;
                        self.wake_one(&inner);
                        Err(format!("failed to spawn compiler daemon: {e}"))
                    }
                };
            }

            // 3. All permits in use and none idle: wait on our priority.
            self.avail[idx].wait(&mut inner);
        }
    }

    fn wake_one(&self, inner: &PoolInner) {
        if let Some(idx) = highest_priority_waiter(&inner.waiters) {
            self.avail[idx].notify_one();
        }
    }
}

/// Index of the highest-priority class with at least one waiter.
///
/// Pure helper so the selection is unit-testable without spawning processes or
/// relying on timing.
fn highest_priority_waiter(waiters: &[usize; CompilePriority::COUNT]) -> Option<usize> {
    (0..CompilePriority::COUNT).find(|&idx| waiters[idx] > 0)
}

/// Whether a registered caller may claim currently available capacity.
fn priority_may_checkout(
    priority: CompilePriority,
    waiters: &[usize; CompilePriority::COUNT],
) -> bool {
    highest_priority_waiter(waiters) == Some(priority.index())
}

/// RAII handle for a worker checked out of the pool.
struct Lease {
    pool: &'static DaemonPool,
    worker: Option<DaemonProcess>,
}

impl Lease {
    /// Return a healthy worker to the idle set, releasing it for reuse.
    fn checkin(mut self) {
        if let Some(worker) = self.worker.take() {
            let mut inner = self.pool.inner.lock();
            inner.idle.push(worker);
            self.pool.wake_one(&mut inner);
        }
    }

    /// Drop crashed worker and free its permit.
    fn discard(mut self) {
        if let Some(worker) = self.worker.take() {
            drop(worker);
            let mut inner = self.pool.inner.lock();
            inner.live -= 1;
            self.pool.wake_one(&mut inner);
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Fail-safe reached only if neither checkin nor discard ran (e.g. a
        // panic mid compile). Drop the worker and free the permit.
        if let Some(worker) = self.worker.take() {
            drop(worker);
            let mut inner = self.pool.inner.lock();
            inner.live -= 1;
            self.pool.wake_one(&mut inner);
        }
    }
}
/// Default worker count when not configured: the smaller of two bounds, then
/// clamped to `[1, MAX_POOL_SIZE]`.
///
/// - CPU: `available_parallelism()`. Each worker compiles with the full rayon
///   pool, so more workers than cores only adds contention.
/// - Memory: `DEFAULT_TOTAL_MEMORY_BUDGET_BYTES /
///   MIN_WORKER_MEMORY_LIMIT_BYTES`. Keeping `workers × per_worker_limit`
///   within the configured RAM budget is what stops a burst of compilations
///   from tripping the kernel OOM killer and taking neard with it.
fn default_pool_size() -> usize {
    let by_cpu = available_parallelism().map_or(4, |n| n.get());
    let by_memory = (DEFAULT_TOTAL_MEMORY_BUDGET_BYTES / MIN_WORKER_MEMORY_LIMIT_BYTES) as usize;
    by_cpu.min(by_memory).clamp(1, MAX_POOL_SIZE)
}

fn get_or_init_pool() -> &'static DaemonPool {
    DAEMON_POOL.get_or_init(|| {
        let binary = DAEMON_BINARY.get().expect("daemon binary not configured").clone();
        let max_workers = DAEMON_POOL_SIZE
            .get()
            .copied()
            .unwrap_or_else(default_pool_size)
            .clamp(1, MAX_POOL_SIZE);
        DaemonPool {
            binary,
            max_workers,
            inner: Mutex::new(PoolInner {
                idle: Vec::new(),
                live: 0,
                waiters: [0; CompilePriority::COUNT],
                #[cfg(feature = "test_features")]
                high_water: 0,
            }),
            avail: from_fn(|_| Condvar::new()),
        }
    })
}

/// Compile prepared WASM code in an out-of-process daemon worker.
///
/// The inner result contains errors reported by the compiler. The outer result
/// contains failures which prevented the compiler worker from returning a
/// compilation result.
///
/// Blocks if all workers are busy, ordered by `priority`. Panics if no daemon
/// binary has been configured via `set_daemon_binary`.
pub fn compile_in_subprocess(
    prepared_code: &[u8],
    limit_config: &LimitConfig,
    priority: CompilePriority,
) -> Result<Result<Vec<u8>, CompilationError>, VMRunnerError> {
    let request = CompileRequest {
        prepared_code: prepared_code.to_vec(),
        max_memory_pages: limit_config.max_memory_pages,
        max_tables_per_contract: limit_config.max_tables_per_contract,
        max_elements_per_contract_table: limit_config
            .max_elements_per_contract_table
            .map(|v| v as u64),
    };

    let pool = get_or_init_pool();

    let mut last_err = String::new();
    for attempt in 0..MAX_SPAWN_ATTEMPTS {
        let mut lease = match pool.checkout(priority) {
            Ok(worker) => Lease { pool, worker: Some(worker) },
            Err(spawn_err) => {
                last_err = spawn_err;
                continue;
            }
        };
        match lease.worker.as_mut().unwrap().compile_raw(&request) {
            Ok(Ok(bytes)) => {
                lease.checkin();
                return Ok(Ok(bytes));
            }
            Ok(Err(msg)) => {
                // Compilation error: the worker is healthy, not retryable.
                lease.checkin();
                return Ok(Err(CompilationError::WasmtimeCompileError { msg }));
            }
            Err(ipc_err) => {
                tracing::warn!(attempt, err = %ipc_err, "compiler daemon worker failed, respawning");
                last_err = ipc_err;
                lease.discard();
            }
        }
    }
    tracing::error!(attempts = MAX_SPAWN_ATTEMPTS, "compiler daemon failed, giving up");
    Err(VMRunnerError::WasmCompilationUnknownError { debug_message: last_err })
}

/// Maximum number of worker subprocesses ever spawned concurrently.
///
/// Diagnostic helper for tests to witness that parallel compilation actually occurred.
#[cfg(feature = "test_features")]
pub fn spawned_worker_high_water() -> usize {
    get_or_init_pool().inner.lock().high_water
}

#[cfg(test)]
mod tests {
    use super::{highest_priority_waiter, priority_may_checkout};
    use crate::compile_priority::CompilePriority;

    #[test]
    fn wakes_highest_priority_class_first() {
        let critical = CompilePriority::Critical.index();
        let interactive = CompilePriority::Interactive.index();
        let background = CompilePriority::Background.index();

        // No waiters -> nobody to wake.
        assert_eq!(highest_priority_waiter(&[0, 0, 0]), None);
        // Only background waiting.
        assert_eq!(highest_priority_waiter(&[0, 0, 5]), Some(background));
        // Interactive beats background.
        assert_eq!(highest_priority_waiter(&[0, 3, 5]), Some(interactive));
        // Critical beats everything.
        assert_eq!(highest_priority_waiter(&[2, 3, 5]), Some(critical));
    }

    #[test]
    fn only_highest_priority_waiters_may_checkout() {
        let waiters = [1, 1, 1];
        assert!(priority_may_checkout(CompilePriority::Critical, &waiters));
        assert!(!priority_may_checkout(CompilePriority::Interactive, &waiters));
        assert!(!priority_may_checkout(CompilePriority::Background, &waiters));

        let waiters = [0, 1, 1];
        assert!(priority_may_checkout(CompilePriority::Interactive, &waiters));
        assert!(!priority_may_checkout(CompilePriority::Background, &waiters));
    }
}
