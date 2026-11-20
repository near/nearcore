//! Out-of-process WASM compiler daemon.
//!
//! Isolates Wasmtime compilation into subprocesses to protect the main
//! neard process from compiler crashes or bugs and to enforce memory limits.
//!
//! [`parent`] manages a priority-aware pool of worker processes.
//!
//! [`child`] runs Wasmtime compilation with minimal system access and raises
//! its `oom_score_adj` so the kernel OOM killer reaps it before neard.

mod allocator;
mod child;
mod parent;
pub mod protocol;
mod sandbox;
mod watchdog;
mod worker_failure;

pub use allocator::{ExitOnWorkerMemoryExhaustion, WORKER_MEMORY_EXHAUSTED_EXIT_CODE};
pub use child::daemon_main;
#[cfg(feature = "test_features")]
pub use parent::{
    WorkerPoolState, set_test_action_for_next_request, set_test_memory_config,
    spawned_worker_high_water, worker_pool_state,
};
pub use parent::{
    compile_in_subprocess, is_daemon_configured, set_daemon_binary, set_daemon_pool_size,
    start_daemon,
};
use std::time::Duration;

// TODO(jakmeier): make the worker resource and pool limits configurable.
/// Initial per-worker virtual address-space limit.
///
/// The parent passes this value explicitly and the child applies it as both
/// the soft and hard `RLIMIT_AS`. A worker's limit is immutable, a future
/// larger retry must use a fresh process.
///
/// With 6 threads, most contracts compile using less than 450MB virtual memory
/// and less than 35MB physical memory. Known valid cases use 600 MB virtual and
/// 170 MB physical memory at the extreme. Virtual address space is not RSS,
/// and this limit does not cover allocations in neard or other processes.
const INITIAL_WORKER_MEMORY_LIMIT_BYTES: u64 = bytesize::GIB;

/// Largest virtual address-space limit used for a single worker retry.
///
/// Confirmed worker-local exhaustion grows through geometric tiers up to this
/// limit. The aggregate reservation still remains bounded independently by
/// [`DEFAULT_TOTAL_MEMORY_BUDGET_BYTES`].
const MAX_WORKER_MEMORY_LIMIT_BYTES: u64 = 16 * bytesize::GIB;

/// Default number of compilation threads per worker subprocess.
///
/// Setting this higher results in higher virtual memory usage, reaching the
/// `RLIMIT_AS` faster. Experimental results on mainnet contracts show
/// diminishing returns for compilation time around 6 threads.
const DEFAULT_THREADS_PER_WORKER: u32 = 6;

/// Default stack size for compiler threads, matching neard's global rayon pool.
const DEFAULT_THREAD_STACK_SIZE_BYTES: u64 = 8 * 1024 * 1024;

/// Hard cap on worker subprocesses regardless of the configured/derived size.
///
/// Each worker has a rayon pool utilizing multiple threads, so a handful of
/// overlapping large compilations already saturate the CPU. More workers only
/// add memory pressure and oversubscription.
const MAX_POOL_SIZE: usize = 8;

/// Total virtual memory budget set aside for compiler-daemon workers, in bytes.
///
/// Not a limit in itself: the default pool size caps the worker count so that
/// `workers x INITIAL_WORKER_MEMORY_LIMIT_BYTES` stays within this budget.
///
/// This is a conservative admission budget for configured worker virtual
/// address-space limits. It is not a physical-memory cap for the node.
const DEFAULT_TOTAL_MEMORY_BUDGET_BYTES: u64 = 16 * bytesize::GIB;

/// Maximum time allowed for a worker to report that it is ready.
const DAEMON_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// A scheduler-displaced request may requeue at the same memory tier this many times.
const MAX_REQUEST_DISPLACEMENTS: u32 = 2;
