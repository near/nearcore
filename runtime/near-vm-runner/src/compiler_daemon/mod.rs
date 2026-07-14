//! Out-of-process WASM compiler daemon.
//!
//! Isolates Wasmtime compilation into subprocesses to protect the main
//! neard process from compiler crashes or bugs and to enforce memory limits.
//!
//! [`parent`] manages a priority-aware pool of worker processes.
//!
//! [`child`] runs Wasmtime compilation with minimal system access and raises
//! its `oom_score_adj` so the kernel OOM killer reaps it before neard.

mod child;
mod parent;
pub mod protocol;
mod sandbox;

pub use child::daemon_main;
pub use parent::{
    compile_in_subprocess, is_daemon_configured, set_daemon_binary, set_daemon_pool_size,
};

#[cfg(feature = "test_features")]
pub use parent::spawned_worker_high_water;

/// Minimum per-worker virtual memory budget.
///
/// Applied as `RLIMIT_AS` in the daemon child (virtual memory, not physical).
/// Lives here so the parent and child share a single source of truth and the
/// value can never drift between them.
///
/// If compilation fails for any reason, it will be retried once.
/// (TODO: increase the memory budget on retry)
///
/// With 6 threads, most contracts compile using less than 450MB virutal memory
/// and less than 35MB physical memory. Known valid cases use 600 MB virtual and
/// 170 MB physical memory at the extreme. Using more than 1GiB virutal memory
/// might be possible but almost certainly would be a maliciously crafted Wasm.
const MIN_WORKER_MEMORY_LIMIT_BYTES: u64 = 1 * bytesize::GIB;

/// Default number of rayon compilation threads per worker subprocess.
///
/// Setting this higher results in higher virtual memory usage, reaching the
/// `RLIMIT_AS` faster. Experimental results on mainnet contracts show
/// diminishing returns for compilation time around 6 threads.
const DEFAULT_RAYON_THREADS_PER_WORKER: usize = 6;

/// Hard cap on worker subprocesses regardless of the configured/derived size.
///
/// Each worker has a rayon pool utilizing multiple threads, so a handful of
/// overlapping large compilations already saturate the CPU. More workers only
/// add memory pressure and oversubscription.
const MAX_POOL_SIZE: usize = 8;

/// Total virtual memory budget set aside for compiler-daemon workers, in bytes.
///
/// Not a limit in itself: the default pool size caps the worker count so that
/// `workers x MIN_WORKER_MEMORY_LIMIT_BYTES` stays within this budget.
///
/// Experiments show that wasmtime usually allocates at least 4 time more
/// virtual memory than gets mapped to physical memory. Hence, a 16GiB limit
/// usually results in less than 4GiB physical memory allocation.
const DEFAULT_TOTAL_MEMORY_BUDGET_BYTES: u64 = 16 * bytesize::GIB;

/// Per-request retry budget on IPC failure (worker crash).
const MAX_SPAWN_ATTEMPTS: u32 = 2;
