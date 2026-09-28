//! Internal compiler-daemon resource and process configuration.

use crate::compiler_daemon::protocol::WorkerConfig;
use crate::compiler_daemon::{
    DEFAULT_THREAD_STACK_SIZE_BYTES, DEFAULT_THREADS_PER_WORKER, DEFAULT_TOTAL_MEMORY_BUDGET_BYTES,
    INITIAL_WORKER_MEMORY_LIMIT_BYTES, MAX_POOL_SIZE,
};
#[cfg(unix)]
use libc::{RLIM_INFINITY, rlim_t};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::thread::available_parallelism;

static DAEMON_BINARY: OnceLock<PathBuf> = OnceLock::new();
static DAEMON_POOL_SIZE: OnceLock<usize> = OnceLock::new();
static DAEMON_MEMORY_CONFIG: OnceLock<MemoryConfig> = OnceLock::new();

/// Set the path to the binary that should be spawned as the compiler daemon.
///
/// Only works once, subsequent calls are ignored.
pub fn set_daemon_binary(path: PathBuf) {
    if DAEMON_BINARY.set(path).is_err() {
        tracing::error!(target: "vm", "set_daemon_binary called more than once, ignoring");
    }
}

/// Configure the maximum number of compiler-daemon worker subprocesses.
/// Must be called before the first compilation; later calls are ignored.
/// If never called, defaults to the smaller of CPU parallelism and
/// `DEFAULT_TOTAL_MEMORY_BUDGET_BYTES` divided by the per-worker memory budget,
/// clamped to `[1, MAX_POOL_SIZE]`.
pub fn set_daemon_pool_size(size: usize) {
    if DAEMON_POOL_SIZE.set(size).is_err() {
        tracing::warn!(target: "vm", "set_daemon_pool_size called more than once, ignoring");
    }
}

/// Returns true if a daemon binary has been configured via `set_daemon_binary`.
pub fn is_daemon_configured() -> bool {
    DAEMON_BINARY.get().is_some()
}

#[derive(Clone, Copy, Debug)]
struct MemoryConfig {
    worker_limit_bytes: u64,
    total_budget_bytes: u64,
}

fn memory_config() -> MemoryConfig {
    DAEMON_MEMORY_CONFIG.get().copied().unwrap_or(MemoryConfig {
        worker_limit_bytes: INITIAL_WORKER_MEMORY_LIMIT_BYTES,
        total_budget_bytes: DEFAULT_TOTAL_MEMORY_BUDGET_BYTES,
    })
}

/// Override daemon memory settings before the singleton pool is initialized.
#[cfg(feature = "test_features")]
pub(super) fn set_test_memory_config(worker_limit_bytes: u64, total_budget_bytes: u64) {
    DAEMON_MEMORY_CONFIG
        .set(MemoryConfig { worker_limit_bytes, total_budget_bytes })
        .expect("compiler daemon memory configuration is already set");
}

fn default_worker_config() -> WorkerConfig {
    WorkerConfig {
        threads: DEFAULT_THREADS_PER_WORKER,
        thread_stack_size_bytes: DEFAULT_THREAD_STACK_SIZE_BYTES,
        memory_limit_bytes: memory_config().worker_limit_bytes,
    }
}

/// Default worker count when not configured: the smaller of the CPU and
/// virtual-address-space budget bounds, clamped to `[1, MAX_POOL_SIZE]`.
fn default_pool_size(memory: MemoryConfig) -> usize {
    let by_cpu = available_parallelism().map_or(4, |n| n.get());
    let by_memory = usize::try_from(memory.total_budget_bytes / memory.worker_limit_bytes)
        .unwrap_or(usize::MAX);
    by_cpu.min(by_memory).clamp(1, MAX_POOL_SIZE)
}

fn validate_resource_config(
    worker_config: WorkerConfig,
    total_budget_bytes: u64,
) -> Result<(), String> {
    if worker_config.memory_limit_bytes == 0 {
        return Err("compiler daemon worker memory limit must be greater than zero".to_owned());
    }
    if total_budget_bytes == 0 {
        return Err("compiler daemon total memory budget must be greater than zero".to_owned());
    }
    usize::try_from(worker_config.memory_limit_bytes)
        .map_err(|_| "compiler daemon worker memory limit exceeds the platform address space")?;
    #[cfg(unix)]
    {
        let limit = rlim_t::try_from(worker_config.memory_limit_bytes)
            .map_err(|_| "compiler daemon worker memory limit is not representable by rlim_t")?;
        if limit == RLIM_INFINITY {
            return Err("compiler daemon worker memory limit must be finite".to_owned());
        }
    }
    if worker_config.memory_limit_bytes > total_budget_bytes {
        return Err(format!(
            "compiler daemon worker requires {} bytes, exceeding the {total_budget_bytes} byte total memory budget",
            worker_config.memory_limit_bytes
        ));
    }
    Ok(())
}

pub(super) struct PoolSettings {
    pub(super) binary: PathBuf,
    pub(super) worker_config: WorkerConfig,
    pub(super) max_workers: usize,
    pub(super) total_budget_bytes: u64,
}

pub(super) fn pool_settings() -> Result<PoolSettings, String> {
    let binary = DAEMON_BINARY.get().expect("daemon binary not configured").clone();
    let memory = memory_config();
    if memory.worker_limit_bytes == 0 {
        return Err("compiler daemon worker memory limit must be greater than zero".to_owned());
    }
    let max_workers = DAEMON_POOL_SIZE
        .get()
        .copied()
        .unwrap_or_else(|| default_pool_size(memory))
        .clamp(1, MAX_POOL_SIZE);
    let worker_config = default_worker_config();
    validate_resource_config(worker_config, memory.total_budget_bytes)?;
    Ok(PoolSettings {
        binary,
        worker_config,
        max_workers,
        total_budget_bytes: memory.total_budget_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::validate_resource_config;
    use crate::compiler_daemon::protocol::WorkerConfig;

    #[test]
    fn validates_worker_memory_budget() {
        let config =
            WorkerConfig { threads: 1, thread_stack_size_bytes: 1024, memory_limit_bytes: 1024 };
        assert!(validate_resource_config(config, 4096).is_ok());
        assert!(validate_resource_config(config, 1023).is_err());
        assert!(
            validate_resource_config(WorkerConfig { memory_limit_bytes: 0, ..config }, 4096)
                .is_err()
        );
        #[cfg(unix)]
        assert!(
            validate_resource_config(
                WorkerConfig { memory_limit_bytes: u64::MAX, ..config },
                u64::MAX
            )
            .is_err()
        );
    }
}
