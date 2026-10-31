//! Parent-side client for the out-of-process compiler daemon.
//!
//! A pool of worker subprocesses serves compilations in parallel, allowing
//! independent shards to compile concurrently with independent memory limits.
//! The pool spawns workers lazily up to a configured maximum and blocks callers
//! when all workers are busy.
//!
//! Worker checkout is priority-ordered: when a worker frees up, the most urgent
//! waiting caller is served first (see [`CompilePriority`]).

mod config;
mod pool;
mod process;

use self::config::pool_settings;
use self::pool::{DaemonPool, RecoveryPermit};
use super::MAX_REQUEST_DISPLACEMENTS;
use super::protocol::{CompileRequest, DaemonStatus};
use crate::compile_priority::CompilePriority;
use crate::compiler_daemon::worker_failure::{WorkerFailure, worker_failure_kind};
use crate::logic::errors::{CompilationError, VMRunnerError};
use crate::metrics::{
    COMPILATION_PATH_TOTAL, COMPILER_DAEMON_FAILURES_TOTAL, COMPILER_DAEMON_RECOVERY_EVENTS_TOTAL,
};
pub use config::{is_daemon_configured, set_daemon_binary, set_daemon_pool_size};
use near_parameters::vm::LimitConfig;
use std::borrow::Cow;
#[cfg(feature = "test_features")]
use std::cell::Cell;
use std::sync::OnceLock;

static DAEMON_POOL: OnceLock<Result<DaemonPool, String>> = OnceLock::new();

#[cfg(feature = "test_features")]
thread_local! {
    static NEXT_TEST_ACTION: Cell<Option<super::protocol::TestAction>> = const { Cell::new(None) };
}

/// Set test-only behavior for the next compiler request made by the current
/// thread.
#[cfg(feature = "test_features")]
pub fn set_test_action_for_next_request(action: super::protocol::TestAction) {
    NEXT_TEST_ACTION.with(|next_action| {
        assert!(
            next_action.replace(Some(action)).is_none(),
            "a compiler daemon test action is already pending"
        );
    });
}

/// Override daemon memory settings before the singleton pool is initialized.
#[cfg(feature = "test_features")]
pub fn set_test_memory_config(
    initial_worker_limit_bytes: u64,
    max_worker_limit_bytes: u64,
    total_budget_bytes: u64,
) {
    assert!(DAEMON_POOL.get().is_none(), "compiler daemon pool is already initialized");
    config::set_test_memory_config(
        initial_worker_limit_bytes,
        max_worker_limit_bytes,
        total_budget_bytes,
    );
}

fn get_or_init_pool() -> Result<&'static DaemonPool, String> {
    DAEMON_POOL
        .get_or_init(|| {
            let settings = pool_settings()?;
            Ok(DaemonPool::new(
                settings.binary,
                settings.worker_config,
                settings.max_workers,
                settings.max_worker_limit_bytes,
                settings.total_budget_bytes,
            ))
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// Eagerly start and validate one worker, leaving it idle in the pool.
///
/// This verifies IPC compatibility, compiler settings, effective address-space
/// enforcement, and process isolation before the node starts serving requests.
pub fn start_daemon() -> Result<DaemonStatus, String> {
    let pool = get_or_init_pool()?;
    let lease = pool.lease(CompilePriority::Critical).map_err(|err| err.to_string())?;
    let status = lease.status().clone();
    lease.check_in();
    Ok(status)
}

/// Compile prepared WASM code in an out-of-process daemon worker.
///
/// The inner result contains errors reported by the compiler. The outer result
/// contains failures which prevented the compiler worker from returning a
/// compilation result.
///
/// Blocks if all workers are busy and serves waiting callers by priority.
/// Panics if no daemon binary has been configured via `set_daemon_binary`.
pub fn compile_in_subprocess(
    prepared_code: &[u8],
    limit_config: &LimitConfig,
    priority: CompilePriority,
) -> Result<Result<Vec<u8>, CompilationError>, VMRunnerError> {
    let request = CompileRequest {
        prepared_code: Cow::Borrowed(prepared_code),
        max_memory_pages: limit_config.max_memory_pages,
        #[cfg(feature = "test_features")]
        test_action: NEXT_TEST_ACTION.with(Cell::take),
    };

    let pool = get_or_init_pool()
        .map_err(|debug_message| VMRunnerError::WasmCompilationUnknownError { debug_message })?;

    let mut memory_limit_bytes = pool.initial_memory_limit();
    let mut generic_retry_available = true;
    let mut displacements = 0;
    let mut attempts = 0;
    let mut recovery: Option<RecoveryPermit> = None;
    let last_err = loop {
        attempts += 1;
        COMPILATION_PATH_TOTAL.with_label_values(&["daemon"]).inc();
        let lease = match &recovery {
            Some(permit) => permit.lease(memory_limit_bytes),
            None => pool.lease(priority),
        };
        let failure = match lease {
            Ok(mut lease) => {
                let worker_id = lease.worker_id();
                match lease.compile_raw(&request) {
                    Ok(Ok(bytes)) => {
                        lease.check_in();
                        return Ok(Ok(bytes));
                    }
                    Ok(Err(msg)) => {
                        // Compilation error: the worker is healthy, not retryable.
                        lease.check_in();
                        return Ok(Err(CompilationError::WasmtimeCompileError { msg }));
                    }
                    Err(failure) => {
                        tracing::warn!(
                            target: "vm",
                            attempt = attempts,
                            worker_id,
                            memory_limit_bytes,
                            cause = worker_failure_kind(&failure),
                            err = %failure,
                            "compiler daemon worker failed"
                        );
                        lease.discard();
                        failure
                    }
                }
            }
            Err(failure) => {
                tracing::warn!(
                    target: "vm",
                    attempt = attempts,
                    memory_limit_bytes,
                    cause = worker_failure_kind(&failure),
                    err = %failure,
                    "failed to spawn compiler daemon worker"
                );
                failure
            }
        };

        let failure_message = failure.to_string();
        COMPILER_DAEMON_FAILURES_TOTAL.with_label_values(&[worker_failure_kind(&failure)]).inc();
        if matches!(&failure, WorkerFailure::LocalMemoryExhaustion) {
            let Some(next_limit) = pool.next_memory_limit(memory_limit_bytes) else {
                break failure_message;
            };
            if recovery.is_none() {
                recovery = Some(pool.begin_recovery(priority));
            }
            COMPILER_DAEMON_RECOVERY_EVENTS_TOTAL.with_label_values(&["memory_escalation"]).inc();
            tracing::info!(
                target: "vm",
                attempt = attempts,
                previous_memory_limit_bytes = memory_limit_bytes,
                memory_limit_bytes = next_limit,
                "escalating compiler daemon memory limit after local exhaustion"
            );
            memory_limit_bytes = next_limit;
            continue;
        }

        if matches!(&failure, WorkerFailure::Evicted) {
            if displacements >= MAX_REQUEST_DISPLACEMENTS {
                break failure_message;
            }
            displacements += 1;
            COMPILER_DAEMON_RECOVERY_EVENTS_TOTAL.with_label_values(&["displacement_retry"]).inc();
            tracing::info!(
                target: "vm",
                attempt = attempts,
                displacements,
                memory_limit_bytes,
                "requeueing compiler daemon request after scheduler eviction"
            );
            continue;
        }

        if generic_retry_available {
            generic_retry_available = false;
            COMPILER_DAEMON_RECOVERY_EVENTS_TOTAL.with_label_values(&["generic_retry"]).inc();
            continue;
        }
        break failure_message;
    };
    tracing::error!(
        target: "vm",
        attempts,
        displacements,
        memory_limit_bytes,
        err = %last_err,
        "compiler daemon failed, giving up"
    );
    Err(VMRunnerError::WasmCompilationUnknownError { debug_message: last_err })
}

/// Maximum number of worker subprocesses ever spawned concurrently.
///
/// Diagnostic helper for tests to witness that parallel compilation actually occurred.
#[cfg(feature = "test_features")]
pub fn spawned_worker_high_water() -> usize {
    get_or_init_pool().expect("invalid compiler daemon configuration").high_water()
}

/// Current worker counts for tests checking that all pool leases were returned.
#[cfg(feature = "test_features")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerPoolState {
    pub live: usize,
    pub idle: usize,
    pub reserved_bytes: u64,
    pub terminating: usize,
    pub scheduler_evictions: usize,
}

#[cfg(feature = "test_features")]
pub fn worker_pool_state() -> WorkerPoolState {
    let (live, idle, reserved_bytes, terminating, scheduler_evictions) =
        get_or_init_pool().expect("invalid compiler daemon configuration").worker_counts();
    WorkerPoolState { live, idle, reserved_bytes, terminating, scheduler_evictions }
}
