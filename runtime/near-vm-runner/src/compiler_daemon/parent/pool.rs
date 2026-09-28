//! Priority-aware worker pool and lease ownership.

use super::process::DaemonProcess;
use crate::compile_priority::CompilePriority;
use crate::compiler_daemon::protocol::{CompileRequest, DaemonStatus, WorkerConfig};
use crate::compiler_daemon::worker_failure::WorkerFailure;
use parking_lot::{Condvar, Mutex};
use std::array::from_fn;
use std::path::PathBuf;

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

// TODO: Use separate critical and background worker pools with fixed OS priorities.
pub(super) struct DaemonPool {
    binary: PathBuf,
    worker_config: WorkerConfig,
    max_workers: usize,
    inner: Mutex<PoolInner>,
    /// One wait queue per priority class; index by `CompilePriority::index`.
    avail: [Condvar; CompilePriority::COUNT],
}

impl DaemonPool {
    pub(super) fn new(binary: PathBuf, worker_config: WorkerConfig, max_workers: usize) -> Self {
        Self {
            binary,
            worker_config,
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
    }

    pub(super) fn lease(&'static self, priority: CompilePriority) -> Result<Lease, WorkerFailure> {
        let worker = self.checkout(priority)?;
        Ok(Lease { pool: self, worker: Some(worker) })
    }

    #[cfg(feature = "test_features")]
    pub(super) fn high_water(&self) -> usize {
        self.inner.lock().high_water
    }

    #[cfg(feature = "test_features")]
    pub(super) fn worker_counts(&self) -> (usize, usize) {
        let inner = self.inner.lock();
        (inner.live, inner.idle.len())
    }

    /// Block until a worker is available.
    fn checkout(&self, priority: CompilePriority) -> Result<DaemonProcess, WorkerFailure> {
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

            // 1. Reuse an idle worker, draining any that have died. Reap dead
            // workers without holding the pool lock because their destructors
            // join threads and wait for processes.
            let mut dead_workers = Vec::new();
            while let Some(worker) = inner.idle.pop() {
                if worker.is_alive() {
                    inner.waiters[idx] -= 1;
                    if !dead_workers.is_empty() || !inner.idle.is_empty() {
                        self.wake_one(&inner);
                    }
                    drop(inner);
                    drop(dead_workers);
                    return Ok(worker);
                }
                inner.live -= 1;
                dead_workers.push(worker);
            }
            if !dead_workers.is_empty() {
                // The current caller can consume one freed permit. Wake
                // another waiter so it can consume the remaining capacity.
                self.wake_one(&inner);
                drop(inner);
                drop(dead_workers);
                inner = self.inner.lock();
                continue;
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
                return match DaemonProcess::spawn(&self.binary, self.worker_config) {
                    Ok(worker) => Ok(worker),
                    Err(e) => {
                        let mut inner = self.inner.lock();
                        inner.live -= 1;
                        self.wake_one(&inner);
                        Err(e)
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
pub(super) struct Lease {
    pool: &'static DaemonPool,
    worker: Option<DaemonProcess>,
}

impl Lease {
    pub(super) fn status(&self) -> &DaemonStatus {
        self.worker.as_ref().expect("worker lease is empty").status()
    }

    pub(super) fn worker_id(&self) -> u32 {
        self.worker.as_ref().expect("worker lease is empty").id()
    }

    pub(super) fn compile_raw(
        &mut self,
        request: &CompileRequest<'_>,
    ) -> Result<Result<Vec<u8>, String>, WorkerFailure> {
        self.worker.as_mut().expect("worker lease is empty").compile_raw(request)
    }

    /// Return a healthy worker to the idle set, releasing it for reuse.
    pub(super) fn check_in(mut self) {
        if let Some(worker) = self.worker.take() {
            let mut inner = self.pool.inner.lock();
            inner.idle.push(worker);
            self.pool.wake_one(&mut inner);
        }
    }

    /// Drop crashed worker and free its permit.
    pub(super) fn discard(mut self) {
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
        // Fail-safe reached only if neither check_in nor discard ran (e.g. a
        // panic mid compile). Drop the worker and free the permit.
        if let Some(worker) = self.worker.take() {
            drop(worker);
            let mut inner = self.pool.inner.lock();
            inner.live -= 1;
            self.pool.wake_one(&mut inner);
        }
    }
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

        assert_eq!(highest_priority_waiter(&[0, 0, 0]), None);
        assert_eq!(highest_priority_waiter(&[0, 0, 5]), Some(background));
        assert_eq!(highest_priority_waiter(&[0, 3, 5]), Some(interactive));
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
