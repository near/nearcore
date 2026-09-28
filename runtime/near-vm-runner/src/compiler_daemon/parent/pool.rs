//! Priority-aware worker coordinator and lease ownership.
//!
//! Lock ordering is deliberately one-way: the coordinator lock may be used to
//! inspect or clone a process control handle, but process control, pipe I/O,
//! spawning, watchdog shutdown, and process reaping always happen after the
//! coordinator lock has been released. Leases exclusively own pipe I/O while
//! the registry retains the independently lockable process control handle.

use super::process::DaemonProcess;
use crate::compile_priority::CompilePriority;
use crate::compiler_daemon::protocol::{CompileRequest, DaemonStatus, WorkerConfig};
use crate::compiler_daemon::watchdog::{ProcessControl, TerminationReason};
use crate::compiler_daemon::worker_failure::WorkerFailure;
use parking_lot::{Condvar, Mutex};
use std::array::from_fn;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::Builder;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WorkerId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerState {
    Starting,
    Idle,
    Leased { priority: CompilePriority },
    Terminating,
}

struct RegistryEntry {
    memory_limit_bytes: u64,
    state: WorkerState,
    control: Option<Arc<ProcessControl>>,
}

struct RegisteredWorker {
    id: WorkerId,
    process: DaemonProcess,
}

struct PoolInner {
    /// Workers that are spawned and currently idle, ready to be checked out.
    idle: Vec<RegisteredWorker>,
    /// All reserved workers, including workers which are starting or terminating.
    registry: HashMap<WorkerId, RegistryEntry>,
    reserved_bytes: u64,
    next_worker_id: u64,
    /// Number of callers blocked waiting for a worker, per priority class.
    waiters: [usize; CompilePriority::COUNT],
    /// Maximum number of simultaneous reservations. Diagnostic witness that
    /// parallelism occurred (used by tests).
    #[cfg(feature = "test_features")]
    high_water: usize,
}

pub(super) struct DaemonPool {
    binary: PathBuf,
    worker_config: WorkerConfig,
    max_workers: usize,
    total_budget_bytes: u64,
    inner: Mutex<PoolInner>,
    /// One wait queue per priority class; index by `CompilePriority::index`.
    avail: [Condvar; CompilePriority::COUNT],
}

impl DaemonPool {
    pub(super) fn new(
        binary: PathBuf,
        worker_config: WorkerConfig,
        max_workers: usize,
        total_budget_bytes: u64,
    ) -> Self {
        Self {
            binary,
            worker_config,
            max_workers,
            total_budget_bytes,
            inner: Mutex::new(PoolInner {
                idle: Vec::new(),
                registry: HashMap::new(),
                reserved_bytes: 0,
                next_worker_id: 0,
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
    pub(super) fn worker_counts(&self) -> (usize, usize, u64, usize) {
        let inner = self.inner.lock();
        let terminating =
            inner.registry.values().filter(|entry| entry.state == WorkerState::Terminating).count();
        (inner.registry.len(), inner.idle.len(), inner.reserved_bytes, terminating)
    }

    /// Block until a worker is available.
    fn checkout(
        &'static self,
        priority: CompilePriority,
    ) -> Result<RegisteredWorker, WorkerFailure> {
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

            // Reuse an idle worker, draining any that have died. Teardown and
            // reaping happen without the coordinator lock.
            let mut dead_workers = Vec::new();
            while let Some(worker) = inner.idle.pop() {
                if worker.process.is_alive() {
                    let entry = inner
                        .registry
                        .get_mut(&worker.id)
                        .expect("idle worker is missing from registry");
                    assert_eq!(entry.state, WorkerState::Idle);
                    entry.state = WorkerState::Leased { priority };
                    inner.waiters[idx] -= 1;
                    if !dead_workers.is_empty() || !inner.idle.is_empty() {
                        self.wake_one(&inner);
                    }
                    drop(inner);
                    for worker in dead_workers {
                        self.retire(worker, TerminationReason::ProcessDrop);
                    }
                    return Ok(worker);
                }
                Self::mark_terminating(&mut inner, worker.id);
                dead_workers.push(worker);
            }
            if !dead_workers.is_empty() {
                drop(inner);
                for worker in dead_workers {
                    self.retire(worker, TerminationReason::ProcessDrop);
                }
                inner = self.inner.lock();
                continue;
            }

            // Reserve count and bytes atomically before spawning. Starting and
            // terminating workers retain both reservations.
            if can_reserve(
                &inner,
                self.max_workers,
                self.total_budget_bytes,
                self.worker_config.memory_limit_bytes,
            ) {
                let id = reserve_worker(&mut inner, self.worker_config.memory_limit_bytes);
                inner.waiters[idx] -= 1;
                #[cfg(feature = "test_features")]
                {
                    inner.high_water = inner.high_water.max(inner.registry.len());
                }
                if can_reserve(
                    &inner,
                    self.max_workers,
                    self.total_budget_bytes,
                    self.worker_config.memory_limit_bytes,
                ) {
                    self.wake_one(&inner);
                }
                drop(inner);

                let process = match DaemonProcess::spawn(&self.binary, self.worker_config) {
                    Ok(process) => process,
                    Err(spawn_failure) => {
                        let mut inner = self.inner.lock();
                        if let Some(control) = spawn_failure.control {
                            let entry = inner
                                .registry
                                .get_mut(&id)
                                .expect("worker reservation disappeared");
                            entry.state = WorkerState::Terminating;
                            entry.control = Some(Arc::clone(&control));
                            drop(inner);
                            self.wait_for_reap(id, control);
                        } else {
                            release_reservation(&mut inner, id);
                            self.wake_one(&inner);
                        }
                        return Err(spawn_failure.failure);
                    }
                };
                let control = process.control();
                let mut inner = self.inner.lock();
                let entry = inner.registry.get_mut(&id).expect("worker reservation disappeared");
                entry.control = Some(control);
                if entry.state == WorkerState::Terminating {
                    drop(inner);
                    self.retire(
                        RegisteredWorker { id, process },
                        TerminationReason::SchedulerEviction,
                    );
                    return Err(WorkerFailure::Evicted);
                }
                assert_eq!(entry.state, WorkerState::Starting);
                entry.state = WorkerState::Leased { priority };
                return Ok(RegisteredWorker { id, process });
            }

            // All count or byte capacity is reserved. Terminating workers are
            // deliberately included until their process has been reaped.
            self.avail[idx].wait(&mut inner);
        }
    }

    fn wake_one(&self, inner: &PoolInner) {
        if let Some(idx) = highest_priority_waiter(&inner.waiters) {
            self.avail[idx].notify_one();
        }
    }

    fn mark_terminating(inner: &mut PoolInner, id: WorkerId) {
        let entry = inner.registry.get_mut(&id).expect("worker is missing from registry");
        entry.state = WorkerState::Terminating;
    }

    /// Mark a worker before asking its process to stop.
    ///
    /// An active lease keeps owning the pipes and will perform final teardown.
    /// An idle worker is removed and returned to the caller for teardown. This
    /// ordering prevents either kind of worker from being checked out or in again.
    fn request_termination(
        &self,
        id: WorkerId,
        reason: TerminationReason,
    ) -> Option<RegisteredWorker> {
        let (control, idle_worker) = {
            let mut inner = self.inner.lock();
            Self::mark_terminating(&mut inner, id);
            let idle_worker = inner
                .idle
                .iter()
                .position(|worker| worker.id == id)
                .map(|index| inner.idle.swap_remove(index));
            let control = inner.registry.get(&id).and_then(|entry| entry.control.clone());
            (control, idle_worker)
        };
        if let Some(control) = control {
            let _ = control.terminate(reason);
        }
        idle_worker
    }

    /// Terminate and reap a worker without holding the coordinator lock.
    /// Capacity is released only after a confirmed reap.
    fn retire(&'static self, worker: RegisteredWorker, reason: TerminationReason) {
        let id = worker.id;
        let control = worker.process.control();
        let _ = control.terminate(reason);
        drop(worker);

        self.wait_for_reap(id, control);
    }

    fn wait_for_reap(&'static self, id: WorkerId, control: Arc<ProcessControl>) {
        if control.try_status().ok().flatten().is_some() {
            self.finish_termination(id);
            return;
        }

        let reaper_control = Arc::clone(&control);
        let spawn_result = Builder::new()
            .name("compiler-daemon-reservation-reaper".to_owned())
            .spawn(move || match reaper_control.reap() {
                Ok(_) => self.finish_termination(id),
                Err(err) => tracing::warn!(
                    target: "vm",
                    worker_id = id.0,
                    %err,
                    "failed to reap compiler daemon; retaining its reservation"
                ),
            });
        if spawn_result.is_err() {
            // Thread creation failure must not leak the process. Blocking here
            // is safe because no coordinator lock is held. Keep the reservation
            // if the process still cannot be confirmed reaped.
            match control.reap() {
                Ok(_) => self.finish_termination(id),
                Err(err) => tracing::warn!(
                    target: "vm",
                    worker_id = id.0,
                    %err,
                    "failed to reap compiler daemon; retaining its reservation"
                ),
            }
        }
    }

    fn finish_termination(&self, id: WorkerId) {
        let mut inner = self.inner.lock();
        let entry = inner.registry.get(&id).expect("reaped worker is missing from registry");
        assert_eq!(entry.state, WorkerState::Terminating);
        release_reservation(&mut inner, id);
        self.wake_one(&inner);
    }

    fn check_in(&'static self, worker: RegisteredWorker) {
        let mut inner = self.inner.lock();
        let entry = inner.registry.get_mut(&worker.id).expect("worker is missing from registry");
        match entry.state {
            WorkerState::Leased { .. } => {
                entry.state = WorkerState::Idle;
                inner.idle.push(worker);
                self.wake_one(&inner);
            }
            WorkerState::Terminating => {
                drop(inner);
                self.retire(worker, TerminationReason::SchedulerEviction);
            }
            WorkerState::Starting | WorkerState::Idle => {
                panic!("invalid worker state while checking in lease")
            }
        }
    }

    fn discard(&'static self, worker: RegisteredWorker) {
        let id = worker.id;
        assert!(
            self.request_termination(id, TerminationReason::ProcessDrop).is_none(),
            "leased worker unexpectedly appeared in the idle set"
        );
        self.retire(worker, TerminationReason::ProcessDrop);
    }
}

fn can_reserve(
    inner: &PoolInner,
    max_workers: usize,
    total_budget_bytes: u64,
    memory_limit_bytes: u64,
) -> bool {
    inner.registry.len() < max_workers
        && inner
            .reserved_bytes
            .checked_add(memory_limit_bytes)
            .is_some_and(|bytes| bytes <= total_budget_bytes)
}

fn reserve_worker(inner: &mut PoolInner, memory_limit_bytes: u64) -> WorkerId {
    let id = WorkerId(inner.next_worker_id);
    inner.next_worker_id =
        inner.next_worker_id.checked_add(1).expect("worker generation overflowed");
    inner.reserved_bytes = inner
        .reserved_bytes
        .checked_add(memory_limit_bytes)
        .expect("worker memory reservation overflowed");
    assert!(
        inner
            .registry
            .insert(
                id,
                RegistryEntry { memory_limit_bytes, state: WorkerState::Starting, control: None },
            )
            .is_none(),
        "worker generation was reused"
    );
    id
}

fn release_reservation(inner: &mut PoolInner, id: WorkerId) {
    let entry = inner.registry.remove(&id).expect("worker reservation is missing");
    inner.reserved_bytes = inner
        .reserved_bytes
        .checked_sub(entry.memory_limit_bytes)
        .expect("worker memory reservation underflowed");
}

/// Index of the highest-priority class with at least one waiter.
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
    worker: Option<RegisteredWorker>,
}

impl Lease {
    pub(super) fn status(&self) -> &DaemonStatus {
        self.worker.as_ref().expect("worker lease is empty").process.status()
    }

    pub(super) fn worker_id(&self) -> u64 {
        self.worker.as_ref().expect("worker lease is empty").id.0
    }

    pub(super) fn compile_raw(
        &mut self,
        request: &CompileRequest<'_>,
    ) -> Result<Result<Vec<u8>, String>, WorkerFailure> {
        self.worker.as_mut().expect("worker lease is empty").process.compile_raw(request)
    }

    /// Return a healthy worker to the idle set, releasing it for reuse.
    pub(super) fn check_in(mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool.check_in(worker);
        }
    }

    /// Drop a failed worker and release its reservations after it is reaped.
    pub(super) fn discard(mut self) {
        if let Some(worker) = self.worker.take() {
            self.pool.discard(worker);
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Fail-safe reached only if neither check_in nor discard ran (e.g. a
        // panic mid compile).
        if let Some(worker) = self.worker.take() {
            self.pool.discard(worker);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PoolInner, WorkerState, can_reserve, highest_priority_waiter, priority_may_checkout,
        release_reservation, reserve_worker,
    };
    use crate::compile_priority::CompilePriority;
    use std::collections::HashMap;

    fn empty_inner() -> PoolInner {
        PoolInner {
            idle: Vec::new(),
            registry: HashMap::new(),
            reserved_bytes: 0,
            next_worker_id: 0,
            waiters: [0; CompilePriority::COUNT],
            #[cfg(feature = "test_features")]
            high_water: 0,
        }
    }

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

    #[test]
    fn count_and_byte_reservations_are_atomic() {
        let mut inner = empty_inner();
        assert!(can_reserve(&inner, 2, 30, 10));
        let first = reserve_worker(&mut inner, 10);
        let second = reserve_worker(&mut inner, 20);
        assert_eq!(inner.registry.len(), 2);
        assert_eq!(inner.reserved_bytes, 30);
        assert!(!can_reserve(&inner, 3, 30, 1));
        assert!(!can_reserve(&inner, 2, 100, 1));

        inner.registry.get_mut(&first).unwrap().state = WorkerState::Terminating;
        assert!(!can_reserve(&inner, 3, 30, 10));
        release_reservation(&mut inner, first);
        assert_eq!(inner.reserved_bytes, 20);
        assert!(can_reserve(&inner, 3, 30, 10));
        release_reservation(&mut inner, second);
        assert_eq!(inner.reserved_bytes, 0);
    }

    #[test]
    fn worker_generations_are_not_reused() {
        let mut inner = empty_inner();
        let first = reserve_worker(&mut inner, 1);
        release_reservation(&mut inner, first);
        let second = reserve_worker(&mut inner, 1);
        assert_ne!(first, second);
    }
}
