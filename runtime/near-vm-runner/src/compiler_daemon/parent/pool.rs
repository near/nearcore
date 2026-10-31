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
use crate::metrics::{
    COMPILER_DAEMON_RECOVERY_EVENTS_TOTAL, COMPILER_DAEMON_RECOVERY_WAIT_SECONDS,
    COMPILER_DAEMON_RESERVED_MEMORY_BYTES, COMPILER_DAEMON_WORKERS,
};
use parking_lot::{Condvar, Mutex};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct WorkerId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerState {
    Starting,
    Idle,
    Leased { priority: CompilePriority, protected: bool },
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
    /// Escalated requests are serialized. They wait without owning a worker.
    recovery_waiters: [usize; CompilePriority::COUNT],
    recovery_owner: bool,
    recovery_request: Option<(CompilePriority, u64)>,
    /// Maximum number of simultaneous reservations. Diagnostic witness that
    /// parallelism occurred (used by tests).
    #[cfg(feature = "test_features")]
    high_water: usize,
    #[cfg(feature = "test_features")]
    scheduler_evictions: usize,
}

pub(super) struct DaemonPool {
    binary: PathBuf,
    worker_config: WorkerConfig,
    max_workers: usize,
    max_worker_limit_bytes: u64,
    total_budget_bytes: u64,
    inner: Mutex<PoolInner>,
    /// All checkout predicates are rechecked after a capacity/state change.
    capacity_changed: Condvar,
    /// Only callers waiting to become the recovery owner wait here.
    recovery_admission: Condvar,
}

impl DaemonPool {
    pub(super) fn new(
        binary: PathBuf,
        worker_config: WorkerConfig,
        max_workers: usize,
        max_worker_limit_bytes: u64,
        total_budget_bytes: u64,
    ) -> Self {
        let inner = PoolInner {
            idle: Vec::new(),
            registry: HashMap::new(),
            reserved_bytes: 0,
            next_worker_id: 0,
            waiters: [0; CompilePriority::COUNT],
            recovery_waiters: [0; CompilePriority::COUNT],
            recovery_owner: false,
            recovery_request: None,
            #[cfg(feature = "test_features")]
            high_water: 0,
            #[cfg(feature = "test_features")]
            scheduler_evictions: 0,
        };
        observe_pool_resources(&inner);
        Self {
            binary,
            worker_config,
            max_workers,
            max_worker_limit_bytes,
            total_budget_bytes,
            inner: Mutex::new(inner),
            capacity_changed: Condvar::new(),
            recovery_admission: Condvar::new(),
        }
    }

    pub(super) fn lease(&'static self, priority: CompilePriority) -> Result<Lease, WorkerFailure> {
        let worker = self.checkout(priority, self.worker_config.memory_limit_bytes, false)?;
        Ok(Lease { pool: self, worker: Some(worker), retire_on_drop: false })
    }

    pub(super) fn initial_memory_limit(&self) -> u64 {
        self.worker_config.memory_limit_bytes
    }

    pub(super) fn next_memory_limit(&self, current: u64) -> Option<u64> {
        if current >= self.max_worker_limit_bytes {
            return None;
        }
        Some(current.saturating_mul(2).min(self.max_worker_limit_bytes))
    }

    pub(super) fn begin_recovery(&'static self, priority: CompilePriority) -> RecoveryPermit {
        let idx = priority.index();
        let started = Instant::now();
        let mut inner = self.inner.lock();
        inner.recovery_waiters[idx] += 1;
        while inner.recovery_owner || highest_priority_waiter(&inner.recovery_waiters) != Some(idx)
        {
            self.recovery_admission.wait(&mut inner);
        }
        inner.recovery_waiters[idx] -= 1;
        inner.recovery_owner = true;
        let recovery_wait = started.elapsed();
        COMPILER_DAEMON_RECOVERY_WAIT_SECONDS
            .with_label_values::<&str>(&[])
            .observe(recovery_wait.as_secs_f64());
        tracing::info!(
            target: "vm",
            priority = ?priority,
            recovery_wait_ms = recovery_wait.as_millis(),
            reserved_bytes = inner.reserved_bytes,
            live_workers = inner.registry.len(),
            "admitted compiler daemon memory recovery"
        );
        RecoveryPermit { pool: self, priority }
    }

    #[cfg(feature = "test_features")]
    pub(super) fn high_water(&self) -> usize {
        self.inner.lock().high_water
    }

    #[cfg(feature = "test_features")]
    pub(super) fn worker_counts(&self) -> (usize, usize, u64, usize, usize) {
        let inner = self.inner.lock();
        let terminating =
            inner.registry.values().filter(|entry| entry.state == WorkerState::Terminating).count();
        (
            inner.registry.len(),
            inner.idle.len(),
            inner.reserved_bytes,
            terminating,
            inner.scheduler_evictions,
        )
    }

    /// Block until a worker is available.
    fn checkout(
        &'static self,
        priority: CompilePriority,
        memory_limit_bytes: u64,
        protected: bool,
    ) -> Result<RegisteredWorker, WorkerFailure> {
        let idx = priority.index();
        let mut inner = self.inner.lock();
        // Register before inspecting capacity so a newly arriving lower-priority
        // caller cannot steal a worker from an already-waiting higher-priority
        // caller while the latter is waking up.
        inner.waiters[idx] += 1;
        if protected {
            assert!(inner.recovery_owner, "recovery checkout without ownership");
            inner.recovery_request = Some((priority, memory_limit_bytes));
        }

        loop {
            // Recovery must be able to reclaim capacity even when a higher-
            // priority ordinary caller is waiting for that same capacity.
            if !protected && !priority_may_checkout(priority, &inner.waiters) {
                self.capacity_changed.wait(&mut inner);
                continue;
            }

            // Reuse an idle worker, draining any that have died. Teardown and
            // reaping happen without the coordinator lock.
            let mut dead_workers = Vec::new();
            while let Some(index) = inner.idle.iter().rposition(|worker| {
                inner.registry[&worker.id].memory_limit_bytes == memory_limit_bytes
            }) {
                let worker = inner.idle.swap_remove(index);
                if worker.process.is_alive() {
                    let entry = inner
                        .registry
                        .get_mut(&worker.id)
                        .expect("idle worker is missing from registry");
                    assert_eq!(entry.state, WorkerState::Idle);
                    entry.state = WorkerState::Leased { priority, protected };
                    inner.waiters[idx] -= 1;
                    self.notify_capacity_changed();
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

            // Do not let an equal or lower priority ordinary spawn consume
            // capacity already being reclaimed for the protected recovery.
            if !protected
                && inner.recovery_request.is_some_and(|(recovery_priority, _)| {
                    priority.index() >= recovery_priority.index()
                })
            {
                self.capacity_changed.wait(&mut inner);
                continue;
            }

            // Reserve count and bytes atomically before spawning. Starting and
            // terminating workers retain both reservations.
            if can_reserve(&inner, self.max_workers, self.total_budget_bytes, memory_limit_bytes) {
                let id = reserve_worker(&mut inner, memory_limit_bytes);
                inner.waiters[idx] -= 1;
                #[cfg(feature = "test_features")]
                {
                    inner.high_water = inner.high_water.max(inner.registry.len());
                }
                self.notify_capacity_changed();
                drop(inner);

                let config = WorkerConfig { memory_limit_bytes, ..self.worker_config };
                let process = match DaemonProcess::spawn(&self.binary, config) {
                    Ok(process) => process,
                    Err(spawn_failure) => {
                        let mut inner = self.inner.lock();
                        if let Some(teardown) = spawn_failure.teardown {
                            Self::mark_terminating(&mut inner, id);
                            drop(inner);
                            teardown.finish(TerminationReason::ProcessDrop, move || {
                                self.finish_termination(id);
                            });
                        } else {
                            release_reservation(&mut inner, id);
                            self.notify_capacity_changed();
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
                entry.state = WorkerState::Leased { priority, protected };
                self.notify_capacity_changed();
                return Ok(RegisteredWorker { id, process });
            }

            // A protected recovery may reclaim idle workers first, then active
            // lower-priority or equal-priority ordinary attempts. Reservations
            // remain charged until each selected process is reaped.
            if protected {
                let victims = select_recovery_victims(
                    &mut inner,
                    priority,
                    memory_limit_bytes,
                    self.max_workers,
                    self.total_budget_bytes,
                );
                if !victims.is_empty() {
                    #[cfg(feature = "test_features")]
                    {
                        inner.scheduler_evictions += victims.idle.len() + victims.active.len();
                    }
                    COMPILER_DAEMON_RECOVERY_EVENTS_TOTAL
                        .with_label_values(&["sibling_eviction"])
                        .inc_by((victims.idle.len() + victims.active.len()) as u64);
                    tracing::info!(
                        target: "vm",
                        memory_limit_bytes,
                        idle_evictions = victims.idle.len(),
                        active_evictions = victims.active.len(),
                        reserved_bytes = inner.reserved_bytes,
                        live_workers = inner.registry.len(),
                        terminating_workers = inner
                            .registry
                            .values()
                            .filter(|entry| entry.state == WorkerState::Terminating)
                            .count(),
                        "reclaiming compiler daemon workers for memory escalation"
                    );
                    drop(inner);
                    for worker in victims.idle {
                        self.retire(worker, TerminationReason::SchedulerEviction);
                    }
                    for control in victims.active {
                        let _ = control.terminate(TerminationReason::SchedulerEviction);
                    }
                    inner = self.inner.lock();
                    continue;
                }
            }

            // All count or byte capacity is reserved. Terminating workers are
            // deliberately included until their process has been reaped.
            self.capacity_changed.wait(&mut inner);
        }
    }

    fn notify_capacity_changed(&self) {
        // Waiters have different priority, tier and recovery predicates. Waking
        // just one could select an ineligible caller and strand available capacity.
        self.capacity_changed.notify_all();
    }

    fn finish_recovery(&self) {
        let mut inner = self.inner.lock();
        assert!(inner.recovery_owner, "recovery ownership was already released");
        inner.recovery_owner = false;
        inner.recovery_request = None;
        self.recovery_admission.notify_all();
        self.notify_capacity_changed();
    }

    fn mark_terminating(inner: &mut PoolInner, id: WorkerId) {
        let entry = inner.registry.get_mut(&id).expect("worker is missing from registry");
        entry.state = WorkerState::Terminating;
        observe_pool_resources(inner);
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
        let RegisteredWorker { id, process } = worker;
        process.retire(reason, move || self.finish_termination(id));
    }

    fn finish_termination(&self, id: WorkerId) {
        let mut inner = self.inner.lock();
        let entry = inner.registry.get(&id).expect("reaped worker is missing from registry");
        assert_eq!(entry.state, WorkerState::Terminating);
        release_reservation(&mut inner, id);
        self.notify_capacity_changed();
    }

    fn check_in(&'static self, worker: RegisteredWorker) {
        let mut inner = self.inner.lock();
        let entry = inner.registry.get_mut(&worker.id).expect("worker is missing from registry");
        match entry.state {
            WorkerState::Leased { .. } => {
                entry.state = WorkerState::Idle;
                inner.idle.push(worker);
                self.notify_capacity_changed();
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

#[derive(Default)]
struct RecoveryVictims {
    idle: Vec<RegisteredWorker>,
    active: Vec<Arc<ProcessControl>>,
}

impl RecoveryVictims {
    fn is_empty(&self) -> bool {
        self.idle.is_empty() && self.active.is_empty()
    }
}

/// Atomically mark the minimum useful set of recovery victims. Capacity held
/// by workers which are already terminating is treated as pending-free, so a
/// recovery never kills extra siblings merely because reaping is still in progress.
fn select_recovery_victims(
    inner: &mut PoolInner,
    recovery_priority: CompilePriority,
    requested_bytes: u64,
    max_workers: usize,
    total_budget_bytes: u64,
) -> RecoveryVictims {
    let terminating_count =
        inner.registry.values().filter(|entry| entry.state == WorkerState::Terminating).count();
    let terminating_bytes = inner
        .registry
        .values()
        .filter(|entry| entry.state == WorkerState::Terminating)
        .map(|entry| entry.memory_limit_bytes)
        .sum::<u64>();
    let mut projected_count = inner.registry.len() - terminating_count;
    let mut projected_bytes = inner.reserved_bytes - terminating_bytes;

    let fits = |count: usize, bytes: u64| {
        count < max_workers
            && bytes.checked_add(requested_bytes).is_some_and(|total| total <= total_budget_bytes)
    };
    if fits(projected_count, projected_bytes) {
        // no eviction necessary
        return RecoveryVictims::default();
    }

    let mut candidates: Vec<(WorkerId, bool, usize, u64)> = inner
        .registry
        .iter()
        .filter_map(|(&id, entry)| {
            let (idle, priority_rank) = recovery_victim_rank(entry.state, recovery_priority)?;
            Some((id, idle, priority_rank, entry.memory_limit_bytes))
        })
        .collect();
    // Eviction order:
    //  - Idle first.
    //  - Lower priorities before equal.
    //  - Larger reservations win ties so the scheduler kills no more than needed.
    candidates.sort_by(compare_recovery_candidates);

    let mut selected = Vec::new();
    for (id, _, _, bytes) in candidates {
        selected.push(id);
        projected_count -= 1;
        projected_bytes -= bytes;
        if fits(projected_count, projected_bytes) {
            break;
        }
    }
    if !fits(projected_count, projected_bytes) {
        tracing::debug!(
            target: "vm",
            priority = ?recovery_priority,
            requested_bytes,
            eligible_victims = selected.len(),
            projected_workers = projected_count,
            projected_reserved_bytes = projected_bytes,
            max_workers,
            total_budget_bytes,
            "compiler daemon memory recovery is waiting for eligible capacity"
        );
        return RecoveryVictims::default();
    }

    let mut victims = RecoveryVictims::default();
    for id in selected {
        let was_idle = inner.registry[&id].state == WorkerState::Idle;
        DaemonPool::mark_terminating(inner, id);
        if was_idle {
            let index = inner
                .idle
                .iter()
                .position(|worker| worker.id == id)
                .expect("idle recovery victim is absent from idle set");
            victims.idle.push(inner.idle.swap_remove(index));
        } else {
            let control = inner.registry[&id]
                .control
                .as_ref()
                .expect("leased recovery victim has no process control");
            victims.active.push(Arc::clone(control));
        }
    }
    victims
}

fn compare_recovery_candidates(
    left: &(WorkerId, bool, usize, u64),
    right: &(WorkerId, bool, usize, u64),
) -> Ordering {
    right.1.cmp(&left.1).then_with(|| right.2.cmp(&left.2)).then_with(|| right.3.cmp(&left.3))
}

fn recovery_victim_rank(
    state: WorkerState,
    recovery_priority: CompilePriority,
) -> Option<(bool, usize)> {
    match state {
        WorkerState::Idle => Some((true, usize::MAX)),
        WorkerState::Leased { priority, protected: false }
            if priority.index() >= recovery_priority.index() =>
        {
            Some((false, priority.index()))
        }
        WorkerState::Starting | WorkerState::Terminating | WorkerState::Leased { .. } => None,
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
    observe_pool_resources(inner);
    id
}

fn release_reservation(inner: &mut PoolInner, id: WorkerId) {
    let entry = inner.registry.remove(&id).expect("worker reservation is missing");
    inner.reserved_bytes = inner
        .reserved_bytes
        .checked_sub(entry.memory_limit_bytes)
        .expect("worker memory reservation underflowed");
    observe_pool_resources(inner);
}

fn observe_pool_resources(inner: &PoolInner) {
    let terminating =
        inner.registry.values().filter(|entry| entry.state == WorkerState::Terminating).count();
    COMPILER_DAEMON_RESERVED_MEMORY_BYTES
        .with_label_values::<&str>(&[])
        .set(inner.reserved_bytes.min(i64::MAX as u64) as i64);
    COMPILER_DAEMON_WORKERS
        .with_label_values(&["live"])
        .set(inner.registry.len().min(i64::MAX as usize) as i64);
    COMPILER_DAEMON_WORKERS
        .with_label_values(&["terminating"])
        .set(terminating.min(i64::MAX as usize) as i64);
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
pub(super) struct RecoveryPermit {
    pool: &'static DaemonPool,
    priority: CompilePriority,
}

impl RecoveryPermit {
    pub(super) fn lease(&self, memory_limit_bytes: u64) -> Result<Lease, WorkerFailure> {
        let worker = self.pool.checkout(self.priority, memory_limit_bytes, true)?;
        Ok(Lease { pool: self.pool, worker: Some(worker), retire_on_drop: true })
    }
}

impl Drop for RecoveryPermit {
    fn drop(&mut self) {
        self.pool.finish_recovery();
    }
}

/// RAII handle for a worker checked out of the pool.
pub(super) struct Lease {
    pool: &'static DaemonPool,
    worker: Option<RegisteredWorker>,
    /// Escalated workers are deliberately not retained after the protected
    /// request, because their large reservation would collapse normal concurrency.
    retire_on_drop: bool,
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
            if self.retire_on_drop {
                self.pool.discard(worker);
            } else {
                self.pool.check_in(worker);
            }
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
        DaemonPool, PoolInner, WorkerId, WorkerState, can_reserve, compare_recovery_candidates,
        highest_priority_waiter, priority_may_checkout, recovery_victim_rank, release_reservation,
        reserve_worker,
    };
    use crate::compile_priority::CompilePriority;
    use crate::compiler_daemon::protocol::WorkerConfig;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::mpsc::channel;
    use std::thread::{sleep, spawn};
    use std::time::{Duration, Instant};

    fn empty_inner() -> PoolInner {
        PoolInner {
            idle: Vec::new(),
            registry: HashMap::new(),
            reserved_bytes: 0,
            next_worker_id: 0,
            waiters: [0; CompilePriority::COUNT],
            recovery_waiters: [0; CompilePriority::COUNT],
            recovery_owner: false,
            recovery_request: None,
            #[cfg(feature = "test_features")]
            high_water: 0,
            #[cfg(feature = "test_features")]
            scheduler_evictions: 0,
        }
    }

    fn wait_for_pool(pool: &DaemonPool, predicate: impl Fn(&PoolInner) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !predicate(&pool.inner.lock()) {
            assert!(Instant::now() < deadline, "pool did not reach the expected state");
            sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn capacity_release_wakes_recovery_owner_with_other_waiters() {
        let config =
            WorkerConfig { threads: 1, thread_stack_size_bytes: 1, memory_limit_bytes: 10 };
        let pool: &'static DaemonPool = Box::leak(Box::new(DaemonPool::new(
            PathBuf::from("compiler-daemon-binary-that-does-not-exist"),
            config,
            1,
            20,
            20,
        )));
        let priority = CompilePriority::Interactive;
        let idx = priority.index();
        let owner = pool.begin_recovery(priority);
        // A synthetic terminating worker holds all process capacity until we
        // explicitly simulate its reap. No subprocess timing is involved.
        let id = {
            let mut inner = pool.inner.lock();
            let id = reserve_worker(&mut inner, 10);
            DaemonPool::mark_terminating(&mut inner, id);
            id
        };

        // Queue another recovery first: it must not consume the capacity wake
        // intended for the current owner, nor acquire ownership prematurely.
        let (admitted_tx, admitted_rx) = channel();
        let next_recovery = spawn(move || {
            let _permit = pool.begin_recovery(priority);
            admitted_tx.send(()).unwrap();
        });
        wait_for_pool(pool, |inner| inner.recovery_waiters[idx] == 1);

        let (owner_tx, owner_rx) = channel();
        let (release_tx, release_rx) = channel();
        let recovery = spawn(move || {
            owner_tx.send(owner.lease(20).is_err()).unwrap();
            release_rx.recv().unwrap();
            drop(owner);
        });
        wait_for_pool(pool, |inner| inner.waiters[idx] == 1);

        // This ordinary same-priority caller also waits for capacity, but the
        // recovery request prevents it from spawning before the owner finishes.
        let (ordinary_tx, ordinary_rx) = channel();
        let ordinary = spawn(move || {
            ordinary_tx.send(pool.lease(priority).is_err()).unwrap();
        });
        wait_for_pool(pool, |inner| inner.waiters[idx] == 2);
        assert_eq!(pool.inner.lock().reserved_bytes, 10);

        pool.finish_termination(id);
        let timeout = Duration::from_secs(5);
        // Reaching the intentional spawn error proves admission made progress.
        assert!(owner_rx.recv_timeout(timeout).unwrap());
        assert!(admitted_rx.try_recv().is_err());
        assert!(ordinary_rx.try_recv().is_err());
        release_tx.send(()).unwrap();
        admitted_rx.recv_timeout(timeout).unwrap();
        assert!(ordinary_rx.recv_timeout(timeout).unwrap());
        recovery.join().unwrap();
        next_recovery.join().unwrap();
        ordinary.join().unwrap();
        let inner = pool.inner.lock();
        assert!(inner.registry.is_empty());
        assert_eq!(inner.reserved_bytes, 0);
        assert_eq!(inner.waiters, [0; CompilePriority::COUNT]);
        assert_eq!(inner.recovery_waiters, [0; CompilePriority::COUNT]);
        assert!(!inner.recovery_owner);
    }

    #[cfg(unix)]
    #[test]
    fn recovery_evicts_with_higher_priority_waiter() {
        use crate::compiler_daemon::watchdog::{ProcessControl, TerminationReason};
        use std::process::{Command, Stdio};
        use std::sync::Arc;

        let config =
            WorkerConfig { threads: 1, thread_stack_size_bytes: 1, memory_limit_bytes: 10 };
        let pool: &'static DaemonPool = Box::leak(Box::new(DaemonPool::new(
            PathBuf::from("compiler-daemon-binary-that-does-not-exist"),
            config,
            1,
            20,
            20,
        )));
        // Model a background compilation that cannot finish on its own. Holding
        // stdin outside Child keeps it open even when waiting for the child.
        let mut child = Command::new("sh")
            .args(["-c", "read line"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let control = Arc::new(ProcessControl::new(child));
        let id = {
            let mut inner = pool.inner.lock();
            let id = reserve_worker(&mut inner, 10);
            let entry = inner.registry.get_mut(&id).unwrap();
            entry.state =
                WorkerState::Leased { priority: CompilePriority::Background, protected: false };
            entry.control = Some(Arc::clone(&control));
            id
        };

        // The critical caller has priority but no capacity, and cannot evict.
        let (ordinary_tx, ordinary_rx) = channel();
        let ordinary = spawn(move || {
            ordinary_tx.send(pool.lease(CompilePriority::Critical).is_err()).unwrap();
        });
        wait_for_pool(pool, |inner| inner.waiters[CompilePriority::Critical.index()] == 1);

        let owner = pool.begin_recovery(CompilePriority::Interactive);
        let (owner_tx, owner_rx) = channel();
        let recovery = spawn(move || {
            owner_tx.send(owner.lease(20).is_err()).unwrap();
        });
        wait_for_pool(pool, |inner| inner.recovery_request.is_some());

        // Recovery must evict despite the critical waiter, before the test
        // releases any capacity. Capture the outcome, then clean up even when
        // the regression prevents eviction so no child or waiter is left behind.
        let timeout = Duration::from_secs(5);
        let evicted_status = control.wait_for_exit(timeout).unwrap();
        let termination_reason = control.termination_reason();
        DaemonPool::mark_terminating(&mut pool.inner.lock(), id);
        control.terminate(TerminationReason::ProcessDrop).unwrap();
        assert!(control.wait_for_exit(timeout).unwrap().is_some());
        drop(stdin);
        pool.finish_termination(id);

        // Intentional spawn errors prove both callers passed admission. Either
        // may win the reclaimed capacity. Recovery need not run before Critical.
        assert!(ordinary_rx.recv_timeout(timeout).unwrap());
        assert!(owner_rx.recv_timeout(timeout).unwrap());
        ordinary.join().unwrap();
        recovery.join().unwrap();
        let inner = pool.inner.lock();
        assert!(inner.registry.is_empty());
        assert_eq!(inner.reserved_bytes, 0);
        assert_eq!(inner.waiters, [0; CompilePriority::COUNT]);
        assert!(!inner.recovery_owner);
        assert!(evicted_status.is_some(), "recovery did not evict the background worker");
        assert_eq!(termination_reason, Some(TerminationReason::SchedulerEviction));
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
    fn recovery_victims_respect_priority_and_protection() {
        let ordinary_background =
            WorkerState::Leased { priority: CompilePriority::Background, protected: false };
        assert!(recovery_victim_rank(ordinary_background, CompilePriority::Critical).is_some());
        assert!(recovery_victim_rank(ordinary_background, CompilePriority::Background).is_some());

        let higher_priority =
            WorkerState::Leased { priority: CompilePriority::Critical, protected: false };
        assert!(recovery_victim_rank(higher_priority, CompilePriority::Interactive).is_none());
        let protected =
            WorkerState::Leased { priority: CompilePriority::Background, protected: true };
        assert!(recovery_victim_rank(protected, CompilePriority::Critical).is_none());
        assert!(recovery_victim_rank(WorkerState::Starting, CompilePriority::Critical).is_none());
        assert!(
            recovery_victim_rank(WorkerState::Terminating, CompilePriority::Critical).is_none()
        );
    }

    #[test]
    fn recovery_victim_order_prefers_idle_then_priority_then_larger_reservation() {
        let mut candidates = [
            (WorkerId(0), false, CompilePriority::Interactive.index(), 20),
            (WorkerId(1), true, usize::MAX, 10),
            (WorkerId(2), false, CompilePriority::Background.index(), 10),
            (WorkerId(3), true, usize::MAX, 30),
            (WorkerId(4), false, CompilePriority::Background.index(), 40),
        ];

        candidates.sort_by(compare_recovery_candidates);

        assert_eq!(
            candidates.iter().map(|candidate| candidate.0).collect::<Vec<_>>(),
            vec![WorkerId(3), WorkerId(1), WorkerId(4), WorkerId(2), WorkerId(0)]
        );
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
    fn failed_spawn_without_child_releases_reservation() {
        let config =
            WorkerConfig { threads: 1, thread_stack_size_bytes: 1, memory_limit_bytes: 10 };
        let pool = Box::leak(Box::new(DaemonPool::new(
            PathBuf::from("compiler-daemon-binary-that-does-not-exist"),
            config,
            1,
            40,
            40,
        )));

        assert!(pool.lease(CompilePriority::Critical).is_err());
        let inner = pool.inner.lock();
        assert!(inner.registry.is_empty());
        assert_eq!(inner.reserved_bytes, 0);
    }

    #[test]
    fn memory_escalation_stops_at_configured_maximum() {
        let config =
            WorkerConfig { threads: 1, thread_stack_size_bytes: 1, memory_limit_bytes: 10 };
        let pool = DaemonPool::new(PathBuf::new(), config, 1, 40, 40);

        assert_eq!(pool.next_memory_limit(10), Some(20));
        assert_eq!(pool.next_memory_limit(20), Some(40));
        assert_eq!(pool.next_memory_limit(40), None);
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
