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
use crate::metrics::COMPILER_DAEMON_RECOVERY_EVENTS_TOTAL;
use parking_lot::{Condvar, Mutex};
use std::array::from_fn;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::Builder;
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
    /// One wait queue per priority class; index by `CompilePriority::index`.
    avail: [Condvar; CompilePriority::COUNT],
    recovery_avail: [Condvar; CompilePriority::COUNT],
}

impl DaemonPool {
    pub(super) fn new(
        binary: PathBuf,
        worker_config: WorkerConfig,
        max_workers: usize,
        max_worker_limit_bytes: u64,
        total_budget_bytes: u64,
    ) -> Self {
        Self {
            binary,
            worker_config,
            max_workers,
            max_worker_limit_bytes,
            total_budget_bytes,
            inner: Mutex::new(PoolInner {
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
            }),
            avail: from_fn(|_| Condvar::new()),
            recovery_avail: from_fn(|_| Condvar::new()),
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
            self.recovery_avail[idx].wait(&mut inner);
        }
        inner.recovery_waiters[idx] -= 1;
        inner.recovery_owner = true;
        tracing::info!(
            target: "vm",
            priority = ?priority,
            recovery_wait_ms = started.elapsed().as_millis(),
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
            if !priority_may_checkout(priority, &inner.waiters) {
                self.avail[idx].wait(&mut inner);
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

            // Do not let an equal or lower priority ordinary spawn consume
            // capacity already being reclaimed for the protected recovery.
            if !protected
                && inner.recovery_request.is_some_and(|(recovery_priority, _)| {
                    priority.index() >= recovery_priority.index()
                })
            {
                self.avail[idx].wait(&mut inner);
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
                if can_reserve(
                    &inner,
                    self.max_workers,
                    self.total_budget_bytes,
                    memory_limit_bytes,
                ) {
                    self.wake_one(&inner);
                }
                drop(inner);

                let config = WorkerConfig { memory_limit_bytes, ..self.worker_config };
                let process = match DaemonProcess::spawn(&self.binary, config) {
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
                entry.state = WorkerState::Leased { priority, protected };
                self.wake_one(&inner);
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
            if protected {
                self.recovery_avail[idx].wait(&mut inner);
            } else {
                self.avail[idx].wait(&mut inner);
            }
        }
    }

    fn wake_one(&self, inner: &PoolInner) {
        if inner.recovery_owner {
            for condvar in &self.recovery_avail {
                condvar.notify_one();
            }
        }
        if let Some(idx) = highest_priority_waiter(&inner.waiters) {
            self.avail[idx].notify_one();
        }
    }

    fn finish_recovery(&self) {
        let mut inner = self.inner.lock();
        assert!(inner.recovery_owner, "recovery ownership was already released");
        inner.recovery_owner = false;
        inner.recovery_request = None;
        if let Some(idx) = highest_priority_waiter(&inner.recovery_waiters) {
            self.recovery_avail[idx].notify_one();
        }
        self.wake_one(&inner);
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
    candidates.sort_by(|left, right| {
        right.1.cmp(&left.1).then_with(|| right.2.cmp(&left.2)).then_with(|| right.3.cmp(&left.3))
    });

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
        PoolInner, WorkerState, can_reserve, highest_priority_waiter, priority_may_checkout,
        recovery_victim_rank, release_reservation, reserve_worker,
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
            recovery_waiters: [0; CompilePriority::COUNT],
            recovery_owner: false,
            recovery_request: None,
            #[cfg(feature = "test_features")]
            high_water: 0,
            #[cfg(feature = "test_features")]
            scheduler_evictions: 0,
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
