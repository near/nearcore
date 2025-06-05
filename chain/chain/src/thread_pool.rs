use near_async::futures::AsyncComputationSpawner;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use thread_priority::{
    RealtimeThreadSchedulePolicy, ThreadBuilder, ThreadPriority, ThreadSchedulePolicy,
};
use tracing::debug;

type Job = Box<dyn FnOnce() + Send + 'static>;
type IdleThreadQueue = Arc<Mutex<VecDeque<oneshot::Sender<Option<Job>>>>>;

/// OS thread pool for spawning latency-critical real time tasks.
///
/// The pool can spawn up to `limit` threads. Idle threads are kept for
/// `idle_timeout` to be potentially reused for new tasks. All threads in the
/// pool are spawned under round-robin realtime policy (`SCHED_RR`) with a
/// configured `priority`. Realtime threads **always** take precedence over
/// threads using normal policy (`SCHED_OTHER`), so `priority` applies **only
/// among other realtime threads**.
pub(crate) struct ThreadPool {
    /// Name of the pool. Used for logging/debugging purposes.
    name: &'static str,
    /// Limit of running threads.
    limit: usize,
    /// Priority of spawned threads (must be in [0; 100) range)
    priority: ThreadPriority,
    /// Timeout after which an idle thread terminates.
    idle_timeout: Duration,
    /// Counter of currently running worker threads (active or idle).
    worker_counter: Arc<AtomicUsize>,
    /// Queue of oneshot senders which allow sending jobs to idle threads.
    /// Once a worker thread is done processing its job, it pushes a sender into this queue.
    idle_thread_queue: IdleThreadQueue,
}

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("thread limit reached for pool {name}: {limit}")]
    ThreadLimitReached { name: &'static str, limit: usize },
    #[error("spawning thread failed: {0}")]
    Spawn(#[from] std::io::Error),
}

impl ThreadPool {
    /// Create a new thread pool. Panics if priority is out of [0; 100) range.
    pub(crate) fn new(
        name: &'static str,
        idle_timeout: Duration,
        limit: usize,
        priority: u8,
    ) -> Self {
        Self {
            name,
            limit,
            priority: priority.try_into().expect("priority out of range"),
            idle_timeout,
            worker_counter: Default::default(),
            idle_thread_queue: Default::default(),
        }
    }

    /// Spawn a new task to be run on the pool. It will re-use existing idle threads
    /// if possible, or spawn a new thread. Returns error when the thread limit is reached
    /// or spawning a new thread fails.
    pub(crate) fn spawn_boxed(&self, job: Job) -> Result<(), Error> {
        // Try to use one of the existing idle threads
        let mut job = Some(job);
        let mut queue_guard = self.idle_thread_queue.lock();
        while let Some(sender) = queue_guard.pop_front() {
            job = match sender.send(job) {
                Ok(()) => return Ok(()),
                Err(err) => err.into_inner(),
            }
        }
        drop(queue_guard);
        self.try_spawn_thread(job.unwrap())
    }

    fn try_spawn_thread(&self, job: Job) -> Result<(), Error> {
        let name = self.name;
        let limit = self.limit;
        self.worker_counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| (v < limit).then_some(v + 1))
            .map_err(|_| Error::ThreadLimitReached { name, limit })?;

        let idle_timeout = self.idle_timeout;
        let idle_queue = self.idle_thread_queue.clone();
        let worker_counter = self.worker_counter.clone();
        ThreadBuilder::default()
            .name(name)
            .policy(ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::RoundRobin))
            .priority(self.priority)
            .spawn(move |res| {
                if let Err(err) = res {
                    debug!("Setting scheduler policy failed: {err}")
                };
                run_worker(job, idle_timeout, idle_queue, worker_counter)
            })
            .map_err(|err| {
                self.worker_counter.fetch_sub(1, Ordering::Relaxed);
                err
            })?;

        Ok(())
    }
}

impl AsyncComputationSpawner for ThreadPool {
    fn spawn_boxed(&self, _name: &str, job: Box<dyn FnOnce() + Send>) {
        self.spawn_boxed(job).expect("thread pool failed to spawn thread")
    }
}

/// This struct ensures that the thread counter decrements when a thread dies,
/// even in case of a panic.
struct WorkerCounterGuard(Arc<AtomicUsize>);

impl Drop for WorkerCounterGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn run_worker(
    mut job: Job,
    idle_timeout: Duration,
    idle_queue: IdleThreadQueue,
    worker_counter: Arc<AtomicUsize>,
) {
    let _counter_guard = WorkerCounterGuard(worker_counter);
    loop {
        job();
        // Notify the pool that this thread is idle by pushing the sender into the idle queue
        let (sender, receiver) = oneshot::channel();
        idle_queue.lock().push_front(sender);

        job = match receiver.recv_timeout(idle_timeout) {
            Ok(Some(job)) => job,
            _ => break,
        }
    }
    // No need to notify the pool that this thread has terminated – dropping the receiver is enough
}
