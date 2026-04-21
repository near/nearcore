use crate::futures::AsyncComputationSpawner;
use near_o11y::metrics::{IntGaugeVec, try_create_int_gauge_vec};
use parking_lot::{Condvar, Mutex};
use std::collections::VecDeque;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
#[cfg(unix)]
use thread_priority::{
    RealtimeThreadSchedulePolicy, ThreadBuilder, ThreadPriority, ThreadSchedulePolicy,
};

type Job = Box<dyn FnOnce() + Send + 'static>;

static THREAD_POOL_NUM_THREADS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_thread_pool_num_threads",
        "current number of threads in the thread pool",
        &["pool_name"],
    )
    .unwrap()
});

static THREAD_POOL_MAX_NUM_THREADS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_thread_pool_max_num_threads",
        "maximum observed number of threads in the thread pool",
        &["pool_name"],
    )
    .unwrap()
});

static THREAD_POOL_QUEUE_SIZE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_thread_pool_queue_size",
        "thread pool job queue size",
        &["pool_name"],
    )
    .unwrap()
});

/// OS thread pool for spawning computation tasks.
///
/// The pool enforces a hard limit on the number of threads. If all threads are
/// busy and the limit is reached, the job execution will be queued until a thread
/// becomes available. Idle threads are kept for `idle_timeout` to be potentially
/// reused for new tasks. All threads in the pool are spawned under round-robin
/// realtime policy (`SCHED_RR`) with a configured `priority`. Realtime threads
/// **always** take precedence over threads using normal policy (`SCHED_OTHER`),
/// so `priority` applies **only among other realtime threads**.
///
/// Workers are plain OS threads — they do **not** participate in rayon's
/// work-stealing scheduler. This is important when spawned tasks may trigger
/// rayon-based parallel work internally (e.g. wasmtime's parallel compilation):
/// since the workers are not rayon threads, nested `rayon::join()` calls fall
/// through to the global rayon pool instead of the worker's pool, avoiding
/// deadlocks from recursive work-stealing.
pub struct ThreadPool {
    name: &'static str,
    /// Thread priority in [0; 99] range. Only used on unix for SCHED_RR policy.
    #[cfg(unix)]
    priority: u8,
    idle_timeout: Duration,
    thread_limit: usize,
    state: Arc<ThreadPoolState>,
}

impl std::fmt::Debug for ThreadPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPool").field("name", &self.name).finish()
    }
}

struct ThreadPoolStateInner {
    name: &'static str,
    queue: VecDeque<Job>,
    total_threads: usize,
    idle_threads: usize,
    shutdown: bool,
}

impl ThreadPoolStateInner {
    fn new(name: &'static str) -> Self {
        Self { name, queue: VecDeque::new(), total_threads: 0, idle_threads: 0, shutdown: false }
    }

    fn enqueue(&mut self, job: Job) {
        self.queue.push_back(job);
        self.update_queue_metrics();
    }

    fn dequeue(&mut self) -> Option<Job> {
        let ret = self.queue.pop_front();
        if ret.is_some() {
            self.update_queue_metrics();
        }
        ret
    }

    fn inc_idle_threads(&mut self) {
        self.idle_threads += 1;
    }

    fn dec_idle_threads(&mut self) {
        self.idle_threads -= 1;
    }

    fn inc_total_threads(&mut self) {
        self.total_threads += 1;
        self.update_total_threads_metrics();
    }

    fn dec_total_threads(&mut self) {
        self.total_threads -= 1;
        self.update_total_threads_metrics();
    }

    fn update_queue_metrics(&self) {
        THREAD_POOL_QUEUE_SIZE.with_label_values(&[self.name]).set(self.queue.len() as i64);
    }

    fn update_total_threads_metrics(&self) {
        THREAD_POOL_NUM_THREADS.with_label_values(&[self.name]).set(self.total_threads as i64);
        let max_num_threads = THREAD_POOL_MAX_NUM_THREADS.with_label_values(&[self.name]);
        if self.total_threads > max_num_threads.get() as usize {
            max_num_threads.set(self.total_threads as i64);
        }
    }
}

struct ThreadPoolState {
    inner: Mutex<ThreadPoolStateInner>,
    /// Signaled when new task is added to the queue or on shutdown.
    condvar: Condvar,
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.state.inner.lock().shutdown = true;
        self.state.condvar.notify_all();
    }
}

impl ThreadPool {
    /// Create a new thread pool. Panics if priority is out of [0; 99] range.
    pub fn new(name: &'static str, idle_timeout: Duration, limit: usize, priority: u8) -> Self {
        assert!(priority <= 99, "priority out of range");
        Self {
            name,
            #[cfg(unix)]
            priority,
            idle_timeout,
            thread_limit: limit,
            state: Arc::new(ThreadPoolState {
                inner: Mutex::new(ThreadPoolStateInner::new(name)),
                condvar: Condvar::new(),
            }),
        }
    }

    /// Spawn a new task to be run on the pool. It will re-use existing idle threads
    /// if possible, or spawn a new thread (up to the configured limit).
    /// If at the thread limit and no idle threads are available, the job will be
    /// queued until a thread becomes available. The caller thread is never blocked.
    pub fn spawn_boxed(&self, job: Job) {
        let mut state_guard = self.state.inner.lock();
        state_guard.enqueue(job);
        if state_guard.idle_threads > 0 {
            self.state.condvar.notify_one();
            return;
        }
        if state_guard.total_threads < self.thread_limit {
            state_guard.inc_total_threads();
            drop(state_guard);
            self.spawn_thread();
        } else {
            tracing::trace!(
                target: "near_async::thread_pool",
                pool = self.name,
                limit = self.thread_limit,
                queue_size = state_guard.queue.len(),
                "job execution is delayed, thread pool is at capacity"
            );
        }
    }

    fn spawn_thread(&self) {
        let name = self.name;
        let idle_timeout = self.idle_timeout;
        let state = self.state.clone();
        #[cfg(unix)]
        {
            let priority: ThreadPriority = self.priority.try_into().expect("priority out of range");
            ThreadBuilder::default()
                .name(name)
                .policy(ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::RoundRobin))
                .priority(priority)
                .spawn(move |res| {
                    if let Err(err) = res {
                        tracing::debug!(
                            target: "near_async::thread_pool",
                            pool = name,
                            err = %err,
                            "set scheduler policy failed"
                        );
                    };
                    run_worker(state, idle_timeout)
                })
                .expect("failed to spawn thread");
        }
        #[cfg(not(unix))]
        {
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(move || run_worker(state, idle_timeout))
                .expect("failed to spawn thread");
        }
    }

    #[cfg(test)]
    fn state(&self) -> &Arc<ThreadPoolState> {
        &self.state
    }
}

impl AsyncComputationSpawner for ThreadPool {
    fn spawn_boxed(&self, _name: &str, job: Box<dyn FnOnce() + Send>) {
        self.spawn_boxed(job)
    }
}

struct ThreadCountGuard(Arc<ThreadPoolState>);

impl Drop for ThreadCountGuard {
    fn drop(&mut self) {
        self.0.inner.lock().dec_total_threads();
    }
}

/// Start a worker thread. It will then pick up jobs from the queue one at a time in
/// a loop. The thread will terminate if it's idle for `idle_timeout` or when
/// shutdown is triggered via `shutdown` flag.
fn run_worker(state: Arc<ThreadPoolState>, idle_timeout: Duration) {
    // `_thread_count_guard` must be declared before `state_guard` so that
    // the mutex lock is dropped first during unwinding (reverse declaration
    // order), avoiding deadlock when the guard acquires the lock in its Drop.
    let _thread_count_guard = ThreadCountGuard(state.clone());
    let mut state_guard = state.inner.lock();
    loop {
        if state_guard.shutdown {
            tracing::trace!(
                target: "near_async::thread_pool",
                pool = state_guard.name,
                "terminate thread at shutdown"
            );
            break;
        }
        if let Some(job) = state_guard.dequeue() {
            drop(state_guard);
            job();
            state_guard = state.inner.lock();
        } else {
            state_guard.inc_idle_threads();
            let timeout_res = state.condvar.wait_for(&mut state_guard, idle_timeout);
            state_guard.dec_idle_threads();
            // Note on queue check on timeout:
            // If an idle thread's `wait_for` times out concurrently with `spawn_boxed` enqueueing a job and
            // calling `notify_one()` (which saw `idle_threads > 0` and thus didn't spawn a new thread), the job
            // can be orphaned in the queue. So we need to make sure the queue is empty before exiting the thread.
            if timeout_res.timed_out() && state_guard.queue.is_empty() {
                tracing::trace!(
                    target: "near_async::thread_pool",
                    pool = state_guard.name,
                    "terminate idle thread"
                );
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Named pool instances.
// ---------------------------------------------------------------------------

// SCHED_RR priorities (higher preempts lower)
/// Contract compilation and pipelining
const PRIORITY_CONTRACT_COMPILATION: u8 = 55;
/// Chunk application, incl. witness validation.
const PRIORITY_APPLY_CHUNKS: u8 = 50;
/// Validation/forwarding of partial encoded witnesses
const PRIORITY_PARTIAL_WITNESS_VALIDATION: u8 = 70;
/// Witness creation
const PRIORITY_WITNESS_CREATION: u8 = 70;

/// Shared thread pool for contract compilation and pipelining.
pub fn contract_compilation_pool() -> &'static Arc<ThreadPool> {
    static POOL: std::sync::OnceLock<Arc<ThreadPool>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let thread_limit = std::thread::available_parallelism().map_or(4, |n| n.get());
        Arc::new(ThreadPool::new(
            "contract_compilation",
            Duration::from_secs(3600),
            thread_limit,
            PRIORITY_CONTRACT_COMPILATION,
        ))
    })
}

/// Async computation spawner to be used for chunk applying tasks.
#[derive(Default)]
pub enum ApplyChunksSpawner {
    /// Use a pool of OS-based high priority threads, limited by the number of shards.
    #[default]
    Default,
    /// Use a custom spawner, e.g. rayon.
    Custom(Arc<dyn AsyncComputationSpawner>),
}

impl ApplyChunksSpawner {
    /// Get the custom spawner, or create the default spawner with the given thread limit.
    pub fn into_spawner(self, thread_limit: usize) -> Arc<dyn AsyncComputationSpawner> {
        match self {
            ApplyChunksSpawner::Default => Arc::new(ThreadPool::new(
                "apply_chunks",
                Duration::from_secs(30),
                thread_limit,
                PRIORITY_APPLY_CHUNKS,
            )),
            ApplyChunksSpawner::Custom(spawner) => spawner,
        }
    }
}

/// High-priority thread pool for validating partial chunk witnesses.
pub struct PartialWitnessValidationThreadPool(ThreadPool);

impl PartialWitnessValidationThreadPool {
    pub fn new() -> Self {
        Self(ThreadPool::new(
            "partial_witness_validation",
            Duration::from_secs(30),
            96,
            PRIORITY_PARTIAL_WITNESS_VALIDATION,
        ))
    }
}

impl AsyncComputationSpawner for PartialWitnessValidationThreadPool {
    fn spawn_boxed(&self, _name: &str, job: Box<dyn FnOnce() + Send>) {
        self.0.spawn_boxed(job)
    }
}

/// High-priority thread pool for creating chunk witnesses.
pub struct WitnessCreationThreadPool(ThreadPool);

impl WitnessCreationThreadPool {
    pub fn new() -> Self {
        Self(ThreadPool::new(
            "witness_creation",
            Duration::from_secs(30),
            6,
            PRIORITY_WITNESS_CREATION,
        ))
    }
}

impl AsyncComputationSpawner for WitnessCreationThreadPool {
    fn spawn_boxed(&self, _name: &str, job: Box<dyn FnOnce() + Send>) {
        self.0.spawn_boxed(job)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread::{self, ThreadId};
    use std::time::Instant;

    const POOL_NAME: &str = "test_pool";
    const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
    const DEFAULT_LIMIT: usize = 2;
    const DEFAULT_PRIORITY: u8 = 50;

    #[derive(Debug)]
    struct JobExecutionOutcome {
        thread_id: ThreadId,
    }

    struct JobHandle {
        scheduled_receiver: mpsc::Receiver<()>,
        start_sender: mpsc::Sender<()>,
        done_receiver: mpsc::Receiver<JobExecutionOutcome>,
    }

    impl JobHandle {
        fn start_execution(self) -> mpsc::Receiver<JobExecutionOutcome> {
            self.start_sender.send(()).unwrap();
            self.done_receiver
        }

        fn wait_scheduled(&self) {
            self.scheduled_receiver.recv().unwrap();
        }

        fn wait_executed(self) -> JobExecutionOutcome {
            self.start_execution().recv().unwrap()
        }
    }

    fn create_job() -> (Job, JobHandle) {
        let (scheduled_sender, scheduled_receiver) = mpsc::channel();
        let (start_sender, start_receiver) = mpsc::channel();
        let (done_sender, done_receiver) = mpsc::channel();
        let job = Box::new(move || {
            let _ = scheduled_sender.send(());
            if start_receiver.recv().is_err() {
                return;
            };
            let thread_id = thread::current().id();
            let outcome = JobExecutionOutcome { thread_id };
            let _ = done_sender.send(outcome);
        });
        let start_trigger = JobHandle { scheduled_receiver, start_sender, done_receiver };
        (job, start_trigger)
    }

    fn execute_job(pool: &ThreadPool) -> JobExecutionOutcome {
        let (job, handle) = create_job();
        pool.spawn_boxed(job);
        handle.wait_executed()
    }

    /// Waits for condition to become true, polling with short yields.
    /// Panics with the given message if timeout is reached.
    fn wait_for(condition: impl Fn() -> bool, msg: &str) {
        const WAIT_TIMEOUT: Duration = Duration::from_secs(1);
        let start = Instant::now();
        while !condition() {
            if start.elapsed() > WAIT_TIMEOUT {
                panic!("timeout waiting for {}", msg);
            }
            thread::yield_now();
        }
    }

    #[test]
    #[should_panic(expected = "priority out of range")]
    fn invalid_priority() {
        ThreadPool::new(POOL_NAME, DEFAULT_IDLE_TIMEOUT, DEFAULT_LIMIT, 100);
    }

    #[test]
    fn single_job() {
        let pool =
            ThreadPool::new(POOL_NAME, DEFAULT_IDLE_TIMEOUT, DEFAULT_LIMIT, DEFAULT_PRIORITY);
        execute_job(&pool);
    }

    #[test]
    fn thread_reuse() {
        let limit = 2;
        let pool = ThreadPool::new(POOL_NAME, DEFAULT_IDLE_TIMEOUT, limit, DEFAULT_PRIORITY);

        let outcome1 = execute_job(&pool);
        wait_for(|| pool.state().inner.lock().idle_threads == 1, "thread to become idle");
        let outcome2 = execute_job(&pool);

        assert_eq!(outcome1.thread_id, outcome2.thread_id);
    }

    #[test]
    fn concurrent_execution() {
        let limit = 2;
        let pool = ThreadPool::new(POOL_NAME, DEFAULT_IDLE_TIMEOUT, limit, DEFAULT_PRIORITY);

        let (job1, handle1) = create_job();
        pool.spawn_boxed(job1);
        handle1.wait_scheduled();
        let (job2, handle2) = create_job();
        pool.spawn_boxed(job2);
        handle2.wait_scheduled();

        let outcome1 = handle1.wait_executed();
        let outcome2 = handle2.wait_executed();

        assert_ne!(outcome1.thread_id, outcome2.thread_id);
    }

    #[test]
    fn idle_timeout() {
        let idle_timeout = Duration::ZERO;
        let pool = ThreadPool::new(POOL_NAME, idle_timeout, DEFAULT_LIMIT, DEFAULT_PRIORITY);

        let outcome1 = execute_job(&pool);
        wait_for(|| pool.state().inner.lock().total_threads == 0, "thread shutdown");
        let outcome2 = execute_job(&pool);

        assert_ne!(outcome1.thread_id, outcome2.thread_id);
    }

    #[test]
    fn thread_limit_enforced() {
        let limit = 1;
        let pool = ThreadPool::new(POOL_NAME, DEFAULT_IDLE_TIMEOUT, limit, DEFAULT_PRIORITY);

        let (job1, handle1) = create_job();
        pool.spawn_boxed(job1);
        handle1.wait_scheduled();

        let (job2, handle2) = create_job();
        pool.spawn_boxed(job2);

        assert_eq!(pool.state().inner.lock().total_threads, 1);
        assert_eq!(pool.state().inner.lock().queue.len(), 1);

        let outcome1 = handle1.wait_executed();
        let outcome2 = handle2.wait_executed();
        assert_eq!(outcome1.thread_id, outcome2.thread_id);
    }

    #[test]
    fn drop_shuts_down_threads() {
        let idle_timeout = Duration::from_secs(1000);
        let pool = ThreadPool::new(POOL_NAME, idle_timeout, DEFAULT_LIMIT, DEFAULT_PRIORITY);

        execute_job(&pool);

        let state = pool.state().clone();
        wait_for(|| state.inner.lock().idle_threads == 1, "thread to become idle");

        drop(pool);

        wait_for(|| state.inner.lock().total_threads == 0, "threads to shut down after drop");
    }
}
