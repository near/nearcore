use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use thread_priority::{RealtimeThreadSchedulePolicy, ThreadBuilder, ThreadSchedulePolicy};

pub(crate) trait ThreadLimit {
    fn max_threads(&self) -> usize;
}

/// TODO: Implement `ThreadLimit` which returns the current number of tracked shards
pub(crate) type ConstLimit = usize;

impl ThreadLimit for ConstLimit {
    fn max_threads(&self) -> usize {
        *self
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;
type IdleQueue = Arc<Mutex<VecDeque<oneshot::Sender<Option<Job>>>>>;

pub(crate) struct ThreadPool<C: ThreadLimit> {
    name: &'static str,
    limit: C,
    idle_timeout: Duration,
    num_threads: AtomicUsize,
    idle_queue: IdleQueue,
}

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("thread limit reached for pool {name}: {limit}")]
    ThreadLimitReached { name: &'static str, limit: usize },
    #[error("spawning thread failed: {0}")]
    Spawn(#[from] std::io::Error),
}

impl<C: ThreadLimit> ThreadPool<C> {
    pub(crate) fn new(name: &'static str, idle_timeout: Duration, limit: C) -> Self {
        Self {
            name,
            limit,
            idle_timeout,
            num_threads: Default::default(),
            idle_queue: Default::default(),
        }
    }

    pub(crate) fn spawn(&self, f: impl FnOnce() + Send + 'static) -> Result<(), Error> {
        self.spawn_boxed(Box::new(f))
    }

    pub(crate) fn spawn_boxed(&self, job: Job) -> Result<(), Error> {
        // Try to use one of the existing idle threads
        let mut job = Some(job);
        let mut idle_guard = self.idle_queue.lock();
        while let Some(sender) = idle_guard.pop_front() {
            job = match sender.send(job) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    // Idle thread has been terminated
                    self.num_threads.fetch_sub(1, Ordering::Relaxed);
                    err.into_inner()
                }
            }
        }
        drop(idle_guard);
        self.try_spawn_thread(job.unwrap())
    }

    fn try_spawn_thread(&self, job: Job) -> Result<(), Error> {
        let name = self.name;
        let limit = self.limit.max_threads();
        if self.num_threads.load(Ordering::Relaxed) >= limit {
            return Err(Error::ThreadLimitReached { name, limit });
        }

        let idle_timeout = self.idle_timeout;
        let idle_queue = self.idle_queue.clone();
        ThreadBuilder::default()
            .name(name)
            .policy(ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::RoundRobin))
            // TODO: Set thread priority
            .spawn_careless(move || run_worker(job, idle_timeout, idle_queue))?;
        self.num_threads.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}

fn run_worker(mut job: Job, idle_timeout: Duration, idle_queue: IdleQueue) {
    loop {
        job();
        // Notify the pool that this thread is idle by pushing the sender into the idle queue
        let (sender, receiver) = oneshot::channel();
        idle_queue.lock().push_back(sender);

        job = match receiver.recv_timeout(idle_timeout) {
            Ok(Some(job)) => job,
            _ => break,
        }
        // No need to notify the pool that this thread has terminated – dropping the receiver is enough
    }
}
