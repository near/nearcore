//! Centralized thread pool definitions for nearcore.
//!
//! All pools use OS threads (not rayon) to avoid deadlocks with wasmtime's
//! parallel-compilation feature, which internally uses `rayon::join()`.
//!
//! SCHED_RR priorities (higher = more important):
//!   contract_compilation:       55   (compilation & pipelining)
//!   apply_chunks:               50   (chunk application, incl. when validating the witness. thread limit = 3 x shard_limit)
//!   partial_witness_validation: 70   (validation/forwarding of partial encoded witnesses)
//!   witness_creation:           70   (witness creation)

use near_async::futures::AsyncComputationSpawner;
use near_async::thread_pool::ThreadPool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Shared thread pool for contract compilation and pipelining.
pub fn contract_compilation_pool() -> &'static Arc<ThreadPool> {
    static POOL: OnceLock<Arc<ThreadPool>> = OnceLock::new();
    POOL.get_or_init(|| {
        let thread_limit = std::thread::available_parallelism().map_or(4, |n| n.get());
        Arc::new(ThreadPool::new(
            "contract_compilation",
            Duration::from_secs(360),
            thread_limit,
            55,
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
            ApplyChunksSpawner::Default => {
                Arc::new(ThreadPool::new("apply_chunks", Duration::from_secs(30), thread_limit, 50))
            }
            ApplyChunksSpawner::Custom(spawner) => spawner,
        }
    }
}

/// High-priority thread pool for validating partial chunk witnesses.
pub struct PartialWitnessValidationThreadPool(ThreadPool);

impl PartialWitnessValidationThreadPool {
    pub fn new() -> Self {
        Self(ThreadPool::new("partial_witness_validation", Duration::from_secs(30), 96, 70))
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
        Self(ThreadPool::new("witness_creation", Duration::from_secs(30), 6, 70))
    }
}

impl AsyncComputationSpawner for WitnessCreationThreadPool {
    fn spawn_boxed(&self, _name: &str, job: Box<dyn FnOnce() + Send>) {
        self.0.spawn_boxed(job)
    }
}
