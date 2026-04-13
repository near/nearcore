use near_async::futures::AsyncComputationSpawner;
use near_async::thread_pool::{Priority, ThreadPool};
use std::sync::Arc;
use std::time::Duration;

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
                Priority::Realtime(50),
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
            Priority::Realtime(70),
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
            Priority::Realtime(70),
        ))
    }
}

impl AsyncComputationSpawner for WitnessCreationThreadPool {
    fn spawn_boxed(&self, _name: &str, job: Box<dyn FnOnce() + Send>) {
        self.0.spawn_boxed(job)
    }
}

/// Small OS-thread pool that runs `precompile_contracts` for deployed
/// contracts seen at witness time.
pub struct PrecompileDeployedContractsThreadPool(ThreadPool);

impl PrecompileDeployedContractsThreadPool {
    pub fn new() -> Self {
        Self(ThreadPool::new(
            "precompile_deployed_contracts",
            Duration::from_secs(30),
            std::cmp::max(2, num_cpus::get() / 4),
            Priority::Default,
        ))
    }
}

impl AsyncComputationSpawner for PrecompileDeployedContractsThreadPool {
    fn spawn_boxed(&self, _name: &str, job: Box<dyn FnOnce() + Send>) {
        self.0.spawn_boxed(job)
    }
}
