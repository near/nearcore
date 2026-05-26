use near_async::futures::DelayedActionRunner;
use near_async::messaging::Actor;

/// Block-level half of the per-shard SPICE chunk-execution subsystem. Owns the
/// per-shard `PerShardExecutor` actors, routes data events to them, and drives
/// the disk-driven execution-head advance.
///
/// Skeleton only for now — fields and handlers are added in the phases that use
/// them so the prototype diff stays minimal. Gated behind
/// [`super::SPICE_PER_SHARD_EXECUTOR`].
#[derive(Default)]
pub struct ChunkExecutorCoordinator {}

impl ChunkExecutorCoordinator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Actor for ChunkExecutorCoordinator {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "chunk_executor", "ChunkExecutorCoordinator started (prototype)");
    }
}
