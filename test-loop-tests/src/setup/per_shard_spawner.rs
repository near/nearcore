use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, Sender};
use near_async::test_loop::data::TestLoopData;
use near_async::test_loop::pending_events_sender::PendingEventsSender;
use near_client::spice::chunk_executor_coordinator::PerShardChunkApplied;
use near_client::spice::per_shard_executor::PerShardExecutorSender;
use near_client::spice::per_shard_spawner::{PerShardDeps, PerShardSpawner};
use near_primitives::types::ShardId;

/// Test-loop spawner: registers each per-shard executor with the loop.
///
/// `register_actor` needs `&mut TestLoopData`, which a running actor's handler
/// never has, so `spawn` enqueues a deferred event that registers the actor and
/// binds the mailbox returned synchronously (a `LateBoundSender`). The
/// coordinator must not route to a freshly-spawned shard within the spawning
/// handler — it self-bootstraps from disk on `start_actor` instead. See
/// `notes/14-dynamic-actor-integration.md`.
pub struct TestLoopPerShardSpawner {
    pub pending_events_sender: PendingEventsSender,
    pub identifier: String,
    pub deps: PerShardDeps,
}

impl PerShardSpawner for TestLoopPerShardSpawner {
    fn spawn(
        &self,
        shard_id: ShardId,
        coordinator_sender: Sender<PerShardChunkApplied>,
    ) -> PerShardExecutorSender {
        let self_adapter = LateBoundSender::new();
        let actor = self.deps.build(shard_id, coordinator_sender, self_adapter.as_sender());
        // The coordinator's mailbox to this shard, bound once the register event runs.
        let actor_adapter = LateBoundSender::new();
        let mailbox = actor_adapter.as_multi_sender();
        let identifier = self.identifier.clone();
        self.pending_events_sender.send(
            format!("RegisterPerShardExecutor({shard_id})"),
            Box::new(move |data: &mut TestLoopData| {
                let handle = data.register_actor(&identifier, actor, Some(self_adapter));
                actor_adapter.bind(handle);
            }),
        );
        mailbox
    }

    fn retire(&self, _shard_id: ShardId) {
        // Test-loop: no teardown needed — an untracked shard self-drops in try_apply.
    }
}
