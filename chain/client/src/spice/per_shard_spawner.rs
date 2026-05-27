use crate::spice::chunk_executor_coordinator::PerShardChunkApplied;
use crate::spice::data_distributor_actor::SpiceDataDistributorAdapter;
use crate::spice::per_shard_executor::{PerShardExecutor, PerShardExecutorSender};
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, Sender};
use near_async::tokio::TokioRuntimeHandle;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::types::{NumBlocks, ShardId};
use near_store::Store;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Abstracts how the coordinator creates and retires per-shard executors so the
/// same coordinator works in both runtimes: production spawns a dedicated tokio
/// actor per shard, while test-loop registers actors with the loop. See
/// `notes/14-dynamic-actor-integration.md`.
pub trait PerShardSpawner: Send + Sync + 'static {
    /// Build + run a `PerShardExecutor` for `shard_id` and return the
    /// coordinator's mailbox to it. `coordinator_sender` is the coordinator's own
    /// `PerShardChunkApplied` sender (the cyclic dep).
    fn spawn(
        &self,
        shard_id: ShardId,
        coordinator_sender: Sender<PerShardChunkApplied>,
    ) -> PerShardExecutorSender;

    /// Retire a shard's executor (production stops its runtime; test-loop is a
    /// no-op — an untracked shard self-drops in `try_apply`).
    fn retire(&self, shard_id: ShardId);
}

/// Static read deps every per-shard executor needs, shared by all spawned
/// actors. Cloned into each `PerShardExecutor`.
#[derive(Clone)]
pub struct PerShardDeps {
    pub store: Store,
    pub transaction_validity_period: NumBlocks,
    /// Mirror the old `ChunkExecutorActor`: thread the client-config persistence
    /// flags into each shard's `ChainStore` so disabled writes stay disabled.
    pub save_trie_changes: bool,
    pub save_tx_outcomes: bool,
    pub save_receipt_to_tx: bool,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub core_reader: SpiceCoreReader,
    pub validator_signer: MutableValidatorSigner,
    pub network_adapter: PeerManagerAdapter,
    pub core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    pub data_distributor_adapter: SpiceDataDistributorAdapter,
}

impl PerShardDeps {
    /// Build (but don't run) a `PerShardExecutor`, wiring its self-sender and the
    /// coordinator callback.
    pub fn build(
        &self,
        shard_id: ShardId,
        coordinator_sender: Sender<PerShardChunkApplied>,
        myself_sender: Sender<crate::spice::per_shard_executor::AppliedContinue>,
    ) -> PerShardExecutor {
        PerShardExecutor::new(
            shard_id,
            self.store.clone(),
            self.transaction_validity_period,
            self.save_trie_changes,
            self.save_tx_outcomes,
            self.save_receipt_to_tx,
            self.runtime_adapter.clone(),
            self.epoch_manager.clone(),
            self.core_reader.clone(),
            self.validator_signer.clone(),
            self.network_adapter.clone(),
            self.core_writer_sender.clone(),
            self.data_distributor_adapter.clone(),
            coordinator_sender,
            myself_sender,
        )
    }
}

/// Production spawner: each shard runs as its own dedicated tokio actor.
pub struct TokioPerShardSpawner {
    actor_system: ActorSystem,
    deps: PerShardDeps,
    handles: Mutex<HashMap<ShardId, TokioRuntimeHandle<PerShardExecutor>>>,
}

impl TokioPerShardSpawner {
    pub fn new(actor_system: ActorSystem, deps: PerShardDeps) -> Self {
        Self { actor_system, deps, handles: Mutex::new(HashMap::new()) }
    }
}

impl PerShardSpawner for TokioPerShardSpawner {
    fn spawn(
        &self,
        shard_id: ShardId,
        coordinator_sender: Sender<PerShardChunkApplied>,
    ) -> PerShardExecutorSender {
        // The actor's AppliedContinue self-sender is late-bound to its own handle.
        let myself_adapter = LateBoundSender::new();
        let actor = self.deps.build(shard_id, coordinator_sender, myself_adapter.as_sender());
        let handle = self.actor_system.spawn_tokio_actor(actor);
        myself_adapter.bind(handle.clone());
        self.handles.lock().insert(shard_id, handle.clone());
        handle.into_multi_sender()
    }

    fn retire(&self, shard_id: ShardId) {
        if let Some(handle) = self.handles.lock().remove(&shard_id) {
            handle.stop();
        }
    }
}
