use crate::spice::chunk_executor_coordinator::PerShardChunkApplied;
use crate::spice::data_distributor_actor::SpiceDataDistributorAdapter;
use crate::spice::executor_shared::ChunkExecutorConfig;
use crate::spice::per_shard_executor::{PerShardExecutor, PerShardExecutorSender};
use near_async::ActorSystem;
use near_async::messaging::{IntoMultiSender, Sender};
use near_async::tokio::TokioRuntimeHandle;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::types::ShardId;
use near_store::{ShardUId, Store};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Abstracts how the coordinator creates and retires per-shard executors so the
/// same coordinator works in both runtimes: production spawns a dedicated tokio
/// actor per shard, while test-loop registers actors with the loop. See
/// `notes/14-dynamic-actor-integration.md`.
pub trait PerShardSpawner: Send + Sync + 'static {
    /// Build + run a `PerShardExecutor` for `shard_uid` and return the
    /// coordinator's mailbox to it. `coordinator_sender` is the coordinator's own
    /// `PerShardChunkApplied` sender (the cyclic dep). The coordinator computes the
    /// `ShardUId` from the spawning epoch's layout (see `PerShardExecutor::shard_uid`).
    fn spawn(
        &self,
        shard_uid: ShardUId,
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
    /// Scalar executor config (persistence flags + transaction validity period).
    pub config: ChunkExecutorConfig,
    pub runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub core_reader: SpiceCoreReader,
    pub validator_signer: MutableValidatorSigner,
    pub network_adapter: PeerManagerAdapter,
    pub core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    pub data_distributor_adapter: SpiceDataDistributorAdapter,
}

impl PerShardDeps {
    /// Build (but don't run) a `PerShardExecutor`, wiring the coordinator callback.
    pub fn build(
        &self,
        shard_uid: ShardUId,
        coordinator_sender: Sender<PerShardChunkApplied>,
    ) -> PerShardExecutor {
        PerShardExecutor::new(
            shard_uid,
            self.store.clone(),
            self.config.clone(),
            self.runtime_adapter.clone(),
            self.epoch_manager.clone(),
            self.core_reader.clone(),
            self.validator_signer.clone(),
            self.network_adapter.clone(),
            self.core_writer_sender.clone(),
            self.data_distributor_adapter.clone(),
            coordinator_sender,
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
        shard_uid: ShardUId,
        coordinator_sender: Sender<PerShardChunkApplied>,
    ) -> PerShardExecutorSender {
        let actor = self.deps.build(shard_uid, coordinator_sender);
        let handle = self.actor_system.spawn_tokio_actor(actor);
        self.handles.lock().insert(shard_uid.shard_id(), handle.clone());
        handle.into_multi_sender()
    }

    fn retire(&self, shard_id: ShardId) {
        if let Some(handle) = self.handles.lock().remove(&shard_id) {
            handle.stop();
        }
    }
}
