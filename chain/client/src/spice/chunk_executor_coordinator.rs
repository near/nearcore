use crate::spice::per_shard_executor::{IncomingBlock, PerShardExecutor};
use lru::LruCache;
use near_async::ActorSystem;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{Actor, CanSend, Handler, IntoSender, LateBoundSender, Sender};
use near_async::tokio::TokioRuntimeHandle;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::ProcessedBlock;
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain::{
    ChainGenesis, ChainStore, ChainStoreAccess, ChainUpdate, DoomslugThresholdMode, Error,
};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::ShardId;
use near_store::Store;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// A per-shard actor finished applying its chunk (and durably wrote its
/// artifacts). Sent to the coordinator for receipt routing, the cross-shard
/// sanity check, and to nudge the disk-driven head advance.
#[derive(Debug)]
pub struct PerShardChunkApplied {
    pub block_hash: CryptoHash,
    pub shard_id: ShardId,
    pub outgoing_receipt_proofs: Vec<ReceiptProof>,
    pub bandwidth_scheduler_state_hash: CryptoHash,
}

/// Bound for the soft cross-shard sanity-check LRU. Skip-on-recovery, so a small
/// window is enough.
const BANDWIDTH_HASH_CACHE_CAP: usize = 256;

/// Block-level half of the per-shard SPICE chunk-execution subsystem. Spawns and
/// owns the per-shard `PerShardExecutor` actors, fans block events out to them,
/// and drives the disk-driven execution-head advance.
///
/// Gated behind [`super::SPICE_PER_SHARD_EXECUTOR`].
pub struct ChunkExecutorCoordinator {
    actor_system: ActorSystem,
    store: Store,
    genesis: ChainGenesis,
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    core_reader: SpiceCoreReader,
    validator_signer: MutableValidatorSigner,
    network_adapter: PeerManagerAdapter,
    core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    /// The coordinator's own `PerShardChunkApplied` sender, handed to each
    /// per-shard actor as its upstream channel (late-bound to this actor).
    self_sender: Sender<PerShardChunkApplied>,
    mailboxes: HashMap<ShardId, TokioRuntimeHandle<PerShardExecutor>>,
    /// Soft cross-shard sanity check: block -> first-seen bandwidth scheduler
    /// state hash. Not completion state; losing it on restart only skips the
    /// check (option (b) from the design).
    bandwidth_hash_cache: LruCache<CryptoHash, CryptoHash>,
}

impl ChunkExecutorCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_system: ActorSystem,
        store: Store,
        genesis: ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        core_reader: SpiceCoreReader,
        validator_signer: MutableValidatorSigner,
        network_adapter: PeerManagerAdapter,
        core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
        self_sender: Sender<PerShardChunkApplied>,
    ) -> Self {
        let chain_store = ChainStore::new(store.clone(), true, genesis.transaction_validity_period);
        Self {
            actor_system,
            store,
            genesis,
            chain_store,
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            core_reader,
            validator_signer,
            network_adapter,
            core_writer_sender,
            self_sender,
            mailboxes: HashMap::new(),
            bandwidth_hash_cache: LruCache::new(
                NonZeroUsize::new(BANDWIDTH_HASH_CACHE_CAP).unwrap(),
            ),
        }
    }

    /// Spawn a `PerShardExecutor` for `shard_id` on its own dedicated thread if
    /// not already live.
    fn ensure_shard_actor(&mut self, shard_id: ShardId) {
        if self.mailboxes.contains_key(&shard_id) {
            return;
        }
        // Per-actor late-bound self-sender for its AppliedContinue self-message.
        let myself_adapter = LateBoundSender::new();
        let actor = PerShardExecutor::new(
            shard_id,
            self.store.clone(),
            &self.genesis,
            self.runtime_adapter.clone(),
            self.epoch_manager.clone(),
            self.shard_tracker.clone(),
            self.core_reader.clone(),
            self.validator_signer.clone(),
            self.network_adapter.clone(),
            self.core_writer_sender.clone(),
            self.self_sender.clone(),
            myself_adapter.as_sender(),
        );
        let handle = self.actor_system.spawn_tokio_actor(actor);
        myself_adapter.bind(handle.clone());
        self.mailboxes.insert(shard_id, handle);
    }

    /// Spawn actors for every shard this node tracks at `parent_hash`'s epoch.
    /// Prototype: keyed on the processed block; with a static tracked set this
    /// reduces to "spawn all tracked shards".
    fn reconcile_shard_actors(&mut self, parent_hash: &CryptoHash) {
        let shard_ids = match self.tracked_shard_ids(parent_hash) {
            Ok(shard_ids) => shard_ids,
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, "failed to resolve tracked shards");
                return;
            }
        };
        for shard_id in shard_ids {
            self.ensure_shard_actor(shard_id);
        }
    }

    fn tracked_shard_ids(&self, parent_hash: &CryptoHash) -> Result<Vec<ShardId>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        let shard_ids = self.epoch_manager.shard_ids(&epoch_id)?;
        Ok(shard_ids
            .into_iter()
            .filter(|&shard_id| self.shard_tracker.cares_about_shard(parent_hash, shard_id))
            .collect())
    }

    /// Disk-driven, idempotent. From the current execution head, while the next
    /// canonical block has all tracked shards' `ChunkExtra` on disk, finalize it.
    fn advance_execution_head(&mut self) {
        loop {
            let head = match self.chain_store.spice_execution_head() {
                Ok(tip) => tip.last_block_hash,
                Err(Error::DBNotFoundErr(_)) => self.genesis_hash(),
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, "failed reading spice execution head");
                    return;
                }
            };
            let next = match self.chain_store.get_next_block_hash(&head) {
                Ok(next) => next,
                Err(Error::DBNotFoundErr(_)) => return,
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, %head, "failed reading next block hash");
                    return;
                }
            };
            match self.all_tracked_shards_applied(&next) {
                Ok(true) => {}
                Ok(false) => return,
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, %next, "failed checking applied shards");
                    return;
                }
            }
            if let Err(err) = self.block_level_finalize(&next) {
                tracing::error!(target: "chunk_executor", ?err, %next, "block_level_finalize failed");
                return;
            }
        }
    }

    fn all_tracked_shards_applied(&self, block_hash: &CryptoHash) -> Result<bool, Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let prev_hash = *block.header().prev_hash();
        for shard_id in self.tracked_shard_ids(&prev_hash)? {
            let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
            let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?;
            match self.chain_store.get_chunk_extra(block_hash, &shard_uid) {
                Ok(_) => {}
                Err(Error::DBNotFoundErr(_)) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    /// Idempotent block-level finalize. Advances the spice heads, flat-storage
    /// head, and memtrie GC. Per-shard artifacts are already on disk (written by
    /// the per-shard actors). Reuses `ChainUpdate` (prototype hack, consistent
    /// with the per-shard persist).
    fn block_level_finalize(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let mut chain_update = ChainUpdate::new(
            &mut self.chain_store,
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            DoomslugThresholdMode::NoApprovals,
        );
        let final_execution_head = chain_update.update_spice_final_execution_head(&block)?;
        chain_update.save_spice_execution_head(block.header())?;
        chain_update.commit()?;
        if let Some(final_execution_head) = final_execution_head {
            self.update_flat_storage_head(&shard_layout, &final_execution_head)?;
            self.gc_memtrie_roots(&shard_layout, &final_execution_head);
        }
        Ok(())
    }

    fn update_flat_storage_head(
        &self,
        shard_layout: &ShardLayout,
        final_execution_head: &Tip,
    ) -> Result<(), Error> {
        let new_flat_head = final_execution_head.prev_block_hash;
        if new_flat_head == CryptoHash::default() {
            return Ok(());
        }
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        for shard_uid in shard_layout.shard_uids() {
            if flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_none() {
                continue;
            }
            flat_storage_manager.update_flat_storage_for_shard(shard_uid, new_flat_head)?;
        }
        Ok(())
    }

    fn gc_memtrie_roots(&self, shard_layout: &ShardLayout, final_execution_head: &Tip) {
        let header = match self.chain_store.get_block_header(&final_execution_head.last_block_hash)
        {
            Ok(header) => header,
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, "missing final exec head header for memtrie gc");
                return;
            }
        };
        let Some(prev_height) = header.prev_height() else {
            return;
        };
        let tries = self.runtime_adapter.get_tries();
        for shard_uid in shard_layout.shard_uids() {
            tries.delete_memtrie_roots_up_to_height(shard_uid, prev_height);
        }
    }

    fn genesis_hash(&self) -> CryptoHash {
        // Prototype: walk back from the chain's final head to genesis. One-time
        // on a cold execution head; mirrors the old executor's bootstrap.
        let final_head = match self.chain_store.final_head() {
            Ok(tip) => tip,
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, "failed reading final head");
                return CryptoHash::default();
            }
        };
        let mut header = match self.chain_store.get_block_header(&final_head.last_block_hash) {
            Ok(header) => header,
            Err(_) => return CryptoHash::default(),
        };
        while !header.is_genesis() {
            header = match self.chain_store.get_block_header(header.prev_hash()) {
                Ok(header) => header,
                Err(_) => return CryptoHash::default(),
            };
        }
        *header.hash()
    }
}

impl Actor for ChunkExecutorCoordinator {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "chunk_executor", "ChunkExecutorCoordinator started (prototype)");
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorCoordinator {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        // Spawn actors for any newly-tracked shard, keyed on this block.
        let parent_hash = match self.chain_store.get_block_header(&block_hash) {
            Ok(header) => *header.prev_hash(),
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, %block_hash, "missing header for processed block");
                return;
            }
        };
        self.reconcile_shard_actors(&parent_hash);
        for handle in self.mailboxes.values() {
            handle.send(IncomingBlock { block_hash });
        }
    }
}

impl Handler<PerShardChunkApplied> for ChunkExecutorCoordinator {
    fn handle(&mut self, msg: PerShardChunkApplied) {
        let PerShardChunkApplied { block_hash, bandwidth_scheduler_state_hash, .. } = msg;
        // Soft cross-shard sanity check (option (b)): all tracked shards must
        // agree on the bandwidth scheduler state hash. Skip-on-recovery (the
        // cache is empty after restart).
        match self.bandwidth_hash_cache.get(&block_hash) {
            Some(first_seen) => assert_eq!(
                *first_seen, bandwidth_scheduler_state_hash,
                "bandwidth scheduler state diverged across shards for {block_hash}",
            ),
            None => {
                self.bandwidth_hash_cache.put(block_hash, bandwidth_scheduler_state_hash);
            }
        }
        // Receipt routing to local destination shards is Phase 4; for now just
        // nudge the disk-driven head advance.
        self.advance_execution_head();
    }
}
