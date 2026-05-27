use crate::spice::chunk_executor_actor::ExecutorIncomingUnverifiedReceipts;
use crate::spice::per_shard_executor::{IncomingReceipt, PerShardExecutorSender, ReceiptSource};
use lru::LruCache;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{Actor, CanSend, Handler};
use near_chain::spice::core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::types::{RuntimeAdapter, Tip};
use near_chain::{
    ChainGenesis, ChainStore, ChainStoreAccess, ChainUpdate, DoomslugThresholdMode, Error,
};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::ShardId;
use near_store::Store;
use std::collections::{HashMap, VecDeque};
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

/// Block-level half of the per-shard SPICE chunk-execution subsystem. Fans block
/// events out to the per-shard executors, routes cross-shard receipts, and drives
/// the disk-driven execution-head advance.
///
/// Prototype: the per-shard actors are spawned/registered up-front by the setup
/// site (which differs between a real node and test-loop) and handed in as
/// `mailboxes`; the coordinator does not spawn them. Dynamic shard reconcile
/// (spawn-early / retire-late) is deferred. Gated behind
/// [`super::SPICE_PER_SHARD_EXECUTOR`].
pub struct ChunkExecutorCoordinator {
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    mailboxes: HashMap<ShardId, PerShardExecutorSender>,
    /// Soft cross-shard sanity check: block -> first-seen bandwidth scheduler
    /// state hash. Not completion state; losing it on restart only skips the
    /// check (option (b) from the design).
    bandwidth_hash_cache: LruCache<CryptoHash, CryptoHash>,
}

impl ChunkExecutorCoordinator {
    pub fn new(
        store: Store,
        genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        mailboxes: HashMap<ShardId, PerShardExecutorSender>,
    ) -> Self {
        let chain_store = ChainStore::new(store, true, genesis.transaction_validity_period);
        Self {
            chain_store,
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            mailboxes,
            bandwidth_hash_cache: LruCache::new(
                NonZeroUsize::new(BANDWIDTH_HASH_CACHE_CAP).unwrap(),
            ),
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
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        for shard_id in self.tracked_shard_ids(&prev_hash)? {
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

    /// Bootstrap/catch-up: broadcast `IncomingBlock` for every block above the
    /// execution head so per-shard actors apply any backlog. Bounded by the
    /// execution-head-to-tip gap (small in steady state; non-trivial only after
    /// a restart with execution lagging). Already-applied blocks are dropped by
    /// the per-shard `AlreadyApplied` check.
    fn replay_blocks_above_execution_head(&self) {
        let head = match self.chain_store.spice_execution_head() {
            Ok(tip) => tip.last_block_hash,
            Err(Error::DBNotFoundErr(_)) => self.genesis_hash(),
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, "failed reading spice execution head for replay");
                return;
            }
        };
        let mut queue: VecDeque<CryptoHash> =
            self.chain_store.get_all_next_block_hashes(&head).into();
        while let Some(block_hash) = queue.pop_front() {
            for mailbox in self.mailboxes.values() {
                mailbox.send(ProcessedBlock { block_hash });
            }
            queue.extend(self.chain_store.get_all_next_block_hashes(&block_hash));
        }
    }
}

impl Actor for ChunkExecutorCoordinator {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "chunk_executor", "ChunkExecutorCoordinator started (prototype)");
        // Recover any applied-but-not-finalized backlog, then replay blocks above
        // the execution head so the per-shard actors re-derive readiness and
        // catch up (e.g. after a restart with execution lagging consensus).
        self.advance_execution_head();
        self.replay_blocks_above_execution_head();
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorCoordinator {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        for mailbox in self.mailboxes.values() {
            mailbox.send(ProcessedBlock { block_hash });
        }
    }
}

impl Handler<PerShardChunkApplied> for ChunkExecutorCoordinator {
    fn handle(&mut self, msg: PerShardChunkApplied) {
        let PerShardChunkApplied {
            block_hash,
            shard_id: _,
            outgoing_receipt_proofs,
            bandwidth_scheduler_state_hash,
        } = msg;
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
        // Wake local destination shards. The proofs are ALREADY on disk (sender
        // wrote them), so LocallyVerified is a wake-up only. Network distribution
        // to the DataDistributor is done (producer-gated) by the per-shard actor.
        for proof in &outgoing_receipt_proofs {
            let to_shard_id = proof.1.to_shard_id;
            if let Some(mailbox) = self.mailboxes.get(&to_shard_id) {
                mailbox.send(IncomingReceipt {
                    block_hash,
                    proof: proof.clone(),
                    source: ReceiptSource::LocallyVerified,
                });
            }
        }
        self.advance_execution_head();
    }
}

impl Handler<ExecutorIncomingUnverifiedReceipts> for ChunkExecutorCoordinator {
    fn handle(&mut self, msg: ExecutorIncomingUnverifiedReceipts) {
        let to_shard_id = msg.receipt_proof.1.to_shard_id;
        if let Some(mailbox) = self.mailboxes.get(&to_shard_id) {
            mailbox.send(IncomingReceipt {
                block_hash: msg.block_hash,
                proof: msg.receipt_proof,
                source: ReceiptSource::FromNetwork,
            });
        }
    }
}

impl Handler<ExecutionResultEndorsed> for ChunkExecutorCoordinator {
    fn handle(&mut self, ExecutionResultEndorsed { block_hash }: ExecutionResultEndorsed) {
        for mailbox in self.mailboxes.values() {
            mailbox.send(ExecutionResultEndorsed { block_hash });
        }
    }
}
