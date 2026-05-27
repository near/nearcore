use crate::spice::chunk_executor_actor::ExecutorIncomingUnverifiedReceipts;
use crate::spice::per_shard_executor::{IncomingReceipt, PerShardExecutorSender, ReceiptSource};
use lru::LruCache;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{Actor, CanSend, Handler};
use near_chain::spice::core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::types::RuntimeAdapter;
use near_chain::{
    ChainGenesis, ChainStore, ChainStoreAccess, ChainUpdate, DoomslugThresholdMode, Error,
};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::ShardId;
use near_store::{ShardUId, Store};
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

    /// Shards this node tracks at `parent_hash`'s epoch — currently or in the
    /// next epoch, so a shard keeps being applied through the epoch where it
    /// stops being tracked.
    fn tracked_shard_ids(&self, parent_hash: &CryptoHash) -> Result<Vec<ShardId>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        Ok(self
            .epoch_manager
            .shard_ids(&epoch_id)?
            .into_iter()
            .filter(|&shard_id| {
                self.shard_tracker.cares_about_shard_this_or_next_epoch(parent_hash, shard_id)
            })
            .collect())
    }

    /// `ShardUId`s this node tracks at `block_hash`'s epoch.
    fn tracked_shard_uids(&self, block_hash: &CryptoHash) -> Result<Vec<ShardUId>, Error> {
        let prev_hash = *self.chain_store.get_block_header(block_hash)?.prev_hash();
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        self.tracked_shard_ids(&prev_hash)?
            .into_iter()
            .map(|shard_id| Ok(shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?))
            .collect()
    }

    /// Current execution head, or the genesis hash if execution hasn't started.
    fn execution_head_or_genesis(&self) -> Result<CryptoHash, Error> {
        match self.chain_store.spice_execution_head() {
            Ok(tip) => Ok(tip.last_block_hash),
            Err(Error::DBNotFoundErr(_)) => self.chain_store.genesis_hash(),
            Err(err) => Err(err),
        }
    }

    /// Disk-driven, idempotent. From the current execution head, finalize each
    /// next canonical block whose tracked shards are all applied on disk.
    fn advance_execution_head(&mut self) -> Result<(), Error> {
        loop {
            let head = self.execution_head_or_genesis()?;
            let next = match self.chain_store.get_next_block_hash(&head) {
                Ok(next) => next,
                Err(Error::DBNotFoundErr(_)) => return Ok(()),
                Err(err) => return Err(err),
            };
            if !self.all_tracked_shards_applied(&next)? {
                return Ok(());
            }
            self.block_level_finalize(&next)?;
        }
    }

    fn all_tracked_shards_applied(&self, block_hash: &CryptoHash) -> Result<bool, Error> {
        for shard_uid in self.tracked_shard_uids(block_hash)? {
            match self.chain_store.get_chunk_extra(block_hash, &shard_uid) {
                Ok(_) => {}
                Err(Error::DBNotFoundErr(_)) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    /// Idempotent block-level finalize: advance the spice heads, then (once a new
    /// final execution head lands) the flat-storage head and memtrie GC over this
    /// node's tracked shards. Per-shard artifacts are already on disk.
    fn block_level_finalize(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let mut chain_update = ChainUpdate::new(
            &mut self.chain_store,
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            DoomslugThresholdMode::NoApprovals,
        );
        let final_execution_head = chain_update.update_spice_final_execution_head(&block)?;
        chain_update.save_spice_execution_head(block.header())?;
        chain_update.commit()?;
        let Some(final_execution_head) = final_execution_head else {
            return Ok(());
        };
        let final_hash = final_execution_head.last_block_hash;
        let new_flat_head = final_execution_head.prev_block_hash;
        let prev_height = self.chain_store.get_block_header(&final_hash)?.prev_height();
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        let tries = self.runtime_adapter.get_tries();
        for shard_uid in self.tracked_shard_uids(&final_hash)? {
            if new_flat_head != CryptoHash::default()
                && flat_storage_manager.get_flat_storage_for_shard(shard_uid).is_some()
            {
                flat_storage_manager.update_flat_storage_for_shard(shard_uid, new_flat_head)?;
            }
            if let Some(prev_height) = prev_height {
                tries.delete_memtrie_roots_up_to_height(shard_uid, prev_height);
            }
        }
        Ok(())
    }

    /// Bootstrap/catch-up: broadcast `ProcessedBlock` for every block above the
    /// execution head so per-shard actors apply any backlog. Bounded by the
    /// execution-head-to-tip gap (small in steady state; non-trivial only after
    /// a restart with execution lagging). Already-applied blocks are dropped by
    /// the per-shard `AlreadyApplied` check.
    fn replay_blocks_above_execution_head(&self) -> Result<(), Error> {
        let head = self.execution_head_or_genesis()?;
        let mut queue: VecDeque<CryptoHash> =
            self.chain_store.get_all_next_block_hashes(&head).into();
        while let Some(block_hash) = queue.pop_front() {
            for mailbox in self.mailboxes.values() {
                mailbox.send(ProcessedBlock { block_hash });
            }
            queue.extend(self.chain_store.get_all_next_block_hashes(&block_hash));
        }
        Ok(())
    }
}

impl Actor for ChunkExecutorCoordinator {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "chunk_executor", "ChunkExecutorCoordinator started (prototype)");
        // Recover any applied-but-not-finalized backlog, then replay blocks above
        // the execution head so the per-shard actors re-derive readiness and
        // catch up (e.g. after a restart with execution lagging consensus).
        let result =
            self.advance_execution_head().and_then(|()| self.replay_blocks_above_execution_head());
        if let Err(err) = result {
            tracing::error!(target: "chunk_executor", ?err, "coordinator bootstrap failed");
        }
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
        // cache is empty after restart). On first report the inserted value
        // equals the reported one, so the assert is a no-op until a sibling
        // reports a different hash.
        let first_seen =
            *self.bandwidth_hash_cache.get_or_insert(block_hash, || bandwidth_scheduler_state_hash);
        assert_eq!(
            first_seen, bandwidth_scheduler_state_hash,
            "bandwidth scheduler state diverged across shards for {block_hash}",
        );
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
        if let Err(err) = self.advance_execution_head() {
            tracing::error!(target: "chunk_executor", ?err, "advance_execution_head failed");
        }
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
