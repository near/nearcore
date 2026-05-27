use crate::spice::executor_shared::ExecutorIncomingUnverifiedReceipts;
use crate::spice::per_shard_executor::{IncomingReceipt, PerShardExecutorSender, ReceiptSource};
use crate::spice::per_shard_spawner::PerShardSpawner;
use lru::LruCache;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_chain::spice::core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::types::RuntimeAdapter;
use near_chain::{Block, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::ShardId;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::{ChainStoreAdapter, ChainStoreUpdateAdapter};
use near_store::{ShardUId, Store};
use std::collections::{HashMap, HashSet};
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
/// retires the per-shard executors (via the [`PerShardSpawner`]), fans block
/// events out to them, routes cross-shard receipts, and drives the disk-driven
/// execution-head advance. Wired only under the `protocol_feature_spice` feature.
pub struct ChunkExecutorCoordinator {
    chain_store: ChainStoreAdapter,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    /// Creates/retires per-shard executors; abstracts production (tokio actor)
    /// vs. test-loop (registered actor) — see [`PerShardSpawner`].
    spawner: Box<dyn PerShardSpawner>,
    /// The coordinator's own `PerShardChunkApplied` sender, handed to each
    /// spawned executor as its upstream channel.
    self_sender: Sender<PerShardChunkApplied>,
    /// Live per-shard mailboxes, grown/shrunk by `reconcile_tracked_shards`.
    mailboxes: HashMap<ShardId, PerShardExecutorSender>,
    /// Soft cross-shard sanity check: block -> first-seen bandwidth scheduler
    /// state hash. Not completion state; losing it on restart only skips the
    /// check (option (b) from the design).
    bandwidth_hash_cache: LruCache<CryptoHash, CryptoHash>,
}

impl ChunkExecutorCoordinator {
    pub fn new(
        store: Store,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        spawner: Box<dyn PerShardSpawner>,
        self_sender: Sender<PerShardChunkApplied>,
    ) -> Self {
        let chain_store = store.chain_store();
        Self {
            chain_store,
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            spawner,
            self_sender,
            mailboxes: HashMap::new(),
            bandwidth_hash_cache: LruCache::new(
                NonZeroUsize::new(BANDWIDTH_HASH_CACHE_CAP).unwrap(),
            ),
        }
    }

    /// Spawn executors for newly-tracked shards and retire ones no longer
    /// tracked, returning the shards spawned in this call (which self-bootstrap
    /// from disk and must NOT be routed to within the same handler — their
    /// test-loop mailbox isn't registered yet).
    fn reconcile_tracked_shards(
        &mut self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ShardId>, Error> {
        let desired: HashSet<ShardId> = self.spawned_shard_ids(parent_hash)?.into_iter().collect();
        let mut newly_spawned = Vec::new();
        for &shard_id in &desired {
            if !self.mailboxes.contains_key(&shard_id) {
                let mailbox = self.spawner.spawn(shard_id, self.self_sender.clone());
                self.mailboxes.insert(shard_id, mailbox);
                newly_spawned.push(shard_id);
            }
        }
        let gone: Vec<ShardId> =
            self.mailboxes.keys().copied().filter(|shard_id| !desired.contains(shard_id)).collect();
        for shard_id in gone {
            self.spawner.retire(shard_id);
            self.mailboxes.remove(&shard_id);
        }
        Ok(newly_spawned)
    }

    /// Reconcile the tracked-shard set for `block_hash`, then route the block to
    /// every shard that was already live (freshly-spawned shards self-bootstrap).
    fn dispatch_block(&mut self, block_hash: CryptoHash) -> Result<(), Error> {
        let parent_hash = *self.chain_store.get_block_header(&block_hash)?.prev_hash();
        let newly_spawned = self.reconcile_tracked_shards(&parent_hash)?;
        for (shard_id, mailbox) in &self.mailboxes {
            if !newly_spawned.contains(shard_id) {
                mailbox.send(ProcessedBlock { block_hash });
            }
        }
        Ok(())
    }

    /// Shards this node spawns an executor for at `parent_hash`'s epoch —
    /// currently *or in the next epoch*. The next-epoch shards are (for a node
    /// joining a shard at the boundary) the ones being state-synced + caught up:
    /// spawning their executor early lets it run catchup ahead of the boundary,
    /// and keeping it for the trailing epoch lets a departing shard finish.
    ///
    /// This is the spawn/retire set only. It must NOT be used to gate block-level
    /// finalization — a shard still state-syncing for the next epoch has no
    /// `ChunkExtra` for the current epoch and would stall the head. Use
    /// `executed_shard_ids` for that. (Compatibility with future spice state sync.)
    fn spawned_shard_ids(&self, parent_hash: &CryptoHash) -> Result<Vec<ShardId>, Error> {
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

    /// Shards this node *executes* at `parent_hash`'s epoch — current epoch only.
    /// This is the set block-level finalization gates on: a shard being synced for
    /// the next epoch is spawned (`spawned_shard_ids`) but not yet executed here,
    /// so it must not block the current epoch's head advance.
    fn executed_shard_ids(&self, parent_hash: &CryptoHash) -> Result<Vec<ShardId>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        Ok(self
            .epoch_manager
            .shard_ids(&epoch_id)?
            .into_iter()
            .filter(|&shard_id| self.shard_tracker.cares_about_shard(parent_hash, shard_id))
            .collect())
    }

    /// `ShardUId`s this node executes at `block_hash`'s epoch (the head-advance /
    /// finalization gate set — current epoch only, see `executed_shard_ids`).
    fn executed_shard_uids(&self, block_hash: &CryptoHash) -> Result<Vec<ShardUId>, Error> {
        let prev_hash = *self.chain_store.get_block_header(block_hash)?.prev_hash();
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        self.executed_shard_ids(&prev_hash)?
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
    fn advance_execution_head(&self) -> Result<(), Error> {
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
        for shard_uid in self.executed_shard_uids(block_hash)? {
            match self.chain_store.chunk_store().get_chunk_extra(block_hash, &shard_uid) {
                Ok(_) => {}
                Err(Error::DBNotFoundErr(_)) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    /// Adapter-native twin of `ChainUpdate::update_spice_final_execution_head`:
    /// reads the current SPICE final execution head, advances it (writing via
    /// `store_update`) when `block`'s last final block is higher, and returns the
    /// (possibly advanced) head. Forward-only by the height comparison.
    fn advance_spice_final_execution_head(
        &self,
        store_update: &mut ChainStoreUpdateAdapter<'static>,
        block: &Block,
    ) -> Result<Option<Tip>, Error> {
        let current = match self.chain_store.spice_final_execution_head() {
            Ok(head) => Some(Tip::clone(&head)),
            Err(Error::DBNotFoundErr(_)) => None,
            Err(err) => return Err(err),
        };
        if block.header().last_final_block() == &CryptoHash::default() {
            return Ok(current);
        }
        let last_final_header =
            self.chain_store.get_block_header(block.header().last_final_block())?;
        let advance = match &current {
            None => true,
            Some(head) => head.height < last_final_header.height(),
        };
        if advance {
            let tip = Tip::from_header(&last_final_header);
            store_update.set_spice_final_execution_head(&tip);
            return Ok(Some(tip));
        }
        Ok(current)
    }

    /// Idempotent block-level finalize: advance the spice heads, then (once a new
    /// final execution head lands) the flat-storage head and memtrie GC over this
    /// node's tracked shards. Per-shard artifacts are already on disk.
    fn block_level_finalize(&self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let mut store_update = self.chain_store.store_update();
        let final_execution_head =
            self.advance_spice_final_execution_head(&mut store_update, &block)?;
        // Forward-only; replaces the old unconditional save_spice_execution_head.
        store_update.set_spice_execution_head(&Tip::from_header(block.header()))?;
        store_update.commit()?;
        let Some(final_execution_head) = final_execution_head else {
            return Ok(());
        };
        let final_hash = final_execution_head.last_block_hash;
        let new_flat_head = final_execution_head.prev_block_hash;
        let prev_height = self.chain_store.get_block_header(&final_hash)?.prev_height();
        let flat_storage_manager = self.runtime_adapter.get_flat_storage_manager();
        let tries = self.runtime_adapter.get_tries();
        for shard_uid in self.executed_shard_uids(&final_hash)? {
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

    /// On start: spawn executors for the currently-tracked shards (they
    /// self-bootstrap from disk) and finalize any applied-but-not-finalized
    /// backlog. No block replay — each per-shard executor catches up on its own
    /// `start_actor`.
    fn bootstrap(&mut self) -> Result<(), Error> {
        let head = self.chain_store.head()?.last_block_hash;
        self.reconcile_tracked_shards(&head)?;
        self.advance_execution_head()
    }
}

impl Actor for ChunkExecutorCoordinator {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "chunk_executor", "ChunkExecutorCoordinator started");
        if let Err(err) = self.bootstrap() {
            tracing::error!(target: "chunk_executor", ?err, "coordinator bootstrap failed");
        }
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorCoordinator {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        if let Err(err) = self.dispatch_block(block_hash) {
            tracing::error!(target: "chunk_executor", ?err, %block_hash, "failed to dispatch processed block");
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
