use crate::spice::chunk_executor_actor::{
    ChunkExecutorActorSender, IncomingReceipt, ReceiptSource,
};
use crate::spice::chunk_executor_spawner::ChunkExecutorSpawner;
use crate::spice::executor_shared::{ExecutorIncomingUnverifiedReceipts, optional};
use lru::LruCache;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::{Actor, CanSend, Handler, Sender};
use near_chain::Error;
use near_chain::spice::core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::types::RuntimeAdapter;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::ShardId;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{ShardUId, Store};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// A per-shard actor finished applying its chunk (and durably wrote its
/// artifacts). Sent to the coordinator for receipt routing, the cross-shard
/// sanity check, and to nudge the disk-driven head advance.
#[derive(Debug)]
pub struct ChunkApplied {
    pub block_hash: CryptoHash,
    pub shard_id: ShardId,
    pub outgoing_receipt_proofs: Vec<ReceiptProof>,
    pub bandwidth_scheduler_state_hash: CryptoHash,
}

/// Bound for the soft cross-shard sanity-check LRU. Skip-on-recovery, so a small
/// window is enough.
const BANDWIDTH_HASH_CACHE_CAP: usize = 256;

/// Block-level half of the per-shard SPICE chunk-execution subsystem. Spawns and
/// retires the per-shard executors (via the [`ChunkExecutorSpawner`]), fans block
/// events out to them, routes cross-shard receipts, and drives the disk-driven
/// execution-head advance. Wired only under the `protocol_feature_spice` feature.
pub struct ChunkExecutorCoordinatorActor {
    chain_store: ChainStoreAdapter,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    /// Creates/retires per-shard executors; abstracts production (tokio actor)
    /// vs. test-loop (registered actor) — see [`ChunkExecutorSpawner`].
    spawner: Box<dyn ChunkExecutorSpawner>,
    /// The coordinator's own `ChunkApplied` sender, handed to each
    /// spawned executor as its upstream channel.
    self_sender: Sender<ChunkApplied>,
    /// Live per-shard mailboxes, grown/shrunk by `reconcile_tracked_shards`.
    mailboxes: HashMap<ShardId, ChunkExecutorActorSender>,
    /// Soft cross-shard sanity check: block -> first-seen bandwidth scheduler
    /// state hash. Not completion state; losing it on restart only skips the
    /// check (option (b) from the design).
    bandwidth_hash_cache: LruCache<CryptoHash, CryptoHash>,
}

impl ChunkExecutorCoordinatorActor {
    pub fn new(
        store: Store,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        spawner: Box<dyn ChunkExecutorSpawner>,
        self_sender: Sender<ChunkApplied>,
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
        // Spawn an executor for every shard tracked this *or* next epoch: the
        // next-epoch shards (for a node joining at the boundary) are the ones being
        // state-synced + caught up, so spawning early lets their executor run catchup
        // ahead of the boundary, and keeping it for the trailing epoch lets a departing
        // shard finish. This spawn/retire set must NOT gate finalization — a shard
        // still syncing for next epoch has no current-epoch ChunkExtra and would stall
        // the head; `executed_shard_uids` (current epoch only) is the gate set.
        let desired: HashSet<ShardId> = self
            .shard_tracker
            .tracked_shard_ids_this_or_next_epoch(parent_hash)?
            .into_iter()
            .collect();
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        let mut newly_spawned = Vec::new();
        for &shard_id in &desired {
            if !self.mailboxes.contains_key(&shard_id) {
                let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?;
                let mailbox = self.spawner.spawn(shard_uid, self.self_sender.clone());
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

    /// `ShardUId`s this node *executes* at `block_hash`'s epoch — current epoch
    /// only. This is the head-advance / finalization gate set: a shard being synced
    /// for the next epoch is spawned (see `reconcile_tracked_shards`) but has no
    /// current-epoch `ChunkExtra`, so it must not block the head.
    fn executed_shard_uids(&self, block_hash: &CryptoHash) -> Result<Vec<ShardUId>, Error> {
        let prev_hash = *self.chain_store.get_block_header(block_hash)?.prev_hash();
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        self.shard_tracker
            .tracked_shard_ids(&prev_hash)?
            .into_iter()
            .map(|shard_id| Ok(shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?))
            .collect()
    }

    /// Disk-driven, idempotent. From the current execution head, finalize each
    /// next canonical block whose tracked shards are all applied on disk. The
    /// execution head is always set: initialized to the genesis tip at genesis (see
    /// `genesis.rs`) and advanced forward-only by `block_level_finalize`. A protocol
    /// upgrade that enables SPICE mid-chain must seed it as part of the upgrade.
    fn advance_execution_head(&self) -> Result<(), Error> {
        loop {
            let head = self.chain_store.spice_execution_head()?.last_block_hash;
            let Some(next) = optional(self.chain_store.get_next_block_hash(&head))? else {
                return Ok(());
            };
            if !self.all_tracked_shards_applied(&next)? {
                return Ok(());
            }
            self.block_level_finalize(&next)?;
        }
    }

    fn all_tracked_shards_applied(&self, block_hash: &CryptoHash) -> Result<bool, Error> {
        for shard_uid in self.executed_shard_uids(block_hash)? {
            let chunk_extra =
                optional(self.chain_store.chunk_store().get_chunk_extra(block_hash, &shard_uid))?;
            if chunk_extra.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Idempotent block-level finalize: advance the spice heads, then (once a new
    /// final execution head lands) the flat-storage head and memtrie GC over this
    /// node's tracked shards. Per-shard artifacts are already on disk.
    fn block_level_finalize(&self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let mut store_update = self.chain_store.store_update();
        let final_execution_head = store_update.update_spice_final_execution_head(&block)?;
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

impl Actor for ChunkExecutorCoordinatorActor {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        tracing::info!(target: "chunk_executor", "ChunkExecutorCoordinatorActor started");
        if let Err(err) = self.bootstrap() {
            tracing::error!(target: "chunk_executor", ?err, "coordinator bootstrap failed");
        }
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorCoordinatorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        if let Err(err) = self.dispatch_block(block_hash) {
            tracing::error!(target: "chunk_executor", ?err, %block_hash, "failed to dispatch processed block");
        }
    }
}

impl Handler<ChunkApplied> for ChunkExecutorCoordinatorActor {
    fn handle(&mut self, msg: ChunkApplied) {
        let ChunkApplied {
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

impl Handler<ExecutorIncomingUnverifiedReceipts> for ChunkExecutorCoordinatorActor {
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

impl Handler<ExecutionResultEndorsed> for ChunkExecutorCoordinatorActor {
    fn handle(&mut self, ExecutionResultEndorsed { block_hash }: ExecutionResultEndorsed) {
        for mailbox in self.mailboxes.values() {
            mailbox.send(ExecutionResultEndorsed { block_hash });
        }
    }
}
