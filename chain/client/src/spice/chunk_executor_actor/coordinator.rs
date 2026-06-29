//! The chunk-executor coordinator: owns the per-shard executor registry, routes
//! upstream events to the per-shard executors, and runs the disk-driven
//! execution-head advance / finalize.

use super::per_shard::{PerShardChunkExecutor, PerShardDeps};
use crate::spice::data_distributor_actor::SpiceDataDistributorAdapter;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{Handler, Sender};
use near_chain::spice::block_application::apply_block_postprocessing;
use near_chain::spice::chunk_application::ChunkPersistenceConfig;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::types::RuntimeAdapter;
use near_chain::update_shard::ShardUpdateResult;
use near_chain::{ChainGenesis, Error};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::{NumBlocks, ShardId};
use near_store::ShardUId;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tracing::instrument;

pub struct ChunkExecutorActor {
    pub(crate) chain_store: ChainStoreAdapter,
    transaction_validity_period: NumBlocks,
    pub(crate) runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub(crate) epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) shard_tracker: ShardTracker,
    network_adapter: PeerManagerAdapter,
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    myself_sender: Sender<ExecutorApplyChunksDone>,
    pub(crate) core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    data_distributor_adapter: SpiceDataDistributorAdapter,
    config: ChunkPersistenceConfig,

    /// One executor per tracked shard, reconciled on every block. Keyed by
    /// `ShardUId` so per-shard state can't be conflated across shard layouts.
    /// Each owns its own network-receipt buffer (no coordinator-side receipt state).
    per_shard_executors: HashMap<ShardUId, PerShardChunkExecutor>,

    pub(crate) validator_signer: MutableValidatorSigner,
    pub(crate) core_reader: SpiceCoreReader,
}

/// Message with incoming unverified receipts corresponding to the block.
#[derive(Debug, PartialEq)]
pub struct ExecutorIncomingUnverifiedReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proof: ReceiptProof,
}

#[derive(Debug)]
pub struct ExecutorApplyChunksDone {
    pub block_hash: CryptoHash,
    pub shard_uid: ShardUId,
    pub apply_result: Result<ShardUpdateResult, FailedToApplyChunkError>,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to apply chunk")]
pub struct FailedToApplyChunkError {
    shard_id: ShardId,
    err: Error,
}

impl FailedToApplyChunkError {
    pub(crate) fn new(shard_id: ShardId, err: Error) -> Self {
        Self { shard_id, err }
    }
}

impl ChunkExecutorActor {
    pub fn new(
        store: Store,
        genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        network_adapter: PeerManagerAdapter,
        validator_signer: MutableValidatorSigner,
        apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
        myself_sender: Sender<ExecutorApplyChunksDone>,
        core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
        data_distributor_adapter: SpiceDataDistributorAdapter,
        config: ChunkPersistenceConfig,
    ) -> Self {
        let core_reader =
            SpiceCoreReader::new(store.chain_store(), epoch_manager.clone(), genesis.gas_limit);
        // Spice reads/writes only through adapters; the `with_save_*` flags on
        // `ChainStore` gate its own write methods, which spice never calls
        // (writes go through `apply_chunk_postprocessing` with `ChunkPersistenceConfig`).
        let chain_store = store.chain_store();
        Self {
            chain_store,
            transaction_validity_period: genesis.transaction_validity_period,
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            network_adapter,
            apply_chunks_spawner,
            myself_sender,
            per_shard_executors: HashMap::new(),
            validator_signer,
            core_reader,
            data_distributor_adapter,
            core_writer_sender,
            config,
        }
    }

    /// Snapshot of the construction-only deps shared with each per-shard executor.
    fn per_shard_deps(&self) -> PerShardDeps {
        PerShardDeps {
            runtime_adapter: self.runtime_adapter.clone(),
            epoch_manager: self.epoch_manager.clone(),
            network_adapter: self.network_adapter.clone(),
            core_writer_sender: self.core_writer_sender.clone(),
            data_distributor_adapter: self.data_distributor_adapter.clone(),
            config: self.config.clone(),
            validator_signer: self.validator_signer.clone(),
            core_reader: self.core_reader.clone(),
        }
    }

    /// Lazily construct the executor for `shard_uid`, cloning a `ChainStoreAdapter`
    /// and the shared deps from the coordinator only when the executor is absent
    /// (the hot reconcile path hits the already-present branch and clones nothing).
    fn get_or_create_per_shard_executor(
        &mut self,
        shard_uid: ShardUId,
    ) -> &mut PerShardChunkExecutor {
        if !self.per_shard_executors.contains_key(&shard_uid) {
            let executor = PerShardChunkExecutor::new(
                shard_uid,
                self.chain_store.clone(),
                self.per_shard_deps(),
                self.transaction_validity_period,
                self.apply_chunks_spawner.clone(),
                self.myself_sender.clone(),
            );
            self.per_shard_executors.insert(shard_uid, executor);
        }
        self.per_shard_executors.get_mut(&shard_uid).expect("inserted above when absent")
    }

    /// Find the executor that owns `shard_id`. Receipt routing identifies the
    /// destination shard by `ShardId` (from the receipt proof), but executors are
    /// keyed by `ShardUId`; within a single shard layout the two are 1:1.
    /// (Tracking multiple layouts at once — during resharding — is a later phase.)
    fn executor_for_shard_id(&mut self, shard_id: ShardId) -> Option<&mut PerShardChunkExecutor> {
        self.per_shard_executors
            .values_mut()
            .find(|executor| executor.shard_uid().shard_id() == shard_id)
    }

    /// Spawn executors for shards tracked this or next epoch and evict ones no
    /// longer tracked. The this-or-next-epoch set matches the
    /// `should_apply_chunk(IsCaughtUp, ..)` gate the monolithic executor used:
    /// a shard rotating in next epoch must keep executing through the boundary
    /// so it can endorse, otherwise certification stalls at the rotation.
    /// Eviction is immediate; the apply-done handler's graceful-drop guard covers
    /// any in-flight apply that lands after eviction.
    fn reconcile_tracked_shards(&mut self, prev_hash: &CryptoHash) -> Result<(), Error> {
        let tracked = self.shard_tracker.tracked_shard_uids_this_or_next_epoch(prev_hash)?;
        for shard_uid in &tracked {
            self.get_or_create_per_shard_executor(*shard_uid);
        }
        let tracked_set: HashSet<ShardUId> = tracked.iter().copied().collect();
        self.per_shard_executors.retain(|shard_uid, _| tracked_set.contains(shard_uid));
        Ok(())
    }

    /// Route a processed block: reconcile the tracked-shard set, then hand the
    /// block to every tracked shard's executor (each drains its own receipt
    /// buffer for the parent block).
    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%block_hash))]
    pub(crate) fn handle_processed_block(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let prev_block_hash = *block.header().prev_hash();
        self.reconcile_tracked_shards(&prev_block_hash)?;
        for executor in self.per_shard_executors.values_mut() {
            executor.handle_processed_block(&block);
        }
        // A node tracking zero shards schedules no applies, so the apply-done
        // finalize trigger never fires; finalize here instead.
        if self.all_tracked_shards_applied(block_hash)? {
            self.finalize_block(block_hash)?;
        }
        Ok(())
    }

    /// Route an apply-done callback to its source shard and return the outgoing
    /// receipt proofs it produced (for local-path fanout). A missing executor
    /// means the shard was evicted while an apply was in flight: log and drop.
    pub(crate) fn per_shard_apply_done(
        &mut self,
        msg: ExecutorApplyChunksDone,
    ) -> Vec<ReceiptProof> {
        let ExecutorApplyChunksDone { block_hash, shard_uid, apply_result } = msg;
        let Some(executor) = self.per_shard_executors.get_mut(&shard_uid) else {
            tracing::debug!(target: "chunk_executor", %block_hash, ?shard_uid, "apply done for untracked shard; dropping");
            return Vec::new();
        };
        match executor.handle_apply_chunks_done(block_hash, apply_result) {
            Ok(proofs) => {
                // Re-drive this shard's own queue: the just-applied block can make
                // its next-height successor (same shard) ready. Cross-shard wakeup
                // is handled by the fanout in `coordinator_post_apply`.
                executor.try_apply_pending();
                proofs
            }
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, %block_hash, ?shard_uid, "per-shard apply postprocessing failed");
                Vec::new()
            }
        }
    }

    /// After a shard applies, wake local destination shards (their incoming
    /// receipts are now on disk — local-path fanout), then finalize the block once
    /// all its tracked shards are applied.
    pub(crate) fn coordinator_post_apply(
        &mut self,
        block_hash: &CryptoHash,
        outgoing_proofs: &[ReceiptProof],
    ) -> Result<(), Error> {
        let destinations: HashSet<ShardId> =
            outgoing_proofs.iter().map(|proof| proof.1.to_shard_id).collect();
        for to_shard_id in destinations {
            if let Some(executor) = self.executor_for_shard_id(to_shard_id) {
                executor.handle_local_chunk_applied();
            }
        }
        // Each tracked shard's apply-done lands here; finalize the block once all of
        // them are applied. `finalize_block` is idempotent, so being driven here
        // repeatedly is harmless.
        if self.all_tracked_shards_applied(block_hash)? {
            self.finalize_block(block_hash)?;
        }
        Ok(())
    }

    /// Disk-driven head recovery: walk canonical-next from `spice_execution_head`
    /// and finalize each block whose tracked shards all have `ChunkExtra` on
    /// disk. Used on startup to recover from a crash between apply-commit and
    /// finalize; the hot path finalizes directly in `coordinator_post_apply`.
    fn advance_execution_head(&mut self) -> Result<(), Error> {
        loop {
            let head = self.chain_store.spice_execution_head()?.last_block_hash;
            let next = match self.chain_store.get_next_block_hash(&head) {
                Ok(hash) => hash,
                Err(Error::DBNotFoundErr(_)) => return Ok(()),
                Err(err) => return Err(err),
            };
            // `NextBlockHashes` is populated when headers are processed, so the
            // block content for `next` may not be on disk yet. Stop walking
            // until it arrives.
            match self.chain_store.get_block(&next) {
                Ok(_) => {}
                Err(Error::DBNotFoundErr(_)) => return Ok(()),
                Err(err) => return Err(err),
            };
            if !self.all_tracked_shards_applied(&next)? {
                return Ok(());
            }
            self.finalize_block(&next)?;
        }
    }

    fn finalize_block(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        apply_block_postprocessing(
            self.runtime_adapter.as_ref(),
            self.epoch_manager.as_ref(),
            &self.chain_store,
            &block,
        )?;
        // The final execution head may have advanced; drop receipts buffered
        // against source blocks at or below it — they can never be applied.
        for executor in self.per_shard_executors.values_mut() {
            executor.prune_unverified_receipts_below_final_head()?;
        }
        Ok(())
    }

    fn all_tracked_shards_applied(&self, block_hash: &CryptoHash) -> Result<bool, Error> {
        let prev_hash = *self.chain_store.get_block_header(block_hash)?.prev_hash();
        let chunk_store = self.chain_store.chunk_store();
        // Gate on shards tracked THIS epoch only: a shard tracked solely for the
        // next epoch has no current-epoch chunk extra and would stall the head.
        for shard_uid in self.shard_tracker.tracked_shard_uids(&prev_hash)? {
            match chunk_store.get_chunk_extra(block_hash, &shard_uid) {
                Ok(_) => {}
                Err(Error::DBNotFoundErr(_)) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }

    fn process_all_ready_blocks(&mut self) -> Result<(), Error> {
        let start_block = self.chain_store.spice_final_execution_head()?.last_block_hash;

        // Park every block above the final execution head into its tracked shards'
        // executors and try to drain. A freshly-started node catches up from disk;
        // blocks not yet ready stay parked and are re-driven by apply-done fanout,
        // incoming receipts, and chunk execution result endorsements.
        let mut next_block_hashes: VecDeque<_> =
            self.chain_store.get_all_next_block_hashes(&start_block).into();
        while let Some(block_hash) = next_block_hashes.pop_front() {
            // A single bad block must not abort startup recovery for the whole chain;
            // log and continue. Normal runtime triggers (incoming receipts, chunk execution result
            // endorsements, apply-done fanout) re-drive it later.
            if let Err(err) = self.handle_processed_block(&block_hash) {
                tracing::error!(target: "chunk_executor", ?err, %block_hash, "failed to process block during startup recovery; skipping");
            }
            next_block_hashes.extend(&self.chain_store.get_all_next_block_hashes(&block_hash));
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn pending_receipts_count(&self) -> usize {
        // Sum across per-shard trackers. Matches the old coordinator-side count
        // under a single-shard regime; may overcount under multi-shard since each
        // shard's tracker keys by source block independently.
        self.per_shard_executors.values().map(|executor| executor.buffered_receipts_count()).sum()
    }
}

impl near_async::messaging::Actor for ChunkExecutorActor {
    fn start_actor(&mut self, _ctx: &mut dyn near_async::futures::DelayedActionRunner<Self>) {
        if !cfg!(feature = "protocol_feature_spice") {
            return;
        }
        // Recover after a crash between apply-commit and finalize: walk forward
        // past anything already-applied on disk before scheduling fresh applies.
        // Idempotent — a clean restart with no pending finalize is a no-op.
        if let Err(err) = self.advance_execution_head() {
            tracing::error!(
                target: "chunk_executor",
                ?err,
                "failed to advance execution head on startup",
            );
        }
        if let Err(err) = self.process_all_ready_blocks() {
            tracing::error!(
                target: "chunk_executor",
                ?err,
                "failed when trying to process all ready blocks on startup",
            );
        }
    }
}

impl Handler<ExecutorIncomingUnverifiedReceipts> for ChunkExecutorActor {
    fn handle(&mut self, receipts: ExecutorIncomingUnverifiedReceipts) {
        let ExecutorIncomingUnverifiedReceipts { block_hash, receipt_proof } = receipts;
        tracing::debug!(
            target: "chunk_executor",
            %block_hash,
            ?receipt_proof,
            "received receipts",
        );
        // Route to the destination shard's executor, which owns the buffer for
        // receipts addressed to it.
        let to_shard_id = receipt_proof.1.to_shard_id;
        // TODO(spice-resharding): a receipt for a shard this node *does* track can be
        // dropped here if it arrives before reconcile created the executor (startup /
        // catch-up, or around an epoch boundary). Reconcile from the source block's
        // parent and retry the lookup before treating the shard as untracked.
        let Some(executor) = self.executor_for_shard_id(to_shard_id) else {
            tracing::debug!(target: "chunk_executor", %block_hash, ?to_shard_id, "receipt for untracked shard; dropping");
            return;
        };
        if let Err(err) = executor.handle_incoming_receipt(block_hash, receipt_proof) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed while handling incoming receipt");
        }
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorActor {
    // TODO(spice): Implement pub(crate) handle functions in ChunkExecutorActor that would return
    // errors/results and use them in tests to make sure we are testing correct errors.
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        if let Err(err) = self.handle_processed_block(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to process block");
        }
    }
}

impl Handler<ExecutionResultEndorsed> for ChunkExecutorActor {
    fn handle(&mut self, ExecutionResultEndorsed { block_hash }: ExecutionResultEndorsed) {
        // A landed chunk execution result lets each shard verify receipts buffered
        // against this source block and unblock parked children gated on it.
        for executor in self.per_shard_executors.values_mut() {
            if let Err(err) = executor.handle_execution_result_endorsed(&block_hash) {
                tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed while handling execution result endorsed");
            }
        }
    }
}

impl Handler<ExecutorApplyChunksDone> for ChunkExecutorActor {
    fn handle(&mut self, msg: ExecutorApplyChunksDone) {
        let block_hash = msg.block_hash;
        let outgoing_proofs = self.per_shard_apply_done(msg);
        if let Err(err) = self.coordinator_post_apply(&block_hash, &outgoing_proofs) {
            tracing::error!(target:"chunk_executor", ?err, ?block_hash, "failed to handle apply chunks done");
        }
    }
}
