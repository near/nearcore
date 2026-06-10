use crate::spice::chunk_validator_actor::send_spice_chunk_endorsement;
use crate::spice::data_distributor_actor::SpiceDataDistributorAdapter;
use crate::spice::data_distributor_actor::SpiceDistributorOutgoingReceipts;
use crate::spice::data_distributor_actor::SpiceDistributorStateWitness;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_chain::BlockHeader;
use near_chain::chain::{NewChunkData, NewChunkResult, ShardContext, StorageContext};
use near_chain::sharding::get_receipts_shuffle_salt;
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::spice::block_application::apply_block_postprocessing;
use near_chain::spice::chunk_application::{
    ChunkPersistenceConfig, apply_chunk_postprocessing, build_spice_apply_chunk_block_context,
};
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::ExecutionResultEndorsed;
use near_chain::spice::core_writer_actor::ProcessedBlock;
use near_chain::types::ApplyChunkResult;
use near_chain::types::{ApplyChunkBlockContext, RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::{ShardUpdateReason, ShardUpdateResult, process_shard_update};
use near_chain::{
    Block, Chain, ChainGenesis, Error, collect_receipts, compute_transaction_validity,
    get_chunk_clone_from_header,
};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardProof;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::SpiceChunkStateTransition;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::spice::state_witness::compute_contract_accesses_hash;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{CodeHash, ContractUpdates};
use near_primitives::types::BlockExecutionResults;
use near_primitives::types::BlockHeight;
use near_primitives::types::ChunkExecutionResult;
use near_primitives::types::ChunkExecutionResultHash;
use near_primitives::types::NumBlocks;
use near_primitives::types::SpiceChunkId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{Gas, ShardId};
use near_primitives::utils::get_contract_accesses_key;
use near_primitives::utils::get_receipt_proof_key;
use near_primitives::utils::get_receipt_proof_target_shard_prefix;
use near_primitives::utils::get_witnesses_key;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::DBCol;
use near_store::ShardUId;
use near_store::Store;
use near_store::StoreUpdate;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::instrument;

/// Data required for validators to initiate the chunk application
struct ChunkExecutionData {
    pub witness: SpiceChunkStateWitness,
    pub code_accesses: HashSet<CodeHash>,
}

/// Construction-only dependencies shared with every per-shard executor. Bundled
/// so `PerShardChunkExecutor::new` takes one argument instead of eight; the
/// coordinator keeps its own clones (the apply path stays coordinator-side for now).
#[derive(Clone)]
pub(crate) struct PerShardDeps {
    pub(crate) runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub(crate) epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) network_adapter: PeerManagerAdapter,
    pub(crate) core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    pub(crate) data_distributor_adapter: SpiceDataDistributorAdapter,
    pub(crate) config: ChunkPersistenceConfig,
    pub(crate) validator_signer: MutableValidatorSigner,
    pub(crate) core_reader: SpiceCoreReader,
}

/// Per-tracked-shard executor. This change lands the chassis and lifecycle; the
/// apply path, sinks, and receipt tracker migrate here in follow-up changes, which
/// wire in these fields and drop the suppression.
#[allow(dead_code)]
pub(crate) struct PerShardChunkExecutor {
    shard_uid: ShardUId,
    chain_store: ChainStoreAdapter,
    transaction_validity_period: NumBlocks,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_adapter: PeerManagerAdapter,
    core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    data_distributor_adapter: SpiceDataDistributorAdapter,
    config: ChunkPersistenceConfig,
    validator_signer: MutableValidatorSigner,
    core_reader: SpiceCoreReader,
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    apply_done_sender: Sender<ExecutorApplyChunksDone>,
    blocks_in_execution: HashSet<CryptoHash>,
    parked_blocks: BTreeSet<(BlockHeight, CryptoHash)>,
    unverified_receipts: HashMap<CryptoHash, Vec<ReceiptProof>>,
}

impl PerShardChunkExecutor {
    fn new(
        shard_uid: ShardUId,
        chain_store: ChainStoreAdapter,
        deps: PerShardDeps,
        transaction_validity_period: NumBlocks,
        apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
        apply_done_sender: Sender<ExecutorApplyChunksDone>,
    ) -> Self {
        let PerShardDeps {
            runtime_adapter,
            epoch_manager,
            network_adapter,
            core_writer_sender,
            data_distributor_adapter,
            config,
            validator_signer,
            core_reader,
        } = deps;
        Self {
            shard_uid,
            chain_store,
            transaction_validity_period,
            runtime_adapter,
            epoch_manager,
            network_adapter,
            core_writer_sender,
            data_distributor_adapter,
            config,
            validator_signer,
            core_reader,
            apply_chunks_spawner,
            apply_done_sender,
            blocks_in_execution: HashSet::new(),
            parked_blocks: BTreeSet::new(),
            unverified_receipts: HashMap::new(),
        }
    }
}

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
    per_shard_executors: HashMap<ShardUId, PerShardChunkExecutor>,

    /// Coordinator-side network-receipt buffer; not yet partitioned per shard.
    pending_unverified_receipts: HashMap<CryptoHash, Vec<ExecutorIncomingUnverifiedReceipts>>,

    pub(crate) validator_signer: MutableValidatorSigner,
    pub(crate) core_reader: SpiceCoreReader,
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
            pending_unverified_receipts: HashMap::new(),
        }
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

/// Message with incoming unverified receipts corresponding to the block.
#[derive(Debug, PartialEq)]
pub struct ExecutorIncomingUnverifiedReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proof: ReceiptProof,
}

struct VerifiedReceipts {
    receipt_proof: ReceiptProof,
    block_hash: CryptoHash,
}

enum ReceiptVerificationContext {
    NotReady,
    Ready { execution_results: HashMap<ShardId, Arc<ChunkExecutionResult>> },
}

impl ExecutorIncomingUnverifiedReceipts {
    /// Returns VerifiedReceipts iff receipts are valid.
    /// execution_results should contain results for all shards of the relevant block.
    fn verify(
        self,
        execution_results: &HashMap<ShardId, Arc<ChunkExecutionResult>>,
    ) -> Result<VerifiedReceipts, Error> {
        let Some(execution_result) = execution_results.get(&self.receipt_proof.1.from_shard_id)
        else {
            debug_assert!(false, "execution results missing results when verifying receipts");
            tracing::error!(
                target: "chunk_executor",
                from_shard_id=?self.receipt_proof.1.from_shard_id,
                "execution results missing results when verifying receipts"
            );
            return Err(Error::InvalidShardId(self.receipt_proof.1.from_shard_id));
        };
        if !self.receipt_proof.verify_against_receipt_root(execution_result.outgoing_receipts_root)
        {
            return Err(Error::InvalidReceiptsProof);
        }
        Ok(VerifiedReceipts { receipt_proof: self.receipt_proof, block_hash: self.block_hash })
    }
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

/// Outcome of a per-shard `try_apply` attempt for a single parked block.
enum ApplyOutcome {
    /// Dispatched to the apply spawner (or already in flight).
    Scheduled,
    /// Block is no longer relevant for this shard (past the final execution
    /// head, not in this shard's layout, or already applied on disk).
    Dropped,
    /// Inputs not yet on disk (prev CER, prev chunk extra, or receipts missing).
    /// Re-driven by a later receipt / endorsement / sibling apply.
    NotReady,
}

impl Handler<ExecutorIncomingUnverifiedReceipts> for ChunkExecutorActor {
    fn handle(&mut self, receipts: ExecutorIncomingUnverifiedReceipts) {
        let block_hash = receipts.block_hash;
        tracing::debug!(
            target: "chunk_executor",
            %block_hash,
            receipt_proofs=?receipts.receipt_proof,
            "received receipts",
        );
        self.pending_unverified_receipts.entry(block_hash).or_default().push(receipts);
        // Some additional receipts can arrive after we have already started executing the block.
        // (For example receiving receipts from push/pull code paths.) In this case we want to make
        // sure we process pending receipts to make sure don't leak memory by storing unverified
        // receipts indefinitely.
        // The receipt buffer is still coordinator-side (not yet partitioned per shard).
        if let Err(err) = self.try_process_pending_unverified_receipts(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed while trying to save pending unverified receipts");
        }
        self.drive_all_pending();
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
        // The unverified-receipt buffer is still coordinator-side; drain it for this
        // source block, then re-drive every shard's parked queue (a landed CER can
        // unblock parked children).
        if let Err(err) = self.try_process_pending_unverified_receipts(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed while processing pending unverified receipts");
        }
        self.drive_all_pending();
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

struct ChunkApplicationContext<'a> {
    incoming_receipts: Vec<ReceiptProof>,
    shard_uid: ShardUId,
    prev_chunk_chunk_extra: Arc<ChunkExtra>,
    chunk_header: &'a ShardChunkHeader,
}

impl ChunkExecutorActor {
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

    /// Route a processed block: reconcile the tracked-shard set, drain the
    /// coordinator-side receipt buffer for the parent (not yet partitioned per
    /// shard), then hand the block to every tracked shard's executor.
    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%block_hash))]
    fn handle_processed_block(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        let prev_block_hash = *block.header().prev_hash();
        self.reconcile_tracked_shards(&prev_block_hash)?;
        // TODO(spice): refactor try_process_pending_unverified_receipts to take prev_block_execution_results as argument.
        if let Err(err) = self.try_process_pending_unverified_receipts(&prev_block_hash) {
            tracing::error!(target: "chunk_executor", ?err, %block_hash, "failure when processing pending unverified receipts");
            return Err(err);
        }
        for executor in self.per_shard_executors.values_mut() {
            executor.handle_processed_block(&block);
        }
        Ok(())
    }

    /// Re-drive every tracked shard's parked queue — used when a receipt or CER
    /// lands that could unblock parked blocks across shards.
    fn drive_all_pending(&mut self) {
        for executor in self.per_shard_executors.values_mut() {
            executor.try_apply_pending();
        }
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

    /// Route an apply-done callback to its source shard and return the outgoing
    /// receipt proofs it produced (for local-path fanout). A missing executor
    /// means the shard was evicted while an apply was in flight: log and drop.
    fn per_shard_apply_done(&mut self, msg: ExecutorApplyChunksDone) -> Vec<ReceiptProof> {
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
    /// receipts are now on disk — local-path fanout), then finalize the block if
    /// all its tracked shards are applied. The dedup guard skips redundant
    /// finalize attempts from the N per-shard arrivals for one block.
    fn coordinator_post_apply(
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
        let block = self.chain_store.get_block(block_hash)?;
        // N shards finishing one block each call this; finalize once. Correct only
        // while the apply-done handler runs on the single coordinator actor mailbox.
        if self.chain_store.spice_execution_head()?.height >= block.header().height() {
            return Ok(());
        }
        if self.all_tracked_shards_applied(block_hash)? {
            self.finalize_block(block_hash)?;
        }
        Ok(())
    }
}

impl PerShardChunkExecutor {
    /// The layout-versioned identity of the shard this executor owns. Receipt
    /// routing (which starts from a `ShardId`) resolves back to the owning
    /// executor through this; the coordinator keys its registry by `ShardUId`.
    pub(crate) fn shard_uid(&self) -> ShardUId {
        self.shard_uid
    }

    /// Park `block` and try to drain — the per-shard half of the coordinator
    /// routing a processed block to this shard.
    fn handle_processed_block(&mut self, block: &Block) {
        self.parked_blocks.insert((block.header().height(), *block.hash()));
        self.try_apply_pending();
    }

    /// Local-path fanout entry: a sibling shard's apply produced receipts for
    /// this shard (already on disk), so re-check the parked queue.
    fn handle_local_chunk_applied(&mut self) {
        self.try_apply_pending();
    }

    /// Apply every parked block that is ready, lowest height first. Applying a
    /// block can make the next height ready, and a higher fork block can be ready
    /// while a lower one waits, so we scan past `NotReady`. Cross-shard
    /// dependencies are re-driven by the next receipt / endorsement / sibling apply.
    fn try_apply_pending(&mut self) {
        let parked: Vec<(BlockHeight, CryptoHash)> = self.parked_blocks.iter().copied().collect();
        for entry in parked {
            match self.try_apply(&entry.1) {
                Ok(ApplyOutcome::NotReady) => continue,
                Ok(ApplyOutcome::Scheduled | ApplyOutcome::Dropped) => {
                    self.parked_blocks.remove(&entry);
                }
                Err(err) => {
                    // Leave this block parked (a transient error may clear) but keep
                    // draining the rest of the queue — one bad block must not strand
                    // ready siblings.
                    tracing::error!(target: "chunk_executor", ?err, block_hash=%entry.1, shard_id=%self.shard_uid.shard_id(), "per-shard apply failed");
                    continue;
                }
            }
        }
    }

    /// Readiness checks for this shard's chunk in `block` (prev CER, prev chunk
    /// extra, incoming receipts on disk), then dispatch to the apply spawner.
    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%block_hash))]
    fn try_apply(&mut self, block_hash: &CryptoHash) -> Result<ApplyOutcome, Error> {
        if self.blocks_in_execution.contains(block_hash) {
            return Ok(ApplyOutcome::Scheduled);
        }
        let shard_uid = self.shard_uid;
        let shard_id = shard_uid.shard_id();
        let block = self.chain_store.get_block(block_hash)?;
        if !is_descendant_of_final_execution_head(&self.chain_store, block.header()) {
            return Ok(ApplyOutcome::Dropped);
        }
        // Resharding: this shard may be absent from `block`'s layout (split/merged
        // away, or — for a child shard — not yet existent). Such a block carries
        // no chunk for this shard, so drop it.
        let current_block_shard_layout =
            self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        if !current_block_shard_layout.shard_ids().any(|id| id == shard_id) {
            return Ok(ApplyOutcome::Dropped);
        }
        // Per-shard already-applied skip. Spice commits each shard independently,
        // so partial application is possible — briefly during apply, durably after
        // a crash between per-shard commits.
        if self.chunk_extra_exists(block_hash, &shard_uid)? {
            return Ok(ApplyOutcome::Dropped);
        }

        let prev_block_hash = *block.header().prev_hash();
        let prev_block = self.chain_store.get_block(&prev_block_hash)?;
        let Some(prev_block_execution_results) =
            self.core_reader.get_block_execution_results(prev_block.header())?
        else {
            tracing::debug!(target: "chunk_executor", %block_hash, %prev_block_hash, ?shard_uid, "not ready: missing prev block execution results");
            return Ok(ApplyOutcome::NotReady);
        };

        // TODO(spice-resharding): convert `prev_block_shard_uid` into the current
        // shard layout's `ShardUId`.
        let prev_block_shard_uid = shard_uid;
        let Some(prev_chunk_chunk_extra) =
            self.get_chunk_extra(&prev_block_hash, &prev_block_shard_uid)?
        else {
            tracing::debug!(target: "chunk_executor", %block_hash, %prev_block_hash, ?shard_uid, "not ready: prev block not executed for this shard");
            return Ok(ApplyOutcome::NotReady);
        };

        let prev_block_epoch_id = self.epoch_manager.get_epoch_id(&prev_block_hash)?;
        let prev_block_shard_uids = self.epoch_manager.shard_uids(&prev_block_epoch_id)?;
        let incoming_receipts = if prev_block.header().is_genesis() {
            // Genesis block has no outgoing receipts.
            vec![]
        } else {
            let proofs = get_receipt_proofs_for_shard(
                &self.chain_store.store(),
                &prev_block_hash,
                prev_block_shard_uid.shard_id(),
            );
            if proofs.len() != prev_block_shard_uids.len() {
                tracing::debug!(target: "chunk_executor", %block_hash, %prev_block_hash, ?shard_uid, have=proofs.len(), want=prev_block_shard_uids.len(), "not ready: missing incoming receipts");
                return Ok(ApplyOutcome::NotReady);
            }
            proofs
        };

        let block_chunk_headers = block.chunks();
        let shard_index = current_block_shard_layout.get_shard_index(shard_id)?;
        let chunk_header =
            block_chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
        let prev_block_header = self.chain_store.get_block_header(&prev_block_hash)?;
        self.dispatch_apply(
            &block,
            &prev_block_execution_results,
            &prev_block_header,
            incoming_receipts,
            chunk_header,
            prev_chunk_chunk_extra,
        )?;
        self.blocks_in_execution.insert(*block_hash);
        Ok(ApplyOutcome::Scheduled)
    }

    /// Build this shard's apply job and submit it to the spawner. The on-done
    /// callback sends a scalar `ExecutorApplyChunksDone` back to the coordinator.
    fn dispatch_apply(
        &self,
        block: &Block,
        prev_block_execution_results: &BlockExecutionResults,
        prev_block_header: &BlockHeader,
        mut incoming_receipts: Vec<ReceiptProof>,
        chunk_header: &ShardChunkHeader,
        prev_chunk_chunk_extra: Arc<ChunkExtra>,
    ) -> Result<(), Error> {
        let block_context = build_spice_apply_chunk_block_context(
            block.header(),
            prev_block_execution_results,
            self.epoch_manager.as_ref(),
        )?;
        // TODO(spice-resharding): We may need to take resharding into account here.
        shuffle_receipt_proofs(&mut incoming_receipts, get_receipts_shuffle_salt(block));
        let chunk_context = ChunkApplicationContext {
            incoming_receipts,
            prev_chunk_chunk_extra,
            shard_uid: self.shard_uid,
            chunk_header,
        };
        let storage_context = StorageContext {
            storage_data_source: StorageDataSource::Db,
            // TODO(spice): add support for sandbox state patching or remove.
            state_patch: SandboxStatePatch::default(),
        };
        let (shard_id, task) = self.build_update_shard_job(
            block_context,
            chunk_context,
            prev_block_header,
            storage_context,
        )?;

        let block_hash = *block.hash();
        let block_height = block.header().height();
        let shard_uid = self.shard_uid;
        let parent_span = tracing::Span::current();
        let apply_done_sender = self.apply_done_sender.clone();
        let boxed: Box<dyn FnOnce() + Send> = Box::new(move || {
            let span = tracing::debug_span!(
                target: "chunk_executor",
                parent: &parent_span,
                "apply_chunk",
                %block_height,
                %shard_id,
            );
            let _guard = span.enter();
            let apply_result = task(&span).map_err(|err| FailedToApplyChunkError { shard_id, err });
            apply_done_sender.send(ExecutorApplyChunksDone { block_hash, shard_uid, apply_result });
        });
        self.apply_chunks_spawner.spawn_boxed("apply_chunks", boxed);
        Ok(())
    }

    /// Apply-done callback for this shard: clear the in-flight marker, then
    /// postprocess and return the outgoing receipt proofs for fanout.
    fn handle_apply_chunks_done(
        &mut self,
        block_hash: CryptoHash,
        apply_result: Result<ShardUpdateResult, FailedToApplyChunkError>,
    ) -> Result<Vec<ReceiptProof>, Error> {
        assert!(self.blocks_in_execution.remove(&block_hash));
        let result = match apply_result {
            Ok(result) => result,
            Err(err) => panic!("failed to apply block {block_hash:?}: {err}"),
        };
        self.apply_chunk_postprocessing_and_emit(block_hash, result)
    }

    fn send_outgoing_receipts(&self, block: &Block, receipt_proofs: Vec<ReceiptProof>) {
        let block_hash = *block.hash();
        tracing::debug!(target: "chunk_executor", %block_hash, ?receipt_proofs, "sending outgoing receipts");
        self.data_distributor_adapter
            .send(SpiceDistributorOutgoingReceipts { block_hash, receipt_proofs });
    }

    /// Postprocess one applied chunk for this shard: persist artifacts, emit the
    /// endorsement / witness / outgoing receipts, and return the produced receipt
    /// proofs so the coordinator can do local-path fanout. Block-level finalize
    /// stays on the coordinator.
    fn apply_chunk_postprocessing_and_emit(
        &self,
        block_hash: CryptoHash,
        result: ShardUpdateResult,
    ) -> Result<Vec<ReceiptProof>, Error> {
        let block = self.chain_store.get_block(&block_hash)?;
        if !is_descendant_of_final_execution_head(&self.chain_store, block.header()) {
            tracing::warn!(
                target: "chunk_executor",
                ?block_hash,
                block_height=%block.header().height(),
                "encountered too old block application; discarding",
            );
            return Ok(Vec::new());
        }

        tracing::debug!(target: "chunk_executor",
            ?block_hash,
            block_height=?block.header().height(),
            head_height=?self.chain_store.head().map(|tip| tip.height),
            "processing chunk application result");
        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let ShardUpdateResult::NewChunk(new_chunk_result) = result else {
            panic!("missing chunks are not expected in SPICE");
        };

        let shard_id = new_chunk_result.shard_uid.shard_id();
        let (outgoing_receipts_root, receipt_proofs) =
            Chain::create_receipts_proofs_from_outgoing_receipts(
                &shard_layout,
                shard_id,
                new_chunk_result.apply_result.outgoing_receipts.clone(),
            )?;
        self.save_produced_receipts(&block_hash, &receipt_proofs);

        if let Some(my_signer) = self.validator_signer.get() {
            // Endorse if we are a chunk validator (regardless of producer status).
            let validators_at_height = self.epoch_manager.get_chunk_validator_assignments(
                &epoch_id,
                shard_id,
                block.header().height(),
            )?;
            if validators_at_height.contains(my_signer.validator_id()) {
                self.send_chunk_endorsement(
                    &block,
                    &my_signer,
                    &new_chunk_result,
                    outgoing_receipts_root,
                );
            }

            // Distribute witness and receipts if we are the chunk producer for the shard.
            let epoch_producers =
                self.epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id)?;
            if epoch_producers.contains(my_signer.validator_id()) {
                self.send_outgoing_receipts(&block, receipt_proofs.clone());
                self.distribute_witness(&block, &new_chunk_result, outgoing_receipts_root)?;
            }
        }
        // RPC nodes skip endorsement/distribution above; persistence below still runs.
        let mut store_update = self.chain_store.store().store_update();
        apply_chunk_postprocessing(
            &mut store_update,
            self.runtime_adapter.as_ref(),
            &block,
            new_chunk_result,
            &self.config,
        )?;
        store_update.commit();
        Ok(receipt_proofs)
    }
}

impl ChunkExecutorActor {
    /// Disk-driven head recovery: walk canonical-next from `spice_execution_head`
    /// and finalize each block whose tracked shards all have `ChunkExtra` on
    /// disk. Used on startup to recover from a crash between apply-commit and
    /// finalize; the hot path finalizes directly in `coordinator_post_apply`.
    fn advance_execution_head(&self) -> Result<(), Error> {
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

    fn finalize_block(&self, block_hash: &CryptoHash) -> Result<(), Error> {
        let block = self.chain_store.get_block(block_hash)?;
        apply_block_postprocessing(
            self.runtime_adapter.as_ref(),
            self.epoch_manager.as_ref(),
            &self.chain_store,
            &block,
        )
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
}

impl PerShardChunkExecutor {
    fn send_chunk_endorsement(
        &self,
        block: &Block,
        my_signer: &ValidatorSigner,
        new_chunk_result: &NewChunkResult,
        outgoing_receipts_root: CryptoHash,
    ) {
        let NewChunkResult { shard_uid, gas_limit, apply_result } = new_chunk_result;
        let shard_id = shard_uid.shard_id();
        let execution_result =
            new_execution_result(*gas_limit, apply_result, outgoing_receipts_root);
        let endorsement = SpiceChunkEndorsement::new(
            SpiceChunkId { block_hash: *block.hash(), shard_id },
            execution_result,
            my_signer,
        );
        send_spice_chunk_endorsement(
            endorsement.clone(),
            self.epoch_manager.as_ref(),
            &self.network_adapter.clone().into_sender(),
            my_signer,
        );
        self.core_writer_sender.send(SpiceChunkEndorsementMessage(endorsement));
    }

    fn distribute_witness(
        &self,
        block: &Block,
        new_chunk_result: &NewChunkResult,
        outgoing_receipts_root: CryptoHash,
    ) -> Result<(), Error> {
        let NewChunkResult { shard_uid, gas_limit, apply_result } = new_chunk_result;
        let shard_id = shard_uid.shard_id();

        let execution_result =
            new_execution_result(*gas_limit, apply_result, outgoing_receipts_root);
        let execution_result_hash = execution_result.compute_hash();

        let ChunkExecutionData { witness: state_witness, code_accesses: contract_accesses } =
            self.create_chunk_execution_data(block, apply_result, shard_id, execution_result_hash)?;

        save_witness_and_contract_accesses(
            &self.chain_store,
            block.hash(),
            shard_id,
            &state_witness,
            &contract_accesses,
        );
        self.data_distributor_adapter
            .send(SpiceDistributorStateWitness { state_witness, contract_accesses });

        Ok(())
    }

    fn create_chunk_execution_data(
        &self,
        block: &Block,
        apply_result: &ApplyChunkResult,
        shard_id: ShardId,
        execution_result_hash: ChunkExecutionResultHash,
    ) -> Result<ChunkExecutionData, Error> {
        let block_hash = block.header().hash();
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash).unwrap();
        let (transactions, proof_of_invalid_chunk) = {
            let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id).unwrap();
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            let chunk_headers = block.chunks();
            let chunk_header = chunk_headers.get(shard_index).unwrap();
            match self.get_new_chunk_if_valid(chunk_header, block.header().height()).unwrap() {
                Some(chunk) => (chunk.into_transactions(), None),
                None => {
                    let proof = if chunk_header.is_new_chunk(block.header().height()) {
                        // Chunk is new but invalid (malicious producer): include proof.
                        self.chain_store
                            .chunk_store()
                            .is_invalid_chunk(chunk_header.chunk_hash())
                            .map(|enc| Box::new(enc.content().clone()))
                    } else {
                        None
                    };
                    (vec![], proof)
                }
            }
        };

        let applied_receipts_hash = apply_result.applied_receipts_hash;
        let ContractUpdates { contract_accesses, contract_deploys: _ } =
            apply_result.contract_updates.clone();

        let main_transition = {
            let PartialState::TrieValues(base_state_values) =
                apply_result.proof.clone().unwrap().nodes;
            // Contract bytecodes are not included in the witness. They are sent
            // separately via SpiceChunkContractAccesses so that validators can
            // check their compiled contract cache and only request missing ones.
            SpiceChunkStateTransition {
                base_state: PartialState::TrieValues(base_state_values),
                post_state_root: apply_result.new_root,
            }
        };
        let source_receipt_proofs: HashMap<ShardId, ReceiptProof> = {
            let prev_block_hash = block.header().prev_hash();
            let (_, prev_block_shard_id, _) =
                self.epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
            let receipt_proofs = get_receipt_proofs_for_shard(
                &self.chain_store.store(),
                prev_block_hash,
                prev_block_shard_id,
            );
            receipt_proofs
                .into_iter()
                .map(|proof| -> Result<_, Error> { Ok((proof.1.from_shard_id, proof)) })
                .collect::<Result<_, Error>>()?
        };
        // TODO(spice-resharding): Handle witness validation when resharding.
        let contract_accesses_hash = compute_contract_accesses_hash(&contract_accesses);
        let state_witness = SpiceChunkStateWitness::new(
            near_primitives::types::SpiceChunkId { block_hash: *block_hash, shard_id },
            main_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            execution_result_hash,
            contract_accesses_hash,
            proof_of_invalid_chunk,
        );
        Ok(ChunkExecutionData { witness: state_witness, code_accesses: contract_accesses })
    }

    /// Returns the chunk if it is a new, valid chunk. Returns `None` if the
    /// chunk is not new, or if the chunk is invalid (malicious chunk producer).
    fn get_new_chunk_if_valid(
        &self,
        chunk_header: &ShardChunkHeader,
        height: BlockHeight,
    ) -> Result<Option<ShardChunk>, Error> {
        if !chunk_header.is_new_chunk(height) {
            return Ok(None);
        }
        let chunk_store = self.chain_store.chunk_store();
        match get_chunk_clone_from_header(&chunk_store, chunk_header) {
            Ok(chunk) => Ok(Some(chunk)),
            Err(Error::ChunkMissing(_))
                if chunk_store.is_invalid_chunk(chunk_header.chunk_hash()).is_some() =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    #[instrument(
        level = "debug",
        target = "chunk_executor",
        skip_all,
        fields(
            block_height=%block_context.height,
            prev_block_hash=?prev_block_header.hash(),
        )
    )]
    fn build_update_shard_job(
        &self,
        block_context: ApplyChunkBlockContext,
        chunk_context: ChunkApplicationContext,
        prev_block_header: &BlockHeader,
        storage_context: StorageContext,
    ) -> Result<UpdateShardJob, Error> {
        let receipts = collect_receipts(&chunk_context.incoming_receipts);
        let chunk_header = chunk_context.chunk_header;
        let (transactions, chunk_hash) = match self
            .get_new_chunk_if_valid(chunk_header, block_context.height)?
        {
            Some(chunk) => {
                let chunk_hash = chunk.chunk_hash().clone();
                let tx_valid_list = compute_transaction_validity(
                    &self.chain_store,
                    self.transaction_validity_period,
                    prev_block_header,
                    &chunk,
                );
                (
                    SignedValidPeriodTransactions::new(chunk.into_transactions(), tx_valid_list),
                    Some(chunk_hash),
                )
            }
            None => (SignedValidPeriodTransactions::new(vec![], vec![]), None),
        };

        let prev_chunk_chunk_extra = chunk_context.prev_chunk_chunk_extra;
        let prev_validator_proposals = self.core_reader.prev_validator_proposals(
            &block_context.prev_block_hash,
            chunk_context.shard_uid.shard_id(),
        )?;
        let shard_update_reason = ShardUpdateReason::NewChunk(NewChunkData {
            gas_limit: prev_chunk_chunk_extra.gas_limit(),
            prev_state_root: *prev_chunk_chunk_extra.state_root(),
            prev_validator_proposals,
            chunk_hash,
            transactions,
            receipts,
            block: block_context,
            storage_context,
        });

        let runtime = self.runtime_adapter.clone();
        let shard_uid = chunk_context.shard_uid;
        // Pin so `gc_memtrie_roots` can't evict the prev-state root before
        // the async apply task runs.
        let memtrie_pin = runtime
            .get_tries()
            .maybe_pin_memtrie_root(shard_uid, *prev_chunk_chunk_extra.state_root())?;
        Ok((
            shard_uid.shard_id(),
            Box::new(move |parent_span| -> Result<ShardUpdateResult, Error> {
                Ok(process_shard_update(
                    parent_span,
                    runtime.as_ref(),
                    shard_update_reason,
                    ShardContext { shard_uid, should_apply_chunk: true },
                    memtrie_pin,
                    None,
                )?)
            }),
        ))
    }

    pub(crate) fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Option<Arc<ChunkExtra>>, Error> {
        match self.chain_store.chunk_store().get_chunk_extra(block_hash, shard_uid) {
            Ok(chunk_extra) => Ok(Some(chunk_extra)),
            Err(Error::DBNotFoundErr(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn chunk_extra_exists(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<bool, Error> {
        self.get_chunk_extra(block_hash, shard_uid).map(|option| option.is_some())
    }

    fn save_produced_receipts(&self, block_hash: &CryptoHash, receipt_proofs: &[ReceiptProof]) {
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        for proof in receipt_proofs {
            save_receipt_proof(&mut store_update, &block_hash, &proof)
        }
        store_update.commit();
    }
}

impl ChunkExecutorActor {
    fn save_verified_receipts(&self, verified_receipts: &VerifiedReceipts) {
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        let VerifiedReceipts { receipt_proof, block_hash } = verified_receipts;
        save_receipt_proof(&mut store_update, &block_hash, &receipt_proof);
        store_update.commit();
    }

    fn receipts_verification_context(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<ReceiptVerificationContext, Error> {
        let block = self.chain_store.get_block(block_hash)?;
        if !self.core_reader.all_execution_results_exist(block.header())? {
            return Ok(ReceiptVerificationContext::NotReady);
        }
        let execution_results =
            self.core_reader.get_execution_results_by_shard_id(block.header())?;
        Ok(ReceiptVerificationContext::Ready { execution_results })
    }

    fn try_process_pending_unverified_receipts(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let verification_context = self.receipts_verification_context(&block_hash)?;
        let ReceiptVerificationContext::Ready { execution_results } = verification_context else {
            return Ok(());
        };
        let pending_receipts = self.pending_unverified_receipts.remove(block_hash);
        for receipt in pending_receipts.into_iter().flatten() {
            let verified_receipts = match receipt.verify(&execution_results) {
                Ok(verified_receipts) => verified_receipts,
                Err(err) => {
                    // TODO(spice): Notify spice data distributor about invalid receipts so it can ban
                    // or de-prioritize the node which sent them.
                    tracing::warn!(target: "chunk_executor", ?err, ?block_hash, "encountered invalid receipts");
                    continue;
                }
            };
            self.save_verified_receipts(&verified_receipts);
        }
        Ok(())
    }

    fn process_all_ready_blocks(&mut self) -> Result<(), Error> {
        let start_block = match self.chain_store.spice_final_execution_head() {
            Ok(final_execution_head) => final_execution_head.last_block_hash,
            Err(Error::DBNotFoundErr(_)) => {
                let final_head_hash = self.chain_store.final_head()?.last_block_hash;
                let mut header = self.chain_store.get_block_header(&final_head_hash)?;
                // TODO(spice): Stop searching on the first non-spice block.
                while !header.is_genesis() {
                    header = self.chain_store.get_block_header(header.prev_hash())?;
                }
                *header.hash()
            }
            Err(err) => return Err(err),
        };

        // Park every block above the execution head into its tracked shards'
        // executors and try to drain. A freshly-started node catches up from disk;
        // blocks not yet ready stay parked and are re-driven by apply-done fanout,
        // incoming receipts, and CER endorsements.
        let mut next_block_hashes: VecDeque<_> =
            self.chain_store.get_all_next_block_hashes(&start_block).into();
        while let Some(block_hash) = next_block_hashes.pop_front() {
            self.handle_processed_block(&block_hash)?;
            next_block_hashes.extend(&self.chain_store.get_all_next_block_hashes(&block_hash));
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn pending_receipts_count(&self) -> usize {
        self.pending_unverified_receipts.len()
    }
}

fn new_execution_result(
    gas_limit: Gas,
    apply_result: &ApplyChunkResult,
    outgoing_receipts_root: CryptoHash,
) -> ChunkExecutionResult {
    let chunk_extra = apply_result.to_chunk_extra(gas_limit);
    ChunkExecutionResult { chunk_extra, outgoing_receipts_root }
}

// We depend on stored receipts for distribution, so we need to store receipt proof and not only
// Vec<Receipt>.
pub(crate) fn save_receipt_proof(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    receipt_proof: &ReceiptProof,
) {
    let &ReceiptProof(_, ShardProof { from_shard_id, to_shard_id, .. }) = receipt_proof;
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    let value = borsh::to_vec(&receipt_proof).unwrap();
    store_update.set(DBCol::receipt_proofs(), &key, &value);
}

fn set_witness(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    witness: &SpiceChunkStateWitness,
) {
    let key = get_witnesses_key(block_hash, shard_id);
    let value = borsh::to_vec(&witness).unwrap();
    store_update.set(DBCol::witnesses(), &key, &value);
}

fn get_receipt_proofs_for_shard(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
) -> Vec<ReceiptProof> {
    let prefix = get_receipt_proof_target_shard_prefix(block_hash, to_shard_id);
    store.iter_prefix_ser::<ReceiptProof>(DBCol::receipt_proofs(), &prefix).map(|kv| kv.1).collect()
}

pub fn get_witness(
    store: &Store,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Option<SpiceChunkStateWitness> {
    let key = get_witnesses_key(block_hash, shard_id);
    store.get_ser(DBCol::witnesses(), &key)
}

fn set_contract_accesses(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    contract_accesses: &HashSet<CodeHash>,
) {
    let key = get_contract_accesses_key(block_hash, shard_id);
    let value: Vec<CodeHash> = contract_accesses.iter().cloned().collect();
    let value = borsh::to_vec(&value).unwrap();
    store_update.set(DBCol::contract_accesses(), &key, &value);
}

/// Saves witness and contract accesses atomically in a single DB transaction.
pub(crate) fn save_witness_and_contract_accesses(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    witness: &SpiceChunkStateWitness,
    contract_accesses: &HashSet<CodeHash>,
) {
    let mut store_update = chain_store.store().store_update();
    set_witness(&mut store_update, block_hash, shard_id, witness);
    set_contract_accesses(&mut store_update, block_hash, shard_id, contract_accesses);
    store_update.commit();
}

pub(crate) fn get_contract_accesses(
    store: &Store,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Option<HashSet<CodeHash>> {
    let key = get_contract_accesses_key(block_hash, shard_id);
    let accesses: Arc<Vec<CodeHash>> = store.caching_get_ser(DBCol::contract_accesses(), &key)?;
    Some(accesses.iter().cloned().collect())
}

pub fn get_receipt_proof(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
    from_shard_id: ShardId,
) -> Option<ReceiptProof> {
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    store.get_ser(DBCol::receipt_proofs(), &key)
}

pub fn receipt_proof_exists(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
    from_shard_id: ShardId,
) -> bool {
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    store.exists(DBCol::receipt_proofs(), &key)
}

type UpdateShardJob =
    (ShardId, Box<dyn FnOnce(&tracing::Span) -> Result<ShardUpdateResult, Error> + Send + 'static>);

pub(crate) fn is_descendant_of_final_execution_head(
    chain_store: &ChainStoreAdapter,
    header: &BlockHeader,
) -> bool {
    let final_execution_head = match chain_store.spice_final_execution_head() {
        Ok(final_header) => final_header,
        // Without final execution head we are either executing on genesis and don't have it yet or
        // executing on top of non-spice blocks. In both cases we can assume that all blocks are on
        // top of final_execution_head until it's set.
        Err(Error::DBNotFoundErr(_)) => return true,
        Err(err) => panic!("failed to find final execution head: {err:?}"),
    };
    let mut height = header.height();
    if height <= final_execution_head.height {
        return false;
    }
    let mut prev_hash = *header.prev_hash();
    while height > final_execution_head.height {
        let header = chain_store.get_block_header(&prev_hash).unwrap();
        prev_hash = *header.prev_hash();
        height = header.height();
    }
    height == final_execution_head.height
}

/// This module is needed for integration tests, otherwise it should not be used.
/// It's kept here to expose less of the actual actor API.
pub mod testonly {
    use super::*;
    use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
    use near_async::messaging::noop;
    use near_chain::spice::core_writer_actor::SpiceCoreWriterActor;
    use parking_lot::RwLock;

    struct FakeSpawner {
        sc: UnboundedSender<Box<dyn FnOnce() + Send>>,
    }

    impl FakeSpawner {
        fn new() -> (FakeSpawner, UnboundedReceiver<Box<dyn FnOnce() + Send>>) {
            let (sc, rc) = unbounded();
            (Self { sc }, rc)
        }
    }

    impl AsyncComputationSpawner for FakeSpawner {
        fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
            self.sc.unbounded_send(f).unwrap();
        }
    }

    /// This struct is needed for integration tests, otherwise it should not be used.
    pub struct TestonlySyncChunkExecutorActor {
        actor: ChunkExecutorActor,
        actor_rc: UnboundedReceiver<ExecutorApplyChunksDone>,
        tasks_rc: UnboundedReceiver<Box<dyn FnOnce() + Send>>,
        core_writer_actor: SpiceCoreWriterActor,
        produced_endorsements: Arc<RwLock<VecDeque<SpiceChunkEndorsementMessage>>>,
        produced_receipt_proofs: Arc<RwLock<VecDeque<SpiceDistributorOutgoingReceipts>>>,
    }

    impl TestonlySyncChunkExecutorActor {
        pub fn new(
            store: Store,
            genesis: &ChainGenesis,
            runtime_adapter: Arc<dyn RuntimeAdapter>,
            epoch_manager: Arc<dyn EpochManagerAdapter>,
            shard_tracker: ShardTracker,
            network_adapter: PeerManagerAdapter,
            validator_signer: MutableValidatorSigner,
            chunk_persistence_config: ChunkPersistenceConfig,
        ) -> Self {
            let (actor_sc, actor_rc) = unbounded();
            let myself_sender = Sender::from_fn(move |event: ExecutorApplyChunksDone| {
                actor_sc.unbounded_send(event).unwrap();
            });
            let (spawner, tasks_rc) = FakeSpawner::new();

            let core_reader = SpiceCoreReader::new(
                runtime_adapter.store().chain_store(),
                epoch_manager.clone(),
                genesis.gas_limit,
            );
            let core_writer_actor = SpiceCoreWriterActor::new(
                runtime_adapter.store().chain_store(),
                epoch_manager.clone(),
                core_reader,
                noop().into_sender(),
                noop().into_sender(),
            );
            let produced_endorsements = Arc::new(RwLock::new(VecDeque::new()));
            let core_writer_sender = Sender::from_fn({
                let endorsements = produced_endorsements.clone();
                move |message| endorsements.write().push_back(message)
            });
            let produced_receipt_proofs = Arc::new(RwLock::new(VecDeque::new()));
            let data_distributor_adapter = SpiceDataDistributorAdapter {
                receipts: Sender::from_fn({
                    let produced_receipt_proofs = produced_receipt_proofs.clone();
                    move |message| produced_receipt_proofs.write().push_back(message)
                }),
                witness: noop().into_sender(),
            };
            Self {
                actor: ChunkExecutorActor::new(
                    store,
                    genesis,
                    runtime_adapter,
                    epoch_manager,
                    shard_tracker,
                    network_adapter,
                    validator_signer,
                    Arc::new(spawner),
                    myself_sender,
                    core_writer_sender,
                    data_distributor_adapter,
                    chunk_persistence_config,
                ),
                actor_rc,
                tasks_rc,
                core_writer_actor,
                produced_endorsements,
                produced_receipt_proofs,
            }
        }

        pub fn pop_endorsement(&self) -> Option<SpiceChunkEndorsementMessage> {
            self.produced_endorsements.write().pop_front()
        }

        pub fn record_endorsement(&mut self, endorsement: SpiceChunkEndorsementMessage) {
            self.core_writer_actor.handle(endorsement);
        }

        pub fn pop_produced_receipts(&self) -> Option<SpiceDistributorOutgoingReceipts> {
            self.produced_receipt_proofs.write().pop_front()
        }

        pub fn handle_incoming_receipts(&mut self, receipts: ExecutorIncomingUnverifiedReceipts) {
            self.actor.handle(receipts);
            self.run_internal_events();
        }

        pub fn handle_processed_block(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
            self.actor.handle_processed_block(&block_hash).unwrap();
            self.run_internal_events();
            self.core_writer_actor.handle(ProcessedBlock { block_hash });
        }

        fn run_internal_events(&mut self) {
            loop {
                let mut events_processed = 0;
                while let Ok(Some(task)) = self.tasks_rc.try_next() {
                    events_processed += 1;
                    task();
                }
                while let Ok(Some(event)) = self.actor_rc.try_next() {
                    events_processed += 1;
                    let block_hash = event.block_hash;
                    let outgoing_proofs = self.actor.per_shard_apply_done(event);
                    self.actor.coordinator_post_apply(&block_hash, &outgoing_proofs).unwrap();
                }
                if events_processed == 0 {
                    break;
                }
            }
        }
    }
}
