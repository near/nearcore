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
use near_chain::PendingShardJobs;
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
use near_chain_primitives::ApplyChunksMode;
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
    pending: BTreeSet<(BlockHeight, CryptoHash)>,
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
            pending: BTreeSet::new(),
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

    /// One executor per tracked shard, reconciled on every block.
    per_shard_executors: HashMap<ShardId, PerShardChunkExecutor>,

    blocks_in_execution: HashSet<CryptoHash>,
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
            blocks_in_execution: HashSet::new(),
            validator_signer,
            core_reader,
            data_distributor_adapter,
            core_writer_sender,
            config,
            pending_unverified_receipts: HashMap::new(),
        }
    }

    // TODO(spice): We should request relevant receipts from spice data distributor either
    // after we finished executing a relevant block or once we know about a block.
    fn handle_apply_chunks_done(
        &mut self,
        ExecutorApplyChunksDone { block_hash, apply_results }: ExecutorApplyChunksDone,
    ) -> Result<(), HandleExecutorApplyChunksDoneError> {
        self.process_apply_chunk_results(block_hash, apply_results)
            .map_err(HandleExecutorApplyChunksDoneError::ProcessApplyChunk)?;
        assert!(self.blocks_in_execution.remove(&block_hash));
        self.try_process_next_blocks(&block_hash)
            .map_err(HandleExecutorApplyChunksDoneError::TryProcessNextBlocks)
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
    pub apply_results: Result<Vec<ShardUpdateResult>, FailedToApplyChunkError>,
}

#[derive(Debug, thiserror::Error)]
#[error("failed to apply chunk")]
pub struct FailedToApplyChunkError {
    shard_id: ShardId,
    err: Error,
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
        if let Err(err) = self.try_process_pending_unverified_receipts(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed while trying to save pending unverified receipts");
        }

        if let Err(err) = self.try_process_next_blocks(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to process next blocks");
        }
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorActor {
    // TODO(spice): Implement pub(crate) handle functions in ChunkExecutorActor that would return
    // errors/results and use them in tests to make sure we are testing correct errors.
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        match self.try_apply_chunks(&block_hash) {
            Ok(TryApplyChunksOutcome::Scheduled) | Ok(TryApplyChunksOutcome::BlockIrrelevant) => {}
            Ok(TryApplyChunksOutcome::NotReady(reason)) => {
                // We will retry applying it by looking at all next blocks after receiving
                // additional execution result endorsements or receipts.
                tracing::debug!(target: "chunk_executor", ?reason, %block_hash, "not yet ready for processing");
            }
            Ok(TryApplyChunksOutcome::BlockAlreadyAccepted) => {
                tracing::warn!(
                    target: "chunk_executor",
                    ?block_hash,
                    "not expected to receive already executed block"
                );
            }
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to apply chunk for block hash");
            }
        }
    }
}

impl Handler<ExecutionResultEndorsed> for ChunkExecutorActor {
    fn handle(&mut self, ExecutionResultEndorsed { block_hash }: ExecutionResultEndorsed) {
        if let Err(err) = self.try_process_next_blocks(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to process next blocks");
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum HandleExecutorApplyChunksDoneError {
    #[error("failed to process apply chunk results")]
    ProcessApplyChunk(Error),
    #[error("failed to process next blocks")]
    TryProcessNextBlocks(Error),
}

impl Handler<ExecutorApplyChunksDone> for ChunkExecutorActor {
    fn handle(&mut self, msg: ExecutorApplyChunksDone) {
        let block_hash = msg.block_hash;
        if let Err(err) = self.handle_apply_chunks_done(msg) {
            tracing::error!(target:"chunk_executor", ?err, ?block_hash, "failed to handle apply chunks done");
        }
    }
}

// Though it's useful for debugging clippy treats all fields as dead code.
#[allow(dead_code)]
#[derive(Debug)]
pub enum NotReadyToApplyChunksReason {
    MissingExecutionResults {
        prev_block_hash: CryptoHash,
        missing_shard_ids: Vec<ShardId>,
    },
    PreviousBlockIsNotExecuted {
        prev_block_hash: CryptoHash,
        prev_block_shard_id: ShardId,
    },
    MissingReceipts {
        prev_block_hash: CryptoHash,
        missing_receipts_from_shard_ids: HashSet<ShardId>,
        missing_receipts_to_shard_id: ShardId,
    },
}

#[derive(Debug)]
pub enum TryApplyChunksOutcome {
    Scheduled,
    NotReady(NotReadyToApplyChunksReason),
    BlockAlreadyAccepted,
    BlockIrrelevant,
}

impl TryApplyChunksOutcome {
    fn missing_execution_results(
        core_reader: &SpiceCoreReader,
        epoch_manager: &dyn EpochManagerAdapter,
        prev_block: &Block,
    ) -> Result<Self, Error> {
        let execution_results =
            core_reader.get_execution_results_by_shard_id(prev_block.header())?;
        let shard_layout = epoch_manager.get_shard_layout(prev_block.header().epoch_id())?;
        let missing_shard_ids = shard_layout
            .shard_ids()
            .filter(|shard_id| !execution_results.contains_key(shard_id))
            .collect();
        Ok(TryApplyChunksOutcome::NotReady(NotReadyToApplyChunksReason::MissingExecutionResults {
            prev_block_hash: *prev_block.hash(),
            missing_shard_ids,
        }))
    }

    fn previous_block_is_not_executed(
        prev_block_hash: CryptoHash,
        prev_block_shard_id: ShardId,
    ) -> Self {
        TryApplyChunksOutcome::NotReady(NotReadyToApplyChunksReason::PreviousBlockIsNotExecuted {
            prev_block_hash,
            prev_block_shard_id,
        })
    }

    fn missing_receipts(
        prev_block_hash: CryptoHash,
        proofs: &[ReceiptProof],
        to_shard_id: ShardId,
        prev_block_shard_ids: &[ShardId],
    ) -> Self {
        let mut missing_receipts: HashSet<ShardId> = prev_block_shard_ids.iter().copied().collect();
        for proof in proofs {
            missing_receipts.remove(&proof.1.from_shard_id);
        }
        TryApplyChunksOutcome::NotReady(NotReadyToApplyChunksReason::MissingReceipts {
            prev_block_hash,
            missing_receipts_from_shard_ids: missing_receipts,
            missing_receipts_to_shard_id: to_shard_id,
        })
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
    /// and the shared deps from the coordinator.
    fn get_or_create_per_shard_executor(
        &mut self,
        shard_uid: ShardUId,
    ) -> &mut PerShardChunkExecutor {
        let deps = self.per_shard_deps();
        let chain_store = self.chain_store.clone();
        let transaction_validity_period = self.transaction_validity_period;
        let spawner = self.apply_chunks_spawner.clone();
        let apply_done_sender = self.myself_sender.clone();
        self.per_shard_executors.entry(shard_uid.shard_id()).or_insert_with(|| {
            PerShardChunkExecutor::new(
                shard_uid,
                chain_store,
                deps,
                transaction_validity_period,
                spawner,
                apply_done_sender,
            )
        })
    }

    /// Spawn executors for newly-tracked shards and evict ones no longer tracked.
    /// Eviction is immediate; the apply-done handler's graceful-drop guard (added
    /// when the apply path migrates) covers any in-flight apply that lands after
    /// eviction.
    fn reconcile_tracked_shards(&mut self, prev_hash: &CryptoHash) -> Result<(), Error> {
        let tracked = self.shard_tracker.tracked_shard_uids(prev_hash)?;
        for shard_uid in &tracked {
            self.get_or_create_per_shard_executor(*shard_uid);
        }
        let tracked_ids: HashSet<ShardId> =
            tracked.iter().map(|shard_uid| shard_uid.shard_id()).collect();
        self.per_shard_executors.retain(|shard_id, _| tracked_ids.contains(shard_id));
        Ok(())
    }

    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%block_hash))]
    fn try_apply_chunks(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<TryApplyChunksOutcome, Error> {
        if self.blocks_in_execution.contains(block_hash) {
            return Ok(TryApplyChunksOutcome::BlockAlreadyAccepted);
        }
        let block = self.chain_store.get_block(block_hash)?;
        if !is_descendant_of_final_execution_head(&self.chain_store, block.header()) {
            tracing::warn!(
                target: "chunk_executor",
                ?block_hash,
                block_height=%block.header().height(),
                "block's parent is too old (past spice final execution head) so block cannot be applied",
            );
            return Ok(TryApplyChunksOutcome::BlockIrrelevant);
        }

        let block_chunk_headers = block.chunks();
        let header = block.header();
        let prev_block_hash = header.prev_hash();
        // Keep the per-shard executor set in sync with tracked shards. Dispatch is
        // still coordinator-side in this PR, so spawn/evict here is inert beyond
        // maintaining the registry; PR 4 routes applies through these executors.
        self.reconcile_tracked_shards(prev_block_hash)?;
        let prev_block = self.chain_store.get_block(&prev_block_hash)?;
        let store = self.chain_store.store();

        let Some(prev_block_execution_results) =
            self.core_reader.get_block_execution_results(prev_block.header())?
        else {
            return TryApplyChunksOutcome::missing_execution_results(
                &self.core_reader,
                self.epoch_manager.as_ref(),
                &prev_block,
            );
        };

        // TODO(spice): refactor try_process_pending_unverified_receipts to take prev_block_execution_results as argument.
        if let Err(err) = self.try_process_pending_unverified_receipts(prev_block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failure when processing pending unverified receipts");
            return Err(err);
        }

        let mut chunk_contexts = Vec::new();
        let mut any_shard_already_applied = false;
        let prev_block_epoch_id = self.epoch_manager.get_epoch_id(prev_block_hash)?;
        let prev_block_shard_uids = self.epoch_manager.shard_uids(&prev_block_epoch_id)?;
        let current_block_shard_layout =
            self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        for &prev_block_shard_uid in &prev_block_shard_uids {
            // TODO(spice-resharding): convert `prev_block_shard_uid` into the
            // current shard layout's `ShardUId`.
            let shard_uid = prev_block_shard_uid;
            let shard_id = shard_uid.shard_id();
            let prev_block_shard_id = prev_block_shard_uid.shard_id();
            if !self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                prev_block_hash,
                shard_id,
            ) {
                continue;
            }

            // Per-shard already-applied skip. Spice commits each shard
            // independently, so partial application is possible — briefly during
            // apply, durably after a crash between per-shard commits. Skip the
            // applied shard rather than bailing the whole block.
            if self.chunk_extra_exists(block_hash, &shard_uid)? {
                any_shard_already_applied = true;
                continue;
            }

            let Some(prev_chunk_chunk_extra) =
                self.get_chunk_extra(prev_block_hash, &prev_block_shard_uid)?
            else {
                return Ok(TryApplyChunksOutcome::previous_block_is_not_executed(
                    *prev_block_hash,
                    prev_block_shard_id,
                ));
            };

            let incoming_receipts = if prev_block.header().is_genesis() {
                // Genesis block has no outgoing receipts.
                vec![]
            } else {
                let proofs =
                    get_receipt_proofs_for_shard(&store, prev_block_hash, prev_block_shard_id);
                if proofs.len() != prev_block_shard_uids.len() {
                    let prev_block_shard_ids: Vec<ShardId> =
                        prev_block_shard_uids.iter().map(|u| u.shard_id()).collect();
                    return Ok(TryApplyChunksOutcome::missing_receipts(
                        *prev_block_hash,
                        &proofs,
                        prev_block_shard_id,
                        &prev_block_shard_ids,
                    ));
                }
                proofs
            };

            let shard_index = current_block_shard_layout.get_shard_index(shard_id)?;
            let chunk_header =
                block_chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;

            chunk_contexts.push(ChunkApplicationContext {
                incoming_receipts,
                prev_chunk_chunk_extra,
                shard_uid,
                chunk_header,
            });
        }
        if chunk_contexts.is_empty() && any_shard_already_applied {
            // All tracked shards already applied on disk; nothing left to schedule.
            // Treat as already-accepted so the caller stops retrying. Empty
            // contexts without `any_shard_already_applied` means this node tracks
            // no shards — fall through to the no-op schedule call.
            return Ok(TryApplyChunksOutcome::BlockAlreadyAccepted);
        }
        self.schedule_apply_chunks(
            &block,
            chunk_contexts,
            prev_block_execution_results,
            // TODO(spice): add support for sandbox state patching or remove.
            SandboxStatePatch::default(),
        )?;
        self.blocks_in_execution.insert(*block_hash);
        Ok(TryApplyChunksOutcome::Scheduled)
    }

    fn try_process_next_blocks(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let next_block_hashes = self.chain_store.get_all_next_block_hashes(block_hash);
        if next_block_hashes.is_empty() {
            // Next block wasn't received yet.
            tracing::debug!(target: "chunk_executor", %block_hash, "no next block hash is available");
            return Ok(());
        }
        for next_block_hash in next_block_hashes {
            match self.try_apply_chunks(&next_block_hash)? {
                TryApplyChunksOutcome::Scheduled | TryApplyChunksOutcome::BlockIrrelevant => {}
                TryApplyChunksOutcome::NotReady(reason) => {
                    tracing::debug!(target: "chunk_executor", ?reason, %next_block_hash, "not yet ready for processing");
                }
                TryApplyChunksOutcome::BlockAlreadyAccepted => {}
            }
        }
        Ok(())
    }

    fn schedule_apply_chunks(
        &self,
        block: &Block,
        chunk_contexts: Vec<ChunkApplicationContext>,
        prev_block_execution_results: BlockExecutionResults,
        mut state_patch: SandboxStatePatch,
    ) -> Result<(), Error> {
        let prev_hash = block.header().prev_hash();
        let prev_block_header = self.chain_store.get_block_header(prev_hash)?;

        let receipts_shuffle_salt = get_receipts_shuffle_salt(&block);

        let block_contexts = {
            let block_context = build_spice_apply_chunk_block_context(
                block.header(),
                &prev_block_execution_results,
                self.epoch_manager.as_ref(),
            )?;
            std::iter::repeat_n(block_context, chunk_contexts.len())
        };
        // TODO(spice-resharding): Make sure shard logic is correct with resharding.
        let jobs: Vec<UpdateShardJob> = chunk_contexts
            .into_iter()
            .zip(block_contexts)
            .map(|(mut chunk_context, block_context)| {
                // XXX: This is a bit questionable -- sandbox state patching works
                // only for a single shard. This so far has been enough.
                let state_patch = state_patch.take();

                // TODO(spice-resharding): We may need to take resharding into account here.
                shuffle_receipt_proofs(&mut chunk_context.incoming_receipts, receipts_shuffle_salt);

                let storage_context =
                    StorageContext { storage_data_source: StorageDataSource::Db, state_patch };
                self.get_update_shard_job(
                    block_context,
                    chunk_context,
                    &prev_block_header,
                    storage_context,
                )
            })
            .collect::<Result<_, _>>()?;

        let block_hash = *block.hash();
        let block_height = block.header().height();
        // Track all children using `parent_span`, as they may be processed in parallel.
        let parent_span = tracing::Span::current();
        let apply_done_sender = self.myself_sender.clone();
        let on_done = move |results: Vec<(ShardId, Result<ShardUpdateResult, Error>)>| {
            let apply_results = results
                .into_iter()
                .map(|(shard_id, result)| {
                    result.map_err(|err| FailedToApplyChunkError { shard_id, err })
                })
                .collect::<Result<Vec<_>, _>>();
            apply_done_sender.send(ExecutorApplyChunksDone { block_hash, apply_results });
        };
        let jobs = jobs
            .into_iter()
            .map(|(shard_id, task)| {
                let parent_span = parent_span.clone();
                let boxed: Box<dyn FnOnce() -> _ + Send> = Box::new(move || {
                    let span = tracing::debug_span!(
                        target: "chunk_executor",
                        parent: &parent_span,
                        "apply_chunk",
                        %block_height,
                        %shard_id,
                    );
                    let _guard = span.enter();
                    task(&span)
                });
                (shard_id, boxed)
            })
            .collect();
        PendingShardJobs::run("apply_chunks", self.apply_chunks_spawner.clone(), jobs, on_done);
        Ok(())
    }

    fn send_outgoing_receipts(&self, block: &Block, receipt_proofs: Vec<ReceiptProof>) {
        let block_hash = *block.hash();
        tracing::debug!(target: "chunk_executor", %block_hash, ?receipt_proofs, "sending outgoing receipts");
        self.data_distributor_adapter
            .send(SpiceDistributorOutgoingReceipts { block_hash, receipt_proofs });
    }

    fn process_apply_chunk_results(
        &self,
        block_hash: CryptoHash,
        results: Result<Vec<ShardUpdateResult>, FailedToApplyChunkError>,
    ) -> Result<(), Error> {
        let block = self.chain_store.get_block(&block_hash).unwrap();
        if !is_descendant_of_final_execution_head(&self.chain_store, block.header()) {
            tracing::warn!(
                target: "chunk_executor",
                ?block_hash,
                block_height=%block.header().height(),
                "encountered too old block application; discarding",
            );
            return Ok(());
        }
        let results = match results {
            Ok(results) => results,
            Err(err) => {
                panic!("failed to apply block {block_hash:?}: {err}")
            }
        };

        tracing::debug!(target: "chunk_executor",
            ?block_hash,
            block_height=?block.header().height(),
            head_height=?self.chain_store.head().map(|tip| tip.height),
            "processing chunk application results");
        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        for result in results {
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
                    self.send_outgoing_receipts(&block, receipt_proofs);
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
        }
        // Finalize directly here, not via the canonical-next walk: under forks
        // `NextBlockHashes[fork]` is never populated (siblings overwrite
        // `NextBlockHashes[fork.prev]`), so the walk can't reach a fork block
        // from the prior head. Forward-only setters make this idempotent.
        if self.all_tracked_shards_applied(&block_hash)? {
            self.finalize_block(&block_hash)?;
        }
        Ok(())
    }

    /// Disk-driven head recovery: walk canonical-next from `spice_execution_head`
    /// and finalize each block whose tracked shards all have `ChunkExtra` on
    /// disk. Used on startup to recover from a crash between apply-commit and
    /// finalize; the hot path finalizes directly in `process_apply_chunk_results`.
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
        for shard_uid in self.shard_tracker.tracked_shard_uids(&prev_hash)? {
            if !self.chunk_extra_exists(block_hash, &shard_uid)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

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
    fn get_update_shard_job(
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

        let mut next_block_hashes: VecDeque<_> =
            self.chain_store.get_all_next_block_hashes(&start_block).into();
        while let Some(block_hash) = next_block_hashes.pop_front() {
            if !matches!(
                self.try_apply_chunks(&block_hash)?,
                TryApplyChunksOutcome::BlockAlreadyAccepted
            ) {
                continue;
            }
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

        pub fn handle_processed_block(
            &mut self,
            ProcessedBlock { block_hash }: ProcessedBlock,
        ) -> TryApplyChunksOutcome {
            let ret = self.actor.try_apply_chunks(&block_hash).unwrap();
            self.run_internal_events();
            self.core_writer_actor.handle(ProcessedBlock { block_hash });
            ret
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
                    self.actor.handle_apply_chunks_done(event).unwrap();
                }
                if events_processed == 0 {
                    break;
                }
            }
        }
    }
}
