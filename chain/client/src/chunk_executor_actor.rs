use crate::spice_chunk_validator_actor::send_spice_chunk_endorsement;
use crate::spice_data_distributor_actor::SpiceDataDistributorAdapter;
use crate::spice_data_distributor_actor::SpiceDistributorOutgoingReceipts;
use crate::spice_data_distributor_actor::SpiceDistributorStateWitness;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_chain::BlockHeader;
use near_chain::ChainStoreAccess;
use near_chain::PendingShardJobs;
use near_chain::chain::{NewChunkData, NewChunkResult, ShardContext, StorageContext};
use near_chain::sharding::get_receipts_shuffle_salt;
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::spice_chunk_application::build_spice_apply_chunk_block_context;
use near_chain::spice_core::SpiceCoreReader;
use near_chain::spice_core_writer_actor::ExecutionResultEndorsed;
use near_chain::spice_core_writer_actor::ProcessedBlock;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::state_sync::is_sync_prev_hash;
use near_chain::types::ApplyChunkResult;
use near_chain::types::Tip;
use near_chain::types::{ApplyChunkBlockContext, RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::{ShardUpdateReason, ShardUpdateResult, process_shard_update};
use near_chain::{
    Block, Chain, ChainGenesis, ChainStore, ChainUpdate, DoomslugThresholdMode, Error,
    collect_receipts, get_chunk_clone_from_header,
};
use near_chain_configs::MutableValidatorSigner;
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardProof;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{CodeHash, ContractUpdates};
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateTransition;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::spice_state_witness::compute_contract_accesses_hash;
use near_primitives::types::BlockExecutionResults;
use near_primitives::types::BlockHeight;
use near_primitives::types::ChunkExecutionResult;
use near_primitives::types::ChunkExecutionResultHash;
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
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::instrument;

#[derive(Clone, Debug)]
pub struct ChunkExecutorConfig {
    pub save_trie_changes: bool,
    pub save_tx_outcomes: bool,
    pub save_receipt_to_tx: bool,
    pub save_state_changes: bool,
}

impl Default for ChunkExecutorConfig {
    fn default() -> Self {
        Self {
            save_trie_changes: true,
            save_tx_outcomes: true,
            save_receipt_to_tx: true,
            save_state_changes: true,
        }
    }
}

/// Data required for validators to initiate the chunk application
struct ChunkExecutionData {
    pub witness: SpiceChunkStateWitness,
    pub code_accesses: HashSet<CodeHash>,
}

pub struct ChunkExecutorActor {
    pub(crate) chain_store: ChainStore,
    pub(crate) runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub(crate) epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) shard_tracker: ShardTracker,
    network_adapter: PeerManagerAdapter,
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    myself_sender: Sender<ExecutorApplyChunksDone>,
    pub(crate) core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    data_distributor_adapter: SpiceDataDistributorAdapter,

    /// Under SPICE chunk apply is asynchronous, so the chain-side snapshot
    /// trigger (`Chain::process_snapshot`) fires too early and is gated off.
    /// The executor instead invokes the callback after `process_apply_chunk_results`
    /// commits, so the snapshot freezes a trie that actually contains the
    /// post-execution state for tracked shards.
    snapshot_callbacks: Option<SnapshotCallbacks>,

    blocks_in_execution: HashSet<CryptoHash>,
    pending_unverified_receipts: HashMap<CryptoHash, Vec<ExecutorIncomingUnverifiedReceipts>>,

    pub(crate) validator_signer: MutableValidatorSigner,
    pub(crate) core_reader: SpiceCoreReader,

    /// Test-only stub that satisfies missing receipts by reading from peer
    /// stores. Wired by `TestLoopBuilder::with_spice_receipt_stub()`. Production
    /// builds replace this with the real T2 pull (PR 2/3).
    #[cfg(feature = "test_features")]
    pub(crate) spice_receipt_stub: Option<Arc<dyn SpiceReceiptStubAdapter>>,
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
        snapshot_callbacks: Option<SnapshotCallbacks>,
        config: ChunkExecutorConfig,
    ) -> Self {
        let core_reader =
            SpiceCoreReader::new(store.chain_store(), epoch_manager.clone(), genesis.gas_limit);
        let chain_store =
            ChainStore::new(store, config.save_trie_changes, genesis.transaction_validity_period)
                .with_save_tx_outcomes(config.save_tx_outcomes)
                .with_save_receipt_to_tx(config.save_receipt_to_tx)
                .with_save_state_changes(config.save_state_changes);
        Self {
            chain_store,
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            network_adapter,
            apply_chunks_spawner,
            myself_sender,
            blocks_in_execution: HashSet::new(),
            validator_signer,
            core_reader,
            data_distributor_adapter,
            core_writer_sender,
            snapshot_callbacks,
            pending_unverified_receipts: HashMap::new(),
            #[cfg(feature = "test_features")]
            spice_receipt_stub: None,
        }
    }

    #[cfg(feature = "test_features")]
    pub fn set_spice_receipt_stub(&mut self, stub: Arc<dyn SpiceReceiptStubAdapter>) {
        self.spice_receipt_stub = Some(stub);
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
        // Race fix: a parallel catchup of `prev` may have committed prev's
        // catchup-shard chunk_extra *while* our regular apply was running. The
        // cascade inside `process_apply_chunk_results` for that prev-catchup
        // walked `BlocksToCatchup[prev]` and saw this block, but bailed with
        // `BlockAlreadyAccepted` because we were still in `blocks_in_execution`.
        // Now that we've cleared `blocks_in_execution`, retry CatchingUp — but
        // only when `prev` is fully caught up (otherwise CatchingUp would
        // defer again, violating `try_apply_chunks_with_mode`'s assert that
        // CatchingUp produces no defers).
        if let Ok(header) = self.chain_store.get_block_header(&block_hash) {
            let prev_hash = *header.prev_hash();
            let block_in_catchup =
                self.chain_store.get_blocks_to_catchup(&prev_hash).contains(&block_hash);
            let prev_caught_up =
                self.chain_store.get_block_header(&prev_hash).ok().is_none_or(|prev_header| {
                    !self
                        .chain_store
                        .get_blocks_to_catchup(prev_header.prev_hash())
                        .contains(&prev_hash)
                });
            if block_in_catchup && prev_caught_up {
                if let Err(err) =
                    self.try_apply_chunks_with_mode(&block_hash, Some(ApplyChunksMode::CatchingUp))
                {
                    tracing::error!(
                        target: "chunk_executor",
                        ?err, %block_hash,
                        "failed self-catchup retry after regular apply",
                    );
                }
            }
        }
        self.try_process_next_blocks(&block_hash)
            .map_err(HandleExecutorApplyChunksDoneError::TryProcessNextBlocks)
    }
}

impl near_async::messaging::Actor for ChunkExecutorActor {
    fn start_actor(&mut self, _ctx: &mut dyn near_async::futures::DelayedActionRunner<Self>) {
        if !cfg!(feature = "protocol_feature_spice") {
            return;
        }
        if let Err(err) = self.process_all_ready_blocks() {
            tracing::error!(
                target: "chunk_executor",
                ?err,
                "failed when trying to process all ready blocks on start up",
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

/// Notifies the executor that receipt proofs for `(block_hash, *, to_shard_id)`
/// have been written to `DBCol::receipt_proofs()`. The executor retries forward
/// execution for `block_hash`'s children.
///
/// Production sender (PR 2): the receipt-pull response handler on
/// `SpiceDataDistributorActor`. Test-only sender (PR 1): `SpiceReceiptStub`.
#[derive(Debug, Clone)]
pub struct ReceiptProofsAvailable {
    pub block_hash: CryptoHash,
    pub to_shard_id: ShardId,
}

/// Sent after `Chain::set_state_finalize` writes the chunk_extra at `sync_hash`
/// for a newly-synced shard. The executor walks `BlocksToCatchup(sync_hash)`
/// and retries `try_apply_chunks` for each child whose deferred shard now has
/// its prev chunk_extra available. Successful applies cascade forward via the
/// existing `try_process_next_blocks` plumbing.
#[derive(Debug, Clone)]
pub struct SpiceStateSyncFinalized {
    pub sync_hash: CryptoHash,
    pub shard_id: ShardId,
}

/// Test-only injection point used by `SpiceReceiptStub` in test-loop tests to
/// supply receipt proofs from peer stores when the executor reports
/// `MissingReceipts`. This trait is the bridge: the production executor knows
/// nothing about peer stores; the stub holds them.
///
/// PR 2/3 will replace this with a real T2 receipt-pull request from
/// `SpiceDataDistributorActor` to a chunk producer of the source shard.
#[cfg(feature = "test_features")]
pub trait SpiceReceiptStubAdapter: Send + Sync + std::fmt::Debug {
    /// Asynchronously satisfy receipt proofs for `(block_hash, *, to_shard_id)`.
    /// On success the implementor sends `ReceiptProofsAvailable` back to the
    /// executor; the executor relies on that callback to retry execution.
    fn satisfy(&self, block_hash: CryptoHash, to_shard_id: ShardId);
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

impl Handler<ReceiptProofsAvailable> for ChunkExecutorActor {
    fn handle(
        &mut self,
        ReceiptProofsAvailable { block_hash, to_shard_id }: ReceiptProofsAvailable,
    ) {
        tracing::debug!(
            target: "chunk_executor",
            %block_hash,
            %to_shard_id,
            "receipt proofs available, retrying forward execution",
        );
        if let Err(err) = self.try_process_next_blocks(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, %block_hash, "failed to process next blocks after receipts arrived");
        }
    }
}

impl Handler<SpiceStateSyncFinalized> for ChunkExecutorActor {
    fn handle(&mut self, SpiceStateSyncFinalized { sync_hash, shard_id }: SpiceStateSyncFinalized) {
        // V3 anchors at sync_prev_prev — that's the block whose chunk_extra we
        // just persisted via `set_state_finalize`. Catchup walks forward from
        // there: children of sync_prev_prev are the first blocks needing apply
        // for the newly-synced shard.
        let sync_prev_prev_hash = match self.chain_store.get_block_header(&sync_hash) {
            Ok(sync_header) => match self.chain_store.get_block_header(sync_header.prev_hash()) {
                Ok(sync_prev_header) => *sync_prev_header.prev_hash(),
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, %sync_hash, "missing sync_prev header");
                    return;
                }
            },
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, %sync_hash, "missing sync_hash header");
                return;
            }
        };
        let pending = self.chain_store.get_blocks_to_catchup(&sync_prev_prev_hash);
        tracing::debug!(
            target: "chunk_executor",
            %sync_hash, %sync_prev_prev_hash, %shard_id, num_pending=pending.len(),
            "state sync finalized for shard, retrying deferred catchup blocks",
        );
        for block_hash in pending {
            // Catchup mode restricts to next-epoch shards we'll newly track —
            // exactly what state sync just delivered. Mirrors pre-SPICE
            // `catchup_blocks_step`.
            match self.try_apply_chunks_with_mode(&block_hash, Some(ApplyChunksMode::CatchingUp)) {
                Ok(TryApplyChunksOutcome::Scheduled)
                | Ok(TryApplyChunksOutcome::BlockIrrelevant)
                | Ok(TryApplyChunksOutcome::BlockAlreadyAccepted) => {}
                Ok(TryApplyChunksOutcome::NotReady(reason)) => {
                    tracing::debug!(
                        target: "chunk_executor",
                        ?reason, %block_hash, %shard_id,
                        "catchup block still not ready after state sync finalize",
                    );
                }
                Err(err) => {
                    tracing::error!(
                        target: "chunk_executor",
                        ?err, %block_hash, %shard_id,
                        "failed to apply catchup block after state sync finalize",
                    );
                }
            }
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
    #[instrument(target = "chunk_executor", level = "debug", skip_all, fields(%block_hash))]
    fn try_apply_chunks(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<TryApplyChunksOutcome, Error> {
        self.try_apply_chunks_with_mode(block_hash, None)
    }

    /// Same as `try_apply_chunks` but allows the caller (catchup walk) to
    /// override the apply mode to `CatchingUp`, which restricts to the
    /// newly-tracked next-epoch shards (mirroring pre-SPICE
    /// `catchup_blocks_step`).
    fn try_apply_chunks_with_mode(
        &mut self,
        block_hash: &CryptoHash,
        mode_override: Option<ApplyChunksMode>,
    ) -> Result<TryApplyChunksOutcome, Error> {
        if self.blocks_in_execution.contains(block_hash) {
            return Ok(TryApplyChunksOutcome::BlockAlreadyAccepted);
        }
        let block = self.chain_store.get_block(block_hash)?;
        // Catchup intentionally re-applies blocks that may already be at or
        // before `spice_final_execution_head` (final advanced from
        // `last_final_block` independently of catchup progress on synced
        // shards). Skip the descendant check in catchup mode.
        let in_catchup = matches!(mode_override, Some(ApplyChunksMode::CatchingUp));
        if !in_catchup && !is_descendant_of_final_execution_head(&self.chain_store, block.header())
        {
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
        let mut deferred_shards: Vec<ShardId> = Vec::new();
        let prev_block_epoch_id = self.epoch_manager.get_epoch_id(prev_block_hash)?;
        let prev_block_shard_ids = self.epoch_manager.shard_ids(&prev_block_epoch_id)?;
        let current_block_shard_layout =
            self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        let apply_chunks_mode = match mode_override {
            Some(mode) => mode,
            None => apply_chunks_mode_from_prev_hash(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                &self.shard_tracker,
                prev_block_hash,
            )?,
        };
        let mut already_applied_any = false;
        for &prev_block_shard_id in &prev_block_shard_ids {
            // TODO(spice-resharding): convert `prev_block_shard_id` into `shard_id` for
            // the current shard layout
            let current_block_shard_id = prev_block_shard_id;
            if !self.shard_tracker.should_apply_chunk(
                apply_chunks_mode,
                prev_block_hash,
                current_block_shard_id,
            ) {
                continue;
            }
            // Existing chunk extra means that the chunk for that shard was already applied.
            if self.chunk_extra_exists(block_hash, current_block_shard_id)? {
                already_applied_any = true;
                continue;
            }

            let Some(prev_chunk_chunk_extra) =
                self.get_chunk_extra(prev_block_hash, prev_block_shard_id)?
            else {
                // No local prev chunk_extra for this shard. Typically a cross-epoch
                // shard switch where this validator didn't track this shard in the
                // previous epoch and is waiting on state sync to populate the
                // chunk_extra at the sync hash. Defer this shard; once state sync
                // completes the catchup walk will retry.
                deferred_shards.push(prev_block_shard_id);
                continue;
            };

            let incoming_receipts = if prev_block.header().is_genesis() {
                // Genesis block has no outgoing receipts.
                vec![]
            } else {
                let proofs =
                    get_receipt_proofs_for_shard(&store, prev_block_hash, prev_block_shard_id);
                if proofs.len() != prev_block_shard_ids.len() {
                    let to_shard_id = prev_block_shard_id;
                    // Test-only: ask the stub to fetch the missing receipts from a
                    // peer node's store. The stub will save them locally and send
                    // `ReceiptProofsAvailable` to retry. PR 2/3 replaces this with
                    // a real T2 receipt pull.
                    #[cfg(feature = "test_features")]
                    if let Some(stub) = &self.spice_receipt_stub {
                        stub.satisfy(*prev_block_hash, to_shard_id);
                    }
                    // Receipts arrive via network; don't add to the catchup column.
                    // The block stays in the executor's regular retry path, driven
                    // by `ReceiptProofsAvailable`. Bail on the whole block so we
                    // don't half-apply with stale state.
                    return Ok(TryApplyChunksOutcome::missing_receipts(
                        *prev_block_hash,
                        &proofs,
                        to_shard_id,
                        &prev_block_shard_ids,
                    ));
                }
                proofs
            };

            let shard_index = current_block_shard_layout.get_shard_index(current_block_shard_id)?;
            let chunk_header = block_chunk_headers
                .get(shard_index)
                .ok_or(Error::InvalidShardId(current_block_shard_id))?;

            let shard_uid = ShardUId::from_shard_id_and_layout(
                current_block_shard_id,
                &current_block_shard_layout,
            );
            chunk_contexts.push(ChunkApplicationContext {
                incoming_receipts,
                prev_chunk_chunk_extra,
                shard_uid,
                chunk_header,
            });
        }

        if !deferred_shards.is_empty() {
            self.add_block_to_catchup(prev_block_hash, block_hash)?;
        }

        if chunk_contexts.is_empty() {
            if let Some(&first_deferred) = deferred_shards.first() {
                return Ok(TryApplyChunksOutcome::previous_block_is_not_executed(
                    *prev_block_hash,
                    first_deferred,
                ));
            }
            // Empty chunk_contexts can mean either "every tracked shard is
            // already applied" or "we don't track any shard for this block".
            // Only the former is `BlockAlreadyAccepted`; the latter must
            // schedule an empty apply so the cascade in
            // `try_process_next_blocks` walks forward (matching pre-PR
            // behavior).
            if already_applied_any {
                return Ok(TryApplyChunksOutcome::BlockAlreadyAccepted);
            }
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

    /// Persist that `block_hash` has at least one shard pending catchup, keyed by `prev_hash`.
    /// Idempotent — duplicate calls are a no-op. Used after `try_apply_chunks` decides to
    /// defer one or more shards because the local chunk_extra for the prev block is missing
    /// (typically a cross-epoch shard switch awaiting state sync).
    fn add_block_to_catchup(
        &self,
        prev_hash: &CryptoHash,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let mut existing = self.chain_store.get_blocks_to_catchup(prev_hash);
        if existing.contains(block_hash) {
            return Ok(());
        }
        existing.push(*block_hash);
        let mut store_update = self.chain_store.store().store_update();
        store_update.set_ser(DBCol::BlocksToCatchup, prev_hash.as_ref(), &existing);
        store_update.commit();
        Ok(())
    }

    fn try_process_next_blocks(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let next_block_hashes = self.chain_store.get_all_next_block_hashes(block_hash);
        let catchup_block_hashes: HashSet<CryptoHash> =
            self.chain_store.get_blocks_to_catchup(block_hash).into_iter().collect();
        if next_block_hashes.is_empty() {
            // Next block wasn't received yet.
            tracing::debug!(target: "chunk_executor", %block_hash, "no next block hash is available");
            return Ok(());
        }
        for next_block_hash in next_block_hashes {
            // If this child is in `BlocksToCatchup`, retry it in catchup mode
            // so the newly-tracked next-epoch shard gets applied. Other
            // descendants run with the regular (NotCaughtUp/IsCaughtUp) mode.
            let mode_override = if catchup_block_hashes.contains(&next_block_hash) {
                Some(ApplyChunksMode::CatchingUp)
            } else {
                None
            };
            match self.try_apply_chunks_with_mode(&next_block_hash, mode_override)? {
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
        &mut self,
        block_hash: CryptoHash,
        results: Result<Vec<ShardUpdateResult>, FailedToApplyChunkError>,
    ) -> Result<(), Error> {
        let block = self.chain_store.get_block(&block_hash).unwrap();
        // Catchup applies blocks at heights at or below `spice_final_execution_head`
        // (final advanced based on last_final_block independently of catchup
        // progress on synced shards). Skip the descendant check for catchup
        // targets — symmetric to the skip in `try_apply_chunks_with_mode`.
        let prev_hash = block.header().prev_hash();
        let in_catchup = self.chain_store.get_blocks_to_catchup(prev_hash).contains(&block_hash);
        if !in_catchup && !is_descendant_of_final_execution_head(&self.chain_store, block.header())
        {
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
        near_chain::metrics::BLOCK_HEIGHT_SPICE_EXECUTION_HEAD.set(block.header().height() as i64);
        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        for result in &results {
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

            let Some(my_signer) = self.validator_signer.get() else {
                // If node isn't validator it shouldn't send outgoing receipts, endorsements and witnesses.
                // RPC nodes can still apply chunks and tracks multiple shards.
                continue;
            };

            // Endorse if we are a chunk validator (regardless of producer status)
            let validators_at_height = self.epoch_manager.get_chunk_validator_assignments(
                &epoch_id,
                shard_id,
                block.header().height(),
            )?;
            if validators_at_height.contains(my_signer.validator_id()) {
                self.send_chunk_endorsement(
                    &block,
                    &my_signer,
                    new_chunk_result,
                    outgoing_receipts_root,
                );
            }

            // Distribute witness and receipts if we are the chunk producer for the shard.
            let epoch_producers =
                self.epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, shard_id)?;
            if epoch_producers.contains(my_signer.validator_id()) {
                self.send_outgoing_receipts(&block, receipt_proofs);
                self.distribute_witness(&block, new_chunk_result, outgoing_receipts_root)?;
            }
        }

        let mut chain_update = self.chain_update();
        let should_save_state_transition_data = false;
        chain_update.apply_chunk_postprocessing(
            &block,
            results,
            should_save_state_transition_data,
        )?;
        let final_execution_head = chain_update.update_spice_final_execution_head(&block)?;
        chain_update.save_spice_execution_head(block.header())?;
        chain_update.commit()?;
        // If the just-completed apply finished off the last pending shard for this
        // block, drop its catchup entry so future state-sync walks don't revisit it.
        self.maybe_remove_block_from_catchup(&block)?;
        // If this apply was part of a catchup walk (i.e. children of this block
        // are in BlocksToCatchup awaiting the new shard), cascade the walk
        // forward using CatchingUp mode so the new-shard chunk_extras get
        // populated for the descendant blocks too.
        for child_hash in self.chain_store.get_blocks_to_catchup(&block_hash) {
            if let Err(err) =
                self.try_apply_chunks_with_mode(&child_hash, Some(ApplyChunksMode::CatchingUp))
            {
                tracing::error!(
                    target: "chunk_executor",
                    ?err, %child_hash,
                    "failed catchup cascade",
                );
            }
        }
        // Pre-SPICE the snapshot trigger fires from chain processing because
        // chunks are applied synchronously inside `start_process_block_impl`.
        // Under SPICE chunk apply is async — the chain trigger is gated off
        // (see `Chain::should_make_snapshot`) and we fire it here instead, so
        // the snapshot freezes a trie that actually contains the just-applied
        // post-execution state for the shards we track.
        self.maybe_make_spice_snapshot(&block)?;
        if let Some(final_execution_head) = final_execution_head {
            self.update_flat_storage_head(&shard_layout, &final_execution_head)?;
            self.gc_memtrie_roots(&shard_layout, &final_execution_head);
        }
        Ok(())
    }

    /// Fire the state snapshot callback if `block` is the prev block of an
    /// upcoming sync_hash (`is_sync_prev_hash`). Mirrors the per-shard filter
    /// from `Chain::process_snapshot`.
    fn maybe_make_spice_snapshot(&self, block: &Block) -> Result<(), Error> {
        let Some(snapshot_callbacks) = &self.snapshot_callbacks else { return Ok(()) };
        let tip = Tip::from_header(block.header());
        if !is_sync_prev_hash(&self.chain_store, &tip)? {
            return Ok(());
        }
        let prev_prev_hash = block.header().prev_hash();
        let mut min_chunk_prev_height = None;
        for chunk in block.chunks().iter() {
            let prev_height = if chunk.prev_block_hash() == &CryptoHash::default() {
                0
            } else {
                self.chain_store.get_block_header(chunk.prev_block_hash())?.height()
            };
            min_chunk_prev_height = Some(match min_chunk_prev_height {
                Some(m) => std::cmp::min(m, prev_height),
                None => prev_height,
            });
        }
        let min_chunk_prev_height = min_chunk_prev_height.unwrap_or(0);
        let epoch_height = self.epoch_manager.get_epoch_height_from_prev_block(prev_prev_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout_from_prev_block(prev_prev_hash)?;
        let shard_uids = shard_layout
            .shard_uids()
            .enumerate()
            .filter(|&(_, shard_uid)| {
                self.shard_tracker.cares_about_shard(prev_prev_hash, shard_uid.shard_id())
            })
            .collect();
        (snapshot_callbacks.make_snapshot_callback)(
            min_chunk_prev_height,
            epoch_height,
            shard_uids,
            Arc::new(block.clone()),
        );
        Ok(())
    }

    /// If every shard this validator tracks for `block` now has a chunk_extra (i.e.
    /// no shards remain deferred for catchup), drop the block's entry from
    /// `BlocksToCatchup`. Idempotent.
    ///
    /// The "fully caught up" question is independent of the dynamic apply
    /// mode used in `try_apply_chunks`: a block is only ready to leave
    /// `BlocksToCatchup` once *every* shard we'd track including next-epoch
    /// pre-apply (i.e. `IsCaughtUp` shards) has a chunk_extra. Otherwise we'd
    /// drop the entry the moment the current-epoch shard applied, leaving the
    /// next-epoch shard quietly stuck.
    fn maybe_remove_block_from_catchup(&self, block: &Block) -> Result<(), Error> {
        let block_hash = block.hash();
        let prev_block_hash = block.header().prev_hash();
        let prev_epoch_id = self.epoch_manager.get_epoch_id(prev_block_hash)?;
        let shard_ids = self.epoch_manager.shard_ids(&prev_epoch_id)?;
        for shard_id in shard_ids {
            if !self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                prev_block_hash,
                shard_id,
            ) {
                continue;
            }
            if !self.chunk_extra_exists(block_hash, shard_id)? {
                return Ok(());
            }
        }
        self.remove_block_from_catchup(prev_block_hash, block_hash)
    }

    fn remove_block_from_catchup(
        &self,
        prev_hash: &CryptoHash,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        let mut existing = self.chain_store.get_blocks_to_catchup(prev_hash);
        let initial_len = existing.len();
        existing.retain(|h| h != block_hash);
        if existing.len() == initial_len {
            return Ok(());
        }
        let mut store_update = self.chain_store.store().store_update();
        if existing.is_empty() {
            store_update.delete(DBCol::BlocksToCatchup, prev_hash.as_ref());
        } else {
            store_update.set_ser(DBCol::BlocksToCatchup, prev_hash.as_ref(), &existing);
        }
        store_update.commit();
        Ok(())
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
        match get_chunk_clone_from_header(&self.chain_store.chunk_store(), chunk_header) {
            Ok(chunk) => Ok(Some(chunk)),
            Err(Error::ChunkMissing(_))
                if self.chain_store.is_invalid_chunk(chunk_header.chunk_hash()).is_some() =>
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
                let tx_valid_list =
                    self.chain_store.compute_transaction_validity(prev_block_header, &chunk);
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
        Ok((
            shard_uid.shard_id(),
            Box::new(move |parent_span| -> Result<ShardUpdateResult, Error> {
                Ok(process_shard_update(
                    parent_span,
                    runtime.as_ref(),
                    shard_update_reason,
                    ShardContext { shard_uid, should_apply_chunk: true },
                    None,
                )?)
            }),
        ))
    }

    pub(crate) fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<Arc<ChunkExtra>>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?;
        match self.chain_store.get_chunk_extra(block_hash, &shard_uid) {
            Ok(chunk_extra) => Ok(Some(chunk_extra)),
            Err(Error::DBNotFoundErr(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn chunk_extra_exists(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<bool, Error> {
        self.get_chunk_extra(block_hash, shard_id).map(|option| option.is_some())
    }

    fn chain_update(&mut self) -> ChainUpdate<'_> {
        ChainUpdate::new(
            &mut self.chain_store,
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            // Since we don't produce blocks, this argument is irrelevant.
            DoomslugThresholdMode::NoApprovals,
        )
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

    fn update_flat_storage_head(
        &self,
        shard_layout: &ShardLayout,
        final_execution_head: &Tip,
    ) -> Result<(), Error> {
        // TODO(spice): Evaluate if using block before final_execution_head still makes sense for
        // spice. For now it's used mainly because it's used for updating flat head without spice
        // with the following reasoning:
        // Using prev_block_hash should be required for `StateSnapshot` to be able to make snapshot of
        // flat storage at the epoch boundary.
        let new_flat_head = final_execution_head.prev_block_hash;
        // TODO(spice): handle state sync and resharding edge cases when updating flat head.

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
        let header =
            self.chain_store.get_block_header(&final_execution_head.last_block_hash).unwrap();
        let Some(chain_prev_height) = header.prev_height() else {
            return;
        };
        for shard_uid in shard_layout.shard_uids() {
            // Under SPICE the executor's catchup walk for a newly-tracked shard runs
            // async and may lag chain finality. We must not delete memtrie state for
            // heights the catchup walk hasn't reached yet, otherwise the next apply
            // would fail to find its prev state in memtrie.
            //
            // Find the highest block on the canonical-from-final-execution-head chain
            // for which we have a chunk_extra for this shard — that's our local
            // executed head for this shard. Cap the gc threshold by it.
            let shard_executed_height = self.find_executed_head_height_for_shard(
                &final_execution_head.last_block_hash,
                shard_uid,
            );
            let prev_height = match shard_executed_height {
                Some(h) if h >= header.height() => chain_prev_height,
                Some(h) => h.saturating_sub(1).min(chain_prev_height),
                None => continue,
            };
            let tries = self.runtime_adapter.get_tries();
            tries.delete_memtrie_roots_up_to_height(shard_uid, prev_height);
        }
    }

    /// Walks back from `final_head_hash` along prev_hash links until it finds a
    /// block for which the validator has a `chunk_extra` saved for `shard_uid`.
    /// Returns that block's height, or `None` if no such block exists in the
    /// chain (e.g. shard never tracked locally).
    fn find_executed_head_height_for_shard(
        &self,
        final_head_hash: &CryptoHash,
        shard_uid: ShardUId,
    ) -> Option<BlockHeight> {
        let mut hash = *final_head_hash;
        loop {
            if self.chain_store.get_chunk_extra(&hash, &shard_uid).is_ok() {
                let header = self.chain_store.get_block_header(&hash).ok()?;
                return Some(header.height());
            }
            let header = self.chain_store.get_block_header(&hash).ok()?;
            if header.is_genesis() {
                return None;
            }
            hash = *header.prev_hash();
        }
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
pub fn save_receipt_proof(
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

pub fn get_receipt_proofs_for_shard(
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

/// Picks the apply-chunks mode for the block whose `prev_hash` is given.
/// Returns `NotCaughtUp` if either:
/// - the prev block is in `DBCol::BlocksToCatchup` (state-sync catchup is in
///   flight for one of its shards), or
/// - the block being processed is the first of a new epoch and the chain has
///   shards we need to state-sync for the next epoch.
/// Otherwise returns `IsCaughtUp`, which permits speculatively pre-applying
/// next-epoch shards.
///
/// This mirrors the pre-SPICE `is_caught_up` decision in
/// `Chain::get_catchup_and_state_sync_infos`: not-caught-up either because of
/// in-flight catchup, or because we're entering an epoch where a shard we'll
/// track in the *next* epoch needs syncing.
pub(crate) fn apply_chunks_mode_from_prev_hash(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    prev_hash: &CryptoHash,
) -> Result<ApplyChunksMode, Error> {
    let prev_header = chain_store.get_block_header(prev_hash)?;
    let prev_is_caught_up = ChainStore::prev_block_is_caught_up(
        chain_store,
        prev_header.prev_hash(),
        prev_header.hash(),
    );
    if !prev_is_caught_up {
        return Ok(ApplyChunksMode::NotCaughtUp);
    }
    // `is_next_block_epoch_start(prev_hash)` means the block after `prev_hash`
    // is the first of a new epoch. The block_hash arg to `get_state_sync_info`
    // is only used to populate the returned StateSyncInfo and doesn't affect
    // the empty/non-empty decision, so we can pass `prev_hash` as a stand-in
    // when we don't have the new block's hash yet.
    if epoch_manager.is_next_block_epoch_start(prev_hash)? {
        if shard_tracker.get_state_sync_info(prev_hash, prev_hash)?.is_some() {
            return Ok(ApplyChunksMode::NotCaughtUp);
        }
    }
    Ok(ApplyChunksMode::IsCaughtUp)
}

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
    // Walk back via `prev_hash` until we either reach `final_execution_head`'s
    // height, or hit a header that isn't on disk. Missing headers are expected
    // — the BlockSync window doesn't go down to genesis, and `final_execution_head`
    // defaults to genesis until the chunk_executor advances it. If the walk
    // can't reach `final_execution_head`, we know this header isn't above it
    // along the same chain, so treat as not-a-descendant (skip the apply).
    //
    // TODO(spice): a tighter alternative is to plumb `Provenance` through
    // `ProcessedBlock` to the chunk_executor and skip this descendant check
    // for `Provenance::SYNC` blocks (mirroring the existing `in_catchup`
    // skip), so we don't depend on header availability at all.
    let mut prev_hash = *header.prev_hash();
    while height > final_execution_head.height {
        let header = match chain_store.get_block_header(&prev_hash) {
            Ok(h) => h,
            Err(Error::DBNotFoundErr(_)) => return false,
            Err(err) => panic!("failed to load block header during descendant check: {err:?}"),
        };
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
    use near_chain::spice_core_writer_actor::SpiceCoreWriterActor;
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
            chunk_executor_config: ChunkExecutorConfig,
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
                    None,
                    chunk_executor_config,
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
