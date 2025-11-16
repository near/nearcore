use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use near_async::futures::AsyncComputationSpawner;
use near_async::futures::AsyncComputationSpawnerExt;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_chain::ApplyChunksIterationMode;
use near_chain::BlockHeader;
use near_chain::ChainStoreAccess;
use near_chain::chain::{NewChunkData, NewChunkResult, ShardContext, StorageContext};
use near_chain::sharding::get_receipts_shuffle_salt;
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::spice_chunk_application::build_spice_apply_chunk_block_context;
use near_chain::spice_core::SpiceCoreReader;
use near_chain::spice_core_writer_actor::ExecutionResultEndorsed;
use near_chain::spice_core_writer_actor::ProcessedBlock;
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
use near_primitives::sharding::ShardChunk;
use near_primitives::sharding::ShardProof;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateTransition;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateWitness;
use near_primitives::types::BlockExecutionResults;
use near_primitives::types::BlockHeight;
use near_primitives::types::ChunkExecutionResult;
use near_primitives::types::ChunkExecutionResultHash;
use near_primitives::types::EpochId;
use near_primitives::types::SpiceChunkId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{Gas, ShardId};
use near_primitives::utils::get_receipt_proof_key;
use near_primitives::utils::get_receipt_proof_target_shard_prefix;
use near_primitives::utils::get_witnesses_key;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::DBCol;
use near_store::Store;
use near_store::StoreUpdate;
use near_store::TrieDBStorage;
use near_store::TrieStorage;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::trie_store::TrieStoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use rayon::iter::IntoParallelIterator as _;
use rayon::iter::ParallelIterator as _;
use tracing::instrument;

use crate::spice_chunk_validator_actor::send_spice_chunk_endorsement;
use crate::spice_data_distributor_actor::SpiceDataDistributorAdapter;
use crate::spice_data_distributor_actor::SpiceDistributorOutgoingReceipts;
use crate::spice_data_distributor_actor::SpiceDistributorStateWitness;

pub struct ChunkExecutorActor {
    pub(crate) chain_store: ChainStore,
    pub(crate) runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub(crate) epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) shard_tracker: ShardTracker,
    network_adapter: PeerManagerAdapter,
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    apply_chunks_iteration_mode: ApplyChunksIterationMode,
    myself_sender: Sender<ExecutorApplyChunksDone>,
    pub(crate) core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    data_distributor_adapter: SpiceDataDistributorAdapter,

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
        apply_chunks_iteration_mode: ApplyChunksIterationMode,
        myself_sender: Sender<ExecutorApplyChunksDone>,
        core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
        data_distributor_adapter: SpiceDataDistributorAdapter,
    ) -> Self {
        let core_reader = SpiceCoreReader::new(store.chain_store(), epoch_manager.clone());
        Self {
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            network_adapter,
            apply_chunks_spawner,
            apply_chunks_iteration_mode,
            myself_sender,
            blocks_in_execution: HashSet::new(),
            validator_signer,
            core_reader,
            data_distributor_adapter,
            core_writer_sender,
            pending_unverified_receipts: HashMap::new(),
        }
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

impl Handler<ExecutorApplyChunksDone> for ChunkExecutorActor {
    // TODO(spice): We should request relevant receipts from spice data distributor either
    // after we finished executing a relevant block or once we know about a block.
    fn handle(
        &mut self,
        ExecutorApplyChunksDone { block_hash, apply_results }: ExecutorApplyChunksDone,
    ) {
        if let Err(err) = self.process_apply_chunk_results(block_hash, apply_results) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to process apply chunk results");
            return;
        };
        assert!(self.blocks_in_execution.remove(&block_hash));
        if let Err(err) = self.try_process_next_blocks(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to process next blocks");
        }
    }
}

// Though it's useful for debugging clippy treats all fields as dead code.
#[allow(dead_code)]
#[derive(Debug)]
enum NotReadyToApplyChunksReason {
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

enum TryApplyChunksOutcome {
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
        let execution_results = core_reader.get_execution_results_by_shard_id(&prev_block)?;
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

impl ChunkExecutorActor {
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

        let header = block.header();
        let prev_block_hash = header.prev_hash();
        let prev_block = self.chain_store.get_block(&prev_block_hash)?;
        let store = self.chain_store.store();

        let Some(prev_block_execution_results) =
            self.core_reader.get_block_execution_results(&prev_block)?
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

        let mut all_receipts: HashMap<ShardId, Vec<ReceiptProof>> = HashMap::new();
        let prev_block_epoch_id = self.epoch_manager.get_epoch_id(prev_block_hash)?;
        let prev_block_shard_ids = self.epoch_manager.shard_ids(&prev_block_epoch_id)?;
        for &prev_block_shard_id in &prev_block_shard_ids {
            // TODO(spice-resharding): convert `prev_block_shard_id` into `shard_id` for
            // the current shard layout
            let current_block_shard_id = prev_block_shard_id;
            if self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                prev_block_hash,
                current_block_shard_id,
            ) {
                // Existing chunk extra means that the chunk for that shard was already applied
                if self.chunk_extra_exists(block_hash, current_block_shard_id)? {
                    return Ok(TryApplyChunksOutcome::BlockAlreadyAccepted);
                }

                // Genesis block has no outgoing receipts.
                if prev_block.header().is_genesis() {
                    all_receipts.insert(prev_block_shard_id, vec![]);
                    continue;
                }

                if !self.chunk_extra_exists(prev_block_hash, prev_block_shard_id)? {
                    return Ok(TryApplyChunksOutcome::previous_block_is_not_executed(
                        *prev_block_hash,
                        prev_block_shard_id,
                    ));
                }

                let proofs =
                    get_receipt_proofs_for_shard(&store, prev_block_hash, prev_block_shard_id)?;
                if proofs.len() != prev_block_shard_ids.len() {
                    let to_shard_id = prev_block_shard_id;
                    return Ok(TryApplyChunksOutcome::missing_receipts(
                        *prev_block_hash,
                        &proofs,
                        to_shard_id,
                        &prev_block_shard_ids,
                    ));
                }
                all_receipts.insert(prev_block_shard_id, proofs);
            }
        }
        self.schedule_apply_chunks(
            block,
            all_receipts,
            prev_block_execution_results,
            SandboxStatePatch::default(),
        )?;
        self.blocks_in_execution.insert(*block_hash);
        Ok(TryApplyChunksOutcome::Scheduled)
    }

    fn try_process_next_blocks(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let next_block_hashes = self.chain_store.get_all_next_block_hashes(block_hash)?;
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
        block: Arc<Block>,
        mut incoming_receipts: HashMap<ShardId, Vec<ReceiptProof>>,
        prev_block_execution_results: BlockExecutionResults,
        mut state_patch: SandboxStatePatch,
    ) -> Result<(), Error> {
        let prev_hash = block.header().prev_hash();
        let prev_block = self.chain_store.get_block(prev_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;
        let chunk_headers = block.chunks();
        let receipts_shuffle_salt = get_receipts_shuffle_salt(&block);

        let mut jobs = Vec::new();
        // TODO(spice-resharding): Make sure shard logic is correct with resharding.
        for shard_id in shard_layout.shard_ids() {
            // XXX: This is a bit questionable -- sandbox state patching works
            // only for a single shard. This so far has been enough.
            let state_patch = state_patch.take();
            // If we don't care about shard we wouldn't have relevant incoming receipts.
            if !self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                prev_hash,
                shard_id,
            ) {
                continue;
            }

            let block_context = build_spice_apply_chunk_block_context(
                block.header(),
                &prev_block_execution_results,
                self.epoch_manager.as_ref(),
            )?;

            // TODO(spice-resharding): We may need to take resharding into account here.
            let mut receipt_proofs = incoming_receipts
                .remove(&shard_id)
                .expect("expected receipts for all tracked shards");
            shuffle_receipt_proofs(&mut receipt_proofs, receipts_shuffle_salt);

            let storage_context =
                StorageContext { storage_data_source: StorageDataSource::Db, state_patch };

            let shard_index = shard_layout.get_shard_index(shard_id)?;
            let chunk_header =
                chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;

            let chunk = if chunk_header.is_new_chunk(block.header().height()) {
                Some(get_chunk_clone_from_header(&self.chain_store, chunk_header)?)
            } else {
                None
            };
            let job = self.get_update_shard_job(
                block_context,
                shard_id,
                chunk,
                &prev_block,
                ApplyChunksMode::IsCaughtUp,
                &receipt_proofs,
                storage_context,
            );
            match job {
                Ok(Some(job)) => jobs.push(job),
                Ok(None) => {}
                Err(e) => panic!("{e:?}"),
            }
        }

        let apply_done_sender = self.myself_sender.clone();
        let iteration_mode = self.apply_chunks_iteration_mode;
        self.apply_chunks_spawner.spawn("apply_chunks", move || {
            let block_hash = *block.hash();
            let apply_results =
                do_apply_chunks(iteration_mode, &block_hash, block.header().height(), jobs)
                    .into_iter()
                    .map(|(shard_id, result)| {
                        result.map_err(|err| FailedToApplyChunkError { shard_id, err })
                    })
                    .collect();
            apply_done_sender.send(ExecutorApplyChunksDone { block_hash, apply_results });
        });
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
            self.save_produced_receipts(&block_hash, &receipt_proofs)?;

            let Some(my_signer) = self.validator_signer.get() else {
                // If node isn't validator it shouldn't send outgoing receipts, endorsed and witnesses.
                // RPC nodes can still apply chunks and tracks multiple shards.
                continue;
            };
            self.send_outgoing_receipts(&block, receipt_proofs);
            self.distribute_witness(&block, my_signer, new_chunk_result, outgoing_receipts_root)?;
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
        if let Some(final_execution_head) = final_execution_head {
            self.update_flat_storage_head(&shard_layout, &final_execution_head)?;
            self.gc_memtrie_roots(&shard_layout, &final_execution_head)?;
        }
        Ok(())
    }

    fn distribute_witness(
        &self,
        block: &Block,
        my_signer: Arc<ValidatorSigner>,
        new_chunk_result: &NewChunkResult,
        outgoing_receipts_root: CryptoHash,
    ) -> Result<(), Error> {
        let NewChunkResult { shard_uid, gas_limit, apply_result } = new_chunk_result;
        let shard_id = shard_uid.shard_id();

        let execution_result =
            new_execution_result(gas_limit, apply_result, outgoing_receipts_root);
        let execution_result_hash = execution_result.compute_hash();
        if self
            .epoch_manager
            .get_chunk_validator_assignments(
                &block.header().epoch_id(),
                shard_id,
                block.header().height(),
            )?
            .contains(my_signer.validator_id())
        {
            // If we're validator we can send endorsement without witness validation.
            let endorsement = SpiceChunkEndorsement::new(
                SpiceChunkId { block_hash: *block.hash(), shard_id },
                execution_result,
                &my_signer,
            );
            send_spice_chunk_endorsement(
                endorsement.clone(),
                self.epoch_manager.as_ref(),
                &self.network_adapter.clone().into_sender(),
                &my_signer,
            );
            self.core_writer_sender.send(SpiceChunkEndorsementMessage(endorsement));
        }

        let state_witness =
            self.create_witness(block, apply_result, shard_id, execution_result_hash)?;

        save_witness(&self.chain_store, block.hash(), shard_id, &state_witness)?;

        self.data_distributor_adapter.send(SpiceDistributorStateWitness { state_witness });
        Ok(())
    }

    fn create_witness(
        &self,
        block: &Block,
        apply_result: &ApplyChunkResult,
        shard_id: ShardId,
        execution_result_hash: ChunkExecutionResultHash,
    ) -> Result<SpiceChunkStateWitness, Error> {
        let block_hash = block.header().hash();
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash).unwrap();
        let transactions = {
            let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id).unwrap();
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            let chunk_headers = block.chunks();
            let chunk_header = chunk_headers.get(shard_index).unwrap();
            if chunk_header.is_new_chunk(block.header().height()) {
                self.chain_store.get_chunk(chunk_header.chunk_hash()).unwrap().into_transactions()
            } else {
                vec![]
            }
        };

        let applied_receipts_hash = apply_result.applied_receipts_hash;
        let main_transition = {
            let ContractUpdates { contract_accesses, contract_deploys: _ } =
                apply_result.contract_updates.clone();

            let PartialState::TrieValues(mut base_state_values) =
                apply_result.proof.clone().unwrap().nodes;
            let trie_storage = TrieDBStorage::new(
                TrieStoreAdapter::new(self.runtime_adapter.store().clone()),
                shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?,
            );
            base_state_values.reserve_exact(contract_accesses.len());
            for contract_hash in contract_accesses {
                let contract = trie_storage.retrieve_raw_bytes(&contract_hash.0)?;
                base_state_values.push(contract);
            }
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
            )?;
            receipt_proofs
                .into_iter()
                .map(|proof| -> Result<_, Error> { Ok((proof.1.from_shard_id, proof)) })
                .collect::<Result<_, Error>>()?
        };
        // TODO(spice-resharding): Handle witness validation when resharding.
        let state_witness = SpiceChunkStateWitness::new(
            near_primitives::types::SpiceChunkId { block_hash: *block_hash, shard_id },
            main_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            execution_result_hash,
        );
        Ok(state_witness)
    }

    fn get_update_shard_job(
        &self,
        block: ApplyChunkBlockContext,
        shard_id: ShardId,
        chunk: Option<ShardChunk>,
        prev_block: &Block,
        mode: ApplyChunksMode,
        incoming_receipts: &[ReceiptProof],
        storage_context: StorageContext,
    ) -> Result<Option<UpdateShardJob>, Error> {
        let prev_block_hash = prev_block.hash();
        let block_height = block.height;
        let _span =
            tracing::debug_span!(target: "chunk_executor", "get_update_shard_job", ?prev_block_hash, block_height)
                .entered();

        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        let shard_context = self.get_shard_context(prev_block_hash, &epoch_id, shard_id, mode)?;

        if !shard_context.should_apply_chunk {
            return Ok(None);
        }

        let shard_update_reason = {
            let receipts = collect_receipts(incoming_receipts);
            let shard_uid = &shard_context.shard_uid;
            let prev_chunk_chunk_extra =
                self.chain_store.get_chunk_extra(prev_block_hash, shard_uid)?;

            let (transactions, chunk_hash) = if let Some(chunk) = chunk {
                let chunk_hash = chunk.chunk_hash().clone();
                let tx_valid_list =
                    self.chain_store.compute_transaction_validity(prev_block.header(), &chunk);
                (
                    SignedValidPeriodTransactions::new(chunk.into_transactions(), tx_valid_list),
                    Some(chunk_hash),
                )
            } else {
                (SignedValidPeriodTransactions::new(vec![], vec![]), None)
            };

            ShardUpdateReason::NewChunk(NewChunkData {
                gas_limit: prev_chunk_chunk_extra.gas_limit(),
                prev_state_root: *prev_chunk_chunk_extra.state_root(),
                prev_validator_proposals: prev_chunk_chunk_extra.validator_proposals().collect(),
                chunk_hash,
                transactions,
                receipts,
                block,
                storage_context,
            })
        };

        let runtime = self.runtime_adapter.clone();
        Ok(Some((
            shard_id,
            Box::new(move |parent_span| -> Result<ShardUpdateResult, Error> {
                Ok(process_shard_update(
                    parent_span,
                    runtime.as_ref(),
                    shard_update_reason,
                    shard_context,
                    None,
                )?)
            }),
        )))
    }

    fn get_shard_context(
        &self,
        prev_hash: &CryptoHash,
        epoch_id: &EpochId,
        shard_id: ShardId,
        mode: ApplyChunksMode,
    ) -> Result<ShardContext, Error> {
        let should_apply_chunk = self.shard_tracker.should_apply_chunk(mode, prev_hash, shard_id);
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, epoch_id)?;
        Ok(ShardContext { shard_uid, should_apply_chunk })
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

    fn chain_update(&mut self) -> ChainUpdate {
        ChainUpdate::new(
            &mut self.chain_store,
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            // Since we don't produce blocks, this argument is irrelevant.
            DoomslugThresholdMode::NoApprovals,
        )
    }

    fn save_produced_receipts(
        &self,
        block_hash: &CryptoHash,
        receipt_proofs: &[ReceiptProof],
    ) -> Result<(), Error> {
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        for proof in receipt_proofs {
            save_receipt_proof(&mut store_update, &block_hash, &proof)?
        }
        store_update.commit()?;
        Ok(())
    }

    fn save_verified_receipts(&self, verified_receipts: &VerifiedReceipts) -> Result<(), Error> {
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        let VerifiedReceipts { receipt_proof, block_hash } = verified_receipts;
        save_receipt_proof(&mut store_update, &block_hash, &receipt_proof)?;
        store_update.commit()?;
        Ok(())
    }

    fn receipts_verification_context(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<ReceiptVerificationContext, Error> {
        let block = self.chain_store.get_block(block_hash)?;
        if !self.core_reader.all_execution_results_exist(&block)? {
            return Ok(ReceiptVerificationContext::NotReady);
        }
        let execution_results = self.core_reader.get_execution_results_by_shard_id(&block)?;
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
            self.save_verified_receipts(&verified_receipts)?;
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

    fn gc_memtrie_roots(
        &self,
        shard_layout: &ShardLayout,
        final_execution_head: &Tip,
    ) -> Result<(), Error> {
        let header =
            self.chain_store.get_block_header(&final_execution_head.last_block_hash).unwrap();
        let Some(prev_height) = header.prev_height() else {
            return Ok(());
        };
        for shard_uid in shard_layout.shard_uids() {
            let tries = self.runtime_adapter.get_tries();
            tries.delete_memtrie_roots_up_to_height(shard_uid, prev_height);
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
            self.chain_store.get_all_next_block_hashes(&start_block)?.into();
        while let Some(block_hash) = next_block_hashes.pop_front() {
            if !matches!(
                self.try_apply_chunks(&block_hash)?,
                TryApplyChunksOutcome::BlockAlreadyAccepted
            ) {
                continue;
            }
            next_block_hashes.extend(&self.chain_store.get_all_next_block_hashes(&block_hash)?);
        }

        Ok(())
    }
}

fn new_execution_result(
    gas_limit: &Gas,
    apply_result: &ApplyChunkResult,
    outgoing_receipts_root: CryptoHash,
) -> ChunkExecutionResult {
    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    let chunk_extra = ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals.clone(),
        apply_result.total_gas_burnt,
        *gas_limit,
        apply_result.total_balance_burnt,
        apply_result.congestion_info,
        apply_result.bandwidth_requests.clone(),
    );
    ChunkExecutionResult { chunk_extra, outgoing_receipts_root }
}

// We depend on stored receipts for distribution, so we need to store receipt proof and not only
// Vec<Receipt>.
pub(crate) fn save_receipt_proof(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    receipt_proof: &ReceiptProof,
) -> Result<(), std::io::Error> {
    let &ReceiptProof(_, ShardProof { from_shard_id, to_shard_id, .. }) = receipt_proof;
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    let value = borsh::to_vec(&receipt_proof)?;
    store_update.set(DBCol::receipt_proofs(), &key, &value);
    Ok(())
}

pub(crate) fn save_witness(
    chain_store: &ChainStoreAdapter,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    witness: &SpiceChunkStateWitness,
) -> Result<(), Error> {
    let mut store_update = chain_store.store().store_update();
    let key = get_witnesses_key(block_hash, shard_id);
    let value = borsh::to_vec(&witness)?;
    store_update.set(DBCol::witnesses(), &key, &value);
    store_update.commit()?;
    Ok(())
}

fn get_receipt_proofs_for_shard(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
) -> Result<Vec<ReceiptProof>, std::io::Error> {
    let prefix = get_receipt_proof_target_shard_prefix(block_hash, to_shard_id);
    store
        .iter_prefix_ser::<ReceiptProof>(DBCol::receipt_proofs(), &prefix)
        .map(|res| res.map(|kv| kv.1))
        .collect()
}

pub fn get_witness(
    store: &Store,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<Option<SpiceChunkStateWitness>, std::io::Error> {
    let key = get_witnesses_key(block_hash, shard_id);
    store.get_ser(DBCol::witnesses(), &key)
}

pub fn get_receipt_proof(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
    from_shard_id: ShardId,
) -> Result<Option<ReceiptProof>, std::io::Error> {
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    store.get_ser(DBCol::receipt_proofs(), &key)
}

pub fn receipt_proof_exists(
    store: &Store,
    block_hash: &CryptoHash,
    to_shard_id: ShardId,
    from_shard_id: ShardId,
) -> Result<bool, std::io::Error> {
    let key = get_receipt_proof_key(block_hash, from_shard_id, to_shard_id);
    store.exists(DBCol::receipt_proofs(), &key)
}

type UpdateShardJob = (
    ShardId,
    Box<dyn FnOnce(&tracing::Span) -> Result<ShardUpdateResult, Error> + Send + Sync + 'static>,
);

#[instrument(
    level = "debug",
    target = "chunk_executor",
    skip_all,
    fields(%block_height, ?block_hash)
)]
// We don't reuse chain's do_apply_chunks because we don't need CachedShardUpdateKey which is used
// for optimistic blocks.
fn do_apply_chunks(
    iteration_mode: ApplyChunksIterationMode,
    block_hash: &CryptoHash,
    block_height: BlockHeight,
    work: Vec<UpdateShardJob>,
) -> Vec<(ShardId, Result<ShardUpdateResult, Error>)> {
    // Track all children using `parent_span`, as they may be processed in parallel.
    let parent_span = tracing::Span::current();
    match iteration_mode {
        ApplyChunksIterationMode::Sequential => {
            work.into_iter().map(|(shard_id, task)| (shard_id, task(&parent_span))).collect()
        }
        ApplyChunksIterationMode::Rayon => {
            work.into_par_iter().map(|(shard_id, task)| (shard_id, task(&parent_span))).collect()
        }
    }
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
    let mut prev_hash = *header.prev_hash();
    while height > final_execution_head.height {
        let header = chain_store.get_block_header(&prev_hash).unwrap();
        prev_hash = *header.prev_hash();
        height = header.height();
    }
    height == final_execution_head.height
}
