use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use near_async::futures::AsyncComputationSpawner;
use near_async::futures::AsyncComputationSpawnerExt;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_chain::ChainStoreAccess;
use near_chain::chain::{
    NewChunkData, NewChunkResult, ShardContext, StorageContext, UpdateShardJob, do_apply_chunks,
};
use near_chain::sharding::get_receipts_shuffle_salt;
use near_chain::sharding::shuffle_receipt_proofs;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain::spice_core::ExecutionResultEndorsed;
use near_chain::types::ApplyChunkResult;
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
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_primitives::block::Chunks;
use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::{BlockToApply, CachedShardUpdateKey};
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::sharding::ChunkHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::sharding::ShardProof;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::stateless_validation::state_witness::ChunkStateTransition;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::types::AccountId;
use near_primitives::types::ChunkExecutionResult;
use near_primitives::types::EpochId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{ShardId, ShardIndex};
use near_primitives::utils::get_receipt_proof_key;
use near_primitives::utils::get_receipt_proof_target_shard_prefix;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::DBCol;
use near_store::Store;
use near_store::StoreUpdate;
use near_store::TrieDBStorage;
use near_store::TrieStorage;
use near_store::adapter::trie_store::TrieStoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use tracing::instrument;

use crate::spice_chunk_validator_actor::send_spice_chunk_endorsement;
use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;

pub struct ChunkExecutorActor {
    pub(crate) chain_store: ChainStore,
    pub(crate) runtime_adapter: Arc<dyn RuntimeAdapter>,
    pub(crate) epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) shard_tracker: ShardTracker,
    network_adapter: PeerManagerAdapter,
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    myself_sender: Sender<ExecutorApplyChunksDone>,

    blocks_in_execution: HashSet<CryptoHash>,

    // Hash of the genesis block.
    genesis_hash: CryptoHash,

    pub(crate) validator_signer: MutableValidatorSigner,
    pub(crate) core_processor: CoreStatementsProcessor,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,

    save_latest_witnesses: bool,
}

impl ChunkExecutorActor {
    pub fn new(
        store: Store,
        genesis: &ChainGenesis,
        genesis_hash: CryptoHash,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        network_adapter: PeerManagerAdapter,
        validator_signer: MutableValidatorSigner,
        core_processor: CoreStatementsProcessor,
        chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
        apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
        myself_sender: Sender<ExecutorApplyChunksDone>,
        save_latest_witnesses: bool,
    ) -> Self {
        Self {
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            network_adapter,
            apply_chunks_spawner,
            myself_sender,
            blocks_in_execution: HashSet::new(),
            genesis_hash,
            validator_signer,
            core_processor,
            chunk_endorsement_tracker,
            save_latest_witnesses,
        }
    }
}

impl near_async::messaging::Actor for ChunkExecutorActor {}

/// Message with incoming receipts corresponding to the block.
/// Eventually this would be handled properly with data availability layer.
/// For now this is useful to do testing with test loop.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ExecutorIncomingReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proofs: Vec<ReceiptProof>,
}

/// Message that should be sent once block is processed.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct ProcessedBlock {
    pub block_hash: CryptoHash,
}

#[derive(actix::Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct ExecutorApplyChunksDone {
    pub block_hash: CryptoHash,
    pub apply_results: Vec<ShardUpdateResult>,
}

impl Handler<ExecutorIncomingReceipts> for ChunkExecutorActor {
    fn handle(
        &mut self,
        ExecutorIncomingReceipts { block_hash, receipt_proofs }: ExecutorIncomingReceipts,
    ) {
        // TODO(spice): receipt proofs should be saved to the database by the distribution layer
        if let Err(err) = self.save_receipt_proofs(&block_hash, receipt_proofs) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to save receipt proofs");
            return;
        }
        if let Err(err) = self.try_process_next_blocks(&block_hash) {
            tracing::error!(target: "chunk_executor", ?err, ?block_hash, "failed to process next blocks");
        }
    }
}

impl Handler<ProcessedBlock> for ChunkExecutorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        match self.try_apply_chunks(&block_hash) {
            Ok(TryApplyChunksOutcome::Scheduled) => {}
            Ok(TryApplyChunksOutcome::NotReady) => {
                // We will retry applying it by looking at all next blocks after receiving
                // additional execution result endorsements or receipts.
                tracing::debug!(target: "chunk_executor", %block_hash, "not yet ready for processing");
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

enum TryApplyChunksOutcome {
    Scheduled,
    NotReady,
    BlockAlreadyAccepted,
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
        let header = block.header();
        let prev_block_hash = header.prev_hash();
        let store = self.chain_store.store();
        let prev_block = self.chain_store.get_block(prev_block_hash)?;
        let prev_block_is_genesis = *prev_block_hash == self.genesis_hash;
        if !prev_block_is_genesis
            && !self.core_processor.all_execution_results_exist(&prev_block)?
        {
            tracing::debug!(target: "chunk_executor", %block_hash, %prev_block_hash, "missing execution results to allow validating receipts");
            return Ok(TryApplyChunksOutcome::NotReady);
        }
        let execution_results =
            self.core_processor.get_execution_results_by_shard_id(&prev_block)?;

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
                if prev_block_is_genesis {
                    all_receipts.insert(prev_block_shard_id, vec![]);
                    continue;
                }

                if !self.chunk_extra_exists(prev_block_hash, prev_block_shard_id)? {
                    tracing::debug!(
                        target: "chunk_executor",
                        %block_hash,
                        %prev_block_hash,
                        %prev_block_shard_id,
                        "previous block is not executed yet");
                    return Ok(TryApplyChunksOutcome::NotReady);
                }

                let proofs =
                    get_receipt_proofs_for_shard(&store, prev_block_hash, prev_block_shard_id)?;
                if proofs.len() != prev_block_shard_ids.len() {
                    tracing::debug!(
                        target: "chunk_executor",
                        %block_hash,
                        %prev_block_hash,
                        %prev_block_shard_id,
                        "missing receipts to apply all tracked chunks for a block"
                    );
                    return Ok(TryApplyChunksOutcome::NotReady);
                }
                for proof in &proofs {
                    let from_shard_id = proof.1.from_shard_id;
                    let execution_result = execution_results.get(&from_shard_id).unwrap();
                    // TODO(spice): Perform this check before saving receipts.
                    if !proof.verify_against_receipt_root(execution_result.outgoing_receipts_root) {
                        tracing::error!(
                            target: "chunk_executor",
                            ?execution_result, ?from_shard_id, %block_hash, %prev_block_hash,
                                    "Receipt proof for chunk has invalid merkle path, doesn't match outgoing receipts root",
                        );
                        unimplemented!("Invalid receipts in spice handling");
                    }
                }
                all_receipts.insert(prev_block_shard_id, proofs);
            }
        }
        self.schedule_apply_chunks(block, all_receipts, SandboxStatePatch::default())?;
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
                TryApplyChunksOutcome::Scheduled => {}
                TryApplyChunksOutcome::NotReady => {
                    tracing::debug!(target: "chunk_executor", %next_block_hash, "not yet ready for processing");
                }
                TryApplyChunksOutcome::BlockAlreadyAccepted => {}
            }
        }
        Ok(())
    }

    // Logic here is based on Chain::apply_chunk_preprocessing
    fn schedule_apply_chunks(
        &self,
        block: Arc<Block>,
        mut incoming_receipts: HashMap<ShardId, Vec<ReceiptProof>>,
        mut state_patch: SandboxStatePatch,
    ) -> Result<(), Error> {
        let block_hash = block.hash();
        let header = block.header();
        let prev_hash = header.prev_hash();
        let prev_block = self.chain_store.get_block(prev_hash)?;

        let prev_chunk_headers = self.epoch_manager.get_prev_chunk_headers(&prev_block)?;

        let epoch_id = block.header().epoch_id();
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;

        let receipts_shuffle_salt = get_receipts_shuffle_salt(self.epoch_manager.as_ref(), &block)?;

        let chunk_headers = &block.chunks();
        let mut jobs = Vec::new();
        // TODO(spice-resharding): Make sure shard logic is correct with resharding.
        for (shard_index, _prev_chunk_header) in prev_chunk_headers.iter().enumerate() {
            // XXX: This is a bit questionable -- sandbox state patching works
            // only for a single shard. This so far has been enough.
            let state_patch = state_patch.take();
            let shard_id = shard_layout.get_shard_id(shard_index)?;
            // If we don't care about shard we wouldn't have relevant incoming receipts.
            if !self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                block_hash,
                shard_id,
            ) {
                continue;
            }

            let is_new_chunk = true;
            let block_context = Chain::get_apply_chunk_block_context_from_block_header(
                block.header(),
                &chunk_headers,
                prev_block.header(),
                is_new_chunk,
            )?;

            // TODO(spice-resharding): We may need to take resharding into account here.
            let mut receipt_proofs = incoming_receipts
                .remove(&shard_id)
                .expect("expected receipts for all tracked shards");
            shuffle_receipt_proofs(&mut receipt_proofs, receipts_shuffle_salt);

            let storage_context =
                StorageContext { storage_data_source: StorageDataSource::Db, state_patch };

            let cached_shard_update_key =
                Chain::get_cached_shard_update_key(&block_context, chunk_headers, shard_id)?;

            let job = self.get_update_shard_job(
                cached_shard_update_key,
                block_context,
                chunk_headers,
                shard_index,
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
        self.apply_chunks_spawner.spawn("apply_chunks", move || {
            let block_hash = *block.hash();
            let apply_results =
                do_apply_chunks(BlockToApply::Normal(block_hash), block.header().height(), jobs)
                    .into_iter()
                    .map(|(shard_id, _, result)| {
                        result.unwrap_or_else(|err| {
                    panic!("failed to apply block {block_hash:?} chunk for shard {shard_id}: {err}")
                })
                    })
                    .collect();
            apply_done_sender.send(ExecutorApplyChunksDone { block_hash, apply_results });
        });
        Ok(())
    }

    fn send_outgoing_receipts(&self, block_hash: CryptoHash, receipt_proofs: Vec<ReceiptProof>) {
        tracing::debug!(target: "chunk_executor", %block_hash, ?receipt_proofs, "sending outgoing receipts");
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::TestonlySpiceIncomingReceipts { block_hash, receipt_proofs },
        ));
    }

    fn send_witness_to_chunk_validators(&self, state_witness: ChunkStateWitness) {
        // TODO(spice): Use distribution layer to distribute witnesses instead.
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::TestonlySpiceStateWitness { state_witness },
        ));
    }

    fn process_apply_chunk_results(
        &mut self,
        block_hash: CryptoHash,
        results: Vec<ShardUpdateResult>,
    ) -> Result<(), Error> {
        let block = self.chain_store.get_block(&block_hash).unwrap();
        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        for result in &results {
            let ShardUpdateResult::NewChunk(new_chunk_result) = result else {
                panic!("missing chunks are not expected in SPICE");
            };
            let Some(my_signer) = self.validator_signer.get() else {
                // If node isn't validator it shouldn't send outgoing receipts, endorsed and witnesses.
                // RPC nodes can still apply chunks and tracks multiple shards.
                continue;
            };
            let shard_id = new_chunk_result.shard_uid.shard_id();
            let (outgoing_receipts_root, receipt_proofs) =
                Chain::create_receipts_proofs_from_outgoing_receipts(
                    &shard_layout,
                    shard_id,
                    new_chunk_result.apply_result.outgoing_receipts.clone(),
                )?;
            self.send_outgoing_receipts(block_hash, receipt_proofs);

            self.distribute_witness(&block, my_signer, new_chunk_result, outgoing_receipts_root)?;
        }

        let mut chain_update = self.chain_update();
        let should_save_state_transition_data = false;
        chain_update.apply_chunk_postprocessing(
            &block,
            results,
            should_save_state_transition_data,
        )?;
        chain_update.commit()?;
        Ok(())
    }

    fn distribute_witness(
        &self,
        block: &Arc<Block>,
        my_signer: Arc<ValidatorSigner>,
        new_chunk_result: &NewChunkResult,
        outgoing_receipts_root: CryptoHash,
    ) -> Result<(), Error> {
        let NewChunkResult { shard_uid, gas_limit, apply_result } = new_chunk_result;
        let shard_id = shard_uid.shard_id();
        let epoch_id = block.header().epoch_id();
        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let chunks = block.chunks();
        let chunk_header = chunks.get(shard_index).unwrap();

        if self
            .epoch_manager
            .get_chunk_validator_assignments(&epoch_id, shard_id, block.header().height())?
            .contains(my_signer.validator_id())
        {
            let execution_result =
                new_execution_result(gas_limit, apply_result, outgoing_receipts_root);
            // If we're validator we can send endorsement without witness validation.
            let endorsement = ChunkEndorsement::new_with_execution_result(
                *epoch_id,
                execution_result,
                *block.header().hash(),
                chunk_header,
                &my_signer,
            );
            send_spice_chunk_endorsement(
                endorsement.clone(),
                self.epoch_manager.as_ref(),
                &self.network_adapter.clone().into_sender(),
                &my_signer,
            );
            self.chunk_endorsement_tracker
                .process_chunk_endorsement(endorsement)
                .expect("Node should always be able to record it's own endorsement");
        }

        let state_witness = self.create_witness(
            block,
            apply_result,
            my_signer.validator_id().clone(),
            chunk_header,
        )?;

        if self.save_latest_witnesses {
            self.chain_store.save_latest_chunk_state_witness(&state_witness)?;
        }

        self.send_witness_to_chunk_validators(state_witness);
        Ok(())
    }

    fn create_witness(
        &self,
        block: &Block,
        apply_result: &ApplyChunkResult,
        me: AccountId,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkStateWitness, Error> {
        let block_hash = block.header().hash();
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash).unwrap();
        let prev_block = self.chain_store.get_block(block.header().prev_hash()).unwrap();
        let shard_id = chunk_header.shard_id();

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
            ChunkStateTransition {
                block_hash: *block_hash,
                base_state: PartialState::TrieValues(base_state_values),
                post_state_root: apply_result.new_root,
            }
        };
        let source_receipt_proofs: HashMap<ChunkHash, ReceiptProof> = {
            let prev_block_hash = prev_block.header().hash();
            let (prev_block_shard_layout, prev_block_shard_id, _) =
                self.epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
            let receipt_proofs = get_receipt_proofs_for_shard(
                &self.chain_store.store(),
                prev_block_hash,
                prev_block_shard_id,
            )?;
            receipt_proofs
                .into_iter()
                .map(|proof| -> Result<_, Error> {
                    let from_shard_id = proof.1.from_shard_id;
                    let from_shard_index =
                        prev_block_shard_layout.get_shard_index(from_shard_id)?;
                    let chunks = prev_block.chunks();
                    let from_chunk_hash = chunks
                        .get(from_shard_index)
                        .ok_or(Error::InvalidShardId(proof.1.from_shard_id))?
                        .chunk_hash();
                    Ok((from_chunk_hash.clone(), proof))
                })
                .collect::<Result<_, Error>>()?
        };
        // TODO(spice-resharding): implicit_transitions are used for resharding.
        let implicit_transitions = Vec::new();
        let new_transactions = Vec::new();
        let chunk = get_chunk_clone_from_header(&self.chain_store, chunk_header)?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let state_witness = ChunkStateWitness::new(
            me,
            epoch_id,
            // Note that for spice we send current, not next, chunk header wrt. witnessed
            // transition.
            chunk_header.clone(),
            main_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            chunk.to_transactions().to_vec(),
            implicit_transitions,
            new_transactions,
            protocol_version,
        );
        Ok(state_witness)
    }

    fn get_update_shard_job(
        &self,
        cached_shard_update_key: CachedShardUpdateKey,
        block: ApplyChunkBlockContext,
        chunk_headers: &Chunks,
        shard_index: ShardIndex,
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
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let shard_id = shard_layout.get_shard_id(shard_index)?;
        let shard_context = self.get_shard_context(prev_block_hash, &epoch_id, shard_id, mode)?;

        if !shard_context.should_apply_chunk {
            return Ok(None);
        }

        let chunk_header = chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;
        let shard_update_reason = {
            let chunk = get_chunk_clone_from_header(&self.chain_store, chunk_header)?;
            let tx_valid_list =
                self.chain_store.compute_transaction_validity(prev_block.header(), &chunk);
            let receipts = collect_receipts(incoming_receipts);

            let shard_uid = &shard_context.shard_uid;
            let chunk_extra = self.chain_store.get_chunk_extra(prev_block_hash, shard_uid)?;
            let chunk_header = chunk_header.clone().into_spice_chunk_execution_header(&chunk_extra);

            let transactions =
                SignedValidPeriodTransactions::new(chunk.into_transactions(), tx_valid_list);
            ShardUpdateReason::NewChunk(NewChunkData {
                chunk_header,
                transactions,
                receipts,
                block,
                storage_context,
            })
        };

        let runtime = self.runtime_adapter.clone();
        Ok(Some((
            shard_id,
            cached_shard_update_key,
            Box::new(move |parent_span| -> Result<ShardUpdateResult, Error> {
                Ok(process_shard_update(
                    parent_span,
                    runtime.as_ref(),
                    shard_update_reason,
                    shard_context,
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
            self.core_processor.clone(),
        )
    }

    fn save_receipt_proofs(
        &self,
        block_hash: &CryptoHash,
        receipt_proofs: Vec<ReceiptProof>,
    ) -> Result<(), Error> {
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        for proof in receipt_proofs {
            save_receipt_proof(&mut store_update, &block_hash, &proof)?
        }
        store_update.commit()?;
        Ok(())
    }
}

fn new_execution_result(
    gas_limit: &u64,
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

fn save_receipt_proof(
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
