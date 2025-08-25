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
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    network_adapter: PeerManagerAdapter,
    apply_chunks_spawner: Arc<dyn AsyncComputationSpawner>,
    myself_sender: Sender<ExecutorApplyChunksDone>,

    blocks_in_execution: HashSet<CryptoHash>,

    // Hash of the genesis block.
    genesis_hash: CryptoHash,

    validator_signer: MutableValidatorSigner,
    core_processor: CoreStatementsProcessor,
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

    fn get_chunk_extra(
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

    fn chunk_extra_exists(
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

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
    use itertools::Itertools as _;
    use near_async::time::Clock;
    use near_chain::stateless_validation::chunk_validation::{
        MainStateTransitionCache, validate_chunk_state_witness,
    };
    use near_chain::stateless_validation::spice_chunk_validation::spice_pre_validate_chunk_state_witness;
    use near_chain::test_utils::{
        get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
    };
    use near_chain::{BlockProcessingArtifact, Provenance};
    use near_chain_configs::test_genesis::{ONE_NEAR, TestGenesisBuilder, ValidatorsSpec};
    use near_chain_configs::{Genesis, MutableConfigValue, TrackedShardsConfig};
    use near_o11y::testonly::init_test_logger;
    use near_primitives::receipt::{Receipt, ReceiptPriority};
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::sharding::ShardChunk;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::{BlockExecutionResults, ChunkExecutionResult, NumShards};
    use near_store::ShardUId;
    use near_store::adapter::StoreAdapter as _;
    use reed_solomon_erasure::ReedSolomon;

    use super::*;

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

    struct TestActor {
        actor: ChunkExecutorActor,
        actor_rc: UnboundedReceiver<ExecutorApplyChunksDone>,
        tasks_rc: UnboundedReceiver<Box<dyn FnOnce() + Send>>,
        chain: Chain,
    }

    impl<M> Handler<M> for TestActor
    where
        M: actix::Message,
        ChunkExecutorActor: Handler<M>,
    {
        fn handle(&mut self, msg: M) {
            self.actor.handle(msg);
        }
    }

    impl TestActor {
        fn new(
            genesis: Genesis,
            validator_signer: MutableValidatorSigner,
            shards: Vec<ShardUId>,
            network_sc: UnboundedSender<PeerManagerMessageRequest>,
        ) -> TestActor {
            let chain = get_chain_with_genesis(Clock::real(), genesis.clone());
            let epoch_manager = chain.epoch_manager.clone();

            let shard_tracker = ShardTracker::new(
                TrackedShardsConfig::Shards(shards),
                epoch_manager.clone(),
                validator_signer.clone(),
            );

            let chain_genesis = ChainGenesis::new(&genesis.config);
            let runtime = chain.runtime_adapter.clone();
            let genesis_hash = *chain.genesis().hash();

            let spice_core_processor = CoreStatementsProcessor::new_with_noop_senders(
                runtime.store().chain_store(),
                epoch_manager.clone(),
            );

            let chunk_endorsement_tracker = Arc::new(ChunkEndorsementTracker::new(
                epoch_manager.clone(),
                runtime.store().clone(),
                spice_core_processor.clone(),
            ));

            let (spawner, tasks_rc) = FakeSpawner::new();
            let save_latest_witnesses = false;
            let (actor_sc, actor_rc) = unbounded();
            let chunk_executor_adapter = Sender::from_fn(move |event: ExecutorApplyChunksDone| {
                actor_sc.unbounded_send(event).unwrap();
            });
            let network_adapter = PeerManagerAdapter {
                async_request_sender: near_async::messaging::noop().into_sender(),
                set_chain_info_sender: near_async::messaging::noop().into_sender(),
                state_sync_event_sender: near_async::messaging::noop().into_sender(),
                request_sender: Sender::from_fn({
                    move |event: PeerManagerMessageRequest| {
                        network_sc.unbounded_send(event).unwrap();
                    }
                }),
            };

            let actor = ChunkExecutorActor::new(
                runtime.store().clone(),
                &chain_genesis,
                genesis_hash,
                runtime.clone(),
                epoch_manager,
                shard_tracker,
                network_adapter,
                validator_signer,
                spice_core_processor,
                chunk_endorsement_tracker,
                Arc::new(spawner),
                chunk_executor_adapter,
                save_latest_witnesses,
            );
            TestActor { chain, actor, actor_rc, tasks_rc }
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
                    self.actor.handle(event);
                }
                if events_processed == 0 {
                    break;
                }
            }
        }

        fn handle_with_internal_events<M>(&mut self, msg: M)
        where
            M: actix::Message,
            ChunkExecutorActor: Handler<M>,
        {
            self.actor.handle(msg);
            self.run_internal_events();
        }
    }

    fn setup_with_shards(
        num_shards: usize,
        network_sc: UnboundedSender<PeerManagerMessageRequest>,
    ) -> Vec<TestActor> {
        init_test_logger();

        let signers: Vec<_> = (0..num_shards)
            .into_iter()
            .map(|i| Arc::new(create_test_signer(&format!("test{i}"))))
            .collect();

        let shard_layout = ShardLayout::multi_shard(num_shards as NumShards, 0);

        let accounts: Vec<_> = signers.iter().map(|signer| signer.validator_id().clone()).collect();
        let validators_spec =
            ValidatorsSpec::desired_roles(&accounts.iter().map(|a| a.as_str()).collect_vec(), &[]);

        let epoch_length = 10;
        let genesis = TestGenesisBuilder::new()
            .genesis_time_from_clock(&Clock::real())
            .epoch_length(epoch_length)
            .shard_layout(shard_layout.clone())
            .validators_spec(validators_spec)
            .add_user_accounts_simple(&accounts, ONE_NEAR)
            .build();

        signers
            .into_iter()
            .zip(shard_layout.shard_uids())
            .map(|(signer, shard_uuid)| {
                let validator_signer = MutableConfigValue::new(Some(signer), "validator_signer");
                TestActor::new(
                    genesis.clone(),
                    validator_signer,
                    vec![shard_uuid],
                    network_sc.clone(),
                )
            })
            .collect::<Vec<_>>()
            .try_into()
            .unwrap_or_else(|_| panic!())
    }

    /// Returns 2 TestActor instances first validators and second not.
    fn setup_with_non_validator(
        network_sc: UnboundedSender<PeerManagerMessageRequest>,
    ) -> [TestActor; 2] {
        init_test_logger();
        let signer = Arc::new(create_test_signer("test1"));
        let shard_layout = ShardLayout::multi_shard(2, 0);
        let genesis = TestGenesisBuilder::new()
            .genesis_time_from_clock(&Clock::real())
            .shard_layout(shard_layout.clone())
            .validators_spec(ValidatorsSpec::desired_roles(&["test1"], &[]))
            .add_user_account_simple(signer.validator_id().clone(), ONE_NEAR)
            .build();

        [
            TestActor::new(
                genesis.clone(),
                MutableConfigValue::new(Some(signer), "validator_signer"),
                shard_layout.shard_uids().collect(),
                network_sc.clone(),
            ),
            TestActor::new(
                genesis,
                MutableConfigValue::new(None, "validator_signer"),
                shard_layout.shard_uids().collect(),
                network_sc,
            ),
        ]
    }

    fn propagate_single_network_request(
        actors: &mut [TestActor],
        event: &PeerManagerMessageRequest,
    ) {
        let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
        match request {
            NetworkRequests::TestonlySpiceIncomingReceipts { block_hash, receipt_proofs } => {
                actors.iter_mut().for_each(|actor| {
                    actor.handle_with_internal_events(ExecutorIncomingReceipts {
                        block_hash: *block_hash,
                        receipt_proofs: receipt_proofs.clone(),
                    });
                });
            }
            NetworkRequests::TestonlySpiceStateWitness { .. } => {}
            NetworkRequests::ChunkEndorsement(..) => {}
            event => unreachable!("{event:?}"),
        }
    }

    fn propagate_network_requests(
        actors: &mut [TestActor],
        network_rc: &mut UnboundedReceiver<PeerManagerMessageRequest>,
    ) {
        while let Ok(Some(event)) = network_rc.try_next() {
            propagate_single_network_request(actors, &event);
        }
    }

    fn block_executed(actor: &TestActor, block: &Block) -> bool {
        let epoch_id = block.header().epoch_id();
        let shard_ids = actor.actor.epoch_manager.shard_ids(epoch_id).unwrap();
        for shard_id in shard_ids {
            if !actor.actor.shard_tracker.cares_about_shard(block.hash(), shard_id) {
                continue;
            }
            if !actor.actor.chunk_extra_exists(block.header().hash(), shard_id).unwrap() {
                return false;
            }
        }
        true
    }

    fn produce_block(actors: &mut [TestActor], prev_block: &Block) -> Arc<Block> {
        let chunks =
            get_fake_next_block_chunk_headers(&prev_block, actors[0].actor.epoch_manager.as_ref());
        for actor in actors.iter_mut() {
            let mut store_update = actor.actor.chain_store.store_update();
            for chunk_header in &chunks {
                store_update.save_chunk(ShardChunk::new(chunk_header.clone(), vec![], vec![]));
            }
            store_update.commit().unwrap();
        }
        let block_producer = actors[0]
            .actor
            .epoch_manager
            .get_block_producer_info(
                prev_block.header().epoch_id(),
                prev_block.header().height() + 1,
            )
            .unwrap();
        let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
        let block = TestBlockBuilder::new(Clock::real(), prev_block, signer).chunks(chunks).build();
        for actor in actors {
            process_block_sync(
                &mut actor.chain,
                block.clone().into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        block
    }

    fn produce_n_blocks(actors: &mut [TestActor], num_blocks: usize) -> Vec<Arc<Block>> {
        let mut prev_block = actors[0].chain.genesis_block();
        let mut blocks = Vec::new();
        for _ in 0..num_blocks {
            let block = produce_block(actors, &prev_block);
            blocks.push(block.clone());
            prev_block = block;
        }
        blocks
    }

    fn find_chunk_execution_result(
        actors: &mut [TestActor],
        block_hash: &CryptoHash,
        shard_layout: &ShardLayout,
        shard_id: ShardId,
    ) -> ChunkExecutionResult {
        for actor in actors {
            if let Some(chunk_extra) = actor.actor.get_chunk_extra(block_hash, shard_id).unwrap() {
                let outgoing_receipts =
                    actor.actor.chain_store.get_outgoing_receipts(block_hash, shard_id).unwrap();
                let (outgoing_receipts_root, _receipt_proofs) =
                    Chain::create_receipts_proofs_from_outgoing_receipts(
                        shard_layout,
                        shard_id,
                        Arc::unwrap_or_clone(outgoing_receipts),
                    )
                    .unwrap();
                return ChunkExecutionResult {
                    chunk_extra: Arc::unwrap_or_clone(chunk_extra),
                    outgoing_receipts_root,
                };
            }
        }
        panic!()
    }

    fn record_endorsements(actors: &mut [TestActor], block: &Block) {
        let epoch_id = block.header().epoch_id();
        let shard_layout = actors[0].actor.epoch_manager.get_shard_layout(epoch_id).unwrap();
        for chunk in block.chunks().iter_raw() {
            let shard_id = chunk.shard_id();
            let execution_result =
                find_chunk_execution_result(actors, block.hash(), &shard_layout, shard_id);

            for actor in actors.iter() {
                let Some(signer) = actor.actor.validator_signer.get() else {
                    continue;
                };
                let endorsement = ChunkEndorsement::new_with_execution_result(
                    *epoch_id,
                    execution_result.clone(),
                    *block.header().hash(),
                    chunk,
                    &signer,
                );
                for actor in actors.iter() {
                    actor
                        .actor
                        .core_processor
                        .record_chunk_endorsement(endorsement.clone())
                        .unwrap();
                }
            }
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_executing_blocks() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_shards(3, network_sc);
        let blocks = produce_n_blocks(&mut actors, 5);
        for (i, block) in blocks.iter().enumerate() {
            for actor in &mut actors {
                assert!(!block_executed(&actor, &block), "block #{} is already executed", i + 1);
                actor.handle_with_internal_events(ProcessedBlock {
                    block_hash: *block.header().hash(),
                });
                assert!(block_executed(&actor, &block), "failed to execute block #{}", i + 1);
            }
            propagate_network_requests(&mut actors, &mut network_rc);
            record_endorsements(&mut actors, &block);
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_scheduling_same_block_twice() {
        let (network_sc, _network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);

        actors[0].handle(ProcessedBlock { block_hash: *blocks[0].hash() });

        assert!(!block_executed(&actors[0], &blocks[0]));
        let mut tasks = Vec::new();
        while let Ok(Some(task)) = actors[0].tasks_rc.try_next() {
            tasks.push(task);
        }
        assert_ne!(tasks.len(), 0);

        actors[0].handle(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(actors[0].tasks_rc.try_next().is_err(), "no new tasks should be scheduled");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_executing_same_block_twice() {
        let (network_sc, _network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);

        assert!(!block_executed(&actors[0], &blocks[0]));
        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actors[0], &blocks[0]));

        actors[0].handle(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(actors[0].tasks_rc.try_next().is_err(), "no new tasks should be scheduled");
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_execution_result_endorsement_trigger_next_blocks_execution() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);
        let fork_block = produce_block(&mut actors, &blocks[0]);

        for actor in &mut actors {
            actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
            assert!(block_executed(&actor, &blocks[0]));
        }

        propagate_network_requests(&mut actors, &mut network_rc);
        record_endorsements(&mut actors, &blocks[0]);

        assert!(!block_executed(&actors[0], &blocks[1]));
        assert!(!block_executed(&actors[0], &fork_block));
        actors[0]
            .handle_with_internal_events(ExecutionResultEndorsed { block_hash: *blocks[0].hash() });

        assert!(block_executed(&actors[0], &blocks[1]));
        assert!(block_executed(&actors[0], &fork_block));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_new_receipts_trigger_next_blocks_execution() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);
        let fork_block = produce_block(&mut actors, &blocks[0]);

        for actor in &mut actors {
            actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
            assert!(block_executed(&actor, &blocks[0]));
        }

        record_endorsements(&mut actors, &blocks[0]);

        assert!(!block_executed(&actors[0], &blocks[1]));
        assert!(!block_executed(&actors[0], &fork_block));
        propagate_network_requests(&mut actors, &mut network_rc);

        assert!(block_executed(&actors[0], &blocks[1]));
        assert!(block_executed(&actors[0], &fork_block));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_not_executing_without_execution_result() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);

        for actor in &mut actors {
            actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
            assert!(block_executed(&actor, &blocks[0]));
        }
        propagate_network_requests(&mut actors, &mut network_rc);

        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
        assert!(!block_executed(&actors[0], &blocks[1]));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_not_executing_without_receipts() {
        let (network_sc, _network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);

        for actor in &mut actors {
            actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
            assert!(block_executed(&actor, &blocks[0]));
        }
        record_endorsements(&mut actors, &blocks[0]);

        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
        assert!(!block_executed(&actors[0], &blocks[1]));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_executing_forks() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);

        for actor in &mut actors {
            actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
            assert!(block_executed(&actor, &blocks[0]));
        }

        propagate_network_requests(&mut actors, &mut network_rc);
        record_endorsements(&mut actors, &blocks[0]);

        let fork_block = produce_block(&mut actors, &blocks[0]);
        assert!(!block_executed(&actors[0], &blocks[1]));
        assert!(!block_executed(&actors[0], &fork_block));

        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
        assert!(block_executed(&actors[0], &blocks[1]));
        assert!(!block_executed(&actors[0], &fork_block));

        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *fork_block.hash() });
        assert!(block_executed(&actors[0], &fork_block));
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    #[should_panic]
    fn test_not_executing_with_bad_receipts() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_shards(2, network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);

        for actor in &mut actors {
            actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
            assert!(block_executed(&actor, &blocks[0]));
        }

        record_endorsements(&mut actors, &blocks[0]);
        while let Ok(Some(event)) = network_rc.try_next() {
            let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
            let NetworkRequests::TestonlySpiceIncomingReceipts { block_hash, mut receipt_proofs } =
                request
            else {
                continue;
            };
            receipt_proofs[0].0.push(Receipt::new_balance_refund(
                &AccountId::from_str("test1").unwrap(),
                ONE_NEAR,
                ReceiptPriority::NoPriority,
            ));
            actors.iter_mut().for_each(|actor| {
                actor.handle_with_internal_events(ExecutorIncomingReceipts {
                    block_hash,
                    receipt_proofs: receipt_proofs.clone(),
                });
            });
        }

        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_tracking_several_shards() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_non_validator(network_sc);

        let blocks = produce_n_blocks(&mut actors, 3);
        for (i, block) in blocks.iter().enumerate() {
            actors[0]
                .handle_with_internal_events(ProcessedBlock { block_hash: *block.header().hash() });

            let epoch_id = block.header().epoch_id();
            let shard_ids = actors[0].actor.epoch_manager.shard_ids(epoch_id).unwrap();
            for shard_id in shard_ids {
                assert!(
                    actors[0].actor.chunk_extra_exists(block.header().hash(), shard_id).unwrap(),
                    "no execution results for block #{} shard_id={shard_id} block_hash {}",
                    i + 1,
                    block.hash(),
                );
            }
            propagate_network_requests(&mut actors, &mut network_rc);
            record_endorsements(&mut actors, &block);
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_not_sending_witness_when_not_validator() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_non_validator(network_sc);
        let blocks = produce_n_blocks(&mut actors, 3);
        let actor = &mut actors[1];

        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));

        let mut witnesses = Vec::new();
        while let Ok(Some(event)) = network_rc.try_next() {
            let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
            let NetworkRequests::TestonlySpiceStateWitness { state_witness } = request else {
                continue;
            };
            witnesses.push(state_witness);
        }
        assert_eq!(witnesses.len(), 0);
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_executing_chain_of_ready_blocks() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_non_validator(network_sc);
        let blocks = produce_n_blocks(&mut actors, 5);

        for block in &blocks {
            actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
            assert!(block_executed(&actors[0], block));
            propagate_network_requests(&mut actors, &mut network_rc);
            record_endorsements(&mut actors, &block);
        }

        for block in &blocks {
            assert!(!block_executed(&actors[1], block));
        }
        actors[1].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        for block in &blocks {
            assert!(block_executed(&actors[1], block));
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_not_executing_out_of_order() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_non_validator(network_sc);
        let blocks = produce_n_blocks(&mut actors, 5);

        for block in &blocks {
            actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
            assert!(block_executed(&actors[0], block));
            propagate_network_requests(&mut actors, &mut network_rc);
            record_endorsements(&mut actors, &block);
        }

        for block in &blocks {
            assert!(!block_executed(&actors[1], block));
        }
        actors[1].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
        for block in &blocks {
            assert!(!block_executed(&actors[1], block));
        }
    }

    #[test]
    #[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
    fn test_witness_is_valid() {
        let (network_sc, mut network_rc) = unbounded();
        let mut actors = setup_with_non_validator(network_sc);

        let prev_block = actors[0].chain.genesis_block();
        let block = produce_block(&mut actors, &prev_block);
        let actor = &mut actors[0];

        actor.handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
        assert!(block_executed(&actor, &block));

        let mut count_witnesses = 0;
        while let Ok(Some(event)) = network_rc.try_next() {
            let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
            let NetworkRequests::TestonlySpiceStateWitness { state_witness } = request else {
                continue;
            };
            let pre_validation_result = spice_pre_validate_chunk_state_witness(
                &state_witness,
                &block,
                &prev_block,
                &BlockExecutionResults(HashMap::new()),
                actor.actor.epoch_manager.as_ref(),
                &actor.actor.chain_store,
            )
            .unwrap();

            let save_witness_if_invalid = false;
            assert!(
                validate_chunk_state_witness(
                    state_witness,
                    pre_validation_result,
                    actor.actor.epoch_manager.as_ref(),
                    actor.actor.runtime_adapter.as_ref(),
                    &MainStateTransitionCache::default(),
                    actor.actor.chain_store.store(),
                    save_witness_if_invalid,
                    Arc::new(ReedSolomon::new(1, 1).unwrap()),
                )
                .is_ok()
            );
            count_witnesses += 1;
        }
        assert!(count_witnesses > 0);
    }
}
