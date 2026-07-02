//! The per-tracked-shard executor: parked-block queue, apply dispatch, and the
//! per-shard apply postprocessing (endorsement, witness, outgoing receipts).

use super::coordinator::{ExecutorApplyChunksDone, FailedToApplyChunkError};
use super::receipt_tracker::UnverifiedReceiptTracker;
use super::storage::{
    get_receipt_proofs_for_shard, save_receipt_proof, save_witness_and_contract_accesses,
};
use crate::spice::chunk_validator_actor::send_spice_chunk_endorsement;
use crate::spice::data_distributor_actor::{
    SpiceDataDistributorAdapter, SpiceDistributorOutgoingReceipts, SpiceDistributorStateWitness,
};
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{CanSend, IntoSender, Sender};
use near_chain::BlockHeader;
use near_chain::chain::{NewChunkData, NewChunkResult, ShardContext, StorageContext};
use near_chain::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use near_chain::spice::chunk_application::{
    ChunkPersistenceConfig, apply_chunk_postprocessing, build_spice_apply_chunk_block_context,
};
use near_chain::spice::core::SpiceCoreReader;
use near_chain::types::ApplyChunkResult;
use near_chain::types::{ApplyChunkBlockContext, RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::{ShardUpdateReason, ShardUpdateResult, process_shard_update};
use near_chain::{
    Block, Chain, Error, collect_receipts, compute_transaction_validity,
    get_chunk_clone_from_header,
};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::sandbox::state_patch::SandboxStatePatch;
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::compute_contract_accesses_hash;
use near_primitives::spice::state_witness::{SpiceChunkStateTransition, SpiceChunkStateWitness};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{CodeHash, ContractUpdates};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    BlockExecutionResults, BlockHeight, ChunkExecutionResult, ChunkExecutionResultHash, Gas,
    NumBlocks, ShardId, SpiceChunkId,
};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::ShardUId;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::{BTreeSet, HashMap, HashSet};
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

/// Per-tracked-shard executor: owns this shard's parked-block queue, in-flight
/// set, and network-receipt buffer, and runs the apply path for its shard.
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
    unverified_receipts: UnverifiedReceiptTracker,
}

/// Outcome of a per-shard `try_apply` attempt for a single parked block.
enum ApplyOutcome {
    /// Dispatched to the apply spawner (or already in flight).
    Scheduled,
    /// Block is no longer relevant for this shard (past the final execution
    /// head, not in this shard's layout, or already applied on disk).
    Dropped,
    /// Inputs not yet on disk (prev chunk execution result, prev chunk extra, or receipts missing).
    /// Re-driven by a later receipt / endorsement / sibling apply.
    NotReady,
}

struct ChunkApplicationContext<'a> {
    incoming_receipts: Vec<ReceiptProof>,
    shard_uid: ShardUId,
    prev_chunk_chunk_extra: Arc<ChunkExtra>,
    chunk_header: &'a ShardChunkHeader,
}

type UpdateShardJob =
    (ShardId, Box<dyn FnOnce(&tracing::Span) -> Result<ShardUpdateResult, Error> + Send + 'static>);

impl PerShardChunkExecutor {
    pub(crate) fn new(
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
            unverified_receipts: UnverifiedReceiptTracker::default(),
        }
    }

    /// The layout-versioned identity of the shard this executor owns. Receipt
    /// routing (which starts from a `ShardId`) resolves back to the owning
    /// executor through this; the coordinator keys its registry by `ShardUId`.
    pub(crate) fn shard_uid(&self) -> ShardUId {
        self.shard_uid
    }

    /// Park `block` and try to drain — the per-shard half of the coordinator
    /// routing a processed block to this shard.
    pub(crate) fn handle_processed_block(&mut self, block: &Block) {
        // The parent's execution results may already be available (e.g. catching
        // up), so verify-drain any receipts buffered against it before parking, so
        // this block can find its incoming receipts on disk.
        let prev_block_hash = *block.header().prev_hash();
        if let Err(err) = self.unverified_receipts.try_drain(
            &self.chain_store,
            &self.core_reader,
            &prev_block_hash,
        ) {
            tracing::error!(target: "chunk_executor", ?err, %prev_block_hash, "failed to drain unverified receipts on processed block");
        }
        self.parked_blocks.insert((block.header().height(), *block.hash()));
        self.try_apply_pending();
    }

    /// Local-path fanout entry: a sibling shard's apply produced receipts for
    /// this shard (already on disk), so re-check the parked queue.
    pub(crate) fn handle_local_chunk_applied(&mut self) {
        self.try_apply_pending();
    }

    /// Network receipt arrival for this shard: buffer it, verify-drain if ready,
    /// then re-drive the parked queue.
    pub(crate) fn handle_incoming_receipt(
        &mut self,
        source_block: CryptoHash,
        receipt_proof: ReceiptProof,
    ) -> Result<(), Error> {
        self.unverified_receipts.buffer(source_block, receipt_proof);
        self.unverified_receipts.try_drain(&self.chain_store, &self.core_reader, &source_block)?;
        self.try_apply_pending();
        Ok(())
    }

    /// The chunk execution result for `source_block` landed: drain receipts buffered
    /// against it and re-drive this shard's parked queue.
    pub(crate) fn handle_execution_result_endorsed(
        &mut self,
        source_block: &CryptoHash,
    ) -> Result<(), Error> {
        self.unverified_receipts.try_drain(&self.chain_store, &self.core_reader, source_block)?;
        self.try_apply_pending();
        Ok(())
    }

    /// Drop receipts buffered against source blocks at or below the final
    /// execution head — they can never be applied.
    pub(crate) fn prune_unverified_receipts_below_final_head(&mut self) -> Result<(), Error> {
        self.unverified_receipts.prune_below_final_head(&self.chain_store)
    }

    /// Number of source blocks with buffered receipts (test accessor).
    #[cfg(test)]
    pub(crate) fn buffered_receipts_count(&self) -> usize {
        self.unverified_receipts.len()
    }

    /// Apply every parked block that is ready, lowest height first. Applying a
    /// block can make the next height ready, and a higher fork block can be ready
    /// while a lower one waits, so we scan past `NotReady`. Cross-shard
    /// dependencies are re-driven by the next receipt / endorsement / sibling apply.
    pub(crate) fn try_apply_pending(&mut self) {
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

    /// Readiness checks for this shard's chunk in `block` (prev chunk execution result, prev chunk
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
            let apply_result =
                task(&span).map_err(|err| FailedToApplyChunkError::new(shard_id, err));
            apply_done_sender.send(ExecutorApplyChunksDone { block_hash, shard_uid, apply_result });
        });
        self.apply_chunks_spawner.spawn_boxed("apply_chunks", boxed);
        Ok(())
    }

    /// Apply-done callback for this shard: clear the in-flight marker, then
    /// postprocess and return the outgoing receipt proofs for fanout.
    pub(crate) fn handle_apply_chunks_done(
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

    fn get_chunk_extra(
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

    fn chunk_extra_exists(
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

fn new_execution_result(
    gas_limit: Gas,
    apply_result: &ApplyChunkResult,
    outgoing_receipts_root: CryptoHash,
) -> ChunkExecutionResult {
    let chunk_extra = apply_result.to_chunk_extra(gas_limit);
    ChunkExecutionResult { chunk_extra, outgoing_receipts_root }
}

pub(crate) fn is_descendant_of_final_execution_head(
    chain_store: &ChainStoreAdapter,
    header: &BlockHeader,
) -> bool {
    // The final execution head is seeded at genesis whenever spice is enabled, and
    // this runs only on spice-gated paths, so its absence is a bug rather than a
    // recoverable "not set yet" state — fail loud.
    let final_execution_head = chain_store
        .spice_final_execution_head()
        .expect("spice final execution head is seeded at genesis when spice is enabled");
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
