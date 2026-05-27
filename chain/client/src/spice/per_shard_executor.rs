use crate::spice::chunk_executor_actor::{
    get_receipt_proofs_for_shard, is_descendant_of_final_execution_head, new_execution_result,
    save_receipt_proof, save_witness_and_contract_accesses,
};
use crate::spice::chunk_executor_coordinator::PerShardChunkApplied;
use crate::spice::data_distributor_actor::{
    SpiceDataDistributorAdapter, SpiceDistributorOutgoingReceipts, SpiceDistributorStateWitness,
};
use near_async::messaging::{Actor, CanSend, Handler, IntoSender, Sender};
use near_chain::chain::{NewChunkData, NewChunkResult, ShardContext, StorageContext};
use near_chain::sharding::{get_receipts_shuffle_salt, shuffle_receipt_proofs};
use near_chain::spice::chunk_application::build_spice_apply_chunk_block_context;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::types::{ApplyChunkResult, RuntimeAdapter, StorageDataSource};
use near_chain::update_shard::{ShardUpdateReason, ShardUpdateResult, process_shard_update};
use near_chain::{
    Block, Chain, ChainGenesis, ChainStore, ChainStoreAccess, ChainUpdate, DoomslugThresholdMode,
    Error, collect_receipts, get_chunk_clone_from_header,
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
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::{
    SpiceChunkStateTransition, SpiceChunkStateWitness, compute_contract_accesses_hash,
};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{CodeHash, ContractUpdates};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ChunkExecutionResultHash, ShardId, SpiceChunkId};
use near_store::ShardUId;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::chunk_validator_actor::send_spice_chunk_endorsement;

/// New block exists in the chain store; park it and try to apply.
#[derive(Debug)]
pub struct IncomingBlock {
    pub block_hash: CryptoHash,
}

/// Self-message after each successful apply; drives the next apply without
/// starving the mailbox (goes to the back of the queue).
#[derive(Debug)]
pub struct AppliedContinue {
    pub just_applied: CryptoHash,
}

/// How a receipt proof reached this shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptSource {
    /// Produced by a sibling shard on this node — already written to disk by the
    /// sender, so this is a wake-up only (no write).
    LocallyVerified,
    /// Reconstructed from the network; must be verified against the source
    /// block's CER and written by this actor.
    FromNetwork,
}

/// A receipt proof addressed to this shard.
#[derive(Debug)]
pub struct IncomingReceipt {
    pub block_hash: CryptoHash,
    pub proof: ReceiptProof,
    pub source: ReceiptSource,
}

/// A source block's CER reached on-chain quorum: verify buffered network
/// receipts from it and re-check parked children.
#[derive(Debug)]
pub struct CERCertified {
    pub block_hash: CryptoHash,
}

struct ParkedBlock {
    block_hash: CryptoHash,
    height: BlockHeight,
}

/// Per-shard half of the per-shard SPICE chunk-execution subsystem: applies one
/// shard's chunks inline on its own dedicated thread. One instance per tracked
/// shard, spawned by [`super::chunk_executor_coordinator::ChunkExecutorCoordinator`].
///
/// Prototype: persistence hack-reuses the monolithic `apply_chunk_postprocessing`
/// path (`ChainStoreUpdate`) for this shard's single result rather than the
/// adapter-only writes from the design — the chain-independence migration is a
/// deferred follow-up.
pub struct PerShardExecutor {
    shard_id: ShardId,
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    core_reader: SpiceCoreReader,
    validator_signer: MutableValidatorSigner,
    network_adapter: PeerManagerAdapter,
    core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
    data_distributor_adapter: SpiceDataDistributorAdapter,
    coordinator_sender: Sender<PerShardChunkApplied>,
    myself_sender: Sender<AppliedContinue>,
    pending: Vec<ParkedBlock>,
    /// Network-path receipt proofs buffered until the source block's CER lands,
    /// keyed by source block. Local-path proofs are never buffered (already on
    /// disk). See `ReceiptSource`.
    pending_unverified_receipts: HashMap<CryptoHash, Vec<ReceiptProof>>,
}

enum ApplyOutcome {
    Applied,
    Dropped,
    NotReady,
}

impl PerShardExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shard_id: ShardId,
        store: Store,
        genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        core_reader: SpiceCoreReader,
        validator_signer: MutableValidatorSigner,
        network_adapter: PeerManagerAdapter,
        core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
        data_distributor_adapter: SpiceDataDistributorAdapter,
        coordinator_sender: Sender<PerShardChunkApplied>,
        myself_sender: Sender<AppliedContinue>,
    ) -> Self {
        let chain_store = ChainStore::new(store, true, genesis.transaction_validity_period);
        Self {
            shard_id,
            chain_store,
            runtime_adapter,
            epoch_manager,
            shard_tracker,
            core_reader,
            validator_signer,
            network_adapter,
            core_writer_sender,
            data_distributor_adapter,
            coordinator_sender,
            myself_sender,
            pending: Vec::new(),
            pending_unverified_receipts: HashMap::new(),
        }
    }

    /// Verify any buffered `FromNetwork` receipts from `source_block` against its
    /// CER and write the valid ones. No-op until the CER is on chain.
    fn try_verify(&mut self, source_block: &CryptoHash) {
        let header = match self.chain_store.get_block_header(source_block) {
            Ok(header) => header,
            Err(_) => return,
        };
        let cer = match self.core_reader.get_block_execution_results(&header) {
            Ok(Some(cer)) => cer,
            Ok(None) => return,
            Err(err) => {
                tracing::error!(target: "chunk_executor", ?err, %source_block, "failed reading CER for receipt verification");
                return;
            }
        };
        let Some(proofs) = self.pending_unverified_receipts.remove(source_block) else {
            return;
        };
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        for proof in proofs {
            let from_shard_id = proof.1.from_shard_id;
            let Some(result) = cer.0.get(&from_shard_id) else {
                continue;
            };
            if proof.verify_against_receipt_root(result.outgoing_receipts_root) {
                save_receipt_proof(&mut store_update, source_block, &proof);
            } else {
                tracing::warn!(target: "chunk_executor", %source_block, ?from_shard_id, "dropping invalid network receipt proof");
            }
        }
        store_update.commit();
    }

    /// Apply at most one parked chunk. Scans in height order and acts on the
    /// first ready/droppable block; continues past `NotReady` (fork
    /// correctness). Self-sends `AppliedContinue` after acting so other queued
    /// messages dispatch first.
    fn try_apply_one(&mut self) {
        self.pending.sort_by_key(|parked| parked.height);
        let hashes: Vec<CryptoHash> = self.pending.iter().map(|parked| parked.block_hash).collect();
        for block_hash in hashes {
            let outcome = match self.try_apply(&block_hash) {
                Ok(outcome) => outcome,
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, %block_hash, shard_id=%self.shard_id, "per-shard apply failed");
                    return;
                }
            };
            match outcome {
                ApplyOutcome::NotReady => continue,
                ApplyOutcome::Applied | ApplyOutcome::Dropped => {
                    self.pending.retain(|parked| parked.block_hash != block_hash);
                    self.myself_sender.send(AppliedContinue { just_applied: block_hash });
                    return;
                }
            }
        }
    }

    fn try_apply(&mut self, block_hash: &CryptoHash) -> Result<ApplyOutcome, Error> {
        let block = self.chain_store.get_block(block_hash)?;
        if !is_descendant_of_final_execution_head(&self.chain_store, block.header()) {
            return Ok(ApplyOutcome::Dropped);
        }
        if self.chunk_extra_exists(block_hash)? {
            return Ok(ApplyOutcome::Dropped);
        }

        let header = block.header();
        let prev_block_hash = *header.prev_hash();
        let prev_block = self.chain_store.get_block(&prev_block_hash)?;

        let Some(prev_block_execution_results) =
            self.core_reader.get_block_execution_results(prev_block.header())?
        else {
            return Ok(ApplyOutcome::NotReady);
        };

        if !self.shard_tracker.should_apply_chunk(
            ApplyChunksMode::IsCaughtUp,
            &prev_block_hash,
            self.shard_id,
        ) {
            // This node doesn't track the shard at this block — nothing to do.
            return Ok(ApplyOutcome::Dropped);
        }

        let Some(prev_chunk_extra) = self.get_chunk_extra(&prev_block_hash)? else {
            return Ok(ApplyOutcome::NotReady);
        };

        let prev_epoch_id = self.epoch_manager.get_epoch_id(&prev_block_hash)?;
        let prev_block_shard_ids = self.epoch_manager.shard_ids(&prev_epoch_id)?;
        let mut incoming_receipts = if prev_block.header().is_genesis() {
            vec![]
        } else {
            let proofs = get_receipt_proofs_for_shard(
                &self.chain_store.store(),
                &prev_block_hash,
                self.shard_id,
            );
            if proofs.len() != prev_block_shard_ids.len() {
                return Ok(ApplyOutcome::NotReady);
            }
            proofs
        };

        let shard_layout = self.epoch_manager.get_shard_layout(&header.epoch_id())?;
        let shard_index = shard_layout.get_shard_index(self.shard_id)?;
        let chunk_header =
            block.chunks().get(shard_index).ok_or(Error::InvalidShardId(self.shard_id))?.clone();
        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);

        // Inline apply (mirrors ChunkExecutorActor::get_update_shard_job, but
        // run synchronously on this shard's own thread — no spawner).
        let block_context = build_spice_apply_chunk_block_context(
            block.header(),
            &prev_block_execution_results,
            self.epoch_manager.as_ref(),
        )?;
        shuffle_receipt_proofs(&mut incoming_receipts, get_receipts_shuffle_salt(&block));
        let receipts = collect_receipts(&incoming_receipts);
        let prev_block_header = self.chain_store.get_block_header(&prev_block_hash)?;
        let (transactions, chunk_hash) = match self
            .get_new_chunk_if_valid(&chunk_header, block_context.height)?
        {
            Some(chunk) => {
                let chunk_hash = chunk.chunk_hash().clone();
                let tx_valid_list =
                    self.chain_store.compute_transaction_validity(&prev_block_header, &chunk);
                (
                    SignedValidPeriodTransactions::new(chunk.into_transactions(), tx_valid_list),
                    Some(chunk_hash),
                )
            }
            None => (SignedValidPeriodTransactions::new(vec![], vec![]), None),
        };
        let prev_validator_proposals =
            self.core_reader.prev_validator_proposals(&prev_block_hash, self.shard_id)?;
        let memtrie_pin = self
            .runtime_adapter
            .get_tries()
            .maybe_pin_memtrie_root(shard_uid, *prev_chunk_extra.state_root())?;
        let shard_update_reason = ShardUpdateReason::NewChunk(NewChunkData {
            gas_limit: prev_chunk_extra.gas_limit(),
            prev_state_root: *prev_chunk_extra.state_root(),
            prev_validator_proposals,
            chunk_hash,
            transactions,
            receipts,
            block: block_context,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Db,
                state_patch: SandboxStatePatch::default(),
            },
        });
        let span = tracing::Span::current();
        let result = process_shard_update(
            &span,
            self.runtime_adapter.as_ref(),
            shard_update_reason,
            ShardContext { shard_uid, should_apply_chunk: true },
            memtrie_pin,
            None,
        )?;
        let ShardUpdateResult::NewChunk(new_chunk_result) = &result else {
            panic!("missing chunks are not expected in SPICE");
        };

        let (outgoing_receipts_root, receipt_proofs) =
            Chain::create_receipts_proofs_from_outgoing_receipts(
                &shard_layout,
                self.shard_id,
                new_chunk_result.apply_result.outgoing_receipts.clone(),
            )?;
        let bandwidth_scheduler_state_hash =
            new_chunk_result.apply_result.bandwidth_scheduler_state_hash;

        // Sender writes its own outgoing receipt proofs durably (matches the old
        // executor's save_produced_receipts) before any outbound message.
        self.save_produced_receipts(block_hash, &receipt_proofs);
        self.emit(&block, new_chunk_result, &receipt_proofs, outgoing_receipts_root)?;

        // Persist this shard's apply artifacts. HACK (prototype): reuse the
        // monolithic apply_chunk_postprocessing with a 1-element results vec
        // rather than adapter-only per-shard writes. The 1-element vec makes the
        // bandwidth sanity check vacuous here; the cross-shard check lives in the
        // coordinator. Head advance is the coordinator's job, so we do NOT touch
        // the spice heads here.
        let mut chain_update = ChainUpdate::new(
            &mut self.chain_store,
            self.epoch_manager.clone(),
            self.runtime_adapter.clone(),
            DoomslugThresholdMode::NoApprovals,
        );
        chain_update.apply_chunk_postprocessing(&block, vec![result], false)?;
        chain_update.commit()?;

        self.coordinator_sender.send(PerShardChunkApplied {
            block_hash: *block_hash,
            shard_id: self.shard_id,
            outgoing_receipt_proofs: receipt_proofs,
            bandwidth_scheduler_state_hash,
        });
        Ok(ApplyOutcome::Applied)
    }

    /// Emit this shard's outputs after a successful apply, mirroring the old
    /// executor's per-result loop: endorse if this node is a chunk validator,
    /// and (producer-gated) distribute outgoing receipts + the state witness.
    fn emit(
        &self,
        block: &Block,
        new_chunk_result: &NewChunkResult,
        receipt_proofs: &[ReceiptProof],
        outgoing_receipts_root: CryptoHash,
    ) -> Result<(), Error> {
        let Some(my_signer) = self.validator_signer.get() else {
            return Ok(());
        };
        let epoch_id = self.epoch_manager.get_epoch_id(block.hash())?;

        // Endorse if we are a chunk validator (regardless of producer status).
        let validators = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            self.shard_id,
            block.header().height(),
        )?;
        if validators.contains(my_signer.validator_id()) {
            let NewChunkResult { gas_limit, apply_result, .. } = new_chunk_result;
            let execution_result =
                new_execution_result(*gas_limit, apply_result, outgoing_receipts_root);
            let endorsement = SpiceChunkEndorsement::new(
                SpiceChunkId { block_hash: *block.hash(), shard_id: self.shard_id },
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

        // Distribute the witness + outgoing receipts only if we are the chunk
        // producer for this shard (the data distributor asserts this).
        let producers =
            self.epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, self.shard_id)?;
        if producers.contains(my_signer.validator_id()) {
            self.data_distributor_adapter.send(SpiceDistributorOutgoingReceipts {
                block_hash: *block.hash(),
                receipt_proofs: receipt_proofs.to_vec(),
            });
            self.distribute_witness(block, new_chunk_result, outgoing_receipts_root)?;
        }
        Ok(())
    }

    fn distribute_witness(
        &self,
        block: &Block,
        new_chunk_result: &NewChunkResult,
        outgoing_receipts_root: CryptoHash,
    ) -> Result<(), Error> {
        let NewChunkResult { gas_limit, apply_result, .. } = new_chunk_result;
        let execution_result =
            new_execution_result(*gas_limit, apply_result, outgoing_receipts_root);
        let execution_result_hash = execution_result.compute_hash();
        let (state_witness, contract_accesses) =
            self.create_chunk_execution_data(block, apply_result, execution_result_hash)?;
        save_witness_and_contract_accesses(
            &self.chain_store,
            block.hash(),
            self.shard_id,
            &state_witness,
            &contract_accesses,
        );
        self.data_distributor_adapter
            .send(SpiceDistributorStateWitness { state_witness, contract_accesses });
        Ok(())
    }

    /// Build the state witness + contract accesses for this shard's chunk.
    /// Ported from the monolithic executor's `create_chunk_execution_data`.
    fn create_chunk_execution_data(
        &self,
        block: &Block,
        apply_result: &ApplyChunkResult,
        execution_result_hash: ChunkExecutionResultHash,
    ) -> Result<(SpiceChunkStateWitness, HashSet<CodeHash>), Error> {
        let block_hash = block.header().hash();
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        let (transactions, proof_of_invalid_chunk) = {
            let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
            let shard_index = shard_layout.get_shard_index(self.shard_id)?;
            let chunk_headers = block.chunks();
            let chunk_header =
                chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(self.shard_id))?;
            match self.get_new_chunk_if_valid(chunk_header, block.header().height())? {
                Some(chunk) => (chunk.into_transactions(), None),
                None => {
                    let proof = if chunk_header.is_new_chunk(block.header().height()) {
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

        let PartialState::TrieValues(base_state_values) = apply_result.proof.clone().unwrap().nodes;
        let main_transition = SpiceChunkStateTransition {
            base_state: PartialState::TrieValues(base_state_values),
            post_state_root: apply_result.new_root,
        };

        let source_receipt_proofs: HashMap<ShardId, ReceiptProof> = {
            let prev_block_hash = block.header().prev_hash();
            let (_, prev_block_shard_id, _) = self
                .epoch_manager
                .get_prev_shard_id_from_prev_hash(prev_block_hash, self.shard_id)?;
            get_receipt_proofs_for_shard(
                &self.chain_store.store(),
                prev_block_hash,
                prev_block_shard_id,
            )
            .into_iter()
            .map(|proof| (proof.1.from_shard_id, proof))
            .collect()
        };

        let contract_accesses_hash = compute_contract_accesses_hash(&contract_accesses);
        let state_witness = SpiceChunkStateWitness::new(
            SpiceChunkId { block_hash: *block_hash, shard_id: self.shard_id },
            main_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            execution_result_hash,
            contract_accesses_hash,
            proof_of_invalid_chunk,
        );
        Ok((state_witness, contract_accesses))
    }

    fn save_produced_receipts(&self, block_hash: &CryptoHash, receipt_proofs: &[ReceiptProof]) {
        let store = self.chain_store.store();
        let mut store_update = store.store_update();
        for proof in receipt_proofs {
            save_receipt_proof(&mut store_update, block_hash, proof);
        }
        store_update.commit();
    }

    fn get_chunk_extra(&self, block_hash: &CryptoHash) -> Result<Option<Arc<ChunkExtra>>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), self.shard_id, &epoch_id)?;
        match self.chain_store.get_chunk_extra(block_hash, &shard_uid) {
            Ok(chunk_extra) => Ok(Some(chunk_extra)),
            Err(Error::DBNotFoundErr(_)) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn chunk_extra_exists(&self, block_hash: &CryptoHash) -> Result<bool, Error> {
        self.get_chunk_extra(block_hash).map(|option| option.is_some())
    }

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
}

impl Actor for PerShardExecutor {}

impl Handler<IncomingBlock> for PerShardExecutor {
    fn handle(&mut self, IncomingBlock { block_hash }: IncomingBlock) {
        if !self.pending.iter().any(|parked| parked.block_hash == block_hash) {
            let height = match self.chain_store.get_block_header(&block_hash) {
                Ok(header) => header.height(),
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, %block_hash, "missing header for incoming block");
                    return;
                }
            };
            self.pending.push(ParkedBlock { block_hash, height });
        }
        self.try_apply_one();
    }
}

impl Handler<AppliedContinue> for PerShardExecutor {
    fn handle(&mut self, _msg: AppliedContinue) {
        self.try_apply_one();
    }
}

impl Handler<IncomingReceipt> for PerShardExecutor {
    fn handle(&mut self, IncomingReceipt { block_hash, proof, source }: IncomingReceipt) {
        match source {
            // Sender already wrote the proof to disk; just re-check.
            ReceiptSource::LocallyVerified => {}
            ReceiptSource::FromNetwork => {
                self.pending_unverified_receipts.entry(block_hash).or_default().push(proof);
                self.try_verify(&block_hash);
            }
        }
        self.try_apply_one();
    }
}

impl Handler<CERCertified> for PerShardExecutor {
    fn handle(&mut self, CERCertified { block_hash }: CERCertified) {
        self.try_verify(&block_hash);
        self.try_apply_one();
    }
}
