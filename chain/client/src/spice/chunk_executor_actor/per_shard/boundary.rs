//! Per-shard bootstrap of the spice activation boundary: endorsing and
//! distributing the activation parent's pre-spice chunk as spice data.

use super::PerShardChunkExecutor;
use crate::spice::chunk_executor_actor::storage::save_witness_and_contract_accesses;
use crate::spice::chunk_validator_actor::send_spice_chunk_endorsement;
use crate::spice::data_distributor_actor::SpiceDistributorStateWitness;
use near_async::messaging::{CanSend, IntoSender};
use near_chain::spice::boundary::{
    anchor_and_replay_blocks, is_spice_activation_parent,
    synthesize_execution_result_and_receipt_proofs,
};
use near_chain::{Block, Error, ReceiptFilter, get_incoming_receipts_for_shard};
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::{
    SpiceBoundaryChunkStateWitness, SpiceChunkStateWitness,
};
use near_primitives::state_sync::ReceiptProofResponse;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::stateless_validation::state_witness::ChunkStateTransition;
use near_primitives::stateless_validation::stored_chunk_state_transition_data::{
    StoredChunkStateTransitionData, StoredChunkStateTransitionDataV1,
};
use near_primitives::types::{ChunkExecutionResult, SpiceChunkId};
use near_primitives::utils::get_block_shard_id;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use std::collections::{HashMap, HashSet};

impl PerShardChunkExecutor {
    /// Bootstraps this shard across the activation boundary; a no-op unless `block`
    /// is an activation parent.
    pub(crate) fn bootstrap_boundary_source_block(&self, block: &Block) -> Result<(), Error> {
        if !is_spice_activation_parent(self.epoch_manager.as_ref(), block.hash())? {
            return Ok(());
        }
        let shard_id = self.shard_uid.shard_id();
        let (execution_result, receipt_proofs) = synthesize_execution_result_and_receipt_proofs(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            block,
            shard_id,
        )?;
        self.save_produced_receipts(block.hash(), &receipt_proofs);

        if let Some(my_signer) = self.validator_signer.get() {
            self.endorse_boundary_execution_result(block, &my_signer, execution_result)?;

            // Distribution keys the boundary data's producers on the next block's
            // epoch
            let next_block_epoch_id =
                self.epoch_manager.get_epoch_id_from_prev_block(block.hash())?;
            let epoch_producers = self
                .epoch_manager
                .get_epoch_chunk_producers_for_shard(&next_block_epoch_id, shard_id)?;
            if epoch_producers.contains(my_signer.validator_id()) {
                self.send_outgoing_receipts(block, receipt_proofs);
                self.distribute_boundary_witness(block)?;
            }
        }
        Ok(())
    }

    /// Absent on nodes that could not produce a witness, and after GC.
    fn read_recorded_transition(
        &self,
        block_hash: &CryptoHash,
    ) -> Option<StoredChunkStateTransitionDataV1> {
        let stored: StoredChunkStateTransitionData = self.chain_store.store().get_ser(
            DBCol::StateTransitionData,
            &get_block_shard_id(block_hash, self.shard_uid.shard_id()),
        )?;
        let StoredChunkStateTransitionData::V1(data) = stored;
        Some(data)
    }

    /// Packages and distributes the state witness of the activation parent's chunk
    /// for this shard.
    fn distribute_boundary_witness(&self, block: &Block) -> Result<(), Error> {
        let shard_id = self.shard_uid.shard_id();
        let epoch_id = self.epoch_manager.get_epoch_id(block.hash())?;
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_headers = block.chunks();
        let chunk_header = chunk_headers.get(shard_index).ok_or(Error::InvalidShardId(shard_id))?;

        let (anchor_block, replay_blocks) = anchor_and_replay_blocks(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            block,
            shard_id,
        )?;

        let Some(chunk) =
            self.get_new_chunk_if_valid(chunk_header, anchor_block.header().height())?
        else {
            // The anchor's chunk is invalid (malicious pre-spice producer): there is
            // no state transition of it to attest.
            return Ok(());
        };
        let transactions = chunk.into_transactions();

        let Some(StoredChunkStateTransitionDataV1 {
            base_state,
            receipts_hash,
            contract_accesses,
            contract_deploys: _,
        }) = self.read_recorded_transition(anchor_block.hash())
        else {
            tracing::warn!(
                target: "chunk_executor",
                block_hash = %block.hash(),
                anchor_block_hash = %anchor_block.hash(),
                %shard_id,
                "no recorded state transition to build the boundary witness from",
            );
            return Ok(());
        };

        let mut implicit_transitions = Vec::with_capacity(replay_blocks.len());
        for replay_block in replay_blocks {
            let Some(replay_transition) = self.read_recorded_transition(replay_block.hash()) else {
                tracing::warn!(
                    target: "chunk_executor",
                    block_hash = %block.hash(),
                    replay_block_hash = %replay_block.hash(),
                    %shard_id,
                    "no recorded state transition for an implicit replay of the boundary witness",
                );
                return Ok(());
            };
            let chunk_extra = self
                .chain_store
                .chunk_store()
                .get_chunk_extra(replay_block.hash(), &self.shard_uid)?;
            implicit_transitions.push(ChunkStateTransition {
                block_hash: *replay_block.hash(),
                base_state: replay_transition.base_state,
                post_state_root: *chunk_extra.state_root(),
            });
        }

        // The anchor's application consumed the incoming receipts of every block
        // since the shard's previous inclusion; the witness carries one proof per
        // chunk included across that whole range.
        let anchor_prev_block = self.chain_store.get_block(anchor_block.header().prev_hash())?;
        let previous_inclusion_height = {
            let prev_shard_layout =
                self.epoch_manager.get_shard_layout(anchor_prev_block.header().epoch_id())?;
            let prev_shard_index = prev_shard_layout.get_shard_index(shard_id)?;
            let anchor_prev_chunks = anchor_prev_block.chunks();
            anchor_prev_chunks
                .get(prev_shard_index)
                .ok_or(Error::InvalidShardId(shard_id))?
                .height_included()
        };
        let anchor_shard_layout =
            self.epoch_manager.get_shard_layout(anchor_block.header().epoch_id())?;
        let mut range_receipt_proofs = vec![ReceiptProofResponse(
            *anchor_block.hash(),
            self.chain_store.get_incoming_receipts(anchor_block.hash(), shard_id)?,
        )];
        range_receipt_proofs.extend(get_incoming_receipts_for_shard(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            shard_id,
            &anchor_shard_layout,
            *anchor_block.header().prev_hash(),
            previous_inclusion_height,
            ReceiptFilter::All,
        )?);
        let mut source_receipt_proofs = HashMap::new();
        for ReceiptProofResponse(source_block_hash, proofs) in &range_receipt_proofs {
            let source_block = self.chain_store.get_block(source_block_hash)?;
            let source_shard_layout =
                self.epoch_manager.get_shard_layout(source_block.header().epoch_id())?;
            let source_chunks = source_block.chunks();
            for proof in proofs.iter() {
                let from_shard_id = proof.1.from_shard_id;
                let shard_index = source_shard_layout.get_shard_index(from_shard_id)?;
                let source_chunk_header =
                    source_chunks.get(shard_index).ok_or(Error::InvalidShardId(from_shard_id))?;
                source_receipt_proofs
                    .insert(source_chunk_header.chunk_hash().clone(), proof.clone());
            }
        }

        let state_witness = SpiceChunkStateWitness::Boundary(SpiceBoundaryChunkStateWitness {
            chunk_id: SpiceChunkId { block_hash: *block.hash(), shard_id },
            pre_state: base_state,
            source_receipt_proofs,
            applied_receipts_hash: receipts_hash,
            transactions,
            contract_accesses: contract_accesses.iter().cloned().collect(),
            implicit_transitions,
        });
        let contract_accesses: HashSet<CodeHash> = contract_accesses.into_iter().collect();
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

    /// Endorses the synthesized result of the activation parent's chunk. A designated
    /// chunk validator broadcasts; any other epoch validator only records locally,
    /// since peers reject an endorsement before the chunk is fallback-eligible.
    fn endorse_boundary_execution_result(
        &self,
        block: &Block,
        my_signer: &ValidatorSigner,
        execution_result: ChunkExecutionResult,
    ) -> Result<(), Error> {
        let epoch_id = self.epoch_manager.get_epoch_id(block.hash())?;
        let validators_at_height = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            self.shard_uid.shard_id(),
            block.header().height(),
        )?;
        let is_designated = validators_at_height.contains(my_signer.validator_id());
        if !is_designated
            && self
                .epoch_manager
                .get_validator_by_account_id(&epoch_id, my_signer.validator_id())
                .is_err()
        {
            return Ok(());
        }
        let endorsement = SpiceChunkEndorsement::new(
            SpiceChunkId { block_hash: *block.hash(), shard_id: self.shard_uid.shard_id() },
            execution_result,
            my_signer,
        );
        if is_designated {
            send_spice_chunk_endorsement(
                endorsement.clone(),
                self.epoch_manager.as_ref(),
                &self.network_adapter.clone().into_sender(),
                my_signer,
            );
        }
        self.core_writer_sender
            .send(SpiceChunkEndorsementMessage(endorsement, RecvMessagePermit::none()));
        Ok(())
    }
}
