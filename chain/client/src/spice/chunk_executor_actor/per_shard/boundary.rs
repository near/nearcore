//! Per-shard bootstrap of the spice activation boundary: endorsing and
//! distributing the last pre-spice block's pre-spice chunk as spice data.

use super::PerShardChunkExecutor;
use crate::spice::chunk_executor_actor::storage::save_witness_and_contract_accesses;
use crate::spice::chunk_validator_actor::send_spice_chunk_endorsement;
use crate::spice::data_distributor_actor::SpiceDistributorStateWitness;
use near_async::messaging::{CanSend, IntoSender};
use near_chain::spice::boundary_synthesis::{
    boundary_state_witness, execution_result_and_receipt_proofs_from_pre_spice_apply,
};
use near_chain::{Block, Error};
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_primitives::errors::EpochError;
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::{ChunkExecutionResult, SpiceChunkId};
use near_primitives::validator_signer::ValidatorSigner;
use std::collections::HashSet;

impl PerShardChunkExecutor {
    /// Returns the receipt proofs it persisted, for local-path fanout. `block` must be a
    /// last pre-spice block.
    pub(crate) fn endorse_and_send_receipts_and_witness_for_last_pre_spice_block(
        &self,
        block: &Block,
    ) -> Result<Vec<ReceiptProof>, Error> {
        let shard_id = self.shard_uid.shard_id();
        let (execution_result, receipt_proofs) =
            execution_result_and_receipt_proofs_from_pre_spice_apply(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                block,
                shard_id,
            )?;
        self.save_produced_receipts(block.hash(), &receipt_proofs);

        if let Some(my_signer) = self.validator_signer.get() {
            // Receipts go first: other shards' progress depends on them, not on this
            // node's endorsement.
            if let Err(err) =
                self.send_boundary_data_as_producer(block, &my_signer, receipt_proofs.clone())
            {
                tracing::error!(target: "chunk_executor", ?err, block_hash = %block.hash(), %shard_id, "failed to send boundary receipts and witness");
            }
            if let Err(err) =
                self.endorse_boundary_execution_result(block, &my_signer, execution_result)
            {
                tracing::error!(target: "chunk_executor", ?err, block_hash = %block.hash(), %shard_id, "failed to endorse boundary execution result");
            }
        }
        Ok(receipt_proofs)
    }

    /// Sends the outgoing receipts and the state witness of the last pre-spice block's
    /// chunk when this node is one of its chunk producers.
    fn send_boundary_data_as_producer(
        &self,
        block: &Block,
        my_signer: &ValidatorSigner,
        receipt_proofs: Vec<ReceiptProof>,
    ) -> Result<(), Error> {
        // Distribution keys the boundary data's producers on the block's own
        // epoch, whose chunk producers applied it and hold its state transition.
        let epoch_producers = self.epoch_manager.get_epoch_chunk_producers_for_shard(
            block.header().epoch_id(),
            self.shard_uid.shard_id(),
        )?;
        if !epoch_producers.contains(my_signer.validator_id()) {
            return Ok(());
        }
        self.send_outgoing_receipts(block, receipt_proofs);
        self.distribute_boundary_witness(block)
    }

    /// Distributes the state witness of the last pre-spice block's chunk for this
    /// shard, when this node recorded the transitions to build it from.
    fn distribute_boundary_witness(&self, block: &Block) -> Result<(), Error> {
        let shard_id = self.shard_uid.shard_id();
        let Some(witness) = boundary_state_witness(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            block,
            shard_id,
        )?
        else {
            return Ok(());
        };
        let contract_accesses: HashSet<CodeHash> =
            witness.contract_accesses.iter().cloned().collect();
        let state_witness = SpiceChunkStateWitness::Boundary(witness);
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

    /// Endorses the synthesized result of the last pre-spice block's chunk. A designated
    /// chunk validator broadcasts; any other epoch validator only records locally,
    /// since peers reject an endorsement before the chunk is fallback-eligible.
    fn endorse_boundary_execution_result(
        &self,
        block: &Block,
        my_signer: &ValidatorSigner,
        execution_result: ChunkExecutionResult,
    ) -> Result<(), Error> {
        let epoch_id = block.header().epoch_id();
        let validators_at_height = self.epoch_manager.get_chunk_validator_assignments(
            epoch_id,
            self.shard_uid.shard_id(),
            block.header().height(),
        )?;
        let is_designated = validators_at_height.contains(my_signer.validator_id());
        if !is_designated {
            match self.epoch_manager.get_validator_by_account_id(epoch_id, my_signer.validator_id())
            {
                Ok(_) => {}
                Err(EpochError::NotAValidator(..)) => return Ok(()),
                Err(err) => return Err(err.into()),
            }
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
