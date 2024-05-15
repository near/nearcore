use std::collections::HashMap;

use near_async::messaging::CanSend;
use near_chain::{BlockHeader, Chain, ChainStoreAccess};
use near_chain_primitives::Error;
use near_o11y::log_assert_fail;
use near_primitives::challenge::PartialState;
use near_primitives::checked_feature;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::stateless_validation::{
    ChunkStateTransition, ChunkStateWitness, StoredChunkStateTransitionData,
};
use near_primitives::types::{AccountId, EpochId};

use crate::Client;

use super::partial_witness::partial_witness_actor::DistributeStateWitnessRequest;

impl Client {
    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
        transactions_storage_proof: Option<PartialState>,
    ) -> Result<(), Error> {
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        if !checked_feature!("stable", StatelessValidationV0, protocol_version) {
            return Ok(());
        }

        let my_signer = self.validator_signer.as_ref().ok_or(Error::NotAValidator)?.clone();
        let state_witness = self.create_state_witness(
            my_signer.validator_id().clone(),
            prev_block_header,
            prev_chunk_header,
            chunk,
            transactions_storage_proof,
        )?;

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&state_witness)?;
        }

        let chunk_header = chunk.cloned_header();

        self.partial_witness_adapter.send(DistributeStateWitnessRequest {
            epoch_id: epoch_id.clone(),
            chunk_header,
            state_witness,
        });
        Ok(())
    }

    pub(crate) fn create_state_witness(
        &mut self,
        chunk_producer: AccountId,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
        transactions_storage_proof: Option<PartialState>,
    ) -> Result<ChunkStateWitness, Error> {
        let chunk_header = chunk.cloned_header();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let prev_chunk = self.chain.get_chunk(&prev_chunk_header.chunk_hash())?;
        let (main_state_transition, implicit_transitions, applied_receipts_hash) =
            self.collect_state_transition_data(&chunk_header, prev_chunk_header)?;

        let new_transactions = chunk.transactions().to_vec();
        let new_transactions_validation_state = if new_transactions.is_empty() {
            PartialState::default()
        } else {
            // With stateless validation chunk producer uses recording reads when validating transactions.
            // The storage proof must be available here.
            transactions_storage_proof.ok_or_else(|| {
                let message = "Missing storage proof for transactions validation";
                log_assert_fail!("{message}");
                Error::Other(message.to_owned())
            })?
        };

        let source_receipt_proofs =
            self.collect_source_receipt_proofs(prev_block_header, prev_chunk_header)?;

        let witness = ChunkStateWitness::new(
            chunk_producer,
            epoch_id,
            chunk_header,
            main_state_transition,
            source_receipt_proofs,
            // (Could also be derived from iterating through the receipts, but
            // that defeats the purpose of this check being a debugging
            // mechanism.)
            applied_receipts_hash,
            prev_chunk.transactions().to_vec(),
            implicit_transitions,
            new_transactions,
            new_transactions_validation_state,
        );
        Ok(witness)
    }

    /// Collect state transition data necessary to produce state witness for
    /// `chunk_header`.
    fn collect_state_transition_data(
        &mut self,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: &ShardChunkHeader,
    ) -> Result<(ChunkStateTransition, Vec<ChunkStateTransition>, CryptoHash), Error> {
        let store = self.chain.chain_store().store();
        let shard_id = chunk_header.shard_id();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
        let prev_chunk_height_included = prev_chunk_header.height_included();
        let mut prev_blocks = self.chain.get_blocks_until_height(
            *chunk_header.prev_block_hash(),
            prev_chunk_height_included,
            true,
        )?;
        prev_blocks.reverse();
        let (main_block, implicit_blocks) = prev_blocks.split_first().unwrap();
        let (base_state, receipts_hash) = if prev_chunk_header.is_genesis() {
            (Default::default(), hash(&borsh::to_vec::<[Receipt]>(&[]).unwrap()))
        } else {
            let StoredChunkStateTransitionData { base_state, receipts_hash } = store
                .get_ser(
                    near_store::DBCol::StateTransitionData,
                    &near_primitives::utils::get_block_shard_id(main_block, shard_id),
                )?
                .ok_or_else(|| {
                    let message = format!(
                        "Missing main transition state proof for block {main_block} and shard {shard_id}"
                    );
                    if !cfg!(feature = "shadow_chunk_validation") {
                        log_assert_fail!("{message}");
                    }
                    Error::Other(message)
                })?;
            (base_state, receipts_hash)
        };
        let main_transition = ChunkStateTransition {
            block_hash: *main_block,
            base_state,
            post_state_root: *self.chain.get_chunk_extra(main_block, &shard_uid)?.state_root(),
        };

        let mut implicit_transitions = vec![];
        for block_hash in implicit_blocks {
            let StoredChunkStateTransitionData { base_state, .. } = store
                .get_ser(
                    near_store::DBCol::StateTransitionData,
                    &near_primitives::utils::get_block_shard_id(block_hash, shard_id),
                )?
                .ok_or_else(|| {
                    let message = format!(
                        "Missing implicit transition state proof for block {block_hash} and shard {shard_id}"
                    );
                    if !cfg!(feature = "shadow_chunk_validation") {
                        log_assert_fail!("{message}");
                    }
                    Error::Other(message)
                })?;
            implicit_transitions.push(ChunkStateTransition {
                block_hash: *block_hash,
                base_state,
                post_state_root: *self.chain.get_chunk_extra(block_hash, &shard_uid)?.state_root(),
            });
        }

        Ok((main_transition, implicit_transitions, receipts_hash))
    }

    /// State witness proves the execution of receipts proposed by `prev_chunk`.
    /// This function collects all incoming receipts for `prev_chunk`, along with the proofs
    /// that those receipts really originate from the right chunks.
    /// TODO(resharding): `get_incoming_receipts_for_shard` generates invalid proofs on resharding
    /// boundaries, because it removes the receipts that target the other half of a split shard,
    /// which makes the proof invalid. We need to collect the original proof and later, after verifcation,
    /// filter it to remove the receipts that were meant for the other half of the split shard.
    fn collect_source_receipt_proofs(
        &self,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
    ) -> Result<HashMap<ChunkHash, ReceiptProof>, Error> {
        if prev_chunk_header.is_genesis() {
            // State witness which proves the execution of the first chunk in the blockchain
            // doesn't have any source receipts.
            return Ok(HashMap::new());
        }

        // Find the first block that included `prev_chunk`.
        // Incoming receipts were generated by the blocks before this one.
        let prev_chunk_original_block: BlockHeader = {
            let mut cur_block: BlockHeader = prev_block_header.clone();
            loop {
                if prev_chunk_header.is_new_chunk(cur_block.height()) {
                    break cur_block;
                }

                cur_block = self.chain.chain_store().get_block_header(cur_block.prev_hash())?;
            }
        };

        // Get the last block that contained `prev_prev_chunk` (the chunk before `prev_chunk`).
        // We are interested in all incoming receipts that weren't handled by `prev_prev_chunk`.
        let prev_prev_chunk_block =
            self.chain.chain_store().get_block(prev_chunk_original_block.prev_hash())?;
        // Find the header of the chunk before `prev_chunk`
        let prev_prev_chunk_header = Chain::get_prev_chunk_header(
            self.epoch_manager.as_ref(),
            &prev_prev_chunk_block,
            prev_chunk_header.shard_id(),
        )?;

        // Fetch all incoming receipts for `prev_chunk`.
        // They will be between `prev_prev_chunk.height_included`` (first block containing `prev_prev_chunk`)
        // and `prev_chunk_original_block`
        let incoming_receipt_proofs = self.chain.chain_store().get_incoming_receipts_for_shard(
            self.epoch_manager.as_ref(),
            prev_chunk_header.shard_id(),
            *prev_chunk_original_block.hash(),
            prev_prev_chunk_header.height_included(),
        )?;

        // Convert to the right format (from [block_hash -> Vec<ReceiptProof>] to [chunk_hash -> ReceiptProof])
        let mut source_receipt_proofs = HashMap::new();
        for receipt_proof_response in incoming_receipt_proofs {
            let from_block = self.chain.chain_store().get_block(&receipt_proof_response.0)?;
            for proof in receipt_proof_response.1.iter() {
                let from_shard_id: usize = proof
                    .1
                    .from_shard_id
                    .try_into()
                    .map_err(|_| Error::Other("Couldn't convert u64 to usize!".into()))?;
                let from_chunk_hash = from_block
                    .chunks()
                    .get(from_shard_id)
                    .ok_or_else(|| Error::InvalidShardId(proof.1.from_shard_id))?
                    .chunk_hash();
                let insert_res =
                    source_receipt_proofs.insert(from_chunk_hash.clone(), proof.clone());
                if insert_res.is_some() {
                    // There should be exactly one chunk proof for every chunk
                    return Err(Error::Other(format!(
                        "collect_source_receipt_proofs: duplicate receipt proof for chunk {:?}",
                        from_chunk_hash
                    )));
                }
            }
        }
        Ok(source_receipt_proofs)
    }
}
