use super::partial_witness::partial_witness_actor::DistributeStateWitnessRequest;
use crate::Client;
use crate::stateless_validation::chunk_validator::send_chunk_endorsement_to_block_producers;
use near_async::messaging::{CanSend, IntoSender};
use near_chain::{
    BlockHeader, Chain, ChainStoreAccess, ReceiptFilter, get_incoming_receipts_for_shard,
};
use near_chain_primitives::Error;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_o11y::log_assert_fail;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateTransition, ChunkStateWitness,
};
use near_primitives::stateless_validation::stored_chunk_state_transition_data::{
    StoredChunkStateTransitionData, StoredChunkStateTransitionDataV1,
};
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use std::collections::HashMap;
use std::sync::Arc;

/// Result of collecting state transition data from the database to generate a state witness.
/// Keep this private to this file.
struct StateTransitionData {
    main_transition: ChunkStateTransition,
    main_transition_shard_id: ShardId,
    implicit_transitions: Vec<ChunkStateTransition>,
    applied_receipts_hash: CryptoHash,
    contract_updates: ContractUpdates,
}

/// Result of creating witness.
///
/// Since we distribute the contracts accessed separately from the state witness,
/// we contain them in separate fields in the result.
pub(crate) struct CreateWitnessResult {
    /// State witness created.
    pub(crate) state_witness: ChunkStateWitness,
    /// Contracts accessed and deployed while applying the chunk.
    pub(crate) contract_updates: ContractUpdates,
    /// Shard id for the main transition. This can be different from the
    /// witness chunk header shard id due to resharding.
    pub(crate) main_transition_shard_id: ShardId,
}

impl Client {
    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
        validator_signer: &Option<Arc<ValidatorSigner>>,
    ) -> Result<(), Error> {
        let chunk_header = chunk.cloned_header();
        let shard_id = chunk_header.shard_id();
        let _span = tracing::debug_span!(target: "client", "send_chunk_state_witness", chunk_hash=?chunk_header.chunk_hash(), ?shard_id).entered();

        let my_signer = validator_signer
            .as_ref()
            .ok_or_else(|| Error::NotAValidator(format!("send state witness")))?;
        let CreateWitnessResult { state_witness, main_transition_shard_id, contract_updates } =
            self.create_state_witness(
                my_signer.validator_id().clone(),
                prev_block_header,
                prev_chunk_header,
                chunk,
            )?;

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&state_witness)?;
        }

        let height = chunk_header.height_created();
        if self
            .epoch_manager
            .get_chunk_validator_assignments(epoch_id, shard_id, height)?
            .contains(my_signer.validator_id())
        {
            // Bypass state witness validation if we created state witness. Endorse the chunk immediately.
            tracing::debug!(target: "client", chunk_hash=?chunk_header.chunk_hash(), ?shard_id, "send_chunk_endorsement_from_chunk_producer");
            if let Some(endorsement) = send_chunk_endorsement_to_block_producers(
                &chunk_header,
                self.epoch_manager.as_ref(),
                my_signer.as_ref(),
                &self.network_adapter.clone().into_sender(),
            ) {
                let mut tracker = self.chunk_endorsement_tracker.lock();
                tracker.process_chunk_endorsement(endorsement)?;
            }
        }

        self.partial_witness_adapter.send(DistributeStateWitnessRequest {
            state_witness,
            contract_updates,
            main_transition_shard_id,
        });
        Ok(())
    }

    pub(crate) fn create_state_witness(
        &self,
        chunk_producer: AccountId,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
    ) -> Result<CreateWitnessResult, Error> {
        let chunk_header = chunk.cloned_header();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let prev_chunk = self.chain.get_chunk(&prev_chunk_header.chunk_hash())?;
        let StateTransitionData {
            main_transition,
            main_transition_shard_id,
            implicit_transitions,
            applied_receipts_hash,
            contract_updates,
        } = self.collect_state_transition_data(&chunk_header, prev_chunk_header)?;

        let source_receipt_proofs =
            self.collect_source_receipt_proofs(prev_block_header, prev_chunk_header)?;

        let state_witness = ChunkStateWitness::new(
            chunk_producer,
            epoch_id,
            chunk_header,
            main_transition,
            source_receipt_proofs,
            // (Could also be derived from iterating through the receipts, but
            // that defeats the purpose of this check being a debugging
            // mechanism.)
            applied_receipts_hash,
            prev_chunk.to_transactions().to_vec(),
            implicit_transitions,
            protocol_version,
        );
        Ok(CreateWitnessResult { state_witness, contract_updates, main_transition_shard_id })
    }

    /// Collect state transition data necessary to produce state witness for
    /// `chunk_header`.
    /// Returns main state transition and implicit transitions, in the order
    /// they should be applied, and the hash of receipts to apply.
    fn collect_state_transition_data(
        &self,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: &ShardChunkHeader,
    ) -> Result<StateTransitionData, Error> {
        let prev_chunk_height_included = prev_chunk_header.height_included();

        // Iterate over blocks in chain from `chunk_header.prev_block_hash()`
        // (inclusive) until the block with height `prev_chunk_height_included`
        // (exclusive).
        // Every block corresponds to one implicit state transition between
        // `prev_chunk_header` and `chunk_header`.
        // There may be one additional implicit transition for a block, if
        // resharding happens after its processing.
        // TODO(logunov): consider uniting with `get_incoming_receipts_for_shard`
        // because it has the same purpose.
        let mut current_block_hash = *chunk_header.prev_block_hash();
        let mut next_epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&current_block_hash)?;
        let mut next_shard_id = chunk_header.shard_id();
        let mut implicit_transitions = vec![];

        loop {
            let header = self.chain.get_block_header(&current_block_hash)?;
            if header.height() < prev_chunk_height_included {
                return Err(Error::InvalidBlockHeight(prev_chunk_height_included));
            }

            let current_epoch_id = *header.epoch_id();
            let current_shard_id = self
                .epoch_manager
                .get_prev_shard_id_from_prev_hash(&current_block_hash, next_shard_id)?
                .1;
            if current_shard_id != next_shard_id {
                // If shard id changes, we need to get implicit state
                // transition from current shard id to the next shard id.
                let (chunk_state_transition, _, _) =
                    self.get_state_transition(&current_block_hash, &next_epoch_id, next_shard_id)?;
                implicit_transitions.push(chunk_state_transition);
            }
            next_epoch_id = current_epoch_id;
            next_shard_id = current_shard_id;

            if header.height() == prev_chunk_height_included {
                break;
            }

            // Add implicit state transition.
            let (chunk_state_transition, _, _) = self.get_state_transition(
                &current_block_hash,
                &current_epoch_id,
                current_shard_id,
            )?;
            implicit_transitions.push(chunk_state_transition);

            current_block_hash = *header.prev_hash();
        }

        let main_block = current_block_hash;
        let epoch_id = next_epoch_id;
        let main_transition_shard_id = next_shard_id;
        implicit_transitions.reverse();

        // Get the main state transition.
        let (main_transition, receipts_hash, contract_updates) = if prev_chunk_header.is_genesis() {
            self.get_genesis_state_transition(&main_block, &epoch_id, main_transition_shard_id)?
        } else {
            self.get_state_transition(&main_block, &epoch_id, main_transition_shard_id)?
        };

        Ok(StateTransitionData {
            main_transition,
            main_transition_shard_id,
            implicit_transitions,
            applied_receipts_hash: receipts_hash,
            contract_updates,
        })
    }

    /// Read state transition data from chain.
    fn get_state_transition(
        &self,
        block_hash: &CryptoHash,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<(ChunkStateTransition, CryptoHash, ContractUpdates), Error> {
        let shard_uid = shard_id_to_uid(self.chain.epoch_manager.as_ref(), shard_id, epoch_id)?;
        let stored_chunk_state_transition_data = self
            .chain
            .chain_store()
            .store()
            .get_ser(
                near_store::DBCol::StateTransitionData,
                &near_primitives::utils::get_block_shard_id(block_hash, shard_id),
            )?
            .ok_or_else(|| {
                let message = format!(
                    "Missing transition state proof for block {block_hash} and shard {shard_id}"
                );
                if !cfg!(feature = "shadow_chunk_validation") {
                    log_assert_fail!("{message}");
                }
                Error::Other(message)
            })?;
        let StoredChunkStateTransitionData::V1(StoredChunkStateTransitionDataV1 {
            base_state,
            receipts_hash,
            contract_accesses,
            contract_deploys,
        }) = stored_chunk_state_transition_data;
        let contract_updates = ContractUpdates {
            contract_accesses: contract_accesses.into_iter().collect(),
            contract_deploys: contract_deploys.into_iter().map(|c| c.into()).collect(),
        };
        Ok((
            ChunkStateTransition {
                block_hash: *block_hash,
                base_state,
                post_state_root: *self.chain.get_chunk_extra(block_hash, &shard_uid)?.state_root(),
            },
            receipts_hash,
            contract_updates,
        ))
    }

    fn get_genesis_state_transition(
        &self,
        block_hash: &CryptoHash,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<(ChunkStateTransition, CryptoHash, ContractUpdates), Error> {
        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), shard_id, &epoch_id)?;
        Ok((
            ChunkStateTransition {
                block_hash: *block_hash,
                base_state: Default::default(),
                post_state_root: *self.chain.get_chunk_extra(block_hash, &shard_uid)?.state_root(),
            },
            hash(&borsh::to_vec::<[Receipt]>(&[]).unwrap()),
            Default::default(),
        ))
    }

    /// State witness proves the execution of receipts proposed by `prev_chunk`.
    /// This function collects all incoming receipts for `prev_chunk`, along with the proofs
    /// that those receipts really originate from the right chunks.
    /// TODO(resharding): `get_incoming_receipts_for_shard` generates invalid proofs on resharding
    /// boundaries, because it removes the receipts that target the other half of a split shard,
    /// which makes the proof invalid. We need to collect the original proof and later, after verification,
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
        // They will be between `prev_prev_chunk.height_included` (first block containing `prev_prev_chunk`)
        // and `prev_chunk_original_block`
        let shard_layout = self
            .epoch_manager
            .get_shard_layout_from_prev_block(prev_chunk_original_block.prev_hash())?;
        let incoming_receipt_proofs = get_incoming_receipts_for_shard(
            &self.chain.chain_store(),
            self.epoch_manager.as_ref(),
            prev_chunk_header.shard_id(),
            &shard_layout,
            *prev_chunk_original_block.hash(),
            prev_prev_chunk_header.height_included(),
            ReceiptFilter::All,
        )?;

        // Convert to the right format (from [block_hash -> Vec<ReceiptProof>] to [chunk_hash -> ReceiptProof])
        let mut source_receipt_proofs = HashMap::new();
        for receipt_proof_response in incoming_receipt_proofs {
            let from_block = self.chain.chain_store().get_block(&receipt_proof_response.0)?;
            let shard_layout =
                self.epoch_manager.get_shard_layout(from_block.header().epoch_id())?;
            for proof in receipt_proof_response.1.iter() {
                let from_shard_id = proof.1.from_shard_id;
                let from_shard_index = shard_layout.get_shard_index(from_shard_id)?;
                let from_chunk_hash = from_block
                    .chunks()
                    .get(from_shard_index)
                    .ok_or(Error::InvalidShardId(proof.1.from_shard_id))?
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
