use super::partial_witness::partial_witness_actor::DistributeStateWitnessRequest;
use crate::Client;
use crate::stateless_validation::chunk_validator::send_chunk_endorsement_to_block_producers;
use near_async::messaging::{CanSend, IntoSender};
use near_chain::BlockHeader;
use near_chain::chain::NewChunkResult;
use near_chain::stateless_validation::state_witness::CreateWitnessResult;
use near_chain_primitives::Error;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::ContractUpdates;
use near_primitives::stateless_validation::state_witness::{
    ChunkApplyWitness, ChunkStateTransition, ChunkStateWitness, ChunkStateWitnessV3,
};
use near_primitives::types::EpochId;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::{TrieDBStorage, TrieStorage};

impl Client {
    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
    ) -> Result<(), Error> {
        let chunk_header = chunk.cloned_header();
        let shard_id = chunk_header.shard_id();
        let height = chunk_header.height_created();
        let _span = tracing::debug_span!(
            target: "client",
            "send_chunk_state_witness",
            chunk_hash=?chunk_header.chunk_hash(),
            height,
            %shard_id,
            tag_block_production = true,
            tag_witness_distribution = true,
        )
        .entered();

        let validator_signer = self.validator_signer.get();
        let my_signer = validator_signer
            .as_ref()
            .ok_or_else(|| Error::NotAValidator(format!("send state witness")))?;
        let CreateWitnessResult { state_witness, main_transition_shard_id, contract_updates } =
            self.chain.chain_store().create_state_witness(
                self.epoch_manager.as_ref(),
                prev_block_header,
                prev_chunk_header,
                chunk,
            )?;

        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&state_witness)?;
        }

        if self
            .epoch_manager
            .get_chunk_validator_assignments(epoch_id, shard_id, height)?
            .contains(my_signer.validator_id())
        {
            // Bypass state witness validation if we created state witness. Endorse the chunk immediately.
            tracing::debug!(target: "client", chunk_hash=?chunk_header.chunk_hash(), %shard_id, "send_chunk_endorsement_from_chunk_producer");
            if let Some(endorsement) = send_chunk_endorsement_to_block_producers(
                &chunk_header,
                self.epoch_manager.as_ref(),
                my_signer.as_ref(),
                &self.network_adapter.clone().into_sender(),
            ) {
                self.chunk_endorsement_tracker.process_chunk_endorsement(endorsement)?;
            }
        }

        self.partial_witness_adapter.send(DistributeStateWitnessRequest {
            state_witness,
            contract_updates,
            main_transition_shard_id,
        });
        Ok(())
    }

    pub fn send_chunk_apply_witness_to_chunk_validators(
        &mut self,
        epoch_id: EpochId,
        new_chunk: NewChunkResult,
    ) -> Result<(), Error> {
        let context = new_chunk.context;
        let apply_result = &new_chunk.apply_result;

        let shard_id = context.chunk_header.as_ref().unwrap().shard_id();

        // let applied_receipts_hash = ;
        let main_state_transition = {
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
                block_hash: Default::default(),
                base_state: PartialState::TrieValues(base_state_values),
                post_state_root: apply_result.new_root,
            }
        };

        // todo(slavas): handle implicit transitions
        let implicit_transitions = Vec::new();

        self.partial_witness_adapter.send(DistributeStateWitnessRequest {
            state_witness: ChunkStateWitness::V3(ChunkStateWitnessV3 {
                chunk_apply_witness: ChunkApplyWitness {
                    epoch_id,
                    chunk_header: context.chunk_header.unwrap(),
                    main_state_transition,
                    receipts: context.receipts,
                    applied_receipts_hash: new_chunk.apply_result.applied_receipts_hash,
                    transactions: context.transactions,
                    implicit_transitions,
                },
                chunk_validate_witness: None,
            }),
            contract_updates: new_chunk.apply_result.contract_updates,
            main_transition_shard_id: shard_id,
        });

        Ok(())
    }
}
