use super::partial_witness::partial_witness_actor::DistributeStateWitnessRequest;
use crate::Client;
use crate::stateless_validation::chunk_validator::send_chunk_endorsement_to_block_producers;
use near_async::messaging::{CanSend, IntoSender};
use near_chain::BlockHeader;
use near_chain::stateless_validation::state_witness::CreateWitnessResult;
use near_chain_primitives::Error;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::types::EpochId;
use near_primitives::version::ProtocolFeature;

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
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
        if ProtocolFeature::Spice.enabled(protocol_version) {
            return Ok(());
        }

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

        // Skip witness generation for chunks from previous blocks.
        // This can happen when optimistic block production starts before chunks from the
        // previous block finish applying (race condition with early OB production).
        // The witness would have already been sent at the correct height.
        let expected_height = prev_block_header.height() + 1;
        if height != expected_height {
            tracing::debug!(
                target: "client",
                chunk_height = height,
                block_height = expected_height,
                %shard_id,
                "Skipping optimistic witness for chunk from previous block"
            );
            return Ok(());
        }

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
}
