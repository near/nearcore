use crate::Client;
use crate::stateless_validation::chunk_validator::send_chunk_endorsement_to_block_producers;
use near_async::messaging::{CanSend, IntoSender};
// unused import removed
use near_chain::stateless_validation::state_witness::{
    CreateWitnessResult, DistributeStateWitnessRequest,
};
// unused import removed
use near_chain_primitives::Error;
// unused import removed
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::stateless_validation::WitnessType;
// unused import removed
use near_primitives::types::EpochId;
use std::sync::Arc;

use near_chain::Block;
use near_chain::update_shard::ShardUpdateResult;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::ShardId;
use near_primitives::types::chunk_extra::ChunkExtra;

impl Client {
    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        prev_block: &Block,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
        get_shard_result: impl Fn(&CryptoHash, ShardId) -> Option<ShardUpdateResult>,
        get_chunk_extra: impl Fn(&CryptoHash, ShardId) -> Option<ChunkExtra>,
        get_incoming_receipts: impl Fn(&CryptoHash, ShardId) -> Option<Arc<Vec<ReceiptProof>>>,
    ) -> Result<(), Error> {
        let chunk_header = chunk.cloned_header();
        let shard_id = chunk_header.shard_id();
        let height = chunk_header.height_created();
        let apply_witness_sent = {
            let lock = self.chain.chunk_apply_witness_sent_cache.lock();
            lock.contains(&(prev_chunk_header.height_created(), shard_id))
        };

        let witness_type =
            if apply_witness_sent { WitnessType::Validate } else { WitnessType::Full };

        let _span = tracing::debug_span!(
            target: "client",
            "send_chunk_state_witness",
            chunk_hash=?chunk_header.chunk_hash(),
            height,
            %shard_id,
            tag_block_production = true,
            tag_witness_distribution = true,
            witness_type = ?witness_type,
        )
        .entered();

        let validator_signer = self.validator_signer.get();
        let my_signer = validator_signer
            .as_ref()
            .ok_or_else(|| Error::NotAValidator(format!("send state witness")))?;
        let CreateWitnessResult { state_witness, main_transition_shard_id, contract_updates } =
            self.chain.chain_store().create_state_witness(
                self.epoch_manager.as_ref(),
                prev_block,
                prev_chunk_header,
                chunk,
                apply_witness_sent,
                get_shard_result,
                get_chunk_extra,
                get_incoming_receipts,
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
