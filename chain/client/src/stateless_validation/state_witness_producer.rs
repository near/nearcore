use super::partial_witness::partial_witness_actor::DistributeStateWitnessRequest;
use crate::Client;
use crate::stateless_validation::chunk_validator::send_chunk_endorsement_to_block_producers;
use near_async::messaging::{CanSend, IntoSender};
use near_chain::BlockHeader;
use near_chain::chain::NewChunkResult;
use near_chain::stateless_validation::state_witness::CreateWitnessResult;
use near_chain::types::ApplyChunkResult;
use near_chain_primitives::Error;
use near_primitives::block::ApplyChunkBlockContext;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};
use near_primitives::stateless_validation::state_witness::{
    ChunkApplyWitness, ChunkStateTransition, ChunkStateWitness, ChunkStateWitnessV3,
};
use near_primitives::types::EpochId;

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
        let apply_witness_sent =
            self.chunk_apply_witness_sent_cache.contains(&(prev_block_header.height(), shard_id));
        let CreateWitnessResult { state_witness, main_transition_shard_id, contract_updates } =
            self.chain.chain_store().create_state_witness(
                self.epoch_manager.as_ref(),
                my_signer.validator_id().clone(),
                prev_block_header,
                prev_chunk_header,
                chunk,
                apply_witness_sent,
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
        new_chunk: NewChunkResult,
        block_context: ApplyChunkBlockContext,
        chunks: Vec<ShardChunkHeader>,
    ) -> Result<(), Error> {
        let context = new_chunk.context;
        let ApplyChunkResult { proof, new_root, contract_updates, applied_receipts_hash, .. } =
            new_chunk.apply_result;
        let prev_block_hash = context.block.prev_block_hash;
        let prev_block_epoch_id = self.epoch_manager.get_epoch_id(&prev_block_hash)?;
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
        if prev_block_epoch_id != epoch_id {
            // Let's just skip it because I don't want to handle resharding yet.
            return Ok(());
        }
        let Some(chunk_header) = context.chunk_header else {
            return Err(Error::Other("Chunk header is missing".to_string()));
        };
        let shard_id = chunk_header.shard_id();

        // Record that we sent chunk apply witness for this (prev_block_hash, shard_id) pair
        self.chunk_apply_witness_sent_cache.put((block_context.height, shard_id), ());

        let main_state_transition = ChunkStateTransition {
            block_hash: Default::default(),
            base_state: proof.unwrap().nodes,
            post_state_root: new_root,
        };

        self.partial_witness_adapter.send(DistributeStateWitnessRequest {
            state_witness: ChunkStateWitness::V3(ChunkStateWitnessV3 {
                chunk_apply_witness: Some(ChunkApplyWitness {
                    epoch_id: prev_block_epoch_id,
                    chunk_header,
                    block_context,
                    chunks,
                    main_state_transition,
                    receipts: context.receipts,
                    applied_receipts_hash,
                    transactions: context.transactions,
                }),
                chunk_validate_witness: None,
            }),
            contract_updates,
            main_transition_shard_id: shard_id,
        });
        Ok(())
    }
}
