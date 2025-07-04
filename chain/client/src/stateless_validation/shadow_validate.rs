use crate::Client;
use near_chain::get_chunk_clone_from_header;
use near_chain::stateless_validation::state_witness::CreateWitnessResult;
use near_chain::{Block, BlockHeader};
use near_chain_primitives::Error;
use near_primitives::sharding::{ShardChunk, ShardChunkHeader};

impl Client {
    // Temporary feature to make node produce state witness for every chunk in every processed block
    // and then self-validate it.
    pub(crate) fn shadow_validate_block_chunks(&mut self, block: &Block) -> Result<(), Error> {
        if !cfg!(feature = "shadow_chunk_validation") {
            return Ok(());
        }
        let block_hash = block.hash();
        tracing::debug!(target: "client", ?block_hash, "shadow validation for block chunks");
        let prev_block = self.chain.get_block(block.header().prev_hash())?;
        let prev_block_chunks = prev_block.chunks();
        for (shard_index, chunk) in block.chunks().iter_new().enumerate() {
            let chunk = get_chunk_clone_from_header(&self.chain.chain_store, chunk)?;
            // TODO(resharding) This doesn't work if shard layout changes.
            let prev_chunk_header = prev_block_chunks.get(shard_index).unwrap();
            if let Err(err) =
                self.shadow_validate_chunk(prev_block.header(), prev_chunk_header, &chunk)
            {
                near_chain::stateless_validation::metrics::SHADOW_CHUNK_VALIDATION_FAILED_TOTAL
                    .inc();
                tracing::error!(
                    target: "client",
                    ?err,
                    shard_id = %chunk.shard_id(),
                    ?block_hash,
                    "shadow chunk validation failed"
                );
            }
        }
        Ok(())
    }

    fn shadow_validate_chunk(
        &mut self,
        prev_block_header: &BlockHeader,
        prev_chunk_header: &ShardChunkHeader,
        chunk: &ShardChunk,
    ) -> Result<(), Error> {
        let CreateWitnessResult { state_witness, .. } =
            self.chain.chain_store().create_state_witness(
                self.epoch_manager.as_ref(),
                // Setting arbitrary chunk producer is OK for shadow validation
                "alice.near".parse().unwrap(),
                prev_block_header,
                prev_chunk_header,
                chunk,
            )?;
        if self.config.save_latest_witnesses {
            self.chain.chain_store.save_latest_chunk_state_witness(&state_witness)?;
        }
        self.chain.shadow_validate_state_witness(
            state_witness,
            self.epoch_manager.as_ref(),
            None,
        )?;
        Ok(())
    }
}
