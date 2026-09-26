//! Chunk validator handling of a boundary witness, the witness of the last
//! pre-spice block's chunk.

use super::{SpiceChunkValidatorActor, WitnessValidationContext};
use near_chain::spice::boundary::{
    PreSpiceChunkApplyBlocks, execution_result_from_pre_spice_child,
    get_last_new_chunk_block_and_old_chunk_blocks,
};
use near_chain::{Block, Error};
use near_primitives::types::{BlockExecutionResults, ShardId};
use std::collections::HashMap;
use std::sync::Arc;

impl SpiceChunkValidatorActor {
    pub(super) fn boundary_witness_validation_context(
        &self,
        block: Arc<Block>,
        shard_id: ShardId,
    ) -> Result<WitnessValidationContext, Error> {
        let prev_block = self.chain_store.get_block(block.header().prev_hash())?;
        let PreSpiceChunkApplyBlocks { last_new_chunk_block, old_chunk_blocks: _ } =
            get_last_new_chunk_block_and_old_chunk_blocks(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                block.as_ref(),
                shard_id,
            )?;
        let (_, prev_shard_id, _) = self.epoch_manager.get_prev_shard_id_from_prev_hash(
            last_new_chunk_block.header().prev_hash(),
            shard_id,
        )?;
        let prev_result = execution_result_from_pre_spice_child(
            self.epoch_manager.as_ref(),
            &last_new_chunk_block,
            shard_id,
        )?
        .ok_or_else(|| {
            Error::Other(format!(
                "anchor block {} includes no chunk of shard {}",
                last_new_chunk_block.hash(),
                shard_id
            ))
        })?;
        let prev_validator_proposals = prev_result.chunk_extra.validator_proposals().collect();
        let prev_block_execution_results =
            BlockExecutionResults(HashMap::from([(prev_shard_id, Arc::new(prev_result))]));
        Ok(WitnessValidationContext {
            block,
            prev_block,
            prev_block_execution_results,
            prev_validator_proposals,
        })
    }
}
