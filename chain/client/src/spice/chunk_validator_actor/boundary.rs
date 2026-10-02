//! Chunk validator handling of a boundary witness, the witness of the last
//! pre-spice block's chunk.

use super::{SpiceChunkValidatorActor, WitnessValidationContext};
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_chain::{Block, Error};
use near_primitives::types::BlockExecutionResults;
use std::collections::HashMap;
use std::sync::Arc;

impl SpiceChunkValidatorActor {
    pub(super) fn boundary_witness_validation_context(
        &self,
        block: Arc<Block>,
    ) -> Result<WitnessValidationContext, Error> {
        if !is_last_pre_spice_block(self.epoch_manager.as_ref(), block.hash())? {
            return Err(Error::InvalidChunkStateWitness(
                "witness for a pre-spice block other than the last one".to_string(),
            ));
        }
        let prev_block = self.chain_store.get_block(block.header().prev_hash())?;
        Ok(WitnessValidationContext {
            block,
            prev_block,
            prev_block_execution_results: BlockExecutionResults(HashMap::new()),
            prev_validator_proposals: Vec::new(),
        })
    }
}
