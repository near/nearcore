//! Core writer handling of the spice activation boundary.

use super::SpiceCoreWriterActor;
use crate::spice::boundary::is_last_pre_spice_block;
use crate::spice::boundary_synthesis::{
    PreSpiceExecutionResultCheck, check_pre_spice_execution_result,
};
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{ChunkExecutionResult, ShardId, SpiceChunkId};

impl SpiceCoreWriterActor {
    /// Checks a certified execution result of a pre-spice chunk against this node's local
    /// synthesis. A certified result carries 2/3 of the stake, so a mismatch means this
    /// node's own pre-spice state is wrong, and it panics rather than build on it.
    pub(super) fn check_boundary_execution_result(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        execution_result: &ChunkExecutionResult,
    ) {
        let chunk_id = SpiceChunkId { block_hash: *block_hash, shard_id };
        match check_pre_spice_execution_result(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &self.shard_tracker,
            &chunk_id,
            execution_result,
        ) {
            Ok(
                PreSpiceExecutionResultCheck::Consistent
                | PreSpiceExecutionResultCheck::NotCheckable,
            ) => {}
            Ok(PreSpiceExecutionResultCheck::Mismatch) => {
                panic!(
                    "certified execution result of pre-spice chunk {chunk_id:?} does not match local synthesis: \
                     this node's state of shard {shard_id} diverged from the network and cannot be repaired in place; \
                     restore the data directory from a snapshot, or wipe it and re-sync from the network; \
                     certified result: {execution_result:?}"
                );
            }
            Err(err) => {
                tracing::warn!(
                    target: "spice_core_writer",
                    ?err,
                    %block_hash,
                    %shard_id,
                    "failed to check certified execution result of pre-spice chunk; saving it unchecked",
                );
            }
        }
    }

    /// A last pre-spice block's chunks' endorsements can arrive before the block and
    /// wait as pending; records them now. A no-op for any other block.
    pub(super) fn handle_processed_last_pre_spice_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        if !is_last_pre_spice_block(self.epoch_manager.as_ref(), block_hash)? {
            return Ok(());
        }
        let block = self.chain_store.get_block(block_hash)?;
        let pending_endorsements = self.pop_pending_endorsement_for_block(&block)?;
        if pending_endorsements.is_empty() {
            return Ok(());
        }
        let store_update =
            self.record_chunk_endorsements_with_block(&block, pending_endorsements)?;
        store_update.commit();
        self.try_sending_execution_result_endorsed(block_hash)
    }
}
