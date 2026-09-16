//! Core writer handling of the spice activation boundary.

use super::SpiceCoreWriterActor;
use crate::spice::boundary::{check_pre_spice_execution_result, is_spice_activation_parent};
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{ChunkExecutionResult, ShardId, SpiceChunkId};

impl SpiceCoreWriterActor {
    /// Whether the boundary tripwire rejects saving the result: one bad pre-spice
    /// chunk must not cost the block its other core statements.
    pub(super) fn boundary_rejects_execution_result(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        execution_result: &ChunkExecutionResult,
    ) -> bool {
        let Err(err) = check_pre_spice_execution_result(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &SpiceChunkId { block_hash: *block_hash, shard_id },
            execution_result,
        ) else {
            return false;
        };
        tracing::error!(
            target: "spice_core_writer",
            ?err,
            %block_hash,
            %shard_id,
            "not saving execution result",
        );
        true
    }

    /// An activation parent's chunks' endorsements can arrive before the block and
    /// wait as pending; records them now. A no-op for any other block.
    pub(super) fn handle_processed_activation_parent(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        if !is_spice_activation_parent(self.epoch_manager.as_ref(), block_hash)? {
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
