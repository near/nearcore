//! Coordinator side of the spice activation boundary bootstrap. Removable once no
//! node needs to cross the boundary.

use super::ChunkExecutorActor;
use near_chain::Error;
use near_chain::spice::boundary::{is_last_pre_spice_block, shards_applied_itself};
use near_primitives::hash::CryptoHash;

impl ChunkExecutorActor {
    /// Runs the boundary bootstrap of `block_hash` when it is a last pre-spice block; a no-op otherwise.
    pub(super) fn bootstrap_last_pre_spice_block(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        if !is_last_pre_spice_block(self.epoch_manager.as_ref(), block_hash)? {
            return Ok(());
        }
        let block = self.chain_store.get_block(block_hash)?;
        let prev_hash = block.header().prev_hash();
        self.reconcile_tracked_shards(prev_hash)?;
        let applied_itself = shards_applied_itself(
            &self.shard_tracker,
            self.epoch_manager.as_ref(),
            block.header(),
        )?;
        let mut outgoing_proofs = Vec::new();
        for executor in self.per_shard_executors.values() {
            if !applied_itself.contains(&executor.shard_uid().shard_id()) {
                continue;
            }
            match executor.endorse_and_send_receipts_and_witness_for_last_pre_spice_block(&block) {
                Ok(proofs) => outgoing_proofs.extend(proofs),
                Err(err) => {
                    tracing::error!(target: "chunk_executor", ?err, %block_hash, shard_uid = ?executor.shard_uid(), "failed boundary bootstrap for shard");
                }
            }
        }
        // The proofs are on disk now; a first spice block parked on them, as after a
        // restart, is re-driven like after a regular apply.
        self.try_progress_receiving_shards(&outgoing_proofs);
        Ok(())
    }
}
