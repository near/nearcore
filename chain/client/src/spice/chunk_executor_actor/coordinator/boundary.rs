//! Coordinator side of the spice activation boundary bootstrap. Removable once no
//! node needs to cross the boundary.

use super::ChunkExecutorActor;
use near_chain::Error;
use near_chain::spice::boundary::is_last_pre_spice_block;
use near_primitives::hash::CryptoHash;

impl ChunkExecutorActor {
    /// Runs the boundary bootstrap of `block_hash` on every tracked shard's executor
    /// when it is a last pre-spice block; a no-op otherwise.
    pub(super) fn bootstrap_last_pre_spice_block(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<(), Error> {
        if !is_last_pre_spice_block(self.epoch_manager.as_ref(), block_hash)? {
            return Ok(());
        }
        let block = self.chain_store.get_block(block_hash)?;
        self.reconcile_tracked_shards(block_hash)?;
        for executor in self.per_shard_executors.values() {
            if let Err(err) =
                executor.endorse_and_send_receipts_and_witness_for_last_pre_spice_block(&block)
            {
                tracing::error!(target: "chunk_executor", ?err, %block_hash, shard_uid = ?executor.shard_uid(), "failed boundary bootstrap for shard");
            }
        }
        Ok(())
    }

    /// Recover after a crash around a last pre-spice block: the boundary bootstrap's
    /// endorsement and receipt sends are not persisted, so re-run it.
    /// A no-op when neither is a last pre-spice block.
    pub(super) fn recover_boundary_bootstrap(&mut self) -> Result<(), Error> {
        let mut candidates = Vec::new();
        match self.chain_store.head() {
            Ok(head) => candidates.push(head.last_block_hash),
            Err(Error::DBNotFoundErr(_)) => {}
            Err(err) => return Err(err),
        }
        match self.chain_store.spice_execution_head() {
            Ok(execution_head) => candidates.push(execution_head.last_block_hash),
            Err(Error::DBNotFoundErr(_)) => {}
            Err(err) => return Err(err),
        }
        candidates.dedup();
        for block_hash in candidates {
            self.bootstrap_last_pre_spice_block(&block_hash)?;
        }
        Ok(())
    }
}
