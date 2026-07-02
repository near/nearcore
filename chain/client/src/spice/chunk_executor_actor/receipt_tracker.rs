//! Per-shard buffer of network-path receipt proofs awaiting verification.

use super::storage::save_receipt_proof;
use near_chain::Error;
use near_chain::spice::core::SpiceCoreReader;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::{ChunkExecutionResult, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::sync::Arc;

/// Network-path receipt proofs buffered against their source block until that
/// block's execution results land and the proofs can be verified. Keyed by
/// source block hash. Local-path proofs never go here — they are already on disk.
#[derive(Default)]
pub(crate) struct UnverifiedReceiptTracker {
    buffer: HashMap<CryptoHash, Vec<ReceiptProof>>,
}

impl UnverifiedReceiptTracker {
    /// Buffer a network-path receipt proof against its source block until that
    /// block's execution results land and the proof can be verified.
    pub(crate) fn buffer(&mut self, source_block: CryptoHash, receipt_proof: ReceiptProof) {
        self.buffer.entry(source_block).or_default().push(receipt_proof);
    }

    /// Number of source blocks with buffered receipts.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Verify and persist any receipts buffered against `source_block` once its
    /// execution results are available. Invalid proofs are dropped with a warning.
    pub(crate) fn try_drain(
        &mut self,
        chain_store: &ChainStoreAdapter,
        core_reader: &SpiceCoreReader,
        source_block: &CryptoHash,
    ) -> Result<(), Error> {
        let block = match chain_store.get_block(source_block) {
            Ok(block) => block,
            // Source block not on disk yet — nothing to drain. A later receipt or
            // chunk execution result endorsement re-drives once it lands.
            Err(Error::DBNotFoundErr(_)) => return Ok(()),
            Err(err) => return Err(err),
        };
        if !core_reader.all_execution_results_exist(block.header())? {
            return Ok(());
        }
        let execution_results = core_reader.get_execution_results_by_shard_id(block.header())?;
        let Some(receipt_proofs) = self.buffer.remove(source_block) else {
            return Ok(());
        };
        for receipt_proof in receipt_proofs {
            match verify_receipt_proof(&receipt_proof, &execution_results) {
                // Commit each proof in its own transaction: duplicate network
                // deliveries share a key, so batching them would overwrite within
                // one transaction. Separate commits make the writes idempotent.
                Ok(()) => {
                    let mut store_update = chain_store.store().store_update();
                    save_receipt_proof(&mut store_update, source_block, &receipt_proof);
                    store_update.commit();
                }
                // TODO(spice): Notify spice data distributor about invalid receipts so it can ban
                // or de-prioritize the node which sent them.
                Err(err) => {
                    tracing::warn!(target: "chunk_executor", ?err, %source_block, "encountered invalid receipts")
                }
            }
        }
        Ok(())
    }

    /// Drop receipts buffered against source blocks at or below the final
    /// execution head — they can never be applied.
    pub(crate) fn prune_below_final_head(
        &mut self,
        chain_store: &ChainStoreAdapter,
    ) -> Result<(), Error> {
        let final_head = chain_store.spice_final_execution_head()?;
        let mut stale = Vec::new();
        for source_block in self.buffer.keys().copied() {
            match chain_store.get_block_header(&source_block) {
                // At or below the final head: can never be applied again — drop it.
                Ok(header) if header.height() <= final_head.height => stale.push(source_block),
                Ok(_) => {}
                // Source block not on disk yet: a receipt can outrun its block, so the
                // block may still arrive and the receipt still apply — keep it. (A block
                // that never arrives leaks its buffered receipts, but that's rare —
                // abandoned forks only — and we have no height to prune it safely.)
                Err(Error::DBNotFoundErr(_)) => {}
                Err(err) => return Err(err),
            }
        }
        for source_block in stale {
            self.buffer.remove(&source_block);
        }
        Ok(())
    }
}

/// Verify a network-path receipt proof against the source block's execution
/// results. `execution_results` must hold results for the proof's source shard.
fn verify_receipt_proof(
    receipt_proof: &ReceiptProof,
    execution_results: &HashMap<ShardId, Arc<ChunkExecutionResult>>,
) -> Result<(), Error> {
    let Some(execution_result) = execution_results.get(&receipt_proof.1.from_shard_id) else {
        debug_assert!(false, "execution results missing results when verifying receipts");
        tracing::error!(
            target: "chunk_executor",
            from_shard_id=?receipt_proof.1.from_shard_id,
            "execution results missing results when verifying receipts"
        );
        return Err(Error::InvalidShardId(receipt_proof.1.from_shard_id));
    };
    if !receipt_proof.verify_against_receipt_root(execution_result.outgoing_receipts_root) {
        return Err(Error::InvalidReceiptsProof);
    }
    Ok(())
}
