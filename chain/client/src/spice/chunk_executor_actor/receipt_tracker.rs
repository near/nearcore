//! Per-shard buffer of network-path receipt proofs awaiting verification.

use super::storage::save_receipt_proof;
use crate::spice::data_distributor_actor::{DataVerification, VerificationFailure};
use crate::spice::data_manager::DataId;
use near_chain::Error;
use near_chain::spice::core::SpiceCoreReader;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::{ChunkExecutionResult, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::sync::Arc;

/// Buffer of receipt proofs mapped by their source blocks.
#[derive(Default)]
pub(crate) struct UnverifiedReceiptTracker {
    /// Each proof is stored with the id and commitment it was delivered under, so
    /// its verification result can be related to those.
    proofs_by_source_block: HashMap<CryptoHash, Vec<(DataId, ReceiptProof, SpiceDataCommitment)>>,
}

impl UnverifiedReceiptTracker {
    pub(crate) fn insert(
        &mut self,
        data_id: DataId,
        receipt_proof: ReceiptProof,
        commitment: SpiceDataCommitment,
    ) {
        let DataId::ReceiptProof { source, .. } = &data_id;
        self.proofs_by_source_block.entry(source.block_hash).or_default().push((
            data_id,
            receipt_proof,
            commitment,
        ));
    }

    /// Number of source blocks with buffered receipts.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.proofs_by_source_block.len()
    }

    /// Verify and persist any receipts buffered against `source_block` once its
    /// execution results are available. Returns each proof's verification result;
    /// invalid proofs are dropped.
    pub(crate) fn try_drain(
        &mut self,
        chain_store: &ChainStoreAdapter,
        core_reader: &SpiceCoreReader,
        source_block: &CryptoHash,
    ) -> Result<Vec<DataVerification>, Error> {
        let block = match chain_store.get_block(source_block) {
            Ok(block) => block,
            // Source block not on disk yet — nothing to drain. A later receipt or
            // chunk execution result endorsement re-drives once it lands.
            Err(Error::DBNotFoundErr(_)) => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        if !core_reader.all_execution_results_exist(block.header())? {
            return Ok(Vec::new());
        }
        let execution_results = core_reader.get_execution_results_by_shard_id(block.header())?;
        let Some(receipt_proofs) = self.proofs_by_source_block.remove(source_block) else {
            return Ok(Vec::new());
        };
        let mut verifications = Vec::new();
        for (data_id, receipt_proof, commitment) in receipt_proofs {
            let verification_result = match verify_receipt_proof(&receipt_proof, &execution_results)
            {
                // Commit each proof in its own transaction: duplicate network
                // deliveries share a key, so batching them would overwrite within
                // one transaction. Separate commits make the writes idempotent.
                Ok(()) => {
                    let mut store_update = chain_store.store().store_update();
                    save_receipt_proof(&mut store_update, source_block, &receipt_proof);
                    store_update.commit();
                    Ok(())
                }
                Err(err) => {
                    tracing::debug!(target: "chunk_executor", ?err, %source_block, "encountered invalid receipts");
                    Err(VerificationFailure)
                }
            };
            verifications.push(DataVerification { data_id, commitment, verification_result });
        }
        Ok(verifications)
    }

    /// Drop receipts buffered against source blocks at or below the final
    /// execution head — they can never be applied.
    pub(crate) fn prune_below_final_head(
        &mut self,
        chain_store: &ChainStoreAdapter,
    ) -> Result<(), Error> {
        let final_head = chain_store.spice_final_execution_head()?;
        let mut stale = Vec::new();
        for source_block in self.proofs_by_source_block.keys().copied() {
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
            self.proofs_by_source_block.remove(&source_block);
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
