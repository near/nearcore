use crate::spice::executor_shared::save_receipt_proof;
use near_chain::Error;
use near_chain::spice::core::SpiceCoreReader;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ReceiptProof;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;

/// Network-path receipt proofs addressed to one shard, buffered until the source
/// block's CER (chunk execution results) lands on chain, then verified against it
/// and written. Local-path proofs are never buffered here — the sibling shard that
/// produced them already wrote them to disk.
///
/// This isolates the only non-trivial cross-shard bookkeeping the per-shard executor
/// does: hold unverified proofs keyed by source block, and drain+verify a source
/// block's batch once its CER is available.
#[derive(Default)]
pub(crate) struct PendingReceipts {
    by_source_block: HashMap<CryptoHash, Vec<ReceiptProof>>,
}

impl PendingReceipts {
    /// Buffer one unverified proof against the block it was produced in.
    pub fn buffer(&mut self, source_block: CryptoHash, proof: ReceiptProof) {
        self.by_source_block.entry(source_block).or_default().push(proof);
    }

    /// Verify the buffered proofs from `source_block` against its CER and write the
    /// valid ones. No-op until the CER is on chain (the caller re-drives this on the
    /// next `ExecutionResultEndorsed`). Invalid proofs are dropped with a warning.
    pub fn try_drain(
        &mut self,
        chain_store: &ChainStoreAdapter,
        core_reader: &SpiceCoreReader,
        source_block: &CryptoHash,
    ) -> Result<(), Error> {
        let header = chain_store.get_block_header(source_block)?;
        let Some(cer) = core_reader.get_block_execution_results(&header)? else {
            return Ok(()); // CER not on chain yet — wait for ExecutionResultEndorsed.
        };
        let Some(proofs) = self.by_source_block.remove(source_block) else {
            return Ok(());
        };
        let store = chain_store.store();
        let mut store_update = store.store_update();
        for proof in proofs {
            let from_shard_id = proof.1.from_shard_id;
            let Some(result) = cer.0.get(&from_shard_id) else {
                continue;
            };
            if proof.verify_against_receipt_root(result.outgoing_receipts_root) {
                save_receipt_proof(&mut store_update, source_block, &proof);
            } else {
                tracing::warn!(target: "chunk_executor", %source_block, ?from_shard_id, "dropping invalid network receipt proof");
            }
        }
        store_update.commit();
        Ok(())
    }
}
