use crate::spice::chunk_executor_actor::get_receipt_proof;
use crate::spice::data_manager::ReceiptProofId;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::sharding::ReceiptProof;
use near_primitives::types::AccountId;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashSet;
use std::sync::Arc;

/// Receipt proofs on a producer: pushed to the destination shard's chunk producers and
/// served from the store.
pub(crate) struct ReceiptProofServePolicy {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
}

impl ReceiptProofServePolicy {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self { chain_store, epoch_manager }
    }

    /// The nodes `id` is pushed to.
    pub(in crate::spice::data_manager::policy) fn recipients(
        &self,
        id: &ReceiptProofId,
    ) -> Result<HashSet<AccountId>, Error> {
        super::recipients(self.epoch_manager.as_ref(), &id.source.block_hash, id.to_shard)
    }

    /// The receipt proof, if this node has it in the store.
    pub(in crate::spice::data_manager::policy) fn stored_data(
        &self,
        id: &ReceiptProofId,
    ) -> Option<ReceiptProof> {
        let ReceiptProofId { source, to_shard } = id;
        get_receipt_proof(
            self.chain_store.store_ref(),
            &source.block_hash,
            *to_shard,
            source.shard_id,
        )
    }
}
