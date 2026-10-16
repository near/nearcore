use super::source_producers;
use crate::spice::chunk_executor_actor::get_receipt_proof;
use crate::spice::data_manager::policy::ServePolicy;
use crate::spice::data_manager::{DataId, SpiceData};
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
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
}

impl ServePolicy for ReceiptProofServePolicy {
    fn producers(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        source_producers(&self.chain_store, self.epoch_manager.as_ref(), id)
    }

    /// The destination shard's chunk producers for the next block.
    // TODO(spice-resharding): validate whether the source and destination shards are correct
    // when resharding.
    fn recipients(&self, id: &DataId) -> Result<HashSet<AccountId>, Error> {
        let DataId::ReceiptProof { source, to_shard } = id;
        let next_block_epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&source.block_hash)?;
        Ok(self
            .epoch_manager
            .get_epoch_chunk_producers_for_shard(&next_block_epoch_id, *to_shard)?
            .into_iter()
            .collect())
    }

    fn stored_data(&self, id: &DataId) -> Option<SpiceData> {
        let DataId::ReceiptProof { source, to_shard } = id;
        get_receipt_proof(
            self.chain_store.store_ref(),
            &source.block_hash,
            *to_shard,
            source.shard_id,
        )
        .map(SpiceData::ReceiptProof)
    }
}
