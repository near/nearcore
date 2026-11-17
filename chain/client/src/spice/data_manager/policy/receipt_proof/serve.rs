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

    /// The next block's chunk producers of the destination shard; across a resharding, of the
    /// shards it splits into.
    pub(in crate::spice::data_manager::policy) fn recipients(
        &self,
        id: &ReceiptProofId,
    ) -> Result<HashSet<AccountId>, Error> {
        let ReceiptProofId { source, to_shard } = id;
        let source_epoch_id = self.epoch_manager.get_epoch_id(&source.block_hash)?;
        let next_block_epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(&source.block_hash)?;
        let source_layout = self.epoch_manager.get_shard_layout(&source_epoch_id)?;
        let next_block_layout = self.epoch_manager.get_shard_layout(&next_block_epoch_id)?;
        let destination_shards = if next_block_layout == source_layout {
            vec![*to_shard]
        } else {
            next_block_layout
                .get_children_shards_ids(*to_shard)
                .ok_or(Error::InvalidShardId(*to_shard))?
        };
        let mut recipients = HashSet::new();
        for shard_id in destination_shards {
            recipients.extend(
                self.epoch_manager
                    .get_epoch_chunk_producers_for_shard(&next_block_epoch_id, shard_id)?,
            );
        }
        Ok(recipients)
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
