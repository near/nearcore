use super::DataId;
use crate::spice::chunk_executor_actor::receipt_proof_exists;
use near_chain::Error;
use near_chain::spice::boundary::shards_applied_itself;
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::types::{AccountId, ShardId, SpiceChunkId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::sync::Arc;

/// Policy to query a fetchable data type's chain-dependent properties: relevance,
/// doneness, who holds the data, etc.
pub(crate) trait DataPolicy {
    /// The ids of this type that this node needs from `block`, each with its producers in
    /// the parts' encoding order.
    // TODO(spice-data-distribution): each id comes with a tri-state `Interest` once
    // witnesses move onto the engine (#16275).
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error>;

    /// Whether the durable artifact this item exists to obtain is already in the store.
    fn is_done(&self, id: &DataId) -> bool;

    /// The chunks that must all be certified before the item is pulled.
    fn chunks_to_certify_before_pull(&self, id: &DataId) -> Vec<SpiceChunkId>;
}

/// Receipt proofs: produced by the source chunk's producers, needed by nodes that apply
/// the destination shard in the next block; done once the proof is persisted.
pub(crate) struct ReceiptProofPolicy {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
}

impl ReceiptProofPolicy {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
    ) -> Self {
        Self { chain_store, epoch_manager, shard_tracker }
    }
}

impl DataPolicy for ReceiptProofPolicy {
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error> {
        let shard_layout = self.epoch_manager.get_shard_layout(block.epoch_id())?;
        // Applying the source shard ourselves produces the proof locally; this is
        // also why a producer never fetches its own proof.
        let applied_itself =
            shards_applied_itself(&self.shard_tracker, self.epoch_manager.as_ref(), block)?;
        let sources: Vec<ShardId> = shard_layout
            .shard_ids()
            .filter(|shard_id| !applied_itself.contains(shard_id))
            .collect();
        // The proof feeds applying the destination shard in the next block.
        let destinations: Vec<ShardId> = shard_layout
            .shard_ids()
            .filter(|shard_id| {
                self.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::IsCaughtUp,
                    block.hash(),
                    *shard_id,
                )
            })
            .collect();
        // TODO(spice-resharding): Handle resharding
        let mut items = Vec::with_capacity(sources.len() * destinations.len());
        for from_shard in sources {
            let producers = self
                .epoch_manager
                .get_epoch_chunk_producers_for_shard(block.epoch_id(), from_shard)?;
            for to_shard in &destinations {
                let id = DataId::receipt_proof(*block.hash(), from_shard, *to_shard);
                items.push((id, producers.clone()));
            }
        }
        Ok(items)
    }

    fn is_done(&self, id: &DataId) -> bool {
        let DataId::ReceiptProof { source, to_shard } = id;
        receipt_proof_exists(
            &self.chain_store.store(),
            &source.block_hash,
            *to_shard,
            source.shard_id,
        )
    }

    /// The source chunk.
    fn chunks_to_certify_before_pull(&self, id: &DataId) -> Vec<SpiceChunkId> {
        let DataId::ReceiptProof { source, .. } = id;
        vec![source.clone()]
    }
}
