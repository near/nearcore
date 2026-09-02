use super::item::DataId;
use crate::spice::chunk_executor_actor::receipt_proof_exists;
use near_chain::Error;
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;

/// Policy to query a fetchable data type's chain-dependent properties: relevance and
/// doneness. Implementations carry the engine's only chain dependencies.
/// Everything else about fetching is generic.
pub(crate) trait DataPolicy {
    /// Whether this node needs the item. Consulted once, when the item is first tracked.
    /// `block` is the id's block header.
    // TODO(spice-data-distribution): becomes a tri-state `Interest` (with a name to match)
    // when the first pull starter lands (#16275).
    fn should_fetch(&self, id: &DataId, block: &BlockHeader) -> Result<bool, Error>;

    /// Whether the durable artifact this item exists to obtain is already in the store.
    fn is_done(&self, id: &DataId) -> Result<bool, Error>;
}

/// Receipt proofs: produced by the source chunk's producers, needed by nodes that apply
/// the destination shard in the next block; done once the proof is persisted.
pub(crate) struct ReceiptProofPolicy {
    chain_store: ChainStoreAdapter,
    shard_tracker: ShardTracker,
}

impl ReceiptProofPolicy {
    pub(crate) fn new(chain_store: ChainStoreAdapter, shard_tracker: ShardTracker) -> Self {
        Self { chain_store, shard_tracker }
    }
}

impl DataPolicy for ReceiptProofPolicy {
    fn should_fetch(&self, id: &DataId, block: &BlockHeader) -> Result<bool, Error> {
        let DataId::ReceiptProof { source, to_shard } = id;
        debug_assert_eq!(&source.block_hash, block.hash());
        // Applying the source shard ourselves produces the proof locally; this is
        // also why a producer never fetches its own proof.
        if self.shard_tracker.should_apply_chunk(
            ApplyChunksMode::IsCaughtUp,
            block.prev_hash(),
            source.shard_id,
        ) {
            return Ok(false);
        }
        // The proof feeds applying `to_shard` in the next block.
        Ok(self.shard_tracker.should_apply_chunk(
            ApplyChunksMode::IsCaughtUp,
            block.hash(),
            *to_shard,
        ))
    }

    fn is_done(&self, id: &DataId) -> Result<bool, Error> {
        let DataId::ReceiptProof { source, to_shard } = id;
        Ok(receipt_proof_exists(
            &self.chain_store.store(),
            &source.block_hash,
            *to_shard,
            source.shard_id,
        ))
    }
}
