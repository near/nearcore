use super::DataId;
use crate::spice::chunk_executor_actor::receipt_proof_exists;
use near_chain::Error;
use near_chain::spice::core::SpiceCoreReader;
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::sync::Arc;

/// Policy to query a fetchable data type's chain-dependent properties: relevance,
/// doneness, who holds the data, etc. Implementations carry the engine's only chain
/// dependencies. Everything else about fetching is generic.
pub(crate) trait DataPolicy {
    /// The ids of this type that this node needs from `block`.
    // TODO(spice-data-distribution): each id comes with a tri-state `Interest` once
    // witnesses move onto the engine (#16275).
    fn needed_ids(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error>;

    /// Whether the durable artifact this item exists to obtain is already in the store.
    fn is_done(&self, id: &DataId) -> Result<bool, Error>;

    /// The nodes that hold the item's full data and can serve a pull. Their count is
    /// the number of parts the data is coded into.
    fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, Error>;

    /// Per shard, the height of the highest block whose chunk is certified as of `block`.
    fn certified_frontier(&self, block: &BlockHeader) -> Result<CertifiedFrontier, Error>;

    /// Whether the item sourced from a block at `height` exists at its sources and can be
    /// verified as of `frontier`.
    fn is_pull_open(&self, id: &DataId, height: BlockHeight, frontier: &CertifiedFrontier) -> bool;
}

/// Per shard, the height of the highest block whose chunk of that shard is certified.
pub(crate) type CertifiedFrontier = HashMap<ShardId, BlockHeight>;

/// Receipt proofs: produced by the source chunk's producers, needed by nodes that apply
/// the destination shard in the next block; done once the proof is persisted.
pub(crate) struct ReceiptProofPolicy {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    core_reader: SpiceCoreReader,
}

impl ReceiptProofPolicy {
    pub(crate) fn new(
        chain_store: ChainStoreAdapter,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        core_reader: SpiceCoreReader,
    ) -> Self {
        Self { chain_store, epoch_manager, shard_tracker, core_reader }
    }
}

impl DataPolicy for ReceiptProofPolicy {
    fn needed_ids(&self, block: &BlockHeader) -> Result<Vec<DataId>, Error> {
        let shard_layout = self.epoch_manager.get_shard_layout(block.epoch_id())?;
        let applies = |prev_hash, shard_id| {
            self.shard_tracker.should_apply_chunk(ApplyChunksMode::IsCaughtUp, prev_hash, shard_id)
        };
        // Applying the source shard ourselves produces the proof locally; this is
        // also why a producer never fetches its own proof.
        let sources: Vec<ShardId> = shard_layout
            .shard_ids()
            .filter(|shard_id| !applies(block.prev_hash(), *shard_id))
            .collect();
        // The proof feeds applying the destination shard in the next block.
        let destinations: Vec<ShardId> =
            shard_layout.shard_ids().filter(|shard_id| applies(block.hash(), *shard_id)).collect();
        // TODO(spice-resharding): Handle resharding
        Ok(sources
            .iter()
            .flat_map(|from_shard| {
                destinations
                    .iter()
                    .map(|to_shard| DataId::receipt_proof(*block.hash(), *from_shard, *to_shard))
            })
            .collect())
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

    fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, Error> {
        let DataId::ReceiptProof { source, .. } = id;
        let block_header = self.chain_store.get_block_header(&source.block_hash)?;
        Ok(self
            .epoch_manager
            .get_epoch_chunk_producers_for_shard(block_header.epoch_id(), source.shard_id)?)
    }

    fn certified_frontier(&self, block: &BlockHeader) -> Result<CertifiedFrontier, Error> {
        self.core_reader.highest_certified_heights(block)
    }

    fn is_pull_open(&self, id: &DataId, height: BlockHeight, frontier: &CertifiedFrontier) -> bool {
        receipt_proof_is_pull_open(id, height, frontier)
    }
}

/// A receipt proof is open once its source chunk is certified.
pub(crate) fn receipt_proof_is_pull_open(
    id: &DataId,
    height: BlockHeight,
    frontier: &CertifiedFrontier,
) -> bool {
    let DataId::ReceiptProof { source, .. } = id;
    frontier.get(&source.shard_id).is_some_and(|certified| *certified >= height)
}
