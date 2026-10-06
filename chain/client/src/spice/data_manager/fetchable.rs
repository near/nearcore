use super::DataId;
use crate::spice::chunk_executor_actor::receipt_proof_exists;
use near_chain::spice::boundary::shards_applied_itself;
use near_chain::{Block, Error};
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, ShardId};
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
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

    /// The ids of this type that `block` makes pullable, tracked or not.
    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error>;
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

    fn needed_shards(&self, block: &BlockHeader) -> Result<NeededShards, Error> {
        let shard_layout = self.epoch_manager.get_shard_layout(block.epoch_id())?;
        // Applying the source shard ourselves produces the proof locally; this is
        // also why a producer never fetches its own proof.
        let applied_itself =
            shards_applied_itself(&self.shard_tracker, self.epoch_manager.as_ref(), block)?;
        let sources = shard_layout
            .shard_ids()
            .filter(|shard_id| !applied_itself.contains(shard_id))
            .collect();
        // The proof feeds applying the destination shard in the next block. `block` is a
        // spice or last pre-spice block, so the next one is a spice block: no boundary case.
        let destinations = shard_layout
            .shard_ids()
            .filter(|shard_id| {
                self.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::IsCaughtUp,
                    block.hash(),
                    *shard_id,
                )
            })
            .collect();
        Ok(NeededShards { sources, destinations })
    }
}

/// The shards of a block whose receipt proofs this node needs.
struct NeededShards {
    /// Source shards this node doesn't apply itself.
    sources: Vec<ShardId>,
    /// Destination shards this node applies in the next block.
    destinations: Vec<ShardId>,
}

impl DataPolicy for ReceiptProofPolicy {
    fn needed_items(&self, block: &BlockHeader) -> Result<Vec<(DataId, Vec<AccountId>)>, Error> {
        let NeededShards { sources, destinations } = self.needed_shards(block)?;
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

    /// The needed proofs from every chunk `block` certifies.
    fn made_pullable_by(&self, block: &Block) -> Result<Vec<DataId>, Error> {
        let mut needed_by_source_block: HashMap<CryptoHash, NeededShards> = HashMap::new();
        let mut ids = Vec::new();
        for (source, _) in block.spice_core_statements().iter_execution_results() {
            let needed = match needed_by_source_block.entry(source.block_hash) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let header = self.chain_store.get_block_header(&source.block_hash)?;
                    entry.insert(self.needed_shards(&header)?)
                }
            };
            if !needed.sources.contains(&source.shard_id) {
                continue;
            }
            for to_shard in &needed.destinations {
                ids.push(DataId::ReceiptProof { source: source.clone(), to_shard: *to_shard });
            }
        }
        Ok(ids)
    }
}
