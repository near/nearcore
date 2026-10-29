use crate::spice::chunk_executor_actor::receipt_proof_exists;
use crate::spice::data_manager::DataId;
use near_chain::spice::boundary::shards_applied_itself;
use near_chain::{Block, Error};
use near_chain_primitives::ApplyChunksMode;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;
use near_primitives::types::SpiceChunkId;
use near_store::Store;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// The shards of `block` whose receipt proofs this node needs.
fn needed_shards(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    block: &BlockHeader,
) -> Result<NeededShards, Error> {
    let shard_layout = epoch_manager.get_shard_layout(block.epoch_id())?;
    // Applying the source shard ourselves produces the proof locally; this is
    // also why a producer never fetches its own proof.
    let applied_itself = shards_applied_itself(shard_tracker, epoch_manager, block)?;
    let sources =
        shard_layout.shard_ids().filter(|shard_id| !applied_itself.contains(shard_id)).collect();
    // The proof feeds applying the destination shard in the next block. `block` is a
    // spice or last pre-spice block, so the next one is a spice block: no boundary case.
    let destinations = shard_layout
        .shard_ids()
        .filter(|shard_id| {
            shard_tracker.should_apply_chunk(ApplyChunksMode::IsCaughtUp, block.hash(), *shard_id)
        })
        .collect();
    Ok(NeededShards { sources, destinations })
}

/// The shards of a block whose receipt proofs this node needs.
struct NeededShards {
    /// Source shards this node doesn't apply itself.
    sources: Vec<ShardId>,
    /// Destination shards this node applies in the next block.
    destinations: Vec<ShardId>,
}

/// The proofs from the shards this node did not apply itself for `block`, into the shards it
/// applies in the next block.
pub(crate) fn needed_items(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    block: &BlockHeader,
) -> Result<Vec<DataId>, Error> {
    let NeededShards { sources, destinations } =
        needed_shards(epoch_manager, shard_tracker, block)?;
    // TODO(spice-resharding): Handle resharding
    let mut ids = Vec::with_capacity(sources.len() * destinations.len());
    for from_shard in sources {
        for to_shard in &destinations {
            ids.push(DataId::receipt_proof(*block.hash(), from_shard, *to_shard));
        }
    }
    Ok(ids)
}

/// Whether the proof from `source` into `to_shard` is persisted.
pub(crate) fn is_done(store: &Store, source: &SpiceChunkId, to_shard: ShardId) -> bool {
    receipt_proof_exists(store, &source.block_hash, to_shard, source.shard_id)
}

/// The needed proofs from every chunk `block` certifies.
pub(crate) fn made_pullable_by(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    block: &Block,
) -> Result<Vec<DataId>, Error> {
    let mut needed_by_source_block: HashMap<CryptoHash, NeededShards> = HashMap::new();
    let mut ids = Vec::new();
    for (source, _) in block.spice_core_statements().iter_execution_results() {
        let needed = match needed_by_source_block.entry(source.block_hash) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let header = chain_store.get_block_header(&source.block_hash)?;
                entry.insert(needed_shards(epoch_manager, shard_tracker, &header)?)
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
