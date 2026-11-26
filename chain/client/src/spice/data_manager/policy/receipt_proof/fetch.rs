use crate::spice::chunk_executor_actor::receipt_proof_exists;
use crate::spice::data_manager::{DataId, ReceiptProofId};
use near_chain::spice::chunk_application::shards_applied_itself;
use near_chain::{Block, Error};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block_header::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, ShardId, SpiceChunkId};
use near_store::Store;
use near_store::adapter::chain_store::ChainStoreAdapter;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// The shards of `block` whose receipt proofs this node `me` needs pushed.
fn needed_shards(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    me: Option<&AccountId>,
    block: &BlockHeader,
) -> Result<NeededShards, Error> {
    let Some(me) = me else {
        return Ok(NeededShards { sources: Vec::new(), destinations: Vec::new() });
    };
    let shard_layout = epoch_manager.get_shard_layout(block.epoch_id())?;
    // Applying the source shard ourselves produces the proof locally
    let applied_itself = shards_applied_itself(shard_tracker, epoch_manager, block)?;
    let sources =
        shard_layout.shard_ids().filter(|shard_id| !applied_itself.contains(shard_id)).collect();
    let mut destinations = Vec::new();
    for to_shard in shard_layout.shard_ids() {
        if super::recipients(epoch_manager, block.hash(), to_shard)?.contains(me) {
            destinations.push(to_shard);
        }
    }
    Ok(NeededShards { sources, destinations })
}

/// The shards of a block whose receipt proofs this node needs.
struct NeededShards {
    /// Source shards this node doesn't apply itself.
    sources: Vec<ShardId>,
    /// Destination shards whose proofs are pushed to this node.
    destinations: Vec<ShardId>,
}

/// The proofs from the shards this node did not apply itself for `block` that are pushed to `me`.
pub(crate) fn needed_items(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    me: Option<&AccountId>,
    block: &BlockHeader,
) -> Result<Vec<DataId>, Error> {
    let NeededShards { sources, destinations } =
        needed_shards(epoch_manager, shard_tracker, me, block)?;
    // TODO(spice-resharding): Handle resharding
    let mut ids = Vec::with_capacity(sources.len() * destinations.len());
    for from_shard in sources {
        for to_shard in &destinations {
            ids.push(DataId::receipt_proof(*block.hash(), from_shard, *to_shard));
        }
    }
    Ok(ids)
}

/// Whether the proofs `id` names are pushed to `me`; never to a node without an account.
pub(crate) fn is_pushed_to(
    epoch_manager: &dyn EpochManagerAdapter,
    me: Option<&AccountId>,
    id: &ReceiptProofId,
) -> Result<bool, Error> {
    let Some(me) = me else {
        return Ok(false);
    };
    Ok(super::recipients(epoch_manager, &id.source.block_hash, id.to_shard)?.contains(me))
}

/// The lowest height whose receipt proofs pushed to this node may still be needed: the final
/// execution head's, whose proofs the next block applies. The genesis height before the first head
/// is recorded.
pub(crate) fn lowest_needed_height(chain_store: &ChainStoreAdapter) -> Result<BlockHeight, Error> {
    match chain_store.spice_final_execution_head() {
        Ok(head) => Ok(head.height),
        Err(Error::DBNotFoundErr(_)) => Ok(chain_store.get_genesis_height()),
        Err(err) => Err(err),
    }
}

/// Whether the proof from `source` into `to_shard` is persisted.
pub(crate) fn is_done(store: &Store, source: &SpiceChunkId, to_shard: ShardId) -> bool {
    receipt_proof_exists(store, &source.block_hash, to_shard, source.shard_id)
}

/// The proofs into every shard from every chunk `block` certifies, whether or not they are pushed
/// to this node: the item's map, chosen when it was tracked, decides.
pub(crate) fn made_pullable_by(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block: &Block,
) -> Result<Vec<DataId>, Error> {
    let mut shards_by_source_block: HashMap<CryptoHash, Vec<ShardId>> = HashMap::new();
    let mut ids = Vec::new();
    for (source, _) in block.spice_core_statements().iter_execution_results() {
        let to_shards = match shards_by_source_block.entry(source.block_hash) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                // A block whose header is not stored precedes this node's sync point, so none of
                // its data is tracked.
                let Some(header) = chain_store.get_block_header_if_exists(&source.block_hash)
                else {
                    continue;
                };
                let shard_layout = epoch_manager.get_shard_layout(header.epoch_id())?;
                entry.insert(shard_layout.shard_ids().collect())
            }
        };
        for to_shard in to_shards.iter() {
            ids.push(DataId::ReceiptProof(ReceiptProofId {
                source: source.clone(),
                to_shard: *to_shard,
            }));
        }
    }
    Ok(ids)
}
