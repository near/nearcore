mod fetch;
mod serve;

use bytesize::ByteSize;
pub(in crate::spice::data_manager) use fetch::{
    is_done, is_pushed_to, lowest_needed_height, made_pullable_by, needed_items,
};
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, ShardId};
pub(super) use serve::ReceiptProofServePolicy;
use std::collections::HashSet;

/// The largest encoded receipt proof; above the bandwidth scheduler's largest grant for one link.
pub(in crate::spice::data_manager) const MAX_ENCODED_LENGTH: u64 = ByteSize::mib(16).0;

/// The nodes the proofs from `source_block` into `to_shard` are pushed to: the next block's chunk
/// producers of `to_shard`; across a resharding, of the shards it splits into.
fn recipients(
    epoch_manager: &dyn EpochManagerAdapter,
    source_block: &CryptoHash,
    to_shard: ShardId,
) -> Result<HashSet<AccountId>, Error> {
    let source_epoch_id = epoch_manager.get_epoch_id(source_block)?;
    let next_block_epoch_id = epoch_manager.get_epoch_id_from_prev_block(source_block)?;
    let source_layout = epoch_manager.get_shard_layout(&source_epoch_id)?;
    let next_block_layout = epoch_manager.get_shard_layout(&next_block_epoch_id)?;
    let destination_shards = if next_block_layout == source_layout {
        vec![to_shard]
    } else {
        next_block_layout
            .get_children_shards_ids(to_shard)
            .ok_or(Error::InvalidShardId(to_shard))?
    };
    let mut recipients = HashSet::new();
    for shard_id in destination_shards {
        recipients.extend(
            epoch_manager.get_epoch_chunk_producers_for_shard(&next_block_epoch_id, shard_id)?,
        );
    }
    Ok(recipients)
}
