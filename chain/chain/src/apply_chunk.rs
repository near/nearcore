use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use std::collections::HashMap;
use std::sync::Arc;

use near_primitives::block::Block;
use near_primitives::sharding::ShardChunkHeader;
use near_store::ShardUId;

/// apply_chunks may be called in two code paths, through process_block or through catchup_blocks
/// When it is called through process_block, it is possible that the shard state for the next epoch
/// has not been caught up yet, thus the two modes IsCaughtUp and NotCaughtUp.
/// CatchingUp is for when apply_chunks is called through catchup_blocks, this is to catch up the
/// shard states for the next epoch
#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub(crate) enum ApplyChunksMode {
    IsCaughtUp,
    CatchingUp,
    NotCaughtUp,
}

pub(crate) struct SplitState {
    roots: HashMap<ShardUId, CryptoHash>,
}

pub(crate) struct ShardInfo {
    shard_uid: ShardUId,
    cares_about_shard_this_epoch: bool,
    cares_about_shard_next_epoch: bool,
    will_shard_layout_change: bool,
}

pub(crate) struct NewChunk {
    incoming_receipts: Vec<Receipt>,
}

pub(crate) fn apply_chunk(
    block: &Block,
    prev_block: &Block,
    chunk_header: &ShardChunkHeader,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard: ShardInfo,
    mode: ApplyChunksMode,
    split_state: Option<SplitState>,
) {
    let prev_hash = block.header().prev_hash();
    let is_new_chunk = chunk_header.height_included() == block.header().height();
    let should_apply_transactions = get_should_apply_transactions(
        mode,
        shard.cares_about_shard_this_epoch,
        shard.cares_about_shard_next_epoch,
    );
    if should_apply_transactions {
        if is_new_chunk {
        } else {
        }
    } else if let Some(split_state) = split_state {
        // Case 3), split state are ready. Read the state changes from the
        // database and apply them to the split states.
        assert!(mode == ApplyChunksMode::CatchingUp && shard.cares_about_shard_this_epoch);
    }
}

fn apply_new_chunk(

) {
}

/// We want to guarantee that transactions are only applied once for each shard,
/// even though apply_chunks may be called twice, once with
/// ApplyChunksMode::NotCaughtUp once with ApplyChunksMode::CatchingUp. Note
/// that it does not guard whether we split states or not, see the comments
/// before `need_to_split_state`
pub(crate) fn get_should_apply_transactions(
    mode: ApplyChunksMode,
    cares_about_shard_this_epoch: bool,
    cares_about_shard_next_epoch: bool,
) -> bool {
    match mode {
        // next epoch's shard states are not ready, only update this epoch's shards
        ApplyChunksMode::NotCaughtUp => cares_about_shard_this_epoch,
        // update both this epoch and next epoch
        ApplyChunksMode::IsCaughtUp => cares_about_shard_this_epoch || cares_about_shard_next_epoch,
        // catching up next epoch's shard states, do not update this epoch's shard state
        // since it has already been updated through ApplyChunksMode::NotCaughtUp
        ApplyChunksMode::CatchingUp => {
            !cares_about_shard_this_epoch && cares_about_shard_next_epoch
        }
    }
}
