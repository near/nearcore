use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

use crate::setup::drop_condition::TestLoopChunksStorage;

type DropChunkCondition = Box<dyn Fn(ShardChunkHeader) -> bool>;

/// Handler to drop all endorsement messages relevant to chunk body, based on
/// `drop_chunks_condition` result.
pub fn chunk_endorsement_dropper_by_hash(
    chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    epoch_manager_adapter: Arc<dyn EpochManagerAdapter>,
    drop_chunk_condition: DropChunkCondition,
) -> Box<dyn Fn(NetworkRequests) -> Option<NetworkRequests>> {
    Box::new(move |request| {
        // Filter out only messages related to distributing chunk in the
        // network; extract `chunk_hash` from the message.
        let chunk_hash = match &request {
            NetworkRequests::ChunkEndorsement(_, endorsement) => Some(endorsement.chunk_hash()),
            _ => None,
        };

        let Some(chunk_hash) = chunk_hash else {
            return Some(request);
        };

        let chunk = {
            let chunks_storage = chunks_storage.lock();
            let chunk = chunks_storage.get(&chunk_hash).unwrap().clone();
            let can_drop_chunk = chunks_storage.can_drop_chunk(&chunk);

            if !can_drop_chunk {
                return Some(request);
            }

            chunk
        };

        // If we don't have block on top of which chunk is built, we can't
        // retrieve epoch id.
        // This case appears to be too rare to interfere with the goal of
        // dropping chunk.
        if epoch_manager_adapter.get_epoch_id_from_prev_block(chunk.prev_block_hash()).is_err() {
            return Some(request);
        };

        if drop_chunk_condition(chunk) {
            return None;
        }

        Some(request)
    })
}

/// Handler to drop all network messages containing chunk endorsements sent
/// from a given chunk-validator account.
pub fn chunk_endorsement_dropper(
    validator: AccountId,
) -> Box<dyn Fn(NetworkRequests) -> Option<NetworkRequests>> {
    Box::new(move |request| {
        if let NetworkRequests::ChunkEndorsement(_target, endorsement) = &request {
            if endorsement.validator_account() == &validator {
                return None;
            }
        }
        Some(request)
    })
}

/// Handler to drop all block broadcasts at certain heights.
/// A few things to note:
/// - This will not fully prevent the blocks from being distributed if they are explicitly requested with
/// a `NetworkRequests::BlockRequest`, because that case isn't handled here. For example, this can happen
/// if the producer for a height, `h`, in `heights` is also the producer for `h+1`, and then distributes `h+1`
/// with prev block equal to the one with height `h`. Then a node that receives that block with height `h+1` will
/// record it as an orphan and request the block with height `h`. We don't handle it here because it's a bit more
/// complicated since the BlockRequest just references the hash and not the height, so we would need to do something
/// like the `TestLoopChunksStorage` but for blocks. For now we only use this in a test setup with enough block
/// producers so that this will not come up.
/// - Using this when there are too few validators will lead to the chain stalling rather than the intended behavior
/// of a skip being generated.
/// - Only the producer of the skipped block will receive it, so we only observe the behavior when we see two different
/// descendants of the same block on one node. This could be improved, though.
pub fn block_dropper_by_height(
    heights: HashSet<BlockHeight>,
) -> Box<dyn Fn(NetworkRequests) -> Option<NetworkRequests>> {
    Box::new(move |request| match &request {
        NetworkRequests::Block { block } => {
            if !heights.contains(&block.header().height()) {
                Some(request)
            } else {
                None
            }
        }
        _ => Some(request),
    })
}
