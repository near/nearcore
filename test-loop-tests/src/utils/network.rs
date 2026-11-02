use crate::setup::drop_condition::{DroppedBlocks, TestLoopChunksStorage};
use crate::setup::peer_manager_actor::HandlerResult;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_network::types::NetworkResponses;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

type DropChunkCondition = Box<dyn Fn(ShardChunkHeader) -> bool>;

/// Handler to drop all endorsement messages relevant to chunk body, based on
/// `drop_chunks_condition` result.
pub fn chunk_endorsement_dropper_by_hash(
    chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    epoch_manager_adapter: Arc<dyn EpochManagerAdapter>,
    drop_chunk_condition: DropChunkCondition,
) -> impl Fn(NetworkRequests) -> HandlerResult {
    move |request| {
        // Filter out only messages related to distributing chunk in the
        // network; extract `chunk_hash` from the message.
        let chunk_hash = match &request {
            NetworkRequests::ChunkEndorsement(_, endorsement) => Some(endorsement.chunk_hash()),
            _ => None,
        };

        let Some(chunk_hash) = chunk_hash else {
            return HandlerResult::Unhandled(request);
        };

        let chunk = {
            let chunks_storage = chunks_storage.lock();
            let chunk = chunks_storage.get(&chunk_hash).unwrap().clone();
            let can_drop_chunk = chunks_storage.can_drop_chunk(&chunk);

            if !can_drop_chunk {
                return HandlerResult::Unhandled(request);
            }

            chunk
        };

        // If we don't have block on top of which chunk is built, we can't
        // retrieve epoch id.
        // This case appears to be too rare to interfere with the goal of
        // dropping chunk.
        if epoch_manager_adapter.get_epoch_id_from_prev_block(chunk.prev_block_hash()).is_err() {
            return HandlerResult::Unhandled(request);
        };

        if drop_chunk_condition(chunk) {
            return HandlerResult::Handled(NetworkResponses::NoResponse);
        }

        HandlerResult::Unhandled(request)
    }
}

/// Drops every SPICE chunk endorsement whose sender is designated for the endorsed chunk, so chunks
/// can only certify via the all-stake fallback. Non-designated endorsements pass through.
pub fn spice_designated_endorsement_dropper(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> impl Fn(NetworkRequests) -> HandlerResult {
    move |request| {
        let NetworkRequests::SpiceChunkEndorsement(_target, endorsement) = &request else {
            return HandlerResult::Unhandled(request);
        };
        let Ok(block_info) = epoch_manager.get_block_info(endorsement.block_hash()) else {
            return HandlerResult::Unhandled(request);
        };
        let Ok(assignments) = epoch_manager.get_chunk_validator_assignments(
            block_info.epoch_id(),
            endorsement.shard_id(),
            block_info.height(),
        ) else {
            return HandlerResult::Unhandled(request);
        };
        if assignments.contains(endorsement.account_id()) {
            return HandlerResult::Handled(NetworkResponses::NoResponse);
        }
        HandlerResult::Unhandled(request)
    }
}

/// Handler to drop every request for spice data, leaving only pushed data to arrive.
pub fn spice_data_request_dropper() -> impl Fn(NetworkRequests) -> HandlerResult {
    move |request| match &request {
        NetworkRequests::SpiceDataRequest { .. } => {
            HandlerResult::Handled(NetworkResponses::NoResponse)
        }
        _ => HandlerResult::Unhandled(request),
    }
}

/// Handler to drop all network messages containing chunk endorsements sent
/// from a given chunk-validator account.
pub fn chunk_endorsement_dropper(
    validator: AccountId,
) -> impl Fn(NetworkRequests) -> HandlerResult {
    move |request| {
        match &request {
            NetworkRequests::ChunkEndorsement(_target, endorsement)
                if endorsement.validator_account() == &validator =>
            {
                return HandlerResult::Handled(NetworkResponses::NoResponse);
            }
            NetworkRequests::SpiceChunkEndorsement(_target, endorsement)
                if endorsement.account_id() == &validator =>
            {
                return HandlerResult::Handled(NetworkResponses::NoResponse);
            }
            _ => {}
        }
        HandlerResult::Unhandled(request)
    }
}

/// Handler to drop all block broadcasts at certain heights, and the requests for the blocks
/// it dropped. The requests matter when the producer of a dropped height `h` also produces
/// `h+1` and builds it on its own block.
/// A few things to note:
/// - Using this when there are too few validators will lead to the chain stalling rather than the intended behavior
/// of a skip being generated.
/// - Only the producer of the skipped block will receive it, so we only observe the behavior when we see two different
/// descendants of the same block on one node. This could be improved, though.
pub fn block_dropper_by_height(
    heights: HashSet<BlockHeight>,
    dropped_blocks: Arc<Mutex<DroppedBlocks>>,
) -> impl Fn(NetworkRequests) -> HandlerResult {
    move |request| match &request {
        NetworkRequests::Block { block } => {
            if !heights.contains(&block.header().height()) {
                HandlerResult::Unhandled(request)
            } else {
                dropped_blocks.lock().hashes.insert(*block.hash());
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
        }
        NetworkRequests::BlockRequest { hash, .. } => {
            let mut dropped_blocks = dropped_blocks.lock();
            if !dropped_blocks.hashes.contains(hash) {
                HandlerResult::Unhandled(request)
            } else {
                dropped_blocks.requests_swallowed += 1;
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
        }
        _ => HandlerResult::Unhandled(request),
    }
}
