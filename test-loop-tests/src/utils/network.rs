//! Predicates on outgoing requests for the network rules, and the one handler that still
//! decides from chain state the rules do not see.

use crate::setup::drop_condition::TestLoopChunksStorage;
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

/// Matches every SPICE chunk endorsement whose sender is designated for the endorsed chunk, so
/// that dropping them lets chunks certify only via the all-stake fallback; non-designated
/// endorsements do not match. Decided on the sender's epoch manager, which knows the endorsed
/// block since the sender endorses what it processed.
pub fn designated_spice_endorsement_matcher(
    epoch_manager: Arc<dyn EpochManagerAdapter>,
) -> impl Fn(&NetworkRequests) -> bool + Send + Sync {
    move |request| {
        let NetworkRequests::SpiceChunkEndorsement(_target, endorsement) = request else {
            return false;
        };
        let Ok(block_info) = epoch_manager.get_block_info(endorsement.block_hash()) else {
            return false;
        };
        epoch_manager
            .get_chunk_validator_assignments(
                block_info.epoch_id(),
                endorsement.shard_id(),
                block_info.height(),
            )
            .is_ok_and(|assignments| assignments.contains(endorsement.account_id()))
    }
}

/// Matches every request for spice data, so that dropping them leaves only pushed data to
/// arrive.
pub fn spice_data_request_matcher() -> impl Fn(&NetworkRequests) -> bool + Send + Sync {
    |request| matches!(request, NetworkRequests::SpiceDataRequest { .. })
}

/// Matches every chunk endorsement, pre-spice or spice, from `validator`.
pub fn endorsements_from_matcher(
    validator: AccountId,
) -> impl Fn(&NetworkRequests) -> bool + Send + Sync {
    move |request| match request {
        NetworkRequests::ChunkEndorsement(_target, endorsement) => {
            endorsement.validator_account() == &validator
        }
        NetworkRequests::SpiceChunkEndorsement(_target, endorsement) => {
            endorsement.account_id() == &validator
        }
        _ => false,
    }
}

/// Matches the broadcast of every block at a height in `heights`. Dropping them on every
/// node skips the heights: only the producer of a dropped block has it, the others time out
/// and build on the block below, and the rules swallow the requests for the dropped block
/// that the producer's own child would otherwise trigger. A few things to note:
/// - Using this when there are too few validators will lead to the chain stalling rather
///   than the intended behavior of a skip being generated.
/// - Only the producer of the skipped block will receive it, so we only observe the
///   behavior when we see two different descendants of the same block on one node.
pub fn blocks_at_heights_matcher(
    heights: HashSet<BlockHeight>,
) -> impl Fn(&NetworkRequests) -> bool + Send + Sync {
    move |request| match request {
        NetworkRequests::Block { block } => heights.contains(&block.header().height()),
        _ => false,
    }
}

/// Matches every message that carries parts of a chunk `selects`: the parts a producer
/// pushes, the parts owners forward, and the request and response pair a node falls back
/// to. The forward, request and response name the chunk by hash only, so the chunk's header
/// comes from the shared chunks storage, which holds every chunk produced; a hash it does
/// not know matches nothing.
pub fn chunk_parts_matcher(
    chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    selects: impl Fn(&ShardChunkHeader) -> bool + Send + Sync + 'static,
) -> impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static {
    move |request| {
        let by_hash = |hash| chunks_storage.lock().get(hash).is_some_and(&selects);
        match request {
            NetworkRequests::PartialEncodedChunkMessage { partial_encoded_chunk, .. } => {
                selects(&partial_encoded_chunk.header)
            }
            NetworkRequests::PartialEncodedChunkForward { forward, .. } => {
                by_hash(&forward.chunk_hash)
            }
            NetworkRequests::PartialEncodedChunkRequest { request, .. } => {
                by_hash(&request.chunk_hash)
            }
            NetworkRequests::PartialEncodedChunkResponse { response, .. } => {
                by_hash(&response.chunk_hash)
            }
            _ => false,
        }
    }
}
