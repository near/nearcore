use crate::test_loop::env::TestLoopChunksStorage;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::AccountId;
use std::sync::{Arc, Mutex};

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
            let chunks_storage = chunks_storage.lock().unwrap();
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
