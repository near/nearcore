use std::sync::{Arc, Mutex};

use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_primitives::types::AccountId;

use crate::test_loop::env::TestLoopChunksStorage;

/// Handler to drop all network messages relevant to chunk validated by
/// `validator_of_chunks_to_drop`. If number of nodes on chain is significant
/// enough (at least three?), this is enough to prevent chunk from being
/// included.
///
/// This logic can be easily extended to dropping chunk based on any rule.
pub fn partial_encoded_chunks_dropper(
    chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    epoch_manager_adapter: Arc<dyn EpochManagerAdapter>,
    validator_of_chunks_to_drop: AccountId,
) -> Arc<dyn Fn(NetworkRequests) -> Option<NetworkRequests>> {
    Arc::new(move |request| {
        // Filter out only messages related to distributing chunk in the
        // network; extract `chunk_hash` from the message.
        let chunk_hash = match &request {
            NetworkRequests::PartialEncodedChunkRequest { request, .. } => {
                Some(request.chunk_hash.clone())
            }
            NetworkRequests::PartialEncodedChunkResponse { response, .. } => {
                Some(response.chunk_hash.clone())
            }
            NetworkRequests::PartialEncodedChunkMessage { partial_encoded_chunk, .. } => {
                Some(partial_encoded_chunk.header.chunk_hash())
            }
            NetworkRequests::PartialEncodedChunkForward { forward, .. } => {
                Some(forward.chunk_hash.clone())
            }
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

        let prev_block_hash = chunk.prev_block_hash();
        let shard_id = chunk.shard_id();
        let height_created = chunk.height_created();

        // If we don't have block on top of which chunk is built, we can't
        // retrieve epoch id.
        // This case appears to be too rare to interfere with the goal of
        // dropping chunk.
        let Ok(epoch_id) = epoch_manager_adapter.get_epoch_id_from_prev_block(prev_block_hash)
        else {
            return Some(request);
        };

        // Finally, we drop chunk if the given account is present in the list
        // of its validators.
        let chunk_validators = epoch_manager_adapter
            .get_chunk_validator_assignments(&epoch_id, shard_id, height_created)
            .unwrap();
        if !chunk_validators.contains(&validator_of_chunks_to_drop) {
            return Some(request);
        }

        return None;
    })
}
