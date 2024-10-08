use crate::test_loop::env::TestLoopChunksStorage;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::NetworkRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, EpochId, ShardId};
use std::sync::{Arc, Mutex};

/// returns chunks_produced[height_created - epoch_start][shard_id].
/// If the chunk producer for this height and shard is the same as the block producer
/// for this height, however, we always return true. Because in that case the block producer
/// will include the chunk anyway even if we return false here, and then the other clients
/// will fail to download chunks for that block.
fn should_produce_chunk(
    chunks_produced: &[Vec<bool>],
    epoch_id: &EpochId,
    prev_block_hash: &CryptoHash,
    height_created: BlockHeight,
    shard_id: ShardId,
    epoch_manager_adapter: &dyn EpochManagerAdapter,
) -> bool {
    let block_producer =
        epoch_manager_adapter.get_block_producer(epoch_id, height_created).unwrap();
    let chunk_producer =
        epoch_manager_adapter.get_chunk_producer(epoch_id, height_created, shard_id).unwrap();

    if block_producer == chunk_producer {
        return true;
    }
    let height_in_epoch =
        if epoch_manager_adapter.is_next_block_epoch_start(prev_block_hash).unwrap() {
            0
        } else {
            let epoch_start =
                epoch_manager_adapter.get_epoch_start_height(prev_block_hash).unwrap();
            height_created - epoch_start
        };
    let chunks_produced = match chunks_produced.get(height_in_epoch as usize) {
        Some(s) => s,
        None => return true,
    };
    match chunks_produced.get(shard_id as usize) {
        Some(should_produce) => *should_produce,
        None => false,
    }
}

/// Handler to drop all network messages relevant to chunk validated by
/// `validator_of_chunks_to_drop` or chunks we shouldn't include on chain as indicated
/// by `chunks_produced`. If number of nodes on chain is significant enough (at least three?),
/// dropping messages by `validator_of_chunks_to_drop` is enough to prevent chunk from being included.
///
/// This logic can be easily extended to dropping chunk based on any rule.
pub fn partial_encoded_chunks_dropper(
    chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    epoch_manager_adapter: Arc<dyn EpochManagerAdapter>,
    validator_of_chunks_to_drop: Option<AccountId>,
    chunks_produced: Option<Vec<Vec<bool>>>,
) -> Box<dyn Fn(NetworkRequests) -> Option<NetworkRequests>> {
    assert!(validator_of_chunks_to_drop.is_some() || chunks_produced.is_some());

    Box::new(move |request| {
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
        if let Some(validator_of_chunks_to_drop) = &validator_of_chunks_to_drop {
            let chunk_validators = epoch_manager_adapter
                .get_chunk_validator_assignments(&epoch_id, shard_id, height_created)
                .unwrap();
            if !chunk_validators.contains(validator_of_chunks_to_drop) {
                return Some(request);
            }
        }

        // If that isn't the case, we drop the chunk if this shard and height_created should miss producing a chunk
        if let Some(chunks_produced) = &chunks_produced {
            if should_produce_chunk(
                chunks_produced,
                &epoch_id,
                prev_block_hash,
                height_created,
                shard_id,
                epoch_manager_adapter.as_ref(),
            ) {
                return Some(request);
            }
        }
        return None;
    })
}

/// Handler to drop all network messages containing chunk endorsements sent from a given chunk-validator account.
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
