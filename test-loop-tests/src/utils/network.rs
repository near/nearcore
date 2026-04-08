use crate::setup::drop_condition::TestLoopChunksStorage;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{PeerMessage, T1MessageBody, TieredMessageBody};
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, BlockHeight};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

type DropChunkCondition = Box<dyn Fn(ShardChunkHeader) -> bool + Send + Sync>;

/// Transport filter that drops all `PeerMessage::Routed` messages containing
/// `VersionedChunkEndorsement` whose chunk matches `drop_chunk_condition`.
///
/// Looks up the chunk header from `chunks_storage` by the endorsement's
/// `chunk_hash()`. If the chunk isn't found or shouldn't be dropped yet,
/// the message passes through.
pub fn chunk_endorsement_dropper_by_hash_filter(
    chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    epoch_manager_adapter: Arc<dyn EpochManagerAdapter>,
    drop_chunk_condition: DropChunkCondition,
) -> Arc<
    dyn Fn(
            &near_primitives::network::PeerId,
            &near_primitives::network::PeerId,
            &PeerMessage,
        ) -> Option<PeerMessage>
        + Send
        + Sync,
> {
    Arc::new(move |_from, _to, msg| {
        let PeerMessage::Routed(routed_msg) = msg else {
            return Some(msg.clone());
        };

        let TieredMessageBody::T1(body) = routed_msg.body() else {
            return Some(msg.clone());
        };

        let T1MessageBody::VersionedChunkEndorsement(endorsement) = &**body else {
            return Some(msg.clone());
        };

        let chunk_hash = endorsement.chunk_hash();

        let chunk = {
            let chunks_storage = chunks_storage.lock();
            let Some(chunk) = chunks_storage.get(&chunk_hash).cloned() else {
                return Some(msg.clone());
            };
            if !chunks_storage.can_drop_chunk(&chunk) {
                return Some(msg.clone());
            }
            chunk
        };

        // If we don't have block on top of which chunk is built, we can't
        // retrieve epoch id. Allow the message through.
        if epoch_manager_adapter.get_epoch_id_from_prev_block(chunk.prev_block_hash()).is_err() {
            return Some(msg.clone());
        }

        if drop_chunk_condition(chunk) {
            return None;
        }

        Some(msg.clone())
    })
}

/// Transport filter that drops all `PeerMessage::Routed` messages containing
/// `VersionedChunkEndorsement` from the given chunk-validator account.
pub fn chunk_endorsement_dropper_filter(
    validator: AccountId,
) -> Arc<
    dyn Fn(
            &near_primitives::network::PeerId,
            &near_primitives::network::PeerId,
            &PeerMessage,
        ) -> Option<PeerMessage>
        + Send
        + Sync,
> {
    Arc::new(move |_from, _to, msg| {
        let PeerMessage::Routed(routed_msg) = msg else {
            return Some(msg.clone());
        };

        let TieredMessageBody::T1(body) = routed_msg.body() else {
            return Some(msg.clone());
        };

        let T1MessageBody::VersionedChunkEndorsement(endorsement) = &**body else {
            return Some(msg.clone());
        };

        if endorsement.validator_account() == &validator {
            return None;
        }

        Some(msg.clone())
    })
}

/// Transport filter that drops all `PeerMessage::Block` broadcasts at certain heights.
///
/// A few things to note:
/// - This will not fully prevent the blocks from being distributed if they are explicitly requested with
/// a `BlockRequest`, because that case isn't handled here.
/// - Using this when there are too few validators will lead to the chain stalling rather than the intended behavior
/// of a skip being generated.
/// - Only the producer of the skipped block will receive it, so we only observe the behavior when we see two different
/// descendants of the same block on one node.
pub fn block_dropper_by_height_filter(
    heights: HashSet<BlockHeight>,
) -> Arc<
    dyn Fn(
            &near_primitives::network::PeerId,
            &near_primitives::network::PeerId,
            &PeerMessage,
        ) -> Option<PeerMessage>
        + Send
        + Sync,
> {
    Arc::new(move |_from, _to, msg| {
        if let PeerMessage::Block(block) = msg {
            if heights.contains(&block.header().height()) {
                return None;
            }
        }
        Some(msg.clone())
    })
}
