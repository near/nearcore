use std::num::NonZeroUsize;

use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, PartialEncodedContractDeploys,
};
use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
use near_primitives::types::BlockHeight;

const DEFAULT_PENDING_SIDE_CHANNEL_POOL_SIZE: usize = 64;

/// Side-channel messages that can be deferred when their `prev_block_hash`
/// is not yet available locally.
pub enum PendingSideChannelMessage {
    PartialWitness(VersionedPartialEncodedStateWitness),
    PartialWitnessForward(VersionedPartialEncodedStateWitness),
    ContractAccesses(ChunkContractAccesses),
    ContractDeploys(PartialEncodedContractDeploys),
}

impl PendingSideChannelMessage {
    fn prev_block_hash(&self) -> &CryptoHash {
        match self {
            Self::PartialWitness(w) | Self::PartialWitnessForward(w) => {
                w.prev_block_hash().expect("only V2 messages with prev_block_hash are deferred")
            }
            Self::ContractAccesses(a) => {
                a.prev_block_hash().expect("only V2 messages with prev_block_hash are deferred")
            }
            Self::ContractDeploys(d) => {
                d.prev_block_hash().expect("only V2 messages with prev_block_hash are deferred")
            }
        }
    }

    fn height_created(&self) -> BlockHeight {
        match self {
            Self::PartialWitness(w) | Self::PartialWitnessForward(w) => {
                w.chunk_production_key().height_created
            }
            Self::ContractAccesses(a) => a.chunk_production_key().height_created,
            Self::ContractDeploys(d) => d.chunk_production_key().height_created,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Self::PartialWitness(_) => "PartialWitness",
            Self::PartialWitnessForward(_) => "PartialWitnessForward",
            Self::ContractAccesses(_) => "ContractAccesses",
            Self::ContractDeploys(_) => "ContractDeploys",
        }
    }
}

/// Composite key for the LRU cache.
/// Uniquely identifies a pending message by (prev_block_hash, kind_discriminant, chunk_production_key).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PendingKey {
    prev_block_hash: CryptoHash,
    kind: u8,
    chunk_production_key: ChunkProductionKey,
}

impl PendingKey {
    fn from_message(msg: &PendingSideChannelMessage) -> Self {
        let kind = match msg {
            PendingSideChannelMessage::PartialWitness(_) => 0,
            PendingSideChannelMessage::PartialWitnessForward(_) => 1,
            PendingSideChannelMessage::ContractAccesses(_) => 2,
            PendingSideChannelMessage::ContractDeploys(_) => 3,
        };
        let chunk_production_key = match msg {
            PendingSideChannelMessage::PartialWitness(w)
            | PendingSideChannelMessage::PartialWitnessForward(w) => w.chunk_production_key(),
            PendingSideChannelMessage::ContractAccesses(a) => a.chunk_production_key().clone(),
            PendingSideChannelMessage::ContractDeploys(d) => d.chunk_production_key().clone(),
        };
        Self { prev_block_hash: *msg.prev_block_hash(), kind, chunk_production_key }
    }
}

/// Pool for side-channel messages (partial witnesses, contract accesses, contract deploys)
/// that arrive before the referenced `prev_block_hash` is available locally.
///
/// Follows the same pattern as `OrphanStateWitnessPool`: bounded LRU cache with
/// block-hash-based retrieval and height-based cleanup.
pub struct PendingSideChannelPool {
    cache: LruCache<PendingKey, PendingSideChannelMessage>,
}

impl PendingSideChannelPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).expect("capacity must be non-zero")),
        }
    }

    /// Defer a message whose `prev_block_hash` is not yet available.
    pub fn add_pending(&mut self, msg: PendingSideChannelMessage) {
        let key = PendingKey::from_message(&msg);
        tracing::debug!(
            target: "client",
            prev_block_hash = ?key.prev_block_hash,
            kind = msg.kind(),
            height = msg.height_created(),
            shard_id = %key.chunk_production_key.shard_id,
            pool_size = self.cache.len(),
            "deferring side-channel message, prev_block not yet available"
        );
        if let Some((evicted_key, _evicted_msg)) = self.cache.push(key, msg) {
            tracing::debug!(
                target: "client",
                prev_block_hash = ?evicted_key.prev_block_hash,
                height = evicted_key.chunk_production_key.height_created,
                shard_id = %evicted_key.chunk_production_key.shard_id,
                "evicted a pending side-channel message due to capacity limit"
            );
        }
    }

    /// Remove and return all messages that were waiting for `block_hash` as their
    /// `prev_block_hash`.
    pub fn take_messages_waiting_for_block(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Vec<PendingSideChannelMessage> {
        let to_remove: Vec<PendingKey> = self
            .cache
            .iter()
            .filter(|(key, _)| &key.prev_block_hash == block_hash)
            .map(|(key, _)| key.clone())
            .collect();
        let mut result = Vec::new();
        for key in to_remove {
            if let Some(msg) = self.cache.pop(&key) {
                result.push(msg);
            }
        }
        result
    }

    /// Remove all messages whose `height_created` is at or below `final_height`.
    pub fn remove_messages_below_final_height(&mut self, final_height: BlockHeight) {
        let to_remove: Vec<PendingKey> = self
            .cache
            .iter()
            .filter(|(key, _)| key.chunk_production_key.height_created <= final_height)
            .map(|(key, _)| key.clone())
            .collect();
        for key in &to_remove {
            tracing::debug!(
                target: "client",
                prev_block_hash = ?key.prev_block_hash,
                height = key.chunk_production_key.height_created,
                shard_id = %key.chunk_production_key.shard_id,
                final_height,
                "removing pending side-channel message below final height"
            );
        }
        for key in to_remove {
            self.cache.pop(&key);
        }
    }
}

impl Default for PendingSideChannelPool {
    fn default() -> Self {
        Self::new(DEFAULT_PENDING_SIDE_CHANNEL_POOL_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
    use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
    use near_primitives::test_utils::create_test_signer;
    use near_primitives::types::{BlockHeight, ShardId};

    use super::{PendingSideChannelMessage, PendingSideChannelPool};

    fn block(height: BlockHeight) -> CryptoHash {
        hash(&height.to_be_bytes())
    }

    /// Create a dummy PartialWitness-type pending message.
    fn make_pending_witness(
        height: BlockHeight,
        shard_id: ShardId,
        prev_block_hash: CryptoHash,
    ) -> PendingSideChannelMessage {
        let witness = ChunkStateWitness::new_dummy(height, shard_id, prev_block_hash);
        let key = witness.chunk_production_key();
        let signer = create_test_signer("test");
        let partial = VersionedPartialEncodedStateWitness::new(
            key.epoch_id,
            witness.chunk_header().clone(),
            prev_block_hash,
            0, // part_ord
            vec![0u8; 32],
            100, // encoded_length
            &signer,
            near_primitives::version::PROTOCOL_VERSION,
        );
        PendingSideChannelMessage::PartialWitness(partial)
    }

    fn make_pending_forward(
        height: BlockHeight,
        shard_id: ShardId,
        prev_block_hash: CryptoHash,
    ) -> PendingSideChannelMessage {
        let witness = ChunkStateWitness::new_dummy(height, shard_id, prev_block_hash);
        let key = witness.chunk_production_key();
        let signer = create_test_signer("test");
        let partial = VersionedPartialEncodedStateWitness::new(
            key.epoch_id,
            witness.chunk_header().clone(),
            prev_block_hash,
            0,
            vec![0u8; 32],
            100,
            &signer,
            near_primitives::version::PROTOCOL_VERSION,
        );
        PendingSideChannelMessage::PartialWitnessForward(partial)
    }

    fn assert_message_count(messages: &[PendingSideChannelMessage], expected: usize) {
        assert_eq!(
            messages.len(),
            expected,
            "expected {expected} messages, got {}",
            messages.len()
        );
    }

    #[test]
    fn basic_add_and_take() {
        let mut pool = PendingSideChannelPool::new(10);

        let msg1 = make_pending_witness(100, ShardId::new(1), block(99));
        let msg2 = make_pending_witness(100, ShardId::new(2), block(99));
        let msg3 = make_pending_witness(101, ShardId::new(1), block(100));

        pool.add_pending(msg1);
        pool.add_pending(msg2);
        pool.add_pending(msg3);

        let waiting_for_99 = pool.take_messages_waiting_for_block(&block(99));
        assert_message_count(&waiting_for_99, 2);

        let waiting_for_100 = pool.take_messages_waiting_for_block(&block(100));
        assert_message_count(&waiting_for_100, 1);

        // Pool should be empty now
        let waiting_for_99 = pool.take_messages_waiting_for_block(&block(99));
        assert_message_count(&waiting_for_99, 0);
    }

    #[test]
    fn different_kinds_same_chunk_key() {
        let mut pool = PendingSideChannelPool::new(10);

        // Same height/shard/prev_block but different message kinds should coexist
        let msg1 = make_pending_witness(100, ShardId::new(1), block(99));
        let msg2 = make_pending_forward(100, ShardId::new(1), block(99));

        pool.add_pending(msg1);
        pool.add_pending(msg2);

        let waiting = pool.take_messages_waiting_for_block(&block(99));
        assert_message_count(&waiting, 2);
    }

    #[test]
    fn replacing_same_key() {
        let mut pool = PendingSideChannelPool::new(10);

        let msg1 = make_pending_witness(100, ShardId::new(1), block(99));
        let msg2 = make_pending_witness(100, ShardId::new(1), block(99));

        pool.add_pending(msg1);
        pool.add_pending(msg2);

        // Should have replaced, so only 1 message
        let waiting = pool.take_messages_waiting_for_block(&block(99));
        assert_message_count(&waiting, 1);
    }

    #[test]
    fn lru_eviction() {
        let mut pool = PendingSideChannelPool::new(2);

        let msg1 = make_pending_witness(100, ShardId::new(1), block(99));
        let msg2 = make_pending_witness(101, ShardId::new(1), block(100));
        let msg3 = make_pending_witness(102, ShardId::new(1), block(101));

        pool.add_pending(msg1);
        pool.add_pending(msg2);
        // This should evict msg1
        pool.add_pending(msg3);

        let waiting_for_99 = pool.take_messages_waiting_for_block(&block(99));
        assert_message_count(&waiting_for_99, 0);

        let waiting_for_100 = pool.take_messages_waiting_for_block(&block(100));
        assert_message_count(&waiting_for_100, 1);

        let waiting_for_101 = pool.take_messages_waiting_for_block(&block(101));
        assert_message_count(&waiting_for_101, 1);
    }

    #[test]
    fn remove_below_final_height() {
        let mut pool = PendingSideChannelPool::new(10);

        let msg1 = make_pending_witness(100, ShardId::new(1), block(99));
        let msg2 = make_pending_witness(101, ShardId::new(1), block(100));
        let msg3 = make_pending_witness(102, ShardId::new(1), block(101));
        let msg4 = make_pending_witness(103, ShardId::new(1), block(102));

        pool.add_pending(msg1);
        pool.add_pending(msg2);
        pool.add_pending(msg3);
        pool.add_pending(msg4);

        pool.remove_messages_below_final_height(101);

        // Messages at height 100 and 101 should be removed
        let waiting_for_99 = pool.take_messages_waiting_for_block(&block(99));
        assert_message_count(&waiting_for_99, 0);

        let waiting_for_100 = pool.take_messages_waiting_for_block(&block(100));
        assert_message_count(&waiting_for_100, 0);

        // Messages at height 102 and 103 should remain
        let waiting_for_101 = pool.take_messages_waiting_for_block(&block(101));
        assert_message_count(&waiting_for_101, 1);

        let waiting_for_102 = pool.take_messages_waiting_for_block(&block(102));
        assert_message_count(&waiting_for_102, 1);
    }

    #[test]
    fn take_returns_nothing_for_unknown_block() {
        let mut pool = PendingSideChannelPool::new(10);
        let msg1 = make_pending_witness(100, ShardId::new(1), block(99));
        pool.add_pending(msg1);

        let waiting = pool.take_messages_waiting_for_block(&block(42));
        assert_message_count(&waiting, 0);
    }
}
