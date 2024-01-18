use itertools::Itertools;
use lru::LruCache;
use std::collections::HashMap;

use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::{AccountId, EpochId, ShardId};

use crate::metrics;

const CHUNK_HEADERS_FOR_INCLUSION_CACHE_SIZE: usize = 2048;
const NUM_EPOCH_CHUNK_PRODUCERS_TO_KEEP_IN_BLOCKLIST: usize = 1000;

pub struct ChunkInclusionTracker {
    prev_block_to_chunk_headers_ready_for_inclusion: LruCache<
        CryptoHash,
        HashMap<ShardId, (ShardChunkHeader, chrono::DateTime<chrono::Utc>, AccountId)>,
    >,

    banned_chunk_producers: LruCache<(EpochId, AccountId), ()>,
}

impl ChunkInclusionTracker {
    pub fn new() -> Self {
        Self {
            prev_block_to_chunk_headers_ready_for_inclusion: LruCache::new(
                CHUNK_HEADERS_FOR_INCLUSION_CACHE_SIZE,
            ),
            // chunk_header_info: LruCache::new(CHUNK_HEADERS_FOR_INCLUSION_CACHE_SIZE * 10),
            banned_chunk_producers: LruCache::new(NUM_EPOCH_CHUNK_PRODUCERS_TO_KEEP_IN_BLOCKLIST),
        }
    }

    pub fn mark_chunk_header_ready_for_inclusion(
        &mut self,
        chunk_header: ShardChunkHeader,
        chunk_producer: AccountId,
    ) {
        let prev_block_hash = chunk_header.prev_block_hash();
        self.prev_block_to_chunk_headers_ready_for_inclusion
            .get_or_insert(*prev_block_hash, || HashMap::new());
        self.prev_block_to_chunk_headers_ready_for_inclusion
            .get_mut(prev_block_hash)
            .unwrap()
            .insert(chunk_header.shard_id(), (chunk_header, chrono::Utc::now(), chunk_producer));
    }

    pub fn ban_chunk_producer(&mut self, epoch_id: EpochId, account_id: AccountId) {
        self.banned_chunk_producers.put((epoch_id, account_id), ());
    }

    pub fn get_chunk_headers_ready_for_inclusion(
        &self,
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
    ) -> HashMap<ShardId, (ShardChunkHeader, chrono::DateTime<chrono::Utc>, AccountId)> {
        self.prev_block_to_chunk_headers_ready_for_inclusion
            .peek(prev_block_hash)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, (chunk_header, _, chunk_producer))| {
                let banned = self
                    .banned_chunk_producers
                    .contains(&(epoch_id.clone(), chunk_producer.clone()));
                if banned {
                    tracing::warn!(
                        target: "client",
                        chunk_hash = ?chunk_header.chunk_hash(),
                        ?chunk_producer,
                        "Not including chunk from a banned validator");
                    metrics::CHUNK_DROPPED_BECAUSE_OF_BANNED_CHUNK_PRODUCER.inc();
                }
                !banned
            })
            .collect()
    }

    pub fn num_chunk_headers_ready_for_inclusion(
        &self,
        epoch_id: &EpochId,
        prev_block_hash: &CryptoHash,
    ) -> usize {
        self.get_chunk_headers_ready_for_inclusion(epoch_id, prev_block_hash).len()
    }

    pub fn get_banned_chunk_producers(&self) -> Vec<(EpochId, Vec<AccountId>)> {
        let mut banned_chunk_producers: HashMap<EpochId, Vec<_>> = HashMap::new();
        for ((epoch_id, account_id), _) in self.banned_chunk_producers.iter() {
            banned_chunk_producers.entry(epoch_id.clone()).or_default().push(account_id.clone());
        }
        banned_chunk_producers.into_iter().collect_vec()
    }
}
