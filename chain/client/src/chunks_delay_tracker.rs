use lru::LruCache;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::ShardId;
use std::collections::HashMap;
use std::time::Instant;

use crate::metrics;

/// Provides monitoring information about the delays of receiving and requesting blocks and their corresponding chunks.
/// Keeps timestamps for all future blocks and chunks that were requested or received.
/// Updates the metrics when the head gets updated. Assumes that if the head moves forward, all needed chunks were received.
/// If a chunk or a block is received or requested multiple times, only the first time is recorded.
#[derive(Debug, Default)]
pub(crate) struct ChunksDelayTracker {
    /// Timestamp of trying to process a block for the first time.
    blocks_received: HashMap<CryptoHash, Instant>,
    chunks_in_progress: HashMap<ChunkHash, ChunkInProgress>,
}

#[derive(Debug, Default)]
struct ChunkInProgress {
    /// Timestamps of requesting missing chunks.
    chunk_requested: Option<Instant>,
    /// Timestamps of receiving each of the shards in a block.
    chunk_received: Option<Instant>,
}

impl ChunksDelayTracker {
    pub fn received_block(&mut self, block_hash: &CryptoHash, timestamp: Instant) {
        self.blocks_received.entry(*block_hash).or_insert(timestamp);
    }

    pub fn received_chunk(&mut self, chunk_hash: &ChunkHash, timestamp: Instant) {
        self.chunks_in_progress.get_mut(&chunk_hash).map(|chunk_in_progress|chunk_in_progress.chunk_received.get_or_insert(timestamp));
    }

    pub fn requested_chunk(&mut self, chunk_hash: &ChunkHash, timestamp: Instant) {
        self.chunks_in_progress.entry(*chunk_hash).or_insert_with(|| ChunkInProgress { chunk_requested: Some(timestamp),chunk_received: None  });
    }

    pub fn finish_block_processing(
        &mut self,
        processed_block_hash: &CryptoHash,
        chunks: &[(ChunkHash, ShardId)],
    ) {
        self.update_block_chunks_metric(processed_block_hash, &chunks);
        self.update_chunks_metric(&chunks);

        self.blocks_received.remove(&processed_block_hash);
        for (chunk_hash, _shard_id) in {
            self.chunks_in_progress.remove(chunk_hash);
        }
    }

    pub fn get_chunks(block: &Block) -> Vec<(ChunkHash, ShardId)> {
        block.chunks().iter().map(|chunk| (chunk.chunk_hash(), chunk.shard_id())).collect()
    }

    fn update_block_chunks_requested_metric(
        &mut self,
        block_hash: &CryptoHash,
        chunks: &[(ChunkHash, ShardId)],
    ) {
        if let Some(block_received) = self.blocks_received.get(&block_hash) {
            for (chunk_hash, shard_id) in chunks {
                let chunk_in_progress = self.chunks_in_progress.peek(&chunk_hash.clone());
                if let Some(chunk_in_progress) = chunk_in_progress {
                    if let Some(chunk_requested) = chunk_in_progress.chunk_requested {
                        metrics::BLOCK_CHUNKS_REQUESTED_DELAY
                            .with_label_values(&[&format!("{}", shard_id)])
                            .observe(
                                chunk_requested
                                    .saturating_duration_since(*block_received)
                                    .as_secs_f64(),
                            );
                    }
                }
            }
        }
    }

    fn update_chunks_metric(&mut self, chunks: &[(ChunkHash, ShardId)]) {
        for (chunk_hash, shard_id) in chunks {
            if let Some(chunk_in_progress) = self.chunks_in_progress.get(&chunk_hash.clone()) {
                if let Some(chunk_received) = chunk_in_progress.chunk_received {
                    if let Some(chunk_requested) = chunk_in_progress.chunk_requested {
                        metrics::CHUNK_RECEIVED_DELAY
                            .with_label_values(&[&format!("{}", shard_id)])
                            .observe(
                                chunk_received
                                    .saturating_duration_since(chunk_requested)
                                    .as_secs_f64(),
                            );
                    }
                }
            }
        }
    }
}
