use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use std::collections::HashMap;
use std::time::Instant;

use crate::metrics;

/// Provides monitoring information about the delays of receiving and requesting blocks and their corresponding chunks.
/// Keeps timestamps for all future blocks and chunks that were requested or received.
/// Updates the metrics when the head gets updated. Assumes that if the head moves forward, all needed chunks were received.
/// If a chunk or a block is received or requested multiple times, only the first time is recorded.
#[derive(Debug, Default)]
pub(crate) struct ChunksDelayTracker {
    /// Timestamps of the first attempt of block processing.
    /// Contains only blocks that are not yet processed.
    blocks_in_progress: HashMap<CryptoHash, Instant>,
    /// An entry gets created when a chunk gets requested for the first time.
    /// Chunks get deleted when the block gets processed.
    chunks_in_progress: HashMap<ChunkHash, ChunkInProgress>,
}

#[derive(Debug)]
/// Records timestamps of requesting and receiving a chunk. Assumes that each chunk is requested
/// before it is received.
struct ChunkInProgress {
    /// Timestamp of requesting a missing chunk.
    chunk_requested: Instant,
    /// Timestamp of receiving a chunk.
    /// If a chunk is received multiple times, only the earliest timestamp is recorded.
    chunk_received: Option<Instant>,
}

impl ChunksDelayTracker {
    pub fn received_block(&mut self, block_hash: &CryptoHash, timestamp: Instant) {
        self.blocks_in_progress.entry(*block_hash).or_insert(timestamp);
    }

    pub fn received_chunk(&mut self, chunk_hash: &ChunkHash, timestamp: Instant) {
        self.chunks_in_progress
            .get_mut(&chunk_hash)
            .map(|chunk_in_progress| chunk_in_progress.chunk_received.get_or_insert(timestamp));
    }

    pub fn requested_chunk(&mut self, chunk_hash: &ChunkHash, timestamp: Instant) {
        self.chunks_in_progress.entry(chunk_hash.clone()).or_insert_with(|| ChunkInProgress {
            chunk_requested: timestamp,
            chunk_received: None,
        });
    }

    pub fn finish_block_processing(
        &mut self,
        processed_block_hash: &CryptoHash,
        chunks: &[ChunkHash],
    ) {
        self.update_block_chunks_requested_metric(processed_block_hash, &chunks);
        self.update_chunks_metric(&chunks);

        self.blocks_in_progress.remove(&processed_block_hash);
        for chunk_hash in chunks {
            self.chunks_in_progress.remove(chunk_hash);
        }
    }

    fn update_block_chunks_requested_metric(
        &mut self,
        block_hash: &CryptoHash,
        chunks: &[ChunkHash],
    ) {
        if let Some(block_received) = self.blocks_in_progress.get(&block_hash) {
            for (shard_id, chunk_hash) in chunks.iter().enumerate() {
                if let Some(chunk_in_progress) = self.chunks_in_progress.get(chunk_hash) {
                    let chunk_requested = chunk_in_progress.chunk_requested;
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

    fn update_chunks_metric(&mut self, chunks: &[ChunkHash]) {
        for (shard_id, chunk_hash) in chunks.iter().enumerate() {
            if let Some(chunk_in_progress) = self.chunks_in_progress.get(&chunk_hash) {
                if let Some(chunk_received) = chunk_in_progress.chunk_received {
                    let chunk_requested = chunk_in_progress.chunk_requested;
                    metrics::CHUNK_RECEIVED_DELAY
                        .with_label_values(&[&format!("{}", shard_id)])
                        .observe(
                            chunk_received.saturating_duration_since(chunk_requested).as_secs_f64(),
                        );
                }
            }
        }
    }
}
