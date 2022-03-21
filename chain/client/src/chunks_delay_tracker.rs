use itertools::Itertools;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::time::Instant;

use crate::metrics;

/// Provides monitoring information about the delays of receiving and requesting blocks and their corresponding chunks.
/// Keeps timestamps for all future blocks and chunks that were requested or received.
/// Updates the metrics when the head gets updated. Assumes that if the head moves forward, all needed chunks were received.
/// If a chunk or a block is received or requested multiple times, only the first time is recorded.
#[derive(Debug, Default)]
pub struct ChunksDelayTracker {
    /// Timestamps of the first attempt of block processing.
    /// Contains only blocks that are not yet processed.
    pub blocks_in_progress: HashMap<CryptoHash, BlockInProgress>,
    /// An entry gets created when a chunk gets requested for the first time.
    /// Chunks get deleted when the block gets processed.
    pub chunks_in_progress: HashMap<ChunkHash, ChunkInProgress>,
}

#[derive(Debug)]
pub struct BlockInProgress {
    /// Timestamp when block was added.
    pub timestamp: Instant,
    /// Height at which the block is going to be added.
    pub height: BlockHeight,
    /// Hashes of the chunks that belong to this block.
    pub chunks: Vec<ChunkHash>,
}

#[derive(Debug)]
/// Records timestamps of requesting and receiving a chunk. Assumes that each chunk is requested
/// before it is received.
pub struct ChunkInProgress {
    /// Timestamp of requesting a missing chunk.
    pub chunk_requested: Instant,
    /// Timestamp of receiving a chunk.
    /// If a chunk is received multiple times, only the earliest timestamp is recorded.
    pub chunk_received: Option<Instant>,
    pub requestor_block_hash: CryptoHash,
}

impl ChunksDelayTracker {
    pub fn received_block(&mut self, block: &Block, timestamp: Instant) {
        let block_hash = block.header().hash();
        let height = block.header().height();
        let chunks = block.chunks().iter().map(|it| it.chunk_hash()).collect_vec();

        self.blocks_in_progress.entry(*block_hash).or_insert(BlockInProgress {
            timestamp,
            height,
            chunks,
        });
    }

    pub fn received_chunk(&mut self, chunk_hash: &ChunkHash, timestamp: Instant) {
        self.chunks_in_progress
            .get_mut(&chunk_hash)
            .map(|chunk_in_progress| chunk_in_progress.chunk_received.get_or_insert(timestamp));
    }

    pub fn requested_chunk(
        &mut self,
        chunk_hash: &ChunkHash,
        timestamp: Instant,
        requestor_block_hash: &CryptoHash,
    ) {
        self.chunks_in_progress.entry(chunk_hash.clone()).or_insert_with(|| ChunkInProgress {
            chunk_requested: timestamp,
            chunk_received: None,
            requestor_block_hash: requestor_block_hash.clone(),
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
                                .saturating_duration_since(block_received.timestamp)
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
