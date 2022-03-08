use near_primitives::hash::CryptoHash;
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;
use tracing::warn;

use near_primitives::types::ShardId;

use crate::metrics;

/// Provides monitoring information about the delays of receiving and requesting blocks and their corresponding chunks.
/// Keeps timestamps for all future blocks and chunks that were requested or received.
/// Updates the metrics when the head gets updated. Assumes that if the head moves forward, all needed chunks were received.
/// If a chunk or a block is received or requested multiple times, only the first time is recorded.
#[derive(Debug, Default)]
pub(crate) struct ChunksDelayTracker {
    blocks_in_progress: BTreeMap<CryptoHash, BlockInProgress>,
}

#[derive(Debug, Default)]
struct BlockInProgress {
    /// Timestamp of trying to process a block for the first time.
    block_received: Option<Instant>,
    /// Timestamps of receiving each of the shards in a block.
    chunks_received: HashMap<ShardId, Instant>,
    /// Timestamps of requesting missing chunks.
    /// Chunks are usually requested when a client falls out-of-sync with the head of the chain.
    chunks_requested: HashMap<ShardId, Instant>,
}

impl ChunksDelayTracker {
    fn update_block_chunks_metric(&mut self, block_hash: &CryptoHash) {
        if let Some(entry) = self.blocks_in_progress.get(&block_hash) {
            if let Some(block_received) = entry.block_received {
                for (shard_id, received) in &entry.chunks_received {
                    metrics::BLOCK_CHUNKS_DELAY
                        .with_label_values(&[&format!("{}", shard_id)])
                        .observe(received.saturating_duration_since(block_received).as_secs_f64());
                }
            }
        }
    }

    fn update_chunks_metric(&mut self, block_hash: &CryptoHash) {
        if let Some(entry) = self.blocks_in_progress.get(block_hash) {
            for (shard_id, requested) in &entry.chunks_requested {
                if let Some(received) = entry.chunks_received.get(&shard_id) {
                    metrics::CHUNK_DELAY
                        .with_label_values(&[&format!("{}", shard_id)])
                        .observe(received.saturating_duration_since(*requested).as_secs_f64());
                }
            }
        }
    }

    pub fn add_block_timestamp(&mut self, prev_block_hash: &CryptoHash, timestamp: Instant) {
        self.blocks_in_progress
            .entry(*prev_block_hash)
            .or_default()
            .block_received
            .get_or_insert(timestamp);

        self.check_num_blocks_in_progress();
    }

    pub fn add_chunk_timestamp(
        &mut self,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
        timestamp: Instant,
    ) {
        self.blocks_in_progress
            .entry(*prev_block_hash)
            .or_default()
            .chunks_received
            .entry(shard_id)
            .or_insert(timestamp);

        self.check_num_blocks_in_progress();
    }

    pub fn requested_chunk(
        &mut self,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
        timestamp: Instant,
    ) {
        self.blocks_in_progress
            .entry(*prev_block_hash)
            .or_default()
            .chunks_requested
            .entry(shard_id)
            .or_insert(timestamp);
    }

    pub fn finish_block_processing(&mut self, processed_block_hash: &CryptoHash) {
        println!(
            "finish_block_processing: {:?}:\n{:#?}",
            processed_block_hash, self.blocks_in_progress
        );
        self.update_block_chunks_metric(processed_block_hash);
        self.update_chunks_metric(processed_block_hash);
        self.blocks_in_progress.remove(processed_block_hash);
    }

    /// Ensures that ChunksDelayTracker doesn't leak memory due to forks or skipped blocks.
    fn check_num_blocks_in_progress(&mut self) {
        const MAX_NUM_BLOCKS_IN_PROGRESS: usize = 10_000;
        if self.blocks_in_progress.len() > MAX_NUM_BLOCKS_IN_PROGRESS {
            warn!(target: "client", "Cleaning up tracked blocks in progress");
            self.blocks_in_progress.clear();
        }
    }
}
