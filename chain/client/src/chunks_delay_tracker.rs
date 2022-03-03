use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use near_primitives::types::{BlockHeight, ShardId};

use crate::metrics;

/// Provides monitoring information about the delays of receiving blocks and their corresponding chunks.
/// Keeps timestamps of a limited number of blocks before the current head.
/// Keeps timestamps of all blocks and chunks from the future.
/// Doesn't make any assumptions about shard layout or the number of shards. If the head was moved forward, we assume that all requirements were satisfied.
/// This object provides two metrics:
/// 1) max delay between discovering a block and receiving the last chunk from that block.
/// 2) the difference between the latest known block and the current head.
/// If a chunk or a block is received multiple times, only the first time is recorded.
#[derive(Debug, Default)]
pub(crate) struct ChunksDelayTracker {
    heights: BTreeMap<BlockHeight, HeightInfo>,
}

#[derive(Debug, Default)]
struct HeightInfo {
    block_received: Option<Instant>,
    chunks_received: HashMap<ShardId, Instant>,
    chunks_requested: HashMap<ShardId, Instant>,
}

impl ChunksDelayTracker {
    fn remove_old_entries(&mut self, head_height: BlockHeight) {
        let heights_to_drop: Vec<BlockHeight> =
            self.heights.range(..head_height).map(|(&k, _v)| k).collect();
        for height in heights_to_drop {
            self.update_block_chunks_metric(height);
            self.update_chunks_metric(height);
            self.heights.remove(&height);
        }
    }

    fn update_block_chunks_metric(&mut self, height: BlockHeight) {
        if let Some(entry) = self.heights.get(&height) {
            if let Some(block_received) = entry.block_received {
                for (shard_id, received) in &entry.chunks_received {
                    metrics::BLOCK_CHUNKS_DELAY
                        .with_label_values(&[&format!("{}", shard_id)])
                        .observe(received.saturating_duration_since(block_received).as_secs_f64());
                }
            }
        }
    }

    fn update_chunks_metric(&mut self, height: BlockHeight) {
        if let Some(entry) = self.heights.get(&height) {
            for (shard_id, requested) in &entry.chunks_requested {
                if let Some(received) = entry.chunks_received.get(&shard_id) {
                    metrics::CHUNK_DELAY
                        .with_label_values(&[&format!("{}", shard_id)])
                        .observe(received.saturating_duration_since(*requested).as_secs_f64());
                }
            }
        }
    }

    pub fn add_block_timestamp(&mut self, height: BlockHeight, timestamp: Instant) {
        self.heights.entry(height).or_default().block_received.get_or_insert(timestamp);
    }

    pub fn add_chunk_timestamp(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
        timestamp: Instant,
    ) {
        self.heights
            .entry(height)
            .or_default()
            .chunks_received
            .entry(shard_id)
            .or_insert(timestamp);
    }

    pub fn processed_block(&mut self, height: BlockHeight) {
        self.remove_old_entries(height);
    }

    pub fn requested_chunk(&mut self, height: BlockHeight, shard_id: ShardId, timestamp: Instant) {
        self.heights
            .entry(height)
            .or_default()
            .chunks_requested
            .entry(shard_id)
            .or_insert(timestamp);
    }
}
