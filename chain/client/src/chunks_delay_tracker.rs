use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use near_primitives::types::{BlockHeight, ShardId};

use crate::metrics;
use std::cmp::max;
use std::ops::{RangeTo, RangeToInclusive};

/// Provides monitoring information about the delays of receiving blocks and their corresponding chunks.
/// Keeps timestamps of a limited number of blocks before the current head.
/// Keeps timestamps of all blocks and chunks from the future.
/// Doesn't make any assumptions about shard layout or the number of shards. If the head was moved forward, we assume that all requirements were satisfied.
/// This object provides two metrics:
/// 1) max delay between discovering a block and receiving the last chunk from that block.
/// 2) number of blocks with missing chunks.
/// If a chunk or a block is received multiple times, only the first time is recorded.
#[derive(Debug, Default)]
pub(crate) struct ChunksDelayTracker {
    heights: BTreeMap<BlockHeight, HeightInfo>,
    // Used as an optimization to delete old blocks only when the head is updated.
    prev_head_height: BlockHeight,
}

const CHUNKS_DELAY_TRACKER_HORIZON: u64 = 10;

#[derive(Debug, Default)]
struct HeightInfo {
    block_received: Option<Instant>,
    chunks_received: HashMap<ShardId, Instant>,
}

impl ChunksDelayTracker {
    // Blocks below this heights are not tracked.
    fn lowest_height(head_height: BlockHeight) -> BlockHeight {
        head_height.saturating_sub(CHUNKS_DELAY_TRACKER_HORIZON)
    }

    fn remove_old_entries(&mut self, head_height: BlockHeight) {
        let mut heights_to_drop = vec![];
        for (&k, _) in
            self.heights.range(RangeTo { end: ChunksDelayTracker::lowest_height(head_height) })
        {
            heights_to_drop.push(k);
        }
        for h in heights_to_drop {
            self.heights.remove(&h);
        }
    }

    fn update_chunks_metric(&mut self, head_height: BlockHeight) {
        if head_height == self.prev_head_height {
            return;
        }
        let mut max_delay = 0;
        for (_, v) in self.heights.range(RangeToInclusive { end: head_height }) {
            if let Some(block_received) = v.block_received {
                if let Some(latest_chunk) = v.chunks_received.values().max() {
                    if latest_chunk > &block_received {
                        let delay = latest_chunk.duration_since(block_received).as_micros();
                        max_delay = max(max_delay, delay);
                    }
                }
            }
        }
        near_metrics::set_gauge(&metrics::CHUNKS_RECEIVING_DELAY_US, max_delay as i64);
        self.prev_head_height = head_height;
    }

    fn update_blocks_missing_chunks_metric(&mut self, head_height: BlockHeight) {
        let value = if let Some((&latest, _)) = self.heights.iter().rev().next() {
            latest.saturating_sub(head_height)
        } else {
            0
        };
        near_metrics::set_gauge(&metrics::BLOCKS_AHEAD_OF_HEAD, value as i64);
    }

    fn update_metrics(&mut self, head_height: BlockHeight) {
        self.update_chunks_metric(head_height);
        self.update_blocks_missing_chunks_metric(head_height);
    }

    pub fn add_block_timestamp(
        &mut self,
        height: BlockHeight,
        head_height: BlockHeight,
        timestamp: Instant,
    ) {
        self.remove_old_entries(head_height);
        if height >= head_height {
            self.heights
                .entry(height)
                .and_modify(|info| {
                    if info.block_received.is_none() {
                        info.block_received = Some(timestamp)
                    }
                })
                .or_insert_with(|| HeightInfo {
                    block_received: Some(timestamp),
                    chunks_received: Default::default(),
                });
        }
        self.update_metrics(head_height);
    }

    pub fn add_chunk_timestamp(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
        head_height: BlockHeight,
        timestamp: Instant,
    ) {
        self.remove_old_entries(head_height);
        if height >= head_height {
            self.heights
                .entry(height)
                .or_default()
                .chunks_received
                .entry(shard_id)
                .or_insert(timestamp);
        }
        self.update_metrics(head_height);
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_blocks_ahead_of_head() {
        let mut tracker = ChunksDelayTracker::default();
        tracker.add_block_timestamp(3, 1, Instant::now());
        assert_eq!(near_metrics::get_gauge(&metrics::BLOCKS_AHEAD_OF_HEAD), Ok(2));
        tracker.add_block_timestamp(4, 1, Instant::now());
        assert_eq!(near_metrics::get_gauge(&metrics::BLOCKS_AHEAD_OF_HEAD), Ok(3));
        tracker.add_block_timestamp(4, 3, Instant::now());
        assert_eq!(near_metrics::get_gauge(&metrics::BLOCKS_AHEAD_OF_HEAD), Ok(1));
        tracker.add_block_timestamp(4, 5, Instant::now());
        assert_eq!(near_metrics::get_gauge(&metrics::BLOCKS_AHEAD_OF_HEAD), Ok(0));
        tracker.add_block_timestamp(4, 999, Instant::now());
        assert_eq!(near_metrics::get_gauge(&metrics::BLOCKS_AHEAD_OF_HEAD), Ok(0));
    }

    #[test]
    fn test_timestamps() {
        let start = Instant::now();
        let mut tracker = ChunksDelayTracker::default();

        // Multiple chunks.
        tracker.add_block_timestamp(3, 1, start);
        tracker.add_chunk_timestamp(3, 0, 1, start + Duration::from_secs(1));
        tracker.add_chunk_timestamp(3, 1, 1, start + Duration::from_secs(2));

        // No chunks.
        tracker.add_block_timestamp(4, 3, start + Duration::from_secs(5));

        // Block and chunk from the past.
        tracker.add_block_timestamp(1, 4, start + Duration::from_secs(7));
        tracker.add_chunk_timestamp(1, 0, 4, start + Duration::from_secs(7));

        assert_eq!(
            near_metrics::get_gauge(&metrics::CHUNKS_RECEIVING_DELAY_US),
            Ok(Duration::from_secs(2).as_micros() as i64)
        );

        // New block with a smaller delay between receiving chunks.
        tracker.add_block_timestamp(5, 4, start + Duration::from_secs(10));
        tracker.add_chunk_timestamp(5, 0, 5, start + Duration::from_secs(11));
        assert_eq!(
            near_metrics::get_gauge(&metrics::CHUNKS_RECEIVING_DELAY_US),
            Ok(Duration::from_secs(2).as_micros() as i64)
        );

        // New block with a smaller delay between receiving chunks but far in the future, the old block should be forgotten.
        tracker.add_block_timestamp(105, 4, start + Duration::from_secs(20));
        tracker.add_chunk_timestamp(105, 0, 105, start + Duration::from_secs(21));
        assert_eq!(
            near_metrics::get_gauge(&metrics::CHUNKS_RECEIVING_DELAY_US),
            Ok(Duration::from_secs(1).as_micros() as i64)
        );

        // New block with a larger delay between receiving chunks.
        tracker.add_block_timestamp(106, 105, start + Duration::from_secs(23));
        tracker.add_chunk_timestamp(106, 0, 106, start + Duration::from_secs(28));
        assert_eq!(
            near_metrics::get_gauge(&metrics::CHUNKS_RECEIVING_DELAY_US),
            Ok(Duration::from_secs(5).as_micros() as i64)
        );

        // A block in the future doesn't matter for the metric.
        tracker.add_block_timestamp(206, 106, start + Duration::from_secs(100));
        tracker.add_chunk_timestamp(206, 0, 106, start + Duration::from_secs(999));
        assert_eq!(
            near_metrics::get_gauge(&metrics::CHUNKS_RECEIVING_DELAY_US),
            Ok(Duration::from_secs(5).as_micros() as i64)
        );
    }
}
