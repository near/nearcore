use itertools::Itertools;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::time::Instant;
use tracing::warn;

use crate::metrics;

/// Provides monitoring information about the important timestamps in throughout the lifetime of
/// blocks and chunks. It keeps information of all pending blocks and chunks that has not been fully processed yet.
/// Blocks are added the first time it is received and removed when it finishes processing
/// Chunks are added the first time it was requested and removed when the block it belongs finishes processing
#[derive(Debug, Default)]
pub struct BlocksDelayTracker {
    /// Timestamps of the first attempt of block processing.
    /// Contains only blocks that are not yet processed.
    pub blocks_in_progress: HashMap<CryptoHash, BlockInProgress>,
    /// An entry gets created when a chunk gets requested for the first time.
    /// Chunks get deleted when the block gets processed.
    pub chunks_in_progress: HashMap<ChunkHash, ChunkInProgress>,
}

#[derive(Debug)]
pub struct BlockInProgress {
    /// Timestamp when block was received.
    pub received_timestamp: Instant,
    /// Timestamp when block was put to the orphan pool, if it ever was
    pub orphaned_timestamp: Option<Instant>,
    /// Timestamp when block was put to the missing chunks pool
    pub missing_chunks_timestamp: Option<Instant>,
    /// Timestamp when block was moved out of the orphan pool
    pub removed_from_orphan_timestamp: Option<Instant>,
    /// Timestamp when block was moved out of the missing chunks pool
    pub removed_from_missing_chunks_timestamp: Option<Instant>,
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
    /// Block hash this chunk belongs to.
    pub block_hash: CryptoHash,
}

impl BlocksDelayTracker {
    pub fn mark_block_received(&mut self, block: &Block, timestamp: Instant) {
        let block_hash = block.header().hash();
        let height = block.header().height();
        let chunks = block.chunks().iter().map(|it| it.chunk_hash()).collect_vec();

        self.blocks_in_progress.entry(*block_hash).or_insert(BlockInProgress {
            received_timestamp: timestamp,
            orphaned_timestamp: None,
            missing_chunks_timestamp: None,
            removed_from_orphan_timestamp: None,
            removed_from_missing_chunks_timestamp: None,
            height,
            chunks,
        });
    }

    pub fn mark_block_orphaned(&mut self, block_hash: &CryptoHash, timestamp: Instant) {
        if let Some(block_entry) = self.blocks_in_progress.get_mut(block_hash) {
            block_entry.orphaned_timestamp = Some(timestamp);
        } else {
            debug_assert!(false);
            warn!(target:"blocks_delay_trakcer", "block {:?} was orphaned but was not marked received", block_hash);
        }
    }

    pub fn mark_block_unorphaned(&mut self, block_hash: &CryptoHash, timestamp: Instant) {
        if let Some(block_entry) = self.blocks_in_progress.get_mut(block_hash) {
            block_entry.removed_from_orphan_timestamp = Some(timestamp);
        } else {
            debug_assert!(false);
            warn!(target:"blocks_delay_trakcer", "block {:?} was unorphaned but was not marked received", block_hash);
        }
    }

    pub fn mark_block_has_missing_chunks(&mut self, block_hash: &CryptoHash, timestamp: Instant) {
        if let Some(block_entry) = self.blocks_in_progress.get_mut(block_hash) {
            block_entry.missing_chunks_timestamp = Some(timestamp);
        } else {
            debug_assert!(false);
            warn!(target:"blocks_delay_trakcer", "block {:?} was marked as having missing chunks but was not marked received", block_hash);
        }
    }

    pub fn mark_block_completed_missing_chunks(
        &mut self,
        block_hash: &CryptoHash,
        timestamp: Instant,
    ) {
        if let Some(block_entry) = self.blocks_in_progress.get_mut(block_hash) {
            block_entry.removed_from_missing_chunks_timestamp = Some(timestamp);
        } else {
            debug_assert!(false);
            warn!(target:"blocks_delay_trakcer", "block {:?} was marked as having no missing chunks but was not marked received", block_hash);
        }
    }

    pub fn mark_chunk_received(&mut self, chunk_hash: &ChunkHash, timestamp: Instant) {
        self.chunks_in_progress
            .get_mut(&chunk_hash)
            .map(|chunk_in_progress| chunk_in_progress.chunk_received.get_or_insert(timestamp));
    }

    pub fn mark_chunk_requested(
        &mut self,
        chunk_hash: &ChunkHash,
        timestamp: Instant,
        requestor_block_hash: &CryptoHash,
    ) {
        self.chunks_in_progress.entry(chunk_hash.clone()).or_insert_with(|| ChunkInProgress {
            chunk_requested: timestamp,
            chunk_received: None,
            block_hash: requestor_block_hash.clone(),
        });
    }

    pub fn finish_block_processing(
        &mut self,
        processed_block_hash: &CryptoHash,
        chunks: &[ChunkHash],
    ) {
        self.update_various_block_metrics(processed_block_hash);
        self.update_block_chunks_requested_metric(processed_block_hash, &chunks);
        self.update_chunks_metric(&chunks);

        self.blocks_in_progress.remove(&processed_block_hash);
        for chunk_hash in chunks {
            self.chunks_in_progress.remove(chunk_hash);
        }
    }

    fn update_various_block_metrics(&mut self, block_hash: &CryptoHash) {
        if let Some(block) = self.blocks_in_progress.get(&block_hash) {
            if let Some(start) = block.orphaned_timestamp {
                if let Some(end) = block.removed_from_orphan_timestamp {
                    metrics::BLOCK_ORPHANED_DELAY
                        .observe(end.saturating_duration_since(start).as_secs_f64());
                }
            } else {
                metrics::BLOCK_ORPHANED_DELAY.observe(0.);
            }
            if let Some(start) = block.missing_chunks_timestamp {
                if let Some(end) = block.removed_from_missing_chunks_timestamp {
                    metrics::BLOCK_MISSING_CHUNKS_DELAY
                        .observe(end.saturating_duration_since(start).as_secs_f64());
                }
            } else {
                metrics::BLOCK_MISSING_CHUNKS_DELAY.observe(0.);
            }
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
                                .saturating_duration_since(block_received.received_timestamp)
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
