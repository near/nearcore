use crate::{Chain, ChainStoreAccess, metrics};
use lru::LruCache;
use near_async::time::{Clock, Instant, Utc};
use near_chain_configs::BLOCK_HORIZON;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::{Block, ChunkType, Tip};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::views::{
    BlockProcessingInfo, BlockProcessingStatus, ChainProcessingInfo, ChunkProcessingInfo,
    ChunkProcessingStatus, DroppedReason,
};
use std::collections::{BTreeMap, HashMap, hash_map::Entry};
use std::mem;
use std::num::NonZeroUsize;
use time::ext::InstantExt as _;

const BLOCK_DELAY_TRACKING_COUNT: u64 = 50;

/// How many blocks the tracker keeps at one height. The window bounds which heights are
/// tracked, not how many competing forks are kept at each of them.
const MAX_TRACKED_BLOCKS_PER_HEIGHT: usize = 8;

/// A centralized place that records monitoring information about the important timestamps throughout
/// the lifetime of blocks and chunks. It keeps information of recent blocks and chunks, tracking
/// heights in `[head - BLOCK_DELAY_TRACKING_COUNT, head + BLOCK_HORIZON)`, and at most
/// `MAX_TRACKED_BLOCKS_PER_HEIGHT` blocks at each height.
/// A block is added the first time when chain tries to process the block.
pub struct BlocksDelayTracker {
    clock: Clock,
    // A block is added at the first time it was received, and
    // removed if it is too far from the chain head.
    blocks: HashMap<CryptoHash, BlockTrackingStats>,
    // Maps block height to block hash. Used for gc.
    // Theoretically, each block height should only have one block, if our block processing code
    // works correctly. We are storing a vector here just in case.
    blocks_height_map: BTreeMap<BlockHeight, Vec<CryptoHash>>,
    // Maps block height to the optimistic block stats.
    optimistic_blocks: LruCache<BlockHeight, OptimisticBlockStats>,
    // Chunks that belong to the blocks in the tracker
    chunks: HashMap<ChunkHash, ChunkTrackingStats>,
    // Chunks that we don't know which block it belongs to yet
    floating_chunks: HashMap<ChunkHash, BlockHeight>,
    head_height: BlockHeight,
}

#[derive(Debug, Clone)]
pub struct BlockTrackingStats {
    /// Timestamp when block was received or self-produced.
    pub received_timestamp: Instant,
    pub received_utc_timestamp: Utc,
    /// Timestamp when block was put to the orphan pool, if it ever was
    pub orphaned_timestamp: Option<Instant>,
    /// Timestamp when block was put to the missing chunks pool
    pub missing_chunks_timestamp: Option<Instant>,
    /// Timestamp when block was put to the pending execution pool
    pub pending_execution_timestamp: Option<Instant>,
    /// Timestamp when block was moved out of the orphan pool
    pub removed_from_orphan_timestamp: Option<Instant>,
    /// Timestamp when block was moved out of the missing chunks pool
    pub removed_from_missing_chunks_timestamp: Option<Instant>,
    /// Timestamp when block was moved out of the pending execution pool
    pub removed_from_pending_timestamp: Option<Instant>,
    /// Timestamp when block was done processing
    pub processed_timestamp: Option<Instant>,
    /// Whether the block is not processed because of different reasons
    pub dropped: Option<DroppedReason>,
    /// Stores the error message encountered during the processing of this block
    pub error: Option<String>,
    /// Only contains new chunks that belong to this block, if the block doesn't produce a new chunk
    /// for a shard, the corresponding item will be None.
    pub chunks: Vec<Option<ChunkHash>>,
}

#[derive(Debug, Clone, Default)]
pub struct OptimisticBlockStats {
    /// Timestamp when the optimistic block became ready for processing.
    pub ready_timestamp: Option<Instant>,
    /// Timestamp when the optimistic block was processed.
    pub processed_timestamp: Option<Instant>,
    /// Timestamp when the full block was received.
    pub full_block_received_timestamp: Option<Instant>,
}

impl OptimisticBlockStats {
    /// Observe metrics for the optimistic block.
    /// Discard timestamps for already observed ones.
    pub fn observe_metrics(&mut self) {
        let Some(full_block_received_timestamp) = self.full_block_received_timestamp else {
            return;
        };

        if let Some(ready_timestamp) = self.ready_timestamp {
            let delay = (full_block_received_timestamp.signed_duration_since(ready_timestamp))
                .as_seconds_f64();
            metrics::OPTIMISTIC_BLOCK_READINESS_GAP.observe(delay);
            self.ready_timestamp = None;
        }

        if let Some(processed_timestamp) = self.processed_timestamp {
            let delay = (full_block_received_timestamp.signed_duration_since(processed_timestamp))
                .as_seconds_f64();
            metrics::OPTIMISTIC_BLOCK_PROCESSED_GAP.observe(delay);
            self.processed_timestamp = None;
        }
    }
}

/// Records timestamps of requesting and receiving a chunk. Assumes that each chunk is requested
/// before it is received.
#[derive(Debug, Clone)]
pub struct ChunkTrackingStats {
    pub height_created: BlockHeight,
    pub shard_id: ShardId,
    pub prev_block_hash: CryptoHash,
    /// Timestamp of first time when we request for this chunk.
    pub requested_timestamp: Option<Utc>,
    /// Timestamp of when the node receives all information it needs for this chunk
    pub completed_timestamp: Option<Utc>,
}

impl ChunkTrackingStats {
    fn new(chunk_header: &ShardChunkHeader) -> Self {
        Self {
            height_created: chunk_header.height_created(),
            shard_id: chunk_header.shard_id(),
            prev_block_hash: *chunk_header.prev_block_hash(),
            requested_timestamp: None,
            completed_timestamp: None,
        }
    }

    fn to_chunk_processing_info(
        &self,
        chunk_hash: ChunkHash,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> ChunkProcessingInfo {
        let status = if self.completed_timestamp.is_some() {
            ChunkProcessingStatus::Completed
        } else if self.requested_timestamp.is_some() {
            ChunkProcessingStatus::Requested
        } else {
            ChunkProcessingStatus::NeedToRequest
        };
        let created_by = epoch_manager
            .get_chunk_producer_info_from_prev_block(&self.prev_block_hash, self.shard_id)
            .map(|info| info.take_account_id())
            .ok();
        let request_duration = if let Some(requested_timestamp) = self.requested_timestamp {
            if let Some(completed_timestamp) = self.completed_timestamp {
                Some((completed_timestamp - requested_timestamp).whole_milliseconds() as u64)
            } else {
                None
            }
        } else {
            None
        };
        ChunkProcessingInfo {
            chunk_hash,
            height_created: self.height_created,
            shard_id: self.shard_id,
            prev_block_hash: self.prev_block_hash,
            created_by,
            status,
            requested_timestamp: self.requested_timestamp,
            completed_timestamp: self.completed_timestamp,
            request_duration,
            chunk_parts_collection: vec![],
        }
    }
}

impl BlocksDelayTracker {
    pub fn new(clock: Clock, head_height: BlockHeight) -> Self {
        Self {
            clock,
            blocks: HashMap::new(),
            blocks_height_map: BTreeMap::new(),
            optimistic_blocks: LruCache::new(
                NonZeroUsize::new(BLOCK_DELAY_TRACKING_COUNT as usize).unwrap(),
            ),
            chunks: HashMap::new(),
            floating_chunks: HashMap::new(),
            head_height,
        }
    }

    fn is_height_at_capacity(&self, height: BlockHeight) -> bool {
        self.blocks_height_map
            .get(&height)
            .is_some_and(|hashes| hashes.len() >= MAX_TRACKED_BLOCKS_PER_HEIGHT)
    }

    /// Why the tracker will not record a block at this height, as a metric label.
    fn refusal_reason(&self, height: BlockHeight) -> Option<&'static str> {
        if height < self.head_height.saturating_sub(BLOCK_DELAY_TRACKING_COUNT) {
            return Some("below_window");
        }
        if height >= self.head_height.saturating_add(BLOCK_HORIZON) {
            return Some("above_window");
        }
        if self.is_height_at_capacity(height) {
            return Some("height_full");
        }
        None
    }

    pub fn mark_block_received(&mut self, block: &Block) {
        let block_hash = block.header().hash();
        let height = block.header().height();

        if let Some(reason) = self.refusal_reason(height) {
            metrics::BLOCKS_DELAY_TRACKER_REFUSED_BLOCKS.with_label_values(&[reason]).inc();
            return;
        }

        let Entry::Vacant(entry) = self.blocks.entry(*block_hash) else {
            return;
        };

        let chunks = block
            .chunks()
            .iter()
            .map(|chunk| match chunk {
                ChunkType::New(chunk) => {
                    let chunk_hash = chunk.chunk_hash();
                    self.chunks
                        .entry(chunk_hash.clone())
                        .or_insert_with(|| ChunkTrackingStats::new(chunk));
                    self.floating_chunks.remove(chunk_hash);
                    Some(chunk_hash.clone())
                }
                ChunkType::Old(_) => None,
            })
            .collect();
        entry.insert(BlockTrackingStats {
            received_timestamp: self.clock.now(),
            received_utc_timestamp: self.clock.now_utc(),
            orphaned_timestamp: None,
            missing_chunks_timestamp: None,
            pending_execution_timestamp: None,
            removed_from_orphan_timestamp: None,
            removed_from_missing_chunks_timestamp: None,
            removed_from_pending_timestamp: None,
            processed_timestamp: None,
            dropped: None,
            error: None,
            chunks,
        });
        self.blocks_height_map.entry(height).or_insert(vec![]).push(*block_hash);

        // Process optimistic block metrics
        let entry =
            self.optimistic_blocks.get_or_insert_mut(height, || OptimisticBlockStats::default());
        entry.full_block_received_timestamp = Some(self.clock.now());
        entry.observe_metrics();
    }

    /// A mark for a block the tracker does not hold. The tracker only records blocks near the
    /// head, so this is expected while the node is behind the tip.
    fn report_missing_entry(&self, block_hash: &CryptoHash, mark: &'static str) {
        metrics::BLOCKS_DELAY_TRACKER_MISSING_ENTRY.with_label_values(&[mark]).inc();
        tracing::debug!(target: "blocks_delay_tracker", ?block_hash, mark, "no tracking entry for block");
    }

    pub fn mark_block_dropped(&mut self, block_hash: &CryptoHash, reason: DroppedReason) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.dropped = Some(reason);
        } else {
            self.report_missing_entry(block_hash, "dropped");
        }
    }

    pub fn mark_block_errored(&mut self, block_hash: &CryptoHash, err: String) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.error = Some(err);
        } else {
            self.report_missing_entry(block_hash, "errored");
        }
    }

    pub fn mark_block_orphaned(&mut self, block_hash: &CryptoHash) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.orphaned_timestamp = Some(self.clock.now());
        } else {
            self.report_missing_entry(block_hash, "orphaned");
        }
    }

    pub fn mark_block_unorphaned(&mut self, block_hash: &CryptoHash) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.removed_from_orphan_timestamp = Some(self.clock.now());
        } else {
            self.report_missing_entry(block_hash, "unorphaned");
        }
    }

    pub fn mark_block_has_missing_chunks(&mut self, block_hash: &CryptoHash) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.missing_chunks_timestamp = Some(self.clock.now());
        } else {
            self.report_missing_entry(block_hash, "has_missing_chunks");
        }
    }

    pub fn mark_block_pending_execution(&mut self, block_hash: &CryptoHash) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.pending_execution_timestamp = Some(self.clock.now());
        } else {
            self.report_missing_entry(block_hash, "pending_execution");
        }
    }

    pub fn mark_block_completed_pending_execution(&mut self, block_hash: &CryptoHash) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.removed_from_pending_timestamp = Some(self.clock.now());
        } else {
            self.report_missing_entry(block_hash, "completed_pending_execution");
        }
    }

    pub fn mark_block_completed_missing_chunks(&mut self, block_hash: &CryptoHash) {
        if let Some(block_entry) = self.blocks.get_mut(block_hash) {
            block_entry.removed_from_missing_chunks_timestamp = Some(self.clock.now());
        } else {
            self.report_missing_entry(block_hash, "completed_missing_chunks");
        }
    }

    pub fn record_optimistic_block_ready(&mut self, block_height: BlockHeight) {
        let entry = self
            .optimistic_blocks
            .get_or_insert_mut(block_height, || OptimisticBlockStats::default());
        entry.ready_timestamp = Some(self.clock.now());
        entry.observe_metrics();
    }

    pub fn record_optimistic_block_processed(&mut self, block_height: BlockHeight) {
        let entry = self
            .optimistic_blocks
            .get_or_insert_mut(block_height, || OptimisticBlockStats::default());
        entry.processed_timestamp = Some(self.clock.now());
        entry.observe_metrics();
    }

    pub fn mark_chunk_completed(&mut self, chunk_header: &ShardChunkHeader) {
        let chunk_hash = chunk_header.chunk_hash();
        self.chunks
            .entry(chunk_hash.clone())
            .or_insert_with(|| {
                self.floating_chunks.insert(chunk_hash.clone(), chunk_header.height_created());
                ChunkTrackingStats::new(chunk_header)
            })
            .completed_timestamp
            .get_or_insert_with(|| self.clock.now_utc());
    }

    pub fn mark_chunk_requested(&mut self, chunk_header: &ShardChunkHeader) {
        let chunk_hash = chunk_header.chunk_hash();
        self.chunks
            .entry(chunk_hash.clone())
            .or_insert_with(|| {
                self.floating_chunks.insert(chunk_hash.clone(), chunk_header.height_created());
                ChunkTrackingStats::new(chunk_header)
            })
            .requested_timestamp
            .get_or_insert_with(|| self.clock.now_utc());
    }

    /// Moves the tracked window to a new head and reclaims what falls below it. Called after
    /// a block updates the head, and after state sync moves the head without processing one.
    pub(crate) fn update_head(&mut self, head_height: BlockHeight) {
        if head_height <= self.head_height {
            return;
        }
        let cutoff_height = head_height.saturating_sub(BLOCK_DELAY_TRACKING_COUNT);
        self.head_height = head_height;
        let mut blocks_to_remove = self.blocks_height_map.split_off(&cutoff_height);
        mem::swap(&mut self.blocks_height_map, &mut blocks_to_remove);

        for block_hash in blocks_to_remove.values().flatten() {
            if let Some(block) = self.blocks.remove(&block_hash) {
                for chunk_hash in block.chunks {
                    if let Some(chunk_hash) = chunk_hash {
                        self.chunks.remove(&chunk_hash);
                    }
                }
            } else {
                debug_assert!(false);
                tracing::error!(target: "block_delay_tracker", ?block_hash, "block in height map but not in blocks");
            }
        }

        self.floating_chunks.extract_if(|_, chunk_height| *chunk_height < cutoff_height).for_each(
            |(chunk_hash, _)| {
                self.chunks.remove(&chunk_hash);
            },
        );
        self.update_size_metrics();
    }

    pub fn finish_block_processing(
        &mut self,
        shard_layout: &ShardLayout,
        block_hash: &CryptoHash,
        new_head: Option<Tip>,
    ) {
        if let Some(processed_block) = self.blocks.get_mut(&block_hash) {
            processed_block.processed_timestamp = Some(self.clock.now());
        }
        // To get around the rust reference scope check
        if let Some(processed_block) = self.blocks.get(&block_hash) {
            let chunks = processed_block.chunks.clone();
            self.update_block_metrics(processed_block);
            for (shard_index, chunk_hash) in chunks.into_iter().enumerate() {
                if let Some(chunk_hash) = chunk_hash {
                    if let Some(processed_chunk) = self.chunks.get(&chunk_hash) {
                        let Ok(shard_id) = shard_layout.get_shard_id(shard_index) else {
                            tracing::error!(target: "block_delay_tracker", ?shard_index, "invalid shard index");
                            continue;
                        };
                        self.update_chunk_metrics(processed_chunk, shard_id);
                    }
                }
            }
        }
        if let Some(head) = new_head {
            self.update_head(head.height);
        }
        self.update_size_metrics();
    }

    fn update_size_metrics(&self) {
        let entries = &metrics::BLOCKS_DELAY_TRACKER_ENTRIES;
        entries.with_label_values(&["blocks"]).set(self.blocks.len() as i64);
        entries.with_label_values(&["chunks"]).set(self.chunks.len() as i64);
        entries.with_label_values(&["floating_chunks"]).set(self.floating_chunks.len() as i64);
    }

    fn update_block_metrics(&self, block: &BlockTrackingStats) {
        if let Some(start) = block.orphaned_timestamp {
            if let Some(end) = block.removed_from_orphan_timestamp {
                metrics::BLOCK_ORPHANED_DELAY
                    .observe((end.signed_duration_since(start)).as_seconds_f64().max(0.0));
            }
        } else {
            metrics::BLOCK_ORPHANED_DELAY.observe(0.);
        }
        if let Some(start) = block.missing_chunks_timestamp {
            if let Some(end) = block.removed_from_missing_chunks_timestamp {
                metrics::BLOCK_MISSING_CHUNKS_DELAY
                    .observe((end.signed_duration_since(start)).as_seconds_f64().max(0.0));
            }
        } else {
            metrics::BLOCK_MISSING_CHUNKS_DELAY.observe(0.);
        }
        if let Some(start) = block.pending_execution_timestamp {
            if let Some(end) = block.removed_from_pending_timestamp.or(block.processed_timestamp) {
                metrics::BLOCK_PENDING_EXECUTION_DELAY
                    .observe((end.signed_duration_since(start)).as_seconds_f64().max(0.0));
            }
        } else {
            metrics::BLOCK_PENDING_EXECUTION_DELAY.observe(0.);
        }
    }

    fn update_chunk_metrics(&self, chunk: &ChunkTrackingStats, shard_id: ShardId) {
        if let Some(chunk_requested) = chunk.requested_timestamp {
            // Theoretically chunk_received should have been set here because a block being processed
            // requires all chunks to be received
            if let Some(chunk_received) = chunk.completed_timestamp {
                metrics::CHUNK_RECEIVED_DELAY
                    .with_label_values(&[&shard_id.to_string()])
                    .observe((chunk_received - chunk_requested).as_seconds_f64());
            }
        }
    }

    fn get_block_processing_info(
        &self,
        block_height: BlockHeight,
        block_hash: &CryptoHash,
        chain: &Chain,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Option<BlockProcessingInfo> {
        self.blocks.get(block_hash).map(|block_stats| {
            let chunks_info: Vec<_> = block_stats
                .chunks
                .iter()
                .map(|chunk_hash| {
                    if let Some(chunk_hash) = chunk_hash {
                        self.chunks
                            .get(chunk_hash)
                            .map(|x| x.to_chunk_processing_info(chunk_hash.clone(), epoch_manager))
                    } else {
                        None
                    }
                })
                .collect();
            let now = self.clock.now();
            let block_status = chain.get_block_status(block_hash, block_stats);
            let in_progress_ms = (block_stats
                .processed_timestamp
                .unwrap_or(now)
                .signed_duration_since(block_stats.received_timestamp))
            .whole_milliseconds()
            .max(0) as u128;
            let orphaned_ms = if let Some(orphaned_time) = block_stats.orphaned_timestamp {
                Some(
                    (block_stats
                        .removed_from_orphan_timestamp
                        .unwrap_or(now)
                        .signed_duration_since(orphaned_time))
                    .whole_milliseconds()
                    .max(0) as u128,
                )
            } else {
                None
            };
            let missing_chunks_ms =
                if let Some(missing_chunks_time) = block_stats.missing_chunks_timestamp {
                    Some(
                        (block_stats
                            .removed_from_missing_chunks_timestamp
                            .unwrap_or(now)
                            .signed_duration_since(missing_chunks_time))
                        .whole_milliseconds()
                        .max(0) as u128,
                    )
                } else {
                    None
                };
            BlockProcessingInfo {
                height: block_height,
                hash: *block_hash,
                received_timestamp: block_stats.received_utc_timestamp,
                in_progress_ms,
                orphaned_ms,
                block_status,
                missing_chunks_ms,
                chunks_info,
            }
        })
    }
}

impl Chain {
    fn get_block_status(
        &self,
        block_hash: &CryptoHash,
        block_info: &BlockTrackingStats,
    ) -> BlockProcessingStatus {
        if self.is_orphan(block_hash) {
            return BlockProcessingStatus::Orphan;
        }
        if self.is_chunk_orphan(block_hash) {
            return BlockProcessingStatus::WaitingForChunks;
        }
        if self.is_in_processing(block_hash) {
            return BlockProcessingStatus::InProcessing;
        }
        if self.chain_store().block_exists(block_hash) {
            return BlockProcessingStatus::Accepted;
        }
        if let Some(dropped_reason) = &block_info.dropped {
            return BlockProcessingStatus::Dropped(dropped_reason.clone());
        }
        if let Some(error) = &block_info.error {
            return BlockProcessingStatus::Error(error.clone());
        }
        return BlockProcessingStatus::Unknown;
    }

    pub fn get_chain_processing_info(&self) -> ChainProcessingInfo {
        let blocks_info: Vec<_> = self
            .blocks_delay_tracker
            .blocks_height_map
            .iter()
            .rev()
            .flat_map(|(height, hashes)| {
                hashes
                    .iter()
                    .filter_map(|hash| {
                        self.blocks_delay_tracker.get_block_processing_info(
                            *height,
                            hash,
                            self,
                            self.epoch_manager.as_ref(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut floating_chunks_info = self
            .blocks_delay_tracker
            .floating_chunks
            .iter()
            .filter_map(|(chunk_hash, _)| {
                self.blocks_delay_tracker.chunks.get(chunk_hash).map(|chunk_stats| {
                    chunk_stats
                        .to_chunk_processing_info(chunk_hash.clone(), self.epoch_manager.as_ref())
                })
            })
            .collect::<Vec<_>>();
        floating_chunks_info.sort_by(|chunk1, chunk2| {
            (chunk1.height_created, chunk1.shard_id)
                .partial_cmp(&(chunk2.height_created, chunk2.shard_id))
                .unwrap()
        });
        ChainProcessingInfo {
            num_blocks_in_processing: self.blocks_in_processing_len(),
            num_orphans: self.orphans_len(),
            num_blocks_missing_chunks: self.blocks_with_missing_chunks_len(),
            blocks_info,
            floating_chunks_info,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BLOCK_DELAY_TRACKING_COUNT, BLOCK_HORIZON, BlocksDelayTracker,
        MAX_TRACKED_BLOCKS_PER_HEIGHT,
    };
    use near_async::time::{Clock, FakeClock, Utc};
    use near_primitives::block::Block;
    use near_primitives::genesis::genesis_block;
    use near_primitives::hash::hash;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::{Balance, BlockHeight};
    use near_primitives::version::PROTOCOL_VERSION;
    use std::sync::Arc;

    const HEAD: BlockHeight = 1_000;

    fn block_at(clock: &Clock, height: BlockHeight) -> Arc<Block> {
        distinct_blocks_at(clock, height, 1).pop().unwrap()
    }

    fn distinct_blocks_at(clock: &Clock, height: BlockHeight, count: usize) -> Vec<Arc<Block>> {
        let signer = Arc::new(create_test_signer("test"));
        let genesis = Arc::new(genesis_block(
            PROTOCOL_VERSION,
            vec![],
            Utc::from_unix_timestamp(0).unwrap(),
            0,
            Balance::ZERO,
            Balance::ZERO,
            &vec![],
        ));
        (0..count)
            .map(|nonce| {
                TestBlockBuilder::from_prev_block(clock.clone(), &genesis, signer.clone())
                    .height(height)
                    .block_merkle_root(hash(&(nonce as u64).to_le_bytes()))
                    .build()
            })
            .collect()
    }

    #[test]
    fn blocks_outside_the_window_are_not_tracked() {
        let clock = FakeClock::default().clock();
        let mut tracker = BlocksDelayTracker::new(clock.clone(), HEAD);

        let within = block_at(&clock, HEAD + BLOCK_HORIZON - 1);
        let above = block_at(&clock, HEAD + BLOCK_HORIZON);
        let below = block_at(&clock, HEAD - BLOCK_DELAY_TRACKING_COUNT - 1);

        for block in [&within, &above, &below] {
            tracker.mark_block_received(block);
        }

        assert_eq!(tracker.blocks.len(), 1);
        assert!(tracker.blocks.contains_key(within.hash()));
        assert_eq!(tracker.blocks_height_map.len(), 1);
    }

    #[test]
    fn blocks_per_height_are_capped() {
        let clock = FakeClock::default().clock();
        let mut tracker = BlocksDelayTracker::new(clock.clone(), HEAD);

        let blocks = distinct_blocks_at(&clock, HEAD, MAX_TRACKED_BLOCKS_PER_HEIGHT + 5);
        for block in &blocks {
            tracker.mark_block_received(block);
        }

        assert_eq!(tracker.blocks.len(), MAX_TRACKED_BLOCKS_PER_HEIGHT);
        assert_eq!(tracker.blocks_height_map[&HEAD].len(), MAX_TRACKED_BLOCKS_PER_HEIGHT);
        assert_eq!(tracker.refusal_reason(HEAD), Some("height_full"));
    }

    #[test]
    fn advancing_the_head_moves_the_window() {
        let clock = FakeClock::default().clock();
        let mut tracker = BlocksDelayTracker::new(clock.clone(), HEAD);
        let synced_head = HEAD + 1_200_000;

        let at_old_head = block_at(&clock, HEAD);
        let at_new_head = block_at(&clock, synced_head);
        tracker.mark_block_received(&at_old_head);
        tracker.mark_block_received(&at_new_head);
        assert_eq!(tracker.blocks.len(), 1);

        tracker.update_head(synced_head);
        tracker.mark_block_received(&at_new_head);

        assert!(tracker.blocks.contains_key(at_new_head.hash()));
        assert!(!tracker.blocks.contains_key(at_old_head.hash()));
        assert_eq!(tracker.blocks_height_map.len(), 1);
    }
}
