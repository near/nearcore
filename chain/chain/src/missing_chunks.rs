use near_primitives::hash::CryptoHash;
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::BlockHeight;
use std::cmp::Ordering;
use std::collections::{
    BinaryHeap, HashSet,
    btree_map::{self, BTreeMap},
    hash_map::{self, HashMap},
};
use tracing::{debug, warn};

type BlockHash = CryptoHash;

const MAX_BLOCKS_MISSING_CHUNKS: usize = 1024;

pub trait BlockLike {
    fn hash(&self) -> BlockHash;
    fn height(&self) -> BlockHeight;
}

#[derive(Debug)]
struct HeightOrdered<T>(T);

impl<T: BlockLike> PartialEq for HeightOrdered<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.height() == other.0.height()
    }
}
impl<T: BlockLike> Eq for HeightOrdered<T> {}
impl<T: BlockLike> PartialOrd for HeightOrdered<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.height().cmp(&other.0.height()))
    }
}
impl<T: BlockLike> Ord for HeightOrdered<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.height().cmp(&other.0.height())
    }
}

/// Structure for keeping track of missing chunks.
/// The reason to have a Block type parameter instead of using the
/// `block::Block` type is to make testing easier (`block::Block` is a complex structure and I
/// don't care about most of it).
#[derive(Debug, Default)]
pub struct MissingChunksPool<Block: BlockLike> {
    missing_chunks: HashMap<ChunkHash, HashSet<BlockHash>>,
    blocks_missing_chunks: HashMap<BlockHash, HashSet<ChunkHash>>,
    blocks_waiting_for_chunks: HashMap<BlockHash, Block>,
    blocks_ready_to_process: BinaryHeap<HeightOrdered<Block>>,
    height_idx: BTreeMap<BlockHeight, HashSet<BlockHash>>,
}

impl<Block: BlockLike> MissingChunksPool<Block> {
    pub fn new() -> Self {
        Self {
            missing_chunks: Default::default(),
            blocks_missing_chunks: Default::default(),
            blocks_waiting_for_chunks: Default::default(),
            blocks_ready_to_process: BinaryHeap::new(),
            height_idx: Default::default(),
        }
    }

    pub fn contains(&self, block_hash: &BlockHash) -> bool {
        self.blocks_waiting_for_chunks.contains_key(block_hash)
    }

    pub fn get(&self, block_hash: &BlockHash) -> Option<&Block> {
        self.blocks_waiting_for_chunks.get(block_hash)
    }

    pub fn len(&self) -> usize {
        self.blocks_waiting_for_chunks.len()
    }

    pub fn ready_blocks(&mut self) -> Vec<Block> {
        if self.blocks_ready_to_process.is_empty() {
            return Vec::new();
        }
        let heap = std::mem::replace(&mut self.blocks_ready_to_process, BinaryHeap::new());
        heap.into_sorted_vec().into_iter().map(|x| x.0).collect()
    }

    pub fn add_block_with_missing_chunks(&mut self, block: Block, missing_chunks: Vec<ChunkHash>) {
        let block_hash = block.hash();
        // This case can only happen when missing chunks are not being eventually received and
        // thus removing blocks from the HashMap. It means the this node has severely stalled out.
        // It is ok to ignore further blocks because either (a) we will start receiving chunks
        // again, work through the backlog of the pool, then naturally sync the later blocks
        // which were not added initially, or (b) someone will restart the node because something
        // has gone horribly wrong, in which case these HashMaps will be lost anyways.
        if self.blocks_missing_chunks.len() >= MAX_BLOCKS_MISSING_CHUNKS {
            warn!(target: "chunks", "Not recording block with hash {} even though it is missing chunks. The missing chunks pool is full.", block_hash);
            return;
        }

        for chunk_hash in missing_chunks.iter().cloned() {
            let blocks_for_chunk =
                self.missing_chunks.entry(chunk_hash).or_insert_with(HashSet::new);
            blocks_for_chunk.insert(block_hash);
        }

        // Convert to HashSet
        let missing_chunks = missing_chunks.into_iter().collect();
        match self.blocks_missing_chunks.entry(block_hash) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(missing_chunks);
            }
            // The Occupied case should never happen since we know
            // all the missing chunks for a block the first time we receive it,
            // and we should not call `add_block_with_missing_chunks` again after
            // we know a block is missing chunks.
            hash_map::Entry::Occupied(mut entry) => {
                let previous_chunks = entry.insert(missing_chunks);
                warn!(target: "chunks", "Block with hash {} was already missing chunks {:?}.", block_hash, previous_chunks);
            }
        }

        let height = block.height();
        let blocks_at_height = self.height_idx.entry(height).or_insert_with(HashSet::new);
        blocks_at_height.insert(block_hash);
        self.blocks_waiting_for_chunks.insert(block_hash, block);
    }

    pub fn accept_chunk(&mut self, chunk_hash: &ChunkHash) {
        let block_hashes = self.missing_chunks.remove(chunk_hash).unwrap_or_else(HashSet::new);
        debug!(target: "chunks", ?chunk_hash, "Chunk accepted, {} blocks were waiting for it.", block_hashes.len());
        for block_hash in block_hashes {
            match self.blocks_missing_chunks.entry(block_hash) {
                hash_map::Entry::Occupied(mut missing_chunks_entry) => {
                    let missing_chunks = missing_chunks_entry.get_mut();
                    missing_chunks.remove(chunk_hash);
                    if missing_chunks.is_empty() {
                        // No more missing chunks!
                        missing_chunks_entry.remove_entry();
                        debug!(target: "chunks", %block_hash, "Block is ready - last chunk received.");
                        self.mark_block_as_ready(&block_hash);
                    } else {
                        debug!(target: "chunks", %block_hash, "Block is still waiting for {} chunks.", missing_chunks.len());
                    }
                }
                hash_map::Entry::Vacant(_) => {
                    warn!(target: "chunks", "Invalid MissingChunksPool state. Block with hash {} was still a value of the missing_chunks map, but not present in the blocks_missing_chunks map", block_hash);
                    self.mark_block_as_ready(&block_hash);
                }
            }
        }
    }

    fn mark_block_as_ready(&mut self, block_hash: &BlockHash) {
        if let Some(block) = self.blocks_waiting_for_chunks.remove(block_hash) {
            let height = block.height();
            if let btree_map::Entry::Occupied(mut entry) = self.height_idx.entry(height) {
                let blocks_at_height = entry.get_mut();
                blocks_at_height.remove(block_hash);
                if blocks_at_height.is_empty() {
                    entry.remove_entry();
                }
            }
            self.blocks_ready_to_process.push(HeightOrdered(block));
        }
    }

    pub fn prune_blocks_below_height(&mut self, height: BlockHeight) {
        let heights_to_remove: Vec<BlockHeight> =
            self.height_idx.keys().copied().take_while(|h| *h < height).collect();
        for h in heights_to_remove {
            if let Some(block_hashes) = self.height_idx.remove(&h) {
                for block_hash in block_hashes {
                    self.blocks_waiting_for_chunks.remove(&block_hash);
                    if let Some(chunk_hashes) = self.blocks_missing_chunks.remove(&block_hash) {
                        for chunk_hash in chunk_hashes {
                            if let hash_map::Entry::Occupied(mut entry) =
                                self.missing_chunks.entry(chunk_hash)
                            {
                                let blocks_for_chunk = entry.get_mut();
                                blocks_for_chunk.remove(&block_hash);
                                if blocks_for_chunk.is_empty() {
                                    entry.remove_entry();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Stores chunks for some optimistic block received so far.
#[derive(Debug, Default)]
struct OptimisticBlockChunks {
    /// Number of remaining chunks to be received. Stored to avoid naive
    /// recomputation.
    remaining_chunks: usize,
    /// Height of the previous block.
    prev_block_height: BlockHeight,
    /// Stores chunks for each shard, if received.
    chunks: Vec<Option<ShardChunkHeader>>,
}

impl OptimisticBlockChunks {
    pub fn new(prev_block_height: BlockHeight, num_shards: usize) -> Self {
        Self { remaining_chunks: num_shards, prev_block_height, chunks: vec![None; num_shards] }
    }
}
/// Stores optimistic blocks and chunks which are waiting to be processed.
///
/// Once block and all chunks on top of the same previous block are received,
/// it can provide the optimistic block to be processed.
#[derive(Debug, Default)]
pub struct OptimisticBlockChunksPool {
    /// All blocks and chunks built on top of blocks *smaller* than this height
    /// can be discarded. Needed to garbage collect old data in case of forks
    /// which never can get finalized.
    minimal_base_height: BlockHeight,
    /// Optimistic block with heights *not greater* than this height are discarded.
    /// Needed on top of `minimal_base_height` as well to ensure that only one
    /// optimistic block on each height can be processed.
    block_height_threshold: BlockHeight,
    /// Maps previous block hash to the received optimistic block with the
    /// highest height on top of it.
    blocks: HashMap<CryptoHash, OptimisticBlock>,
    /// Maps previous block hash to the vector of chunk headers corresponding
    /// to complete chunks.
    chunks: HashMap<CryptoHash, OptimisticBlockChunks>,
    /// Optimistic block with the largest height which is ready to process.
    /// Block and chunks must correspond to the same previous block hash.
    latest_ready_block: Option<(OptimisticBlock, Vec<ShardChunkHeader>)>,
}

impl OptimisticBlockChunksPool {
    pub fn new() -> Self {
        Self {
            minimal_base_height: 0,
            block_height_threshold: 0,
            blocks: Default::default(),
            chunks: Default::default(),
            latest_ready_block: None,
        }
    }

    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    pub fn add_block(&mut self, block: OptimisticBlock) {
        if block.height() <= self.block_height_threshold {
            return;
        }

        let prev_block_hash = *block.prev_block_hash();
        self.blocks.insert(prev_block_hash, block);
        self.update_latest_ready_block(&prev_block_hash);
    }

    pub fn add_chunk(&mut self, shard_layout: &ShardLayout, chunk_header: ShardChunkHeader) {
        // We assume that `chunk_header.height_created() = prev_block_height + 1`.
        let prev_block_height = chunk_header.height_created().saturating_sub(1);
        if prev_block_height < self.minimal_base_height {
            return;
        }

        let prev_block_hash = *chunk_header.prev_block_hash();
        let entry = self.chunks.entry(prev_block_hash).or_insert_with(|| {
            OptimisticBlockChunks::new(prev_block_height, shard_layout.num_shards() as usize)
        });

        let shard_index = shard_layout.get_shard_index(chunk_header.shard_id()).unwrap();
        let chunk_entry = entry.chunks.get_mut(shard_index).unwrap();
        let chunk_hash = chunk_header.chunk_hash();
        if let Some(chunk) = &chunk_entry {
            let existing_chunk_hash = chunk.chunk_hash();
            tracing::info!(target: "chunks", ?prev_block_hash, ?chunk_hash, ?existing_chunk_hash, "Chunk already found for OptimisticBlock");
            return;
        }

        *chunk_entry = Some(chunk_header);
        entry.remaining_chunks -= 1;
        tracing::debug!(
            target: "chunks",
            ?prev_block_hash,
            ?chunk_hash,
            remaining_chunks = entry.remaining_chunks,
            "New chunk found for OptimisticBlock"
        );

        if entry.remaining_chunks == 0 {
            tracing::debug!(
                target: "chunks",
                ?prev_block_hash,
                "All chunks received for OptimisticBlock"
            );
            self.update_latest_ready_block(&prev_block_hash);
        }
    }

    /// Takes the latest optimistic block and chunks which are ready to
    /// be processed.
    pub fn take_latest_ready_block(&mut self) -> Option<(OptimisticBlock, Vec<ShardChunkHeader>)> {
        self.latest_ready_block.take()
    }

    /// If the optimistic block on top of `prev_block_hash` is ready to
    /// process, sets the latest ready block to it.
    fn update_latest_ready_block(&mut self, prev_block_hash: &CryptoHash) {
        let Some(chunks) = self.chunks.get(prev_block_hash) else {
            return;
        };
        if chunks.remaining_chunks != 0 {
            return;
        }
        let Some(block) = self.blocks.remove(prev_block_hash) else {
            return;
        };
        if block.height() <= self.block_height_threshold {
            return;
        }

        tracing::debug!(
            target: "chunks",
            ?prev_block_hash,
            optimistic_block_hash = ?block.hash(),
            block_height = block.height(),
            "OptimisticBlock is ready"
        );
        let chunks = chunks
            .chunks
            .iter()
            .map(|c| {
                let mut chunk = c.clone().unwrap();
                // Debatable but probably ok
                *chunk.height_included_mut() = block.height();
                chunk
            })
            .collect();
        self.update_block_height_threshold(block.height());
        self.latest_ready_block = Some((block, chunks));
    }

    /// Updates the block height threshold and cleans up old blocks.
    pub fn update_block_height_threshold(&mut self, height: BlockHeight) {
        self.block_height_threshold = std::cmp::max(self.block_height_threshold, height);
        let hashes_to_remove: Vec<_> = self
            .blocks
            .iter()
            .filter(|(_, h)| h.height() <= self.block_height_threshold)
            .map(|(h, _)| *h)
            .collect();
        for h in hashes_to_remove {
            self.blocks.remove(&h);
        }

        let Some((block, _)) = &self.latest_ready_block else {
            return;
        };
        if block.height() <= self.block_height_threshold {
            self.latest_ready_block = None;
        }
    }

    /// Updates the minimal base height and cleans up old blocks and chunks.
    pub fn update_minimal_base_height(&mut self, height: BlockHeight) {
        // If *new* chunks must be built on top of *at least* this height,
        // optimistic block height must be *strictly greater* than this height.
        self.update_block_height_threshold(height);

        self.minimal_base_height = std::cmp::max(self.minimal_base_height, height);
        let hashes_to_remove: Vec<_> = self
            .chunks
            .iter()
            .filter(|(_, h)| h.prev_block_height < self.minimal_base_height)
            .map(|(h, _)| *h)
            .collect();
        for h in hashes_to_remove {
            self.chunks.remove(&h);
        }
    }
}

#[cfg(test)]
mod missing_chunks_test {
    use super::{BlockHash, BlockLike, MAX_BLOCKS_MISSING_CHUNKS, MissingChunksPool};
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::sharding::ChunkHash;
    use near_primitives::types::BlockHeight;

    fn get_hash(idx: u64) -> CryptoHash {
        hash(&idx.to_le_bytes())
    }

    fn get_chunk_hash(idx: u64) -> ChunkHash {
        ChunkHash(get_hash(idx))
    }

    #[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
    struct MockBlock {
        hash: BlockHash,
        height: BlockHeight,
    }
    impl MockBlock {
        fn new(height: BlockHeight) -> Self {
            Self { hash: get_hash(height), height }
        }
    }
    impl BlockLike for MockBlock {
        fn hash(&self) -> BlockHash {
            self.hash
        }

        fn height(&self) -> u64 {
            self.height
        }
    }

    #[test]
    fn should_mark_blocks_as_ready_after_all_chunks_accepted() {
        let mut pool: MissingChunksPool<MockBlock> = MissingChunksPool::default();

        let block_height = 0;
        let block = MockBlock::new(block_height);
        let chunk_hashes: Vec<ChunkHash> = (101..105).map(get_chunk_hash).collect();

        pool.add_block_with_missing_chunks(block, chunk_hashes.clone());
        assert!(pool.contains(&block.hash));

        for chunk_hash in chunk_hashes.iter().skip(1) {
            pool.accept_chunk(chunk_hash);
            assert!(pool.contains(&block.hash));
        }

        // after the last chunk is accepted the block is ready to process
        pool.accept_chunk(&chunk_hashes[0]);
        assert!(!pool.contains(&block.hash));
        assert_eq!(pool.ready_blocks(), vec![block]);
    }

    #[test]
    fn should_not_add_new_blocks_after_size_limit() {
        let mut pool: MissingChunksPool<MockBlock> = MissingChunksPool::default();
        let mut chunk_hash_idx = MAX_BLOCKS_MISSING_CHUNKS as BlockHeight;

        for block_height in 0..MAX_BLOCKS_MISSING_CHUNKS {
            let block_height = block_height as BlockHeight;
            let block = MockBlock::new(block_height);
            chunk_hash_idx += 1;
            let missing_chunk_hash = get_chunk_hash(chunk_hash_idx);
            let block_hash = block.hash;
            pool.add_block_with_missing_chunks(block, vec![missing_chunk_hash]);
            assert!(pool.contains(&block_hash));
        }

        let block_height = MAX_BLOCKS_MISSING_CHUNKS as BlockHeight;
        let block = MockBlock::new(block_height);
        chunk_hash_idx += 1;
        let missing_chunk_hash = get_chunk_hash(chunk_hash_idx);
        let block_hash = block.hash;
        pool.add_block_with_missing_chunks(block, vec![missing_chunk_hash]);
        assert!(!pool.contains(&block_hash));
    }

    #[test]
    fn should_remove_old_blocks_when_prune_called() {
        let mut pool: MissingChunksPool<MockBlock> = MissingChunksPool::default();

        let block = MockBlock::new(0);
        let early_block_hash = block.hash;
        let missing_chunk_hash = get_chunk_hash(100);
        pool.add_block_with_missing_chunks(block, vec![missing_chunk_hash]);

        let block_height = 1;
        let block = MockBlock::new(block_height);
        let missing_chunk_hash = get_chunk_hash(200);
        pool.add_block_with_missing_chunks(block, vec![missing_chunk_hash.clone()]);

        let later_block = MockBlock::new(block_height + 1);
        let later_block_hash = later_block.hash;
        pool.add_block_with_missing_chunks(later_block, vec![get_chunk_hash(300)]);

        pool.accept_chunk(&missing_chunk_hash);
        pool.prune_blocks_below_height(block_height);
        assert_eq!(pool.ready_blocks(), vec![block]);
        assert!(!pool.contains(&early_block_hash));
        assert!(pool.contains(&later_block_hash));
    }
}

// TODO: tests for adding multiple blocks and reusing chunks.
#[cfg(test)]
mod optimistic_block_chunks_pool_test {
    use super::{OptimisticBlock, OptimisticBlockChunksPool, ShardChunkHeader};
    use itertools::Itertools;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::ShardId;

    #[test]
    fn test_add_block() {
        let mut pool = OptimisticBlockChunksPool::new();
        let prev_hash = CryptoHash::default();
        let block = OptimisticBlock::new_dummy(1, prev_hash);

        pool.add_block(block);
        assert_eq!(pool.num_blocks(), 1);
        assert!(pool.blocks.contains_key(&prev_hash));
    }

    #[test]
    fn test_add_chunk() {
        let mut pool = OptimisticBlockChunksPool::new();
        let prev_hash = CryptoHash::default();
        let shard_layout = ShardLayout::single_shard();
        let chunk_header = ShardChunkHeader::new_dummy(0, ShardId::new(0), prev_hash);

        pool.add_chunk(&shard_layout, chunk_header);
        assert_eq!(pool.num_chunks(), 1);
        assert!(pool.chunks.contains_key(&prev_hash));
    }

    #[test]
    fn test_ready_block() {
        let mut pool = OptimisticBlockChunksPool::new();
        let prev_hash = CryptoHash::default();
        let block = OptimisticBlock::new_dummy(1, prev_hash);
        let shard_layout = ShardLayout::multi_shard(2, 3);
        let shard_ids = shard_layout.shard_ids().collect_vec();

        let chunk_header = ShardChunkHeader::new_dummy(0, shard_ids[0], prev_hash);
        pool.add_block(block);
        pool.add_chunk(&shard_layout, chunk_header);

        pool.update_latest_ready_block(&prev_hash);
        assert!(pool.take_latest_ready_block().is_none());

        let new_chunk_header = ShardChunkHeader::new_dummy(0, shard_ids[1], prev_hash);
        pool.add_chunk(&shard_layout, new_chunk_header);

        assert!(pool.take_latest_ready_block().is_some());
        assert!(pool.take_latest_ready_block().is_none());
    }

    #[test]
    fn test_thresholds() {
        let mut pool = OptimisticBlockChunksPool::new();
        pool.block_height_threshold = 10;
        pool.minimal_base_height = 5;

        let prev_hash = CryptoHash::default();
        let block_below_threshold = OptimisticBlock::new_dummy(10, prev_hash);
        pool.add_block(block_below_threshold);
        assert_eq!(pool.num_blocks(), 0, "Block below threshold should not be added");

        let shard_layout = ShardLayout::single_shard();
        let chunk_header_below_threshold =
            ShardChunkHeader::new_dummy(5, ShardId::new(0), prev_hash);
        pool.add_chunk(&shard_layout, chunk_header_below_threshold);
        assert_eq!(pool.num_chunks(), 0, "Chunk strictly below threshold should not be added");

        let chunk_header_passing_threshold =
            ShardChunkHeader::new_dummy(6, ShardId::new(0), prev_hash);
        pool.add_chunk(&shard_layout, chunk_header_passing_threshold);
        assert_eq!(pool.num_chunks(), 1);
    }
}
