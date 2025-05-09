use std::collections::{HashMap, HashSet};

use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use tracing::warn;

use crate::metrics;
use crate::missing_chunks::BlockLike;

/// Blocks that are waiting for optimistic block to be applied.
pub struct PendingBlocksPool<Block: BlockLike> {
    /// Maps block height to the pending block.
    /// For simplicity, store only one block per height. If there are more, it
    /// is the malicious case and we process such blocks without passing
    /// through this pool.
    blocks: HashMap<BlockHeight, Block>,
    /// Block hashes that are in the pending blocks pool.
    block_hashes: HashSet<CryptoHash>,
}

impl<Block: BlockLike> PendingBlocksPool<Block> {
    pub fn new() -> Self {
        Self { blocks: HashMap::new(), block_hashes: HashSet::new() }
    }

    pub fn add_block(&mut self, block: Block) {
        let height = block.height();
        if self.blocks.contains_key(&height) {
            warn!(target: "chain", "Block {:?} already exists in pending blocks pool", block.hash());
            return;
        }
        self.block_hashes.insert(block.hash());
        self.blocks.insert(height, block);
        metrics::NUM_PENDING_BLOCKS.set(self.len() as i64);
    }

    pub fn contains_key(&self, height: &BlockHeight) -> bool {
        self.blocks.contains_key(height)
    }

    pub fn contains_block_hash(&self, hash: &CryptoHash) -> bool {
        self.block_hashes.contains(hash)
    }

    pub fn take_block(&mut self, height: &BlockHeight) -> Option<Block> {
        let block = self.blocks.remove(height);
        metrics::NUM_PENDING_BLOCKS.set(self.len() as i64);
        block
    }

    pub fn prune_blocks_below_height(&mut self, height: BlockHeight) {
        self.blocks.retain(|&h, block| {
            if h < height {
                self.block_hashes.remove(&block.hash());
                return false;
            }
            true
        });
        metrics::NUM_PENDING_BLOCKS.set(self.len() as i64);
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::hash::{CryptoHash, hash};
    use near_primitives::types::BlockHeight;

    // Simple Block-like object for testing
    #[derive(Clone)]
    struct MockBlock {
        height: BlockHeight,
        hash: CryptoHash,
    }

    impl MockBlock {
        fn new(height: BlockHeight) -> Self {
            Self { height, hash: hash(height.to_le_bytes().as_ref()) }
        }
    }

    impl BlockLike for MockBlock {
        fn hash(&self) -> CryptoHash {
            self.hash
        }

        fn height(&self) -> u64 {
            self.height
        }
    }

    #[test]
    fn test_add_block() {
        let mut pool = PendingBlocksPool::<MockBlock>::new();
        let block = MockBlock::new(1);
        pool.add_block(block);
        assert_eq!(pool.len(), 1);
        assert!(pool.contains_key(&1));
    }

    #[test]
    fn test_add_block_duplicate() {
        let mut pool = PendingBlocksPool::<MockBlock>::new();

        let block1 = MockBlock::new(1);
        let block2 = MockBlock::new(1);

        pool.add_block(block1);
        assert_eq!(pool.len(), 1);

        // Adding another block with the same height should be ignored
        pool.add_block(block2);
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_contains() {
        let mut pool = PendingBlocksPool::<MockBlock>::new();
        let block = MockBlock::new(1);
        let block_hash = block.hash();
        pool.add_block(block);

        assert!(pool.contains_key(&1));
        assert!(!pool.contains_key(&2));

        assert!(pool.contains_block_hash(&block_hash));
        assert!(!pool.contains_block_hash(&CryptoHash::default()));
    }

    #[test]
    fn test_take_block() {
        let mut pool = PendingBlocksPool::<MockBlock>::new();
        let block = MockBlock::new(1);
        pool.add_block(block);

        // Take an existing block
        let taken = pool.take_block(&1);
        assert!(taken.is_some());
        assert_eq!(pool.len(), 0);

        // Take a non-existing block
        let not_taken = pool.take_block(&2);
        assert!(not_taken.is_none());
    }

    #[test]
    fn test_prune_blocks_below_height() {
        let mut pool = PendingBlocksPool::<MockBlock>::new();

        // Add blocks with heights 1, 2, 3, 4, 5
        let blocks: Vec<_> = (1..=5).map(|i| MockBlock::new(i)).collect();
        let block_hashes: Vec<_> = blocks.iter().map(|b| b.hash()).collect();

        for block in blocks {
            pool.add_block(block);
        }
        assert_eq!(pool.len(), 5);

        // Verify all block hashes are present before pruning
        for hash in &block_hashes {
            assert!(pool.contains_block_hash(hash));
        }

        // Prune blocks below height 3
        pool.prune_blocks_below_height(3);

        // Should only have blocks with heights 3, 4, 5
        assert_eq!(pool.len(), 3);
        assert!(!pool.contains_key(&1));
        assert!(!pool.contains_key(&2));
        assert!(pool.contains_key(&3));
        assert!(pool.contains_key(&4));
        assert!(pool.contains_key(&5));

        // Verify block hashes are properly removed for pruned blocks
        assert!(!pool.contains_block_hash(&block_hashes[0]));
        assert!(!pool.contains_block_hash(&block_hashes[1]));
        assert!(pool.contains_block_hash(&block_hashes[2]));
        assert!(pool.contains_block_hash(&block_hashes[3]));
        assert!(pool.contains_block_hash(&block_hashes[4]));
    }

    #[test]
    fn test_len() {
        let mut pool = PendingBlocksPool::<MockBlock>::new();
        assert_eq!(pool.len(), 0);

        // Add a block
        let block = MockBlock::new(1);
        pool.add_block(block);
        assert_eq!(pool.len(), 1);

        // Add another block
        let block = MockBlock::new(2);
        pool.add_block(block);
        assert_eq!(pool.len(), 2);

        // Remove a block
        pool.take_block(&1);
        assert_eq!(pool.len(), 1);

        // Remove all blocks
        pool.take_block(&2);
        assert_eq!(pool.len(), 0);
    }
}
