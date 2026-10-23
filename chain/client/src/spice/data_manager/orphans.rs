use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use std::collections::HashMap;
use std::num::NonZeroUsize;

/// Data waiting for its block, grouped by block hash. Holds the most recently parked
/// `max_blocks` blocks.
pub(crate) struct OrphanPool<T> {
    by_block: LruCache<CryptoHash, Vec<T>>,
    /// Blocks above the final head whose data was released, with their heights.
    released: HashMap<CryptoHash, BlockHeight>,
}

impl<T> OrphanPool<T> {
    pub(crate) fn new(max_blocks: NonZeroUsize) -> Self {
        Self { by_block: LruCache::new(max_blocks), released: HashMap::new() }
    }

    /// Parks `data` for the block. Returns false, dropping the data, if the block was released.
    #[must_use]
    pub(crate) fn insert(&mut self, block_hash: CryptoHash, data: T) -> bool {
        if self.released.contains_key(&block_hash) {
            return false;
        }
        // TODO(spice-data-distribution): bound the data parked per sender.
        self.by_block.get_or_insert_mut(block_hash, Vec::new).push(data);
        true
    }

    /// Removes and returns the data parked for `block_hash`.
    pub(crate) fn take(&mut self, block_hash: &CryptoHash) -> Vec<T> {
        self.by_block.pop(block_hash).unwrap_or_default()
    }

    /// Removes and returns the data parked for the block, and records it as released until
    /// the final head reaches `height`.
    pub(crate) fn release(&mut self, height: BlockHeight, block_hash: &CryptoHash) -> Vec<T> {
        self.released.insert(*block_hash, height);
        self.take(block_hash)
    }

    /// Forgets the released blocks at or below `final_height`.
    pub(crate) fn prune_at_or_below(&mut self, final_height: BlockHeight) {
        self.released.retain(|_, height| *height > final_height);
    }
}
