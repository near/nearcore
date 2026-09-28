use super::SenderFault;
use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::partial_data::SpiceVerifiedPartialData;
use std::num::NonZeroUsize;

/// Signed partial data whose block is not known yet, grouped by block hash.
pub(crate) struct PendingPartialData {
    by_block: LruCache<CryptoHash, Vec<SpiceVerifiedPartialData>>,
}

impl PendingPartialData {
    pub(crate) fn new(max_blocks: NonZeroUsize) -> Self {
        Self { by_block: LruCache::new(max_blocks) }
    }

    /// Refuses a message that carries no parts.
    pub(crate) fn insert(&mut self, data: SpiceVerifiedPartialData) -> Result<(), SenderFault> {
        if data.parts.is_empty() {
            return Err(SenderFault::EmptyMessage);
        }
        // TODO(spice): Verify that size of partial data isn't too large.
        self.by_block.get_or_insert_mut(*data.id.block_hash(), Vec::new).push(data);
        Ok(())
    }

    /// Removes and returns the data buffered for `block_hash`.
    pub(crate) fn take(&mut self, block_hash: &CryptoHash) -> Vec<SpiceVerifiedPartialData> {
        self.by_block.pop(block_hash).unwrap_or_default()
    }
}
