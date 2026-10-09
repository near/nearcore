use super::SenderFault;
use lru::LruCache;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::partial_data::SpicePartialData;
use std::num::NonZeroUsize;

/// Partial data whose block is not known yet, grouped by block hash. With the final head in
/// epoch E, it was checked against the sender's keys in E and E+1. It is checked again when the
/// block arrives, because a block in epoch X accepts only the sender's keys in X and X+1.
pub(crate) struct PendingPartialData {
    by_block: LruCache<CryptoHash, Vec<SpicePartialData>>,
}

impl PendingPartialData {
    pub(crate) fn new(max_blocks: NonZeroUsize) -> Self {
        Self { by_block: LruCache::new(max_blocks) }
    }

    /// Refuses a message that carries no parts.
    pub(crate) fn insert(&mut self, data: SpicePartialData) -> Result<(), SenderFault> {
        if !data.has_parts() {
            return Err(SenderFault::EmptyMessage);
        }
        // TODO(spice): Verify that size of partial data isn't too large.
        self.by_block.get_or_insert_mut(*data.block_hash(), Vec::new).push(data);
        Ok(())
    }

    /// Removes and returns the data buffered for `block_hash`.
    pub(crate) fn take(&mut self, block_hash: &CryptoHash) -> Vec<SpicePartialData> {
        self.by_block.pop(block_hash).unwrap_or_default()
    }
}
