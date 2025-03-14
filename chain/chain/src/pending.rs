use std::collections::BTreeMap;

use near_primitives::types::BlockHeight;
use tracing::warn;

use crate::orphan::Orphan;

/// Blocks that are waiting for optimistic block to be applied.
pub struct PendingBlocksPool {
    /// Maps block height to block.
    /// There may be only one optimistic block on each height, so we
    /// store only one entry.
    blocks: BTreeMap<BlockHeight, Orphan>,
}

impl PendingBlocksPool {
    pub fn new() -> Self {
        Self { blocks: BTreeMap::new() }
    }

    pub fn add_block(&mut self, orphan: Orphan) {
        let height = orphan.block.header().height();
        if self.blocks.contains_key(&height) {
            warn!(target: "chain", "Block {:?} already exists in pending blocks pool", orphan.block.hash());
            return;
        }
        self.blocks.insert(height, orphan);
    }

    pub fn contains_key(&self, height: &BlockHeight) -> bool {
        self.blocks.contains_key(height)
    }

    pub fn take_block(&mut self, height: &BlockHeight) -> Option<Orphan> {
        self.blocks.remove(height)
    }

    pub fn prune_blocks_below_height(&mut self, height: BlockHeight) {
        self.blocks = self.blocks.split_off(&height);
    }
}
