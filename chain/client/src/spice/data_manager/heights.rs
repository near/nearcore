use near_primitives::types::BlockHeight;
use std::ops::Bound;

/// The chain heights the manager has seen, each `None` until first seen.
/// Each height only increases: an update to a lower height leaves it unchanged.
#[derive(Debug, Default)]
pub(super) struct ChainHeights {
    /// Highest final execution head.
    final_execution_head: Option<BlockHeight>,
    /// Highest consensus final head.
    final_head: Option<BlockHeight>,
}

impl ChainHeights {
    pub(super) fn update_final_execution_head(&mut self, height: BlockHeight) {
        self.final_execution_head = self.final_execution_head.max(Some(height));
    }

    /// Updates the final head to `height` and returns the lower bound of the heights newly at
    /// or below it; `None` if it did not rise.
    pub(super) fn update_final_head(&mut self, height: BlockHeight) -> Option<Bound<BlockHeight>> {
        let from = match self.final_head {
            Some(final_head) if final_head >= height => return None,
            Some(final_head) => Bound::Excluded(final_head),
            None => Bound::Unbounded,
        };
        self.final_head = Some(height);
        Some(from)
    }

    /// Whether `height` is at or below the final execution head.
    pub(super) fn at_or_below_final_execution(&self, height: BlockHeight) -> bool {
        self.final_execution_head.is_some_and(|head| height <= head)
    }

    /// Whether `height` is at or below the final head.
    pub(super) fn at_or_below_final_head(&self, height: BlockHeight) -> bool {
        self.final_head.is_some_and(|final_head| height <= final_head)
    }
}
