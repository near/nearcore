use super::DataId;
use near_primitives::types::BlockHeight;
use std::ops::{Bound, Range, RangeInclusive};

/// The lowest height whose pushed items of each kind may still be needed.
#[derive(Debug, Clone, Copy)]
pub(super) struct LowestNeededHeights {
    pub(super) receipt_proofs: BlockHeight,
    pub(super) witnesses: BlockHeight,
}

/// The chain heights the manager has seen, each `None` until first seen.
/// Each height only increases: an update to a lower height leaves it unchanged.
#[derive(Debug, Default)]
pub(super) struct ChainHeights {
    /// Highest lowest needed heights of the items pushed to this node, per kind.
    lowest_needed: Option<LowestNeededHeights>,
    /// Highest consensus final head.
    final_head: Option<BlockHeight>,
    /// Highest height of a processed block.
    highest_processed: Option<BlockHeight>,
}

impl ChainHeights {
    /// Raises each kind's lowest needed height to `heights` and returns the heights newly below
    /// each.
    pub(super) fn update_lowest_needed(&mut self, heights: LowestNeededHeights) -> ExpiredHeights {
        let previous =
            self.lowest_needed.unwrap_or(LowestNeededHeights { receipt_proofs: 0, witnesses: 0 });
        let raised = LowestNeededHeights {
            receipt_proofs: previous.receipt_proofs.max(heights.receipt_proofs),
            witnesses: previous.witnesses.max(heights.witnesses),
        };
        self.lowest_needed = Some(raised);
        ExpiredHeights {
            receipt_proofs: newly_below(previous.receipt_proofs, raised.receipt_proofs),
            witnesses: newly_below(previous.witnesses, raised.witnesses),
        }
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

    /// Raises the highest processed height to `height` and returns the heights newly reached;
    /// `None` if it did not rise.
    pub(super) fn update_highest_processed(
        &mut self,
        height: BlockHeight,
    ) -> Option<RangeInclusive<BlockHeight>> {
        let from = match self.highest_processed {
            Some(highest) if highest >= height => return None,
            Some(highest) => highest + 1,
            None => height,
        };
        self.highest_processed = Some(height);
        Some(from..=height)
    }

    /// Whether a block at `height` or above has been processed.
    pub(super) fn processed_at_or_above(&self, height: BlockHeight) -> bool {
        self.highest_processed.is_some_and(|highest| highest >= height)
    }

    /// Whether `id` at `height` is below its kind's lowest needed height.
    pub(super) fn expired(&self, id: &DataId, height: BlockHeight) -> bool {
        self.lowest_needed.is_some_and(|lowest_needed| {
            let lowest = match id {
                DataId::ReceiptProof(_) => lowest_needed.receipt_proofs,
                DataId::Witness(_) => lowest_needed.witnesses,
            };
            height < lowest
        })
    }

    /// The lowest needed height of any kind; `None` until first seen.
    pub(super) fn lowest_needed(&self) -> Option<BlockHeight> {
        self.lowest_needed.map(|lowest_needed| lowest_needed.lowest())
    }

    /// Whether `height` is at or below the final head.
    pub(super) fn at_or_below_final_head(&self, height: BlockHeight) -> bool {
        self.final_head.is_some_and(|final_head| height <= final_head)
    }
}

impl LowestNeededHeights {
    fn lowest(&self) -> BlockHeight {
        self.receipt_proofs.min(self.witnesses)
    }
}

/// The heights newly below each kind's lowest needed height; `None` for a kind whose height did
/// not rise.
pub(super) struct ExpiredHeights {
    pub(super) receipt_proofs: Option<Range<BlockHeight>>,
    pub(super) witnesses: Option<Range<BlockHeight>>,
}

/// The heights from `previous` up to `current`; `None` if empty.
fn newly_below(previous: BlockHeight, current: BlockHeight) -> Option<Range<BlockHeight>> {
    (previous < current).then_some(previous..current)
}
