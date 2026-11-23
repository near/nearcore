use super::DataId;
use near_primitives::types::BlockHeight;
use std::ops::Bound;

/// The lowest height whose pushed items of each kind may still be needed.
#[derive(Debug, Clone, Copy)]
pub(super) struct Horizons {
    pub(super) receipt_proofs: BlockHeight,
    pub(super) witnesses: BlockHeight,
}

/// The chain heights the manager has seen, each `None` until first seen.
/// Each height only increases: an update to a lower height leaves it unchanged.
#[derive(Debug, Default)]
pub(super) struct ChainHeights {
    /// Highest horizon of receipt proofs pushed to this node.
    receipt_proofs: Option<BlockHeight>,
    /// Highest horizon of witnesses.
    witnesses: Option<BlockHeight>,
    /// Highest consensus final head.
    final_head: Option<BlockHeight>,
}

impl ChainHeights {
    /// Raises each kind's horizon to `horizons` and returns, for each horizon that rose, the
    /// heights newly below it.
    pub(super) fn update_horizons(
        &mut self,
        horizons: Horizons,
    ) -> Vec<(Bound<BlockHeight>, Bound<BlockHeight>)> {
        [
            raise(&mut self.receipt_proofs, horizons.receipt_proofs),
            raise(&mut self.witnesses, horizons.witnesses),
        ]
        .into_iter()
        .flatten()
        .collect()
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

    /// Whether `id` at `height` is below its kind's horizon.
    pub(super) fn expired(&self, id: &DataId, height: BlockHeight) -> bool {
        let horizon = match id {
            DataId::ReceiptProof(_) => self.receipt_proofs,
            DataId::Witness(_) => self.witnesses,
        };
        horizon.is_some_and(|horizon| height < horizon)
    }

    /// The lowest horizon of any kind; `None` until every horizon is seen.
    pub(super) fn lowest_horizon(&self) -> Option<BlockHeight> {
        Some(self.receipt_proofs?.min(self.witnesses?))
    }

    /// Whether `height` is at or below the final head.
    pub(super) fn at_or_below_final_head(&self, height: BlockHeight) -> bool {
        self.final_head.is_some_and(|final_head| height <= final_head)
    }
}

/// Raises `horizon` to `height` and returns the heights newly below it; `None` if it did not rise.
fn raise(
    horizon: &mut Option<BlockHeight>,
    height: BlockHeight,
) -> Option<(Bound<BlockHeight>, Bound<BlockHeight>)> {
    let from = match *horizon {
        Some(previous) if previous >= height => return None,
        Some(previous) => Bound::Included(previous),
        None => Bound::Unbounded,
    };
    *horizon = Some(height);
    Some((from, Bound::Excluded(height)))
}
