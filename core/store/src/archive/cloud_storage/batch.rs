//! Batch identifiers, ranges, and arithmetic.
//!
//! Batches sit on a global grid of `batch_size`-aligned windows and are
//! keyed in cloud paths by the window's `BatchId`. The heights actually
//! covered live inside the blob and may be narrower than the window for a
//! partial first batch (genesis / fresh init / new child shard post-
//! resharding). TODO(cloud_archival, resharding): a parent shard's last
//! batch can also end early at a reshard boundary.

use near_primitives::types::BlockHeight;

/// Start height of the global batch window. Names the cloud blob for that
/// window (scoped by `shard_id` for shard batches).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchId(pub BlockHeight);

/// Returns the `BatchId` of the window containing `height`.
pub fn compute_batch_id(height: BlockHeight, batch_size: u32) -> BatchId {
    let batch_size = batch_size as u64;
    BatchId((height / batch_size) * batch_size)
}

/// Inclusive height range `[start, end]` covered by a single batch.
#[derive(Clone, Copy, Debug)]
pub struct BatchRange {
    start: BlockHeight,
    end: BlockHeight,
}

impl BatchRange {
    /// See the module doc for partial batches where the range is narrower
    /// than `batch_size`.
    pub fn new(start: BlockHeight, end: BlockHeight) -> Self {
        assert!(end >= start, "invalid batch range [{start}, {end}]");
        Self { start, end }
    }

    pub fn start(&self) -> BlockHeight {
        self.start
    }

    pub fn end(&self) -> BlockHeight {
        self.end
    }
}

/// Returns the next batch to archive after `height` (the last archived
/// height, or a pre-archive cut for genesis / fresh init / resharded
/// child). Starts at `height + 1` and ends at the next batch-aligned
/// boundary.
/// TODO(cloud_archival, resharding): also end early at reshard boundaries.
pub fn compute_next_batch(height: BlockHeight, batch_size: u32) -> BatchRange {
    let batch_size = batch_size as u64;
    let start = height + 1;
    // Steady state: full window.
    if start % batch_size == 0 {
        return BatchRange { start, end: start + batch_size - 1 };
    }
    // Genesis / fresh init / resharded child: partial window to next boundary.
    let end = ((start / batch_size) + 1) * batch_size - 1;
    BatchRange { start, end }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_batch_id_aligns_down_to_multiple() {
        assert_eq!(compute_batch_id(0, 4), BatchId(0));
        assert_eq!(compute_batch_id(1, 4), BatchId(0));
        assert_eq!(compute_batch_id(3, 4), BatchId(0));
        assert_eq!(compute_batch_id(4, 4), BatchId(4));
        assert_eq!(compute_batch_id(7, 4), BatchId(4));
        assert_eq!(compute_batch_id(100, 32), BatchId(96));
    }

    #[test]
    fn compute_batch_id_batch_size_one() {
        assert_eq!(compute_batch_id(0, 1), BatchId(0));
        assert_eq!(compute_batch_id(42, 1), BatchId(42));
    }

    #[test]
    #[should_panic]
    fn compute_batch_id_panics_on_zero_batch_size() {
        let _ = compute_batch_id(10, 0);
    }

    /// `height` on a batch boundary - full window.
    #[test]
    fn compute_next_batch_steady_state() {
        let r = compute_next_batch(3, 4);
        assert_eq!((r.start(), r.end()), (4, 7));

        let r = compute_next_batch(127, 32);
        assert_eq!((r.start(), r.end()), (128, 159));
    }

    /// `height = 0` means "genesis already archived".
    #[test]
    fn compute_next_batch_genesis() {
        let r = compute_next_batch(0, 4);
        assert_eq!((r.start(), r.end()), (1, 3));

        let r = compute_next_batch(0, 32);
        assert_eq!((r.start(), r.end()), (1, 31));
    }

    /// Fresh init: `height` lands mid-window; first batch is partial.
    #[test]
    fn compute_next_batch_fresh_init_mid_window() {
        let r = compute_next_batch(99, 32);
        assert_eq!((r.start(), r.end()), (100, 127));
    }

    /// Post-resharding child: pre-archive cut mid-window; first batch is
    /// partial. `BatchId` coincides with the parent's last batch; paths
    /// are disambiguated by `shard_id`.
    #[test]
    fn compute_next_batch_child_shard_start() {
        let r = compute_next_batch(47, 32);
        assert_eq!((r.start(), r.end()), (48, 63));
        assert_eq!(compute_batch_id(r.start(), 32), BatchId(32));
    }
}
