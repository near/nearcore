//! Hint-fallback scan. Walks `OutcomeIds` / `TransactionResultForBlock`
//! around caller anchor → locates receipt's producing outcome. Used when
//! `ReceiptToTx` column disabled or rotated out.

use crate::{ChainStore, ChainStoreAccess};
use near_chain_primitives::Error;
use near_o11y::tracing;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction, ReceiptToTxInfo,
    ReceiptToTxInfoV1,
};
use near_primitives::types::{BlockHeight, BlockHeightDelta, ShardId};
use near_store::DBCol;

/// Default `Scan::CenterOut` window (blocks) when caller omits `window`.
/// Scan inspects `[h-window, h+window]`. `Scan::Ancestor` uses
/// `receipt_to_tx_max_hop_distance` instead.
pub const DEFAULT_HINT_WINDOW: BlockHeightDelta = 5;

/// Hint scan mode + width. Monotonic: once the walker resolves a hop via
/// scan it stays in `Ancestor` for the rest of the walk; column hits do
/// not flip it back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scan {
    /// `±window` around the caller's literal hint. Used pre-first-scan.
    CenterOut { window: BlockHeightDelta },
    /// Anchor-inclusive backward scan `h, h-1, ..., h-max_distance`.
    /// Used after any prior hop in the walk was scan-resolved.
    Ancestor { max_distance: BlockHeightDelta },
}

impl Scan {
    /// Inclusive `(low, high)` bounds this scan visits. Single source of truth:
    /// `center_out_heights`/`ancestor_heights` and `resolve_scan_shards` all derive
    /// from it, so a reshard boundary can't be covered by one and missed by another.
    /// `scan_bounds_match_iterators` locks the iterators to it.
    pub fn height_bounds(self, anchor: BlockHeight) -> (BlockHeight, BlockHeight) {
        match self {
            Scan::CenterOut { window } => {
                (anchor.saturating_sub(window), anchor.saturating_add(window))
            }
            Scan::Ancestor { max_distance } => (anchor.saturating_sub(max_distance), anchor),
        }
    }
}

/// Iterate heights center-out around `block_height` up to `±window`,
/// saturating at 0. Order: `h, h-1, h+1, h-2, h+2, ...`. Both "hint =
/// creation block" and "hint = child execution block" interpretations
/// early-return because parent typically executes at `h` or `h-1` of child.
fn center_out_heights(
    block_height: BlockHeight,
    window: BlockHeightDelta,
) -> impl Iterator<Item = BlockHeight> {
    std::iter::once(block_height).chain((1..=window).flat_map(move |offset| {
        let low = block_height.saturating_sub(offset);
        let high = block_height.saturating_add(offset);
        // saturating_sub repeats 0 once offset > block_height. Drop dup
        // so iterator visits each height once.
        let lower = (offset <= block_height).then_some(low);
        let upper = (low != high).then_some(high);
        [lower, upper].into_iter().flatten()
    }))
}

/// Iterate heights backward `h, h-1, ..., h-max_distance`, saturating at 0.
/// Anchor included so scan finds same-shard local receipts (execute in
/// same block as producing outcome). No forward heights — receipts emit
/// before execute.
fn ancestor_heights(
    block_height: BlockHeight,
    max_distance: BlockHeightDelta,
) -> impl Iterator<Item = BlockHeight> {
    let (lo, hi) = (Scan::Ancestor { max_distance }).height_bounds(block_height);
    (lo..=hi).rev()
}

/// Hint-scan hit. Carries synthesized `ReceiptToTxInfo` + execution block
/// height of resolved outcome (caller refreshes `block_height` for next
/// hop). Outcome shard not propagated — next hop's scan target lives on
/// producer's shard, derived by handler from
/// `ReceiptOriginReceipt.parent_predecessor_id` at resolved height.
pub struct HintResolution {
    pub info: ReceiptToTxInfo,
    /// Block height parent outcome (transaction or receipt) executed.
    /// For receipt-origin parents, also the height child receipt was created.
    pub outcome_block_height: BlockHeight,
}

/// Per-scan I/O accounting. Handler drives hint-scan metrics from this
/// regardless of hit/miss.
#[derive(Default, Clone, Copy)]
pub struct HintScanStats {
    pub heights_scanned: u64,
    pub outcomes_scanned: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveHintError {
    #[error(transparent)]
    Chain(#[from] Error),
    #[error("hint-scan outcome budget exceeded")]
    BudgetExceeded,
}

/// Resolve immediate parent of `receipt_id` by scanning `OutcomeIds` /
/// `TransactionResultForBlock` around `block_height` on `shard_id`. Mode
/// set by `scan` — `CenterOut` pre-first-scan, `Ancestor` after.
///
/// `Ok(Some(_))` — parent located, info synthesized in-flight.
/// `Ok(None)` — window exhausted, receipt not found.
/// `Err(_)` — I/O error or budget exhaustion mid-scan; bubbles to handler.
///
/// `stats` accumulates in-place (callers emit metrics on hit/miss/error).
/// Missing data (no block at height, GC'd outcome, deleted receipt) is
/// skip-and-continue. `remaining_budget` decrements per outcome inspected
/// — one budget shared across all shards + hops.
pub fn resolve_receipt_via_hint(
    chain_store: &ChainStore,
    receipt_id: CryptoHash,
    block_height: BlockHeight,
    shard_id: ShardId,
    scan: Scan,
    stats: &mut HintScanStats,
    remaining_budget: &mut u64,
) -> Result<Option<HintResolution>, ResolveHintError> {
    let store = chain_store.store();

    let heights: Box<dyn Iterator<Item = BlockHeight>> = match scan {
        Scan::CenterOut { window } => Box::new(center_out_heights(block_height, window)),
        Scan::Ancestor { max_distance } => Box::new(ancestor_heights(block_height, max_distance)),
    };
    for height in heights {
        stats.heights_scanned += 1;
        let block_hash = match chain_store.get_block_hash_by_height(height) {
            Ok(h) => h,
            Err(Error::DBNotFoundErr(_)) => continue,
            Err(e) => return Err(e.into()),
        };

        let outcome_ids =
            chain_store.get_outcomes_by_block_hash_and_shard_id(&block_hash, shard_id);
        for outcome_id in outcome_ids {
            if *remaining_budget == 0 {
                return Err(ResolveHintError::BudgetExceeded);
            }
            *remaining_budget -= 1;
            stats.outcomes_scanned += 1;
            let owp = match chain_store.get_outcome_by_id_and_block_hash(&outcome_id, &block_hash) {
                Some(o) => o,
                None => continue,
            };
            if !owp.outcome.receipt_ids.contains(&receipt_id) {
                continue;
            }

            let child = match chain_store.get_receipt(&receipt_id) {
                Some(r) => r,
                None => continue,
            };

            // Both columns reference-counted + GC'd at archival horizon.
            // Check both, skip if neither — tx-origin row may have been
            // GC'd, leaving outcome row with only the Receipts side.
            //
            // `exists` not snapshotted. Concurrent GC may downgrade
            // (true, true) → any subset. Worst case: spurious skip, never
            // misclassification — both downstream paths re-read same store.
            let in_txs = store.exists(DBCol::Transactions, outcome_id.as_ref());
            let in_receipts = store.exists(DBCol::Receipts, outcome_id.as_ref());
            let origin = match (in_txs, in_receipts) {
                (true, false) => ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                    tx_hash: outcome_id,
                    sender_account_id: owp.outcome.executor_id,
                }),
                (false, true) => {
                    let parent = match chain_store.get_receipt(&outcome_id) {
                        Some(r) => r,
                        None => continue,
                    };
                    ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                        parent_receipt_id: outcome_id,
                        parent_predecessor_id: parent.predecessor_id().clone(),
                    })
                }
                (false, false) => continue,
                (true, true) => {
                    tracing::debug!(
                        %outcome_id,
                        height,
                        "outcome id present in both DBCol::Transactions and DBCol::Receipts; \
                         skipping ambiguous classification"
                    );
                    continue;
                }
            };

            let resolution = HintResolution {
                info: ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                    origin,
                    receiver_account_id: child.receiver_id().clone(),
                    shard_id,
                }),
                outcome_block_height: height,
            };
            return Ok(Some(resolution));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(h: BlockHeight, w: BlockHeightDelta) -> Vec<BlockHeight> {
        center_out_heights(h, w).collect()
    }

    fn collect_ancestor(h: BlockHeight, w: BlockHeightDelta) -> Vec<BlockHeight> {
        ancestor_heights(h, w).collect()
    }

    #[test]
    fn center_out_basic() {
        assert_eq!(collect(100, 0), vec![100]);
        assert_eq!(collect(100, 1), vec![100, 99, 101]);
        assert_eq!(collect(100, 3), vec![100, 99, 101, 98, 102, 97, 103]);
    }

    #[test]
    fn center_out_saturates_at_zero() {
        // Offsets > block_height drop lower side, no duplicates.
        assert_eq!(collect(2, 5), vec![2, 1, 3, 0, 4, 5, 6, 7]);
        assert_eq!(collect(0, 2), vec![0, 1, 2]);
    }

    #[test]
    fn ancestor_heights_basic() {
        assert_eq!(collect_ancestor(100, 0), vec![100]);
        assert_eq!(collect_ancestor(100, 3), vec![100, 99, 98, 97]);
    }

    #[test]
    fn ancestor_heights_saturates_at_zero() {
        assert_eq!(collect_ancestor(2, 5), vec![2, 1, 0]);
        assert_eq!(collect_ancestor(0, 5), vec![0]);
    }

    #[test]
    fn ancestor_heights_no_forward() {
        // Lock invariant on iterator: ancestor scan never emits height > anchor.
        for h in [0, 1, 5, 100, 1_000_000] {
            for w in [0, 1, 5, 20, 100] {
                for emitted in ancestor_heights(h, w) {
                    assert!(
                        emitted <= h,
                        "ancestor_heights({h}, {w}) emitted {emitted} > anchor {h}"
                    );
                }
            }
        }
    }

    #[test]
    fn scan_bounds_match_iterators() {
        // `height_bounds` must equal the iterators' min/max, else `resolve_scan_shards`
        // resolves layouts over the wrong window and misses a reshard boundary.
        for (h, w) in [(100u64, 1u64), (100, 3), (2, 5), (0, 0), (0, 2), (50, 20)] {
            let co = collect(h, w);
            let (lo, hi) = (Scan::CenterOut { window: w }).height_bounds(h);
            assert_eq!(*co.iter().min().unwrap(), lo, "center_out lo h={h} w={w}");
            assert_eq!(*co.iter().max().unwrap(), hi, "center_out hi h={h} w={w}");

            let anc = collect_ancestor(h, w);
            let (lo, hi) = (Scan::Ancestor { max_distance: w }).height_bounds(h);
            assert_eq!(*anc.iter().min().unwrap(), lo, "ancestor lo h={h} w={w}");
            assert_eq!(*anc.iter().max().unwrap(), hi, "ancestor hi h={h} w={w}");
        }
    }
}
