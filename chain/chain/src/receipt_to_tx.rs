//! Hint-fallback resolver for the receipt-to-tx walk.
//!
//! Two scan modes share one `resolve_receipt_via_hint` entry point:
//!
//! * **Hop 0 — `ScanDirection::CenterOut`.** The anchor is a caller-supplied
//!   hint that could be off either side of the producing outcome, so the
//!   resolver visits `h, h-1, h+1, h-2, h+2, ...` up to `±window`.
//! * **Hop 1+ — `ScanDirection::Ancestor`.** The anchor is the previously
//!   resolved parent outcome's *exact* execution height. The producing
//!   outcome we still need to locate must live at or before the anchor —
//!   receipts are produced before they execute, so forward heights are
//!   physically impossible. The anchor itself stays in the iteration
//!   because same-shard local receipts can execute in the same block as
//!   their producing outcome (`process_local_receipts` runs inside the
//!   same `apply()` call as the transactions that emit them).
//!
//! Both modes consume the per-request outcome budget enforced by the
//! handler; the kernel only counts and reports per-scan progress.

use crate::{ChainStore, ChainStoreAccess};
use near_chain_primitives::Error;
use near_o11y::tracing;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction, ReceiptToTxInfo,
    ReceiptToTxInfoV1,
};
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, ShardId};
use near_store::DBCol;

/// Default scan window (in blocks) for the caller-controlled hop 0 scan of
/// [`resolve_receipt_via_hint`] when the caller does not specify one. Hop 0
/// runs in `ScanDirection::CenterOut`, inspecting `[h-window, h+window]`.
/// Hop 1+ uses [`ScanDirection::Ancestor`] with its own width set by the
/// node config, not this default.
pub const DEFAULT_HINT_WINDOW: BlockHeightDelta = 5;

/// Direction of the hint scan around an anchor block height.
///
/// Hop 0 anchors on a caller-supplied hint that could be off either side,
/// so the resolver visits `±window` heights center-out. Hop 1+ anchors on
/// the previously resolved parent's execution height — which is exact, not
/// a hint — so the producing outcome of interest must live at the anchor
/// or earlier. Forward heights are physically impossible because receipts
/// are produced before they execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    /// Center-out `±window` scan. Used for hop 0 (caller hint).
    CenterOut,
    /// Anchor-inclusive backward scan: `h, h-1, ..., h-window`. The anchor
    /// is included because same-shard local receipts can execute in the
    /// same block as their producing outcome (`process_local_receipts`
    /// runs within the same `apply()` call as the transactions that emit
    /// them; see `runtime/runtime/src/lib.rs`). Used for hop 1+
    /// (boundary-refresh from a resolved parent height).
    Ancestor,
}

/// Versioned constructor for `ReceiptToTxInfo`. Used by the RPC hint-scan
/// resolver (`resolve_receipt_via_hint`) when synthesizing origin info
/// from chain data on the fly.
pub fn build_receipt_to_tx_info(
    origin: ReceiptOrigin,
    receiver_account_id: AccountId,
    shard_id: ShardId,
) -> ReceiptToTxInfo {
    ReceiptToTxInfo::V1(ReceiptToTxInfoV1 { origin, receiver_account_id, shard_id })
}

/// Iterate heights center-out around `block_height` up to `±window`, saturating at 0.
///
/// Order: `h, h-1, h+1, h-2, h+2, ...`. Both the "hint = creation block" and
/// "hint = child execution block" interpretations early-return well because
/// the parent typically executed at `h` or `h-1` of the child.
fn center_out_heights(
    block_height: BlockHeight,
    window: BlockHeightDelta,
) -> impl Iterator<Item = BlockHeight> {
    std::iter::once(block_height).chain((1..=window).flat_map(move |offset| {
        let low = block_height.saturating_sub(offset);
        let high = block_height.saturating_add(offset);
        // saturating_sub repeats height 0 once offset exceeds block_height; drop the
        // duplicate so the iterator visits each height at most once.
        let lower = (offset <= block_height).then_some(low);
        let upper = (low != high).then_some(high);
        [lower, upper].into_iter().flatten()
    }))
}

/// Iterate heights backward from `block_height` down to `block_height -
/// max_distance`, saturating at 0. The anchor is included.
///
/// Order: `h, h-1, h-2, ..., h-max_distance`. The anchor is included so the
/// scan still finds same-shard local receipts (which execute in the same
/// block as their producing outcome). Forward heights are never visited
/// because receipts are produced before they execute.
fn ancestor_heights(
    block_height: BlockHeight,
    max_distance: BlockHeightDelta,
) -> impl Iterator<Item = BlockHeight> {
    (0..=max_distance).map_while(move |offset| {
        if offset > block_height {
            return None;
        }
        Some(block_height - offset)
    })
}

/// Successful resolution of a parent outcome via the hint scan. Carries the
/// synthesized `ReceiptToTxInfo` plus the execution block height of the outcome
/// itself, which the caller uses to refresh `block_height` for the next hop.
/// The outcome's shard is *not* propagated: the next hop's scan target lives
/// on a different shard (the producer of the parent receipt). The handler
/// derives that shard from `ReceiptOriginReceipt.parent_predecessor_id` at
/// the resolved height.
pub struct HintResolution {
    pub info: ReceiptToTxInfo,
    /// Block height at which the parent outcome (transaction or receipt) executed.
    /// For receipt-origin parents, this is also the height at which the child
    /// receipt was created.
    pub outcome_block_height: BlockHeight,
}

/// Per-scan I/O accounting surfaced to the handler so it can drive the
/// hint-scan metrics regardless of whether the scan hit or missed.
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

/// Attempt to resolve the immediate parent of `receipt_id` by scanning
/// `OutcomeIds` / `TransactionResultForBlock` rows in a block range around
/// `block_height` on `shard_id`. The scan direction is set by `direction`:
/// hop 0 uses `CenterOut` (`±window` around a caller hint); hop 1+ uses
/// `Ancestor` (`h, h-1, ..., h-window` backward from the previously
/// resolved parent's execution height).
///
/// `Ok(Some(_))` — parent located, info synthesized in-flight.
/// `Ok(None)` — window exhausted without finding the receipt.
/// `Err(_)` — genuine I/O error or budget exhaustion mid-scan; bubbles to the handler.
///
/// `stats` is accumulated in-place so callers can emit metrics in every
/// outcome — hit, miss, or error mid-scan. Missing-data inside the scan
/// (no block at height, GC'd outcome row, deleted receipt row) is
/// skip-and-continue. `remaining_budget` is decremented for every outcome row
/// inspected so callers can enforce one budget across all shards and hops.
pub fn resolve_receipt_via_hint(
    chain_store: &ChainStore,
    receipt_id: CryptoHash,
    block_height: BlockHeight,
    shard_id: ShardId,
    window: BlockHeightDelta,
    direction: ScanDirection,
    stats: &mut HintScanStats,
    remaining_budget: &mut u64,
) -> Result<Option<HintResolution>, ResolveHintError> {
    let store = chain_store.store();

    let heights: Box<dyn Iterator<Item = BlockHeight>> = match direction {
        ScanDirection::CenterOut => Box::new(center_out_heights(block_height, window)),
        ScanDirection::Ancestor => Box::new(ancestor_heights(block_height, window)),
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

            // Both `Transactions` and `Receipts` are reference-counted and GC'd at
            // the historical horizon this endpoint exists to serve. Checking only
            // `Transactions` misclassifies tx-origin outcomes whose tx row has been
            // collected as receipt-origin and then silently fails the parent-receipt
            // lookup. Check both columns; skip the candidate when neither has the
            // row so the scan moves on to the next height.
            //
            // The two `exists` calls are not snapshotted. Concurrent GC between
            // them can downgrade a (true, true) candidate to (true, false) or
            // (false, true), or downgrade either to (false, false). The
            // worst-case outcome is a spurious skip — never a misclassification
            // — because both subsequent paths re-read from the same store
            // before producing a result.
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
                    // warn rather than error so a corrupted outcome row that
                    // recurs across many candidates in a single request doesn't
                    // flood error-level logging.
                    tracing::warn!(
                        %outcome_id,
                        height,
                        "outcome id present in both DBCol::Transactions and DBCol::Receipts; \
                         skipping ambiguous classification"
                    );
                    continue;
                }
            };

            let resolution = HintResolution {
                info: build_receipt_to_tx_info(origin, child.receiver_id().clone(), shard_id),
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
        // Offsets greater than block_height should drop the lower side, not produce duplicates.
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
        // Lock the invariant directly on the iterator: ancestor scan must
        // never emit a height greater than its anchor.
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
}
