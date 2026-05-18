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

/// Default scan window (in blocks) for [`resolve_receipt_via_hint`] when the
/// caller does not specify one. The scan inspects heights `[h-window, h+window]`.
pub const DEFAULT_HINT_WINDOW: BlockHeightDelta = 5;

/// Maximum scan window that the hint-scan resolver will accept. Requests above
/// this cap are rejected so a single RPC call can't blow up I/O cost.
pub const MAX_HINT_WINDOW: BlockHeightDelta = 50;

/// Bundle the three components of a `ReceiptToTxInfo` record into a versioned value.
///
/// Used by both the bulk backfill path (`backfill_receipt_to_tx::process_height`)
/// and the live RPC hint-scan resolver. Centralising the constructor avoids
/// two copies of the variant-selection logic drifting out of sync.
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

/// Successful resolution of a parent outcome via the hint scan. Carries both
/// the synthesized `ReceiptToTxInfo` and the execution coordinates of the
/// outcome itself, so the caller can use `(outcome_block_height, outcome_shard_id)`
/// as the next-hop hint when recursing toward the originating transaction.
pub struct HintResolution {
    pub info: ReceiptToTxInfo,
    /// Block height at which the parent outcome (transaction or receipt) executed.
    /// For receipt-origin parents, this is also the height at which the child
    /// receipt was created.
    pub outcome_block_height: BlockHeight,
    /// Shard at which the parent outcome executed.
    pub outcome_shard_id: ShardId,
}

/// Attempt to resolve the immediate parent of `receipt_id` by scanning
/// `OutcomeIds` / `TransactionResultForBlock` rows in a `±window` block range
/// around `block_height` on `shard_id`.
///
/// `Ok(Some(_))` — parent located, info synthesized in-flight from chain data.
/// `Ok(None)` — window exhausted without finding the receipt; caller should
/// treat as `ReceiptNotFoundInHintWindow`.
/// `Err(_)` — genuine I/O error mid-scan; bubbles to the handler.
///
/// Missing-data inside the scan (no block at height, GC'd outcome row, deleted
/// receipt row) is skip-and-continue — the goal is best-effort historical
/// lookup, not a strict invariant check.
pub fn resolve_receipt_via_hint(
    chain_store: &ChainStore,
    receipt_id: CryptoHash,
    block_height: BlockHeight,
    shard_id: ShardId,
    window: BlockHeightDelta,
) -> Result<Option<HintResolution>, Error> {
    let store = chain_store.store();

    for height in center_out_heights(block_height, window) {
        let block_hash = match chain_store.get_block_hash_by_height(height) {
            Ok(h) => h,
            Err(Error::DBNotFoundErr(_)) => continue,
            Err(e) => return Err(e),
        };

        let outcome_ids =
            chain_store.get_outcomes_by_block_hash_and_shard_id(&block_hash, shard_id);
        for outcome_id in outcome_ids {
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
                    tracing::error!(
                        %outcome_id,
                        height,
                        "outcome id present in both DBCol::Transactions and DBCol::Receipts; \
                         skipping ambiguous classification"
                    );
                    continue;
                }
            };

            return Ok(Some(HintResolution {
                info: build_receipt_to_tx_info(origin, child.receiver_id().clone(), shard_id),
                outcome_block_height: height,
                outcome_shard_id: shard_id,
            }));
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
}
