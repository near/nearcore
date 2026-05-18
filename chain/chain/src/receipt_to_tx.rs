use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo, ReceiptToTxInfoV1};
use near_primitives::types::{AccountId, ShardId};

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
