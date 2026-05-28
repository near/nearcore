use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, ShardId};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ReceiptReference {
    pub receipt_id: CryptoHash,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptRequest {
    #[serde(flatten)]
    pub receipt_reference: ReceiptReference,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptResponse {
    #[serde(flatten)]
    pub receipt_view: near_primitives::views::ReceiptView,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcReceiptError {
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    #[error("Receipt with id {receipt_id} has never been observed on this node")]
    UnknownReceipt { receipt_id: CryptoHash },
}

impl From<RpcReceiptError> for crate::errors::RpcError {
    fn from(error: RpcReceiptError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcReceiptError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptToTxRequest {
    #[serde(flatten)]
    pub receipt_reference: ReceiptReference,
    /// Optional block height near where the receipt was created. Supplying
    /// it enables a best-effort hinted fallback scan when the local
    /// `ReceiptToTx` column is missing an entry mid-walk. The handler uses
    /// this height as the anchor for the first hint scan. Each scan that
    /// resolves a hop refreshes the anchor to that resolved parent's exact
    /// execution height. After the first scan resolves a hop, the anchor
    /// is an upper bound for every later ancestor in the walk (causality:
    /// receipts emit before they execute). All subsequent column-miss
    /// scans use `Ancestor` direction, anchored on the most-recent
    /// scan-refreshed height. A missing `ReceiptToTx` row has no block
    /// height of its own; the gap that matters for `max_hop_distance` is
    /// from the scan-refreshed anchor to the producer-outcome height of
    /// the receipt whose column row is missing. Bump
    /// `receipt_to_tx_max_hop_distance` if your workload sees gaps wider
    /// than the default 20 blocks.
    ///
    /// Cold-storage usage: this endpoint primarily serves historical queries,
    /// so the scan typically reads from cold storage where per-row latency is
    /// orders of magnitude higher than hot. To keep request cost bounded,
    /// callers should:
    ///   - Supply `block_height` within the parent outcome's `±window` range
    ///     (default 5 blocks).
    ///   - Supply `shard_id` when the producing shard is known. Omitting
    ///     it leaves `current_shard` unset until the walk crosses a
    ///     `FromReceipt` arm (which derives the shard via
    ///     predecessor-account lookup); any scan that runs before that
    ///     enumerates all tracked shards, multiplying cold-read cost.
    ///   - Avoid increasing `window` beyond what the indexer's height
    ///     estimate actually requires; the scan budget is shared across the
    ///     full ancestry walk.
    ///
    /// Receipt-id-only queries against periods where `save_receipt_to_tx`
    /// was disabled at processing time remain unsupported — the column was
    /// never written and this endpoint provides no self-locating mechanism.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_height: Option<BlockHeight>,
    /// Shard hint. Narrows the scan to this shard at the hint height. Omit
    /// to enumerate all tracked shards (higher cost). After the walker
    /// crosses a receipt-origin hop the shard is derived from the parent's
    /// predecessor account and this hint no longer applies. Best-effort
    /// across resharding: layout shifts can miss the producer, walk returns
    /// `UnknownReceipt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<ShardId>,
    /// Pre-first-scan width: `±window` heights around the hint. Caps at the
    /// node's `receipt_to_tx_max_hint_window` (default 20). Ignored after
    /// the first scan-resolved hop; the walker switches to `Ancestor` mode
    /// at `receipt_to_tx_max_hop_distance` width.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<BlockHeightDelta>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcReceiptToTxResponse {
    pub transaction_hash: CryptoHash,
    pub sender_account_id: AccountId,
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum RpcReceiptToTxError {
    #[error("Receipt with id {receipt_id} has never been observed on this node")]
    UnknownReceipt { receipt_id: CryptoHash },
    #[error("depth limit {limit} exceeded when resolving receipt {receipt_id}")]
    DepthExceeded { receipt_id: CryptoHash, limit: u32 },
    #[error("this node does not support receipt-to-tx lookup: {error_message}")]
    Unsupported { error_message: String },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    #[error("execution outcomes are not stored on this node (save_tx_outcomes=false)")]
    OutcomesNotStored,
    #[error("requested window {requested} exceeds maximum {maximum}")]
    WindowTooLarge { requested: BlockHeightDelta, maximum: BlockHeightDelta },
    #[error("malformed hint: {error_message}")]
    MalformedHint { error_message: String },
    #[error("hint-scan budget exceeded: {scanned} outcomes scanned, limit {limit}")]
    BudgetExceeded { scanned: u64, limit: u64 },
}

impl From<RpcReceiptToTxError> for crate::errors::RpcError {
    fn from(error: RpcReceiptToTxError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcReceiptToTxError: {:?}", err),
                );
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}
