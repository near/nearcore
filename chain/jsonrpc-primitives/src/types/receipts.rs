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
    /// execution height; the next column-miss scan (if it immediately
    /// follows the scan-resolved hop) anchors on that exact height. Column
    /// hits do not refresh the anchor, so a column-miss scan that follows a
    /// column hit re-uses whatever height the prior scan wrote (or the
    /// caller's hint, if no scan has run).
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
    /// Optional shard hint consumed by any hint scan that runs before the
    /// walk has crossed a `FromReceipt` arm. Once a `FromReceipt` arm
    /// fires, the handler overwrites `current_shard` via predecessor-account
    /// derivation (`shard_for_account_at_height(parent_predecessor_id,
    /// current_height)`), so this field does not narrow the rest of the
    /// walk. If the very first walker step is a column hit returning
    /// `FromReceipt`, the caller's shard hint is discarded without being
    /// used by any scan. Omitting `shard_id` leaves `current_shard` unset;
    /// any scan that runs before a `FromReceipt` arm has derived it then
    /// enumerates all tracked shards at the hint height, multiplying the
    /// scan budget by the number of shards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<ShardId>,
    /// Optional override for the `±window` scan range used whenever the
    /// handler scans against a caller-supplied or stale anchor (i.e.
    /// against `current_height` that was not refreshed by an immediately
    /// preceding scan). Defaults to `DEFAULT_HINT_WINDOW` when omitted;
    /// rejected with `WindowTooLarge` when greater than the node's
    /// `receipt_to_tx_max_hint_window` setting (default 20). On cold
    /// storage, every extra block in the window translates directly into
    /// additional remote reads — keep this tight.
    ///
    /// This field does not control scan width on a scan that immediately
    /// follows a scan-resolved hop; those use a backward iterator capped
    /// by the node's `receipt_to_tx_max_hop_distance` config (default
    /// 10), not by `window`. If a column hit follows a scan-resolved
    /// hop, the next column-miss scan reverts to `CenterOut` with this
    /// `window` again — the policy tracks the immediately preceding
    /// hop, not the walk's all-time scan history.
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
