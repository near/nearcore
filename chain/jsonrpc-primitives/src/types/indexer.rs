use crate::errors::RpcError;
use near_indexer_primitives::StreamerMessage;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;
use serde_json::to_value;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RpcIndexerBlockRequest {
    pub block_hash: CryptoHash,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcIndexerBlockResponse {
    /// Existing consumers can deserialize this result directly as StreamerMessage.
    #[serde(flatten)]
    pub message: StreamerMessage,
    /// The node's configured chunk and execution coverage, in block layout order.
    /// Carried chunks remain None inside the message even for tracked shards.
    pub tracked_shards: Vec<ShardId>,
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcIndexerBlockError {
    #[error("block or execution data unavailable: {error_message}")]
    DataUnavailable { error_message: String },
    #[error("indexer data incomplete: {error_message}")]
    IncompleteData { error_message: String },
    #[error("unsupported indexer request: {error_message}")]
    Unsupported { error_message: String },
    #[error("indexer response exceeds size limit")]
    LimitExceeded,
    #[error("indexer request concurrency limit reached")]
    Busy,
    #[error("internal error: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcIndexerBlockError> for RpcError {
    fn from(error: RpcIndexerBlockError) -> Self {
        match to_value(error) {
            Ok(data) => Self::new_internal_or_handler_error(Some(data.clone()), data),
            Err(error) => Self::new_internal_error(None, error.to_string()),
        }
    }
}
