use serde_json::Value;
use std::sync::Arc;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcLightClientExecutionProofRequest {
    #[serde(flatten)]
    pub id: near_primitives::types::TransactionOrReceiptId,
    pub light_client_head: near_primitives::hash::CryptoHash,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcLightClientNextBlockRequest {
    pub last_block_hash: near_primitives::hash::CryptoHash,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcLightClientExecutionProofResponse {
    pub outcome_proof: near_primitives::views::ExecutionOutcomeWithIdView,
    pub outcome_root_proof: near_primitives::merkle::MerklePath,
    pub block_header_lite: near_primitives::views::LightClientBlockLiteView,
    pub block_proof: near_primitives::merkle::MerklePath,
}

#[derive(Debug, serde::Serialize)]
pub struct RpcLightClientNextBlockResponse {
    #[serde(flatten)]
    pub light_client_block: Option<Arc<near_primitives::views::LightClientBlockView>>,
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcLightClientProofError {
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
    },
    #[error("Inconsistent state. Total number of shards is {number_or_shards} but the execution outcome is in shard {execution_outcome_shard_id}")]
    InconsistentState {
        number_or_shards: usize,
        execution_outcome_shard_id: near_primitives::types::ShardId,
    },
    #[error("{transaction_or_receipt_id} has not been confirmed")]
    NotConfirmed { transaction_or_receipt_id: near_primitives::hash::CryptoHash },
    #[error("{transaction_or_receipt_id} does not exist")]
    UnknownTransactionOrReceipt { transaction_or_receipt_id: near_primitives::hash::CryptoHash },
    #[error("Node doesn't track the shard where {transaction_or_receipt_id} is executed")]
    UnavailableShard {
        transaction_or_receipt_id: near_primitives::hash::CryptoHash,
        shard_id: near_primitives::types::ShardId,
    },
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
}

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcLightClientNextBlockError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock {
        #[serde(skip_serializing)]
        error_message: String,
    },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
}

impl From<RpcLightClientProofError> for crate::errors::RpcError {
    fn from(error: RpcLightClientProofError) -> Self {
        let error_data = match &error {
            RpcLightClientProofError::UnknownBlock { error_message } => {
                Some(Value::String(format!("DB Not Found Error: {}", error_message)))
            }
            _ => Some(Value::String(error.to_string())),
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcLightClientProofError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}

impl From<RpcLightClientNextBlockError> for crate::errors::RpcError {
    fn from(error: RpcLightClientNextBlockError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcLightClientNextBlockError: {:?}", err),
                )
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}
