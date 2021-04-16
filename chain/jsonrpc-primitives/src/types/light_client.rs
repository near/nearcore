use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcLightClientExecutionProofRequest {
    #[serde(flatten)]
    pub id: near_primitives::types::TransactionOrReceiptId,
    pub light_client_head: near_primitives::hash::CryptoHash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcLightClientNextBlockRequest {
    pub last_block_hash: near_primitives::hash::CryptoHash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcLightClientExecutionProofResponse {
    pub outcome_proof: near_primitives::views::ExecutionOutcomeWithIdView,
    pub outcome_root_proof: near_primitives::merkle::MerklePath,
    pub block_header_lite: near_primitives::views::LightClientBlockLiteView,
    pub block_proof: near_primitives::merkle::MerklePath,
}

#[derive(Debug, Serialize)]
pub struct RpcLightClientNextBlockResponse {
    #[serde(flatten)]
    pub light_client_block: Option<near_primitives::views::LightClientBlockView>,
}

#[derive(thiserror::Error, Debug, Serialize)]
pub enum RpcLightClientProofError {
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
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
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

#[derive(thiserror::Error, Debug, Serialize)]
pub enum RpcLightClientNextBlockError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
    #[error("Block either has never been observed on the node or has been garbage collected: {error_message}")]
    UnknownBlock { error_message: String },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

impl RpcLightClientExecutionProofRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        Ok(crate::utils::parse_params::<Self>(value)?)
    }
}

impl RpcLightClientNextBlockRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        if let Ok((last_block_hash,)) =
            crate::utils::parse_params::<(near_primitives::hash::CryptoHash,)>(value.clone())
        {
            Ok(Self { last_block_hash })
        } else {
            Ok(crate::utils::parse_params::<Self>(value)?)
        }
    }
}

impl From<Option<near_primitives::views::LightClientBlockView>>
    for RpcLightClientNextBlockResponse
{
    fn from(light_client_block: Option<near_primitives::views::LightClientBlockView>) -> Self {
        Self { light_client_block }
    }
}

impl From<near_client_primitives::types::GetExecutionOutcomeError> for RpcLightClientProofError {
    fn from(error: near_client_primitives::types::GetExecutionOutcomeError) -> Self {
        match error {
            near_client_primitives::types::GetExecutionOutcomeError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            },
            near_client_primitives::types::GetExecutionOutcomeError::InconsistentState {
                number_or_shards, execution_outcome_shard_id
            } => Self::InconsistentState { number_or_shards, execution_outcome_shard_id },
            near_client_primitives::types::GetExecutionOutcomeError::NotConfirmed {
                transaction_or_receipt_id
            } => Self::NotConfirmed { transaction_or_receipt_id },
            near_client_primitives::types::GetExecutionOutcomeError::UnknownTransactionOrReceipt {
                transaction_or_receipt_id
            } => Self::UnknownTransactionOrReceipt { transaction_or_receipt_id },
            near_client_primitives::types::GetExecutionOutcomeError::UnavailableShard {
                transaction_or_receipt_id,
                shard_id
            } => Self::UnavailableShard { transaction_or_receipt_id, shard_id },
            near_client_primitives::types::GetExecutionOutcomeError::InternalError { error_message } => {
                Self::InternalError { error_message }
            },
            near_client_primitives::types::GetExecutionOutcomeError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcLightClientProofError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<near_client_primitives::types::GetBlockProofError> for RpcLightClientProofError {
    fn from(error: near_client_primitives::types::GetBlockProofError) -> Self {
        match error {
            near_client_primitives::types::GetBlockProofError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            near_client_primitives::types::GetBlockProofError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetBlockProofError::Unreachable { error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcLightClientProofError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<near_client_primitives::types::GetNextLightClientBlockError>
    for RpcLightClientNextBlockError
{
    fn from(error: near_client_primitives::types::GetNextLightClientBlockError) -> Self {
        match error {
            near_client_primitives::types::GetNextLightClientBlockError::InternalError {
                error_message,
            } => Self::InternalError { error_message },
            near_client_primitives::types::GetNextLightClientBlockError::UnknownBlock {
                error_message,
            } => Self::UnknownBlock { error_message },
            near_client_primitives::types::GetNextLightClientBlockError::EpochOutOfBounds {
                epoch_id,
            } => Self::EpochOutOfBounds { epoch_id },
            near_client_primitives::types::GetNextLightClientBlockError::Unreachable {
                error_message,
            } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcLightClientNextBlockError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcLightClientProofError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<actix::MailboxError> for RpcLightClientNextBlockError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<RpcLightClientProofError> for crate::errors::RpcError {
    fn from(error: RpcLightClientProofError) -> Self {
        let error_data = match error {
            RpcLightClientProofError::UnknownBlock { error_message } => {
                Some(Value::String(format!("DB Not Found Error: {}", error_message)))
            }
            _ => Some(Value::String(error.to_string())),
        };

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}

impl From<RpcLightClientNextBlockError> for crate::errors::RpcError {
    fn from(error: RpcLightClientNextBlockError) -> Self {
        let error_data = Some(Value::String(error.to_string()));

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}
