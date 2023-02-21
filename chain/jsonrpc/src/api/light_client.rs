use std::sync::Arc;

use serde_json::Value;

use near_client_primitives::types::{
    GetBlockProofError, GetExecutionOutcomeError, GetNextLightClientBlockError,
};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::light_client::{
    RpcLightClientExecutionProofRequest, RpcLightClientNextBlockError,
    RpcLightClientNextBlockRequest, RpcLightClientNextBlockResponse, RpcLightClientProofError,
};
use near_primitives::views::LightClientBlockView;

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcLightClientExecutionProofRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcRequest for RpcLightClientNextBlockRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::new(value)
            .try_singleton(|last_block_hash| Ok(Self { last_block_hash }))
            .unwrap_or_parse()
    }
}

impl RpcFrom<Option<Arc<LightClientBlockView>>> for RpcLightClientNextBlockResponse {
    fn rpc_from(light_client_block: Option<Arc<LightClientBlockView>>) -> Self {
        Self { light_client_block }
    }
}

impl RpcFrom<GetExecutionOutcomeError> for RpcLightClientProofError {
    fn rpc_from(error: GetExecutionOutcomeError) -> Self {
        match error {
            GetExecutionOutcomeError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            GetExecutionOutcomeError::InconsistentState {
                number_or_shards,
                execution_outcome_shard_id,
            } => Self::InconsistentState { number_or_shards, execution_outcome_shard_id },
            GetExecutionOutcomeError::NotConfirmed { transaction_or_receipt_id } => {
                Self::NotConfirmed { transaction_or_receipt_id }
            }
            GetExecutionOutcomeError::UnknownTransactionOrReceipt { transaction_or_receipt_id } => {
                Self::UnknownTransactionOrReceipt { transaction_or_receipt_id }
            }
            GetExecutionOutcomeError::UnavailableShard { transaction_or_receipt_id, shard_id } => {
                Self::UnavailableShard { transaction_or_receipt_id, shard_id }
            }
            GetExecutionOutcomeError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            GetExecutionOutcomeError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcLightClientProofError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcFrom<actix::MailboxError> for RpcLightClientProofError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetBlockProofError> for RpcLightClientProofError {
    fn rpc_from(error: GetBlockProofError) -> Self {
        match error {
            GetBlockProofError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            GetBlockProofError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            GetBlockProofError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcLightClientProofError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcFrom<actix::MailboxError> for RpcLightClientNextBlockError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetNextLightClientBlockError> for RpcLightClientNextBlockError {
    fn rpc_from(error: GetNextLightClientBlockError) -> Self {
        match error {
            GetNextLightClientBlockError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            GetNextLightClientBlockError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            GetNextLightClientBlockError::EpochOutOfBounds { epoch_id } => {
                Self::EpochOutOfBounds { epoch_id }
            }
            GetNextLightClientBlockError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcLightClientNextBlockError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
