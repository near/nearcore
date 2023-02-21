use serde_json::Value;

use near_client_primitives::types::{GetBlockError, GetStateChangesError};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::changes::{
    RpcStateChangesError, RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockRequest,
};

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcStateChangesInBlockRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcRequest for RpcStateChangesInBlockByTypeRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcFrom<actix::MailboxError> for RpcStateChangesError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetBlockError> for RpcStateChangesError {
    fn rpc_from(error: GetBlockError) -> Self {
        match error {
            GetBlockError::UnknownBlock { error_message } => Self::UnknownBlock { error_message },
            GetBlockError::NotSyncedYet => Self::NotSyncedYet,
            GetBlockError::IOError { error_message } => Self::InternalError { error_message },
            GetBlockError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStateChangesError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcFrom<GetStateChangesError> for RpcStateChangesError {
    fn rpc_from(error: GetStateChangesError) -> Self {
        match error {
            GetStateChangesError::IOError { error_message } => {
                Self::InternalError { error_message }
            }
            GetStateChangesError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            GetStateChangesError::NotSyncedYet => Self::NotSyncedYet,
            GetStateChangesError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStateChangesError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
