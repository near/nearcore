use near_client_primitives::types::GetSplitStorageInfoError;
use near_jsonrpc_primitives::{
    errors::RpcParseError,
    types::split_storage::{RpcSplitStorageInfoError, RpcSplitStorageInfoRequest},
};
use serde_json::Value;

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcSplitStorageInfoRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcFrom<actix::MailboxError> for RpcSplitStorageInfoError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetSplitStorageInfoError> for RpcSplitStorageInfoError {
    fn rpc_from(error: GetSplitStorageInfoError) -> Self {
        match error {
            GetSplitStorageInfoError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            GetSplitStorageInfoError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcSplitStorageInfoError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
