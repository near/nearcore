use serde_json::Value;

use near_client_primitives::types::GetProtocolConfigError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::config::{RpcProtocolConfigError, RpcProtocolConfigRequest};

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcProtocolConfigRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value).map(|block_reference| Self { block_reference })
    }
}

impl RpcFrom<actix::MailboxError> for RpcProtocolConfigError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetProtocolConfigError> for RpcProtocolConfigError {
    fn rpc_from(error: GetProtocolConfigError) -> Self {
        match error {
            GetProtocolConfigError::UnknownBlock(error_message) => {
                Self::UnknownBlock { error_message }
            }
            GetProtocolConfigError::IOError(error_message) => Self::InternalError { error_message },
            GetProtocolConfigError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcProtocolConfigError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
