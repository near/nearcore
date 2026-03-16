use super::{Params, RpcFrom, RpcRequest};
use near_async::messaging::AsyncSendError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::sandbox::{
    RpcSandboxFastForwardError, RpcSandboxFastForwardRequest, RpcSandboxPatchStateError,
    RpcSandboxPatchStateRequest,
};
use serde_json::Value;

impl RpcRequest for RpcSandboxPatchStateRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcRequest for RpcSandboxFastForwardRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcFrom<AsyncSendError> for RpcSandboxPatchStateError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<AsyncSendError> for RpcSandboxFastForwardError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}
