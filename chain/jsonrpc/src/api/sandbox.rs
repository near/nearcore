use near_async::messaging::AsyncSendError;
use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::sandbox::{
    RpcSandboxFastForwardError, RpcSandboxFastForwardRequest, RpcSandboxPatchStateError,
    RpcSandboxPatchStateRequest,
};
use near_jsonrpc_traits::ParamsTrait;

use super::{Params, RpcFrom, RpcRequest};

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
