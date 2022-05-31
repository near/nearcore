use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::sandbox::{
    RpcSandboxFastForwardError, RpcSandboxFastForwardRequest, RpcSandboxPatchStateError,
    RpcSandboxPatchStateRequest,
};

use super::{parse_params, RpcFrom, RpcRequest};

impl RpcRequest for RpcSandboxPatchStateRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcRequest for RpcSandboxFastForwardRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcFrom<actix::MailboxError> for RpcSandboxPatchStateError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError> for RpcSandboxFastForwardError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}
