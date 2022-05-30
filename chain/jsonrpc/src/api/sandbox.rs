use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::sandbox::{
    RpcSandboxFastForwardRequest, RpcSandboxPatchStateRequest,
};

use super::{parse_params, RpcRequest};

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
