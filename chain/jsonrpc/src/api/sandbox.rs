use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::sandbox::{
    RpcSandboxFastForwardRequest, RpcSandboxPatchStateRequest,
};

use super::{Params, RpcRequest};

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
