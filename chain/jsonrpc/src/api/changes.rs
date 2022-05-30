use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::changes::{
    RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockRequest,
};

use super::{parse_params, RpcRequest};

impl RpcRequest for RpcStateChangesInBlockRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcRequest for RpcStateChangesInBlockByTypeRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}
