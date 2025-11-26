use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::view_code::RpcViewCodeRequest;

use super::{Params, RpcRequest};

impl RpcRequest for RpcViewCodeRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::new(value).unwrap_or_parse()
    }
}
