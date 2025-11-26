use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::view_state::RpcViewStateRequest;

use super::{Params, RpcRequest};

impl RpcRequest for RpcViewStateRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::new(value).unwrap_or_parse()
    }
}
