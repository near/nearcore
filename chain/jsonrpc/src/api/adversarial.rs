use serde_json::Value;

use near_jsonrpc_adversarial_primitives::SetRoutingTableRequest;
use near_jsonrpc_primitives::errors::RpcParseError;

use super::{parse_params, RpcRequest};

impl RpcRequest for SetRoutingTableRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}
