use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest;
use near_primitives::types::BlockReference;

use super::{parse_params, RpcRequest};

impl RpcRequest for RpcProtocolConfigRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<BlockReference>(value).map(|block_reference| Self { block_reference })
    }
}
