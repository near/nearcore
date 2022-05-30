use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::blocks::RpcBlockRequest;
use near_primitives::types::{BlockId, BlockReference};

use super::{parse_params, RpcRequest};

impl RpcRequest for RpcBlockRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let block_reference = if let Ok((block_id,)) = parse_params::<(BlockId,)>(value.clone()) {
            BlockReference::BlockId(block_id)
        } else {
            parse_params::<BlockReference>(value)?
        };
        Ok(Self { block_reference })
    }
}
