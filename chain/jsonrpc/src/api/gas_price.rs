use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest;
use near_primitives::types::MaybeBlockId;

use super::{parse_params, RpcRequest};

impl RpcRequest for RpcGasPriceRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<(MaybeBlockId,)>(value).map(|(block_id,)| Self { block_id })
    }
}
