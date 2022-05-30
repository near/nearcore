use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::light_client::{
    RpcLightClientExecutionProofRequest, RpcLightClientNextBlockRequest,
};
use near_primitives::hash::CryptoHash;

use super::{parse_params, RpcRequest};

impl RpcRequest for RpcLightClientExecutionProofRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        Ok(parse_params::<Self>(value)?)
    }
}

impl RpcRequest for RpcLightClientNextBlockRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        if let Ok((last_block_hash,)) = parse_params::<(CryptoHash,)>(value.clone()) {
            Ok(Self { last_block_hash })
        } else {
            Ok(parse_params::<Self>(value)?)
        }
    }
}
