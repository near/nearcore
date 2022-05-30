use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::validator::{RpcValidatorRequest, RpcValidatorsOrderedRequest};
use near_primitives::types::{EpochReference, MaybeBlockId};

use super::{parse_params, RpcRequest};

impl RpcRequest for RpcValidatorRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let epoch_reference =
            if let Ok((block_id,)) = parse_params::<(MaybeBlockId,)>(value.clone()) {
                match block_id {
                    Some(id) => EpochReference::BlockId(id),
                    None => EpochReference::Latest,
                }
            } else {
                parse_params::<EpochReference>(value)?
            };
        Ok(Self { epoch_reference })
    }
}

impl RpcRequest for RpcValidatorsOrderedRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}
