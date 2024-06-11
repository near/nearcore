use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::congestion::RpcCongestionLevelRequest;
use serde_json::Value;

use super::chunks::parse_chunk_reference;
use super::RpcRequest;

impl RpcRequest for RpcCongestionLevelRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        let chunk_reference = parse_chunk_reference(value)?;
        Ok(Self { chunk_reference })
    }
}
