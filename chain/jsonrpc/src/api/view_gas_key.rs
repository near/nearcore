use near_async::messaging::AsyncSendError;
use near_client_primitives::types::QueryError;
use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::query::RpcQueryError;
use near_jsonrpc_primitives::types::view_gas_key::{RpcViewGasKeyError, RpcViewGasKeyRequest};

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcViewGasKeyRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::new(value).unwrap_or_parse()
    }
}

impl RpcFrom<AsyncSendError> for RpcViewGasKeyError {
    fn rpc_from(error: AsyncSendError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}

impl RpcFrom<QueryError> for RpcViewGasKeyError {
    fn rpc_from(error: QueryError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}
