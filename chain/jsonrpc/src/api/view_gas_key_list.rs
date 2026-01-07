use near_async::messaging::AsyncSendError;
use near_client_primitives::types::QueryError;
use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::query::RpcQueryError;
use near_jsonrpc_primitives::types::view_gas_key_list::{
    RpcViewGasKeyListError, RpcViewGasKeyListRequest,
};

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcViewGasKeyListRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcFrom<AsyncSendError> for RpcViewGasKeyListError {
    fn rpc_from(error: AsyncSendError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}

impl RpcFrom<QueryError> for RpcViewGasKeyListError {
    fn rpc_from(error: QueryError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}
