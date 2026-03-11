use super::{Params, RpcFrom, RpcRequest};
use near_async::messaging::AsyncSendError;
use near_client_primitives::types::QueryError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::query::RpcQueryError;
use near_jsonrpc_primitives::types::view_access_key_list::{
    RpcViewAccessKeyListError, RpcViewAccessKeyListRequest,
};
use serde_json::Value;

impl RpcRequest for RpcViewAccessKeyListRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcFrom<AsyncSendError> for RpcViewAccessKeyListError {
    fn rpc_from(error: AsyncSendError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}

impl RpcFrom<QueryError> for RpcViewAccessKeyListError {
    fn rpc_from(error: QueryError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}
