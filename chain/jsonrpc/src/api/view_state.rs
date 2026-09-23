use super::{Params, RpcFrom, RpcRequest};
use near_async::messaging::AsyncSendError;
use near_client_primitives::types::QueryError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::query::RpcQueryError;
use near_jsonrpc_primitives::types::view_state::{RpcViewStateError, RpcViewStateRequest};
use serde_json::Value;

impl RpcRequest for RpcViewStateRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        let request: Self = Params::parse(value)?;
        super::validate_view_state_pagination(
            request.prefix.as_slice(),
            request.after_key.as_ref().map(|k| k.as_slice()),
            request.limit,
            request.include_proof,
        )?;
        Ok(request)
    }
}

impl RpcFrom<AsyncSendError> for RpcViewStateError {
    fn rpc_from(error: AsyncSendError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}

impl RpcFrom<QueryError> for RpcViewStateError {
    fn rpc_from(error: QueryError) -> Self {
        RpcQueryError::rpc_from(error).into()
    }
}
