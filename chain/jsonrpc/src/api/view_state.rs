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
        // TODO(#15612): a resumed page seeks with AccessOptions::NO_SIDE_EFFECTS, so it
        // doesn't record the trie path from the root and the proof can't chain back to
        // the state root. Until we record the seek's nodes, reject proof + pagination.
        if request.include_proof && (request.from_key.is_some() || request.limit.is_some()) {
            return Err(RpcParseError(
                "include_proof is not supported with paginated view_state \
                   (from_key_base64 / limit)"
                    .to_string(),
            ));
        }
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
