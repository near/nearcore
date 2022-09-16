use serde_json::Value;

use near_client_primitives::types::QueryError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::query::{RpcQueryError, RpcQueryRequest, RpcQueryResponse};
use near_primitives::views::QueryResponse;

use super::{parse_params, RpcFrom, RpcRequest};

impl RpcRequest for RpcQueryRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        Ok(parse_params::<Self>(value)?)
    }
}

impl RpcFrom<actix::MailboxError> for RpcQueryError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<QueryError> for RpcQueryError {
    fn rpc_from(error: QueryError) -> Self {
        match error {
            QueryError::InternalError { error_message } => Self::InternalError { error_message },
            QueryError::NoSyncedBlocks => Self::NoSyncedBlocks,
            QueryError::UnavailableShard { requested_shard_id } => {
                Self::UnavailableShard { requested_shard_id }
            }
            QueryError::UnknownBlock { block_reference } => Self::UnknownBlock { block_reference },
            QueryError::GarbageCollectedBlock { block_height, block_hash } => {
                Self::GarbageCollectedBlock { block_height, block_hash }
            }
            QueryError::InvalidAccount { requested_account_id, block_height, block_hash } => {
                Self::InvalidAccount { requested_account_id, block_height, block_hash }
            }
            QueryError::UnknownAccount { requested_account_id, block_height, block_hash } => {
                Self::UnknownAccount { requested_account_id, block_height, block_hash }
            }
            QueryError::NoContractCode { contract_account_id, block_height, block_hash } => {
                Self::NoContractCode { contract_account_id, block_height, block_hash }
            }
            QueryError::UnknownAccessKey { public_key, block_height, block_hash } => {
                Self::UnknownAccessKey { public_key, block_height, block_hash }
            }
            QueryError::ContractExecutionError { vm_error, block_height, block_hash } => {
                Self::ContractExecutionError { vm_error, block_height, block_hash }
            }
            QueryError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcQueryError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
            QueryError::TooLargeContractState { contract_account_id, block_height, block_hash } => {
                Self::TooLargeContractState { contract_account_id, block_height, block_hash }
            }
        }
    }
}

impl RpcFrom<QueryResponse> for RpcQueryResponse {
    fn rpc_from(query_response: QueryResponse) -> Self {
        Self {
            kind: RpcFrom::rpc_from(query_response.kind),
            block_hash: query_response.block_hash,
            block_height: query_response.block_height,
        }
    }
}

impl RpcFrom<near_primitives::views::QueryResponseKind>
    for near_jsonrpc_primitives::types::query::QueryResponseKind
{
    fn rpc_from(query_response_kind: near_primitives::views::QueryResponseKind) -> Self {
        match query_response_kind {
            near_primitives::views::QueryResponseKind::ViewAccount(account_view) => {
                Self::ViewAccount(account_view)
            }
            near_primitives::views::QueryResponseKind::ViewCode(contract_code_view) => {
                Self::ViewCode(contract_code_view)
            }
            near_primitives::views::QueryResponseKind::ViewState(view_state_result) => {
                Self::ViewState(view_state_result)
            }
            near_primitives::views::QueryResponseKind::CallResult(call_result) => {
                Self::CallResult(call_result)
            }
            near_primitives::views::QueryResponseKind::AccessKey(access_key_view) => {
                Self::AccessKey(access_key_view)
            }
            near_primitives::views::QueryResponseKind::AccessKeyList(access_key_list) => {
                Self::AccessKeyList(access_key_list)
            }
        }
    }
}
