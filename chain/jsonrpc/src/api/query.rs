use serde_json::Value;

use near_client_primitives::types::QueryError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::query::{RpcQueryError, RpcQueryRequest, RpcQueryResponse};
use near_primitives::types::BlockReference;
use near_primitives::views::{QueryRequest, QueryResponse};

use super::{Params, RpcFrom, RpcRequest};

/// Max size of the query path (soft-deprecated)
const QUERY_DATA_MAX_SIZE: usize = 10 * 1024;

/// Parses base58-encoded data from legacy path+data request format.
fn parse_bs58_data(max_len: usize, encoded: String) -> Result<Vec<u8>, RpcParseError> {
    // N-byte encoded base58 string decodes to at most N bytes so there’s no
    // need to allocate full max_len output buffer if encoded length is shorter.
    let mut data = vec![0u8; max_len.min(encoded.len())];
    match bs58::decode(encoded.as_bytes()).into(data.as_mut_slice()) {
        Ok(len) => {
            data.truncate(len);
            Ok(data)
        }
        Err(bs58::decode::Error::BufferTooSmall) => {
            Err(RpcParseError("Query data size is too large".to_string()))
        }
        Err(err) => Err(RpcParseError(err.to_string())),
    }
}

impl RpcRequest for RpcQueryRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::new(value).try_pair(parse_path_data).unwrap_or_parse()
    }
}

fn parse_path_data(path: String, data: String) -> Result<RpcQueryRequest, RpcParseError> {
    // Handle a soft-deprecated version of the query API, which is based on
    // positional arguments with a "path"-style first argument.
    //
    // This whole block can be removed one day, when the new API is 100% adopted.

    let parse_data = || {
        let max_len = QUERY_DATA_MAX_SIZE.saturating_sub(path.len());
        parse_bs58_data(max_len, data)
    };

    let mut path_parts = path.splitn(3, '/');
    let make_err = || RpcParseError("Not enough query parameters provided".to_string());
    let query_command = path_parts.next().ok_or_else(make_err)?;
    let account_id = path_parts
        .next()
        .ok_or_else(make_err)?
        .parse()
        .map_err(|err| RpcParseError(format!("{}", err)))?;
    let maybe_extra_arg = path_parts.next();

    let request = match query_command {
        "account" => QueryRequest::ViewAccount { account_id },
        "access_key" => match maybe_extra_arg {
            None => QueryRequest::ViewAccessKeyList { account_id },
            Some(pk) => QueryRequest::ViewAccessKey {
                account_id,
                public_key: pk
                    .parse()
                    .map_err(|_| RpcParseError("Invalid public key".to_string()))?,
            },
        },
        "code" => QueryRequest::ViewCode { account_id },
        "contract" => QueryRequest::ViewState {
            account_id,
            prefix: parse_data()?.into(),
            include_proof: false,
        },
        "call" => match maybe_extra_arg {
            Some(method_name) => QueryRequest::CallFunction {
                account_id,
                method_name: method_name.to_string(),
                args: parse_data()?.into(),
            },
            None => return Err(RpcParseError("Method name is missing".to_string())),
        },
        _ => return Err(RpcParseError(format!("Unknown path {}", query_command))),
    };
    // Use Finality::None here to make backward compatibility tests work
    Ok(RpcQueryRequest { request, block_reference: BlockReference::latest() })
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
