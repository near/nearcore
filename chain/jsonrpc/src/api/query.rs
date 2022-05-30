use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::query::RpcQueryRequest;
use near_primitives::serialize;
use near_primitives::types::BlockReference;
use near_primitives::views::QueryRequest;

use super::{parse_params, RpcRequest};

/// Max size of the query path (soft-deprecated)
const QUERY_DATA_MAX_SIZE: usize = 10 * 1024;

impl RpcRequest for RpcQueryRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let query_request = if let Ok((path, data)) =
            parse_params::<(String, String)>(value.clone())
        {
            // Handle a soft-deprecated version of the query API, which is based on
            // positional arguments with a "path"-style first argument.
            //
            // This whole block can be removed one day, when the new API is 100% adopted.
            let data = serialize::from_base(&data).map_err(|err| RpcParseError(err.to_string()))?;
            let query_data_size = path.len() + data.len();
            if query_data_size > QUERY_DATA_MAX_SIZE {
                return Err(RpcParseError(format!(
                    "Query data size {} is too large",
                    query_data_size
                )));
            }

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
                "contract" => QueryRequest::ViewState { account_id, prefix: data.into() },
                "call" => match maybe_extra_arg {
                    Some(method_name) => QueryRequest::CallFunction {
                        account_id,
                        method_name: method_name.to_string(),
                        args: data.into(),
                    },
                    None => return Err(RpcParseError("Method name is missing".to_string())),
                },
                _ => return Err(RpcParseError(format!("Unknown path {}", query_command))),
            };
            // Use Finality::None here to make backward compatibility tests work
            Self { request, block_reference: BlockReference::latest() }
        } else {
            parse_params::<Self>(value)?
        };
        Ok(query_request)
    }
}
