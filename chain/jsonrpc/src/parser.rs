use serde::de::DeserializeOwned;
use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_primitives::borsh::BorshDeserialize;

pub(crate) trait RpcRequest: Sized {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError>;
}

fn parse_params<T: DeserializeOwned>(value: Option<Value>) -> Result<T, RpcParseError> {
    if let Some(value) = value {
        serde_json::from_value(value)
            .map_err(|err| RpcParseError(format!("Failed parsing args: {}", err)))
    } else {
        Err(RpcParseError("Require at least one parameter".to_owned()))
    }
}

fn parse_signed_transaction(
    value: Option<Value>,
) -> Result<near_primitives::transaction::SignedTransaction, RpcParseError> {
    let (encoded,) = parse_params::<(String,)>(value)?;
    let bytes = near_primitives::serialize::from_base64(&encoded)
        .map_err(|err| RpcParseError(err.to_string()))?;
    Ok(near_primitives::transaction::SignedTransaction::try_from_slice(&bytes)
        .map_err(|err| RpcParseError(format!("Failed to decode transaction: {}", err)))?)
}

impl RpcRequest for near_jsonrpc_primitives::types::blocks::RpcBlockRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let block_reference = if let Ok((block_id,)) =
            parse_params::<(near_primitives::types::BlockId,)>(value.clone())
        {
            near_primitives::types::BlockReference::BlockId(block_id)
        } else {
            parse_params::<near_primitives::types::BlockReference>(value)?
        };
        Ok(Self { block_reference })
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockByTypeRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::chunks::RpcChunkRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        // Try to parse legacy positioned args and if it fails parse newer named args
        let chunk_reference = if let Ok((chunk_id,)) =
            parse_params::<(near_primitives::hash::CryptoHash,)>(value.clone())
        {
            near_jsonrpc_primitives::types::chunks::ChunkReference::ChunkHash { chunk_id }
        } else if let Ok(((block_id, shard_id),)) = parse_params::<((
            near_primitives::types::BlockId,
            near_primitives::types::ShardId,
        ),)>(value.clone())
        {
            near_jsonrpc_primitives::types::chunks::ChunkReference::BlockShardId {
                block_id,
                shard_id,
            }
        } else {
            parse_params::<near_jsonrpc_primitives::types::chunks::ChunkReference>(value)?
        };
        Ok(Self { chunk_reference })
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<near_primitives::types::BlockReference>(value)
            .map(|block_reference| Self { block_reference })
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<(near_primitives::types::MaybeBlockId,)>(value)
            .map(|(block_id,)| Self { block_id })
    }
}

impl RpcRequest
    for near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest
{
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        Ok(parse_params::<Self>(value)?)
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        if let Ok((last_block_hash,)) =
            parse_params::<(near_primitives::hash::CryptoHash,)>(value.clone())
        {
            Ok(Self { last_block_hash })
        } else {
            Ok(parse_params::<Self>(value)?)
        }
    }
}

/// Max size of the query path (soft-deprecated)
const QUERY_DATA_MAX_SIZE: usize = 10 * 1024;

impl RpcRequest for near_jsonrpc_primitives::types::query::RpcQueryRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let query_request = if let Ok((path, data)) =
            parse_params::<(String, String)>(value.clone())
        {
            // Handle a soft-deprecated version of the query API, which is based on
            // positional arguments with a "path"-style first argument.
            //
            // This whole block can be removed one day, when the new API is 100% adopted.
            let data = near_primitives::serialize::from_base(&data)
                .map_err(|err| RpcParseError(err.to_string()))?;
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
                "account" => near_primitives::views::QueryRequest::ViewAccount { account_id },
                "access_key" => match maybe_extra_arg {
                    None => near_primitives::views::QueryRequest::ViewAccessKeyList { account_id },
                    Some(pk) => near_primitives::views::QueryRequest::ViewAccessKey {
                        account_id,
                        public_key: pk
                            .parse()
                            .map_err(|_| RpcParseError("Invalid public key".to_string()))?,
                    },
                },
                "code" => near_primitives::views::QueryRequest::ViewCode { account_id },
                "contract" => near_primitives::views::QueryRequest::ViewState {
                    account_id,
                    prefix: data.into(),
                },
                "call" => match maybe_extra_arg {
                    Some(method_name) => near_primitives::views::QueryRequest::CallFunction {
                        account_id,
                        method_name: method_name.to_string(),
                        args: data.into(),
                    },
                    None => return Err(RpcParseError("Method name is missing".to_string())),
                },
                _ => return Err(RpcParseError(format!("Unknown path {}", query_command))),
            };
            // Use Finality::None here to make backward compatibility tests work
            Self { request, block_reference: near_primitives::types::BlockReference::latest() }
        } else {
            parse_params::<Self>(value)?
        };
        Ok(query_request)
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::receipts::RpcReceiptRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let receipt_reference =
            parse_params::<near_jsonrpc_primitives::types::receipts::ReceiptReference>(value)?;
        Ok(Self { receipt_reference })
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::sandbox::RpcSandboxPatchStateRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::sandbox::RpcSandboxFastForwardRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let signed_transaction = parse_signed_transaction(value)?;
        Ok(Self { signed_transaction })
    }
}

impl RpcRequest
    for near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest
{
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        if let Ok((hash, account_id)) = parse_params::<(
            near_primitives::hash::CryptoHash,
            near_primitives::types::AccountId,
        )>(value.clone())
        {
            let transaction_info =
                near_jsonrpc_primitives::types::transactions::TransactionInfo::TransactionId {
                    hash,
                    account_id,
                };
            Ok(Self { transaction_info })
        } else {
            let signed_transaction = parse_signed_transaction(value)?;
            let transaction_info =
                near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(
                    signed_transaction,
                );
            Ok(Self { transaction_info })
        }
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::validator::RpcValidatorRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let epoch_reference = if let Ok((block_id,)) =
            parse_params::<(near_primitives::types::MaybeBlockId,)>(value.clone())
        {
            match block_id {
                Some(id) => near_primitives::types::EpochReference::BlockId(id),
                None => near_primitives::types::EpochReference::Latest,
            }
        } else {
            parse_params::<near_primitives::types::EpochReference>(value)?
        };
        Ok(Self { epoch_reference })
    }
}

impl RpcRequest for near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

#[cfg(feature = "test_features")]
impl RpcRequest for near_jsonrpc_adversarial_primitives::SetRoutingTableRequest {
    fn parse(value: Option<serde_json::Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}
