use serde::de::DeserializeOwned;
use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_primitives::borsh::BorshDeserialize;

#[cfg(feature = "test_features")]
mod adversarial;
mod blocks;
mod changes;
mod chunks;
mod config;
mod gas_price;
mod light_client;
mod network_info;
mod query;
mod receipts;
mod sandbox;
mod status;
mod transactions;
mod validator;

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
