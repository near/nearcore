use serde::de::DeserializeOwned;
use serde_json::Value;

use near_primitives::borsh::BorshDeserialize;

pub(crate) fn parse_params<T: DeserializeOwned>(
    value: Option<Value>,
) -> Result<T, crate::errors::RpcParseError> {
    if let Some(value) = value {
        serde_json::from_value(value)
            .map_err(|err| crate::errors::RpcParseError(format!("Failed parsing args: {}", err)))
    } else {
        Err(crate::errors::RpcParseError("Require at least one parameter".to_owned()))
    }
}

fn from_base64_or_parse_err(encoded: String) -> Result<Vec<u8>, crate::errors::RpcParseError> {
    near_primitives_core::serialize::from_base64(&encoded)
        .map_err(|err| crate::errors::RpcParseError(err.to_string()))
}

pub(crate) fn parse_signed_transaction(
    value: Option<Value>,
) -> Result<near_primitives::transaction::SignedTransaction, crate::errors::RpcParseError> {
    let (encoded,) = crate::utils::parse_params::<(String,)>(value.clone())?;
    let bytes = crate::utils::from_base64_or_parse_err(encoded)?;
    Ok(near_primitives::transaction::SignedTransaction::try_from_slice(&bytes).map_err(|err| {
        crate::errors::RpcParseError(format!("Failed to decode transaction: {}", err))
    })?)
}
