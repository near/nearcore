use serde::de::DeserializeOwned;
use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::errors::{RpcError, ServerError};
use near_primitives::borsh::BorshDeserialize;

mod blocks;
mod changes;
mod chunks;
mod client_config;
mod config;
mod gas_price;
mod light_client;
mod maintenance;
mod network_info;
mod query;
mod receipts;
mod sandbox;
mod split_storage;
mod status;
mod transactions;
mod validator;

pub(crate) trait RpcRequest: Sized {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError>;
}

impl RpcRequest for () {
    fn parse(_: Option<Value>) -> Result<Self, RpcParseError> {
        Ok(())
    }
}

pub trait RpcFrom<T> {
    fn rpc_from(_: T) -> Self;
}

pub trait RpcInto<T> {
    fn rpc_into(self) -> T;
}

impl<T> RpcFrom<T> for T {
    fn rpc_from(val: T) -> Self {
        val
    }
}

impl<T, X> RpcInto<X> for T
where
    X: RpcFrom<T>,
{
    fn rpc_into(self) -> X {
        X::rpc_from(self)
    }
}

impl RpcFrom<actix::MailboxError> for RpcError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        RpcError::new(
            -32_000,
            "Server error".to_string(),
            Some(serde_json::Value::String(error.to_string())),
        )
    }
}

impl RpcFrom<actix::MailboxError> for ServerError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        match error {
            actix::MailboxError::Closed => ServerError::Closed,
            actix::MailboxError::Timeout => ServerError::Timeout,
        }
    }
}

impl RpcFrom<near_primitives::errors::InvalidTxError> for ServerError {
    fn rpc_from(e: near_primitives::errors::InvalidTxError) -> ServerError {
        ServerError::TxExecutionError(near_primitives::errors::TxExecutionError::InvalidTxError(e))
    }
}

pub(crate) fn parse_params<T: DeserializeOwned>(value: Option<Value>) -> Result<T, RpcParseError> {
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
