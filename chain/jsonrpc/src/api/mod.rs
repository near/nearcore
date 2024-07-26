use near_async::messaging::AsyncSendError;
use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::errors::{RpcError, ServerError};

mod blocks;
mod changes;
mod chunks;
mod client_config;
mod config;
mod congestion;
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
    fn parse(value: Value) -> Result<Self, RpcParseError>;
}

impl RpcRequest for () {
    fn parse(_: Value) -> Result<Self, RpcParseError> {
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

impl RpcFrom<AsyncSendError> for RpcError {
    fn rpc_from(error: AsyncSendError) -> Self {
        RpcError::new(
            -32_000,
            "Server error".to_string(),
            Some(serde_json::Value::String(error.to_string())),
        )
    }
}

impl RpcFrom<AsyncSendError> for ServerError {
    fn rpc_from(error: AsyncSendError) -> Self {
        match error {
            AsyncSendError::Timeout => ServerError::Timeout,
            AsyncSendError::Closed => ServerError::Closed,
            AsyncSendError::Dropped => ServerError::Closed,
        }
    }
}

impl RpcFrom<near_primitives::errors::InvalidTxError> for ServerError {
    fn rpc_from(e: near_primitives::errors::InvalidTxError) -> ServerError {
        ServerError::TxExecutionError(near_primitives::errors::TxExecutionError::InvalidTxError(e))
    }
}
