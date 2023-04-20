use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::errors::{RpcError, ServerError};

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

mod params {
    use serde::de::DeserializeOwned;
    use serde_json::Value;

    use near_jsonrpc_primitives::errors::RpcParseError;

    /// Helper wrapper for parsing JSON value into expected request format.
    ///
    /// If you don’t need to handle legacy APIs you most likely just want to do
    /// `Params::parse(params)` to convert JSON value into parameters format you
    /// expect.
    ///
    /// This is over-engineered to help handle legacy API for some of the JSON
    /// API requests.  For example, parameters for block request can be a block
    /// request object (the new API) or a one-element array with block id
    /// element (the old API).
    pub(crate) struct Params<T>(
        /// Regarding representation:
        /// - `Ok(Ok(value))` means value has been parsed successfully.  No
        ///   further parsing attempts will happen.
        /// - `Ok(Err(err))` means value has been parsed unsuccessfully
        ///   (resulting in a parse error).  No further parsing attempts will
        ///   happen.
        /// - `Err(value)` means value hasn’t been parsed yet and needs to be
        ///   parsed with one of the methods.
        Result<Result<T, RpcParseError>, Value>,
    );

    impl<T> Params<T> {
        pub fn new(params: Value) -> Self {
            Self(Err(params))
        }

        pub fn parse(value: Value) -> Result<T, RpcParseError>
        where
            T: DeserializeOwned,
        {
            serde_json::from_value(value)
                .map_err(|e| RpcParseError(format!("Failed parsing args: {e}")))
        }

        /// If value hasn’t been parsed yet, tries to deserialise it directly
        /// into `T`.
        pub fn unwrap_or_parse(self) -> Result<T, RpcParseError>
        where
            T: DeserializeOwned,
        {
            self.0.unwrap_or_else(Self::parse)
        }

        /// If value hasn’t been parsed yet, tries to deserialise it directly
        /// into `T` using given parse function.
        pub fn unwrap_or_else(
            self,
            func: impl FnOnce(Value) -> Result<T, RpcParseError>,
        ) -> Result<T, RpcParseError> {
            self.0.unwrap_or_else(func)
        }

        /// If value hasn’t been parsed yet and it’s a one-element array
        /// (i.e. singleton) deserialises the element and calls `func` on it.
        ///
        /// `try_singleton` and `try_pair` methods can be chained together
        /// (though it doesn’t make sense to use the same method twice) before
        /// a final `parse` call.
        pub fn try_singleton<U: DeserializeOwned>(
            self,
            func: impl FnOnce(U) -> Result<T, RpcParseError>,
        ) -> Self {
            Self(match self.0 {
                Err(Value::Array(mut array)) if array.len() == 1 => {
                    Ok(Params::parse(array[0].take()).and_then(func))
                }
                x => x,
            })
        }

        /// If value hasn’t been parsed yet and it’s a two-element array
        /// (i.e. couple) deserialises the element and calls `func` on it.
        ///
        /// `try_singleton` and `try_pair` methods can be chained together
        /// (though it doesn’t make sense to use the same method twice) before
        /// a final `parse` call.
        pub fn try_pair<U: DeserializeOwned, V: DeserializeOwned>(
            self,
            func: impl FnOnce(U, V) -> Result<T, RpcParseError>,
        ) -> Self {
            Self(match self.0 {
                Err(Value::Array(mut array)) if array.len() == 2 => {
                    Ok(Params::parse(array[0].take())
                        .and_then(|u| Ok((u, Params::parse(array[1].take())?)))
                        .and_then(|(u, v)| func(u, v)))
                }
                x => x,
            })
        }
    }
}

pub(crate) use params::Params;
