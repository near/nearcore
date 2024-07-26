use near_jsonrpc_primitives::errors::RpcParseError;

use serde::de::DeserializeOwned;
use serde_json::Value;

pub trait ParamsExt<T> {
    fn new(params: Value) -> Self;
    fn parse(value: Value) -> Result<T, RpcParseError>
    where
        T: DeserializeOwned;
    fn unwrap_or_parse(self) -> Result<T, RpcParseError>
    where
        T: DeserializeOwned;
    fn try_singleton<U: DeserializeOwned>(
        self,
        func: impl FnOnce(U) -> Result<T, RpcParseError>,
    ) -> Self;
    fn try_pair<U: DeserializeOwned, V: DeserializeOwned>(
        self,
        func: impl FnOnce(U, V) -> Result<T, RpcParseError>,
    ) -> Self;
}

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
pub struct Params<T>(
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

impl<T> ParamsExt<T> for Params<T> {
    fn new(params: Value) -> Self {
        Self(Err(params))
    }

    fn parse(value: Value) -> Result<T, RpcParseError>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(value)
            .map_err(|e| RpcParseError(format!("Failed parsing args: {e}")))
    }

    /// If value hasn’t been parsed yet, tries to deserialise it directly
    /// into `T`.
    fn unwrap_or_parse(self) -> Result<T, RpcParseError>
    where
        T: DeserializeOwned,
    {
        self.0.unwrap_or_else(Self::parse)
    }

    /// If value hasn’t been parsed yet and it’s a one-element array
    /// (i.e. singleton) deserialises the element and calls `func` on it.
    ///
    /// `try_singleton` and `try_pair` methods can be chained together
    /// (though it doesn’t make sense to use the same method twice) before
    /// a final `parse` call.
    fn try_singleton<U: DeserializeOwned>(
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
    fn try_pair<U: DeserializeOwned, V: DeserializeOwned>(
        self,
        func: impl FnOnce(U, V) -> Result<T, RpcParseError>,
    ) -> Self {
        Self(match self.0 {
            Err(Value::Array(mut array)) if array.len() == 2 => Ok(Params::parse(array[0].take())
                .and_then(|u| Ok((u, Params::parse(array[1].take())?)))
                .and_then(|(u, v)| func(u, v))),
            x => x,
        })
    }
}
