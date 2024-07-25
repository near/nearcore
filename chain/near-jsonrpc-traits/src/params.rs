use near_jsonrpc_primitives::errors::RpcParseError;

use serde::de::DeserializeOwned;
use serde_json::Value;

pub trait ParamsTrait<T> {
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
