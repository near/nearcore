pub mod errors;
pub mod message;
pub(crate) mod metrics;
pub mod types;
#[cfg(feature = "server")]
pub mod utils;

#[cfg(feature = "server")]
pub trait RpcRequest: Sized {
    fn parse(value: Option<serde_json::Value>) -> Result<Self, errors::RpcParseError>;
}
