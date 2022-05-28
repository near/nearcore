use near_jsonrpc_primitives::errors::{RpcError, ServerError};

macro_rules! _rpc_try {
    ($val:expr) => {
        match $val {
            Ok(val) => val,
            Err(err) => return Err(RpcFrom::rpc_from(err)),
        }
    };
}

pub(crate) use _rpc_try as rpc_try;

pub trait RpcFrom<T> {
    fn rpc_from(_: T) -> Self;
}

// --

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

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::blocks::RpcBlockError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::changes::RpcStateChangesError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::chunks::RpcChunkError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::config::RpcProtocolConfigError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::gas_price::RpcGasPriceError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientProofError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::network_info::RpcNetworkInfoError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::query::RpcQueryError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::receipts::RpcReceiptError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::sandbox::RpcSandboxPatchStateError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::sandbox::RpcSandboxFastForwardError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::status::RpcStatusError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError>
    for near_jsonrpc_primitives::types::transactions::RpcTransactionError
{
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { debug_info: error.to_string() }
    }
}

impl RpcFrom<actix::MailboxError> for near_jsonrpc_primitives::types::validator::RpcValidatorError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}
