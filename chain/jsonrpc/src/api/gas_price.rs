use serde_json::Value;

use near_client_primitives::types::GetGasPriceError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::gas_price::{RpcGasPriceError, RpcGasPriceRequest};

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcGasPriceRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value).map(|(block_id,)| Self { block_id })
    }
}

impl RpcFrom<actix::MailboxError> for RpcGasPriceError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetGasPriceError> for RpcGasPriceError {
    fn rpc_from(error: GetGasPriceError) -> Self {
        match error {
            GetGasPriceError::UnknownBlock { error_message } => {
                Self::UnknownBlock { error_message }
            }
            GetGasPriceError::InternalError { error_message } => {
                Self::InternalError { error_message }
            }
            GetGasPriceError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcGasPriceError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
