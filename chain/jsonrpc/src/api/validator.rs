use serde_json::Value;

use near_client_primitives::types::GetValidatorInfoError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::validator::{
    RpcValidatorError, RpcValidatorRequest, RpcValidatorsOrderedRequest,
};
use near_primitives::types::EpochReference;

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcValidatorRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        let epoch_reference = Params::new(value)
            .try_singleton(|block_id| match block_id {
                Some(id) => Ok(EpochReference::BlockId(id)),
                None => Ok(EpochReference::Latest),
            })
            .unwrap_or_parse()?;
        Ok(Self { epoch_reference })
    }
}

impl RpcRequest for RpcValidatorsOrderedRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcFrom<actix::MailboxError> for RpcValidatorError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetValidatorInfoError> for RpcValidatorError {
    fn rpc_from(error: GetValidatorInfoError) -> Self {
        match error {
            GetValidatorInfoError::UnknownEpoch => Self::UnknownEpoch,
            GetValidatorInfoError::ValidatorInfoUnavailable => Self::ValidatorInfoUnavailable,
            GetValidatorInfoError::IOError(error_message) => Self::InternalError { error_message },
            GetValidatorInfoError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcValidatorError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
