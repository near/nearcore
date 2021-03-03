use near_primitives::types::EpochReference;
use near_primitives::views::EpochValidatorInfo;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum RpcValidatorError {
    #[error("Epoch not found")]
    UnknownEpoch,
    #[error("Validator info unavailable")]
    ValidatorInfoUnavailable,
    #[error("The node reached its limits. Try again later. More details: {0}")]
    InternalError(String),
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {0}")]
    Unreachable(String),
}

#[derive(Serialize, Deserialize)]
pub struct RpcValidatorRequest {
    #[serde(flatten)]
    pub epoch_reference: EpochReference,
}

#[derive(Serialize, Deserialize)]
pub struct RpcValidatorResponse {
    #[serde(flatten)]
    pub validator_info: EpochValidatorInfo,
}

impl From<near_client_primitives::types::GetValidatorInfoError> for RpcValidatorError {
    fn from(error: near_client_primitives::types::GetValidatorInfoError) -> Self {
        match error {
            near_client_primitives::types::GetValidatorInfoError::UnknownEpoch => {
                Self::UnknownEpoch
            }
            near_client_primitives::types::GetValidatorInfoError::ValidatorInfoUnavailable => {
                Self::ValidatorInfoUnavailable
            }
            near_client_primitives::types::GetValidatorInfoError::IOError(s) => {
                Self::InternalError(s)
            }
            near_client_primitives::types::GetValidatorInfoError::Unreachable(s) => {
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcValidatorError", &s],
                );
                Self::Unreachable(s)
            }
        }
    }
}

impl From<actix::MailboxError> for RpcValidatorError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError(error.to_string())
    }
}

impl RpcValidatorRequest {
    pub fn parse(
        value: Option<Value>,
    ) -> Result<RpcValidatorRequest, crate::errors::RpcParseError> {
        let epoch_reference = if let Ok((block_id,)) =
            crate::utils::parse_params::<(near_primitives::types::MaybeBlockId,)>(value.clone())
        {
            match block_id {
                Some(id) => EpochReference::BlockId(id),
                None => EpochReference::Latest,
            }
        } else {
            crate::utils::parse_params::<EpochReference>(value)?
        };
        Ok(RpcValidatorRequest { epoch_reference })
    }
}

impl From<RpcValidatorError> for crate::errors::RpcError {
    fn from(error: RpcValidatorError) -> Self {
        let error_data = match error {
            RpcValidatorError::UnknownEpoch => Some(Value::String(format!("Unknown Epoch"))),
            RpcValidatorError::ValidatorInfoUnavailable => {
                Some(Value::String(format!("Validator info unavailable")))
            }
            RpcValidatorError::Unreachable(s) => Some(Value::String(s)),
            RpcValidatorError::InternalError(_) => Some(Value::String(error.to_string())),
        };

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}
