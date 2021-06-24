use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RpcValidatorsOrderedResponse =
    Vec<near_primitives::views::validator_stake_view::ValidatorStakeView>;

#[derive(thiserror::Error, Debug, Serialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcValidatorError {
    #[error("Epoch not found")]
    UnknownEpoch,
    #[error("Validator info unavailable")]
    ValidatorInfoUnavailable,
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcValidatorRequest {
    #[serde(flatten)]
    pub epoch_reference: near_primitives::types::EpochReference,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcValidatorsOrderedRequest {
    pub block_id: near_primitives::types::MaybeBlockId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcValidatorResponse {
    #[serde(flatten)]
    pub validator_info: near_primitives::views::EpochValidatorInfo,
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
            near_client_primitives::types::GetValidatorInfoError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            near_client_primitives::types::GetValidatorInfoError::Unreachable(error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", &error_message);
                near_metrics::inc_counter_vec(
                    &crate::metrics::RPC_UNREACHABLE_ERROR_COUNT,
                    &["RpcValidatorError"],
                );
                Self::Unreachable { error_message }
            }
        }
    }
}

impl From<actix::MailboxError> for RpcValidatorError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcValidatorRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        let epoch_reference = if let Ok((block_id,)) =
            crate::utils::parse_params::<(near_primitives::types::MaybeBlockId,)>(value.clone())
        {
            match block_id {
                Some(id) => near_primitives::types::EpochReference::BlockId(id),
                None => near_primitives::types::EpochReference::Latest,
            }
        } else {
            crate::utils::parse_params::<near_primitives::types::EpochReference>(value)?
        };
        Ok(Self { epoch_reference })
    }
}

impl RpcValidatorsOrderedRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        Ok(crate::utils::parse_params::<RpcValidatorsOrderedRequest>(value)?)
    }
}

impl From<RpcValidatorError> for crate::errors::RpcError {
    fn from(error: RpcValidatorError) -> Self {
        let error_data = match &error {
            RpcValidatorError::UnknownEpoch => Some(Value::String(format!("Unknown Epoch"))),
            RpcValidatorError::ValidatorInfoUnavailable => {
                Some(Value::String(format!("Validator info unavailable")))
            }
            RpcValidatorError::Unreachable { error_message } => {
                Some(Value::String(error_message.clone()))
            }
            RpcValidatorError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        Self::new_handler_error(
            error_data,
            serde_json::to_value(error)
                .expect("Not expected serialization error while serializing struct"),
        )
    }
}
