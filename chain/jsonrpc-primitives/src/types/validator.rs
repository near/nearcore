use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type RpcValidatorsOrderedResponse =
    Vec<near_primitives::views::validator_stake_view::ValidatorStakeView>;

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcValidatorError {
    #[error("Epoch not found")]
    UnknownEpoch,
    #[error("Validator info unavailable")]
    ValidatorInfoUnavailable,
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

#[derive(Serialize, Deserialize, Debug, arbitrary::Arbitrary)]
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

impl From<RpcValidatorError> for crate::errors::RpcError {
    fn from(error: RpcValidatorError) -> Self {
        let error_data = match &error {
            RpcValidatorError::UnknownEpoch => Some(Value::String(format!("Unknown Epoch"))),
            RpcValidatorError::ValidatorInfoUnavailable => {
                Some(Value::String(format!("Validator info unavailable")))
            }
            RpcValidatorError::InternalError { .. } => Some(Value::String(error.to_string())),
        };

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcValidatorError: {:?}", err),
                )
            }
        };

        Self::new_internal_or_handler_error(error_data, error_data_value)
    }
}
