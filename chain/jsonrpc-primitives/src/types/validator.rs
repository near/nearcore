use serde_json::Value;

pub type RpcValidatorsOrderedResponse =
    Vec<near_primitives::views::validator_stake_view::ValidatorStakeView>;

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcValidatorError {
    #[error("Epoch not found")]
    UnknownEpoch,
    #[error("Validator info unavailable")]
    ValidatorInfoUnavailable,
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

#[derive(serde::Serialize, serde::Deserialize, Debug, arbitrary::Arbitrary)]
pub struct RpcValidatorRequest {
    #[serde(flatten)]
    pub epoch_reference: near_primitives::types::EpochReference,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcValidatorsOrderedRequest {
    pub block_id: near_primitives::types::MaybeBlockId,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RpcValidatorResponse {
    #[serde(flatten)]
    pub validator_info: near_primitives::views::EpochValidatorInfo,
}

impl From<RpcValidatorError> for crate::errors::RpcError {
    fn from(error: RpcValidatorError) -> Self {
        let error_data = match &error {
            RpcValidatorError::UnknownEpoch => Some(Value::String("Unknown Epoch".to_string())),
            RpcValidatorError::ValidatorInfoUnavailable => {
                Some(Value::String("Validator info unavailable".to_string()))
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
