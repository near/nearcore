use near_async::messaging::AsyncSendError;
use serde_json::Value;

use near_client_primitives::types::GetValidatorInfoError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::validator::{
    RpcValidatorError, RpcValidatorRequest, RpcValidatorsOrderedRequest,
};
use near_jsonrpc_traits::params::{Params, ParamsExt};
use near_primitives::types::EpochReference;

use super::{RpcFrom, RpcRequest};

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

impl RpcFrom<AsyncSendError> for RpcValidatorError {
    fn rpc_from(error: AsyncSendError) -> Self {
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

#[cfg(test)]
mod tests {
    use crate::api::RpcRequest;
    use near_jsonrpc_primitives::types::validator::RpcValidatorRequest;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::{BlockId, EpochId, EpochReference};

    #[test]
    fn test_serialize_validators_params_as_vec() {
        let block_hash = CryptoHash::new();
        let params = serde_json::json!([block_hash.to_string()]);
        let result = RpcValidatorRequest::parse(params);
        assert_eq!(
            result.unwrap(),
            RpcValidatorRequest {
                epoch_reference: EpochReference::BlockId(BlockId::Hash(block_hash)),
            }
        );
    }

    #[test]
    fn test_serialize_validators_params_as_object_input_block_hash() {
        let block_hash = CryptoHash::new();
        let params = serde_json::json!({"block_id": block_hash.to_string()});
        let result = RpcValidatorRequest::parse(params);
        assert_eq!(
            result.unwrap(),
            RpcValidatorRequest {
                epoch_reference: EpochReference::BlockId(BlockId::Hash(block_hash)),
            }
        );
    }

    #[test]
    fn test_serialize_validators_params_as_object_input_block_height() {
        let block_height: u64 = 12345;
        let params = serde_json::json!({"block_id": block_height});
        let result = RpcValidatorRequest::parse(params);
        assert_eq!(
            result.unwrap(),
            RpcValidatorRequest {
                epoch_reference: EpochReference::BlockId(BlockId::Height(block_height)),
            }
        );
    }

    #[test]
    fn test_serialize_validators_params_as_object_input_epoch_id() {
        let epoch_id = CryptoHash::new();
        let params = serde_json::json!({"epoch_id": epoch_id.to_string()});
        let result = RpcValidatorRequest::parse(params);
        assert_eq!(
            result.unwrap(),
            RpcValidatorRequest { epoch_reference: EpochReference::EpochId(EpochId(epoch_id)) }
        );
    }
}
