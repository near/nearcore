use super::{Params, RpcFrom, RpcRequest};
use near_async::messaging::AsyncSendError;
use near_client::indexer::FailedToFetchData;
use near_client_primitives::types::{GetBlockError, GetClientConfigError, GetProtocolConfigError};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::indexer::{RpcIndexerBlockError, RpcIndexerBlockRequest};
use serde_json::Value;

impl RpcRequest for RpcIndexerBlockRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Params::parse(value)
    }
}

impl RpcFrom<AsyncSendError> for RpcIndexerBlockError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetClientConfigError> for RpcIndexerBlockError {
    fn rpc_from(error: GetClientConfigError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetBlockError> for RpcIndexerBlockError {
    fn rpc_from(error: GetBlockError) -> Self {
        Self::DataUnavailable { error_message: error.to_string() }
    }
}

impl RpcFrom<GetProtocolConfigError> for RpcIndexerBlockError {
    fn rpc_from(error: GetProtocolConfigError) -> Self {
        Self::DataUnavailable { error_message: error.to_string() }
    }
}

impl RpcFrom<FailedToFetchData> for RpcIndexerBlockError {
    fn rpc_from(error: FailedToFetchData) -> Self {
        let FailedToFetchData::String(error_message) = error;
        Self::DataUnavailable { error_message }
    }
}

#[cfg(test)]
mod tests {
    use super::{RpcIndexerBlockRequest, RpcRequest};
    use crate::RpcConfig;
    use near_primitives::hash::CryptoHash;
    use serde_json::{from_value, json, to_value};

    #[test]
    fn disabled_by_default() {
        assert!(!RpcConfig::default().enable_indexer_rpc);
        let mut legacy_config = to_value(RpcConfig::default()).unwrap();
        let fields = legacy_config.as_object_mut().unwrap();
        fields.remove("enable_indexer_rpc");
        fields.remove("indexer_max_concurrent_requests");
        let config: RpcConfig = from_value(legacy_config).unwrap();
        assert!(!config.enable_indexer_rpc);
        assert_eq!(config.indexer_max_concurrent_requests, 1);
    }

    #[test]
    fn requires_block_hash() {
        let block_hash = CryptoHash::default();
        assert_eq!(
            RpcIndexerBlockRequest::parse(json!({ "block_hash": block_hash })).unwrap().block_hash,
            block_hash
        );
        for params in [
            json!({ "finality": "optimistic" }),
            json!({ "block_id": 1 }),
            json!({ "block_hash": block_hash, "finality": "final" }),
            json!({ "block_hash": block_hash, "extra": true }),
            json!({ "block_hash": "invalid" }),
        ] {
            assert!(RpcIndexerBlockRequest::parse(params).is_err());
        }
    }
}
