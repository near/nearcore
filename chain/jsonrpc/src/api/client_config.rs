use near_async::messaging::AsyncSendError;
use near_client_primitives::types::GetClientConfigError;
use near_jsonrpc_primitives::types::client_config::RpcClientConfigError;

use super::RpcFrom;

impl RpcFrom<AsyncSendError> for RpcClientConfigError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetClientConfigError> for RpcClientConfigError {
    fn rpc_from(error: GetClientConfigError) -> Self {
        match error {
            GetClientConfigError::IOError(error_message) => Self::InternalError { error_message },
            GetClientConfigError::Unreachable(ref error_message) => {
                tracing::warn!(%error_message, "unreachable error occurred");
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcClientConfigError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
