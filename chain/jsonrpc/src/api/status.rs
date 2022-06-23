use near_client_primitives::types::StatusError;
use near_jsonrpc_primitives::types::status::{
    RpcHealthResponse, RpcStatusError, RpcStatusResponse,
};
use near_primitives::views::StatusResponse;

use super::RpcFrom;

impl RpcFrom<actix::MailboxError> for RpcStatusError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<StatusResponse> for RpcStatusResponse {
    fn rpc_from(status_response: StatusResponse) -> Self {
        Self { status_response }
    }
}

impl RpcFrom<near_client_primitives::debug::DebugStatusResponse>
    for near_jsonrpc_primitives::types::status::RpcDebugStatusResponse
{
    fn rpc_from(status_response: near_client_primitives::debug::DebugStatusResponse) -> Self {
        Self { status_response }
    }
}

impl RpcFrom<StatusResponse> for RpcHealthResponse {
    fn rpc_from(_status_response: StatusResponse) -> Self {
        Self {}
    }
}

impl RpcFrom<StatusError> for RpcStatusError {
    fn rpc_from(error: StatusError) -> Self {
        match error {
            StatusError::InternalError { error_message } => Self::InternalError { error_message },
            StatusError::NodeIsSyncing => Self::NodeIsSyncing,
            StatusError::NoNewBlocks { elapsed } => Self::NoNewBlocks { elapsed },
            StatusError::EpochOutOfBounds { epoch_id } => Self::EpochOutOfBounds { epoch_id },
            StatusError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcStatusError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
