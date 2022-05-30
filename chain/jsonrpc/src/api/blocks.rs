use serde_json::Value;

use near_client_primitives::types::GetBlockError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::blocks::{RpcBlockError, RpcBlockRequest};
use near_primitives::types::{BlockId, BlockReference};

use super::{parse_params, RpcFrom, RpcRequest};

impl RpcRequest for RpcBlockRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let block_reference = if let Ok((block_id,)) = parse_params::<(BlockId,)>(value.clone()) {
            BlockReference::BlockId(block_id)
        } else {
            parse_params::<BlockReference>(value)?
        };
        Ok(Self { block_reference })
    }
}

impl RpcFrom<actix::MailboxError> for RpcBlockError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetBlockError> for RpcBlockError {
    fn rpc_from(error: GetBlockError) -> Self {
        match error {
            GetBlockError::UnknownBlock { error_message } => Self::UnknownBlock { error_message },
            GetBlockError::NotSyncedYet => Self::NotSyncedYet,
            GetBlockError::IOError { error_message } => Self::InternalError { error_message },
            GetBlockError::Unreachable { ref error_message } => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcBlockError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
