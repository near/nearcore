use serde_json::Value;

use near_client_primitives::types::GetMaintenanceWindowsError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::maintenance::{
    RpcMaintenanceWindowsError, RpcMaintenanceWindowsRequest,
};

use super::{parse_params, RpcFrom, RpcRequest};

impl RpcRequest for RpcMaintenanceWindowsRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        parse_params::<Self>(value)
    }
}

impl RpcFrom<actix::MailboxError> for RpcMaintenanceWindowsError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<GetMaintenanceWindowsError> for RpcMaintenanceWindowsError {
    fn rpc_from(error: GetMaintenanceWindowsError) -> Self {
        match error {
            GetMaintenanceWindowsError::IOError(error_message) => {
                Self::InternalError { error_message }
            }
            GetMaintenanceWindowsError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
