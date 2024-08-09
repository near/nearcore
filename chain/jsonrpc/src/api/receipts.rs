use near_async::messaging::AsyncSendError;
use near_client_primitives::types::{GetReceipt, GetReceiptError};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::receipts::{
    ReceiptReference, RpcReceiptError, RpcReceiptRequest,
};
use near_jsonrpc_traits::params::{Params, ParamsExt};

use serde_json::Value;

use super::{RpcFrom, RpcRequest};

impl RpcRequest for RpcReceiptRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Ok(Self { receipt_reference: Params::parse(value)? })
    }
}

impl RpcFrom<AsyncSendError> for RpcReceiptError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<ReceiptReference> for GetReceipt {
    fn rpc_from(receipt_reference: ReceiptReference) -> Self {
        Self { receipt_id: receipt_reference.receipt_id }
    }
}

impl RpcFrom<GetReceiptError> for RpcReceiptError {
    fn rpc_from(error: GetReceiptError) -> Self {
        match error {
            GetReceiptError::IOError(error_message) => Self::InternalError { error_message },
            GetReceiptError::UnknownReceipt(hash) => Self::UnknownReceipt { receipt_id: hash },
            GetReceiptError::Unreachable(ref error_message) => {
                tracing::warn!(target: "jsonrpc", "Unreachable error occurred: {}", error_message);
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcReceiptError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}
