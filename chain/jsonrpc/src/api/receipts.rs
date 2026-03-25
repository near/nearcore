use super::{Params, RpcFrom, RpcRequest};
use near_async::messaging::AsyncSendError;
use near_client_primitives::types::{
    GetReceipt, GetReceiptError, GetReceiptToTx, GetReceiptToTxError,
};
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::receipts::{
    ReceiptReference, RpcReceiptError, RpcReceiptRequest, RpcReceiptToTxError,
    RpcReceiptToTxRequest,
};
use serde_json::Value;

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
                tracing::warn!(target: "jsonrpc", %error_message, "unreachable error occurred");
                crate::metrics::RPC_UNREACHABLE_ERROR_COUNT
                    .with_label_values(&["RpcReceiptError"])
                    .inc();
                Self::InternalError { error_message: error.to_string() }
            }
        }
    }
}

impl RpcRequest for RpcReceiptToTxRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Ok(Self { receipt_reference: Params::parse(value)? })
    }
}

impl RpcFrom<AsyncSendError> for RpcReceiptToTxError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<ReceiptReference> for GetReceiptToTx {
    fn rpc_from(receipt_reference: ReceiptReference) -> Self {
        Self { receipt_id: receipt_reference.receipt_id }
    }
}

impl RpcFrom<GetReceiptToTxError> for RpcReceiptToTxError {
    fn rpc_from(error: GetReceiptToTxError) -> Self {
        match error {
            GetReceiptToTxError::UnknownReceipt(receipt_id) => Self::UnknownReceipt { receipt_id },
            GetReceiptToTxError::DepthExceeded { receipt_id, limit } => {
                Self::DepthExceeded { receipt_id, limit }
            }
            GetReceiptToTxError::Unsupported(error_message) => Self::Unsupported { error_message },
        }
    }
}
