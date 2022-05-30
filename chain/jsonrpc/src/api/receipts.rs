use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::receipts::{ReceiptReference, RpcReceiptRequest};

use super::{parse_params, RpcRequest};

impl RpcRequest for RpcReceiptRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let receipt_reference = parse_params::<ReceiptReference>(value)?;
        Ok(Self { receipt_reference })
    }
}
