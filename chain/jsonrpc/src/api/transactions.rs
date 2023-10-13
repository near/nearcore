use serde_json::Value;

use near_client_primitives::types::TxStatusError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::transactions::{
    RpcSendTransactionRequest, RpcTransactionError, RpcTransactionStatusRequest, TransactionInfo,
};
use near_primitives::borsh::BorshDeserialize;
use near_primitives::transaction::SignedTransaction;
use near_primitives::views::TxExecutionStatus;

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcSendTransactionRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        let tx_request = Params::new(value)
            .try_singleton(|value| {
                Ok(RpcSendTransactionRequest {
                    signed_transaction: decode_signed_transaction(value)?,
                    // will be ignored in `broadcast_tx_async`, `broadcast_tx_commit`
                    finality: Default::default(),
                })
            })
            .try_pair(|encoded_tx, finality: String| {
                let finality = format!("\"{}\"", finality);
                Ok(RpcSendTransactionRequest {
                    signed_transaction: decode_signed_transaction(encoded_tx)?,
                    // will be ignored in `broadcast_tx_async`, `broadcast_tx_commit`
                    finality: serde_json::from_str::<TxExecutionStatus>(&finality).map_err(
                        |err| RpcParseError(format!("Failed to decode finality: {}", err)),
                    )?,
                })
            })
            .unwrap()?;
        Ok(tx_request)
    }
}

impl RpcRequest for RpcTransactionStatusRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Ok(Params::new(value)
            .try_singleton(|signed_tx| {
                Ok(RpcTransactionStatusRequest {
                    transaction_info: decode_signed_transaction(signed_tx)?.into(),
                    finality: Default::default(),
                })
            })
            .try_pair(|tx_hash, sender_account_id| {
                Ok(RpcTransactionStatusRequest {
                    transaction_info: TransactionInfo::TransactionId { tx_hash, sender_account_id }
                        .into(),
                    finality: Default::default(),
                })
            })
            .unwrap_or_parse()?)
    }
}

impl RpcFrom<actix::MailboxError> for RpcTransactionError {
    fn rpc_from(error: actix::MailboxError) -> Self {
        Self::InternalError { debug_info: error.to_string() }
    }
}

impl RpcFrom<TxStatusError> for RpcTransactionError {
    fn rpc_from(error: TxStatusError) -> Self {
        match error {
            TxStatusError::ChainError(err) => {
                Self::InternalError { debug_info: format!("{:?}", err) }
            }
            TxStatusError::MissingTransaction(requested_transaction_hash) => {
                Self::UnknownTransaction { requested_transaction_hash }
            }
            TxStatusError::InternalError(debug_info) => Self::InternalError { debug_info },
            TxStatusError::TimeoutError => Self::TimeoutError,
        }
    }
}

fn decode_signed_transaction(value: String) -> Result<SignedTransaction, RpcParseError> {
    let bytes = near_primitives::serialize::from_base64(&value)
        .map_err(|err| RpcParseError(format!("Failed to decode transaction: {}", err)))?;
    SignedTransaction::try_from_slice(&bytes)
        .map_err(|err| RpcParseError(format!("Failed to decode transaction: {}", err)))
}

#[cfg(test)]
mod tests {
    use crate::api::RpcRequest;
    use near_jsonrpc_primitives::types::transactions::{
        RpcSendTransactionRequest, RpcTransactionStatusRequest,
    };
    use near_primitives::borsh;
    use near_primitives::hash::CryptoHash;
    use near_primitives::serialize::to_base64;
    use near_primitives::transaction::SignedTransaction;

    #[test]
    fn test_serialize_tx_status_params_as_vec() {
        let tx_hash = CryptoHash::new().to_string();
        let account_id = "sender.testnet";
        let params = serde_json::json!([tx_hash, account_id]);
        assert!(RpcTransactionStatusRequest::parse(params).is_ok());
    }

    #[test]
    fn test_serialize_tx_status_params_as_object() {
        let tx_hash = CryptoHash::new().to_string();
        let account_id = "sender.testnet";
        let params = serde_json::json!({"tx_hash": tx_hash, "sender_account_id": account_id});
        assert!(RpcTransactionStatusRequest::parse(params).is_ok());
    }

    #[test]
    fn test_serialize_tx_status_params_as_object_with_finality() {
        let tx_hash = CryptoHash::new().to_string();
        let account_id = "sender.testnet";
        let finality = "INCLUDED";
        let params = serde_json::json!({"tx_hash": tx_hash, "sender_account_id": account_id, "finality": finality});
        assert!(RpcTransactionStatusRequest::parse(params).is_ok());
    }

    #[test]
    fn test_serialize_tx_status_params_as_binary_signed_tx() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = borsh::to_vec(&tx).unwrap();
        let str_tx = to_base64(&bytes_tx);
        let params = serde_json::json!([str_tx]);
        assert!(RpcTransactionStatusRequest::parse(params).is_ok());
    }

    // The params are invalid because sender_account_id is missing
    #[test]
    fn test_serialize_invalid_tx_status_params() {
        let tx_hash = CryptoHash::new().to_string();
        let params = serde_json::json!([tx_hash]);
        assert!(RpcTransactionStatusRequest::parse(params).is_err());
    }

    // The params are invalid because finality is supported only in tx status params passed by object
    #[test]
    fn test_serialize_tx_status_too_many_params() {
        let tx_hash = CryptoHash::new().to_string();
        let account_id = "sender.testnet";
        let finality = "EXECUTED";
        let params = serde_json::json!([tx_hash, account_id, finality]);
        assert!(RpcTransactionStatusRequest::parse(params).is_err());
    }

    #[test]
    fn test_serialize_send_tx_params_as_binary_signed_tx() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = borsh::to_vec(&tx).unwrap();
        let str_tx = to_base64(&bytes_tx);
        let params = serde_json::json!([str_tx]);
        assert!(RpcSendTransactionRequest::parse(params).is_ok());
    }

    #[test]
    fn test_serialize_send_tx_params_as_binary_signed_tx_with_finality() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = borsh::to_vec(&tx).unwrap();
        let str_tx = to_base64(&bytes_tx);
        let finality = "EXECUTED";
        let params = serde_json::json!([str_tx, finality]);
        assert!(RpcSendTransactionRequest::parse(params).is_ok());
    }
}
