use serde_json::Value;
use serde_with::base64::Base64;
use serde_with::serde_as;

use near_client_primitives::types::TxStatusError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::transactions::{
    RpcBroadcastTransactionRequest, RpcTransactionError, RpcTransactionStatusCommonRequest,
    TransactionInfo,
};
use near_primitives::borsh::BorshDeserialize;
use near_primitives::transaction::SignedTransaction;

use super::{Params, RpcFrom, RpcRequest};

impl RpcRequest for RpcBroadcastTransactionRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        let signed_transaction = decode_signed_transaction(value)?;
        Ok(Self { signed_transaction })
    }
}

impl RpcRequest for RpcTransactionStatusCommonRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        let transaction_info = Params::<TransactionInfo>::new(value)
            .try_pair(|hash, account_id| Ok(TransactionInfo::TransactionId { hash, account_id }))
            .unwrap_or_else(|value| {
                decode_signed_transaction(value).map(TransactionInfo::Transaction)
            })?;
        Ok(Self { transaction_info })
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

fn decode_signed_transaction(value: Value) -> Result<SignedTransaction, RpcParseError> {
    #[serde_as]
    #[derive(serde::Deserialize)]
    struct Payload(#[serde_as(as = "(Base64,)")] (Vec<u8>,));

    let Payload((bytes,)) = Params::<Payload>::parse(value)?;
    SignedTransaction::try_from_slice(&bytes)
        .map_err(|err| RpcParseError(format!("Failed to decode transaction: {}", err)))
}

#[cfg(test)]
mod tests {
    use crate::api::RpcRequest;
    use near_jsonrpc_primitives::types::transactions::{
        RpcBroadcastTransactionRequest, RpcTransactionStatusCommonRequest,
    };
    use near_primitives::borsh::BorshSerialize;
    use near_primitives::hash::CryptoHash;
    use near_primitives::serialize::to_base64;
    use near_primitives::transaction::SignedTransaction;

    #[test]
    fn test_serialize_tx_status_params_as_vec() {
        let tx_hash = CryptoHash::new().to_string();
        let account_id = "sender.testnet";
        let params = serde_json::json!([tx_hash, account_id]);
        assert!(RpcTransactionStatusCommonRequest::parse(params).is_ok());
    }

    #[test]
    fn test_serialize_tx_status_params_as_binary_signed_tx() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = tx.try_to_vec().unwrap();
        let str_tx = to_base64(&bytes_tx);
        let params = serde_json::json!([str_tx]);
        assert!(RpcTransactionStatusCommonRequest::parse(params).is_ok());
    }

    // The params are invalid because sender_account_id is missing
    #[test]
    fn test_serialize_invalid_tx_status_params() {
        let tx_hash = CryptoHash::new().to_string();
        let params = serde_json::json!([tx_hash]);
        assert!(RpcTransactionStatusCommonRequest::parse(params).is_err());
    }

    #[test]
    fn test_serialize_send_tx_params_as_binary_signed_tx() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = tx.try_to_vec().unwrap();
        let str_tx = to_base64(&bytes_tx);
        let params = serde_json::json!([str_tx]);
        assert!(RpcBroadcastTransactionRequest::parse(params).is_ok());
    }
}
