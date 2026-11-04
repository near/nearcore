use super::{Params, RpcFrom, RpcRequest};
use near_async::messaging::AsyncSendError;
use near_client_primitives::types::TxStatusError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::transactions::{
    RpcSendTransactionRequest, RpcTransactionError, RpcTransactionStatusRequest, TransactionInfo,
};
use near_primitives::borsh::BorshDeserialize;
use near_primitives::serialize::from_base64;
use near_primitives::transaction::{SignedTransaction, TransactionEnvelope};
use serde_json::Value;

impl RpcRequest for RpcSendTransactionRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        let tx_request = Params::new(value)
            .try_singleton(|value| {
                Ok(RpcSendTransactionRequest {
                    transaction: decode_transaction_envelope(value)?,
                    // will be ignored in `broadcast_tx_async`, `broadcast_tx_commit`
                    wait_until: Default::default(),
                })
            })
            .try_pair(|_: String, _: String| {
                // Here, we restrict serde parsing object from the array
                // `wait_until` is a new feature supported only in object
                Err(RpcParseError(
                    "Unable to parse send request: too many params passed".to_string(),
                ))
            })
            .unwrap_or_parse()?;
        Ok(tx_request)
    }
}

impl RpcRequest for RpcTransactionStatusRequest {
    fn parse(value: Value) -> Result<Self, RpcParseError> {
        Ok(Params::new(value)
            .try_singleton(|signed_tx| {
                Ok(RpcTransactionStatusRequest {
                    transaction_info: decode_signed_transaction(signed_tx)?.into(),
                    wait_until: Default::default(),
                })
            })
            .try_pair(|tx_hash, sender_account_id| {
                Ok(RpcTransactionStatusRequest {
                    transaction_info: TransactionInfo::TransactionId { tx_hash, sender_account_id }
                        .into(),
                    wait_until: Default::default(),
                })
            })
            .unwrap_or_parse()?)
    }
}

impl RpcFrom<AsyncSendError> for RpcTransactionError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { debug_info: error.to_string() }
    }
}

impl RpcFrom<TxStatusError> for RpcTransactionError {
    fn rpc_from(error: TxStatusError) -> Self {
        match error {
            TxStatusError::ChainError(err) => {
                Self::InternalError { debug_info: format!("{:?}", err) }
            }
            TxStatusError::InternalError(debug_info) => Self::InternalError { debug_info },
        }
    }
}

fn decode_transaction_envelope(value: String) -> Result<TransactionEnvelope, RpcParseError> {
    decode_borsh_base64(value)
}

fn decode_signed_transaction(value: String) -> Result<SignedTransaction, RpcParseError> {
    decode_borsh_base64(value)
}

fn decode_borsh_base64<T: BorshDeserialize>(value: String) -> Result<T, RpcParseError> {
    let bytes = from_base64(&value)
        .map_err(|err| RpcParseError(format!("Failed to decode transaction: {}", err)))?;
    T::try_from_slice(&bytes)
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
    use near_primitives::transaction::{EccTransaction, SignedTransaction, TransactionEnvelope};
    use near_primitives::views::TxExecutionStatus;

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
    fn test_serialize_tx_status_params_as_object_with_signed_tx() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = borsh::to_vec(&tx).unwrap();
        let str_tx = to_base64(&bytes_tx);
        let wait_until = "INCLUDED_FINAL";
        let params = serde_json::json!({"signed_tx_base64": str_tx, "wait_until": wait_until});
        assert!(RpcTransactionStatusRequest::parse(params).is_ok());
    }

    #[test]
    fn test_serialize_tx_status_params_as_object_with_wait_until() {
        let tx_hash = CryptoHash::new().to_string();
        let account_id = "sender.testnet";
        let wait_until = "INCLUDED";
        let params = serde_json::json!({"tx_hash": tx_hash, "sender_account_id": account_id, "wait_until": wait_until});
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

    // The params are invalid because wait_until is supported only in tx status params passed by object
    #[test]
    fn test_serialize_tx_status_too_many_params() {
        let tx_hash = CryptoHash::new().to_string();
        let account_id = "sender.testnet";
        let wait_until = "EXECUTED";
        let params = serde_json::json!([tx_hash, account_id, wait_until]);
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
    fn test_serialize_send_tx_params_as_object() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = borsh::to_vec(&tx).unwrap();
        let str_tx = to_base64(&bytes_tx);
        let params = serde_json::json!({"signed_tx_base64": str_tx});
        assert!(RpcSendTransactionRequest::parse(params).is_ok());
    }

    #[test]
    fn test_serialize_send_tx_params_as_object_with_wait_until() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = borsh::to_vec(&tx).unwrap();
        let str_tx = to_base64(&bytes_tx);
        let wait_until = "INCLUDED_FINAL";
        let params = serde_json::json!({"signed_tx_base64": str_tx, "wait_until": wait_until});
        assert!(RpcSendTransactionRequest::parse(params).is_ok());
    }

    // The params are invalid because wait_until is supported only in send tx params passed by object
    #[test]
    fn test_serialize_send_tx_too_many_params() {
        let tx_hash = CryptoHash::new();
        let tx = SignedTransaction::empty(tx_hash);
        let bytes_tx = borsh::to_vec(&tx).unwrap();
        let str_tx = to_base64(&bytes_tx);
        let wait_until = "EXECUTED";
        let params = serde_json::json!([str_tx, wait_until]);
        assert!(RpcSendTransactionRequest::parse(params).is_err());
    }

    fn ecc_base64() -> (EccTransaction, String) {
        let ecc = EccTransaction::new("contract.near".parse().unwrap(), vec![1, 2, 3]);
        let base64 = to_base64(&borsh::to_vec(&ecc).unwrap());
        (ecc, base64)
    }

    fn signed_tx_base64() -> String {
        to_base64(&borsh::to_vec(&SignedTransaction::empty(CryptoHash::default())).unwrap())
    }

    #[test]
    fn test_serialize_send_tx_params_with_ecc() {
        let (ecc, str_ecc) = ecc_base64();
        let params = serde_json::json!({"ecc_base64": str_ecc, "wait_until": "NONE"});
        let request = RpcSendTransactionRequest::parse(params).unwrap();
        assert_eq!(request.transaction, TransactionEnvelope::from(ecc));
    }

    #[test]
    fn test_serialize_send_tx_params_with_ecc_as_binary() {
        let (ecc, str_ecc) = ecc_base64();
        let request = RpcSendTransactionRequest::parse(serde_json::json!([str_ecc])).unwrap();
        assert_eq!(request.transaction, TransactionEnvelope::from(ecc));
    }

    // A request carries exactly one of the transaction fields, and each holds its own kind
    #[test]
    fn test_serialize_send_tx_params_with_wrong_transaction_fields() {
        let (_, str_ecc) = ecc_base64();
        let str_tx = signed_tx_base64();
        let invalid = [
            serde_json::json!({"signed_tx_base64": str_tx, "ecc_base64": str_ecc}),
            serde_json::json!({"wait_until": "NONE"}),
            serde_json::json!({"signed_tx_base64": str_ecc}),
            serde_json::json!({"ecc_base64": str_tx}),
        ];
        invalid.into_iter().for_each(|params| {
            assert!(RpcSendTransactionRequest::parse(params.clone()).is_err(), "{params}");
        });
    }

    // Requests are serialized the way they are parsed, e.g. by the JSON-RPC client
    #[test]
    fn test_send_tx_request_round_trip() {
        let (_, str_ecc) = ecc_base64();
        [("signed_tx_base64", signed_tx_base64()), ("ecc_base64", str_ecc)].into_iter().for_each(
            |(field, value)| {
                let params = serde_json::json!({field: value, "wait_until": "INCLUDED"});
                let request = RpcSendTransactionRequest::parse(params.clone()).unwrap();
                assert_eq!(serde_json::to_value(&request).unwrap(), params);
            },
        );
    }

    /// The request as it was before ECCs.
    #[derive(serde::Serialize, serde::Deserialize)]
    struct LegacyRpcSendTransactionRequest {
        #[serde(rename = "signed_tx_base64")]
        signed_transaction: SignedTransaction,
        #[serde(default)]
        wait_until: TxExecutionStatus,
    }

    // A signed transaction keeps its wire format, in both directions
    #[test]
    fn test_send_tx_request_legacy_format() {
        let signed_tx = SignedTransaction::empty(CryptoHash::default());
        let legacy = LegacyRpcSendTransactionRequest {
            signed_transaction: signed_tx.clone(),
            wait_until: TxExecutionStatus::Included,
        };

        let json = serde_json::to_value(&legacy).unwrap();
        let request = serde_json::from_value::<RpcSendTransactionRequest>(json).unwrap();
        assert_eq!(request.transaction, TransactionEnvelope::Signed(signed_tx.clone()));
        assert_eq!(request.wait_until, TxExecutionStatus::Included);

        let json = serde_json::to_value(&request).unwrap();
        let legacy = serde_json::from_value::<LegacyRpcSendTransactionRequest>(json).unwrap();
        assert_eq!(legacy.signed_transaction, signed_tx);
        assert_eq!(legacy.wait_until, TxExecutionStatus::Included);
    }

    // `tx` looks a transaction up by its signer, which an ECC does not have
    #[test]
    fn test_serialize_tx_status_params_with_ecc() {
        let ecc = EccTransaction::new("contract.near".parse().unwrap(), vec![1, 2, 3]);
        let str_ecc = to_base64(&borsh::to_vec(&ecc).unwrap());
        let params = serde_json::json!([str_ecc]);
        assert!(RpcTransactionStatusRequest::parse(params).is_err());
    }
}
