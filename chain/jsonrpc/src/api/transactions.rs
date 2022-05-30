use serde_json::Value;

use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::transactions::{
    RpcBroadcastTransactionRequest, RpcTransactionStatusCommonRequest, TransactionInfo,
};
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;

use super::{parse_params, parse_signed_transaction, RpcRequest};

impl RpcRequest for RpcBroadcastTransactionRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        let signed_transaction = parse_signed_transaction(value)?;
        Ok(Self { signed_transaction })
    }
}

impl RpcRequest for RpcTransactionStatusCommonRequest {
    fn parse(value: Option<Value>) -> Result<Self, RpcParseError> {
        if let Ok((hash, account_id)) = parse_params::<(CryptoHash, AccountId)>(value.clone()) {
            let transaction_info = TransactionInfo::TransactionId { hash, account_id };
            Ok(Self { transaction_info })
        } else {
            let signed_transaction = parse_signed_transaction(value)?;
            let transaction_info = TransactionInfo::Transaction(signed_transaction);
            Ok(Self { transaction_info })
        }
    }
}
