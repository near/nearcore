use serde_json::Value;

use near_client_primitives::types::TxStatusError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::transactions::{
    RpcBroadcastTransactionRequest, RpcTransactionError, RpcTransactionResponse,
    RpcTransactionStatusCommonRequest, TransactionInfo,
};
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::views::FinalExecutionOutcomeViewEnum;

use super::{parse_params, parse_signed_transaction, RpcFrom, RpcRequest};

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
            TxStatusError::InvalidTx(context) => Self::InvalidTransaction { context },
            TxStatusError::InternalError(debug_info) => Self::InternalError { debug_info },
            TxStatusError::TimeoutError => Self::TimeoutError,
        }
    }
}

impl RpcFrom<FinalExecutionOutcomeViewEnum> for RpcTransactionResponse {
    fn rpc_from(final_execution_outcome: FinalExecutionOutcomeViewEnum) -> Self {
        Self { final_execution_outcome }
    }
}
