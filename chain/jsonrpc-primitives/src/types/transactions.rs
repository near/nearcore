use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RpcBroadcastTransactionRequest {
    pub signed_transaction: near_primitives::transaction::SignedTransaction,
}

#[derive(Debug)]
pub struct RpcTransactionStatusCommonRequest {
    pub transaction_info: TransactionInfo,
}

#[derive(Clone, Debug)]
pub enum TransactionInfo {
    Transaction(near_primitives::transaction::SignedTransaction),
    TransactionId {
        hash: near_primitives::hash::CryptoHash,
        account_id: near_primitives::types::AccountId,
    },
}

#[derive(thiserror::Error, Debug)]
pub enum RpcTransactionError {
    #[error("An error happened during transaction execution: {context:?}")]
    InvalidTransaction { context: near_primitives::errors::InvalidTxError },
    #[error("Node doesn't track this shard. Cannot determine whether the transaction is valid")]
    DoesNotTrackShard,
    #[error("Transaction with hash {transaction_hash} was routed")]
    RequestRouted { transaction_hash: near_primitives::hash::CryptoHash },
    #[error("Transaction {requested_transaction_hash} doesn't exist")]
    UnknownTransaction { requested_transaction_hash: near_primitives::hash::CryptoHash },
    #[error("The node reached its limits. Try again later. More details: {debug_info}")]
    InternalError { debug_info: String },
    #[error("Timeout")]
    TimeoutError,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcTransactionResponse {
    #[serde(flatten)]
    pub final_execution_outcome: near_primitives::views::FinalExecutionOutcomeViewEnum,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcBroadcastTxSyncResponse {
    pub transaction_hash: near_primitives::hash::CryptoHash,
}

impl RpcBroadcastTransactionRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        let signed_transaction = crate::utils::parse_signed_transaction(value)?;
        Ok(Self { signed_transaction })
    }
}

impl RpcTransactionStatusCommonRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        if let Ok((hash, account_id)) =
            crate::utils::parse_params::<(near_primitives::hash::CryptoHash, String)>(value.clone())
        {
            if !near_runtime_utils::is_valid_account_id(&account_id) {
                return Err(crate::errors::RpcParseError(format!(
                    "Invalid account id: {}",
                    account_id
                )));
            }
            let transaction_info = TransactionInfo::TransactionId { hash, account_id };
            Ok(Self { transaction_info })
        } else {
            let signed_transaction = crate::utils::parse_signed_transaction(value)?;
            let transaction_info = TransactionInfo::Transaction(signed_transaction);
            Ok(Self { transaction_info })
        }
    }
}

impl From<near_client_primitives::types::TxStatusError> for RpcTransactionError {
    fn from(error: near_client_primitives::types::TxStatusError) -> Self {
        match error {
            near_client_primitives::types::TxStatusError::ChainError(err) => {
                Self::InternalError { debug_info: format!("{:?}", err) }
            }
            near_client_primitives::types::TxStatusError::MissingTransaction(
                requested_transaction_hash,
            ) => Self::UnknownTransaction { requested_transaction_hash },
            near_client_primitives::types::TxStatusError::InvalidTx(context) => {
                Self::InvalidTransaction { context }
            }
            near_client_primitives::types::TxStatusError::InternalError(debug_info) => {
                Self::InternalError { debug_info }
            }
            near_client_primitives::types::TxStatusError::TimeoutError => Self::TimeoutError,
        }
    }
}

impl From<near_primitives::views::FinalExecutionOutcomeViewEnum> for RpcTransactionResponse {
    fn from(
        final_execution_outcome: near_primitives::views::FinalExecutionOutcomeViewEnum,
    ) -> Self {
        Self { final_execution_outcome }
    }
}

impl From<RpcTransactionError> for crate::errors::RpcError {
    fn from(error: RpcTransactionError) -> Self {
        let error_data = match &error {
            RpcTransactionError::InvalidTransaction { context } => {
                if let Ok(value) =
                    serde_json::to_value(crate::errors::ServerError::TxExecutionError(
                        near_primitives::errors::TxExecutionError::InvalidTxError(context.clone()),
                    ))
                {
                    value
                } else {
                    Value::String(error.to_string())
                }
            }
            _ => Value::String(error.to_string()),
        };
        Self::new(-32_000, "Server error".to_string(), Some(error_data))
    }
}

impl From<actix::MailboxError> for RpcTransactionError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { debug_info: error.to_string() }
    }
}
