use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use serde_json::Value;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcSendTransactionRequest {
    #[serde(rename = "signed_tx_base64")]
    pub signed_transaction: near_primitives::transaction::SignedTransaction,
    #[serde(default)]
    pub wait_until: near_primitives::views::TxExecutionStatus,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcTransactionStatusRequest {
    #[serde(flatten)]
    pub transaction_info: TransactionInfo,
    #[serde(default)]
    pub wait_until: near_primitives::views::TxExecutionStatus,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum TransactionInfo {
    Transaction {
        #[serde(rename = "signed_tx_base64")]
        signed_tx: near_primitives::transaction::SignedTransaction,
    },
    TransactionId {
        tx_hash: CryptoHash,
        sender_account_id: AccountId,
    },
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcTransactionError {
    #[error("An error happened during transaction execution: {context:?}")]
    InvalidTransaction {
        #[serde(skip_serializing)]
        context: near_primitives::errors::InvalidTxError,
    },
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

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcTransactionResponse {
    #[serde(flatten)]
    pub final_execution_outcome: Option<near_primitives::views::FinalExecutionOutcomeViewEnum>,
    pub final_execution_status: near_primitives::views::TxExecutionStatus,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcBroadcastTxSyncResponse {
    pub transaction_hash: near_primitives::hash::CryptoHash,
}

impl TransactionInfo {
    pub fn from_signed_tx(signed_tx: near_primitives::transaction::SignedTransaction) -> Self {
        Self::Transaction { signed_tx }
    }

    pub fn to_signed_tx(&self) -> Option<&near_primitives::transaction::SignedTransaction> {
        match self {
            TransactionInfo::Transaction { signed_tx } => Some(signed_tx),
            TransactionInfo::TransactionId { .. } => None,
        }
    }

    pub fn to_tx_hash_and_account(&self) -> (CryptoHash, &AccountId) {
        match self {
            TransactionInfo::Transaction { signed_tx } => {
                (signed_tx.get_hash(), &signed_tx.transaction.signer_id())
            }
            TransactionInfo::TransactionId { tx_hash, sender_account_id } => {
                (*tx_hash, sender_account_id)
            }
        }
    }
}

impl From<near_primitives::transaction::SignedTransaction> for TransactionInfo {
    fn from(signed_tx: near_primitives::transaction::SignedTransaction) -> Self {
        Self::Transaction { signed_tx }
    }
}

impl From<near_primitives::views::TxStatusView> for RpcTransactionResponse {
    fn from(view: near_primitives::views::TxStatusView) -> Self {
        Self {
            final_execution_outcome: view.execution_outcome,
            final_execution_status: view.status,
        }
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

        let error_data_value = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcTransactionError: {:?}", err),
                );
            }
        };

        Self::new_internal_or_handler_error(Some(error_data), error_data_value)
    }
}
