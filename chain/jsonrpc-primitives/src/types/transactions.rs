use near_primitives::borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RpcTransactionRequest {
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
    #[error("The node does not track shard")]
    DoesNotTrackShard,
    #[error(
        "Requested transaction with hash {requested_transaction_hash} is not found on the node"
    )]
    UnknownTransaction { requested_transaction_hash: near_primitives::hash::CryptoHash },
    #[error("The node reached its limits. Try again later. More details: {debug_info}")]
    InternalError { debug_info: String },
    #[error("Timeout")]
    TimeoutError,
    // NOTE: Currently, the underlying errors are too broad, and while we tried to handle
    // expected cases, we cannot statically guarantee that no other errors will be returned
    // in the future.
    // TODO #3851: Remove this variant once we can exhaustively match all the underlying errors
    #[error("It is a bug if you receive this error type, please, report this incident: https://github.com/near/nearcore/issues/new/choose. Details: {error_message}")]
    Unreachable { error_message: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcTransactionResponse {
    #[serde(flatten)]
    pub final_execution_outcome: near_primitives::views::FinalExecutionOutcomeViewEnum,
}

// TODO: delete it
#[derive(Debug)]
pub struct RpcTransactionCheckValidResponse;

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcBroadcastTxSyncResponse {
    pub transaction_hash: String,
    pub is_routed: bool,
    // transaction_validation_status: enum {
    // Valid
    // Unknown
}

impl RpcTransactionRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
        // TODO: move this code to crate::utils::parse_signed_transaction
        let (encoded,) = crate::utils::parse_params::<(String,)>(value.clone())?;
        let bytes = crate::utils::from_base64_or_parse_err(encoded)?;
        let signed_transaction = near_primitives::transaction::SignedTransaction::try_from_slice(
            &bytes,
        )
        .map_err(|err| {
            crate::errors::RpcParseError(format!("Failed to decode transaction: {}", err))
        })?;
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
            // TODO: move this code to crate::utils::parse_signed_transaction
            let (encoded,) = crate::utils::parse_params::<(String,)>(value.clone())?;
            let bytes = crate::utils::from_base64_or_parse_err(encoded)?;
            let signed_transaction =
                near_primitives::transaction::SignedTransaction::try_from_slice(&bytes).map_err(
                    |err| {
                        crate::errors::RpcParseError(format!(
                            "Failed to decode transaction: {}",
                            err
                        ))
                    },
                )?;
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
            near_client_primitives::types::TxStatusError::InternalError => {
                Self::InternalError { debug_info: format!("Internal error") }
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
        let error_data = Some(Value::String(error.to_string()));

        Self::new(-32_000, "Server error".to_string(), error_data)
    }
}

impl From<actix::MailboxError> for RpcTransactionError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { debug_info: error.to_string() }
    }
}
