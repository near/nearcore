use near_primitives::borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RpcTransactionRequest {
    pub signed_transaction: near_primitives::transaction::SignedTransaction,
}

#[derive(thiserror::Error, Debug)]
pub enum RpcTransactionError {
    // TxStatusError
    // NetworkClientResponses::InvalidTx
    //NetworkClientResponses::NoResponse
    // DoesNotTrackShard,
    // Ban { ban_reason: ReasonForBan },
    // ServerError (Timeout, Closed)
    #[error("The node does not track shard")]
    DoesNotTrackShard,
    #[error("An error happened during transaction execution: {context:?}")]
    InvalidTransaction { context: near_primitives::errors::InvalidTxError },
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

// TODO: ask if we want to create From<NetworkClientResponse> for it
#[derive(Serialize, Deserialize, Debug)]
pub struct RpcTransactionResponse {
    #[serde(flatten)]
    pub final_execution_outcome: near_primitives::views::FinalExecutionOutcomeViewEnum,
}

#[derive(Debug)]
pub struct RpcTransactionCheckValidResponse;

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcBroadcastTxSyncResponse {
    pub transaction_hash: String,
    pub is_routed: bool,
}

impl RpcTransactionRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, crate::errors::RpcParseError> {
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

impl RpcTransactionError {
    pub fn from_network_client_responses(
        responses: near_client_primitives::near_network::types::NetworkClientResponses,
    ) -> Self {
        match responses {
            near_client_primitives::near_network::types::NetworkClientResponses::InvalidTx(context) => {
                Self::InvalidTransaction { context }
            }
            near_client_primitives::near_network::types::NetworkClientResponses::NoResponse => {
                Self::TimeoutError
            }
            near_client_primitives::near_network::types::NetworkClientResponses::DoesNotTrackShard | near_client_primitives::near_network::types::NetworkClientResponses::RequestRouted => {
                Self::DoesNotTrackShard
            }
            internal_error => Self::InternalError { debug_info: format!("{:?}", internal_error)}
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
