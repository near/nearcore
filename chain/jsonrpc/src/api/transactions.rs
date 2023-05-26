use serde_json::Value;
use serde_with::base64::Base64;
use serde_with::serde_as;

use near_client_primitives::types::TxStatusError;
use near_jsonrpc_primitives::errors::RpcParseError;
use near_jsonrpc_primitives::types::transactions::{
    RpcBroadcastTransactionRequest, RpcTransactionError, RpcTransactionResponse,
    RpcTransactionStatusCommonRequest, TransactionInfo,
};
use near_primitives::borsh::BorshDeserialize;
use near_primitives::transaction::SignedTransaction;
use near_primitives::views::FinalExecutionOutcomeViewEnum;

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

impl RpcFrom<FinalExecutionOutcomeViewEnum> for RpcTransactionResponse {
    fn rpc_from(final_execution_outcome: FinalExecutionOutcomeViewEnum) -> Self {
        Self { final_execution_outcome }
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
