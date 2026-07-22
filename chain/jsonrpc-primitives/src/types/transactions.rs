use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, ShardId};
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
        #[cfg_attr(feature = "schemars", schemars(skip))]
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
    /// `None` when the response comes from a node running an older version that omitted the
    /// cause, so newer clients can still parse it.
    #[error("Timeout")]
    TimeoutError(Option<TimeoutErrorCause>),
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcTransactionResponse {
    #[serde(flatten)]
    pub final_execution_outcome: Option<near_primitives::views::FinalExecutionOutcomeViewEnum>,
    pub final_execution_status: near_primitives::views::TxExecutionStatus,
}

/// Explains why a transaction status request returned a `RpcTransactionError::TimeoutError`:
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "cause", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TimeoutErrorCause {
    /// The node never observed the transaction on chain.
    NotObserved,
    /// The transaction was observed but is still pending the requested finality. The
    /// last-known status is included so the caller can re-poll for a higher finality.
    Pending { status: Box<RpcTransactionResponse> },
    /// The node does not track the transaction's shard and could not get an answer from a
    /// chunk producer that does before the timeout.
    DoesNotTrackShard { shard_id: ShardId },
    /// The node could not produce a usable transaction status before the timeout (for
    /// example a repeated internal error, or no response at all).
    Error { debug_info: String },
}

impl Default for TimeoutErrorCause {
    // A generic timeout error cause when extra context is not available.
    fn default() -> Self {
        Self::Error {
            debug_info: "the node timed out before returning a transaction status".to_string(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::views::TxExecutionStatus;

    /// On timeout the RPC returns a `TimeoutError` whose `cause` says how far the
    /// transaction got. The `Pending` cause carries the last-known status so callers
    /// retain full information and can re-poll for a higher finality.
    #[test]
    fn timeout_error_reports_pending() {
        let error = RpcTransactionError::TimeoutError(Some(TimeoutErrorCause::Pending {
            status: Box::new(RpcTransactionResponse {
                final_execution_outcome: None,
                final_execution_status: TxExecutionStatus::Included,
            }),
        }));

        // The cause and last-known status survive the conversion to the wire `RpcError`.
        let rpc_error: crate::errors::RpcError = error.into();
        let wire = serde_json::to_value(&rpc_error).unwrap();
        assert_eq!(wire["cause"]["name"], "TIMEOUT_ERROR");
        let info = &wire["cause"]["info"];
        assert_eq!(info["cause"], "PENDING");
        assert_eq!(info["status"]["final_execution_status"], "INCLUDED");
    }

    /// The `NotObserved` cause serializes as a bare tag with no payload.
    #[test]
    fn timeout_error_reports_not_observed() {
        let error = RpcTransactionError::TimeoutError(Some(TimeoutErrorCause::NotObserved));
        let value = serde_json::to_value(&error).unwrap();
        assert_eq!(value["name"], "TIMEOUT_ERROR");
        assert_eq!(value["info"]["cause"], "NOT_OBSERVED");
    }

    /// The error round-trips through serde, including the flattened status carried by
    /// `Pending`.
    #[test]
    fn timeout_error_round_trips() {
        let error = RpcTransactionError::TimeoutError(Some(TimeoutErrorCause::Pending {
            status: Box::new(RpcTransactionResponse {
                final_execution_outcome: None,
                final_execution_status: TxExecutionStatus::Included,
            }),
        }));
        let json = serde_json::to_string(&error).unwrap();
        let decoded: RpcTransactionError = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            decoded,
            RpcTransactionError::TimeoutError(Some(TimeoutErrorCause::Pending { .. }))
        ));
    }

    /// Backwards compatibility (old node -> new client): a node running the previous code
    /// serializes the unit `TimeoutError` as `{"name":"TIMEOUT_ERROR"}` with no `info`; a newer
    /// client must still parse that, as `TimeoutError(None)`.
    #[test]
    fn old_timeout_format_parses_on_new_client() {
        let old_wire = r#"{"name":"TIMEOUT_ERROR"}"#;
        let decoded: RpcTransactionError = serde_json::from_str(old_wire).unwrap();
        assert!(matches!(decoded, RpcTransactionError::TimeoutError(None)));
    }

    /// Backwards compatibility (new node -> old client): a newer node serializes the cause under
    /// `info`. A client strictly typed against the previous schema (the *unit* `TimeoutError`)
    /// CANNOT parse that — the `Option` wrapper does not fix this direction, because it's the
    /// already-deployed old client's deserializer that rejects the unexpected `info` map. This
    /// test pins that known limitation.
    #[test]
    fn new_timeout_format_breaks_strict_old_client() {
        #[derive(serde::Deserialize, Debug)]
        #[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
        enum OldRpcTransactionError {
            #[allow(dead_code)]
            InternalError {
                debug_info: String,
            },
            TimeoutError,
        }
        let new_wire = serde_json::to_string(&RpcTransactionError::TimeoutError(Some(
            TimeoutErrorCause::NotObserved,
        )))
        .unwrap();
        // Fails with "invalid type: map, expected unit variant".
        assert!(serde_json::from_str::<OldRpcTransactionError>(&new_wire).is_err());
    }
}
