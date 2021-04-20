use std::string::FromUtf8Error;
use std::time::Duration;

use actix::Addr;
use actix_cors::Cors;
use actix_web::{http, middleware, web, App, Error as HttpError, HttpResponse, HttpServer};
use futures::Future;
use futures::FutureExt;
use prometheus;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};
use tracing::info;

use near_chain_configs::GenesisConfig;
use near_client::{
    ClientActor, GetBlock, GetBlockProof, GetChunk, GetExecutionOutcome, GetGasPrice,
    GetNetworkInfo, GetNextLightClientBlock, GetProtocolConfig, GetReceipt, GetStateChanges,
    GetStateChangesInBlock, GetValidatorInfo, GetValidatorOrdered, Query, Status, TxStatus,
    TxStatusError, ViewClientActor,
};
pub use near_jsonrpc_client as client;
use near_jsonrpc_primitives::errors::RpcError;
use near_jsonrpc_primitives::message::{Message, Request};
use near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse;
use near_metrics::{Encoder, TextEncoder};
#[cfg(feature = "adversarial")]
use near_network::types::{NetworkAdversarialMessage, NetworkViewClientMessages};
use near_network::{NetworkClientMessages, NetworkClientResponses};
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::BaseEncode;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::FinalExecutionOutcomeViewEnum;

mod metrics;

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct RpcPollingConfig {
    pub polling_interval: Duration,
    pub polling_timeout: Duration,
}

impl Default for RpcPollingConfig {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_millis(500),
            polling_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcLimitsConfig {
    /// Maximum byte size of the json payload.
    pub json_payload_max_size: usize,
}

impl Default for RpcLimitsConfig {
    fn default() -> Self {
        Self { json_payload_max_size: 10 * 1024 * 1024 }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcConfig {
    pub addr: String,
    pub cors_allowed_origins: Vec<String>,
    pub polling_config: RpcPollingConfig,
    #[serde(default)]
    pub limits_config: RpcLimitsConfig,
}

impl Default for RpcConfig {
    fn default() -> Self {
        RpcConfig {
            addr: "0.0.0.0:3030".to_owned(),
            cors_allowed_origins: vec!["*".to_owned()],
            polling_config: Default::default(),
            limits_config: Default::default(),
        }
    }
}

impl RpcConfig {
    pub fn new(addr: &str) -> Self {
        RpcConfig { addr: addr.to_owned(), ..Default::default() }
    }
}

#[cfg(feature = "adversarial")]
fn parse_params<T: serde::de::DeserializeOwned>(value: Option<Value>) -> Result<T, RpcError> {
    if let Some(value) = value {
        serde_json::from_value(value)
            .map_err(|err| RpcError::invalid_params(format!("Failed parsing args: {}", err)))
    } else {
        Err(RpcError::invalid_params("Require at least one parameter".to_owned()))
    }
}

#[cfg(feature = "adversarial")]
fn jsonify<T: serde::Serialize>(
    response: Result<Result<T, String>, actix::MailboxError>,
) -> Result<Value, RpcError> {
    response
        .map_err(|err| err.to_string())
        .and_then(|value| {
            value.and_then(|value| serde_json::to_value(value).map_err(|err| err.to_string()))
        })
        .map_err(|err| RpcError::server_error(Some(err)))
}

#[easy_ext::ext(FromNetworkClientResponses)]
impl near_jsonrpc_primitives::types::transactions::RpcTransactionError {
    pub fn from_network_client_responses(responses: NetworkClientResponses) -> Self {
        match responses {
            NetworkClientResponses::InvalidTx(context) => Self::InvalidTransaction { context },
            NetworkClientResponses::NoResponse => Self::TimeoutError,
            NetworkClientResponses::DoesNotTrackShard | NetworkClientResponses::RequestRouted => {
                Self::DoesNotTrackShard
            }
            internal_error => Self::InternalError { debug_info: format!("{:?}", internal_error) },
        }
    }
}

/// This function processes response from query method to introduce
/// backward compatible response in case of specific errors
fn process_query_response(
    query_response: Result<
        near_jsonrpc_primitives::types::query::RpcQueryResponse,
        near_jsonrpc_primitives::types::query::RpcQueryError,
    >,
) -> Result<Value, RpcError> {
    // This match is used here to give backward compatible error message for specific
    // error variants. Should be refactored once structured errors fully shipped
    match query_response {
        Ok(rpc_query_response) => serde_json::to_value(rpc_query_response)
            .map_err(|err| RpcError::parse_error(err.to_string())),
        Err(err) => match err {
            near_jsonrpc_primitives::types::query::RpcQueryError::ContractExecutionError {
                vm_error,
                block_height,
                block_hash,
            } => Ok(json!({
                "error": vm_error,
                "logs": json!([]),
                "block_height": block_height,
                "block_hash": block_hash,
            })),
            near_jsonrpc_primitives::types::query::RpcQueryError::UnknownAccessKey {
                public_key,
                block_height,
                block_hash,
            } => Ok(json!({
                "error": format!(
                    "access key {} does not exist while viewing",
                    public_key.to_string()
                ),
                "logs": json!([]),
                "block_height": block_height,
                "block_hash": block_hash,
            })),
            near_jsonrpc_primitives::types::query::RpcQueryError::UnknownBlock {
                ref block_reference,
            } => match block_reference {
                near_primitives::types::BlockReference::BlockId(block_id) => Err(RpcError::new(
                    -32_000,
                    "Server error".to_string(),
                    Some(match block_id {
                        near_primitives::types::BlockId::Height(height) => json!(format!(
                            "DB Not Found Error: BLOCK HEIGHT: {} \n Cause: Unknown",
                            height
                        )),
                        near_primitives::types::BlockId::Hash(block_hash) => {
                            json!(format!("DB Not Found Error: BLOCK HEADER: {}", block_hash))
                        }
                    }),
                )),
                _ => Err(err.into()),
            },
            _ => Err(err.into()),
        },
    }
}

struct JsonRpcHandler {
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
    polling_config: RpcPollingConfig,
    genesis_config: GenesisConfig,
}

impl JsonRpcHandler {
    pub async fn process(&self, message: Message) -> Result<Message, HttpError> {
        let id = message.id();
        match message {
            Message::Request(request) => {
                Ok(Message::response(id, self.process_request(request).await))
            }
            _ => Ok(Message::error(RpcError::invalid_request())),
        }
    }

    async fn process_request(&self, request: Request) -> Result<Value, RpcError> {
        near_metrics::inc_counter_vec(&metrics::HTTP_RPC_REQUEST_COUNT, &[request.method.as_ref()]);
        let _rpc_processing_time = near_metrics::start_timer_vec(
            &metrics::RPC_PROCESSING_TIME,
            &[request.method.as_ref()],
        );

        #[cfg(feature = "adversarial")]
        {
            let params = request.params.clone();

            let res = match request.method.as_ref() {
                // Adversarial controls
                "adv_set_weight" => Some(self.adv_set_sync_info(params).await),
                "adv_disable_header_sync" => Some(self.adv_disable_header_sync(params).await),
                "adv_disable_doomslug" => Some(self.adv_disable_doomslug(params).await),
                "adv_produce_blocks" => Some(self.adv_produce_blocks(params).await),
                "adv_switch_to_height" => Some(self.adv_switch_to_height(params).await),
                "adv_get_saved_blocks" => Some(self.adv_get_saved_blocks(params).await),
                "adv_check_store" => Some(self.adv_check_store(params).await),
                _ => None,
            };

            if let Some(res) = res {
                return res;
            }
        }

        let response: Result<Value, RpcError> = match request.method.as_ref() {
            // Handlers ordered alphabetically
            "block" => {
                let rpc_block_request =
                    near_jsonrpc_primitives::types::blocks::RpcBlockRequest::parse(request.params)?;
                let block = self.block(rpc_block_request).await?;
                serde_json::to_value(block)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "broadcast_tx_async" => {
                let rpc_transaction_request =
                    near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest::parse(
                        request.params,
                    )?;
                let transaction_hash = self.send_tx_async(rpc_transaction_request).await;
                serde_json::to_value((&transaction_hash).to_base())
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "broadcast_tx_commit" => {
                let rpc_transaction_request =
                    near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest::parse(
                        request.params,
                    )?;
                let send_tx_response = self.send_tx_commit(rpc_transaction_request).await?;
                serde_json::to_value(send_tx_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "chunk" => {
                let rpc_chunk_request =
                    near_jsonrpc_primitives::types::chunks::RpcChunkRequest::parse(request.params)?;
                let chunk = self.chunk(rpc_chunk_request).await?;
                serde_json::to_value(chunk)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "gas_price" => {
                let rpc_gas_price_request =
                    near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest::parse(
                        request.params,
                    )?;
                let gas_price = self.gas_price(rpc_gas_price_request).await?;
                serde_json::to_value(gas_price)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "health" => {
                let health_response = self.health().await?;
                serde_json::to_value(health_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "light_client_proof" => {
                let rpc_light_client_execution_proof_request = near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest::parse(request.params)?;
                let rpc_light_client_execution_proof_response = self
                    .light_client_execution_outcome_proof(rpc_light_client_execution_proof_request)
                    .await?;
                serde_json::to_value(rpc_light_client_execution_proof_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "next_light_client_block" => {
                let rpc_light_client_next_block_request = near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockRequest::parse(request.params)?;
                let next_light_client_block =
                    self.next_light_client_block(rpc_light_client_next_block_request).await?;
                serde_json::to_value(next_light_client_block)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "network_info" => {
                let network_info_response = self.network_info().await?;
                serde_json::to_value(network_info_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "query" => {
                let rpc_query_request =
                    near_jsonrpc_primitives::types::query::RpcQueryRequest::parse(request.params)?;
                let query_response = self.query(rpc_query_request).await;
                process_query_response(query_response)
            }
            "status" => {
                let status_response = self.status().await?;
                serde_json::to_value(status_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "tx" => {
                let rpc_transaction_status_common_request =
                    near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest::parse(request.params)?;
                let rpc_transaction_response =
                    self.tx_status_common(rpc_transaction_status_common_request, false).await?;
                serde_json::to_value(rpc_transaction_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "validators" => {
                let rpc_validator_request =
                    near_jsonrpc_primitives::types::validator::RpcValidatorRequest::parse(
                        request.params,
                    )?;
                let validator_info = self.validators(rpc_validator_request).await?;
                serde_json::to_value(validator_info)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_broadcast_tx_sync" => {
                let rpc_transaction_request =
                    near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest::parse(
                        request.params,
                    )?;
                let broadcast_tx_sync_response = self.send_tx_sync(rpc_transaction_request).await?;
                serde_json::to_value(broadcast_tx_sync_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_changes" => {
                let rpc_state_changes_request =
                    near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest::parse(
                        request.params,
                    )?;
                let state_changes =
                    self.changes_in_block_by_type(rpc_state_changes_request).await?;
                serde_json::to_value(state_changes)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_changes_in_block" => {
                let rpc_state_changes_request =
                    near_jsonrpc_primitives::types::changes::RpcStateChangesRequest::parse(
                        request.params,
                    )?;
                let state_changes = self.changes_in_block(rpc_state_changes_request).await?;
                serde_json::to_value(state_changes)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_check_tx" => {
                let rpc_transaction_request =
                    near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest::parse(
                        request.params,
                    )?;
                let broadcast_tx_sync_response = self.check_tx(rpc_transaction_request).await?;
                serde_json::to_value(broadcast_tx_sync_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_genesis_config" => {
                let genesis_config = self.genesis_config().await;
                serde_json::to_value(genesis_config)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_light_client_proof" => {
                let rpc_light_client_execution_proof_request = near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest::parse(request.params)?;
                let rpc_light_client_execution_proof_response = self
                    .light_client_execution_outcome_proof(rpc_light_client_execution_proof_request)
                    .await?;
                serde_json::to_value(rpc_light_client_execution_proof_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_protocol_config" => {
                let rpc_protocol_config_request =
                    near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest::parse(
                        request.params,
                    )?;
                let config = self.protocol_config(rpc_protocol_config_request).await?;
                serde_json::to_value(config)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_receipt" => {
                let rpc_receipt_request =
                    near_jsonrpc_primitives::types::receipts::RpcReceiptRequest::parse(
                        request.params,
                    )?;
                let receipt = self.receipt(rpc_receipt_request).await?;
                serde_json::to_value(receipt)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_tx_status" => {
                let rpc_transaction_status_common_request = near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest::parse(request.params)?;
                let rpc_transaction_response =
                    self.tx_status_common(rpc_transaction_status_common_request, true).await?;
                serde_json::to_value(rpc_transaction_response)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            "EXPERIMENTAL_validators_ordered" => {
                let rpc_validators_ordered_request =
                    near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest::parse(
                        request.params,
                    )?;
                let validators = self.validators_ordered(rpc_validators_ordered_request).await?;
                serde_json::to_value(validators)
                    .map_err(|err| RpcError::serialization_error(err.to_string()))
            }
            _ => Err(RpcError::method_not_found(request.method.clone())),
        };

        if let Err(err) = &response {
            near_metrics::inc_counter_vec(
                &metrics::RPC_ERROR_COUNT,
                &[request.method.as_ref(), &err.code.to_string()],
            );
        }

        response
    }

    async fn send_tx_async(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest,
    ) -> CryptoHash {
        let tx = request_data.signed_transaction;
        let hash = tx.get_hash().clone();
        self.client_addr.do_send(NetworkClientMessages::Transaction {
            transaction: tx,
            is_forwarded: false,
            check_only: false, // if we set true here it will not actually send the transaction
        });
        hash
    }

    async fn tx_exists(
        &self,
        tx_hash: CryptoHash,
        signer_account_id: &AccountId,
    ) -> Result<bool, near_jsonrpc_primitives::types::transactions::RpcTransactionError> {
        timeout(self.polling_config.polling_timeout, async {
            loop {
                // TODO(optimization): Introduce a view_client method to only get transaction
                // status without the information about execution outcomes.
                match self
                    .view_client_addr
                    .send(TxStatus {
                        tx_hash,
                        signer_account_id: signer_account_id.clone(),
                        fetch_receipt: false,
                    })
                    .await
                {
                    Ok(Ok(Some(_))) => {
                        return Ok(true);
                    }
                    Ok(Err(TxStatusError::MissingTransaction(_))) => {
                        return Ok(false);
                    }
                    Err(err) => return Err(near_jsonrpc_primitives::types::transactions::RpcTransactionError::InternalError {
                        debug_info: format!("{:?}", err)
                    }),
                    _ => {}
                }
                sleep(self.polling_config.polling_interval).await;
            }
        })
        .await
        .map_err(|_| {
            near_metrics::inc_counter(&metrics::RPC_TIMEOUT_TOTAL);
            tracing::warn!(
                target: "jsonrpc", "Timeout: tx_exists method. tx_hash {:?} signer_account_id {:?}",
                tx_hash,
                signer_account_id
            );
            near_jsonrpc_primitives::types::transactions::RpcTransactionError::TimeoutError
        })?
    }

    async fn tx_status_fetch(
        &self,
        tx_info: near_jsonrpc_primitives::types::transactions::TransactionInfo,
        fetch_receipt: bool,
    ) -> Result<FinalExecutionOutcomeViewEnum, TxStatusError> {
        let (tx_hash, account_id) = match &tx_info {
            near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(tx) => {
                (tx.get_hash(), tx.transaction.signer_id.clone())
            }
            near_jsonrpc_primitives::types::transactions::TransactionInfo::TransactionId {
                hash,
                account_id,
            } => (*hash, account_id.clone()),
        };
        timeout(self.polling_config.polling_timeout, async {
            loop {
                let tx_status_result = self
                    .view_client_addr
                    .send(TxStatus {
                        tx_hash,
                        signer_account_id: account_id.clone(),
                        fetch_receipt,
                    })
                    .await;
                match tx_status_result {
                    Ok(Ok(Some(outcome))) => break Ok(outcome),
                    Ok(Ok(None)) => {} // No such transaction recorded on chain yet
                    Ok(Err(err @ TxStatusError::MissingTransaction(_))) => {
                        if let near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(tx) = &tx_info {
                            if let Ok(NetworkClientResponses::InvalidTx(e)) =
                                self.send_tx(tx.clone(), true).await
                            {
                                break Err(TxStatusError::InvalidTx(e));
                            }
                        }
                        break Err(err);
                    }
                    Ok(Err(err)) => break Err(err),
                    Err(err) => break Err(TxStatusError::InternalError(err.to_string())),
                }
                let _ = sleep(self.polling_config.polling_interval).await;
            }
        })
        .await
        .map_err(|_| {
            near_metrics::inc_counter(&metrics::RPC_TIMEOUT_TOTAL);
            tracing::warn!(
                target: "jsonrpc", "Timeout: tx_status_fetch method. tx_info {:?} fetch_receipt {:?}",
                tx_info,
                fetch_receipt,
            );
            TxStatusError::TimeoutError
        })?
    }

    async fn tx_polling(
        &self,
        tx_info: near_jsonrpc_primitives::types::transactions::TransactionInfo,
    ) -> Result<
        near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
        near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    > {
        timeout(self.polling_config.polling_timeout, async {
            loop {
                match self.tx_status_fetch(tx_info.clone(), false).await {
                    Ok(tx_status) => {
                        break Ok(
                            near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                                final_execution_outcome: tx_status,
                            },
                        )
                    }
                    // If transaction is missing, keep polling.
                    Err(TxStatusError::MissingTransaction(_)) => {}
                    // If we hit any other error, we return to the user.
                    Err(err) => {
                        break Err(err.into());
                    }
                }
                let _ = sleep(self.polling_config.polling_interval).await;
            }
        })
        .await
        .map_err(|_| {
            near_metrics::inc_counter(&metrics::RPC_TIMEOUT_TOTAL);
            tracing::warn!(
                target: "jsonrpc", "Timeout: tx_polling method. tx_info {:?}",
                tx_info,
            );
            near_jsonrpc_primitives::types::transactions::RpcTransactionError::TimeoutError
        })?
    }

    /// Send a transaction idempotently (subsequent send of the same transaction will not cause
    /// any new side-effects and the result will be the same unless we garbage collected it
    /// already).
    async fn send_tx(
        &self,
        tx: SignedTransaction,
        check_only: bool,
    ) -> Result<
        NetworkClientResponses,
        near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    > {
        let tx_hash = tx.get_hash();
        let signer_account_id = tx.transaction.signer_id.clone();
        let response = self
            .client_addr
            .send(NetworkClientMessages::Transaction {
                transaction: tx,
                is_forwarded: false,
                check_only,
            })
            .await?;

        // If we receive InvalidNonce error, it might be the case that the transaction was
        // resubmitted, and we should check if that is the case and return ValidTx response to
        // maintain idempotence of the send_tx method.
        if let NetworkClientResponses::InvalidTx(
            near_primitives::errors::InvalidTxError::InvalidNonce { .. },
        ) = response
        {
            if self.tx_exists(tx_hash, &signer_account_id).await? {
                return Ok(NetworkClientResponses::ValidTx);
            }
        }

        Ok(response)
    }

    async fn send_tx_sync(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::transactions::RpcBroadcastTxSyncResponse,
        near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    > {
        match self.send_tx(request_data.clone().signed_transaction, false).await? {
            NetworkClientResponses::ValidTx => {
                Ok(near_jsonrpc_primitives::types::transactions::RpcBroadcastTxSyncResponse {
                    transaction_hash: request_data.signed_transaction.get_hash(),
                })
            }
            NetworkClientResponses::RequestRouted => {
                Err(near_jsonrpc_primitives::types::transactions::RpcTransactionError::RequestRouted {
                    transaction_hash: request_data.signed_transaction.get_hash(),
                })
            }
            network_client_responses=> Err(
                near_jsonrpc_primitives::types::transactions::RpcTransactionError::from_network_client_responses(
                    network_client_responses
                )
            )
        }
    }

    async fn check_tx(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::transactions::RpcBroadcastTxSyncResponse,
        near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    > {
        match self.send_tx(request_data.clone().signed_transaction, true).await? {
            NetworkClientResponses::ValidTx => {
                Ok(near_jsonrpc_primitives::types::transactions::RpcBroadcastTxSyncResponse {
                    transaction_hash: request_data.signed_transaction.get_hash(),
                })
            }
            NetworkClientResponses::RequestRouted => {
                Err(near_jsonrpc_primitives::types::transactions::RpcTransactionError::RequestRouted {
                    transaction_hash: request_data.signed_transaction.get_hash(),
                })
            }
            network_client_responses => Err(
                near_jsonrpc_primitives::types::transactions::RpcTransactionError::from_network_client_responses(network_client_responses)
            )
        }
    }

    async fn send_tx_commit(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcBroadcastTransactionRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
        near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    > {
        let tx = request_data.signed_transaction;
        match self
            .tx_status_fetch(
                near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(
                    tx.clone(),
                ),
                false,
            )
            .await
        {
            Ok(outcome) => {
                return Ok(near_jsonrpc_primitives::types::transactions::RpcTransactionResponse {
                    final_execution_outcome: outcome,
                });
            }
            Err(TxStatusError::InvalidTx(invalid_tx_error)) => {
                return Err(near_jsonrpc_primitives::types::transactions::RpcTransactionError::InvalidTransaction {
                    context: invalid_tx_error
                });
            }
            _ => {}
        }
        match self.send_tx(tx.clone(), false).await? {
            NetworkClientResponses::ValidTx | NetworkClientResponses::RequestRouted => {
                self.tx_polling(near_jsonrpc_primitives::types::transactions::TransactionInfo::Transaction(tx)).await
            }
            network_client_response=> {
                Err(
                    near_jsonrpc_primitives::types::transactions::RpcTransactionError::from_network_client_responses(
                        network_client_response
                    )
                )
            }
        }
    }

    async fn health(
        &self,
    ) -> Result<
        near_jsonrpc_primitives::types::status::RpcHealthResponse,
        near_jsonrpc_primitives::types::status::RpcStatusError,
    > {
        Ok(self.client_addr.send(Status { is_health_check: true }).await??.into())
    }

    pub async fn status(
        &self,
    ) -> Result<
        near_jsonrpc_primitives::types::status::RpcStatusResponse,
        near_jsonrpc_primitives::types::status::RpcStatusError,
    > {
        Ok(self.client_addr.send(Status { is_health_check: false }).await??.into())
    }

    /// Expose Genesis Config (with internal Runtime Config) without state records to keep the
    /// output at a reasonable size.
    ///
    /// See also `genesis_records` API.
    pub async fn genesis_config(&self) -> &GenesisConfig {
        &self.genesis_config
    }

    pub async fn protocol_config(
        &self,
        request_data: near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::config::RpcProtocolConfigResponse,
        near_jsonrpc_primitives::types::config::RpcProtocolConfigError,
    > {
        let config_view = self
            .view_client_addr
            .send(GetProtocolConfig(request_data.block_reference.into()))
            .await??;
        Ok(RpcProtocolConfigResponse { config_view })
    }

    async fn query(
        &self,
        request_data: near_jsonrpc_primitives::types::query::RpcQueryRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::query::RpcQueryResponse,
        near_jsonrpc_primitives::types::query::RpcQueryError,
    > {
        let query = Query::new(request_data.block_reference, request_data.request);
        Ok(self.view_client_addr.send(query).await??.into())
    }

    async fn tx_status_common(
        &self,
        request_data: near_jsonrpc_primitives::types::transactions::RpcTransactionStatusCommonRequest,
        fetch_receipt: bool,
    ) -> Result<
        near_jsonrpc_primitives::types::transactions::RpcTransactionResponse,
        near_jsonrpc_primitives::types::transactions::RpcTransactionError,
    > {
        Ok(self.tx_status_fetch(request_data.transaction_info, fetch_receipt).await?.into())
    }

    async fn block(
        &self,
        request_data: near_jsonrpc_primitives::types::blocks::RpcBlockRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::blocks::RpcBlockResponse,
        near_jsonrpc_primitives::types::blocks::RpcBlockError,
    > {
        let block_view =
            self.view_client_addr.send(GetBlock(request_data.block_reference.into())).await??;
        Ok(near_jsonrpc_primitives::types::blocks::RpcBlockResponse { block_view })
    }

    async fn chunk(
        &self,
        request_data: near_jsonrpc_primitives::types::chunks::RpcChunkRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::chunks::RpcChunkResponse,
        near_jsonrpc_primitives::types::chunks::RpcChunkError,
    > {
        let chunk_view =
            self.view_client_addr.send(GetChunk::from(request_data.chunk_reference)).await??;
        Ok(near_jsonrpc_primitives::types::chunks::RpcChunkResponse { chunk_view })
    }

    async fn receipt(
        &self,
        request_data: near_jsonrpc_primitives::types::receipts::RpcReceiptRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::receipts::RpcReceiptResponse,
        near_jsonrpc_primitives::types::receipts::RpcReceiptError,
    > {
        match self
            .view_client_addr
            .send(GetReceipt { receipt_id: request_data.receipt_reference.receipt_id })
            .await??
        {
            Some(receipt_view) => {
                Ok(near_jsonrpc_primitives::types::receipts::RpcReceiptResponse { receipt_view })
            }
            None => Err(near_jsonrpc_primitives::types::receipts::RpcReceiptError::UnknownReceipt(
                request_data.receipt_reference.receipt_id,
            )),
        }
    }

    async fn changes_in_block(
        &self,
        request: near_jsonrpc_primitives::types::changes::RpcStateChangesRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse,
        near_jsonrpc_primitives::types::changes::RpcStateChangesError,
    > {
        let block = self.view_client_addr.send(GetBlock(request.block_reference.into())).await??;

        let block_hash = block.header.hash.clone();
        let changes = self.view_client_addr.send(GetStateChangesInBlock { block_hash }).await??;

        Ok(near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockResponse {
            block_hash: block.header.hash,
            changes,
        })
    }

    async fn changes_in_block_by_type(
        &self,
        request: near_jsonrpc_primitives::types::changes::RpcStateChangesInBlockRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::changes::RpcStateChangesResponse,
        near_jsonrpc_primitives::types::changes::RpcStateChangesError,
    > {
        let block = self.view_client_addr.send(GetBlock(request.block_reference.into())).await??;

        let block_hash = block.header.hash.clone();
        let changes = self
            .view_client_addr
            .send(GetStateChanges {
                block_hash,
                state_changes_request: request.state_changes_request,
            })
            .await??;

        Ok(near_jsonrpc_primitives::types::changes::RpcStateChangesResponse {
            block_hash: block.header.hash,
            changes,
        })
    }

    async fn next_light_client_block(
        &self,
        request: near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockResponse,
        near_jsonrpc_primitives::types::light_client::RpcLightClientNextBlockError,
    > {
        Ok(self
            .view_client_addr
            .send(GetNextLightClientBlock { last_block_hash: request.last_block_hash })
            .await??
            .into())
    }

    async fn light_client_execution_outcome_proof(
        &self,
        request: near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofResponse,
        near_jsonrpc_primitives::types::light_client::RpcLightClientProofError,
    > {
        let near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofRequest {
            id,
            light_client_head,
        } = request;

        let execution_outcome_proof =
            self.view_client_addr.send(GetExecutionOutcome { id }).await??;

        let block_proof = self
            .view_client_addr
            .send(GetBlockProof {
                block_hash: execution_outcome_proof.outcome_proof.block_hash,
                head_block_hash: light_client_head,
            })
            .await??;

        Ok(near_jsonrpc_primitives::types::light_client::RpcLightClientExecutionProofResponse {
            outcome_proof: execution_outcome_proof.outcome_proof,
            outcome_root_proof: execution_outcome_proof.outcome_root_proof,
            block_header_lite: block_proof.block_header_lite,
            block_proof: block_proof.proof,
        })
    }

    async fn network_info(
        &self,
    ) -> Result<
        near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse,
        near_jsonrpc_primitives::types::network_info::RpcNetworkInfoError,
    > {
        Ok(self.client_addr.send(GetNetworkInfo {}).await??.into())
    }

    async fn gas_price(
        &self,
        request_data: near_jsonrpc_primitives::types::gas_price::RpcGasPriceRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse,
        near_jsonrpc_primitives::types::gas_price::RpcGasPriceError,
    > {
        let gas_price_view =
            self.view_client_addr.send(GetGasPrice { block_id: request_data.block_id }).await??;
        Ok(near_jsonrpc_primitives::types::gas_price::RpcGasPriceResponse { gas_price_view })
    }

    pub async fn metrics(&self) -> Result<String, FromUtf8Error> {
        // Gather metrics and return them as a String
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        encoder.encode(&prometheus::gather(), &mut buffer).unwrap();

        String::from_utf8(buffer)
    }

    async fn validators(
        &self,
        request_data: near_jsonrpc_primitives::types::validator::RpcValidatorRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::validator::RpcValidatorResponse,
        near_jsonrpc_primitives::types::validator::RpcValidatorError,
    > {
        let validator_info = self
            .view_client_addr
            .send(GetValidatorInfo { epoch_reference: request_data.epoch_reference })
            .await??;
        Ok(near_jsonrpc_primitives::types::validator::RpcValidatorResponse { validator_info })
    }

    /// Returns the current epoch validators ordered in the block producer order with repetition.
    /// This endpoint is solely used for bridge currently and is not intended for other external use
    /// cases.
    async fn validators_ordered(
        &self,
        request: near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest,
    ) -> Result<
        near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedResponse,
        near_jsonrpc_primitives::types::validator::RpcValidatorError,
    > {
        let near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest { block_id } =
            request;
        Ok(self.view_client_addr.send(GetValidatorOrdered { block_id }).await??.into())
    }
}

#[cfg(feature = "adversarial")]
impl JsonRpcHandler {
    async fn adv_set_sync_info(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let height = parse_params::<u64>(params)?;
        actix::spawn(
            self.view_client_addr
                .send(NetworkViewClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvSetSyncInfo(height),
                ))
                .map(|_| ()),
        );
        Ok(Value::String("".to_string()))
    }

    async fn adv_disable_header_sync(&self, _params: Option<Value>) -> Result<Value, RpcError> {
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvDisableHeaderSync,
                ))
                .map(|_| ()),
        );
        actix::spawn(
            self.view_client_addr
                .send(NetworkViewClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvDisableHeaderSync,
                ))
                .map(|_| ()),
        );
        Ok(Value::String("".to_string()))
    }

    async fn adv_disable_doomslug(&self, _params: Option<Value>) -> Result<Value, RpcError> {
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvDisableDoomslug,
                ))
                .map(|_| ()),
        );
        actix::spawn(
            self.view_client_addr
                .send(NetworkViewClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvDisableDoomslug,
                ))
                .map(|_| ()),
        );
        Ok(Value::String("".to_string()))
    }

    async fn adv_produce_blocks(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (num_blocks, only_valid) = parse_params::<(u64, bool)>(params)?;
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvProduceBlocks(num_blocks, only_valid),
                ))
                .map(|_| ()),
        );
        Ok(Value::String("".to_string()))
    }

    async fn adv_switch_to_height(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (height,) = parse_params::<(u64,)>(params)?;
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvSwitchToHeight(height),
                ))
                .map(|_| ()),
        );
        actix::spawn(
            self.view_client_addr
                .send(NetworkViewClientMessages::Adversarial(
                    NetworkAdversarialMessage::AdvSwitchToHeight(height),
                ))
                .map(|_| ()),
        );
        Ok(Value::String("".to_string()))
    }

    async fn adv_get_saved_blocks(&self, _params: Option<Value>) -> Result<Value, RpcError> {
        match self
            .client_addr
            .send(NetworkClientMessages::Adversarial(NetworkAdversarialMessage::AdvGetSavedBlocks))
            .await
        {
            Ok(result) => match result {
                NetworkClientResponses::AdvResult(value) => jsonify(Ok(Ok(value))),
                _ => Err(RpcError::server_error::<String>(None)),
            },
            _ => Err(RpcError::server_error::<String>(None)),
        }
    }

    async fn adv_check_store(&self, _params: Option<Value>) -> Result<Value, RpcError> {
        match self
            .client_addr
            .send(NetworkClientMessages::Adversarial(
                NetworkAdversarialMessage::AdvCheckStorageConsistency,
            ))
            .await
        {
            Ok(result) => match result {
                NetworkClientResponses::AdvResult(value) => jsonify(Ok(Ok(value))),
                _ => Err(RpcError::server_error::<String>(None)),
            },
            _ => Err(RpcError::server_error::<String>(None)),
        }
    }
}

fn rpc_handler(
    message: web::Json<Message>,
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Output = Result<HttpResponse, HttpError>> {
    let response = async move {
        let message = handler.process(message.0).await?;
        Ok(HttpResponse::Ok().json(&message))
    };
    response.boxed()
}

fn status_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Output = Result<HttpResponse, HttpError>> {
    near_metrics::inc_counter(&metrics::HTTP_STATUS_REQUEST_COUNT);

    let response = async move {
        match handler.status().await {
            Ok(value) => Ok(HttpResponse::Ok().json(&value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed()
}

fn health_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Output = Result<HttpResponse, HttpError>> {
    let response = async move {
        match handler.health().await {
            Ok(value) => Ok(HttpResponse::Ok().json(&value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed()
}

fn network_info_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Output = Result<HttpResponse, HttpError>> {
    let response = async move {
        match handler.network_info().await {
            Ok(value) => Ok(HttpResponse::Ok().json(&value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed()
}

fn prometheus_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Output = Result<HttpResponse, HttpError>> {
    near_metrics::inc_counter(&metrics::PROMETHEUS_REQUEST_COUNT);

    let response = async move {
        match handler.metrics().await {
            Ok(value) => Ok(HttpResponse::Ok().body(value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed()
}

fn get_cors(cors_allowed_origins: &[String]) -> Cors {
    let mut cors = Cors::permissive();
    if cors_allowed_origins != ["*".to_string()] {
        for origin in cors_allowed_origins {
            cors = cors.allowed_origin(&origin);
        }
    }
    cors.allowed_methods(vec!["GET", "POST"])
        .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
        .allowed_header(http::header::CONTENT_TYPE)
        .max_age(3600)
}

pub fn start_http(
    config: RpcConfig,
    genesis_config: GenesisConfig,
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
) {
    let RpcConfig { addr, cors_allowed_origins, polling_config, limits_config } = config;
    info!(target:"network", "Starting http server at {}", addr);
    HttpServer::new(move || {
        App::new()
            .wrap(get_cors(&cors_allowed_origins))
            .data(JsonRpcHandler {
                client_addr: client_addr.clone(),
                view_client_addr: view_client_addr.clone(),
                polling_config,
                genesis_config: genesis_config.clone(),
            })
            .app_data(web::JsonConfig::default().limit(limits_config.json_payload_max_size))
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::post().to(rpc_handler)))
            .service(
                web::resource("/status")
                    .route(web::get().to(status_handler))
                    .route(web::head().to(status_handler)),
            )
            .service(
                web::resource("/health")
                    .route(web::get().to(health_handler))
                    .route(web::head().to(health_handler)),
            )
            .service(web::resource("/network_info").route(web::get().to(network_info_handler)))
            .service(web::resource("/metrics").route(web::get().to(prometheus_handler)))
    })
    .bind(addr)
    .unwrap()
    .workers(4)
    .shutdown_timeout(5)
    .run();
}
