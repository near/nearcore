extern crate prometheus;

use std::convert::TryFrom;
use std::string::FromUtf8Error;
use std::time::Duration;

use actix::{Addr, MailboxError};
use actix_cors::{Cors, CorsFactory};
use actix_web::{http, middleware, web, App, Error as HttpError, HttpResponse, HttpServer};
use borsh::BorshDeserialize;
use futures::Future;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};

use futures::{FutureExt, TryFutureExt};
use message::Message;
use message::{Request, RpcError};
use near_client::{
    ClientActor, GetBlock, GetChunk, GetGasPrice, GetKeyValueChanges, GetNetworkInfo,
    GetNextLightClientBlock, GetValidatorInfo, Query, Status, TxStatus, ViewClientActor,
};
pub use near_jsonrpc_client as client;
use near_jsonrpc_client::{message, BlockId, ChunkId};
use near_metrics::{Encoder, TextEncoder};
use near_network::{NetworkClientMessages, NetworkClientResponses};
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::{from_base, from_base64, BaseEncode};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::views::{ExecutionErrorView, FinalExecutionStatus};
use tokio::time::{delay_for, timeout};

mod metrics;
pub mod test_utils;

/// Maximum byte size of the json payload.
const JSON_PAYLOAD_MAX_SIZE: usize = 2 * 1024 * 1024;
const QUERY_DATA_MAX_SIZE: usize = 2 * 1024;

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
pub struct RpcConfig {
    pub addr: String,
    pub cors_allowed_origins: Vec<String>,
    pub polling_config: RpcPollingConfig,
}

impl Default for RpcConfig {
    fn default() -> Self {
        RpcConfig {
            addr: "0.0.0.0:3030".to_owned(),
            cors_allowed_origins: vec!["*".to_owned()],
            polling_config: Default::default(),
        }
    }
}

impl RpcConfig {
    pub fn new(addr: &str) -> Self {
        RpcConfig { addr: addr.to_owned(), ..Default::default() }
    }
}

fn from_base_or_parse_err(encoded: String) -> Result<Vec<u8>, RpcError> {
    from_base(&encoded).map_err(|err| RpcError::parse_error(err.to_string()))
}

fn from_base64_or_parse_err(encoded: String) -> Result<Vec<u8>, RpcError> {
    from_base64(&encoded).map_err(|err| RpcError::parse_error(err.to_string()))
}

fn parse_params<T: DeserializeOwned>(value: Option<Value>) -> Result<T, RpcError> {
    if let Some(value) = value {
        serde_json::from_value(value)
            .map_err(|err| RpcError::invalid_params(Some(format!("Failed parsing args: {}", err))))
    } else {
        Err(RpcError::invalid_params(Some("Require at least one parameter".to_owned())))
    }
}

fn jsonify<T: serde::Serialize>(
    response: Result<Result<T, String>, MailboxError>,
) -> Result<Value, RpcError> {
    response
        .map_err(|err| err.to_string())
        .and_then(|value| {
            value.and_then(|value| serde_json::to_value(value).map_err(|err| err.to_string()))
        })
        .map_err(|err| RpcError::server_error(Some(err)))
}

fn parse_tx(params: Option<Value>) -> Result<SignedTransaction, RpcError> {
    let (encoded,) = parse_params::<(String,)>(params)?;
    let bytes = from_base64_or_parse_err(encoded)?;
    SignedTransaction::try_from_slice(&bytes)
        .map_err(|e| RpcError::invalid_params(Some(format!("Failed to decode transaction: {}", e))))
}

fn parse_hash(params: Option<Value>) -> Result<CryptoHash, RpcError> {
    let (encoded,) = parse_params::<(String,)>(params)?;
    from_base_or_parse_err(encoded).and_then(|bytes| {
        CryptoHash::try_from(bytes).map_err(|err| RpcError::parse_error(err.to_string()))
    })
}

fn convert_mailbox_error(e: MailboxError) -> ExecutionErrorView {
    ExecutionErrorView { error_message: e.to_string(), error_type: "MailBoxError".to_string() }
}

fn timeout_err() -> RpcError {
    RpcError::server_error(Some(ExecutionErrorView {
        error_message: "send_tx_commit has timed out".to_string(),
        error_type: "TimeoutError".to_string(),
    }))
}

struct JsonRpcHandler {
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
    polling_config: RpcPollingConfig,
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
        match request.method.as_ref() {
            "broadcast_tx_async" => self.send_tx_async(request.params).await,
            "broadcast_tx_commit" => self.send_tx_commit(request.params).await,
            "validators" => self.validators(request.params).await,
            "query" => self.query(request.params).await,
            "health" => self.health().await,
            "status" => self.status().await,
            "tx" => self.tx_status(request.params).await,
            "block" => self.block(request.params).await,
            "chunk" => self.chunk(request.params).await,
            "changes" => self.changes(request.params).await,
            "next_light_client_block" => self.next_light_client_block(request.params).await,
            "network_info" => self.network_info().await,
            "gas_price" => self.gas_price(request.params).await,
            _ => Err(RpcError::method_not_found(request.method)),
        }
    }

    async fn send_tx_async(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let tx = parse_tx(params)?;
        let hash = (&tx.get_hash()).to_base();
        actix::spawn(self.client_addr.send(NetworkClientMessages::Transaction(tx)).map(drop));
        Ok(Value::String(hash))
    }

    async fn tx_polling(
        &self,
        tx_hash: CryptoHash,
        account_id: AccountId,
    ) -> Result<Value, RpcError> {
        timeout(self.polling_config.polling_timeout, async {
            loop {
                let final_tx = self
                    .view_client_addr
                    .send(TxStatus { tx_hash, signer_account_id: account_id.clone() })
                    .await;
                if let Ok(Ok(Some(ref tx_result))) = final_tx {
                    match tx_result.status {
                        FinalExecutionStatus::Started | FinalExecutionStatus::NotStarted => {}
                        FinalExecutionStatus::Failure(_)
                        | FinalExecutionStatus::SuccessValue(_) => {
                            break jsonify(final_tx);
                        }
                    }
                }
                let _ = delay_for(self.polling_config.polling_interval).await;
            }
        })
        .await
        .map_err(|_| timeout_err())?
    }

    async fn send_tx_commit(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let tx = parse_tx(params)?;
        let tx_hash = tx.get_hash();
        let signer_account_id = tx.transaction.signer_id.clone();
        let result = self
            .client_addr
            .send(NetworkClientMessages::Transaction(tx))
            .map_err(|err| RpcError::server_error(Some(convert_mailbox_error(err))))
            .await?;
        match result {
            NetworkClientResponses::ValidTx | NetworkClientResponses::RequestRouted => {
                self.tx_polling(tx_hash, signer_account_id).await
            }
            NetworkClientResponses::InvalidTx(err) => {
                Err(RpcError::server_error(Some(ExecutionErrorView::from(err))))
            }
            NetworkClientResponses::NoResponse => {
                Err(RpcError::server_error(Some(ExecutionErrorView {
                    error_message: "send_tx_commit has timed out".to_string(),
                    error_type: "TimeoutError".to_string(),
                })))
            }
            _ => unreachable!(),
        }
    }

    async fn health(&self) -> Result<Value, RpcError> {
        match self.client_addr.send(Status { is_health_check: true }).await {
            Ok(Ok(_)) => Ok(Value::Null),
            Ok(Err(err)) => Err(RpcError::new(-32_001, err, None)),
            Err(_) => Err(RpcError::server_error::<String>(None)),
        }
    }

    pub async fn status(&self) -> Result<Value, RpcError> {
        match self.client_addr.send(Status { is_health_check: false }).await {
            Ok(Ok(result)) => jsonify(Ok(Ok(result))),
            Ok(Err(err)) => Err(RpcError::new(-32_001, err, None)),
            Err(_) => Err(RpcError::server_error::<String>(None)),
        }
    }

    async fn query(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (path, data) = parse_params::<(String, String)>(params)?;
        let data = from_base_or_parse_err(data)?;
        let query_data_size = path.len() + data.len();
        if query_data_size > QUERY_DATA_MAX_SIZE {
            return Err(RpcError::server_error(Some(format!(
                "Query data size {} is too large",
                query_data_size
            ))));
        }
        let query = Query::new(path, data);
        timeout(self.polling_config.polling_timeout, async {
            loop {
                let result = self.view_client_addr.send(query.clone()).await;
                match result {
                    Ok(ref r) => match r {
                        Ok(Some(_)) => break jsonify(result),
                        Ok(None) => {}
                        Err(e) => break Err(RpcError::server_error(Some(e))),
                    },
                    Err(e) => break Err(RpcError::server_error(Some(e.to_string()))),
                }
                let _ = delay_for(self.polling_config.polling_interval).await;
            }
        })
        .await
        .map_err(|_| RpcError::server_error(Some("query has timed out".to_string())))?
    }

    async fn tx_status(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (hash, account_id) = parse_params::<(String, String)>(params)?;
        let tx_hash = from_base_or_parse_err(hash).and_then(|bytes| {
            CryptoHash::try_from(bytes).map_err(|err| RpcError::parse_error(err.to_string()))
        })?;

        self.tx_polling(tx_hash, account_id).await
    }

    async fn block(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (block_id,) = parse_params::<(BlockId,)>(params)?;
        jsonify(
            self.view_client_addr
                .send(match block_id {
                    BlockId::Height(height) => GetBlock::Height(height),
                    BlockId::Hash(hash) => GetBlock::Hash(hash.into()),
                })
                .await,
        )
    }

    async fn chunk(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (chunk_id,) = parse_params::<(ChunkId,)>(params)?;
        jsonify(
            self.view_client_addr
                .send(match chunk_id {
                    ChunkId::BlockShardId(block_id, shard_id) => match block_id {
                        BlockId::Height(height) => GetChunk::Height(height, shard_id),
                        BlockId::Hash(block_hash) => {
                            GetChunk::BlockHash(block_hash.into(), shard_id)
                        }
                    },
                    ChunkId::Hash(chunk_hash) => GetChunk::ChunkHash(chunk_hash.into()),
                })
                .await,
        )
    }

    async fn changes(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (block_hash, account_id, key_prefix) =
            parse_params::<(CryptoHash, AccountId, Vec<u8>)>(params)?;
        let block_hash_copy = block_hash.clone();
        let account_id_copy = account_id.clone();
        let key_prefix_copy = key_prefix.clone();
        jsonify(
            self.view_client_addr
                .send(GetKeyValueChanges { block_hash, account_id, key_prefix })
                .await
                .map(|v| {
                    v.map(|changes| {
                        json!({
                            "block_hash": block_hash_copy,
                            "account_id": account_id_copy,
                            "key_prefix": key_prefix_copy,
                            "changes_by_key": changes
                            .into_iter()
                            .map(|(key, changes)| {
                                json!({
                                    "key": key,
                                    "changes": changes.into_iter().map(|(cause, value)| {
                                        json!({
                                            "cause": cause,
                                            "value": value
                                        })
                                    }).collect::<Vec<_>>()
                                })
                            })
                            .collect::<Vec<_>>()
                        })
                    })
                }),
        )
    }

    async fn next_light_client_block(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (last_block_hash,) = parse_params::<(CryptoHash,)>(params)?;
        jsonify(self.view_client_addr.send(GetNextLightClientBlock { last_block_hash }).await)
    }

    async fn network_info(&self) -> Result<Value, RpcError> {
        jsonify(self.client_addr.send(GetNetworkInfo {}).await)
    }

    async fn gas_price(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (block_id,) = parse_params::<(Option<BlockId>,)>(params)?;
        let gas_price_request = match block_id {
            None => GetGasPrice::None,
            Some(BlockId::Height(height)) => GetGasPrice::Height(height),
            Some(BlockId::Hash(hash)) => GetGasPrice::Hash(hash),
        };
        jsonify(self.view_client_addr.send(gas_price_request).await)
    }

    pub async fn metrics(&self) -> Result<String, FromUtf8Error> {
        // Gather metrics and return them as a String
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        encoder.encode(&prometheus::gather(), &mut buffer).unwrap();

        String::from_utf8(buffer)
    }

    async fn validators(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let block_hash = parse_hash(params)?;
        jsonify(self.view_client_addr.send(GetValidatorInfo { last_block_hash: block_hash }).await)
    }
}

fn rpc_handler(
    message: web::Json<Message>,
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Output = Result<HttpResponse, HttpError>> {
    near_metrics::inc_counter(&metrics::HTTP_RPC_REQUEST_COUNT);

    let response = async move {
        let message = handler.process(message.0).await?;
        Ok(HttpResponse::Ok().json(message))
    };
    response.boxed()
}

fn status_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Output = Result<HttpResponse, HttpError>> {
    near_metrics::inc_counter(&metrics::HTTP_STATUS_REQUEST_COUNT);

    let response = async move {
        match handler.status().await {
            Ok(value) => Ok(HttpResponse::Ok().json(value)),
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
            Ok(value) => Ok(HttpResponse::Ok().json(value)),
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

fn get_cors(cors_allowed_origins: &[String]) -> CorsFactory {
    let mut cors = Cors::new();
    if cors_allowed_origins != ["*".to_string()] {
        for origin in cors_allowed_origins {
            cors = cors.allowed_origin(&origin);
        }
    }
    cors.allowed_methods(vec!["GET", "POST"])
        .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
        .allowed_header(http::header::CONTENT_TYPE)
        .max_age(3600)
        .finish()
}

pub fn start_http(
    config: RpcConfig,
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
) {
    let RpcConfig { addr, polling_config, cors_allowed_origins } = config;
    HttpServer::new(move || {
        App::new()
            .wrap(get_cors(&cors_allowed_origins))
            .data(JsonRpcHandler {
                client_addr: client_addr.clone(),
                view_client_addr: view_client_addr.clone(),
                polling_config,
            })
            .app_data(web::JsonConfig::default().limit(JSON_PAYLOAD_MAX_SIZE))
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::post().to(rpc_handler)))
            .service(
                web::resource("/status")
                    .route(web::get().to(status_handler))
                    .route(web::head().to(status_handler)),
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
