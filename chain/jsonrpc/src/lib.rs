extern crate prometheus;

use std::convert::TryFrom;
use std::string::FromUtf8Error;
use std::time::Duration;

use actix::{Addr, MailboxError};
use actix_cors::Cors;
use actix_web::{http, middleware, web, App, Error as HttpError, HttpResponse, HttpServer};
use borsh::BorshDeserialize;
use futures::future::Future;
use futures03::{compat::Future01CompatExt as _, FutureExt as _, TryFutureExt as _};
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use async_utils::{delay, timeout};
use message::Message;
use message::{Request, RpcError};
use near_client::{
    ClientActor, GetBlock, GetChunk, GetNetworkInfo, GetValidatorInfo, Status, TxStatus,
    ViewClientActor,
};
pub use near_jsonrpc_client as client;
use near_jsonrpc_client::{message, BlockId, ChunkId};
use near_metrics::{Encoder, TextEncoder};
use near_network::{NetworkClientMessages, NetworkClientResponses};
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::{from_base, from_base64, BaseEncode};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::utils::generate_random_string;
use near_primitives::views::{ExecutionErrorView, FinalExecutionStatus};

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

fn jsonify_client_response(
    client_response: Result<NetworkClientResponses, MailboxError>,
) -> Result<Value, RpcError> {
    match client_response {
        Ok(NetworkClientResponses::TxStatus(tx_result)) => serde_json::to_value(tx_result)
            .map_err(|err| RpcError::server_error(Some(err.to_string()))),
        Ok(NetworkClientResponses::QueryResponse { response, .. }) => {
            serde_json::to_value(response)
                .map_err(|err| RpcError::server_error(Some(err.to_string())))
        }
        Ok(response) => Err(RpcError::server_error(Some(ExecutionErrorView {
            error_message: format!("Wrong client response: {:?}", response),
            error_type: "ResponseError".to_string(),
        }))),
        Err(e) => Err(RpcError::server_error(Some(convert_mailbox_error(e)))),
    }
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
            "network_info" => self.network_info().await,
            _ => Err(RpcError::method_not_found(request.method)),
        }
    }

    async fn send_tx_async(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let tx = parse_tx(params)?;
        let hash = (&tx.get_hash()).to_base();
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Transaction(tx))
                .map(|_| ())
                .map_err(|_| ()),
        );
        Ok(Value::String(hash))
    }

    async fn tx_polling(
        &self,
        result: NetworkClientResponses,
        tx_hash: CryptoHash,
        account_id: AccountId,
    ) -> Result<Value, RpcError> {
        match result {
            NetworkClientResponses::ValidTx | NetworkClientResponses::RequestRouted => {
                let needs_routing = result == NetworkClientResponses::RequestRouted;
                timeout(self.polling_config.polling_timeout, async {
                    loop {
                        if needs_routing {
                            let final_tx = self
                                .client_addr
                                .send(NetworkClientMessages::TxStatus {
                                    tx_hash,
                                    signer_account_id: account_id.clone(),
                                })
                                .compat()
                                .await;
                            if let Ok(NetworkClientResponses::TxStatus(ref tx_result)) = final_tx {
                                match tx_result.status {
                                    FinalExecutionStatus::Started
                                    | FinalExecutionStatus::NotStarted => {}
                                    FinalExecutionStatus::Failure(_)
                                    | FinalExecutionStatus::SuccessValue(_) => {
                                        break jsonify_client_response(final_tx);
                                    }
                                }
                            }
                        } else {
                            let final_tx =
                                self.view_client_addr.send(TxStatus { tx_hash }).compat().await;
                            if let Ok(Ok(ref tx)) = final_tx {
                                match tx.status {
                                    FinalExecutionStatus::Started
                                    | FinalExecutionStatus::NotStarted => {}
                                    FinalExecutionStatus::Failure(_)
                                    | FinalExecutionStatus::SuccessValue(_) => {
                                        break jsonify(final_tx);
                                    }
                                }
                            }
                        }
                        let _ = delay(self.polling_config.polling_interval).await;
                    }
                })
                .await
                .map_err(|_| timeout_err())?
            }
            NetworkClientResponses::TxStatus(tx_result) => {
                serde_json::to_value(tx_result).map_err(|err| {
                    RpcError::server_error(Some(ExecutionErrorView {
                        error_message: err.to_string(),
                        error_type: "SerializationError".to_string(),
                    }))
                })
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

    async fn send_tx_commit(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let tx = parse_tx(params)?;
        let tx_hash = tx.get_hash();
        let signer_account_id = tx.transaction.signer_id.clone();
        let result = self
            .client_addr
            .send(NetworkClientMessages::Transaction(tx))
            .map_err(|err| RpcError::server_error(Some(convert_mailbox_error(err))))
            .compat()
            .await?;
        self.tx_polling(result, tx_hash, signer_account_id).await
    }

    async fn health(&self) -> Result<Value, RpcError> {
        match self.client_addr.send(Status {}).compat().await {
            Ok(Ok(_)) => Ok(Value::Null),
            Ok(Err(err)) => Err(RpcError::new(-32_001, err, None)),
            Err(_) => Err(RpcError::server_error::<String>(None)),
        }
    }

    pub async fn status(&self) -> Result<Value, RpcError> {
        match self.client_addr.send(Status {}).compat().await {
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
        if !path.contains('/') {
            return Err(RpcError::server_error(Some(
                "At least one query parameter is required".to_string(),
            )));
        }
        let request_id = generate_random_string(10);
        timeout(self.polling_config.polling_timeout, async {
            loop {
                let result = self
                    .client_addr
                    .send(NetworkClientMessages::Query {
                        path: path.clone(),
                        data: data.clone(),
                        id: request_id.clone(),
                    })
                    .compat()
                    .await;
                match result {
                    Ok(NetworkClientResponses::QueryResponse { .. }) => {
                        break jsonify_client_response(result);
                    }
                    Ok(NetworkClientResponses::RequestRouted)
                    | Ok(NetworkClientResponses::NoResponse) => {}
                    Ok(response) => {
                        break Err(RpcError::server_error(Some(ExecutionErrorView {
                            error_message: format!("Wrong client response: {:?}", response),
                            error_type: "ResponseError".to_string(),
                        })));
                    }
                    Err(e) => break Err(RpcError::server_error(Some(convert_mailbox_error(e)))),
                }
                let _ = delay(self.polling_config.polling_interval).await;
            }
        })
        .await
        .map_err(|_| timeout_err())?
    }

    async fn tx_status(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (hash, account_id) = parse_params::<(String, String)>(params)?;
        let tx_hash = from_base_or_parse_err(hash).and_then(|bytes| {
            CryptoHash::try_from(bytes).map_err(|err| RpcError::parse_error(err.to_string()))
        })?;
        let result = self
            .client_addr
            .send(NetworkClientMessages::TxStatus {
                tx_hash,
                signer_account_id: account_id.clone(),
            })
            .compat()
            .map_err(|err| RpcError::server_error(Some(convert_mailbox_error(err))))
            .await?;
        self.tx_polling(result, tx_hash, account_id).await
    }

    async fn block(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (block_id,) = parse_params::<(BlockId,)>(params)?;
        jsonify(
            self.view_client_addr
                .send(match block_id {
                    BlockId::Height(height) => GetBlock::Height(height),
                    BlockId::Hash(hash) => GetBlock::Hash(hash.into()),
                })
                .compat()
                .await,
        )
    }

    async fn chunk(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (chunk_id,) = parse_params::<(ChunkId,)>(params)?;
        jsonify(
            self.view_client_addr
                .send(match chunk_id {
                    ChunkId::BlockShardId(block_id, shard_id) => match block_id {
                        BlockId::Height(block_height) => {
                            GetChunk::BlockHeight(block_height, shard_id)
                        }
                        BlockId::Hash(block_hash) => {
                            GetChunk::BlockHash(block_hash.into(), shard_id)
                        }
                    },
                    ChunkId::Hash(chunk_hash) => GetChunk::ChunkHash(chunk_hash.into()),
                })
                .compat()
                .await,
        )
    }

    async fn network_info(&self) -> Result<Value, RpcError> {
        jsonify(self.client_addr.send(GetNetworkInfo {}).compat().await)
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
        jsonify(
            self.view_client_addr
                .send(GetValidatorInfo { last_block_hash: block_hash })
                .compat()
                .await,
        )
    }
}

fn rpc_handler(
    message: web::Json<Message>,
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Item = HttpResponse, Error = HttpError> {
    near_metrics::inc_counter(&metrics::HTTP_RPC_REQUEST_COUNT);

    let response = async move {
        let message = handler.process(message.0).await?;
        Ok(HttpResponse::Ok().json(message))
    };
    response.boxed().compat()
}

fn status_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Item = HttpResponse, Error = HttpError> {
    near_metrics::inc_counter(&metrics::HTTP_STATUS_REQUEST_COUNT);

    let response = async move {
        match handler.status().await {
            Ok(value) => Ok(HttpResponse::Ok().json(value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed().compat()
}

fn network_info_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Item = HttpResponse, Error = HttpError> {
    let response = async move {
        match handler.network_info().await {
            Ok(value) => Ok(HttpResponse::Ok().json(value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed().compat()
}

fn prometheus_handler(
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Item = HttpResponse, Error = HttpError> {
    near_metrics::inc_counter(&metrics::PROMETHEUS_REQUEST_COUNT);

    let response = async move {
        match handler.metrics().await {
            Ok(value) => Ok(HttpResponse::Ok().body(value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed().compat()
}

fn get_cors(cors_allowed_origins: &[String]) -> Cors {
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
            .data(web::JsonConfig::default().limit(JSON_PAYLOAD_MAX_SIZE))
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::post().to_async(rpc_handler)))
            .service(
                web::resource("/status")
                    .route(web::get().to_async(status_handler))
                    .route(web::head().to_async(status_handler)),
            )
            .service(
                web::resource("/network_info").route(web::get().to_async(network_info_handler)),
            )
            .service(web::resource("/metrics").route(web::get().to_async(prometheus_handler)))
    })
    .bind(addr)
    .unwrap()
    .workers(4)
    .shutdown_timeout(5)
    .start();
}
