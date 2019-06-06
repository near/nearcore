#![feature(await_macro, async_await)]

use std::convert::TryFrom;
use std::convert::TryInto;
use std::time::Duration;

use actix::{Addr, MailboxError};
use actix_web::{App, Error as HttpError, HttpResponse, HttpServer, middleware, web};
use futures03::{compat::Future01CompatExt as _, FutureExt as _, TryFutureExt as _};
use futures::future::Future;
use protobuf::parse_from_bytes;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use async_utils::{delay, timeout};
use message::Message;
use near_client::{ClientActor, GetBlock, Query, Status, TxDetails, TxStatus, ViewClientActor};
use near_network::{NetworkClientMessages, NetworkClientResponses};
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::{BaseEncode, from_base};
use near_primitives::transaction::{FinalTransactionStatus, SignedTransaction};
use near_primitives::types::BlockIndex;
use near_protos::signed_transaction as transaction_proto;

use crate::message::{Request, RpcError};

pub mod client;
mod message;
pub mod test_utils;

/// Maximum byte size of the json payload.
const JSON_PAYLOAD_MAX_SIZE: usize = 2 * 1024 * 1024;

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct RpcPollingConfig {
    pub polling_interval: Duration,
    pub polling_timeout: Duration,
}

impl Default for RpcPollingConfig {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_millis(100),
            polling_timeout: Duration::from_secs(5),
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
    let bytes = from_base_or_parse_err(encoded)?;
    let tx: transaction_proto::SignedTransaction = parse_from_bytes(&bytes).map_err(|e| {
        RpcError::invalid_params(Some(format!("Failed to decode transaction proto: {}", e)))
    })?;
    Ok(tx.try_into().map_err(|e| {
        RpcError::invalid_params(Some(format!("Failed to decode transaction: {}", e)))
    })?)
}

fn parse_hash(params: Option<Value>) -> Result<CryptoHash, RpcError> {
    let (encoded,) = parse_params::<(String,)>(params)?;
    from_base_or_parse_err(encoded).and_then(|bytes| {
        CryptoHash::try_from(bytes).map_err(|err| RpcError::parse_error(err.to_string()))
    })
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
            "query" => self.query(request.params).await,
            "health" => self.health().await,
            "status" => self.status().await,
            "tx" => self.tx_status(request.params).await,
            "tx_details" => self.tx_details(request.params).await,
            "block" => self.block(request.params).await,
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

    async fn send_tx_commit(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let tx = parse_tx(params)?;
        let tx_hash = tx.get_hash();
        let result = self.client_addr
            .send(NetworkClientMessages::Transaction(tx))
            .map_err(|err| RpcError::server_error(Some(err.to_string())))
            .compat()
            .await?;
        match result {
            NetworkClientResponses::ValidTx => {
                timeout(self.polling_config.polling_timeout, async {
                    loop {
                        let final_tx = self.view_client_addr.send(TxStatus { tx_hash }).compat().await;
                        if let Ok(Ok(ref tx)) = final_tx {
                            match tx.status {
                                FinalTransactionStatus::Started | FinalTransactionStatus::Unknown => {}
                                _ => {
                                    break jsonify(final_tx);
                                }
                            }
                        }
                        let _ = delay(self.polling_config.polling_interval).await;
                    }
                })
                    .await
                    .map_err(|_| RpcError::server_error(Some("send_tx_commit has timed out.".to_owned())))?
            },
            NetworkClientResponses::InvalidTx(err) => {
                Err(RpcError::server_error(Some(err)))
            }
            _ => unreachable!(),
        }
    }

    async fn health(&self) -> Result<Value, RpcError> {
        Ok(Value::Null)
    }

    pub async fn status(&self) -> Result<Value, RpcError> {
        jsonify(self.client_addr.send(Status {}).compat().await)
    }

    async fn query(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (path, data) = parse_params::<(String, String)>(params)?;
        let data = from_base_or_parse_err(data)?;
        jsonify(self.view_client_addr.send(Query { path, data }).compat().await)
    }

    async fn tx_status(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let tx_hash = parse_hash(params)?;
        jsonify(self.view_client_addr.send(TxStatus { tx_hash }).compat().await)
    }

    async fn tx_details(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let tx_hash = parse_hash(params)?;
        jsonify(self.view_client_addr.send(TxDetails { tx_hash }).compat().await)
    }

    async fn block(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let (height,) = parse_params::<(BlockIndex,)>(params)?;
        jsonify(self.view_client_addr.send(GetBlock::Height(height)).compat().await)
    }
}

fn rpc_handler(
    message: web::Json<Message>,
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Item = HttpResponse, Error = HttpError> {
    let response = async move {
        let message = handler.process(message.0).await?;
        Ok(HttpResponse::Ok().json(message))
    };
    response.boxed().compat()
}

fn status_handler(handler: web::Data<JsonRpcHandler>) -> impl Future<Item = HttpResponse, Error = HttpError> {
    let response = async move {
        match handler.status().await {
            Ok(value) => Ok(HttpResponse::Ok().json(value)),
            Err(_) => Ok(HttpResponse::ServiceUnavailable().finish()),
        }
    };
    response.boxed().compat()
}

pub fn start_http(
    config: RpcConfig,
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
) {
    let RpcConfig { addr, polling_config, .. } = config;
    HttpServer::new(move || {
        App::new()
            .data(JsonRpcHandler {
                client_addr: client_addr.clone(),
                view_client_addr: view_client_addr.clone(),
                polling_config,
            })
            .data(web::JsonConfig::default().limit(JSON_PAYLOAD_MAX_SIZE))
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::post().to_async(rpc_handler)))
            .service(web::resource("/status").route(web::get().to_async(status_handler)))
    })
    .bind(addr)
    .unwrap()
    .workers(4)
    .shutdown_timeout(5)
    .start();
}
