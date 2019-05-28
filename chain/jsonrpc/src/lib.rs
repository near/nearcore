use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use actix::{Addr, MailboxError};
use actix_web::{middleware, web, App, Error as HttpError, HttpResponse, HttpServer};
use futures::future;
use futures::future::Future;
use futures::stream::Stream;
use log::error;
use protobuf::parse_from_bytes;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use tokio::timer::Interval;
use tokio::util::FutureExt;

use message::Message;
use near_client::{ClientActor, GetBlock, Query, Status, TxDetails, TxStatus, ViewClientActor};
use near_network::NetworkClientMessages;
use near_primitives::hash::CryptoHash;
use near_primitives::serialize::{from_base, BaseEncode};
use near_primitives::transaction::{FinalTransactionStatus, SignedTransaction};
use near_primitives::types::BlockIndex;
use near_protos::signed_transaction as transaction_proto;

use crate::message::{Request, RpcError};

pub mod client;
mod message;

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

macro_rules! ok_or_rpc_error(($obj: expr) => (match $obj {
    Ok(value) => value,
    Err(err) => {
        error!(target: "rpc", "RPC error: {:?}", err);
        return Box::new(future::err(err))
    }
}));

fn parse_params<T: DeserializeOwned>(value: Option<Value>) -> Result<T, RpcError> {
    if let Some(value) = value {
        serde_json::from_value(value)
            .map_err(|err| RpcError::invalid_params(Some(format!("Failed parsing args: {}", err))))
    } else {
        Err(RpcError::invalid_params(Some("Require at least one parameter".to_string())))
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
    let bytes = from_base(&encoded).map_err(|err| RpcError::parse_error(err.to_string()))?;
    let tx: transaction_proto::SignedTransaction = parse_from_bytes(&bytes).map_err(|e| {
        RpcError::invalid_params(Some(format!("Failed to decode transaction proto: {}", e)))
    })?;
    Ok(tx.try_into().map_err(|e| {
        RpcError::invalid_params(Some(format!("Failed to decode transaction: {}", e)))
    })?)
}

fn parse_hash(params: Option<Value>) -> Result<CryptoHash, RpcError> {
    let (encoded,) = parse_params::<(String,)>(params)?;
    from_base(&encoded).map_err(|err| RpcError::parse_error(err.to_string())).and_then(|bytes| {
        CryptoHash::try_from(bytes).map_err(|err| RpcError::parse_error(err.to_string()))
    })
}

struct JsonRpcHandler {
    client_addr: Addr<ClientActor>,
    view_client_addr: Arc<Addr<ViewClientActor>>,
    polling_config: RpcPollingConfig,
}

impl JsonRpcHandler {
    pub fn process(&self, message: Message) -> Box<Future<Item = Message, Error = HttpError>> {
        let id = message.id();
        match message {
            Message::Request(request) => Box::new(
                self.process_request(request).then(|result| Ok(Message::response(id, result))),
            ),
            _ => Box::new(future::ok::<_, HttpError>(Message::error(RpcError::invalid_request()))),
        }
    }

    fn process_request(&self, request: Request) -> Box<Future<Item = Value, Error = RpcError>> {
        match request.method.as_ref() {
            "broadcast_tx_async" => self.send_tx_async(request.params),
            "broadcast_tx_commit" => self.send_tx_commit(request.params),
            "query" => self.query(request.params),
            "health" => self.health(),
            "status" => self.status(),
            "tx" => self.tx_status(request.params),
            "tx_details" => self.tx_details(request.params),
            "block" => self.block(request.params),
            _ => Box::new(future::err(RpcError::method_not_found(request.method))),
        }
    }

    fn send_tx_async(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let tx = ok_or_rpc_error!(parse_tx(params));
        let hash = (&tx.get_hash()).to_base();
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Transaction(tx))
                .map(|_| ())
                .map_err(|_| ()),
        );
        Box::new(future::ok(Value::String(hash)))
    }

    fn send_tx_commit(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let tx = ok_or_rpc_error!(parse_tx(params));
        let tx_hash = tx.get_hash();
        let view_client_addr = self.view_client_addr.clone();
        let RpcPollingConfig { polling_interval, polling_timeout, .. } = self.polling_config;
        Box::new(
            self.client_addr
                .send(NetworkClientMessages::Transaction(tx))
                .map_err(|err| RpcError::server_error(Some(err.to_string())))
                .and_then(move |_result| {
                    Interval::new_interval(polling_interval)
                        .map(move |_| view_client_addr.send(TxStatus { tx_hash }).wait())
                        .filter(|tx_status| match tx_status {
                            Ok(Ok(tx_status)) => match tx_status.status {
                                FinalTransactionStatus::Started
                                | FinalTransactionStatus::Unknown => false,
                                _ => true,
                            },
                            _ => false,
                        })
                        .map_err(|err| RpcError::server_error(Some(err.to_string())))
                        .take(1)
                        .fold(Value::Null, |_, tx_status| jsonify(tx_status))
                })
                .timeout(polling_timeout)
                .map_err(|_| {
                    RpcError::server_error(Some("send_tx_commit has timed out.".to_owned()))
                }),
        )
    }

    fn health(&self) -> Box<Future<Item = Value, Error = RpcError>> {
        Box::new(future::ok(Value::Null))
    }

    fn status(&self) -> Box<Future<Item = Value, Error = RpcError>> {
        Box::new(self.client_addr.send(Status {}).then(jsonify))
    }

    fn query(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let (path, data) = ok_or_rpc_error!(parse_params::<(String, Vec<u8>)>(params));
        Box::new(self.view_client_addr.send(Query { path, data }).then(jsonify))
    }

    fn tx_status(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let tx_hash = ok_or_rpc_error!(parse_hash(params));
        Box::new(self.view_client_addr.send(TxStatus { tx_hash }).then(jsonify))
    }

    fn tx_details(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let tx_hash = ok_or_rpc_error!(parse_hash(params));
        Box::new(self.view_client_addr.send(TxDetails { tx_hash }).then(jsonify))
    }

    fn block(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let (height,) = ok_or_rpc_error!(parse_params::<(BlockIndex,)>(params));
        Box::new(self.view_client_addr.send(GetBlock::Height(height)).then(jsonify))
    }
}

fn rpc_handler(
    message: web::Json<Message>,
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Item = HttpResponse, Error = HttpError> {
    handler.process(message.0).and_then(|message| Ok(HttpResponse::Ok().json(message)))
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
                view_client_addr: Arc::new(view_client_addr.clone()),
                polling_config,
            })
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::post().to_async(rpc_handler)))
    })
    .bind(addr)
    .unwrap()
    .workers(4)
    .shutdown_timeout(5)
    .start();
}
