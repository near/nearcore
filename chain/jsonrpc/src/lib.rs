use std::convert::TryFrom;
use std::convert::TryInto;

use actix::{Addr, MailboxError};
use actix_web::{middleware, web, App, Error as HttpError, HttpResponse, HttpServer};
use base64;
use futures::future;
use futures::future::Future;
use protobuf::parse_from_bytes;
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use message::Message;
use near_client::{ClientActor, GetBlock, Query, Status, TxStatus, ViewClientActor};
use near_network::NetworkClientMessages;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::BlockIndex;
use near_protos::signed_transaction as transaction_proto;

use crate::message::{Request, RpcError};

pub mod client;
mod message;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcConfig {
    pub addr: String,
    pub cors_allowed_origins: Vec<String>,
}

impl Default for RpcConfig {
    fn default() -> Self {
        RpcConfig { addr: "0.0.0.0:3030".to_string(), cors_allowed_origins: vec!["*".to_string()] }
    }
}

impl RpcConfig {
    pub fn new(addr: &str) -> Self {
        RpcConfig { addr: addr.to_string(), cors_allowed_origins: vec!["*".to_string()] }
    }
}

macro_rules! ok_or_rpc_error(($obj: expr) => (match $obj {
    Ok(value) => value,
    Err(err) => {
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
    let (bs64,) = parse_params::<(String,)>(params)?;
    let bytes = base64::decode(&bs64).map_err(|err| RpcError::parse_error(err.to_string()))?;
    let tx: transaction_proto::SignedTransaction = parse_from_bytes(&bytes)
            .map_err(|e| {
                RpcError::invalid_params(Some(format!("Failed to decode transaction proto: {}", e)))
            })?;
    Ok(tx.try_into().map_err(|e| {
            RpcError::invalid_params(Some(format!("Failed to decode transaction: {}", e)))
        })?)
}

struct JsonRpcHandler {
    client_addr: Addr<ClientActor>,
    view_client_addr: Addr<ViewClientActor>,
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
            "block" => self.block(request.params),
            _ => Box::new(future::err(RpcError::method_not_found(request.method))),
        }
    }

    fn send_tx_async(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let tx = ok_or_rpc_error!(parse_tx(params));
        let hash = (&tx.get_hash()).into();
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
        let hash = tx.get_hash();
        Box::new(self.client_addr.send(NetworkClientMessages::Transaction(tx))
            .map(|result| {
                // TODO: run check on tx status or until timeout
                Value::Null
            })
            .map_err(|err| RpcError::server_error(Some(err.to_string()))))
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
        let (bs64,) = ok_or_rpc_error!(parse_params::<(String,)>(params));
        let tx_hash = ok_or_rpc_error!(base64::decode(&bs64)
            .map_err(|err| RpcError::parse_error(err.to_string()))
            .and_then(|bytes| CryptoHash::try_from(bytes)
                .map_err(|err| RpcError::parse_error(err.to_string()))));
        Box::new(self.view_client_addr.send(TxStatus { tx_hash }).then(jsonify))
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
    HttpServer::new(move || {
        App::new()
            .data(JsonRpcHandler {
                client_addr: client_addr.clone(),
                view_client_addr: view_client_addr.clone(),
            })
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::post().to_async(rpc_handler)))
    })
    .bind(config.addr)
    .unwrap()
    .workers(4)
    .shutdown_timeout(5)
    .start();
}
