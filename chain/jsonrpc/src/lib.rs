use std::convert::TryInto;

use actix::Addr;
use actix_web::{middleware, web, App, Error as HttpError, HttpResponse, HttpServer};
use base64;
use futures::future;
use futures::future::Future;
use protobuf::parse_from_bytes;
use serde::de::DeserializeOwned;
use serde_derive::{Serialize, Deserialize};
use serde_json::Value;

use message::Message;
use near_client::{ClientActor, Query};
use near_network::NetworkClientMessages;
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
        RpcConfig {
            addr: "0.0.0.0:3030".to_string(),
            cors_allowed_origins: vec!["*".to_string()]
        }
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

struct JsonRpcHandler {
    client_addr: Addr<ClientActor>,
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
            "query" => self.query(request.params),
            _ => Box::new(future::err(RpcError::method_not_found(request.method))),
        }
    }

    fn send_tx_async(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let (bs64,) = ok_or_rpc_error!(parse_params::<(String,)>(params));
        let bytes = ok_or_rpc_error!(
            base64::decode(&bs64).map_err(|err| RpcError::parse_error(err.to_string()))
        );
        let tx: transaction_proto::SignedTransaction = ok_or_rpc_error!(parse_from_bytes(&bytes)
            .map_err(|e| {
                RpcError::invalid_params(Some(format!("Failed to decode transaction proto: {}", e)))
            }));
        let tx = ok_or_rpc_error!(tx.try_into().map_err(|e| {
            RpcError::invalid_params(Some(format!("Failed to decode transaction: {}", e)))
        }));
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Transaction(tx))
                .map(|_| {})
                .map_err(|_| {}),
        );
        Box::new(future::ok(Value::Null))
    }

    fn query(&self, params: Option<Value>) -> Box<Future<Item = Value, Error = RpcError>> {
        let (path, data) = ok_or_rpc_error!(parse_params::<(String, Vec<u8>)>(params));
        // TODO: simplify this.
        Box::new(
            self.client_addr
                .send(Query { path, data })
                .then(|response| match response {
                    Ok(response) => response.map_err(|e| RpcError::server_error(Some(e))),
                    _ => Err(RpcError::server_error(Some("Failed to query client"))),
                })
                .then(|result| match result {
                    Ok(result) => match serde_json::to_value(result) {
                        Ok(value) => Ok(value),
                        Err(err) => Err(RpcError::server_error(Some(err.to_string()))),
                    },
                    Err(err) => Err(RpcError::server_error(Some(err))),
                }),
        )
    }
}

fn rpc_handler(
    message: web::Json<Message>,
    handler: web::Data<JsonRpcHandler>,
) -> impl Future<Item = HttpResponse, Error = HttpError> {
    handler.process(message.0).and_then(|message| Ok(HttpResponse::Ok().json(message)))
}

pub fn start_http(config: RpcConfig, client_addr: Addr<ClientActor>) {
    HttpServer::new(move || {
        App::new()
            .data(JsonRpcHandler { client_addr: client_addr.clone() })
            .wrap(middleware::Logger::default())
            .service(web::resource("/").route(web::post().to_async(rpc_handler)))
    })
    .bind(config.addr)
    .unwrap()
    .workers(4)
    .shutdown_timeout(5)
    .start();
}
