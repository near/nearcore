use std::convert::TryInto;

use actix::Addr;
use actix_web::{web, App, HttpResponse, HttpServer};
use futures::future::Future;
use log::{error, info};
use protobuf::parse_from_bytes;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use message::Message;
use near_client::ClientActor;
use near_network::NetworkClientMessages;
use near_protos::signed_transaction as transaction_proto;

use crate::message::{Request, RpcError};
use std::net::SocketAddr;

pub mod client;
mod message;

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
    pub fn process(&self, message: Message) -> Message {
        let id = message.id();
        match message {
            Message::Request(request) => Message::response(id, self.process_request(request)),
            _ => Message::error(RpcError::invalid_request()),
        }
    }

    fn process_request(&self, request: Request) -> Result<Value, RpcError> {
        match request.method.as_ref() {
            "broadcast_tx_sync" => self.send_tx_sync(request.params),
            "query" => self.query(request.params),
            _ => Err(RpcError::method_not_found(request.method)),
        }
    }

    fn send_tx_sync(&self, params: Option<Value>) -> Result<Value, RpcError> {
        let params = parse_params::<Vec<Vec<u8>>>(params)?;
        if params.len() != 1 || params[0].len() == 0 {
            return Err(RpcError::invalid_params(Some("Missing tx bytes".to_string())));
        }
        println!("Params: {:?}", params);
        let tx: transaction_proto::SignedTransaction =
            parse_from_bytes(&params[0]).map_err(|e| {
                RpcError::invalid_params(Some(format!("Failed to decode transaction proto: {}", e)))
            })?;
        let tx = tx.try_into().map_err(|e| {
            RpcError::invalid_params(Some(format!("Failed to decode transaction: {}", e)))
        })?;
        actix::spawn(
            self.client_addr
                .send(NetworkClientMessages::Transaction(tx))
                .map(|_| {})
                .map_err(|_| {}),
        );
        Ok(Value::Null)
    }

    fn query(&self, params: Option<Value>) -> Result<Value, RpcError> {
        Ok(Value::Null)
    }
}

fn index(handler: web::Data<JsonRpcHandler>, message: web::Json<Message>) -> HttpResponse {
    HttpResponse::Ok().json(handler.process(message.0))
}

pub fn start_http(server_addr: SocketAddr, client_addr: Addr<ClientActor>) {
    HttpServer::new(move || {
        App::new()
            .data(JsonRpcHandler { client_addr: client_addr.clone() })
            .service(web::resource("/").route(web::post().to(index)))
    })
    .bind(server_addr)
    .unwrap()
    .workers(4)
    .shutdown_timeout(5)
    .start();
}
