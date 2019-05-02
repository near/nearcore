use std::collections::HashMap;
use std::sync::Arc;

use actix::Addr;
use jsonrpc_core::{futures::Future, MetaIoHandler, Params, Value};
use jsonrpc_pubsub::{PubSubHandler, Session, SubscriptionId};

use near_client::ClientActor;
use near_network::NetworkClientMessages;
use primitives::transaction::SignedTransaction;

pub type Subscriptions = HashMap<SubscriptionId, (jsonrpc_pubsub::Sink, Value)>;
type JsonRpcResult = Result<Value, jsonrpc_core::Error>;
type JsonRpcResultAsync = Box<dyn Future<Item = Value, Error = jsonrpc_core::Error> + Send>;

use crate::message::{Message, Response, RpcError};

/// Actual API call handler to underlying systems.
pub struct RPCAPIHandler {
    client_addr: Addr<ClientActor>,
}

impl RPCAPIHandler {
    pub fn new(client_addr: Addr<ClientActor>) -> Self {
        RPCAPIHandler { client_addr }
    }
    pub fn call(&self, request: &Message) -> Message {
        Message::error(RpcError::new(1, "123".to_string(), None))
    }
    /// Sends transaction to the client and returns right away.
    fn broadcast_tx_async(
        &self,
        tx: Result<SignedTransaction, jsonrpc_core::Error>,
    ) -> JsonRpcResult {
        println!("broadcast_tx_async: {:?}", tx);
        self.client_addr.do_send(NetworkClientMessages::Transaction(tx?));
        Ok(Value::Null)
    }

    /// Sends transaction to the client and returns after tx was validated.
    fn broadcast_tx_sync(
        &self,
        tx: Result<SignedTransaction, jsonrpc_core::Error>,
    ) -> JsonRpcResult {
        println!("broadcast_tx_sync: {:?}", tx);
        // TODO: implement
        // let result = client_addr.do_send();
        Ok(Value::Null)
    }

    /// Queries runtime.
    fn query(
        &self,
        query: Result<(String, Vec<u8>, String, bool), jsonrpc_core::Error>,
    ) -> JsonRpcResult {
        println!("query: {:?}", query);
        Ok(Value::Null)
    }
}

/// Wraps JsonRpc to handle requests and subscriptions.
pub struct JsonRpcHandler {
    /// Actual JSON RPC pub sub handler.
    handler: PubSubHandler<Arc<Session>>,
    /// List of subscriptions.
    subscriptions: Subscriptions,
    /// Underlying api calls to other parts of the system.
    api: Arc<RPCAPIHandler>,
}

impl JsonRpcHandler {
    pub fn new(client_addr: Addr<ClientActor>) -> Self {
        let mut jsonrpc_handler = JsonRpcHandler {
            handler: PubSubHandler::new(MetaIoHandler::default()),
            subscriptions: HashMap::default(),
            api: Arc::new(RPCAPIHandler { client_addr }),
        };
        jsonrpc_handler.setup();
        jsonrpc_handler
    }

    pub fn handler(&self) -> &PubSubHandler<Arc<Session>> {
        &self.handler
    }

    // XXX: What is the correct return type here? Replace handler() function with this.
    //    pub fn handle_request(
    //        &self,
    //        request: &str,
    //        session: Arc<Session>,
    //    ) -> HandleRequestResult {
    //        self.handler.handle_request(request, session)
    //    }

    fn setup(&mut self) {
        let api = self.api.clone();
        self.handler.add_method("query", move |params: Params| api.query(params.parse()?));
        let api = self.api.clone();
        self.handler.add_method("broadcast_tx_async", move |params: Params| {
            api.broadcast_tx_async(params.parse()?)
        });
        let api = self.api.clone();
        self.handler.add_method("broadcast_tx_sync", move |params: Params| {
            api.broadcast_tx_sync(params.parse()?)
        });
        // TODO: add health, statuc, blocks, etc.
        // TODO: add subscriptions, for example receving new blocks accepted to the chain.
    }
}
