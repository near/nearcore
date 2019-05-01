use std::collections::HashMap;
use std::sync::Arc;

use jsonrpc_core::{futures::Future, MetaIoHandler, Params, Value};
use jsonrpc_pubsub::{PubSubHandler, Session, SubscriptionId};

use actix::Addr;
use near_client::ClientActor;
use primitives::transaction::SignedTransaction;

pub type Subscriptions = HashMap<SubscriptionId, (jsonrpc_pubsub::Sink, Value)>;
type JsonRpcResult = Result<Value, jsonrpc_core::Error>;
type JsonRpcResultAsync = Box<dyn Future<Item = Value, Error = jsonrpc_core::Error> + Send>;

/// Actual API call handler to underlying systems.
struct RPCAPIHandler {
    client_addr: Addr<ClientActor>,
}

impl RPCAPIHandler {
    /// Sends transaction to the client and returns right away.
    fn broadcast_tx_async(
        &self,
        _tx: Result<SignedTransaction, jsonrpc_core::Error>,
    ) -> JsonRpcResult {
        // TODO: implement
        // client_addr.send();
        Ok(Value::Null)
    }
    /// Sends transaction to the client and returns after tx was validated.
    fn broadcast_tx_sync(
        &self,
        _tx: Result<SignedTransaction, jsonrpc_core::Error>,
    ) -> JsonRpcResult {
        // TODO: implement
        // let result = client_addr.do_send();
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
        self.handler.add_method("broadcast_tx_async", move |params: Params| {
            api.broadcast_tx_async(params.parse()?)
        });
        let api = self.api.clone();
        self.handler.add_method("broadcast_tx_sync", move |params: Params| {
            api.broadcast_tx_async(params.parse()?)
        });
        // TODO: add health, statuc, blocks, etc.
        // TODO: add subscriptions, for example receving new blocks accepted to the chain.
    }
}
