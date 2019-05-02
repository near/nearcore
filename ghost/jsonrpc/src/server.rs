use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use actix::{io::FramedWrite, Actor, Addr, AsyncContext, Context, Handler, StreamHandler};
use futures::stream::Stream;
use futures::sync::mpsc;
use jsonrpc_pubsub::Session;
use log::{debug, error, info};
use tokio::{
    codec::FramedRead,
    io::AsyncRead,
    net::{TcpListener, TcpStream},
};

use near_client::ClientActor;
use near_network::types::InboundTcpConnect;

use crate::codec::LineCodec;
use crate::connection::{JsonRpc, Unregister};
use crate::methods::JsonRpcHandler;

/// JSON Rpc Server.
pub struct JsonRpcServer {
    /// Server address.
    server_addr: SocketAddr,
    /// Opened connections.
    open_connections: HashSet<Addr<JsonRpc>>,
    /// JSON RPC methods handler, shared across all connections.
    jsonrpc_handler: Arc<JsonRpcHandler>,
}

impl JsonRpcServer {
    pub fn new(server_addr: SocketAddr, client_addr: Addr<ClientActor>) -> Self {
        let jsonrpc_handler = Arc::new(JsonRpcHandler::new(client_addr));
        JsonRpcServer { server_addr, open_connections: HashSet::default(), jsonrpc_handler }
    }
    fn add_connection(&mut self, parent: Addr<JsonRpcServer>, stream: TcpStream) {
        let jsonrpc_handler = self.jsonrpc_handler.clone();
        let (sender, receiver) = mpsc::channel(16);

        let addr = JsonRpc::create(|ctx| {
            let (read, write) = stream.split();
            JsonRpc::add_stream(FramedRead::new(read, LineCodec), ctx);
            JsonRpc::add_stream(receiver, ctx);
            JsonRpc {
                framed: FramedWrite::new(write, LineCodec, ctx),
                parent,
                jsonrpc_handler,
                session: Arc::new(Session::new(sender)),
            }
        });

        self.open_connections.insert(addr);
        debug!(target: "jsonrpc", "JSONRPC connection added ({} open connections)", self.open_connections.len() + 1);
    }

    fn remove_connection(&mut self, addr: &Addr<JsonRpc>) {
        self.open_connections.remove(addr);
    }
}

impl Actor for JsonRpcServer {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let listener = match TcpListener::bind(&self.server_addr) {
            Ok(listener) => listener,
            Err(err) => {
                error!(target: "jsonrpc", "Could not start JSON RPC server on {}: {}", self.server_addr, err);
                panic!("Could not start JSON RPC server on {}: {}", self.server_addr, err);
            }
        };

        ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(InboundTcpConnect::new));

        info!(target: "jsonrpc", "JSON RPC started: {}", self.server_addr);
    }
}

impl Handler<InboundTcpConnect> for JsonRpcServer {
    type Result = ();

    fn handle(&mut self, msg: InboundTcpConnect, ctx: &mut Self::Context) {
        self.add_connection(ctx.address(), msg.stream);
    }
}

impl Handler<Unregister> for JsonRpcServer {
    type Result = ();

    fn handle(&mut self, msg: Unregister, _ctx: &mut Self::Context) {
        self.remove_connection(&msg.addr);
    }
}

// TODO: handle messages from Client like NewBlock to send to subscribers.
