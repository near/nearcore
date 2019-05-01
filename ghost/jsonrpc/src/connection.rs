use std::io;
use std::sync::Arc;

use actix::fut::WrapFuture;
use actix::io::{FramedWrite, WriteHandler};
use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Message, Running,
    StreamHandler,
};
use bytes::BytesMut;
use jsonrpc_pubsub::Session;
use log::{debug, error};
use tokio::{io::WriteHalf, net::TcpStream};

use crate::codec::LineCodec;
use crate::methods::JsonRpcHandler;
use crate::server::JsonRpcServer;

/// Unregister a closed connection from the list of open connections
#[derive(Message)]
pub struct Unregister {
    pub addr: Addr<JsonRpc>,
}

/// Single JSON-RPC connection.
pub struct JsonRpc {
    /// Writing stream.
    pub framed: FramedWrite<WriteHalf<TcpStream>, LineCodec>,
    /// Reference to JsonRpcServer.
    pub parent: Addr<JsonRpcServer>,
    /// IO pub sub handler.
    pub jsonrpc_handler: Arc<JsonRpcHandler>,
    /// IO session.
    pub session: Arc<Session>,
}

impl Actor for JsonRpc {
    type Context = Context<Self>;

    /// On stopping unregister from the server.
    fn stopping(&mut self, ctx: &mut Self::Context) -> Running {
        self.parent.do_send(Unregister { addr: ctx.address() });
        Running::Stop
    }
}

impl WriteHandler<io::Error> for JsonRpc {}

impl StreamHandler<BytesMut, io::Error> for JsonRpc {
    fn handle(&mut self, bytes: BytesMut, ctx: &mut Self::Context) {
        let msg = match String::from_utf8(bytes.to_vec()) {
            Ok(msg) => msg,
            Err(err) => {
                error!(target: "jsonrpc", "Invalid UTF8 in JSON-RPC input: {}", err);
                // Return empty string to fail with parse error later.
                "".to_string()
            }
        };
        debug!(target: "jsonrpc", "Received message: {}", msg);

        let session = self.session.clone();

        self.jsonrpc_handler
            .handler()
            .handle_request(&msg, session)
            .into_actor(self)
            .then(|res, act, _ctx| {
                if let Ok(Some(response)) = res {
                    act.framed.write(BytesMut::from(response));
                }
                actix::fut::ok(())
            })
            .wait(ctx);
    }
}

impl StreamHandler<String, ()> for JsonRpc {
    fn handle(&mut self, item: String, _ctx: &mut Self::Context) {
        self.framed.write(BytesMut::from(item));
    }
}
