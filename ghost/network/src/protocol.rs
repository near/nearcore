use actix::{
    Actor, ActorContext, ActorFuture, Context, ContextFutureSpawner, Handler, StreamHandler,
    Supervised, System, SystemService, WrapFuture,
};

use crate::types::ProtocolMessage;

impl actix::Message for ProtocolMessage {
    type Result = ();
}

/// Protocol agent that connects network with the rest of the system.
pub struct Protocol {
    // TODO: network adapter.
}

impl Protocol {
    pub fn new() -> Self {
        Protocol {}
    }
}

impl Actor for Protocol {
    type Context = Context<Self>;
}

impl Handler<ProtocolMessage> for Protocol {
    type Result = ();
    fn handle(&mut self, msg: ProtocolMessage, _ctx: &mut Self::Context) {
        match msg {
            ProtocolMessage::Transaction(t) => {
                // TODO: handle
            }
        }
    }
}

impl Default for Protocol {
    fn default() -> Protocol {
        Protocol {}
    }
}

/// Required traits for being able to retrieve Protocol address from registry.
impl Supervised for Protocol {}

impl SystemService for Protocol {}
