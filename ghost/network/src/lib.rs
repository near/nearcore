use actix::{Actor, Addr, Arbiter, Context, Handler, Message, Recipient, System};

use primitives::hash::CryptoHash;
use types::PeerInfo;

pub mod types;

pub enum NetworkRequests {
    Block { hash: CryptoHash, peer_info: PeerInfo },
}

impl Message for NetworkRequests {
    type Result = ();
}

pub struct NetworkActor {}

impl Actor for NetworkActor {
    type Context = Context<NetworkActor>;
}

impl Handler<NetworkRequests> for NetworkActor {
    type Result = ();

    fn handle(&mut self, msg: NetworkRequests, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            _ => panic!("123"),
        }
    }
}
