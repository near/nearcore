use std::convert::{TryFrom, TryInto};
use std::io;
use std::net::SocketAddr;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;

use actix::actors::resolver::{ConnectAddr, Resolver};
use actix::io::{FramedWrite, WriteHandler};
use actix::prelude::Stream;
use actix::{
    Actor, ActorContext, ActorFuture, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner,
    Handler, Message, Recipient, Running, StreamHandler, System, SystemService, WrapFuture,
};
use log::{debug, error, info, warn};
use protobuf::{parse_from_bytes, Message as ProtoMessage, ProtobufResult};
use tokio::codec::FramedRead;
use tokio::io::AsyncRead;
use tokio::io::WriteHalf;
use tokio::net::{TcpListener, TcpStream};

use near_protos::network as network_proto;
use primitives::transaction::SignedTransaction;
use primitives::utils::DisplayOption;

use crate::codec::Codec;
use crate::types::{Consolidate, Handshake, NetworkClientMessages, PeerInfo, PeerMessage, PeerStatus, PeerType, SendMessage, Unregister, PROTOCOL_VERSION, PeerChainInfo};
use crate::PeerManagerActor;

pub struct Peer {
    /// This node's id and address (either listening or socket address).
    pub node_info: PeerInfo,
    /// Peer address from connection.
    pub peer_addr: SocketAddr,
    /// Peer id and info. Present if outbound or ready.
    pub peer_info: DisplayOption<PeerInfo>,
    /// Peer type.
    pub peer_type: PeerType,
    /// Peer status.
    pub peer_status: PeerStatus,
    /// Framed wrapper to send messages through the TCP connection.
    framed: FramedWrite<WriteHalf<TcpStream>, Codec>,
    /// Handshake timeout.
    handshake_timeout: Duration,
    /// Peer manager recipient to break the dependency loop.
    peer_manager_addr: Addr<PeerManagerActor>,
    client_addr: Recipient<NetworkClientMessages>,
}

impl Peer {
    pub fn new(
        node_info: PeerInfo,
        peer_addr: SocketAddr,
        peer_info: Option<PeerInfo>,
        peer_type: PeerType,
        framed: FramedWrite<WriteHalf<TcpStream>, Codec>,
        handshake_timeout: Duration,
        peer_manager_addr: Addr<PeerManagerActor>,
        client_addr: Recipient<NetworkClientMessages>,
    ) -> Self {
        Peer {
            node_info,
            peer_addr,
            peer_info: peer_info.into(),
            peer_type,
            peer_status: PeerStatus::Connecting,
            framed,
            handshake_timeout,
            peer_manager_addr,
            client_addr,
        }
    }

    fn send_message(&mut self, msg: PeerMessage) {
        debug!(target: "network", "Sending {:?} message to peer {}", msg, self.peer_info);
        self.framed.write(msg.into());
    }

    fn send_handshake(&mut self) {
        let handshake = Handshake::new(
            self.node_info.id,
            self.node_info.account_id.clone(),
            self.node_info.addr_port(),
            PeerChainInfo { height: 0, total_weight: 0.into() }
        );
        self.send_message(PeerMessage::Handshake(handshake));
    }

    fn receive_message(&mut self, ctx: &mut Context<Peer>, msg: PeerMessage) {
        debug!(target: "network", "Received {:?} message from {}", msg, self.peer_info);
        let peer_info = match self.peer_info.as_ref() {
            Some(peer_info) => peer_info.clone(),
            None => {
                return;
            }
        };
        // TODO: we are waiting here until we processed, should we?
        let result = match msg {
            PeerMessage::BlockAnnounce(block) => {
                self.client_addr.do_send(NetworkClientMessages::Block(block, peer_info, false))
            }
            PeerMessage::BlockHeaderAnnounce(header) => {
                self.client_addr.do_send(NetworkClientMessages::BlockHeader(header, peer_info))
            }
            PeerMessage::Transaction(transaction) => {
                self.client_addr.do_send(NetworkClientMessages::Transaction(transaction))
            }
            _ => unreachable!(),
        };
        // TODO: deal with the result -> ban peer or whatever.
    }
}

impl Actor for Peer {
    type Context = Context<Peer>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!(target: "network", "Peer {:?} {:?} started", self.peer_addr, self.peer_type);
        // Set Handshake timeout for stopping actor if peer is not ready after given period of time.
        ctx.run_later(self.handshake_timeout, move |act, ctx| {
            if act.peer_status != PeerStatus::Ready {
                info!(target: "network", "Handshake timeout expired for {}", act.peer_info);
                ctx.stop();
            }
        });

        // If outbound peer, initiate handshake.
        if self.peer_type == PeerType::Outbound {
            self.send_handshake();
        }
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        debug!(target: "network", "Peer {} disconnected.", self.peer_info);
        if let Some(peer_info) = self.peer_info.as_ref() {
            if self.peer_status == PeerStatus::Ready {
                self.peer_manager_addr.do_send(Unregister { peer_id: peer_info.id })
            }
        }
        Running::Stop
    }
}

impl WriteHandler<io::Error> for Peer {}

impl StreamHandler<PeerMessage, io::Error> for Peer {
    fn handle(&mut self, msg: PeerMessage, ctx: &mut Self::Context) {
        match (self.peer_type, self.peer_status, msg) {
            (_, PeerStatus::Connecting, PeerMessage::Handshake(handshake)) => {
                debug!(target: "network", "{} received handshake {:?}", self.node_info.id, handshake);
                if handshake.peer_id == self.node_info.id {
                    warn!(target: "network", "Received info about itself. Disconnecting this peer.");
                    ctx.stop();
                }
                let peer_info = PeerInfo {
                    id: handshake.peer_id,
                    addr: Some(self.peer_addr),
                    account_id: handshake.account_id.clone(),
                };
                self.peer_manager_addr
                    .send(Consolidate {
                        actor: ctx.address().recipient(),
                        peer_info: peer_info.clone(),
                        peer_type: self.peer_type,
                        chain_info: handshake.chain_info,
                    })
                    .into_actor(self)
                    .then(move |res, act, ctx| {
                        match res {
                            Ok(true) => {
                                debug!(target: "network", "Peer {:?} successfully consolidated", act.peer_addr);
                                act.peer_info = Some(peer_info).into();
                                act.peer_status = PeerStatus::Ready;
                                // Respond to handshake if it's inbound and connection was consolidated.
                                if act.peer_type == PeerType::Inbound {
                                    act.send_handshake();
                                }
                                actix::fut::ok(())
                            },
                            _ => {
                                info!(target: "network", "Peer with handshake {:?} wasn't consolidated, disconnecting.", handshake);
                                ctx.stop();
                                actix::fut::err(())
                            }
                        }
                    })
                    .wait(ctx);
            }
            (_, PeerStatus::Ready, PeerMessage::InfoGossip(_)) => {
                // TODO: implement gossip of peers.
            }
            (_, PeerStatus::Ready, msg) => {
                self.receive_message(ctx, msg);
            }
            (_, _, msg) => {
                warn!(target: "network", "Received {} while {:?} from {:?} connection.", msg, self.peer_status, self.peer_type);
            }
        }
    }
}

impl Handler<SendMessage> for Peer {
    type Result = ();

    fn handle(&mut self, msg: SendMessage, _: &mut Self::Context) {
        self.send_message(msg.message);
    }
}
