use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use actix::io::{FramedWrite, WriteHandler};
use actix::{
    Actor, ActorContext, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler,
    Recipient, Running, StreamHandler, WrapFuture,
};
use log::{debug, error, info, warn};
use tokio::io::WriteHalf;
use tokio::net::TcpStream;

use near_primitives::utils::DisplayOption;

use crate::codec::Codec;
use crate::types::{
    Ban, Consolidate, Handshake, NetworkClientMessages, PeerChainInfo, PeerInfo, PeerMessage,
    PeerStatus, PeerType, PeersRequest, PeersResponse, SendMessage, Unregister,
};
use crate::{NetworkClientResponses, PeerManagerActor};

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

    fn send_handshake(&mut self, ctx: &mut Context<Peer>) {
        self.client_addr
            .send(NetworkClientMessages::GetChainInfo)
            .into_actor(self)
            .then(move |res, act, _ctx| match res {
                Ok(NetworkClientResponses::ChainInfo { height, total_weight }) => {
                    let handshake = Handshake::new(
                        act.node_info.id,
                        act.node_info.account_id.clone(),
                        act.node_info.addr_port(),
                        PeerChainInfo { height, total_weight },
                    );
                    act.send_message(PeerMessage::Handshake(handshake));
                    actix::fut::ok(())
                }
                Err(err) => {
                    error!(target: "network", "Failed sending GetChain to client: {}", err);
                    actix::fut::err(())
                }
                _ => actix::fut::err(()),
            })
            .spawn(ctx);
    }

    /// Process non handshake/peer related messages.
    fn receive_client_message(&mut self, ctx: &mut Context<Peer>, msg: PeerMessage) {
        debug!(target: "network", "Received {:?} message from {}", msg, self.peer_info);
        let peer_id = match self.peer_info.as_ref() {
            Some(peer_info) => peer_info.id.clone(),
            None => {
                return;
            }
        };

        // Wrap peer message into what client expects.
        let network_client_msg = match msg {
            PeerMessage::Block(block) => {
                // TODO: add tracking of requests here.
                NetworkClientMessages::Block(block, peer_id, false)
            }
            PeerMessage::BlockHeaderAnnounce(header) => {
                NetworkClientMessages::BlockHeader(header, peer_id)
            }
            PeerMessage::Transaction(transaction) => {
                NetworkClientMessages::Transaction(transaction)
            }
            PeerMessage::BlockApproval(account_id, hash, signature) => {
                NetworkClientMessages::BlockApproval(account_id, hash, signature)
            }
            PeerMessage::BlockRequest(hash) => NetworkClientMessages::BlockRequest(hash),
            PeerMessage::BlockHeadersRequest(hashes) => {
                NetworkClientMessages::BlockHeadersRequest(hashes)
            }
            PeerMessage::BlockHeaders(headers) => {
                NetworkClientMessages::BlockHeaders(headers, peer_id)
            }
            _ => unreachable!(),
        };
        self.client_addr
            .send(network_client_msg)
            .into_actor(self)
            .then(|res, act, ctx| {
                // Ban peer if client thinks received data is bad.
                match res {
                    Ok(NetworkClientResponses::InvalidTx(err)) => {
                        warn!(target: "network", "Received invalid tx from peer {}: {}", act.peer_info, err);
                        // TODO: count as malicious behaviour?
                    }
                    Ok(NetworkClientResponses::Ban { ban_reason }) => {
                        act.peer_status = PeerStatus::Banned(ban_reason);
                        ctx.stop();
                    }
                    Ok(NetworkClientResponses::Block(block)) => {
                        act.send_message(PeerMessage::Block(block))
                    }
                    Ok(NetworkClientResponses::BlockHeaders(headers)) => {
                        act.send_message(PeerMessage::BlockHeaders(headers))
                    }
                    Err(err) => {
                        error!(
                            target: "network",
                            "Received error sending message to client: {} for {}",
                            err, act.peer_info
                        );
                        return actix::fut::err(());
                    }
                    _ => {}
                };
                actix::fut::ok(())
            })
            .spawn(ctx);
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
            self.send_handshake(ctx);
        }
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        debug!(target: "network", "Peer {} disconnected.", self.peer_info);
        if let Some(peer_info) = self.peer_info.as_ref() {
            if self.peer_status == PeerStatus::Ready {
                self.peer_manager_addr.do_send(Unregister { peer_id: peer_info.id })
            } else if let PeerStatus::Banned(ban_reason) = self.peer_status {
                self.peer_manager_addr.do_send(Ban { peer_id: peer_info.id, ban_reason });
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
                debug!(target: "network", "{:?} received handshake {:?}", self.node_info.id, handshake);
                if handshake.peer_id == self.node_info.id {
                    warn!(target: "network", "Received info about itself. Disconnecting this peer.");
                    ctx.stop();
                }
                let peer_info = PeerInfo {
                    id: handshake.peer_id,
                    addr: handshake
                        .listen_port
                        .map(|port| SocketAddr::new(self.peer_addr.ip(), port)),
                    account_id: handshake.account_id.clone(),
                };
                self.peer_manager_addr
                    .send(Consolidate {
                        actor: ctx.address(),
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
                                    act.send_handshake(ctx);
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
            (_, PeerStatus::Ready, PeerMessage::PeersRequest) => {
                self.peer_manager_addr.send(PeersRequest {}).into_actor(self).then(|res, act, _ctx| {
                    if let Ok(peers) = res {
                        debug!(target: "network", "Peers request from {}: sending {} peers.", act.peer_info, peers.peers.len());
                        act.send_message(PeerMessage::PeersResponse(peers.peers));
                    }
                    actix::fut::ok(())
                }).spawn(ctx);
            }
            (_, PeerStatus::Ready, PeerMessage::PeersResponse(peers)) => {
                debug!(target: "network", "Received peers from {}: {} peers.", self.peer_info, peers.len());
                self.peer_manager_addr.do_send(PeersResponse { peers });
            }
            (_, PeerStatus::Ready, msg) => {
                self.receive_client_message(ctx, msg);
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
