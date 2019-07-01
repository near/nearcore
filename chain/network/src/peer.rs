use std::cmp::max;
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

use near_primitives::hash::CryptoHash;
use near_primitives::utils::DisplayOption;

use crate::codec::{bytes_to_peer_message, peer_message_to_bytes, Codec};
use crate::rate_counter::RateCounter;
use crate::types::{
    Ban, Consolidate, Handshake, NetworkClientMessages, PeerChainInfo, PeerInfo, PeerMessage,
    PeerStatsResult, PeerStatus, PeerType, PeersRequest, PeersResponse, QueryPeerStats,
    SendMessage, Unregister,
};
use crate::{NetworkClientResponses, PeerManagerActor};

/// Maximum number of requests and responses to track.
const MAX_TRACK_SIZE: usize = 30;

/// Maximum number of messages per minute from single peer.
// TODO: current limit is way to high due to us sending lots of messages during sync.
const MAX_PEER_MSG_PER_MIN: u64 = 50000;

/// Keeps track of requests and received hashes of transactions and blocks.
/// Also keeps track of number of bytes sent and received from this peer to prevent abuse.
pub struct Tracker {
    /// Bytes we've sent.
    sent_bytes: RateCounter,
    /// Bytes we've received.
    received_bytes: RateCounter,
    /// Sent requests.
    requested: Vec<CryptoHash>,
    /// Received elements.
    received: Vec<CryptoHash>,
}

impl Default for Tracker {
    fn default() -> Self {
        Tracker {
            sent_bytes: RateCounter::new(),
            received_bytes: RateCounter::new(),
            requested: Default::default(),
            received: Default::default(),
        }
    }
}

impl Tracker {
    fn increment_received(&mut self, size: u64) {
        self.received_bytes.increment(size);
    }

    fn increment_sent(&mut self, size: u64) {
        self.sent_bytes.increment(size);
    }

    fn has_received(&self, hash: CryptoHash) -> bool {
        self.received.contains(&hash)
    }

    fn push_received(&mut self, hash: CryptoHash) {
        if self.received.len() > MAX_TRACK_SIZE {
            self.received.truncate(MAX_TRACK_SIZE)
        }
        if !self.received.contains(&hash) {
            self.received.insert(0, hash);
        }
    }

    fn has_request(&self, hash: CryptoHash) -> bool {
        self.requested.contains(&hash)
    }

    fn push_request(&mut self, hash: CryptoHash) {
        if self.requested.len() > MAX_TRACK_SIZE {
            self.requested.truncate(MAX_TRACK_SIZE);
        }
        if !self.requested.contains(&hash) {
            self.requested.insert(0, hash);
        }
    }
}

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
    /// Addr for client to send messages related to the chain.
    client_addr: Recipient<NetworkClientMessages>,
    /// Tracker for requests and responses.
    tracker: Tracker,
    /// This node genesis hash.
    genesis: CryptoHash,
    /// Latest chain info from the peer.
    chain_info: PeerChainInfo,
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
            tracker: Default::default(),
            genesis: Default::default(),
            chain_info: Default::default(),
        }
    }

    /// Whether the peer is considered abusive due to sending too many messages.
    fn is_abusive(&self) -> bool {
        self.tracker.received_bytes.count_per_min() > MAX_PEER_MSG_PER_MIN
            || self.tracker.sent_bytes.count_per_min() > MAX_PEER_MSG_PER_MIN
    }

    fn send_message(&mut self, msg: PeerMessage) {
        // Skip sending block and headers if we received it or header from this peer.
        // Record block requests in tracker.
        match &msg {
            PeerMessage::Block(b) if self.tracker.has_received(b.hash()) => return,
            PeerMessage::BlockHeaderAnnounce(h) if self.tracker.has_received(h.hash()) => return,
            PeerMessage::BlockRequest(h) => self.tracker.push_request(*h),
            _ => (),
        };
        debug!(target: "network", "{:?}: Sending {:?} message to peer {}", self.node_info.id, msg, self.peer_info);
        match peer_message_to_bytes(msg) {
            Ok(bytes) => {
                self.tracker.increment_sent(bytes.len() as u64);
                self.framed.write(bytes);
            }
            Err(err) => error!(target: "network", "Error converting proto to bytes: {}", err),
        };
    }

    fn fetch_client_chain_info(&mut self, ctx: &mut Context<Peer>) {
        ctx.wait(self.client_addr
            .send(NetworkClientMessages::GetChainInfo)
            .into_actor(self)
            .then(move |res, act, _ctx| match res {
                Ok(NetworkClientResponses::ChainInfo { genesis, .. }) => {
                    act.genesis = genesis;
                    actix::fut::ok(())
                }
                Err(err) => {
                    error!(target: "network", "Failed sending GetChain to client: {}", err);
                    actix::fut::err(())
                }
                _ => actix::fut::err(()),
            }));
    }

    fn send_handshake(&mut self, ctx: &mut Context<Peer>) {
        self.client_addr
            .send(NetworkClientMessages::GetChainInfo)
            .into_actor(self)
            .then(move |res, act, _ctx| match res {
                Ok(NetworkClientResponses::ChainInfo { genesis, height, total_weight }) => {
                    let handshake = Handshake::new(
                        act.node_info.id,
                        act.node_info.account_id.clone(),
                        act.node_info.addr_port(),
                        PeerChainInfo { genesis, height, total_weight },
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
                let block_hash = block.hash();
                self.tracker.push_received(block_hash);
                self.chain_info.height = max(self.chain_info.height, block.header.height);
                self.chain_info.total_weight =
                    max(self.chain_info.total_weight, block.header.total_weight);
                NetworkClientMessages::Block(block, peer_id, self.tracker.has_request(block_hash))
            }
            PeerMessage::BlockHeaderAnnounce(header) => {
                let block_hash = header.hash();
                self.tracker.push_received(block_hash);
                self.chain_info.height = max(self.chain_info.height, header.height);
                self.chain_info.total_weight =
                    max(self.chain_info.total_weight, header.total_weight);
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
            PeerMessage::StateRequest(shard_id, hash) => {
                NetworkClientMessages::StateRequest(shard_id, hash)
            },
            PeerMessage::StateResponse(shard_id, hash, payload, receipts) => {
                NetworkClientMessages::StateResponse(shard_id, hash, payload, receipts)
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
                    Ok(NetworkClientResponses::StateResponse { shard_id, hash, payload, receipts }) => {
                        act.send_message(PeerMessage::StateResponse(shard_id, hash, payload, receipts))
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
        // Fetch genesis hash from the client.
        self.fetch_client_chain_info(ctx);

        debug!(target: "network", "{:?}: Peer {:?} {:?} started", self.node_info.id, self.peer_addr, self.peer_type);
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
        debug!(target: "network", "{:?}: Peer {} disconnected.", self.node_info.id, self.peer_info);
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

impl StreamHandler<Vec<u8>, io::Error> for Peer {
    fn handle(&mut self, msg: Vec<u8>, ctx: &mut Self::Context) {
        self.tracker.increment_received(msg.len() as u64);
        let peer_msg = match bytes_to_peer_message(&msg) {
            Ok(peer_msg) => peer_msg,
            Err(err) => {
                error!(target: "network", "Received invalid data {:?} from {}: {}", msg, self.peer_info, err);
                return;
            }
        };
        match (self.peer_type, self.peer_status, peer_msg) {
            (_, PeerStatus::Connecting, PeerMessage::Handshake(handshake)) => {
                debug!(target: "network", "{:?}: Received handshake {:?}", self.node_info.id, handshake);
                if handshake.chain_info.genesis != self.genesis {
                    info!(target: "network", "Received connection from node with different genesis.");
                    ctx.stop();
                }
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
                self.chain_info = handshake.chain_info;
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
                                debug!(target: "network", "{:?}: Peer {:?} successfully consolidated", act.node_info.id, act.peer_addr);
                                act.peer_info = Some(peer_info).into();
                                act.peer_status = PeerStatus::Ready;
                                // Respond to handshake if it's inbound and connection was consolidated.
                                if act.peer_type == PeerType::Inbound {
                                    act.send_handshake(ctx);
                                }
                                actix::fut::ok(())
                            },
                            _ => {
                                info!(target: "network", "{:?}: Peer with handshake {:?} wasn't consolidated, disconnecting.", act.node_info.id, handshake);
                                ctx.stop();
                                actix::fut::err(())
                            }
                        }
                    })
                    .wait(ctx);
            }
            (_, PeerStatus::Ready, PeerMessage::Handshake(_)) => {
                // Received handshake after already have seen handshake from this peer.
                debug!(target: "network", "Duplicate handshake from {}", self.peer_info);
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

impl Handler<QueryPeerStats> for Peer {
    type Result = PeerStatsResult;

    fn handle(&mut self, _: QueryPeerStats, _: &mut Self::Context) -> Self::Result {
        PeerStatsResult {
            chain_info: self.chain_info,
            received_bytes_per_sec: self.tracker.received_bytes.bytes_per_min() / 60,
            sent_bytes_per_sec: self.tracker.sent_bytes.bytes_per_min() / 60,
            is_abusive: self.is_abusive(),
            message_counts: (
                self.tracker.sent_bytes.count_per_min(),
                self.tracker.received_bytes.count_per_min(),
            ),
        }
    }
}
