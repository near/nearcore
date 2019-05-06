use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use actix::actors::resolver::{ConnectAddr, Resolver};
use actix::io::FramedWrite;
use actix::prelude::Stream;
use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient,
    StreamHandler, SystemService, WrapFuture,
};
use chrono::Utc;
use futures::future;
use log::{debug, error, info, warn};
use rand::{thread_rng, Rng};
use tokio::codec::FramedRead;
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};

use near_store::Store;
use primitives::types::AccountId;

use crate::codec::Codec;
use crate::peer::Peer;
use crate::types::{Ban, Consolidate, FullPeerInfo, InboundTcpConnect, KnownPeerState, KnownPeerStatus, OutboundTcpConnect, PeerId, PeerMessage, PeerType, SendMessage, Unregister, ReasonForBan};
use crate::types::{
    NetworkClientMessages, NetworkConfig, NetworkRequests, NetworkResponses, PeerInfo,
};

pub struct PeerManagerActor {
    /// Networking configuration.
    config: NetworkConfig,
    /// Peer information for this node.
    peer_id: PeerId,
    /// Address of the client actor.
    client_addr: Recipient<NetworkClientMessages>,
    /// Access to store to read/write peers.
    store: Arc<Store>,
    /// Full list of known peers loaded from disk.
    /// TODO: figure out if we need this, or instead should go directly via store.
    peer_states: HashMap<PeerId, KnownPeerState>,
    /// Set of outbound connections that were not consolidated yet.
    outgoing_peers: HashSet<PeerId>,
    /// Active peers (inbound and outbound) with their full peer information.
    active_peers: HashMap<PeerId, (Addr<Peer>, FullPeerInfo)>,
    /// Peers with known account ids.
    account_peers: HashMap<AccountId, PeerId>,
}

impl PeerManagerActor {
    pub fn new(
        store: Arc<Store>,
        config: NetworkConfig,
        client_addr: Recipient<NetworkClientMessages>,
    ) -> Self {
        let mut peer_states = HashMap::default();
        for peer_info in config.boot_nodes.iter() {
            peer_states.insert(peer_info.id, KnownPeerState::new(peer_info.clone()));
        }
        // TODO: load from store.
        debug!(target: "network", "Found known peers: {} (boot nodes={})", peer_states.len(), config.boot_nodes.len());
        PeerManagerActor {
            peer_id: config.public_key.into(),
            config,
            client_addr,
            store,
            peer_states,
            active_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            account_peers: HashMap::default(),
        }
    }

    fn num_active_peers(&self) -> usize {
        self.active_peers.len()
    }

    fn register_peer(&mut self, peer_info: FullPeerInfo, addr: Addr<Peer>) {
        if self.outgoing_peers.contains(&peer_info.peer_info.id) {
            self.outgoing_peers.remove(&peer_info.peer_info.id);
        }
        let entry = self
            .peer_states
            .entry(peer_info.peer_info.id)
            .or_insert(KnownPeerState::new(peer_info.peer_info.clone()));
        entry.last_seen = Utc::now();
        entry.status = KnownPeerStatus::Connected;
        if let Some(account_id) = &peer_info.peer_info.account_id {
            self.account_peers.insert(account_id.clone(), peer_info.peer_info.id);
        }
        self.active_peers.insert(peer_info.peer_info.id, (addr, peer_info));
    }

    fn unregister_peer(&mut self, peer_id: PeerId) {
        // If this is an unconsolidated peer because failed / connected inbound, just delete it.
        if self.outgoing_peers.contains(&peer_id) {
            self.outgoing_peers.remove(&peer_id);
            return;
        }
        if let Some(peer_state) = self.peer_states.get_mut(&peer_id) {
            if let Some((_, peer_info)) = self.active_peers.get(&peer_id) {
                if let Some(account_id) = &peer_info.peer_info.account_id {
                    self.account_peers.remove(account_id);
                }
                self.active_peers.remove(&peer_id);
            }
            peer_state.last_seen = Utc::now();
            peer_state.status = KnownPeerStatus::NotConnected;
        } else {
            error!(target: "network", "Unregistering unknown peer: {}", peer_id);
        }
    }

    fn ban_peer(&mut self, peer_id: PeerId, ban_reason: ReasonForBan) {
        if let Some(peer_state) = self.peer_states.get_mut(&peer_id) {
            info!(target: "network", "Banning peer {:?}", peer_state.peer_info);
            self.active_peers.remove(&peer_id);
            peer_state.last_seen = Utc::now();
            peer_state.status = KnownPeerStatus::Banned(ban_reason);
        } else {
            error!(target: "network", "Trying to ban unknown peer: {}", peer_id);
        }
    }

    /// Connects peer with given TcpStream and optional information if it's outbound.
    fn connect_peer(
        &mut self,
        recipient: Addr<Self>,
        stream: TcpStream,
        peer_type: PeerType,
        peer_info: Option<PeerInfo>,
    ) {
        let peer_id = self.peer_id;
        let account_id = self.config.account_id.clone();
        let server_addr = self.config.addr;
        let handshake_timeout = self.config.handshake_timeout;
        let client_addr = self.client_addr.clone();
        Peer::create(move |ctx| {
            let server_addr = server_addr.unwrap_or_else(|| stream.local_addr().unwrap());
            let remote_addr = stream.peer_addr().unwrap();
            let (read, write) = stream.split();

            Peer::add_stream(FramedRead::new(read, Codec::new()), ctx);
            Peer::new(
                PeerInfo { id: peer_id, addr: Some(server_addr), account_id },
                remote_addr,
                peer_info,
                peer_type,
                FramedWrite::new(write, Codec::new(), ctx),
                handshake_timeout,
                recipient,
                client_addr,
            )
        });
    }

    fn is_outbound_bootstrap_needed(&self) -> bool {
        self.active_peers.len() < (self.config.peer_max_count as usize)
    }

    /// Returns single random peer with the most weight.
    fn most_weight_peers(&self) -> Vec<FullPeerInfo> {
        let max_weight =
            match self.active_peers.values().map(|(_, x)| x.chain_info.total_weight).max() {
                Some(w) => w,
                None => return vec![],
            };
        self
            .active_peers
            .values()
            .filter_map(
                |(_, x)| if x.chain_info.total_weight == max_weight { Some(x.clone()) } else { None },
            )
            .collect::<Vec<_>>()
    }

    /// Get a random peer we are not connected to from the known list.
    fn sample_random_peer(&self) -> Option<PeerInfo> {
        let unconnected_peers: Vec<PeerInfo> = self
            .peer_states
            .values()
            .filter_map(|p| {
                if p.status == KnownPeerStatus::NotConnected || p.status == KnownPeerStatus::Unknown
                {
                    Some(p.peer_info.clone())
                } else {
                    None
                }
            })
            .collect();
        let index = thread_rng().gen_range(0, std::cmp::max(unconnected_peers.len(), 1));

        unconnected_peers
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if i == index { Some(v.clone()) } else { None })
            .next()
    }

    /// Periodically bootstrap outbound connections from known peers.
    fn bootstrap_peers(&self, ctx: &mut Context<Self>) {
        if self.is_outbound_bootstrap_needed() {
            if let Some(peer_info) = self.sample_random_peer() {
                ctx.notify(OutboundTcpConnect { peer_info });
            }
        }

        // Reschedule the bootstrap peer task.
        ctx.run_later(self.config.bootstrap_peers_period, move |act, ctx| {
            act.bootstrap_peers(ctx);
        });
    }

    /// Broadcast message to all active peers.
    fn broadcast_message(&self, ctx: &mut Context<Self>, msg: SendMessage) {
        let requests: Vec<_> =
            self.active_peers.values().map(|peer| peer.0.send(msg.clone())).collect();
        future::join_all(requests)
            .into_actor(self)
            .map_err(|e, _, _| error!("Failed sending broadcast message: {}", e))
            .and_then(|_, _, _| actix::fut::ok(()))
            .wait(ctx);
    }

    /// Send message to specific account.
    /// TODO: currently sends in direct message, need to support indirect routing.
    fn send_message_to_account(
        &self,
        ctx: &mut Context<Self>,
        account_id: AccountId,
        msg: SendMessage,
    ) {
        if let Some(peer_id) = self.account_peers.get(&account_id) {
            if let Some((addr, _)) = self.active_peers.get(peer_id) {
                addr.send(msg)
                    .into_actor(self)
                    .map_err(|e, _, _| error!("Failed sending message: {}", e))
                    .and_then(|_, _, _| actix::fut::ok(()))
                    .wait(ctx);
            } else {
                error!(target: "network", "Missing peer {} that is related to account {}", peer_id, account_id);
            }
        } else {
            warn!(target: "network", "Unknown account {} in peers, not supported indirect routing", account_id);
        }
    }
}

impl Actor for PeerManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start server if address provided.
        if let Some(server_addr) = self.config.addr {
            // TODO: for now crashes if server didn't start.
            let listener = TcpListener::bind(&server_addr).unwrap();
            info!(target: "network", "Server listening at {}", server_addr);
            ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(InboundTcpConnect::new));
        }

        // Start outbound peer bootstrapping.
        self.bootstrap_peers(ctx);
    }
}

impl Handler<NetworkRequests> for PeerManagerActor {
    type Result = NetworkResponses;

    fn handle(&mut self, msg: NetworkRequests, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkRequests::FetchInfo => NetworkResponses::Info {
                num_active_peers: self.num_active_peers(),
                peer_max_count: self.config.peer_max_count,
                most_weight_peers: self.most_weight_peers(),
            },
            NetworkRequests::Block { block } => {
                self.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::Block(block) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockHeaderAnnounce { header, approval } => {
                if let Some(approval) = approval {
                    if let Some(account_id) = &self.config.account_id {
                        self.send_message_to_account(
                            ctx,
                            approval.target,
                            SendMessage {
                                message: PeerMessage::BlockApproval(
                                    account_id.clone(),
                                    approval.hash,
                                    approval.signature,
                                ),
                            },
                        );
                    }
                }
                self.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::BlockHeaderAnnounce(header) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                if let Some((addr, _)) = self.active_peers.get(&peer_id) {
                    addr.do_send(SendMessage { message: PeerMessage::BlockRequest(hash) });
                }
                NetworkResponses::NoResponse
            },
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if let Some((addr, _)) = self.active_peers.get(&peer_id) {
                    addr.do_send(SendMessage { message: PeerMessage::BlockHeadersRequest(hashes) });
                }
                NetworkResponses::NoResponse
            },
            NetworkRequests::StateRequest { shard_id, state_root } => NetworkResponses::NoResponse,
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                if let Some((addr, _full_info)) = self.active_peers.get(&peer_id) {
                    // TODO: send stop signal to the addr.
                }
                self.ban_peer(peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
        }
    }
}

impl Handler<InboundTcpConnect> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: InboundTcpConnect, ctx: &mut Self::Context) {
        self.connect_peer(ctx.address(), msg.stream, PeerType::Inbound, None);
    }
}

impl Handler<OutboundTcpConnect> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: OutboundTcpConnect, ctx: &mut Self::Context) {
        if let Some(addr) = msg.peer_info.addr {
            Resolver::from_registry()
                .send(ConnectAddr(addr))
                .into_actor(self)
                .then(move |res, act, ctx| match res {
                    Ok(res) => match res {
                        Ok(stream) => {
                            debug!(target: "network", "Connected to {}", msg.peer_info);
                            act.outgoing_peers.insert(msg.peer_info.id);
                            act.connect_peer(
                                ctx.address(),
                                stream,
                                PeerType::Outbound,
                                Some(msg.peer_info),
                            );
                            actix::fut::ok(())
                        }
                        Err(err) => {
                            error!(target: "network", "Error connecting to {}: {}", addr, err);
                            actix::fut::err(())
                        }
                    },
                    Err(err) => {
                        error!(target: "network", "Error connecting to {}: {}", addr, err);
                        actix::fut::err(())
                    }
                })
                .wait(ctx);
        } else {
            warn!(target: "network", "Trying to connect to peer with no public address: {:?}", msg.peer_info);
        }
    }
}

impl Handler<Consolidate> for PeerManagerActor {
    type Result = bool;

    fn handle(&mut self, msg: Consolidate, _ctx: &mut Self::Context) -> Self::Result {
        // We already connected to this peer.
        if self.active_peers.contains_key(&msg.peer_info.id) {
            return false;
        }
        // This is incoming connection but we have this peer already in outgoing.
        // This only happens when both of us connect at the same time, break tie using higher peer id.
        if msg.peer_type == PeerType::Inbound && self.outgoing_peers.contains(&msg.peer_info.id) {
            // We pick connection that has lower id.
            if msg.peer_info.id > self.peer_id {
                return false;
            }
        }
        // TODO: double check that address is connectable and add account id.
        self.register_peer(
            FullPeerInfo { peer_info: msg.peer_info, chain_info: msg.chain_info },
            msg.actor,
        );
        true
    }
}

impl Handler<Unregister> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: Unregister, _ctx: &mut Self::Context) {
        self.unregister_peer(msg.peer_id);
    }
}

impl Handler<Ban> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: Ban, _ctx: &mut Self::Context) {
        self.ban_peer(msg.peer_id, msg.ban_reason);
    }
}
