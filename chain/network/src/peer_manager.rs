use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use actix::actors::resolver::{ConnectAddr, Resolver};
use actix::io::FramedWrite;
use actix::prelude::Stream;
use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient,
    StreamHandler, SystemService, WrapFuture,
};
use chrono::offset::TimeZone;
use chrono::{DateTime, Utc};
use futures::future;
use log::{debug, error, info, warn};
use rand::{thread_rng, Rng};
use tokio::codec::FramedRead;
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};

use near_primitives::types::AccountId;
use near_store::Store;

use crate::codec::Codec;
use crate::peer::Peer;
use crate::peer_store::PeerStore;
use crate::types::{
    AnnounceAccount, Ban, Consolidate, FullPeerInfo, InboundTcpConnect, KnownPeerStatus,
    NetworkInfo, OutboundTcpConnect, PeerId, PeerList, PeerMessage, PeerType, PeersRequest,
    PeersResponse, QueryPeerStats, ReasonForBan, SendMessage, Unregister,
};
use crate::types::{
    NetworkClientMessages, NetworkConfig, NetworkRequests, NetworkResponses, PeerInfo,
};

/// How often to request peers from active peers.
const REQUEST_PEERS_SECS: i64 = 60;

macro_rules! unwrap_or_error(($obj: expr, $error: expr) => (match $obj {
    Ok(result) => result,
    Err(err) => {
        error!(target: "network", "{}: {}", $error, err);
        return;
    }
}));

/// Contains information relevant to an active peer.
struct ActivePeer {
    addr: Addr<Peer>,
    full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    sent_bytes_per_sec: u64,
    /// Last time requested peers.
    last_time_peer_requested: DateTime<Utc>,
}

enum RoutingTableUpdate {
    NewAccount,
    UpdatedAccount,
    Ignore,
}

impl RoutingTableUpdate {
    fn is_new(&self) -> bool {
        match self {
            RoutingTableUpdate::NewAccount => true,
            _ => false,
        }
    }
}

// TODO: Clear routing table periodically
struct RoutingTable {
    account_peers: HashMap<AccountId, (PeerId, usize)>,
}

impl RoutingTable {
    fn new() -> Self {
        Self { account_peers: HashMap::new() }
    }

    fn update(&mut self, data: &AnnounceAccount) -> RoutingTableUpdate {
        match self.account_peers.get(&data.account_id) {
            // If this account id is already tracked in the routing table ...
            Some((_, num_hops)) => {
                // check if this connection is better than the one we keep track
                // regarding number of intermediate hops.
                if data.num_hops() < *num_hops {
                    // and add it
                    self.account_peers
                        .insert(data.account_id.clone(), (data.peer_id_sender(), data.num_hops()));
                    RoutingTableUpdate::UpdatedAccount
                } else {
                    RoutingTableUpdate::Ignore
                }
            }
            // If we don't have this account id store it in the routing table.
            None => {
                self.account_peers
                    .insert(data.account_id.clone(), (data.peer_id_sender(), data.num_hops()));
                RoutingTableUpdate::NewAccount
            }
        }
    }

    fn get_route(&self, account_id: &AccountId) -> Option<&PeerId> {
        self.account_peers.get(account_id).map(|(peer_id, _)| peer_id)
    }
}

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    /// Networking configuration.
    config: NetworkConfig,
    /// Peer information for this node.
    peer_id: PeerId,
    /// Address of the client actor.
    client_addr: Recipient<NetworkClientMessages>,
    /// Peer store that provides read/write access to peers.
    peer_store: PeerStore,
    /// Set of outbound connections that were not consolidated yet.
    outgoing_peers: HashSet<PeerId>,
    /// Active peers (inbound and outbound) with their full peer information.
    active_peers: HashMap<PeerId, ActivePeer>,
    /// Routing table to keep track of account id
    routing_table: RoutingTable,
    /// Monitor peers attempts, used for fast checking in the beginning with exponential backoff.
    monitor_peers_attempts: u64,
}

impl PeerManagerActor {
    pub fn new(
        store: Arc<Store>,
        config: NetworkConfig,
        client_addr: Recipient<NetworkClientMessages>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let peer_store = PeerStore::new(store, &config.boot_nodes)?;
        debug!(target: "network", "Found known peers: {} (boot nodes={})", peer_store.len(), config.boot_nodes.len());
        Ok(PeerManagerActor {
            peer_id: config.public_key.into(),
            config,
            client_addr,
            peer_store,
            active_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            // account_peers: HashMap::default(),
            routing_table: RoutingTable::new(),
            monitor_peers_attempts: 0,
        })
    }

    fn num_active_peers(&self) -> usize {
        self.active_peers.len()
    }

    fn register_peer(&mut self, full_peer_info: FullPeerInfo, addr: Addr<Peer>) {
        if self.outgoing_peers.contains(&full_peer_info.peer_info.id) {
            self.outgoing_peers.remove(&full_peer_info.peer_info.id);
        }
        unwrap_or_error!(
            self.peer_store.peer_connected(&full_peer_info),
            "Failed to save peer data"
        );

        self.active_peers.insert(
            full_peer_info.peer_info.id,
            ActivePeer {
                addr,
                full_peer_info,
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                last_time_peer_requested: Utc.timestamp(0, 0),
            },
        );
    }

    fn unregister_peer(&mut self, peer_id: PeerId) {
        // If this is an unconsolidated peer because failed / connected inbound, just delete it.
        if self.outgoing_peers.contains(&peer_id) {
            self.outgoing_peers.remove(&peer_id);
            return;
        }
        self.active_peers.remove(&peer_id);
        unwrap_or_error!(self.peer_store.peer_disconnected(&peer_id), "Failed to save peer data");
    }

    fn ban_peer(&mut self, peer_id: &PeerId, ban_reason: ReasonForBan) {
        info!(target: "network", "Banning peer {:?}", peer_id);
        self.active_peers.remove(&peer_id);
        unwrap_or_error!(self.peer_store.peer_ban(peer_id, ban_reason), "Failed to save peer data");
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

            // TODO: check if peer is banned or known based on IP address and port.

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
        (self.active_peers.len() + self.outgoing_peers.len())
            < (self.config.peer_max_count as usize)
    }

    /// Returns single random peer with the most weight.
    fn most_weight_peers(&self) -> Vec<FullPeerInfo> {
        let max_weight = match self
            .active_peers
            .values()
            .map(|active_peer| active_peer.full_peer_info.chain_info.total_weight)
            .max()
        {
            Some(w) => w,
            None => return vec![],
        };
        self.active_peers
            .values()
            .filter_map(|active_peer| {
                if active_peer.full_peer_info.chain_info.total_weight == max_weight {
                    Some(active_peer.full_peer_info.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    /// Returns bytes sent/received across all peers.
    fn get_total_bytes_per_sec(&self) -> (u64, u64) {
        let sent_bps = self.active_peers.values().map(|x| x.sent_bytes_per_sec).sum();
        let received_bps = self.active_peers.values().map(|x| x.received_bytes_per_sec).sum();
        (sent_bps, received_bps)
    }

    /// Get a random peer we are not connected to from the known list.
    fn sample_random_peer(&self, ignore_list: &HashSet<PeerId>) -> Option<PeerInfo> {
        let unconnected_peers = self.peer_store.unconnected_peers(ignore_list);
        let index = thread_rng().gen_range(0, std::cmp::max(unconnected_peers.len(), 1));

        unconnected_peers
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if i == index { Some(v.clone()) } else { None })
            .next()
    }

    /// Query current peers for more peers.
    fn query_active_peers_for_more_peers(&mut self, ctx: &mut Context<Self>) {
        let mut requests = vec![];
        let msg = SendMessage { message: PeerMessage::PeersRequest };
        for (_, active_peer) in self.active_peers.iter_mut() {
            if Utc::now().signed_duration_since(active_peer.last_time_peer_requested).num_seconds()
                > REQUEST_PEERS_SECS
            {
                active_peer.last_time_peer_requested = Utc::now();
                requests.push(active_peer.addr.send(msg.clone()));
            }
        }
        future::join_all(requests)
            .into_actor(self)
            .map_err(|e, _, _| error!("Failed sending broadcast message: {}", e))
            .and_then(|_, _, _| actix::fut::ok(()))
            .spawn(ctx);
    }

    /// Periodically query peer actors for latest weight and traffic info.
    fn monitor_peer_stats(&mut self, ctx: &mut Context<Self>) {
        for (peer_id, active_peer) in self.active_peers.iter() {
            let peer_id1 = *peer_id;
            active_peer.addr.send(QueryPeerStats {})
                .into_actor(self)
                .map_err(|err, _, _| error!("Failed sending message: {}", err))
                .and_then(move |res, act, _| {
                    if res.is_abusive {
                        warn!(target: "network", "Banning peer {} for abuse ({} sent, {} recv)", peer_id1, res.message_counts.0, res.message_counts.1);
                        act.ban_peer(&peer_id1, ReasonForBan::Abusive);
                    } else if let Some(active_peer) = act.active_peers.get_mut(&peer_id1) {
                        active_peer.full_peer_info.chain_info = res.chain_info;
                        active_peer.sent_bytes_per_sec = res.sent_bytes_per_sec;
                        active_peer.received_bytes_per_sec = res.received_bytes_per_sec;
                    }
                    actix::fut::ok(())
                })
                .spawn(ctx);
        }

        ctx.run_later(self.config.peer_stats_period, move |act, ctx| {
            act.monitor_peer_stats(ctx);
        });
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - unban peers that have been banned for awhile,
    ///  - remove expired peers,
    fn monitor_peers(&mut self, ctx: &mut Context<Self>) {
        let mut to_unban = vec![];
        for (peer_id, peer_state) in self.peer_store.iter() {
            match peer_state.status {
                KnownPeerStatus::Banned(_, last_banned) => {
                    let interval = unwrap_or_error!(
                        (Utc::now() - last_banned).to_std(),
                        "Failed to convert time"
                    );
                    if interval > self.config.ban_window {
                        info!(target: "network", "Monitor peers: unbanned {} after {:?}.", peer_id, interval);
                        to_unban.push(peer_id.clone());
                    }
                }
                _ => {}
            }
        }
        for peer_id in to_unban {
            unwrap_or_error!(self.peer_store.peer_unban(&peer_id), "Failed to unban a peer");
        }

        if self.is_outbound_bootstrap_needed() {
            if let Some(peer_info) = self.sample_random_peer(&self.outgoing_peers) {
                self.outgoing_peers.insert(peer_info.id);
                ctx.notify(OutboundTcpConnect { peer_info });
            } else {
                self.query_active_peers_for_more_peers(ctx);
            }
        }

        unwrap_or_error!(
            self.peer_store.remove_expired(&self.config),
            "Failed to remove expired peers"
        );

        // Reschedule the bootstrap peer task, starting of as quick as possible with exponential backoff.
        let wait = Duration::from_millis(cmp::min(
            self.config.bootstrap_peers_period.as_millis() as u64,
            10 << self.monitor_peers_attempts,
        ));
        self.monitor_peers_attempts = cmp::min(13, self.monitor_peers_attempts + 1);
        ctx.run_later(wait, move |act, ctx| {
            act.monitor_peers(ctx);
        });
    }

    /// Broadcast message to all active peers.
    fn broadcast_message(&self, ctx: &mut Context<Self>, msg: SendMessage) {
        let requests: Vec<_> =
            self.active_peers.values().map(|peer| peer.addr.send(msg.clone())).collect();
        future::join_all(requests)
            .into_actor(self)
            .map_err(|e, _, _| error!("Failed sending broadcast message: {}", e))
            .and_then(|_, _, _| actix::fut::ok(()))
            .spawn(ctx);
    }

    fn announce_account(&mut self, ctx: &mut Context<Self>, mut announce_account: AnnounceAccount) {
        // If this is a new account send an announcement to random set of peers.
        if self.routing_table.update(&announce_account).is_new() {
            if announce_account.header().peer_id != self.peer_id {
                // If this announcement was not sent by this peer, add peer information
                announce_account.extend(self.peer_id, &self.config.secret_key);
            }

            let msg = SendMessage { message: PeerMessage::AnnounceAccount(announce_account) };
            self.broadcast_message(ctx, msg);
        }
    }

    /// Send message to specific account.
    fn send_message_to_account(
        &self,
        ctx: &mut Context<Self>,
        account_id: AccountId,
        msg: SendMessage,
    ) {
        if let Some(peer_id) = self.routing_table.get_route(&account_id) {
            if let Some(active_peer) = self.active_peers.get(peer_id) {
                active_peer
                    .addr
                    .send(msg)
                    .into_actor(self)
                    .map_err(|e, _, _| error!("Failed sending message: {}", e))
                    .and_then(|_, _, _| actix::fut::ok(()))
                    .spawn(ctx);
            } else {
                error!(target: "network", "Missing peer {:?} that is related to account {}", peer_id, account_id);
            }
        } else {
            warn!(target: "network", "Unknown account {} in routing table.", account_id);
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
            info!(target: "info", "Server listening at {}@{}", self.peer_id, server_addr);
            ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(InboundTcpConnect::new));
        }

        // Start peer monitoring.
        self.monitor_peers(ctx);

        // Start active peer stats querying.
        self.monitor_peer_stats(ctx);
    }
}

impl Handler<NetworkRequests> for PeerManagerActor {
    type Result = NetworkResponses;

    fn handle(&mut self, msg: NetworkRequests, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkRequests::FetchInfo { level } => {
                let (sent_bytes_per_sec, received_bytes_per_sec) = self.get_total_bytes_per_sec();

                let routes =
                    if level > 0 { Some(self.routing_table.account_peers.clone()) } else { None };

                NetworkResponses::Info(NetworkInfo {
                    num_active_peers: self.num_active_peers(),
                    peer_max_count: self.config.peer_max_count,
                    most_weight_peers: self.most_weight_peers(),
                    sent_bytes_per_sec,
                    received_bytes_per_sec,
                    routes,
                })
            }
            NetworkRequests::Block { block } => {
                self.broadcast_message(ctx, SendMessage { message: PeerMessage::Block(block) });
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
                if let Some(active_peer) = self.active_peers.get(&peer_id) {
                    active_peer
                        .addr
                        .do_send(SendMessage { message: PeerMessage::BlockRequest(hash) });
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if let Some(active_peer) = self.active_peers.get(&peer_id) {
                    active_peer
                        .addr
                        .do_send(SendMessage { message: PeerMessage::BlockHeadersRequest(hashes) });
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::StateRequest { shard_id, hash, peer_id } => {
                if let Some(active_peer) = self.active_peers.get(&peer_id) {
                    active_peer.addr.do_send(SendMessage {
                        message: PeerMessage::StateRequest(shard_id, hash),
                    });
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                if let Some(_) = self.active_peers.get(&peer_id) {
                    // TODO: send stop signal to the addr.
                }
                self.ban_peer(&peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                self.announce_account(ctx, announce_account);
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
                            act.connect_peer(
                                ctx.address(),
                                stream,
                                PeerType::Outbound,
                                Some(msg.peer_info),
                            );
                            actix::fut::ok(())
                        }
                        Err(err) => {
                            info!(target: "network", "Error connecting to {}: {}", addr, err);
                            act.outgoing_peers.remove(&msg.peer_info.id);
                            actix::fut::err(())
                        }
                    },
                    Err(err) => {
                        info!(target: "network", "Error connecting to {}: {}", addr, err);
                        act.outgoing_peers.remove(&msg.peer_info.id);
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
        self.ban_peer(&msg.peer_id, msg.ban_reason);
    }
}

impl Handler<PeersRequest> for PeerManagerActor {
    type Result = PeerList;

    fn handle(&mut self, _msg: PeersRequest, _ctx: &mut Self::Context) -> Self::Result {
        PeerList { peers: self.peer_store.healthy_peers(self.config.max_send_peers) }
    }
}

impl Handler<PeersResponse> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, mut msg: PeersResponse, _ctx: &mut Self::Context) {
        self.peer_store.add_peers(
            msg.peers.drain(..).filter(|peer_info| peer_info.id != self.peer_id).collect(),
        );
    }
}
