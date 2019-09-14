use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::actors::resolver::{ConnectAddr, Resolver};
use actix::io::FramedWrite;
use actix::prelude::Stream;
use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, Recipient,
    StreamHandler, SystemService, WrapFuture,
};
use chrono::offset::TimeZone;
use chrono::{DateTime, Utc};
use futures::{future, Future};
use log::{debug, error, info, warn};
use rand::{thread_rng, Rng};
use tokio::codec::FramedRead;
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};

use near_primitives::types::AccountId;
use near_primitives::utils::from_timestamp;
use near_store::Store;

use crate::codec::Codec;
use crate::peer::Peer;
use crate::peer_store::PeerStore;
use crate::types::{
    AnnounceAccount, Ban, Consolidate, FullPeerInfo, InboundTcpConnect, KnownPeerStatus,
    NetworkInfo, OutboundTcpConnect, PeerId, PeerList, PeerMessage, PeerType, PeersRequest,
    PeersResponse, QueryPeerStats, RawRoutedMessage, ReasonForBan, RoutedMessage,
    RoutedMessageBody, SendMessage, Unregister,
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
    NewRoute,
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

#[derive(Debug, Clone)]
pub struct RoutingTableEntry {
    /// Keep track of several routes for the same account id.
    pub routes: Vec<AnnounceAccount>,
    /// Last time a route was used.
    last_update: Instant,
    /// Index of the last route reported to the Peer Manager to send message.
    last_route_reported: usize,
    /// When new peers get connected to us, we will send one (among the shortest routes)
    /// for every account id known to us.
    /// This is the index of the last best route shared.
    last_shortest_route_reported: usize,
    /// Maximum number of routes that we should keep track.
    max_routes_to_save: usize,
}

impl RoutingTableEntry {
    fn new(route: AnnounceAccount, max_routes_to_save: usize) -> Self {
        Self {
            routes: vec![route],
            last_update: Instant::now(),
            last_route_reported: 0,
            last_shortest_route_reported: 0,
            max_routes_to_save,
        }
    }

    fn route_through_peer_index(&self, peer_id: &PeerId) -> Option<usize> {
        self.routes
            .iter()
            .enumerate()
            .find(|(_, route)| route.peer_id() == *peer_id)
            .map(|(position, _)| position)
    }

    fn add_route(&mut self, route: AnnounceAccount) -> RoutingTableUpdate {
        self.last_update = Instant::now();
        let peer_index = self.route_through_peer_index(&route.peer_id());

        if let Some(peer_index) = peer_index {
            // We already have a route through this peer.
            if self.routes[peer_index].num_hops() <= route.num_hops() {
                // Ignore this route since we already have a route at least as good
                // as this one through this peer.
                RoutingTableUpdate::Ignore
            } else {
                // Overwrite old route through this peer with better route.
                self.routes[peer_index] = route;
                RoutingTableUpdate::UpdatedAccount
            }
        } else {
            if self.routes.len() == self.max_routes_to_save {
                // Don't exceed maximum number of routes to store for every
                RoutingTableUpdate::Ignore
            } else {
                self.routes.push(route);
                RoutingTableUpdate::NewRoute
            }
        }
    }

    fn next_peer_id(&mut self) -> PeerId {
        self.last_route_reported += 1;
        if self.last_route_reported == self.routes.len() {
            self.last_route_reported = 0;
        }
        self.routes[self.last_route_reported].peer_id()
    }

    pub fn next_best_route(&mut self) -> AnnounceAccount {
        // Save unwrap (there is at least one route always).
        let shortest_route_hops = self.routes.iter().map(|route| route.num_hops()).min().unwrap();

        loop {
            self.last_shortest_route_reported += 1;
            if self.last_shortest_route_reported == self.routes.len() {
                self.last_shortest_route_reported = 0;
            }

            if self.routes[self.last_shortest_route_reported].num_hops() == shortest_route_hops {
                return self.routes[self.last_shortest_route_reported].clone();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingTable {
    pub account_peers: HashMap<AccountId, RoutingTableEntry>,
    last_purge: Instant,
    ttl_account_id_router: Duration,
    /// Maximum number of routes that we should keep track for each Account id
    max_routes_to_save: usize,
}

impl RoutingTable {
    fn new(ttl_account_id_router: Duration, max_routes_to_save: usize) -> Self {
        Self {
            account_peers: HashMap::new(),
            last_purge: Instant::now(),
            ttl_account_id_router,
            max_routes_to_save,
        }
    }

    fn remove_old_routes(&mut self) {
        let now = Instant::now();
        let ttl_account_id_router = self.ttl_account_id_router;

        if (now - self.last_purge) >= ttl_account_id_router {
            self.account_peers = self
                .account_peers
                .drain()
                .filter(|entry| now - entry.1.last_update < ttl_account_id_router)
                .collect();

            self.last_purge = Instant::now();
        }
    }

    fn update(&mut self, data: AnnounceAccount) -> RoutingTableUpdate {
        self.remove_old_routes();
        match self.account_peers.get_mut(&data.account_id) {
            // If this account id is already tracked in the routing table ...
            Some(entry) => entry.add_route(data),
            // If we don't have this account id store it in the routing table.
            None => {
                self.account_peers.insert(
                    data.account_id.clone(),
                    RoutingTableEntry::new(data, self.max_routes_to_save),
                );
                RoutingTableUpdate::NewAccount
            }
        }
    }

    /// Remove all routes that contains this peer as the first hop.
    fn remove(&mut self, peer_id: &PeerId) {
        let mut to_delete = vec![];
        for (account_id, mut value) in self.account_peers.iter_mut() {
            value.routes =
                value.routes.drain(..).filter(|route| &route.peer_id() != peer_id).collect();

            if value.routes.is_empty() {
                to_delete.push(account_id.clone());
            }
        }

        for account_id in to_delete.into_iter() {
            self.account_peers.remove(&account_id);
        }
    }

    fn get_route(&mut self, account_id: &AccountId) -> Option<PeerId> {
        self.remove_old_routes();
        self.account_peers.get_mut(account_id).map(|entry| {
            entry.last_update = Instant::now();
            entry.next_peer_id()
        })
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
        let ttl_account_id_router = config.ttl_account_id_router;
        let max_routes_to_save = config.max_routes_to_store;
        Ok(PeerManagerActor {
            peer_id: config.public_key.clone().into(),
            config,
            client_addr,
            peer_store,
            active_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            routing_table: RoutingTable::new(ttl_account_id_router, max_routes_to_save),
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
            full_peer_info.peer_info.id.clone(),
            ActivePeer {
                addr: addr.clone(),
                full_peer_info,
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                last_time_peer_requested: Utc.timestamp(0, 0),
            },
        );

        // Sync routes to known account ids with new peer.
        for mut announcement in
            self.routing_table.account_peers.values_mut().map(|entry| entry.next_best_route())
        {
            // Update route with our information in case we are not the source.
            if announcement.header().peer_id != self.peer_id {
                announcement.extend(self.peer_id.clone(), &self.config.secret_key);
            }
            actix::spawn(
                addr.send(SendMessage { message: PeerMessage::AnnounceAccount(announcement) })
                    .map_err(|e| error!(target: "network", "{}", e))
                    .map(|_| ()),
            );
        }
    }

    fn unregister_peer(&mut self, peer_id: PeerId) {
        // If this is an unconsolidated peer because failed / connected inbound, just delete it.
        if self.outgoing_peers.contains(&peer_id) {
            self.outgoing_peers.remove(&peer_id);
            return;
        }
        self.active_peers.remove(&peer_id);
        self.routing_table.remove(&peer_id);
        unwrap_or_error!(self.peer_store.peer_disconnected(&peer_id), "Failed to save peer data");
    }

    // TODO: Should broadcast this with evidence in case of *intentional* misbehaviour.
    fn ban_peer(&mut self, peer_id: &PeerId, ban_reason: ReasonForBan) {
        info!(target: "network", "Banning peer {:?} for {:?}", peer_id, ban_reason);
        self.active_peers.remove(&peer_id);
        self.routing_table.remove(&peer_id);
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
        let peer_id = self.peer_id.clone();
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
            None => {
                return vec![];
            }
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
            let peer_id1 = peer_id.clone();
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
            if let KnownPeerStatus::Banned(_, last_banned) = peer_state.status {
                let interval = unwrap_or_error!(
                    (Utc::now() - from_timestamp(last_banned)).to_std(),
                    "Failed to convert time"
                );
                if interval > self.config.ban_window {
                    info!(target: "network", "Monitor peers: unbanned {} after {:?}.", peer_id, interval);
                    to_unban.push(peer_id.clone());
                }
            }
        }
        for peer_id in to_unban {
            unwrap_or_error!(self.peer_store.peer_unban(&peer_id), "Failed to unban a peer");
        }

        if self.is_outbound_bootstrap_needed() {
            if let Some(peer_info) = self.sample_random_peer(&self.outgoing_peers) {
                self.outgoing_peers.insert(peer_info.id.clone());
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
        // If this is an announcement from our account id.
        if announce_account.original_peer_id() == self.peer_id {
            // Check that this announcement doesn't contain any other hop.
            // We must avoid cycle in the routes.
            if announce_account.num_hops() == 0 {
                let msg = SendMessage { message: PeerMessage::AnnounceAccount(announce_account) };
                self.broadcast_message(ctx, msg);
            }
        } else {
            // Check that we don't belong to this route.
            if announce_account.route.iter().any(|announce| announce.peer_id == self.peer_id) {
                return;
            }

            // Use shorter suffix such that we know peer in that suffix.
            let mut found = false;
            announce_account.route = announce_account
                .route
                .into_iter()
                .take_while(|x| {
                    if found {
                        false
                    } else {
                        if self.active_peers.contains_key(&x.peer_id) {
                            found = true;
                        }
                        true
                    }
                })
                .collect();

            assert!(self.active_peers.contains_key(&announce_account.peer_id()));

            // If this is a new account send an announcement to random set of peers.
            if self.routing_table.update(announce_account.clone()).is_new() {
                let routes: Vec<_> =
                    announce_account.route.iter().map(|hop| hop.peer_id.clone()).collect();
                announce_account.extend(self.peer_id.clone(), &self.config.secret_key);
                let msg = SendMessage { message: PeerMessage::AnnounceAccount(announce_account) };

                // Broadcast announcements to peer that don't belong to this route.
                let requests: Vec<_> = self
                    .active_peers
                    .values()
                    .filter(|&peer| {
                        routes.iter().all(|peer_id| peer_id != &peer.full_peer_info.peer_info.id)
                    })
                    .map(|peer| peer.addr.send(msg.clone()))
                    .collect();

                future::join_all(requests)
                    .into_actor(self)
                    .map_err(|e, _, _| error!("Failed sending broadcast message: {}", e))
                    .and_then(|_, _, _| actix::fut::ok(()))
                    .spawn(ctx);
            }
        }
    }

    /// Send message to specific account.
    fn send_message_to_account(&mut self, ctx: &mut Context<Self>, msg: RoutedMessage) {
        let account_id = &msg.account_id;
        if let Some(peer_id) = self.routing_table.get_route(account_id) {
            if let Some(active_peer) = self.active_peers.get(&peer_id) {
                active_peer
                    .addr
                    .send(SendMessage { message: PeerMessage::Routed(msg) })
                    .into_actor(self)
                    .map_err(|e, _, _| error!("Failed sending message: {}", e))
                    .and_then(|_, _, _| actix::fut::ok(()))
                    .spawn(ctx);
            } else {
                // This should be unreachable!
                error!(target: "network",
                    "Sending message to {} / {} not a peer: {:?}\n{:?}",
                    account_id,
                    peer_id,
                    self.active_peers.keys(),
                    msg
                );
            }
        } else {
            // TODO WTF: remove this
            //            panic!(format!(
            //                "Unknown account {} in routing table: {:?}",
            //                account_id, self.routing_table
            //            ));
            warn!(target: "network", "Unknown account {} in routing table. Known accounts: {:?}", account_id, self.routing_table.account_peers.keys());
        }
    }

    fn sign_routed_message(&self, msg: RawRoutedMessage) -> RoutedMessage {
        msg.sign(self.peer_id.clone(), &self.config.secret_key)
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
            NetworkRequests::FetchInfo => {
                let (sent_bytes_per_sec, received_bytes_per_sec) = self.get_total_bytes_per_sec();

                let known_producers =
                    self.routing_table.account_peers.keys().cloned().collect::<Vec<_>>();

                NetworkResponses::Info(NetworkInfo {
                    num_active_peers: self.num_active_peers(),
                    peer_max_count: self.config.peer_max_count,
                    most_weight_peers: self.most_weight_peers(),
                    sent_bytes_per_sec,
                    received_bytes_per_sec,
                    known_producers,
                })
            }
            NetworkRequests::Block { block } => {
                self.broadcast_message(ctx, SendMessage { message: PeerMessage::Block(block) });
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockHeaderAnnounce { header, approval } => {
                if let Some(approval) = approval {
                    if let Some(account_id) = self.config.account_id.clone() {
                        self.send_message_to_account(
                            ctx,
                            self.sign_routed_message(RawRoutedMessage {
                                account_id: approval.target,
                                body: RoutedMessageBody::BlockApproval(
                                    account_id,
                                    approval.hash,
                                    approval.signature,
                                ),
                            }),
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
            NetworkRequests::StateRequest { shard_id, hash, account_id } => {
                self.send_message_to_account(
                    ctx,
                    self.sign_routed_message(RawRoutedMessage {
                        account_id,
                        body: RoutedMessageBody::StateRequest(shard_id, hash),
                    }),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                // TODO: send stop signal to the addr.
                // if let Some(_) = self.active_peers.get(&peer_id) {}
                self.ban_peer(&peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                self.announce_account(ctx, announce_account);
                NetworkResponses::NoResponse
            }
            NetworkRequests::ChunkPartRequest { account_id, part_request } => {
                self.send_message_to_account(
                    ctx,
                    self.sign_routed_message(RawRoutedMessage {
                        account_id,
                        body: RoutedMessageBody::ChunkPartRequest(part_request),
                    }),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::ChunkOnePartRequest { account_id, one_part_request } => {
                self.send_message_to_account(
                    ctx,
                    self.sign_routed_message(RawRoutedMessage {
                        account_id,
                        body: RoutedMessageBody::ChunkOnePartRequest(one_part_request),
                    }),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::ChunkOnePartResponse { peer_id, header_and_part } => {
                if let Some(active_peer) = self.active_peers.get(&peer_id) {
                    active_peer.addr.do_send(SendMessage {
                        message: PeerMessage::ChunkOnePart(header_and_part),
                    });
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::ChunkPart { peer_id, part } => {
                if let Some(active_peer) = self.active_peers.get(&peer_id) {
                    active_peer.addr.do_send(SendMessage { message: PeerMessage::ChunkPart(part) });
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::ChunkOnePartMessage { account_id, header_and_part } => {
                self.send_message_to_account(
                    ctx,
                    self.sign_routed_message(RawRoutedMessage {
                        account_id,
                        body: RoutedMessageBody::ChunkOnePart(header_and_part),
                    }),
                );
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

    fn handle(&mut self, msg: PeersResponse, _ctx: &mut Self::Context) {
        self.peer_store.add_peers(
            msg.peers.into_iter().filter(|peer_info| peer_info.id != self.peer_id).collect(),
        );
    }
}

/// "Return" true if this message is for this peer and should be sent to the client.
/// Otherwise try to route this message to the final receiver and return false.
impl Handler<RoutedMessage> for PeerManagerActor {
    type Result = bool;

    fn handle(&mut self, msg: RoutedMessage, ctx: &mut Self::Context) -> Self::Result {
        if self.config.account_id.as_ref().map_or(false, |me| me == &msg.account_id) {
            true
        } else {
            self.send_message_to_account(ctx, msg);
            // Otherwise route it to its corresponding destination.
            false
        }
    }
}

impl Handler<RawRoutedMessage> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: RawRoutedMessage, ctx: &mut Self::Context) {
        self.send_message_to_account(ctx, self.sign_routed_message(msg));
    }
}
