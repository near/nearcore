use crate::accounts_data;
use crate::client;
use crate::concurrency;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::config;
use crate::network_protocol::{
    Edge, EdgeState, PartialEdgeInfo, PeerIdOrHash, PeerInfo, PeerMessage, Ping, Pong,
    RawRoutedMessage, RoutedMessageBody, RoutedMessageV2, RoutingTableUpdate,
};
use crate::peer_manager::connection;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::peer_store;
use crate::private_actix::RegisterPeerError;
use crate::routing;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use crate::store;
use crate::time;
use crate::types::{ChainInfo, PeerType, ReasonForBan};
use arc_swap::ArcSwap;
use near_o11y::WithSpanContextExt;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use parking_lot::RwLock;
use rayon::iter::ParallelBridge;
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tracing::{debug, trace};

/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;

/// Send important messages three times.
/// We send these messages multiple times to reduce the chance that they are lost
const IMPORTANT_MESSAGE_RESENT_COUNT: usize = 3;

/// How much time to wait after we send update nonce request before disconnecting.
/// This number should be large to handle pair of nodes with high latency.
const WAIT_ON_TRY_UPDATE_NONCE: time::Duration = time::Duration::seconds(6);
/// If we see an edge between us and other peer, but this peer is not a current connection, wait this
/// timeout and in case it didn't become a connected peer, broadcast edge removal update.
const WAIT_PEER_BEFORE_REMOVE: time::Duration = time::Duration::seconds(6);

impl TryFrom<&PeerInfo> for WhitelistNode {
    type Error = anyhow::Error;
    fn try_from(pi: &PeerInfo) -> anyhow::Result<Self> {
        Ok(Self {
            id: pi.id.clone(),
            addr: if let Some(addr) = pi.addr {
                addr.clone()
            } else {
                anyhow::bail!("addess is missing");
            },
            account_id: pi.account_id.clone(),
        })
    }
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct WhitelistNode {
    id: PeerId,
    addr: SocketAddr,
    account_id: Option<AccountId>,
}

pub(crate) struct NetworkState {
    /// Arbiter for executing mutable operation on NetworkState.
    /// Async methods of NetworkState are not cancellable,
    /// so calling them from, for example, PeerActor is dangerous because
    /// PeerActor can be stopped at any moment.
    /// WARNING: DO NOT spawn infinite futures/background loops on this arbiter,
    /// as it will be automatically closed only when the NetworkState is dropped.
    // TODO(gprusak): make arbiter private, and spawn relevant futures from
    // within the NetworkState async methods, rather than from outside.
    pub arbiter: actix::ArbiterHandle,
    /// PeerManager config.
    pub config: Arc<config::VerifiedConfig>,
    /// When network state has been constructed.
    pub start_time: time::Instant,
    /// GenesisId of the chain.
    pub genesis_id: GenesisId,
    pub client: Arc<dyn client::Client>,
    /// RoutingTableActor, responsible for computing routing table, routing table exchange, etc.
    pub routing_table_addr: actix::Addr<routing::Actor>,

    /// Network-related info about the chain.
    pub chain_info: ArcSwap<ChainInfo>,
    /// AccountsData for TIER1 accounts.
    pub accounts_data: Arc<accounts_data::Cache>,
    /// Connected peers (inbound and outbound) with their full peer information.
    pub tier2: connection::Pool,
    /// Semaphore limiting inflight inbound handshakes.
    pub inbound_handshake_permits: Arc<tokio::sync::Semaphore>,
    /// Peer store that provides read/write access to peers.
    pub peer_store: peer_store::PeerStore,
    /// A graph of the whole NEAR network.
    pub graph: Arc<RwLock<routing::GraphWithCache>>,

    /// View of the Routing table. It keeps:
    /// - routing information - how to route messages
    /// - edges adjacent to my_peer_id
    /// - account id
    /// Full routing table (that currently includes information about all edges in the graph) is now inside Routing Table.
    pub routing_table_view: RoutingTableView,

    /// Shared counter across all PeerActors, which counts number of `RoutedMessageBody::ForwardTx`
    /// messages sincce last block.
    pub txns_since_last_block: AtomicUsize,

    /// Whitelisted nodes, which are allowed to connect even if the connection limit has been
    /// reached.
    whitelist_nodes: Vec<WhitelistNode>,
    /// Maximal allowed number of peer connections.
    /// It is initialized with config.max_num_peers and is mutable
    /// only so that it can be changed in tests.
    /// TODO(gprusak): determine why tests need to change that dynamically
    /// in the first place.
    pub max_num_peers: AtomicCell<u32>,
}

impl Drop for NetworkState {
    fn drop(&mut self) {
        self.arbiter.stop();
    }
}

impl NetworkState {
    pub fn new(
        clock: &time::Clock,
        store: store::Store,
        peer_store: peer_store::PeerStore,
        config: Arc<config::VerifiedConfig>,
        genesis_id: GenesisId,
        client: Arc<dyn client::Client>,
        whitelist_nodes: Vec<WhitelistNode>,
    ) -> Self {
        let graph = Arc::new(RwLock::new(routing::GraphWithCache::new(config.node_id())));
        Self {
            arbiter: actix::Arbiter::new().handle(),
            routing_table_addr: routing::Actor::spawn(clock.clone(), store.clone(), graph.clone()),
            graph,
            genesis_id,
            client,
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            peer_store,
            accounts_data: Arc::new(accounts_data::Cache::new()),
            routing_table_view: RoutingTableView::new(store, config.node_id()),
            txns_since_last_block: AtomicUsize::new(0),
            whitelist_nodes,
            max_num_peers: AtomicCell::new(config.max_num_peers),
            config,
            start_time: clock.now(),
        }
    }

    pub fn propose_edge(&self, peer1: &PeerId, with_nonce: Option<u64>) -> PartialEdgeInfo {
        // When we create a new edge we increase the latest nonce by 2 in case we miss a removal
        // proposal from our partner.
        let nonce = with_nonce.unwrap_or_else(|| {
            self.routing_table_view.get_local_edge(peer1).map_or(1, |edge| edge.next())
        });
        PartialEdgeInfo::new(&self.config.node_id(), peer1, nonce, &self.config.node_key)
    }

    /// Stops peer instance if it is still connected,
    /// and then mark peer as banned in the peer store.
    pub fn disconnect_and_ban(
        &self,
        clock: &time::Clock,
        peer_id: &PeerId,
        ban_reason: ReasonForBan,
    ) {
        let tier2 = self.tier2.load();
        if let Some(peer) = tier2.ready.get(peer_id) {
            peer.stop(Some(ban_reason));
        } else {
            if let Err(err) = self.peer_store.peer_ban(clock, peer_id, ban_reason) {
                tracing::error!(target: "network", ?err, "Failed to save peer data");
            }
        }
    }

    /// is_peer_whitelisted checks whether a peer is a whitelisted node.
    /// whitelisted nodes are allowed to connect, even if the inbound connections limit has
    /// been reached. This predicate should be evaluated AFTER the Handshake.
    pub fn is_peer_whitelisted(&self, peer_info: &PeerInfo) -> bool {
        self.whitelist_nodes
            .iter()
            .filter(|wn| wn.id == peer_info.id)
            .filter(|wn| Some(wn.addr) == peer_info.addr)
            .any(|wn| wn.account_id.is_none() || wn.account_id == peer_info.account_id)
    }

    /// predicate checking whether we should allow an inbound connection from peer_info.
    fn is_inbound_allowed(&self, peer_info: &PeerInfo) -> bool {
        // Check if we have spare inbound connections capacity.
        let tier2 = self.tier2.load();
        if tier2.ready.len() + tier2.outbound_handshakes.len() < self.max_num_peers.load() as usize
            && !self.config.inbound_disabled
        {
            return true;
        }
        // Whitelisted nodes are allowed to connect, even if the inbound connections limit has
        // been reached.
        if self.is_peer_whitelisted(peer_info) {
            return true;
        }
        false
    }

    /// Register a direct connection to a new peer. This will be called after successfully
    /// establishing a connection with another peer. It become part of the connected peers.
    ///
    /// To build new edge between this pair of nodes both signatures are required.
    /// Signature from this node is passed in `edge_info`
    /// Signature from the other node is passed in `full_peer_info.edge_info`.
    pub async fn register(
        &self,
        clock: &time::Clock,
        conn: Arc<connection::Connection>,
    ) -> Result<(), RegisterPeerError> {
        let peer_info = &conn.peer_info;
        // Check if this is a blacklisted peer.
        if peer_info.addr.as_ref().map_or(true, |addr| self.peer_store.is_blacklisted(addr)) {
            tracing::debug!(target: "network", peer_info = ?peer_info, "Dropping connection from blacklisted peer or unknown address");
            return Err(RegisterPeerError::Blacklisted);
        }

        if self.peer_store.is_banned(&peer_info.id) {
            tracing::debug!(target: "network", id = ?peer_info.id, "Dropping connection from banned peer");
            return Err(RegisterPeerError::Banned);
        }

        if conn.peer_type == PeerType::Inbound {
            if !self.is_inbound_allowed(&peer_info) {
                // TODO(1896): Gracefully drop inbound connection for other peer.
                let tier2 = self.tier2.load();
                tracing::debug!(target: "network",
                    tier2 = tier2.ready.len(), outgoing_peers = tier2.outbound_handshakes.len(),
                    max_num_peers = self.max_num_peers.load(),
                    "Dropping handshake (network at max capacity)."
                );
                return Err(RegisterPeerError::ConnectionLimitExceeded);
            }
        }
        // Verify and broadcast the edge of the connection. Only then insert the new
        // connection to TIER2 pool.
        self.add_edges_to_routing_table(&clock, vec![conn.edge.clone()])
            .await
            .map_err(|_| RegisterPeerError::InvalidEdge)?;
        self.tier2.insert_ready(conn.clone()).map_err(RegisterPeerError::PoolError)?;
        // Best effort write to DB.
        if let Err(err) = self.peer_store.peer_connected(clock, peer_info) {
            tracing::error!(target: "network", ?err, "Failed to save peer data");
        }
        Ok(())
    }

    /// Removes the connection from the state.
    pub async fn unregister(
        &self,
        clock: &time::Clock,
        conn: &Arc<connection::Connection>,
        ban_reason: Option<ReasonForBan>,
    ) {
        let peer_id = conn.peer_info.id.clone();
        self.tier2.remove(&peer_id);

        // If the last edge we have with this peer represent a connection addition, create the edge
        // update that represents the connection removal.
        if let Some(edge) = self.routing_table_view.get_local_edge(&peer_id) {
            if edge.edge_type() == EdgeState::Active {
                let edge_update = edge.remove_edge(self.config.node_id(), &self.config.node_key);
                self.add_edges_to_routing_table(clock, vec![edge_update.clone()]).await.unwrap();
                self.broadcast_routing_table_update(Arc::new(RoutingTableUpdate::from_edges(
                    vec![edge_update],
                )))
                .await;
            }
        }

        // Save the fact that we are disconnecting to the PeerStore.
        let res = match ban_reason {
            Some(ban_reason) => self.peer_store.peer_ban(&clock, &conn.peer_info.id, ban_reason),
            None => self.peer_store.peer_disconnected(clock, &conn.peer_info.id),
        };
        if let Err(err) = res {
            tracing::error!(target: "network", ?err, "Failed to save peer data");
        }
    }

    async fn broadcast_routing_table_update(&self, rtu: Arc<RoutingTableUpdate>) {
        let handles: Vec<_> = self
            .tier2
            .load()
            .ready
            .values()
            .map(|conn| conn.send_routing_table_update(rtu.clone()))
            .collect();
        futures_util::future::join_all(handles).await;
    }

    /// Determine if the given target is referring to us.
    pub fn message_for_me(&self, target: &PeerIdOrHash) -> bool {
        let my_peer_id = self.config.node_id();
        match target {
            PeerIdOrHash::PeerId(peer_id) => &my_peer_id == peer_id,
            PeerIdOrHash::Hash(hash) => {
                self.routing_table_view.compare_route_back(*hash, &my_peer_id)
            }
        }
    }

    pub fn send_ping(&self, clock: &time::Clock, nonce: u64, target: PeerId) {
        let body = RoutedMessageBody::Ping(Ping { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(clock, self.sign_message(clock, msg));
    }

    pub fn send_pong(&self, clock: &time::Clock, nonce: u64, target: CryptoHash) {
        let body = RoutedMessageBody::Pong(Pong { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: PeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(clock, self.sign_message(clock, msg));
    }

    pub fn sign_message(&self, clock: &time::Clock, msg: RawRoutedMessage) -> Box<RoutedMessageV2> {
        Box::new(msg.sign(
            &self.config.node_key,
            self.config.routed_message_ttl,
            Some(clock.now_utc()),
        ))
    }

    /// Route signed message to target peer.
    /// Return whether the message is sent or not.
    pub fn send_message_to_peer(&self, clock: &time::Clock, msg: Box<RoutedMessageV2>) -> bool {
        let my_peer_id = self.config.node_id();

        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = &msg.target {
            if target == &my_peer_id {
                debug!(target: "network", account_id = ?self.config.validator.as_ref().map(|v|v.account_id()), ?my_peer_id, ?msg, "Drop signed message to myself");
                metrics::CONNECTED_TO_MYSELF.inc();
                return false;
            }
        }
        match self.routing_table_view.find_route(&clock, &msg.target) {
            Ok(peer_id) => {
                // Remember if we expect a response for this message.
                if msg.author == my_peer_id && msg.expect_response() {
                    trace!(target: "network", ?msg, "initiate route back");
                    self.routing_table_view.add_route_back(&clock, msg.hash(), my_peer_id);
                }
                return self.tier2.send_message(peer_id, Arc::new(PeerMessage::Routed(msg)));
            }
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                metrics::MessageDropped::NoRouteFound.inc(&msg.body);

                debug!(target: "network",
                      account_id = ?self.config.validator.as_ref().map(|v|v.account_id()),
                      to = ?msg.target,
                      reason = ?find_route_error,
                      known_peers = ?self.routing_table_view.reachable_peers(),
                      msg = ?msg.body,
                    "Drop signed message"
                );
                return false;
            }
        }
    }

    /// Send message to specific account.
    /// Return whether the message is sent or not.
    pub fn send_message_to_account(
        &self,
        clock: &time::Clock,
        account_id: &AccountId,
        msg: RoutedMessageBody,
    ) -> bool {
        let target = match self.routing_table_view.account_owner(account_id) {
            Some(peer_id) => peer_id,
            None => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                metrics::MessageDropped::UnknownAccount.inc(&msg);
                debug!(target: "network",
                       account_id = ?self.config.validator.as_ref().map(|v|v.account_id()),
                       to = ?account_id,
                       ?msg,"Drop message: unknown account",
                );
                trace!(target: "network", known_peers = ?self.routing_table_view.get_accounts_keys(), "Known peers");
                return false;
            }
        };

        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body: msg };
        let msg = self.sign_message(clock, msg);
        if msg.body.is_important() {
            let mut success = false;
            for _ in 0..IMPORTANT_MESSAGE_RESENT_COUNT {
                success |= self.send_message_to_peer(clock, msg.clone());
            }
            success
        } else {
            self.send_message_to_peer(clock, msg)
        }
    }

    pub async fn broadcast_accounts(&self, accounts: Vec<AnnounceAccount>) {
        let new_accounts = self.routing_table_view.add_accounts(accounts);
        tracing::debug!(target: "network", account_id = ?self.config.validator.as_ref().map(|v|v.account_id()), ?new_accounts, "Received new accounts");
        if new_accounts.len() > 0 {
            self.broadcast_routing_table_update(Arc::new(RoutingTableUpdate::from_accounts(
                new_accounts,
            )))
            .await;
        }
    }

    /// Sends list of edges, from peer `peer_id` to check their signatures to `EdgeValidatorActor`.
    /// Bans peer `peer_id` if an invalid edge is found.
    /// `PeerManagerActor` periodically runs `broadcast_validated_edges_trigger`, which gets edges
    /// from `EdgeValidatorActor` concurrent queue and sends edges to be added to `RoutingTableActor`.
    pub async fn add_edges_to_routing_table(
        &self,
        clock: &time::Clock,
        mut edges: Vec<Edge>,
    ) -> Result<(), ReasonForBan> {
        {
            let graph = self.graph.read();
            edges.retain(|x| !graph.has(x));
        }
        if edges.is_empty() {
            return Ok(());
        }
        // Verify the edges in parallel on rayon.
        let (edges, ok) = concurrency::rayon::run(move || {
            concurrency::rayon::try_map(edges.into_iter().par_bridge(), |e| {
                if e.verify() {
                    Some(e)
                } else {
                    None
                }
            })
        })
        .await;
        self.routing_table_view.add_local_edges(&edges);

        let resp = match self
            .routing_table_addr
            .send(routing::actor::Message::AddVerifiedEdges { edges }.with_span_context())
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                tracing::error!(target:"network","routing::actor::Actor closed: {err}");
                return Ok(());
            }
        };
        let mut edges = match resp {
            routing::actor::Response::AddVerifiedEdges(edges) => edges,
            _ => panic!("expected AddVerifiedEdges"),
        };
        // Don't send tombstones during the initial time.
        // Most of the network is created during this time, which results
        // in us sending a lot of tombstones to peers.
        // Later, the amount of new edges is a lot smaller.
        if let Some(skip_tombstones_duration) = self.config.skip_tombstones {
            if clock.now() < self.start_time + skip_tombstones_duration {
                edges.retain(|edge| edge.edge_type() == EdgeState::Active);
                metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
            }
        }
        // Broadcast new edges to all other peers.
        if edges.len() > 0 {
            self.broadcast_routing_table_update(Arc::new(RoutingTableUpdate::from_edges(edges)))
                .await;
        }
        if !ok {
            return Err(ReasonForBan::InvalidEdge);
        }
        Ok(())
    }

    pub async fn update_routing_table(
        &self,
        prune_unreachable_since: Option<time::Instant>,
        prune_edges_older_than: Option<time::Utc>,
    ) {
        let resp = match self
            .routing_table_addr
            .send(
                routing::actor::Message::RoutingTableUpdate {
                    prune_unreachable_since,
                    prune_edges_older_than,
                }
                .with_span_context(),
            )
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                // This can happen only when the PeerManagerActor is being shut down.
                tracing::error!(target:"network","routing::actor::Actor: {err}");
                return;
            }
        };
        match resp {
            routing::actor::Response::RoutingTableUpdate { pruned_edges, next_hops } => {
                self.routing_table_view.update(&pruned_edges, next_hops.clone());
                self.config.event_sink.push(Event::RoutingTableUpdate { next_hops, pruned_edges });
            }
            _ => panic!("expected RoutingTableUpdateResponse"),
        }
    }

    /// Check for edges indicating that
    /// a) there is a peer we should be connected to, but we aren't
    /// b) there is an edge indicating that we should be disconnected from a peer, but we are connected.
    /// Try to resolve the inconsistency.
    pub fn fix_local_edges(self: &Arc<Self>) {
        let local_edges = self.routing_table_view.get_local_edges();
        let tier2 = self.tier2.load();
        for edge in local_edges {
            let node_id = self.config.node_id();
            let other_peer = edge.other(&node_id).unwrap();
            match (tier2.ready.get(other_peer), edge.edge_type()) {
                // This is an active connection, while the edge indicates it shouldn't.
                (Some(conn), EdgeState::Removed) => {
                    let this = self.clone();
                    let conn = conn.clone();
                    self.arbiter.spawn(async move {
                        conn.send_message(Arc::new(PeerMessage::RequestUpdateNonce(
                            PartialEdgeInfo::new(
                                &node_id,
                                &conn.peer_info.id,
                                edge.next(),
                                &this.config.node_key,
                            ),
                        )));
                        // TODO(gprusak): here we should synchronically wait for the RequestUpdateNonce
                        // response (with timeout). Until network round trips are implemented, we just
                        // blindly wait for a while, then check again.
                        tokio::time::sleep(WAIT_ON_TRY_UPDATE_NONCE.try_into().unwrap()).await;
                        match this.routing_table_view.get_local_edge(&conn.peer_info.id) {
                            Some(edge) if edge.edge_type() == EdgeState::Active => return,
                            _ => conn.stop(None),
                        }
                    });
                }
                // We are not connected to this peer, but routing table contains
                // information that we do. We should wait and remove that peer
                // from routing table
                (None, EdgeState::Active) => {
                    let this = self.clone();
                    let other_peer = other_peer.clone();
                    self.arbiter.spawn(async move {
                        // This edge says this is an connected peer, which is currently not in the set of connected peers.
                        // Wait for some time to let the connection begin or broadcast edge removal instead.
                        tokio::time::sleep(WAIT_PEER_BEFORE_REMOVE.try_into().unwrap()).await;
                        if this.tier2.load().ready.contains_key(&other_peer) {
                            return;
                        }
                        // Peer is still not connected after waiting a timeout.
                        let new_edge =
                            edge.remove_edge(this.config.node_id(), &this.config.node_key);
                        this.broadcast_routing_table_update(Arc::new(
                            RoutingTableUpdate::from_edges(vec![new_edge]),
                        ))
                        .await;
                    });
                }
                // OK
                _ => {}
            }
        }
    }
}
