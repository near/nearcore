use crate::accounts_data;
use crate::client;
use crate::config;
use crate::network_protocol::{
    Edge, EdgeState, PartialEdgeInfo, PeerIdOrHash, PeerMessage, Ping, Pong, RawRoutedMessage,
    RoutedMessageBody, RoutedMessageV2, RoutingTableUpdate,
};
use crate::peer_manager::connection;
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::peer_store;
use crate::private_actix::{PeerToManagerMsg, ValidateEdgeList};
use crate::routing;
use crate::routing::edge_validator_actor::EdgeValidatorHelper;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use crate::store;
use crate::time;
use crate::types::{ChainInfo, ReasonForBan};
use actix::Recipient;
use arc_swap::ArcSwap;
use near_o11y::{WithSpanContext, WithSpanContextExt};
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use parking_lot::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tracing::{debug, trace, Instrument};

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

struct Runtime {
    handle: tokio::runtime::Handle,
    stop: Arc<tokio::sync::Notify>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Runtime {
    fn new() -> Self {
        let stop = Arc::new(tokio::sync::Notify::new());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let handle = runtime.handle().clone();
        let thread = std::thread::spawn({
            let stop = stop.clone();
            move || runtime.block_on(stop.notified())
        });
        Self { handle, stop, thread: Some(thread) }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop.notify_one();
        let thread = self.thread.take().unwrap();
        // Await for the thread to stop, unless it is the current thread
        // (i.e. nobody waits for it).
        if std::thread::current().id() != thread.thread().id() {
            thread.join().unwrap();
        }
    }
}

pub(crate) struct NetworkState {
    /// Dedicated runtime for `NetworkState` which runs in a separate thread.
    /// Async methods of NetworkState are not cancellable,
    /// so calling them from, for example, PeerActor is dangerous because
    /// PeerActor can be stopped at any moment.
    /// WARNING: DO NOT spawn infinite futures/background loops on this arbiter,
    /// as it will be automatically closed only when the NetworkState is dropped.
    /// WARNING: actix actors can be spawned only when actix::System::current() is set.
    /// DO NOT spawn actors from a task on this runtime.
    runtime: Runtime,
    /// PeerManager config.
    pub config: Arc<config::VerifiedConfig>,
    /// When network state has been constructed.
    pub start_time: time::Instant,
    /// GenesisId of the chain.
    pub genesis_id: GenesisId,
    pub client: Arc<dyn client::Client>,
    /// Address of the peer manager actor.
    pub peer_manager_addr: Recipient<WithSpanContext<PeerToManagerMsg>>,
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
    /// Fields used for communicating with EdgeValidatorActor
    pub routing_table_exchange_helper: EdgeValidatorHelper,

    /// Shared counter across all PeerActors, which counts number of `RoutedMessageBody::ForwardTx`
    /// messages sincce last block.
    pub txns_since_last_block: AtomicUsize,
}

impl NetworkState {
    pub fn new(
        clock: &time::Clock,
        store: store::Store,
        peer_store: peer_store::PeerStore,
        config: Arc<config::VerifiedConfig>,
        genesis_id: GenesisId,
        client: Arc<dyn client::Client>,
        peer_manager_addr: Recipient<WithSpanContext<PeerToManagerMsg>>,
    ) -> Self {
        let graph = Arc::new(RwLock::new(routing::GraphWithCache::new(config.node_id())));
        Self {
            runtime: Runtime::new(),
            routing_table_addr: routing::Actor::spawn(clock.clone(), store.clone(), graph.clone()),
            graph,
            genesis_id,
            client,
            peer_manager_addr,
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            peer_store,
            accounts_data: Arc::new(accounts_data::Cache::new()),
            routing_table_view: RoutingTableView::new(store, config.node_id()),
            routing_table_exchange_helper: Default::default(),
            config,
            txns_since_last_block: AtomicUsize::new(0),
            start_time: clock.now(),
        }
    }

    fn spawn<R: 'static + Send>(
        &self,
        fut: impl std::future::Future<Output = R> + 'static + Send,
    ) -> tokio::task::JoinHandle<R> {
        self.runtime.handle.spawn(fut.in_current_span())
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

    /// Removes the connection from the state.
    pub fn unregister(
        self: &Arc<Self>,
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
                self.add_verified_edges_to_routing_table(clock, vec![edge_update.clone()]);
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

    pub fn add_verified_edges_to_routing_table(
        self: &Arc<Self>,
        clock: &time::Clock,
        edges: Vec<Edge>,
    ) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_view.add_local_edges(&edges);
        let this = self.clone();
        let clock = clock.clone();
        self.spawn(async move {
            match this
                .routing_table_addr
                .send(routing::actor::Message::AddVerifiedEdges { edges }.with_span_context())
                .await
            {
                Ok(routing::actor::Response::AddVerifiedEdgesResponse(mut edges)) => {
                    this.config.event_sink.push(Event::EdgesVerified(edges.clone()));
                    // Don't send tombstones during the initial time.
                    // Most of the network is created during this time, which results
                    // in us sending a lot of tombstones to peers.
                    // Later, the amount of new edges is a lot smaller.
                    if let Some(skip_tombstones_duration) = this.config.skip_tombstones {
                        if clock.now() < this.start_time + skip_tombstones_duration {
                            edges.retain(|e| e.edge_type() == EdgeState::Active);
                            metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
                        }
                    }
                    this.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                        RoutingTableUpdate::from_edges(edges),
                    )));
                }
                _ => tracing::error!(target: "network", "expected AddVerifiedEdgesResponse"),
            }
        });
    }

    pub fn broadcast_accounts(&self, accounts: Vec<AnnounceAccount>) {
        let new_accounts = self.routing_table_view.add_accounts(accounts);
        tracing::debug!(target: "network", account_id = ?self.config.validator.as_ref().map(|v|v.account_id()), ?new_accounts, "Received new accounts");
        if new_accounts.len() > 0 {
            self.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                RoutingTableUpdate::from_accounts(new_accounts),
            )));
        }
    }

    /// Sends list of edges, from peer `peer_id` to check their signatures to `EdgeValidatorActor`.
    /// Bans peer `peer_id` if an invalid edge is found.
    /// `PeerManagerActor` periodically runs `broadcast_validated_edges_trigger`, which gets edges
    /// from `EdgeValidatorActor` concurrent queue and sends edges to be added to `RoutingTableActor`.
    pub fn validate_edges_and_add_to_routing_table(&self, peer_id: PeerId, edges: Vec<Edge>) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_addr.do_send(
            routing::actor::Message::ValidateEdgeList(ValidateEdgeList {
                source_peer_id: peer_id,
                edges,
                edges_info_shared: self.routing_table_exchange_helper.edges_info_shared.clone(),
                sender: self.routing_table_exchange_helper.edges_to_add_sender.clone(),
            })
            .with_span_context(),
        );
    }

    /// Check for edges indicating that
    /// a) there is a peer we should be connected to, but we aren't
    /// b) there is an edge indicating that we should be disconnected from a peer, but we are connected.
    /// Try to resolve the inconsistency.
    /// We call this function every FIX_LOCAL_EDGES_INTERVAL from peer_manager_actor.rs.
    pub async fn fix_local_edges(self: &Arc<Self>, clock: &time::Clock) {
        let local_edges = self.routing_table_view.get_local_edges();
        let tier2 = self.tier2.load();
        let mut tasks = vec![];
        for edge in local_edges {
            let node_id = self.config.node_id();
            let other_peer = edge.other(&node_id).unwrap();
            match (tier2.ready.get(other_peer), edge.edge_type()) {
                // This is an active connection, while the edge indicates it shouldn't.
                (Some(conn), EdgeState::Removed) => {
                    let this = self.clone();
                    let conn = conn.clone();
                    let clock = clock.clone();
                    tasks.push(self.spawn(async move {
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
                        clock.sleep(WAIT_ON_TRY_UPDATE_NONCE).await;
                        match this.routing_table_view.get_local_edge(&conn.peer_info.id) {
                            Some(edge) if edge.edge_type() == EdgeState::Active => return,
                            _ => conn.stop(None),
                        }
                    }));
                }
                // We are not connected to this peer, but routing table contains
                // information that we do. We should wait and remove that peer
                // from routing table
                (None, EdgeState::Active) => {
                    let this = self.clone();
                    let clock = clock.clone();
                    let other_peer = other_peer.clone();
                    tasks.push(self.spawn(async move {
                        // This edge says this is an connected peer, which is currently not in the set of connected peers.
                        // Wait for some time to let the connection begin or broadcast edge removal instead.
                        clock.sleep(WAIT_PEER_BEFORE_REMOVE).await;
                        if this.tier2.load().ready.contains_key(&other_peer) {
                            return;
                        }
                        // Peer is still not connected after waiting a timeout.
                        let new_edge =
                            edge.remove_edge(this.config.node_id(), &this.config.node_key);
                        this.add_verified_edges_to_routing_table(&clock, vec![new_edge.clone()]);
                    }));
                }
                // OK
                _ => {}
            }
        }
        for t in tasks {
            let _ = t.await;
        }
    }
}
