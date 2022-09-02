use crate::accounts_data;
use crate::concurrency::demux;
use crate::config;
use crate::network_protocol::{AccountData, SyncAccountsData};
use crate::peer::codec::Codec;
use crate::peer::peer_actor::{ConnectingStatus, PeerActor};
use crate::peer_manager::connection;
use crate::peer_manager::peer_store::PeerStore;
use crate::private_actix::{
    PeerRequestResult, PeersRequest, RegisterPeer, RegisterPeerResponse, StopMsg, Unregister,
    ValidateEdgeList,
};
use crate::private_actix::{PeerToManagerMsg, PeerToManagerMsgResp, PeersResponse};
use crate::routing;
use crate::routing::edge_validator_actor::EdgeValidatorHelper;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use crate::store;
use crate::types::{
    ChainInfo, ConnectedPeerInfo, FullPeerInfo, GetNetworkInfo, NetworkClientMessages, NetworkInfo,
    NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
    PeerMessage, RoutingTableUpdate, SetChainInfo,
};
use actix::{
    Actor, ActorFutureExt, Addr, Arbiter, AsyncContext, Context, ContextFutureSpawner, Handler,
    Recipient, Running, StreamHandler, WrapFuture,
};
use anyhow::bail;
use anyhow::Context as _;
use arc_swap::ArcSwap;
use near_network_primitives::time;
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, Ban, Edge, InboundTcpConnect, KnownPeerStatus, KnownProducer,
    NetworkViewClientMessages, NetworkViewClientResponses, OutboundTcpConnect, PeerIdOrHash,
    PeerInfo, PeerType, Ping, Pong, RawRoutedMessage, ReasonForBan, RoutedMessageBody,
    RoutedMessageV2, StateResponseInfo,
};
use near_network_primitives::types::{EdgeState, PartialEdgeInfo};
use near_performance_metrics::framed_write::FramedWrite;
use near_performance_metrics_macros::perf;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::{AccountId, EpochId};
use crate::peer::framed_read::{FramedRead};
use parking_lot::RwLock;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use std::cmp::{min};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
//use std::ops::Sub;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, trace, warn, Instrument};

/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: time::Duration = time::Duration::milliseconds(60_000);
/// How much time to wait (in milliseconds) after we send update nonce request before disconnecting.
/// This number should be large to handle pair of nodes with high latency.
const WAIT_ON_TRY_UPDATE_NONCE: time::Duration = time::Duration::milliseconds(6_000);
/// If we see an edge between us and other peer, but this peer is not a current connection, wait this
/// timeout and in case it didn't become a connected peer, broadcast edge removal update.
const WAIT_PEER_BEFORE_REMOVE: time::Duration = time::Duration::milliseconds(6_000);
/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The maximum waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_MAX_DURATION: time::Duration = time::Duration::milliseconds(60_000);
/// The initial waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_INITIAL_DURATION: time::Duration = time::Duration::milliseconds(10);
/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;
/// How ofter should we broadcast edges.
const BROADCAST_VALIDATED_EDGES_INTERVAL: time::Duration = time::Duration::milliseconds(50);
/// Maximum amount of time spend processing edges.
const BROAD_CAST_EDGES_MAX_WORK_ALLOWED: time::Duration = time::Duration::milliseconds(50);
/// Delay syncinc for 1 second to avoid race condition
const WAIT_FOR_SYNC_DELAY: time::Duration = time::Duration::milliseconds(1_000);
/// How often should we update the routing table
const UPDATE_ROUTING_TABLE_INTERVAL: time::Duration = time::Duration::milliseconds(1_000);
/// How often to report bandwidth stats.
const REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL: time::Duration =
    time::Duration::milliseconds(60_000);

/// Max number of messages we received from peer, and they are in progress, before we start throttling.
/// Disabled for now (TODO PUT UNDER FEATURE FLAG)
// const MAX_MESSAGES_COUNT: usize = usize::MAX;
/// Max total size of all messages that are in progress, before we start throttling.
/// Disabled for now (TODO PUT UNDER FEATURE FLAG)
// const MAX_MESSAGES_TOTAL_SIZE: usize = usize::MAX;
/// If we received more than `REPORT_BANDWIDTH_THRESHOLD_BYTES` of data from given peer it's bandwidth stats will be reported.
// const REPORT_BANDWIDTH_THRESHOLD_BYTES: usize = 10_000_000;
/// If we received more than REPORT_BANDWIDTH_THRESHOLD_COUNT` of messages from given peer it's bandwidth stats will be reported.
// const REPORT_BANDWIDTH_THRESHOLD_COUNT: usize = 10_000;
/// How long a peer has to be unreachable, until we prune it from the in-memory graph.
const PRUNE_UNREACHABLE_PEERS_AFTER: time::Duration = time::Duration::hours(1);

// Remove the edges that were created more that this duration ago.
const PRUNE_EDGES_AFTER: time::Duration = time::Duration::minutes(30);
// Don't accept nonces (edges) that are more than this delta from current time.
// This value should be smaller than PRUNE_EDGES_AFTER (otherwise, the might accept the edge and garbage collect it seconds later).
const EDGE_NONCE_MAX_TIME_DELTA: time::Duration = time::Duration::minutes(20);

/// Send important messages three times.
/// We send these messages multiple times to reduce the chance that they are lost
const IMPORTANT_MESSAGE_RESENT_COUNT: usize = 3;

// If a peer is more than these blocks behind (comparing to our current head) - don't route any messages through it.
// We are updating the list of unreliable peers every MONITOR_PEER_MAX_DURATION (60 seconds) - so the current
// horizon value is roughly matching this threshold (if the node is 60 blocks behind, it will take it a while to recover).
// If we set this horizon too low (for example 2 blocks) - we're risking excluding a lot of peers in case of a short
// network issue.
const UNRELIABLE_PEER_HORIZON: u64 = 60;

#[derive(Clone, PartialEq, Eq)]
struct WhitelistNode {
    id: PeerId,
    addr: SocketAddr,
    account_id: Option<AccountId>,
}

impl TryFrom<&PeerInfo> for WhitelistNode {
    type Error = anyhow::Error;
    fn try_from(pi: &PeerInfo) -> anyhow::Result<Self> {
        Ok(Self {
            id: pi.id.clone(),
            addr: if let Some(addr) = pi.addr {
                addr.clone()
            } else {
                bail!("addess is missing");
            },
            account_id: pi.account_id.clone(),
        })
    }
}

pub(crate) struct NetworkState {
    /// PeerManager config.
    pub config: Arc<config::VerifiedConfig>,
    /// GenesisId of the chain.
    pub genesis_id: GenesisId,
    pub send_accounts_data_rl: demux::RateLimit,
    /// Address of the client actor.
    pub client_addr: Recipient<NetworkClientMessages>,
    /// Address of the view client actor.
    pub view_client_addr: Recipient<NetworkViewClientMessages>,

    /// Network-related info about the chain.
    pub chain_info: ArcSwap<ChainInfo>,
    /// AccountsData for TIER1 accounts.
    pub accounts_data: Arc<accounts_data::Cache>,
    /// Connected peers (inbound and outbound) with their full peer information.
    pub tier2: connection::Pool,
    /// Semaphore limiting inflight inbound handshakes.
    pub inbound_handshake_permits: Arc<tokio::sync::Semaphore>,

    /// View of the Routing table. It keeps:
    /// - routing information - how to route messages
    /// - edges adjacent to my_peer_id
    /// - account id
    /// Full routing table (that currently includes information about all edges in the graph) is now inside Routing Table.
    pub routing_table_view: RoutingTableView,

    /// Shared counter across all PeerActors, which counts number of `RoutedMessageBody::ForwardTx`
    /// messages sincce last block.
    pub txns_since_last_block: AtomicUsize,
}

impl NetworkState {
    pub fn new(
        config: Arc<config::VerifiedConfig>,
        genesis_id: GenesisId,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
        routing_table_view: RoutingTableView,
        send_accounts_data_rl: demux::RateLimit,
    ) -> Self {
        Self {
            genesis_id,
            client_addr,
            view_client_addr,
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            accounts_data: Arc::new(accounts_data::Cache::new()),
            routing_table_view,
            send_accounts_data_rl,
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            config,
            txns_since_last_block: AtomicUsize::new(0),
        }
    }

    /// Query connected peers for more peers.
    pub fn ask_for_more_peers(&self, clock: &time::Clock) {
        let now = clock.now();
        let msg = Arc::new(PeerMessage::PeersRequest);
        for peer in self.tier2.load().ready.values() {
            if now > peer.last_time_peer_requested.load() + REQUEST_PEERS_INTERVAL {
                peer.send_message(msg.clone());
            }
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

    // Determine if the given target is referring to us.
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
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(clock, msg);
    }

    pub fn send_pong(&self, clock: &time::Clock, nonce: u64, target: CryptoHash) {
        let body = RoutedMessageBody::Pong(Pong { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(clock, msg);
    }

    /// Route message to target peer.
    /// Return whether the message is sent or not.
    pub fn send_message_to_peer(&self, clock: &time::Clock, msg: RawRoutedMessage) -> bool {
        self.send_signed_message_to_peer(
            clock,
            Box::new(msg.sign(
                self.config.node_id(),
                &self.config.node_key,
                self.config.routed_message_ttl,
                Some(clock.now_utc()),
            )),
        )
    }

    /// Route signed message to target peer.
    /// Return whether the message is sent or not.
    pub fn send_signed_message_to_peer(
        &self,
        clock: &time::Clock,
        msg: Box<RoutedMessageV2>,
    ) -> bool {
        let my_peer_id = self.config.node_id();

        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = &msg.msg.target {
            if target == &my_peer_id {
                debug!(target: "network", account_id = ?self.config.validator.as_ref().map(|v|v.account_id()), ?my_peer_id, ?msg, "Drop signed message to myself");
                metrics::CONNECTED_TO_MYSELF.inc();
                return false;
            }
        }

        match self.routing_table_view.find_route(&clock, &msg.target) {
            Ok(peer_id) => {
                // Remember if we expect a response for this message.
                if msg.msg.author == my_peer_id && msg.expect_response() {
                    trace!(target: "network", ?msg, "initiate route back");
                    self.routing_table_view.add_route_back(&clock, msg.hash(), my_peer_id);
                }
                self.tier2.send_message(peer_id, Arc::new(PeerMessage::Routed(msg)))
            }
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                metrics::MessageDropped::NoRouteFound.inc(&msg.msg.body);

                debug!(target: "network",
                      account_id = ?self.config.validator.as_ref().map(|v|v.account_id()),
                      to = ?msg.msg.target,
                      reason = ?find_route_error,
                      known_peers = ?self.routing_table_view.reachable_peers(),
                      msg = ?msg.msg.body,
                    "Drop signed message"
                );
                false
            }
        }
    }
}

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    clock: time::Clock,
    /// Networking configuration.
    /// TODO(gprusak): this field is duplicated with
    /// NetworkState.config. Remove it from here.
    config: Arc<config::VerifiedConfig>,
    /// Maximal allowed number of peer connections.
    /// It is initialized with config.max_num_peers and is mutable
    /// only so that it can be changed in tests.
    /// TODO(gprusak): determine why tests need to change that dynamically
    /// in the first place.
    max_num_peers: u32,
    /// Peer information for this node.
    my_peer_id: PeerId,
    /// Peer store that provides read/write access to peers.
    peer_store: PeerStore,
    /// A graph of the whole NEAR network, shared between routing::Actor
    /// and PeerManagerActor. PeerManagerActor should have read-only access to the graph.
    /// TODO: this is an intermediate step towards replacing actix runtime with a
    /// generic threadpool (or multiple pools) in the near-network crate.
    /// It the threadpool setup, inevitably some of the state will be shared.
    network_graph: Arc<RwLock<routing::GraphWithCache>>,
    /// Fields used for communicating with EdgeValidatorActor
    routing_table_exchange_helper: EdgeValidatorHelper,
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,
    /// Connected peers we have sent new edge update, but we haven't received response so far.
    local_peer_pending_update_nonce_request: HashMap<PeerId, u64>,
    /// RoutingTableActor, responsible for computing routing table, routing table exchange, etc.
    routing_table_addr: Addr<routing::Actor>,
    /// Whitelisted nodes, which are allowed to connect even if the connection limit has been
    /// reached.
    whitelist_nodes: Vec<WhitelistNode>,

    pub(crate) state: Arc<NetworkState>,
}

/// TEST-ONLY
/// A generic set of events (observable in tests) that the Network may generate.
/// Ideally the tests should observe only public API properties, but until
/// we are at that stage, feel free to add any events that you need to observe.
/// In particular prefer emitting a new event to polling for a state change.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Event {
    ServerStarted,
    RoutedMessageDropped,
    RoutingTableUpdate(Arc<routing::NextHopTable>),
    PeerRegistered(PeerInfo),
    Ping(Ping),
    Pong(Pong),
    SetChainInfo,
    // Reported once a message has been processed.
    // In contrast to typical RPC protocols, many P2P messages do not trigger
    // sending a response at the end of processing.
    // However, for precise instrumentation in tests it is useful to know when
    // processing has been finished. We simulate the "RPC response" by reporting
    // an event MessageProcessed.
    //
    // Given that processing is asynchronous and unstructured as of now,
    // it is hard to pinpoint all the places when the processing of a message is
    // actually complete. Currently this event is reported only for some message types,
    // feel free to add support for more.
    MessageProcessed(PeerMessage),
    // Reported when the actix actor has been stopped.
    PeerActorStopped,
}

impl Actor for PeerManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start server if address provided.
        if let Some(server_addr) = self.config.node_addr {
            debug!(target: "network", at = ?server_addr, "starting public server");
            let peer_manager_addr = ctx.address();
            let state = self.state.clone();

            ctx.spawn(
                async move {
                    let listener = match TcpListener::bind(server_addr).await {
                        Ok(it) => it,
                        Err(e) => {
                            panic!(
                                "failed to start listening on server_addr={:?} e={:?}",
                                server_addr, e
                            );
                        }
                    };
                    state.config.event_sink.push(Event::ServerStarted);
                    loop {
                        if let Ok((conn, client_addr)) = listener.accept().await {
                            peer_manager_addr.do_send(PeerToManagerMsg::InboundTcpConnect(
                                InboundTcpConnect::new(conn),
                            ));
                            debug!(target: "network", from = ?client_addr, "got new connection");
                        }
                    }
                }
                .into_actor(self),
            );
        }

        // Periodically push network information to client.
        self.push_network_info_trigger(ctx, self.config.push_info_period);

        // Periodically starts peer monitoring.
        let max_interval = min(MONITOR_PEERS_MAX_DURATION, self.config.bootstrap_peers_period);
        debug!(target: "network", ?max_interval, "monitor_peers_trigger");
        self.monitor_peers_trigger(
            ctx,
            MONITOR_PEERS_INITIAL_DURATION,
            (MONITOR_PEERS_INITIAL_DURATION, max_interval),
        );

        let skip_tombstones = self.config.skip_tombstones.map(|it| self.clock.now() + it);

        // Periodically reads valid edges from `EdgesVerifierActor` and broadcast.
        self.broadcast_validated_edges_trigger(
            ctx,
            BROADCAST_VALIDATED_EDGES_INTERVAL,
            skip_tombstones,
        );

        // Periodically updates routing table and prune edges that are no longer reachable.
        self.update_routing_table_trigger(ctx, UPDATE_ROUTING_TABLE_INTERVAL);

        // Periodically prints bandwidth stats for each peer.
        self.report_bandwidth_stats_trigger(ctx, REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL);
    }

    /// Try to gracefully disconnect from connected peers.
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        warn!("PeerManager: stopping");
        self.state.tier2.broadcast_message(Arc::new(PeerMessage::Disconnect));
        self.routing_table_addr.do_send(StopMsg {});
        Running::Stop
    }
}

impl PeerManagerActor {
    pub fn new(
        clock: time::Clock,
        store: Arc<dyn near_store::db::Database>,
        config: config::NetworkConfig,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
        genesis_id: GenesisId,
    ) -> anyhow::Result<Self> {
        let config = config.verify().context("config")?;
        let store = store::Store::from(store);
        let peer_store = PeerStore::new(
            &clock,
            store.clone(),
            &config.boot_nodes,
            config.blacklist.clone(),
            config.connect_only_to_boot_nodes,
        )
        .map_err(|e| anyhow::Error::msg(e.to_string()))?;
        debug!(target: "network",
               len = peer_store.len(),
               boot_nodes = config.boot_nodes.len(),
               banned = peer_store.count_banned(),
               "Found known peers");
        debug!(target: "network", blacklist = ?config.blacklist, "Blacklist");

        let my_peer_id = config.node_id();
        let network_graph = Arc::new(RwLock::new(routing::GraphWithCache::new(my_peer_id.clone())));
        let arbiter = Arbiter::new();
        let routing_table_addr = routing::Actor::start_in_arbiter(&arbiter.handle(), {
            let clock = clock.clone();
            let store = store.clone();
            let network_graph = network_graph.clone();
            move |_ctx| routing::Actor::new(clock, store, network_graph)
        });

        let whitelist_nodes = {
            let mut v = vec![];
            for wn in &config.whitelist_nodes {
                v.push(wn.try_into()?);
            }
            v
        };
        let config = Arc::new(config);
        let rl = config.accounts_data_broadcast_rate_limit;
        Ok(Self {
            clock,
            my_peer_id: my_peer_id.clone(),
            config: config.clone(),
            max_num_peers: config.max_num_peers,
            peer_store,
            network_graph,
            routing_table_exchange_helper: Default::default(),
            started_connect_attempts: false,
            local_peer_pending_update_nonce_request: HashMap::new(),
            routing_table_addr,
            whitelist_nodes,
            state: Arc::new(NetworkState::new(
                config.clone(),
                genesis_id,
                client_addr,
                view_client_addr,
                RoutingTableView::new(store, my_peer_id.clone()),
                rl,
            )),
        })
    }

    fn update_routing_table(
        &self,
        ctx: &mut Context<Self>,
        prune_unreachable_since: Option<time::Instant>,
        prune_edges_older_than: Option<time::Utc>,
    ) {
        self.routing_table_addr
            .send(routing::actor::Message::RoutingTableUpdate {
                prune_unreachable_since,
                prune_edges_older_than,
            })
            .into_actor(self)
            .map(|response, act, _ctx| match response {
                Ok(routing::actor::Response::RoutingTableUpdateResponse {
                    local_edges_to_remove,
                    next_hops,
                    peers_to_ban,
                }) => {
                    act.state.routing_table_view.update(&local_edges_to_remove, next_hops.clone());
                    for peer in peers_to_ban {
                        act.ban_peer(&peer, ReasonForBan::InvalidEdge);
                    }
                    act.config.event_sink.push(Event::RoutingTableUpdate(next_hops));
                }
                _ => error!(target: "network", "expected RoutingTableUpdateResponse"),
            })
            .spawn(ctx);
    }

    fn add_verified_edges_to_routing_table(&mut self, edges: Vec<Edge>) {
        if edges.is_empty() {
            return;
        }
        self.state.routing_table_view.add_local_edges(&edges);
        self.routing_table_addr.do_send(routing::actor::Message::AddVerifiedEdges { edges });
    }

    fn broadcast_accounts(&mut self, accounts: Vec<AnnounceAccount>) {
        let new_accounts = self.state.routing_table_view.add_accounts(accounts);
        debug!(target: "network", account_id = ?self.config.validator.as_ref().map(|v|v.account_id()), ?new_accounts, "Received new accounts");
        if new_accounts.len() > 0 {
            self.state.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                RoutingTableUpdate::from_accounts(new_accounts),
            )));
        }
    }

    /// `update_routing_table_trigger` schedule updating routing table to `RoutingTableActor`
    /// Usually we do edge pruning once per hour. However it may be disabled in following cases:
    /// - there are edges, that were supposed to be added, but are still in EdgeValidatorActor,
    ///   waiting to have their signatures checked.
    /// - edge pruning may be disabled for unit testing.
    fn update_routing_table_trigger(&self, ctx: &mut Context<Self>, interval: time::Duration) {
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["update_routing_table"])
            .start_timer();
        self.update_routing_table(
            ctx,
            self.clock.now().checked_sub(PRUNE_UNREACHABLE_PEERS_AFTER),
            self.clock.now_utc().checked_sub(PRUNE_EDGES_AFTER),
        );

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.update_routing_table_trigger(ctx, interval);
            },
        );
    }

    /// Periodically prints bandwidth stats for each peer.
    fn report_bandwidth_stats_trigger(&mut self, ctx: &mut Context<Self>, every: time::Duration) {
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["report_bandwidth_stats"])
            .start_timer();
        let total_bandwidth_used_by_all_peers: usize = 0;
        let total_msg_received_count: usize = 0;
        let max_max_record_num_messages_in_progress: usize = 0;
        for (_peer_id, _connected_peer) in &self.state.tier2.load().ready {
            /*if bandwidth_used > REPORT_BANDWIDTH_THRESHOLD_BYTES
                || total_msg_received_count > REPORT_BANDWIDTH_THRESHOLD_COUNT
            {
                debug!(target: "bandwidth",
                    ?peer_id,
                    bandwidth_used, msg_received_count, "Peer bandwidth exceeded threshold",
                );
            }
            total_bandwidth_used_by_all_peers += bandwidth_used;
            total_msg_received_count += msg_received_count;
            max_max_record_num_messages_in_progress =
                max(max_max_record_num_messages_in_progress, max_record);
            */
        }

        info!(
            target: "bandwidth",
            total_bandwidth_used_by_all_peers,
            total_msg_received_count, max_max_record_num_messages_in_progress, "Bandwidth stats"
        );

        near_performance_metrics::actix::run_later(
            ctx,
            every.try_into().unwrap(),
            move |act, ctx| {
                act.report_bandwidth_stats_trigger(ctx, every);
            },
        );
    }

    /// Receives list of edges that were verified, in a trigger every 20ms, and adds them to
    /// the routing table.
    fn broadcast_validated_edges_trigger(
        &mut self,
        ctx: &mut Context<Self>,
        interval: time::Duration,
        // If set, don't push any tombstones until this time.
        skip_tombstones_until: Option<time::Instant>,
    ) {
        let _span =
            tracing::trace_span!(target: "network", "broadcast_validated_edges_trigger").entered();
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["broadcast_validated_edges"])
            .start_timer();
        let start = self.clock.now();
        let mut new_edges = Vec::new();
        while let Some(edge) = self.routing_table_exchange_helper.edges_to_add_receiver.pop() {
            new_edges.push(edge);
            // TODO: do we really need this limit?
            if self.clock.now() >= start + BROAD_CAST_EDGES_MAX_WORK_ALLOWED {
                break;
            }
        }

        if !new_edges.is_empty() {
            // Check whenever there is an edge indicating whenever there is a peer we should be
            // connected to but we aren't. And try to resolve the inconsistency.
            // Also check whenever there is an edge indicating that we should be disconnected
            // from a peer, but we are connected. And try to resolve the inconsistency.
            let new_local_edges = self.state.routing_table_view.add_local_edges(&new_edges);
            let tier2 = self.state.tier2.load();
            for edge in new_local_edges {
                let other_peer = edge.other(&self.my_peer_id).unwrap();
                match (tier2.ready.contains_key(other_peer), edge.edge_type()) {
                    // This is an active connection, while the edge indicates it shouldn't.
                    (true, EdgeState::Removed) => {
                        self.maybe_remove_connected_peer(ctx, &edge, other_peer)
                    }
                    // We are not connected to this peer, but routing table contains
                    // information that we do. We should wait and remove that peer
                    // from routing table
                    (false, EdgeState::Active) => Self::wait_peer_or_remove(ctx, edge),
                    // OK
                    _ => {}
                }
            }
            self.routing_table_addr
                .send(routing::actor::Message::AddVerifiedEdges { edges: new_edges })
                .in_current_span()
                .into_actor(self)
                .map(move |response, act, _ctx| {
                    let _span = tracing::trace_span!(
                        target: "network",
                        "broadcast_validated_edges_trigger_response")
                    .entered();

                    match response {
                        Ok(routing::actor::Response::AddVerifiedEdgesResponse(
                            mut filtered_edges,
                        )) => {
                            // Don't send tombstones during the initial time.
                            // Most of the network is created during this time, which results
                            // in us sending a lot of tombstones to peers.
                            // Later, the amount of new edges is a lot smaller.
                            if let Some(skip_tombstones_until) = skip_tombstones_until {
                                if act.clock.now() < skip_tombstones_until {
                                    filtered_edges
                                        .retain(|edge| edge.edge_type() == EdgeState::Active);
                                    metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
                                }
                            }
                            // Broadcast new edges to all other peers.
                            act.state.tier2.broadcast_message(Arc::new(
                                PeerMessage::SyncRoutingTable(RoutingTableUpdate::from_edges(
                                    filtered_edges,
                                )),
                            ));
                        }
                        _ => error!(target: "network", "expected AddVerifiedEdgesResponse"),
                    }
                })
                .spawn(ctx);
        };

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.broadcast_validated_edges_trigger(ctx, interval, skip_tombstones_until);
            },
        );
    }

    /// Register a direct connection to a new peer. This will be called after successfully
    /// establishing a connection with another peer. It become part of the connected peers.
    ///
    /// To build new edge between this pair of nodes both signatures are required.
    /// Signature from this node is passed in `edge_info`
    /// Signature from the other node is passed in `full_peer_info.edge_info`.
    fn register_peer(
        &mut self,
        connection: Arc<connection::Connection>,
        partial_edge_info: PartialEdgeInfo,
        ctx: &mut Context<Self>,
    ) -> Result<(), connection::PoolError> {
        let peer_info = &connection.peer_info;
        let _span = tracing::trace_span!(target: "network", "register_peer").entered();
        debug!(target: "network", ?peer_info, "Consolidated connection");

        let target_peer_id = peer_info.id.clone();

        let new_edge = Edge::new(
            self.my_peer_id.clone(),
            target_peer_id.clone(),
            partial_edge_info.nonce,
            partial_edge_info.signature,
            connection.partial_edge_info.signature.clone(),
        );

        self.state.tier2.insert_ready(connection.clone())?;
        self.add_verified_edges_to_routing_table(vec![new_edge.clone()]);
        // Best effort write to DB.
        if let Err(err) = self.peer_store.peer_connected(&self.clock, peer_info) {
            error!(target: "network", ?err, "Failed to save peer data");
        }
        self.sync_after_handshake(connection.clone(), ctx, new_edge);
        self.config.event_sink.push(Event::PeerRegistered(peer_info.clone()));
        Ok(())
    }

    fn sync_after_handshake(
        &self,
        peer: Arc<connection::Connection>,
        ctx: &mut Context<Self>,
        new_edge: Edge,
    ) {
        let run_later_span = tracing::trace_span!(target: "network", "sync_after_handshake");
        // The full sync is delayed, so that handshake is completed before the sync starts.
        near_performance_metrics::actix::run_later(
            ctx,
            WAIT_FOR_SYNC_DELAY.try_into().unwrap(),
            move |act, _ctx| {
                let _guard = run_later_span.enter();
                // Start syncing network point of view. Wait until both parties are connected before start
                // sending messages.
                let mut known_edges: Vec<Edge> =
                    act.network_graph.read().edges().values().cloned().collect();
                if act.config.skip_tombstones.is_some() {
                    known_edges.retain(|edge| edge.removal_info().is_none());
                    metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
                }
                let known_accounts = act.state.routing_table_view.get_announce_accounts();
                peer.send_message(Arc::new(PeerMessage::SyncRoutingTable(
                    RoutingTableUpdate::new(known_edges, known_accounts),
                )));

                // Ask for peers list on connection.
                peer.send_message(Arc::new(PeerMessage::PeersRequest));

                if peer.peer_type == PeerType::Outbound {
                    // Only broadcast new message from the outbound endpoint.
                    // Wait a time out before broadcasting this new edge to let the other party finish handshake.
                    act.state.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                        RoutingTableUpdate::from_edges(vec![new_edge]),
                    )));
                }
            },
        );
    }

    /// Remove peer from connected set.
    /// Check it match peer_type to avoid removing a peer that both started connection to each other.
    /// If peer_type is None, remove anyway disregarding who started the connection.
    fn remove_connected_peer(&mut self, peer_id: &PeerId, peer_type: Option<PeerType>) {
        let state = self.state.clone();
        let tier2 = state.tier2.load();
        if let Some(peer_type) = peer_type {
            if let Some(peer) = tier2.ready.get(peer_id) {
                if peer.peer_type != peer_type {
                    // Don't remove the peer
                    return;
                }
            }
        }

        // If the last edge we have with this peer represent a connection addition, create the edge
        // update that represents the connection removal.
        self.state.tier2.remove(peer_id);

        if let Some(edge) = self.state.routing_table_view.get_local_edge(peer_id) {
            if edge.edge_type() == EdgeState::Active {
                let edge_update = edge.remove_edge(self.my_peer_id.clone(), &self.config.node_key);
                self.add_verified_edges_to_routing_table(vec![edge_update.clone()]);
                self.state.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                    RoutingTableUpdate::from_edges(vec![edge_update]),
                )));
            }
        }
    }

    /// Remove a peer from the connected peer set. If the peer doesn't belong to the connected peer set
    /// data from ongoing connection established is removed.
    fn unregister_peer(
        &mut self,
        peer_id: PeerId,
        peer_type: PeerType,
        remove_from_peer_store: bool,
    ) {
        debug!(target: "network", ?peer_id, ?peer_type, "Unregister peer");
        if remove_from_peer_store {
            self.remove_connected_peer(&peer_id, Some(peer_type));
            if let Err(err) = self.peer_store.peer_disconnected(&self.clock, &peer_id) {
                error!(target: "network", ?err, "Failed to save peer data");
            };
        }
    }

    /// Add peer to ban list.
    /// This function should only be called after Peer instance is stopped.
    /// Note: Use `try_ban_peer` if there might be a Peer instance still connected.
    fn ban_peer(&mut self, peer_id: &PeerId, ban_reason: ReasonForBan) {
        warn!(target: "network", ?peer_id, ?ban_reason, "Banning peer");
        self.remove_connected_peer(peer_id, None);
        if let Err(err) = self.peer_store.peer_ban(&self.clock, peer_id, ban_reason) {
            error!(target: "network", ?err, "Failed to save peer data");
        };
    }

    /// Ban peer. Stop peer instance if it is still connected,
    /// and then mark peer as banned in the peer store.
    pub(crate) fn try_ban_peer(&mut self, peer_id: &PeerId, ban_reason: ReasonForBan) {
        let state = self.state.clone();
        if let Some(peer) = state.tier2.load().ready.get(peer_id) {
            peer.ban(ban_reason);
        } else {
            warn!(target: "network", ?ban_reason, ?peer_id, "Try to ban a disconnected peer for");
            // Call `ban_peer` in peer manager to trigger action that persists information
            // of ban in disk.
            self.ban_peer(peer_id, ban_reason);
        };
    }

    /// Connects peer with given TcpStream and optional information if it's outbound.
    /// This might fail if the other peers drop listener at its endpoint while establishing connection.
    fn try_connect_peer(
        &self,
        recipient: Addr<Self>,
        stream: TcpStream,
        connecting_status: ConnectingStatus,
        peer_info: Option<PeerInfo>,
    ) {
        let my_peer_id = self.my_peer_id.clone();
        let account_id = self.config.validator.as_ref().map(|v| v.account_id());
        let server_addr = self.config.node_addr;

        let server_addr = match server_addr {
            Some(server_addr) => server_addr,
            None => match stream.local_addr() {
                Ok(server_addr) => server_addr,
                _ => {
                    warn!(target: "network", ?peer_info, "Failed establishing connection with");
                    return;
                }
            },
        };

        let remote_addr = match stream.peer_addr() {
            Ok(remote_addr) => remote_addr,
            _ => {
                warn!(target: "network", ?peer_info, "Failed establishing connection with");
                return;
            }
        };

        // Start every peer actor on separate thread.
        let arbiter = Arbiter::new();
        let clock = self.clock.clone();
        let state = self.state.clone();
        PeerActor::start_in_arbiter(&arbiter.handle(), move |ctx| {
            let (read, write) = tokio::io::split(stream);

            // TODO: check if peer is banned or known based on IP address and port.
            PeerActor::add_stream(
                FramedRead::new(read).map_while(
                    |x| match x {
                        Ok(x) => Some(x),
                        Err(err) => {
                            warn!(target: "network", ?err, "Peer stream error");
                            None
                        }
                    },
                ),
                ctx,
            );

            PeerActor::new(
                clock,
                PeerInfo { id: my_peer_id, addr: Some(server_addr), account_id },
                remote_addr,
                peer_info,
                connecting_status,
                FramedWrite::new(write, Codec::default(), Codec::default(), ctx),
                recipient.clone().recipient(),
                None,
                state,
            )
        });
    }

    /// Check if it is needed to create a new outbound connection.
    /// If the number of active connections is less than `ideal_connections_lo` or
    /// (the number of outgoing connections is less than `minimum_outbound_peers`
    ///     and the total connections is less than `max_num_peers`)
    fn is_outbound_bootstrap_needed(&self) -> bool {
        let tier2 = self.state.tier2.load();
        let total_connections = tier2.ready.len() + tier2.outbound_handshakes.len();
        let potential_outbound_connections =
            tier2.ready.values().filter(|peer| peer.peer_type == PeerType::Outbound).count()
                + tier2.outbound_handshakes.len();

        (total_connections < self.config.ideal_connections_lo as usize
            || (total_connections < self.max_num_peers as usize
                && potential_outbound_connections < self.config.minimum_outbound_peers as usize))
            && !self.config.outbound_disabled
    }

    fn is_inbound_allowed(&self) -> bool {
        let tier2 = self.state.tier2.load();
        tier2.ready.len() + tier2.outbound_handshakes.len() < self.max_num_peers as usize
            && !self.config.inbound_disabled
    }

    /// is_peer_whitelisted checks whether a peer is a whitelisted node.
    /// whitelisted nodes are allowed to connect, even if the inbound connections limit has
    /// been reached. This predicate should be evaluated AFTER the Handshake.
    fn is_peer_whitelisted(&self, peer_info: &PeerInfo) -> bool {
        self.whitelist_nodes
            .iter()
            .filter(|wn| wn.id == peer_info.id)
            .filter(|wn| Some(wn.addr) == peer_info.addr)
            .any(|wn| wn.account_id.is_none() || wn.account_id == peer_info.account_id)
    }

    /// is_ip_whitelisted checks whether the IP address of an inbound
    /// connection may belong to a whitelisted node. All whitelisted nodes
    /// are required to have IP:port specified. We consider only IPs since
    /// the port of an inbound TCP connection is assigned at random.
    /// This predicate should be evaluated BEFORE the Handshake.
    fn is_ip_whitelisted(&self, ip: &IpAddr) -> bool {
        self.whitelist_nodes.iter().any(|wn| wn.addr.ip() == *ip)
    }

    /// Returns peers close to the highest height
    fn highest_height_peers(&self) -> Vec<FullPeerInfo> {
        let infos: Vec<_> =
            self.state.tier2.load().ready.values().map(|p| p.full_peer_info()).collect();

        // This finds max height among peers, and returns one peer close to such height.
        let max_height = match infos.iter().map(|i| i.chain_info.height).max() {
            Some(height) => height,
            None => return vec![],
        };
        // Find all peers whose height is within `highest_peer_horizon` from max height peer(s).
        infos
            .into_iter()
            .filter(|i| {
                i.chain_info.height.saturating_add(self.config.highest_peer_horizon) >= max_height
            })
            .collect()
    }

    // Get peers that are potentially unreliable and we should avoid routing messages through them.
    // Currently we're picking the peers that are too much behind (in comparison to us).
    fn unreliable_peers(&self) -> HashSet<PeerId> {
        let my_height = self.state.chain_info.load().height;
        // Find all peers whose height is below `highest_peer_horizon` from max height peer(s).
        self.state
            .tier2
            .load()
            .ready
            .values()
            .filter(|p| {
                p.chain_height.load(Ordering::Relaxed).saturating_add(UNRELIABLE_PEER_HORIZON)
                    < my_height
            })
            .map(|p| p.peer_info.id.clone())
            .collect()
    }

    fn wait_peer_or_remove(ctx: &mut Context<Self>, edge: Edge) {
        // This edge says this is an connected peer, which is currently not in the set of connected peers.
        // Wait for some time to let the connection begin or broadcast edge removal instead.
        near_performance_metrics::actix::run_later(
            ctx,
            WAIT_PEER_BEFORE_REMOVE.try_into().unwrap(),
            move |act, _ctx| {
                let other = edge.other(&act.my_peer_id).unwrap();
                if act.state.tier2.load().ready.contains_key(other) {
                    return;
                }
                // Peer is still not connected after waiting a timeout.
                let new_edge = edge.remove_edge(act.my_peer_id.clone(), &act.config.node_key);
                act.state.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                    RoutingTableUpdate::from_edges(vec![new_edge]),
                )));
            },
        );
    }

    // If we receive an edge indicating that we should no longer be connected to a peer.
    // We will broadcast that edge to that peer, and if that peer doesn't reply within specific time,
    // that peer will be removed. However, the connected peer may gives us a new edge indicating
    // that we should in fact be connected to it.
    fn maybe_remove_connected_peer(
        &mut self,
        ctx: &mut Context<Self>,
        edge: &Edge,
        other: &PeerId,
    ) {
        let nonce = edge.next();

        if let Some(last_nonce) = self.local_peer_pending_update_nonce_request.get(other) {
            if *last_nonce >= nonce {
                // We already tried to update an edge with equal or higher nonce.
                return;
            }
        }

        self.state.tier2.send_message(
            other.clone(),
            Arc::new(PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
                &self.my_peer_id,
                other,
                nonce,
                &self.config.node_key,
            ))),
        );

        self.local_peer_pending_update_nonce_request.insert(other.clone(), nonce);

        let other = other.clone();
        near_performance_metrics::actix::run_later(
            ctx,
            WAIT_ON_TRY_UPDATE_NONCE.try_into().unwrap(),
            move |act, _ctx| {
                if let Some(cur_nonce) = act.local_peer_pending_update_nonce_request.get(&other) {
                    if *cur_nonce == nonce {
                        if let Some(peer) = act.state.tier2.load().ready.get(&other) {
                            // Send disconnect signal to this peer if we haven't edge update.
                            peer.unregister();
                        }
                        act.local_peer_pending_update_nonce_request.remove(&other);
                    }
                }
            },
        );
    }

    /// Check if the number of connections (excluding whitelisted ones) exceeds ideal_connections_hi.
    /// If so, constructs a safe set of peers and selects one random peer outside of that set
    /// and sends signal to stop connection to it gracefully.
    ///
    /// Safe set contruction process:
    /// 1. Add all whitelisted peers to the safe set.
    /// 2. If the number of outbound connections is less or equal than minimum_outbound_connections,
    ///    add all outbound connections to the safe set.
    /// 3. Find all peers who sent us a message within the last peer_recent_time_window,
    ///    and add them one by one to the safe_set (starting from earliest connection time)
    ///    until safe set has safe_set_size elements.
    fn maybe_stop_active_connection(&self) {
        let tier2 = self.state.tier2.load();
        let filter_peers = |predicate: &dyn Fn(&connection::Connection) -> bool| -> Vec<_> {
            tier2
                .ready
                .values()
                .filter(|peer| predicate(&*peer))
                .map(|peer| peer.peer_info.id.clone())
                .collect()
        };

        // Build safe set
        let mut safe_set = HashSet::new();

        // If there is not enough non-whitelisted peers, return without disconnecting anyone.
        let whitelisted_peers = filter_peers(&|p| self.is_peer_whitelisted(&p.peer_info));
        if tier2.ready.len() - whitelisted_peers.len() <= self.config.ideal_connections_hi as usize
        {
            return;
        }
        // Add whitelisted nodes to the safe set.
        safe_set.extend(whitelisted_peers);

        // If there is not enough outbound peers, add them to the safe set.
        let outbound_peers = filter_peers(&|p| p.peer_type == PeerType::Outbound);
        if outbound_peers.len() + tier2.outbound_handshakes.len()
            <= self.config.minimum_outbound_peers as usize
        {
            safe_set.extend(outbound_peers);
        }

        // If there is not enough archival peers, add them to the safe set.
        if self.config.archive {
            let archival_peers = filter_peers(&|p| p.initial_chain_info.archival);
            if archival_peers.len() <= self.config.archival_peer_connections_lower_bound as usize {
                safe_set.extend(archival_peers);
            }
        }

        // Find all recently active peers.
        let now = self.clock.now();
        let mut active_peers: Vec<Arc<connection::Connection>> = tier2
            .ready
            .values()
            .filter(|p| {
                now - p.last_time_received_message.load() < self.config.peer_recent_time_window
            })
            .cloned()
            .collect();
        // Sort by established time.
        active_peers.sort_by_key(|p| p.connection_established_time);
        // Saturate safe set with recently active peers.
        let set_limit = self.config.safe_set_size as usize;
        for p in active_peers {
            if safe_set.len() >= set_limit {
                break;
            }
            safe_set.insert(p.peer_info.id.clone());
        }

        // Build valid candidate list to choose the peer to be removed. All peers outside the safe set.
        let candidates = tier2.ready.values().filter(|p| !safe_set.contains(&p.peer_info.id));
        if let Some(p) = candidates.choose(&mut rand::thread_rng()) {
            debug!(target: "network", id = ?p.peer_info.id,
                tier2_len = tier2.ready.len(),
                ideal_connections_hi = self.config.ideal_connections_hi,
                "Stop active connection"
            );
            p.unregister();
        }
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - unban peers that have been banned for awhile,
    ///  - remove expired peers,
    ///
    /// # Arguments:
    /// - `interval` - Time between consequent runs.
    /// - `default_interval` - we will set `interval` to this value once, after first successful connection
    /// - `max_interval` - maximum value of interval
    /// NOTE: in the current implementation `interval` increases by 1% every time, and it will
    ///       reach value of `max_internal` eventually.
    fn monitor_peers_trigger(
        &mut self,
        ctx: &mut Context<Self>,
        mut interval: time::Duration,
        (default_interval, max_interval): (time::Duration, time::Duration),
    ) {
        let _span = tracing::trace_span!(target: "network", "monitor_peers_trigger").entered();
        let _timer =
            metrics::PEER_MANAGER_TRIGGER_TIME.with_label_values(&["monitor_peers"]).start_timer();
        let mut to_unban = vec![];
        for (peer_id, peer_state) in self.peer_store.iter() {
            if let KnownPeerStatus::Banned(_, last_banned) = peer_state.status {
                let interval = self.clock.now_utc() - last_banned;
                if interval > self.config.ban_window {
                    info!(target: "network", unbanned = ?peer_id, after = ?interval, "Monitor peers:");
                    to_unban.push(peer_id.clone());
                }
            }
        }

        for peer_id in to_unban {
            if let Err(err) = self.peer_store.peer_unban(&peer_id) {
                error!(target: "network", ?err, "Failed to unban a peer");
            }
        }

        if self.is_outbound_bootstrap_needed() {
            let tier2 = self.state.tier2.load();
            if let Some(peer_info) = self.peer_store.unconnected_peer(|peer_state| {
                // Ignore connecting to ourself
                self.my_peer_id == peer_state.peer_info.id
                    || self.config.node_addr == peer_state.peer_info.addr
                    // Or to peers we are currently trying to connect to
                    || tier2.outbound_handshakes.contains(&peer_state.peer_info.id)
            }) {
                // Start monitor_peers_attempts from start after we discover the first healthy peer
                if !self.started_connect_attempts {
                    self.started_connect_attempts = true;
                    interval = default_interval;
                }

                ctx.notify(PeerManagerMessageRequest::OutboundTcpConnect(OutboundTcpConnect {
                    peer_info,
                }));
            } else {
                self.state.ask_for_more_peers(&self.clock);
            }
        }

        // If there are too many active connections try to remove some connections
        self.maybe_stop_active_connection();

        if let Err(err) = self.peer_store.remove_expired(&self.clock, &self.config) {
            error!(target: "network", ?err, "Failed to remove expired peers");
        };

        // Find peers that are not reliable (too much behind) - and make sure that we're not routing messages through them.
        let unreliable_peers = self.unreliable_peers();
        metrics::PEER_UNRELIABLE.set(unreliable_peers.len() as i64);
        self.network_graph.write().set_unreliable_peers(unreliable_peers);

        let new_interval = min(max_interval, interval * EXPONENTIAL_BACKOFF_RATIO);

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.monitor_peers_trigger(ctx, new_interval, (default_interval, max_interval));
            },
        );
    }

    /// Sends list of edges, from peer `peer_id` to check their signatures to `EdgeValidatorActor`.
    /// Bans peer `peer_id` if an invalid edge is found.
    /// `PeerManagerActor` periodically runs `broadcast_validated_edges_trigger`, which gets edges
    /// from `EdgeValidatorActor` concurrent queue and sends edges to be added to `RoutingTableActor`.
    fn validate_edges_and_add_to_routing_table(
        &self,
        peer_id: PeerId,
        edges: Vec<Edge>,
    ) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_addr.do_send(
            routing::actor::Message::ValidateEdgeList(ValidateEdgeList {
                source_peer_id: peer_id,
                edges,
                edges_info_shared: self.routing_table_exchange_helper.edges_info_shared.clone(),
                sender: self.routing_table_exchange_helper.edges_to_add_sender.clone(),
            }),
        );
    }

    /// Return whether the message is sent or not.
    fn send_message_to_account_or_peer_or_hash(
        &mut self,
        target: &AccountOrPeerIdOrHash,
        msg: RoutedMessageBody,
    ) -> bool {
        match target {
            AccountOrPeerIdOrHash::AccountId(account_id) => {
                self.send_message_to_account(account_id, msg)
            }
            peer_or_hash @ AccountOrPeerIdOrHash::PeerId(_)
            | peer_or_hash @ AccountOrPeerIdOrHash::Hash(_) => self.state.send_message_to_peer(
                &self.clock,
                RawRoutedMessage { target: peer_or_hash.clone(), body: msg },
            ),
        }
    }

    /// Send message to specific account.
    /// Return whether the message is sent or not.
    fn send_message_to_account(&mut self, account_id: &AccountId, msg: RoutedMessageBody) -> bool {
        let target = match self.state.routing_table_view.account_owner(account_id) {
            Ok(peer_id) => peer_id,
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                metrics::MessageDropped::UnknownAccount.inc(&msg);
                debug!(target: "network",
                       account_id = ?self.config.validator.as_ref().map(|v|v.account_id()),
                       to = ?account_id,
                       reason = ?find_route_error,
                       ?msg,"Drop message",
                );
                trace!(target: "network", known_peers = ?self.state.routing_table_view.get_accounts_keys(), "Known peers");
                return false;
            }
        };

        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body: msg };
        if msg.body.is_important() {
            let mut success = false;
            for _ in 0..IMPORTANT_MESSAGE_RESENT_COUNT {
                success |= self.state.send_message_to_peer(&self.clock, msg.clone());
            }
            success
        } else {
            self.state.send_message_to_peer(&self.clock, msg)
        }
    }

    pub(crate) fn get_network_info(&self) -> NetworkInfo {
        let tier2 = self.state.tier2.load();
        NetworkInfo {
            connected_peers: tier2
                .ready
                .values()
                .map(|cp| {
                    let stats = cp.stats.load();
                    ConnectedPeerInfo {
                        full_peer_info: cp.full_peer_info(),
                        received_bytes_per_sec: stats.received_bytes_per_sec,
                        sent_bytes_per_sec: stats.sent_bytes_per_sec,
                        last_time_peer_requested: cp.last_time_peer_requested.load(),
                        last_time_received_message: cp.last_time_received_message.load(),
                        connection_established_time: cp.connection_established_time,
                        peer_type: cp.peer_type,
                    }
                })
                .collect(),
            num_connected_peers: tier2.ready.len(),
            peer_max_count: self.max_num_peers,
            highest_height_peers: self.highest_height_peers(),
            sent_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.load().sent_bytes_per_sec)
                .sum(),
            received_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.load().received_bytes_per_sec)
                .sum(),
            known_producers: self
                .state
                .routing_table_view
                .get_announce_accounts()
                .into_iter()
                .map(|announce_account| KnownProducer {
                    account_id: announce_account.account_id,
                    peer_id: announce_account.peer_id.clone(),
                    // TODO: fill in the address.
                    addr: None,
                    next_hops: self.state.routing_table_view.view_route(&announce_account.peer_id),
                })
                .collect(),
            tier1_accounts: self.state.accounts_data.load().data.values().cloned().collect(),
        }
    }

    fn push_network_info_trigger(&self, ctx: &mut Context<Self>, interval: time::Duration) {
        let network_info = self.get_network_info();
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["push_network_info"])
            .start_timer();

        let _ = self.state.client_addr.do_send(NetworkClientMessages::NetworkInfo(network_info));

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.push_network_info_trigger(ctx, interval);
            },
        );
    }

    #[perf]
    fn handle_msg_network_requests(
        &mut self,
        msg: NetworkRequests,
        _ctx: &mut Context<Self>,
    ) -> NetworkResponses {
        let _span =
            tracing::trace_span!(target: "network", "handle_msg_network_requests").entered();
        let _d = delay_detector::DelayDetector::new(|| {
            format!("network request {}", msg.as_ref()).into()
        });
        metrics::REQUEST_COUNT_BY_TYPE_TOTAL.with_label_values(&[msg.as_ref()]).inc();
        match msg {
            NetworkRequests::Block { block } => {
                self.state.tier2.broadcast_message(Arc::new(PeerMessage::Block(block)));
                NetworkResponses::NoResponse
            }
            NetworkRequests::Approval { approval_message } => {
                self.send_message_to_account(
                    &approval_message.target,
                    RoutedMessageBody::BlockApproval(approval_message.approval),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                if self.state.tier2.send_message(peer_id, Arc::new(PeerMessage::BlockRequest(hash)))
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if self
                    .state
                    .tier2
                    .send_message(peer_id, Arc::new(PeerMessage::BlockHeadersRequest(hashes)))
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, target } => {
                if self.send_message_to_account_or_peer_or_hash(
                    &target,
                    RoutedMessageBody::StateRequestHeader(shard_id, sync_hash),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateRequestPart { shard_id, sync_hash, part_id, target } => {
                if self.send_message_to_account_or_peer_or_hash(
                    &target,
                    RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateResponse { route_back, response } => {
                let body = match response {
                    StateResponseInfo::V1(response) => RoutedMessageBody::StateResponse(response),
                    response @ StateResponseInfo::V2(_) => {
                        RoutedMessageBody::VersionedStateResponse(response)
                    }
                };
                if self.state.send_message_to_peer(
                    &self.clock,
                    RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(route_back), body },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            // unused: epoch sync is not implemented
            NetworkRequests::EpochSyncRequest { peer_id, epoch_id } => {
                if self
                    .state
                    .tier2
                    .send_message(peer_id, Arc::new(PeerMessage::EpochSyncRequest(epoch_id)))
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            // unused: epoch sync is not implemented
            NetworkRequests::EpochSyncFinalizationRequest { peer_id, epoch_id } => {
                if self.state.tier2.send_message(
                    peer_id,
                    Arc::new(PeerMessage::EpochSyncFinalizationRequest(epoch_id)),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                self.try_ban_peer(&peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                self.broadcast_accounts(vec![announce_account]);
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedChunkRequest { target, request, create_time } => {
                metrics::PARTIAL_ENCODED_CHUNK_REQUEST_DELAY
                    .observe((self.clock.now() - create_time.0).as_seconds_f64());
                let mut success = false;

                // Make two attempts to send the message. First following the preference of `prefer_peer`,
                // and if it fails, against the preference.
                for prefer_peer in &[target.prefer_peer, !target.prefer_peer] {
                    if !prefer_peer {
                        if let Some(account_id) = target.account_id.as_ref() {
                            if self.send_message_to_account(
                                account_id,
                                RoutedMessageBody::PartialEncodedChunkRequest(request.clone()),
                            ) {
                                success = true;
                                break;
                            }
                        }
                    } else {
                        let mut matching_peers = vec![];
                        for (peer_id, peer) in &self.state.tier2.load().ready {
                            if (peer.initial_chain_info.archival || !target.only_archival)
                                && peer.chain_height.load(Ordering::Relaxed) >= target.min_height
                                && peer.initial_chain_info.tracked_shards.contains(&target.shard_id)
                            {
                                matching_peers.push(peer_id.clone());
                            }
                        }

                        if let Some(matching_peer) = matching_peers.iter().choose(&mut thread_rng())
                        {
                            if self.state.send_message_to_peer(
                                &self.clock,
                                RawRoutedMessage {
                                    target: AccountOrPeerIdOrHash::PeerId(matching_peer.clone()),
                                    body: RoutedMessageBody::PartialEncodedChunkRequest(
                                        request.clone(),
                                    ),
                                },
                            ) {
                                success = true;
                                break;
                            }
                        } else {
                            debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find any matching peer for chunk");
                        }
                    }
                }

                if success {
                    NetworkResponses::NoResponse
                } else {
                    debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find a route for chunk");
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                if self.state.send_message_to_peer(
                    &self.clock,
                    RawRoutedMessage {
                        target: AccountOrPeerIdOrHash::Hash(route_back),
                        body: RoutedMessageBody::PartialEncodedChunkResponse(response),
                    },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                if self.send_message_to_account(&account_id, partial_encoded_chunk.into()) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                if self.send_message_to_account(
                    &account_id,
                    RoutedMessageBody::PartialEncodedChunkForward(forward),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ForwardTx(account_id, tx) => {
                if self.send_message_to_account(&account_id, RoutedMessageBody::ForwardTx(tx)) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::TxStatus(account_id, signer_account_id, tx_hash) => {
                if self.send_message_to_account(
                    &account_id,
                    RoutedMessageBody::TxStatusRequest(signer_account_id, tx_hash),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::Challenge(challenge) => {
                // TODO(illia): smarter routing?
                self.state.tier2.broadcast_message(Arc::new(PeerMessage::Challenge(challenge)));
                NetworkResponses::NoResponse
            }
        }
    }

    #[perf]
    fn handle_msg_set_adv_options(&mut self, msg: crate::test_utils::SetAdvOptions) {
        if let Some(set_max_peers) = msg.set_max_peers {
            self.max_num_peers = set_max_peers as u32;
        }
    }

    #[perf]
    fn handle_msg_inbound_tcp_connect(&self, msg: InboundTcpConnect, ctx: &mut Context<Self>) {
        let _d = delay_detector::DelayDetector::new(|| "inbound tcp connect".into());
        let handshake_permit = match self
            .state
            .inbound_handshake_permits
            .clone()
            .try_acquire_owned()
        {
            Ok(it) => it,
            Err(_) => {
                debug!(target: "network", "Inbound connection dropped (too many ongoing inbound handshakes");
                return;
            }
        };
        if self.is_inbound_allowed()
            || msg
                .stream
                .peer_addr()
                .map(|addr| self.is_ip_whitelisted(&addr.ip()))
                .unwrap_or(false)
        {
            self.try_connect_peer(
                ctx.address(),
                msg.stream,
                ConnectingStatus::Inbound(handshake_permit),
                None,
            );
        } else {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network", "Inbound connection dropped (network at max capacity).");
        }
    }

    #[perf]
    fn handle_msg_outbound_tcp_connect(&self, msg: OutboundTcpConnect, ctx: &mut Context<Self>) {
        let _d = delay_detector::DelayDetector::new(|| "outbound tcp connect".into());
        debug!(target: "network", to = ?msg.peer_info, "Trying to connect");
        if let Some(addr) = msg.peer_info.addr {
            let handshake_permit = match self.state.tier2.start_outbound(msg.peer_info.id.clone()) {
                Ok(it) => it,
                Err(err) => {
                    tracing::warn!(target: "network", peer_info = ?msg.peer_info, "cannot connect: {err}");
                    return;
                }
            };
            // The `connect` may take several minutes. This happens when the
            // `SYN` packet for establishing a TCP connection gets silently
            // dropped, in which case the default TCP timeout is applied. That's
            // too long for us, so we shorten it to one second.
            //
            // Why exactly a second? It was hard-coded in a library we used
            // before, so we keep it to preserve behavior. Removing the timeout
            // completely was observed to break stuff for real on the testnet.
            tokio::time::timeout(std::time::Duration::from_secs(1), TcpStream::connect(addr))
                .into_actor(self)
                .then(move |res, act, ctx| match res {
                    Ok(res) => match res {
                        Ok(stream) => {
                            debug!(target: "network", peer_info = ?msg.peer_info, "Connecting");
                            act.try_connect_peer(
                                ctx.address(),
                                stream,
                                ConnectingStatus::Outbound(handshake_permit),
                                Some(msg.peer_info),
                            );
                            actix::fut::ready(())
                        }
                        Err(err) => {
                            info!(target: "network", ?addr, ?err, "Error connecting to");
                            actix::fut::ready(())
                        }
                    },
                    Err(err) => {
                        info!(target: "network", ?addr, ?err, "Error connecting to");
                        actix::fut::ready(())
                    }
                })
                .wait(ctx);
        } else {
            warn!(target: "network", peer_info = ?msg.peer_info, "Trying to connect to peer with no public address");
        }
    }

    #[perf]
    fn handle_msg_register_peer(
        &mut self,
        msg: RegisterPeer,
        ctx: &mut Context<Self>,
    ) -> RegisterPeerResponse {
        let _d = delay_detector::DelayDetector::new(|| "consolidate".into());

        let peer_info = &msg.connection.peer_info;
        // Check if this is a blacklisted peer.
        if peer_info.addr.as_ref().map_or(true, |addr| self.peer_store.is_blacklisted(addr)) {
            debug!(target: "network", peer_info = ?peer_info, "Dropping connection from blacklisted peer or unknown address");
            return RegisterPeerResponse::Reject;
        }

        if self.peer_store.is_banned(&peer_info.id) {
            debug!(target: "network", id = ?peer_info.id, "Dropping connection from banned peer");
            return RegisterPeerResponse::Reject;
        }

        let tier2 = self.state.tier2.load();
        if msg.connection.peer_type == PeerType::Inbound
            && !self.is_inbound_allowed()
            && !self.is_peer_whitelisted(&peer_info)
        {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network",
                tier2 = tier2.ready.len(), outgoing_peers = tier2.outbound_handshakes.len(),
                max_num_peers = self.max_num_peers,
                "Inbound connection dropped (network at max capacity)."
            );
            return RegisterPeerResponse::Reject;
        }

        let other_edge_info = &msg.connection.partial_edge_info;
        if other_edge_info.nonce == 0 {
            debug!(target: "network", nonce = other_edge_info.nonce, "Invalid nonce. It must be greater than 0.");
            return RegisterPeerResponse::Reject;
        }

        let last_edge = self.state.routing_table_view.get_local_edge(&peer_info.id);
        let last_nonce = last_edge.as_ref().map_or(0, |edge| edge.nonce());

        // Check that the received nonce is greater than the current nonce of this connection.
        if last_nonce >= other_edge_info.nonce {
            debug!(target: "network", nonce = other_edge_info.nonce, last_nonce, my_peer_id = ?self.my_peer_id, ?peer_info.id, "Too low nonce");
            // If the check fails don't allow this connection.
            return RegisterPeerResponse::InvalidNonce(Box::new(last_edge.unwrap().clone()));
        }

        if let Ok(maybe_nonce_timestamp) = Edge::nonce_to_utc(other_edge_info.nonce) {
            if let Some(nonce_timestamp) = maybe_nonce_timestamp {
                if (self.clock.now_utc() - nonce_timestamp).abs() < EDGE_NONCE_MAX_TIME_DELTA {
                    metrics::EDGE_NONCE.with_label_values(&["new_style"]).inc();
                } else {
                    metrics::EDGE_NONCE.with_label_values(&["error_timestamp_too_distant"]).inc();
                    debug!(target: "network", nonce = other_edge_info.nonce, clock=self.clock.now_utc().unix_timestamp(), ?EDGE_NONCE_MAX_TIME_DELTA, ?self.my_peer_id, ?peer_info.id, "Nonce too much in future.");
                    return RegisterPeerResponse::Reject;
                }
            } else {
                metrics::EDGE_NONCE.with_label_values(&["old_style"]).inc();
            }
        } else {
            debug!(target: "network", nonce = other_edge_info.nonce, clock=self.clock.now_utc().unix_timestamp(), ?EDGE_NONCE_MAX_TIME_DELTA, ?self.my_peer_id, ?peer_info.id, "Nonce is overflowing i64.");
            return RegisterPeerResponse::Reject;
        }
        let require_response = msg.this_edge_info.is_none();

        let edge_info = msg
            .this_edge_info
            .clone()
            .unwrap_or_else(|| self.state.propose_edge(&peer_info.id, Some(other_edge_info.nonce)));

        let edge_info_response = if require_response { Some(edge_info.clone()) } else { None };

        if let Err(err) = self.register_peer(msg.connection.clone(), edge_info, ctx) {
            debug!(target: "network", nonce = other_edge_info.nonce, clock=self.clock.now_utc().unix_timestamp(), ?EDGE_NONCE_MAX_TIME_DELTA, ?self.my_peer_id, ?peer_info.id, ?err, "state.register_peer()");
            return RegisterPeerResponse::Reject;
        }

        RegisterPeerResponse::Accept(edge_info_response)
    }

    #[perf]
    fn handle_msg_unregister(&mut self, msg: Unregister) {
        let _d = delay_detector::DelayDetector::new(|| "unregister".into());
        self.unregister_peer(msg.peer_id, msg.peer_type, msg.remove_from_peer_store);
    }

    #[perf]
    fn handle_msg_ban(&mut self, msg: Ban) {
        let _d = delay_detector::DelayDetector::new(|| "ban".into());
        self.ban_peer(&msg.peer_id, msg.ban_reason);
    }

    #[perf]
    fn handle_msg_peers_request(&self, _msg: PeersRequest) -> PeerRequestResult {
        let _d = delay_detector::DelayDetector::new(|| "peers request".into());
        PeerRequestResult {
            peers: self.peer_store.healthy_peers(self.config.max_send_peers as usize),
        }
    }

    fn handle_msg_peers_response(&mut self, msg: PeersResponse) {
        let _d = delay_detector::DelayDetector::new(|| "peers response".into());
        if let Err(err) = self.peer_store.add_indirect_peers(
            &self.clock,
            msg.peers.into_iter().filter(|peer_info| peer_info.id != self.my_peer_id),
        ) {
            error!(target: "network", ?err, "Fail to update peer store");
        };
    }

    fn handle_peer_manager_message(
        &mut self,
        msg: PeerManagerMessageRequest,
        ctx: &mut Context<Self>,
    ) -> PeerManagerMessageResponse {
        let _span =
            tracing::trace_span!(target: "network", "handle_peer_manager_message").entered();
        match msg {
            PeerManagerMessageRequest::NetworkRequests(msg) => {
                PeerManagerMessageResponse::NetworkResponses(self.handle_msg_network_requests(
                    msg,
                    ctx,
                ))
            }
            PeerManagerMessageRequest::OutboundTcpConnect(msg) => {
                self.handle_msg_outbound_tcp_connect(msg, ctx);
                PeerManagerMessageResponse::OutboundTcpConnect
            }
            // TEST-ONLY
            PeerManagerMessageRequest::SetAdvOptions(msg) => {
                self.handle_msg_set_adv_options(msg);
                PeerManagerMessageResponse::SetAdvOptions
            }
            // TEST-ONLY
            PeerManagerMessageRequest::FetchRoutingTable => {
                PeerManagerMessageResponse::FetchRoutingTable(self.state.routing_table_view.info())
            }
            // TEST-ONLY
            PeerManagerMessageRequest::PingTo { nonce, target } => {
                self.state.send_ping(&self.clock, nonce, target);
                PeerManagerMessageResponse::PingTo
            }
        }
    }

    fn handle_peer_to_manager_msg(
        &mut self,
        msg: PeerToManagerMsg,
        ctx: &mut Context<Self>,
    ) -> PeerToManagerMsgResp {
        match msg {
            PeerToManagerMsg::RegisterPeer(msg) => {
                PeerToManagerMsgResp::RegisterPeer(self.handle_msg_register_peer(msg, ctx))
            }
            PeerToManagerMsg::PeersRequest(msg) => {
                PeerToManagerMsgResp::PeersRequest(self.handle_msg_peers_request(msg))
            }
            PeerToManagerMsg::PeersResponse(msg) => {
                self.handle_msg_peers_response(msg);
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::RouteBack(body, target) => {
                trace!(target: "network", ?target, "Sending message to route back");
                self.state.send_message_to_peer(
                    &self.clock,
                    RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body: *body },
                );
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::UpdatePeerInfo(peer_info) => {
                if let Err(err) = self.peer_store.add_direct_peer(&self.clock, peer_info) {
                    error!(target: "network", ?err, "Fail to update peer store");
                }
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::InboundTcpConnect(msg) => {
                self.handle_msg_inbound_tcp_connect(msg, ctx);
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::Unregister(msg) => {
                self.handle_msg_unregister(msg);
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::Ban(msg) => {
                self.handle_msg_ban(msg);
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::RequestUpdateNonce(peer_id, edge_info) => {
                if Edge::partial_verify(&self.my_peer_id, &peer_id, &edge_info) {
                    if let Some(cur_edge) = self.state.routing_table_view.get_local_edge(&peer_id) {
                        if cur_edge.edge_type() == EdgeState::Active
                            && cur_edge.nonce() >= edge_info.nonce
                        {
                            return PeerToManagerMsgResp::EdgeUpdate(Box::new(cur_edge.clone()));
                        }
                    }

                    let new_edge = Edge::build_with_secret_key(
                        self.my_peer_id.clone(),
                        peer_id,
                        edge_info.nonce,
                        &self.config.node_key,
                        edge_info.signature,
                    );

                    self.add_verified_edges_to_routing_table(vec![new_edge.clone()]);
                    PeerToManagerMsgResp::EdgeUpdate(Box::new(new_edge))
                } else {
                    PeerToManagerMsgResp::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            PeerToManagerMsg::ResponseUpdateNonce(edge) => {
                if let Some(other_peer) = edge.other(&self.my_peer_id) {
                    if edge.verify() {
                        // This happens in case, we get an edge, in `RoutingTableActor`,
                        // which says that we shouldn't be connected to local peer, but we are.
                        // This is a part of logic used to ask peer, if he really want to be disconnected.
                        if self
                            .state
                            .routing_table_view
                            .is_local_edge_newer(other_peer, edge.nonce())
                        {
                            if let Some(nonce) =
                                self.local_peer_pending_update_nonce_request.get(other_peer)
                            {
                                if edge.nonce() >= *nonce {
                                    // This means that, `other_peer` responded that we should keep
                                    // the connection that that peer. Therefore, we are
                                    // cleaning up this data structure.
                                    self.local_peer_pending_update_nonce_request.remove(other_peer);
                                }
                            }
                        }
                        self.add_verified_edges_to_routing_table(vec![edge.clone()]);
                        PeerToManagerMsgResp::Empty
                    } else {
                        PeerToManagerMsgResp::BanPeer(ReasonForBan::InvalidEdge)
                    }
                } else {
                    PeerToManagerMsgResp::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            PeerToManagerMsg::SyncRoutingTable { peer_id, routing_table_update } => {
                // Process edges and add new edges to the routing table. Also broadcast new edges.
                let edges = routing_table_update.edges;
                let accounts = routing_table_update.accounts;

                // Filter known accounts before validating them.
                let old = self
                    .state
                    .routing_table_view
                    .get_announces(accounts.iter().map(|a| &a.account_id));
                let accounts: Vec<(AnnounceAccount, Option<EpochId>)> = accounts
                    .into_iter()
                    .map(|aa| {
                        let id = aa.account_id.clone();
                        (aa, old.get(&id).map(|old| old.epoch_id.clone()))
                    })
                    .collect();

                // Ask client to validate accounts before accepting them.
                let peer_id_clone = peer_id.clone();
                self.state.view_client_addr
                    .send(NetworkViewClientMessages::AnnounceAccount(accounts))
                    .in_current_span()
                    .into_actor(self)
                    .then(move |response, act, _ctx| {
                        let _span = tracing::trace_span!(target: "network", "announce_account").entered();
                        match response {
                            Ok(NetworkViewClientResponses::Ban { ban_reason }) => {
                                act.try_ban_peer(&peer_id_clone, ban_reason);
                            }
                            Ok(NetworkViewClientResponses::AnnounceAccount(accounts)) => {
                                act.broadcast_accounts(accounts);
                            }
                            _ => {
                                debug!(target: "network", "Received invalid account confirmation from client.");
                            }
                        }
                        actix::fut::ready(())
                    }).spawn(ctx);

                self.validate_edges_and_add_to_routing_table(peer_id, edges);
                PeerToManagerMsgResp::Empty
            }
        }
    }
}

/// Fetches NetworkInfo, which contains a bunch of stats about the
/// P2P network state. Currently used only in tests.
/// TODO(gprusak): In prod, NetworkInfo is pushed periodically from PeerManagerActor to ClientActor.
/// It would be cleaner to replace the push loop in PeerManagerActor with a pull loop
/// in the ClientActor.
impl Handler<GetNetworkInfo> for PeerManagerActor {
    type Result = NetworkInfo;
    fn handle(&mut self, _: GetNetworkInfo, _ctx: &mut Self::Context) -> NetworkInfo {
        let _timer = metrics::PEER_MANAGER_MESSAGES_TIME
            .with_label_values(&["GetNetworkInfo"])
            .start_timer();
        let _span =
            tracing::trace_span!(target: "network", "handle", handler = "GetNetworkInfo").entered();
        self.get_network_info()
    }
}

impl Handler<SetChainInfo> for PeerManagerActor {
    type Result = ();
    fn handle(&mut self, info: SetChainInfo, _ctx: &mut Self::Context) {
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&["SetChainInfo"]).start_timer();
        let _span =
            tracing::trace_span!(target: "network", "handle", handler = "SetChainInfo").entered();
        let now = self.clock.now_utc();
        let SetChainInfo(info) = info;
        let state = self.state.clone();
        // We set state.chain_info and call accounts_data.set_keys
        // synchronously, therefore, assuming actix in-order delivery,
        // there will be no race condition between subsequent SetChainInfo
        // calls.
        // TODO(gprusak): if we could make handle() async, then we could
        // just require the caller to await for completion before calling
        // SetChainInfo again. Alternatively we could have an async mutex
        // on the handler.
        state.chain_info.store(Arc::new(info.clone()));

        // If enable_tier1 is false, we skip set_keys() call.
        // This way self.state.accounts_data is always empty, hence no data
        // will be collected or broadcasted.
        if !state.config.features.enable_tier1 {
            return;
        }
        // If the key set didn't change, early exit.
        if !state.accounts_data.set_keys(info.tier1_accounts.clone()) {
            return;
        }
        tokio::spawn(async move {
            // If the set of keys has changed, and the node is a validator,
            // we should try to sign data and broadcast it. However, this is
            // also a trigger for a full sync, so a dedicated broadcast is
            // not required.
            //
            // TODO(gprusak): For dynamic self-IP-discovery, add a STUN daemon which
            // will add new AccountData and trigger an incremental broadcast.
            if let Some(vc) = &state.config.validator {
                let my_account_id = vc.signer.validator_id();
                let my_public_key = vc.signer.public_key();
                // TODO(gprusak): STUN servers should be queried periocally by a daemon
                // so that the my_peers list is always resolved.
                // Note that currently we will broadcast an empty list.
                // It won't help us to connect the the validator BUT it
                // will indicate that a validator is misconfigured, which
                // is could be useful for debugging. Consider keeping this
                // behavior for situations when the IPs are not known.
                let my_peers = match &vc.endpoints {
                    config::ValidatorEndpoints::TrustedStunServers(_) => vec![],
                    config::ValidatorEndpoints::PublicAddrs(peer_addrs) => peer_addrs.clone(),
                };
                let my_data = info.tier1_accounts.iter().filter_map(|((epoch_id,account_id),key)| {
                    if account_id != my_account_id{
                        return None;
                    }
                    if key != &my_public_key {
                        warn!(target: "network", "node's account_id found in TIER1 accounts, but the public keys do not match");
                        return None;
                    }
                    // This unwrap is safe, because we did signed a sample payload during
                    // config validation. See config::Config::new().
                    Some(Arc::new(AccountData {
                        epoch_id: epoch_id.clone(),
                        account_id: my_account_id.clone(),
                        timestamp: now,
                        peers: my_peers.clone(),
                    }.sign(vc.signer.as_ref()).unwrap()))
                }).collect();
                // Insert node's own AccountData should never fail.
                // We ignore the new data, because we trigger a full sync anyway.
                if let (_, Some(err)) = state.accounts_data.insert(my_data).await {
                    panic!("inserting node's own AccountData to self.state.accounts_data: {err}");
                }
            }
            // The set of tier1 accounts has changed.
            // We might miss some data, so we start a full sync with the connected peers.
            // TODO(gprusak): add a daemon which does a periodic full sync in case some messages
            // are lost (at a frequency which makes the additional network load negligible).
            state.tier2.broadcast_message(Arc::new(PeerMessage::SyncAccountsData(
                SyncAccountsData {
                    incremental: false,
                    requesting_full_sync: true,
                    accounts_data: state.accounts_data.load().data.values().cloned().collect(),
                },
            )));
            state.config.event_sink.push(Event::SetChainInfo);
        });
    }
}

impl Handler<PeerToManagerMsg> for PeerManagerActor {
    type Result = PeerToManagerMsgResp;
    fn handle(&mut self, msg: PeerToManagerMsg, ctx: &mut Self::Context) -> Self::Result {
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&[(&msg).into()]).start_timer();
        let _span = tracing::trace_span!(target: "network", "handle", handler = "PeerToManagerMsg")
            .entered();
        self.handle_peer_to_manager_msg(msg, ctx)
    }
}

impl Handler<PeerManagerMessageRequest> for PeerManagerActor {
    type Result = PeerManagerMessageResponse;
    fn handle(&mut self, msg: PeerManagerMessageRequest, ctx: &mut Self::Context) -> Self::Result {
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&[(&msg).into()]).start_timer();
        let _span = tracing::trace_span!(target: "network", "handle", handler = "PeerManagerMessageRequest").entered();
        self.handle_peer_manager_message(msg, ctx)
    }
}
