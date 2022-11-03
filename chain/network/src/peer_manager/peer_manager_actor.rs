use crate::client;
use crate::config;
use crate::debug::{DebugStatus, GetDebugStatus};
use crate::network_protocol::{
    AccountData, AccountOrPeerIdOrHash, Edge, EdgeState, PeerInfo, PeerMessage, Ping, Pong,
    RawRoutedMessage, RoutedMessageBody, StateResponseInfo, SyncAccountsData,
};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_store;
use crate::private_actix::{
    PeerRequestResult, PeersRequest, RegisterPeer, RegisterPeerError, RegisterPeerResponse, StopMsg,
};
use crate::private_actix::{PeerToManagerMsg, PeerToManagerMsgResp, PeersResponse};
use crate::routing;
use crate::stats::metrics;
use crate::store;
use crate::tcp;
use crate::time;
use crate::types::{
    ConnectedPeerInfo, FullPeerInfo, GetNetworkInfo, KnownProducer, NetworkInfo, NetworkRequests,
    NetworkResponses, PeerIdOrHash, PeerManagerMessageRequest, PeerManagerMessageResponse,
    PeerType, ReasonForBan, SetChainInfo,
};
use actix::fut::future::wrap_future;
use actix::{
    Actor, ActorFutureExt, AsyncContext, Context, ContextFutureSpawner, Handler, Running,
    WrapFuture,
};
use anyhow::bail;
use anyhow::Context as _;
use near_o11y::{handler_trace_span, OpenTelemetrySpanExt, WithSpanContext, WithSpanContextExt};
use near_performance_metrics_macros::perf;
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use near_primitives::views::{KnownPeerStateView, PeerStoreView};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use rand::Rng;
use std::cmp::min;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The initial waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_INITIAL_DURATION: time::Duration = time::Duration::milliseconds(10);
/// How ofter should we broadcast edges.
const BROADCAST_VALIDATED_EDGES_INTERVAL: time::Duration = time::Duration::milliseconds(50);
/// Maximum amount of time spend processing edges.
const BROAD_CAST_EDGES_MAX_WORK_ALLOWED: time::Duration = time::Duration::milliseconds(50);
/// How often should we update the routing table
const UPDATE_ROUTING_TABLE_INTERVAL: time::Duration = time::Duration::milliseconds(1_000);
/// How often should we check wheter local edges match the connection pool.
const FIX_LOCAL_EDGES_INTERVAL: time::Duration = time::Duration::seconds(60);

/// How often to report bandwidth stats.
const REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL: time::Duration =
    time::Duration::milliseconds(60_000);

/// If we received more than `REPORT_BANDWIDTH_THRESHOLD_BYTES` of data from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_BYTES: usize = 10_000_000;
/// If we received more than REPORT_BANDWIDTH_THRESHOLD_COUNT` of messages from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_COUNT: usize = 10_000;
/// How long a peer has to be unreachable, until we prune it from the in-memory graph.
const PRUNE_UNREACHABLE_PEERS_AFTER: time::Duration = time::Duration::hours(1);

/// Remove the edges that were created more that this duration ago.
const PRUNE_EDGES_AFTER: time::Duration = time::Duration::minutes(30);

/// If a peer is more than these blocks behind (comparing to our current head) - don't route any messages through it.
/// We are updating the list of unreliable peers every MONITOR_PEER_MAX_DURATION (60 seconds) - so the current
/// horizon value is roughly matching this threshold (if the node is 60 blocks behind, it will take it a while to recover).
/// If we set this horizon too low (for example 2 blocks) - we're risking excluding a lot of peers in case of a short
/// network issue.
const UNRELIABLE_PEER_HORIZON: u64 = 60;

/// Due to implementation limits of `Graph` in `near-network`, we support up to 128 client.
pub const MAX_NUM_PEERS: usize = 128;

/// When picking a peer to connect to, we'll pick from the 'safer peers'
/// (a.k.a. ones that we've been connected to in the past) with these odds.
/// Otherwise, we'd pick any peer that we've heard about.
const PREFER_PREVIOUSLY_CONNECTED_PEER: f64 = 0.6;

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

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    pub(crate) clock: time::Clock,
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
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,
    /// Whitelisted nodes, which are allowed to connect even if the connection limit has been
    /// reached.
    whitelist_nodes: Vec<WhitelistNode>,

    /// State that is shared between multiple threads (including PeerActors).
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
    RoutingTableUpdate { next_hops: Arc<routing::NextHopTable>, pruned_edges: Vec<Edge> },
    EdgesVerified(Vec<Edge>),
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
    // Reported when a handshake has been started.
    HandshakeStarted(crate::peer::peer_actor::HandshakeStartedEvent),
    // Reported when a handshake has been successfully completed.
    HandshakeCompleted(crate::peer::peer_actor::HandshakeCompletedEvent),
    // Reported when the TCP connection has been closed.
    ConnectionClosed(crate::peer::peer_actor::ConnectionClosedEvent),
}

impl Actor for PeerManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start server if address provided.
        if let Some(server_addr) = self.config.node_addr {
            debug!(target: "network", at = ?server_addr, "starting public server");
            let clock = self.clock.clone();
            let state = self.state.clone();
            ctx.spawn(wrap_future(async move {
                let mut listener = match tcp::Listener::bind(server_addr).await {
                    Ok(it) => it,
                    Err(e) => {
                        panic!("failed to start listening on server_addr={server_addr:?} e={e:?}")
                    }
                };
                state.config.event_sink.push(Event::ServerStarted);
                loop {
                    if let Ok(stream) = listener.accept().await {
                        // Always let the new peer to send a handshake message.
                        // Only then we can decide whether we should accept a connection.
                        // It is expected to be reasonably cheap: eventually, for TIER2 network
                        // we would like to exchange set of connected peers even without establishing
                        // a proper connection.
                        debug!(target: "network", from = ?stream.peer_addr, "got new connection");
                        if let Err(err) =
                            PeerActor::spawn(clock.clone(), stream, None, state.clone())
                        {
                            tracing::info!(target:"network", ?err, "PeerActor::spawn()");
                        }
                    }
                }
            }));
        }

        // Periodically push network information to client.
        self.push_network_info_trigger(ctx, self.config.push_info_period);

        // Periodically starts peer monitoring.
        debug!(target: "network", max_period=?self.config.monitor_peers_max_period, "monitor_peers_trigger");
        self.monitor_peers_trigger(
            ctx,
            MONITOR_PEERS_INITIAL_DURATION,
            (MONITOR_PEERS_INITIAL_DURATION, self.config.monitor_peers_max_period),
        );

        let state = self.state.clone();
        let clock = self.clock.clone();
        ctx.spawn(wrap_future(async move {
            let mut interval = tokio::time::interval(FIX_LOCAL_EDGES_INTERVAL.try_into().unwrap());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                state.fix_local_edges(&clock).await;
            }
        }));

        // Periodically reads valid edges from `EdgesVerifierActor` and broadcast.
        self.broadcast_validated_edges_trigger(ctx, BROADCAST_VALIDATED_EDGES_INTERVAL);

        // Periodically updates routing table and prune edges that are no longer reachable.
        self.update_routing_table_trigger(ctx, UPDATE_ROUTING_TABLE_INTERVAL);

        // Periodically prints bandwidth stats for each peer.
        self.report_bandwidth_stats_trigger(ctx, REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL);
    }

    /// Try to gracefully disconnect from connected peers.
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        warn!("PeerManager: stopping");
        self.state.tier2.broadcast_message(Arc::new(PeerMessage::Disconnect));
        self.state.routing_table_addr.do_send(StopMsg {}.with_span_context());
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        actix::Arbiter::current().stop();
    }
}

impl PeerManagerActor {
    pub fn spawn(
        clock: time::Clock,
        store: Arc<dyn near_store::db::Database>,
        config: config::NetworkConfig,
        client: Arc<dyn client::Client>,
        genesis_id: GenesisId,
    ) -> anyhow::Result<actix::Addr<Self>> {
        let config = config.verify().context("config")?;
        let store = store::Store::from(store);
        let peer_store =
            peer_store::PeerStore::new(&clock, config.peer_store.clone(), store.clone())
                .context("PeerStore::new")?;
        tracing::debug!(target: "network",
               len = peer_store.len(),
               boot_nodes = config.peer_store.boot_nodes.len(),
               banned = peer_store.count_banned(),
               "Found known peers");
        tracing::debug!(target: "network", blacklist = ?config.peer_store.blacklist, "Blacklist");

        let my_peer_id = config.node_id();
        let whitelist_nodes = {
            let mut v = vec![];
            for wn in &config.whitelist_nodes {
                v.push(wn.try_into()?);
            }
            v
        };
        let config = Arc::new(config);
        Ok(Self::start_in_arbiter(&actix::Arbiter::new().handle(), move |ctx| Self {
            my_peer_id: my_peer_id.clone(),
            config: config.clone(),
            max_num_peers: config.max_num_peers,
            started_connect_attempts: false,
            whitelist_nodes,
            state: Arc::new(NetworkState::new(
                &clock,
                store.clone(),
                peer_store,
                config.clone(),
                genesis_id,
                client,
                ctx.address().recipient(),
            )),
            clock,
        }))
    }

    fn update_routing_table(
        &self,
        ctx: &mut Context<Self>,
        prune_unreachable_since: Option<time::Instant>,
        prune_edges_older_than: Option<time::Utc>,
    ) {
        self.state
            .routing_table_addr
            .send(
                routing::actor::Message::RoutingTableUpdate {
                    prune_unreachable_since,
                    prune_edges_older_than,
                }
                .with_span_context(),
            )
            .into_actor(self)
            .map(|response, act, _ctx| match response {
                Ok(routing::actor::Response::RoutingTableUpdateResponse {
                    pruned_edges,
                    next_hops,
                    peers_to_ban,
                }) => {
                    act.state.routing_table_view.update(&pruned_edges, next_hops.clone());
                    for peer in peers_to_ban {
                        act.state.disconnect_and_ban(&act.clock, &peer, ReasonForBan::InvalidEdge);
                    }
                    act.config
                        .event_sink
                        .push(Event::RoutingTableUpdate { next_hops, pruned_edges });
                }
                _ => error!(target: "network", "expected RoutingTableUpdateResponse"),
            })
            .spawn(ctx);
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
        let mut total_bandwidth_used_by_all_peers: usize = 0;
        let mut total_msg_received_count: usize = 0;
        for (peer_id, connected_peer) in &self.state.tier2.load().ready {
            let bandwidth_used =
                connected_peer.stats.received_bytes.swap(0, Ordering::Relaxed) as usize;
            let msg_received_count =
                connected_peer.stats.received_messages.swap(0, Ordering::Relaxed) as usize;
            if bandwidth_used > REPORT_BANDWIDTH_THRESHOLD_BYTES
                || msg_received_count > REPORT_BANDWIDTH_THRESHOLD_COUNT
            {
                debug!(target: "bandwidth",
                    ?peer_id,
                    bandwidth_used, msg_received_count, "Peer bandwidth exceeded threshold",
                );
            }
            total_bandwidth_used_by_all_peers += bandwidth_used;
            total_msg_received_count += msg_received_count;
        }

        info!(
            target: "bandwidth",
            total_bandwidth_used_by_all_peers,
            total_msg_received_count, "Bandwidth stats"
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
    ) {
        let _span =
            tracing::trace_span!(target: "network", "broadcast_validated_edges_trigger").entered();
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["broadcast_validated_edges"])
            .start_timer();
        let start = self.clock.now();
        let mut new_edges = Vec::new();
        while let Ok(edge) =
            self.state.routing_table_exchange_helper.edges_to_add_receiver.try_recv()
        {
            new_edges.push(edge);
            // TODO: do we really need this limit?
            if self.clock.now() >= start + BROAD_CAST_EDGES_MAX_WORK_ALLOWED {
                break;
            }
        }
        self.state.add_verified_edges_to_routing_table(&self.clock, new_edges);

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.broadcast_validated_edges_trigger(ctx, interval);
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
    ) -> Result<(), connection::PoolError> {
        let peer_info = &connection.peer_info;
        let _span = tracing::trace_span!(target: "network", "register_peer").entered();
        debug!(target: "network", ?peer_info, "Consolidated connection");
        self.state.tier2.insert_ready(connection.clone())?;
        // Best effort write to DB.
        if let Err(err) = self.state.peer_store.peer_connected(&self.clock, peer_info) {
            error!(target: "network", ?err, "Failed to save peer data");
        }
        self.state.add_verified_edges_to_routing_table(&self.clock, vec![connection.edge.clone()]);
        Ok(())
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

    // predicate checking whether we should allow an inbound connection from peer_info.
    fn is_inbound_allowed(&self, peer_info: &PeerInfo) -> bool {
        // Check if we have spare inbound connections capacity.
        let tier2 = self.state.tier2.load();
        if tier2.ready.len() + tier2.outbound_handshakes.len() < self.max_num_peers as usize
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

        // Add whitelisted nodes to the safe set.
        let whitelisted_peers = filter_peers(&|p| self.is_peer_whitelisted(&p.peer_info));
        safe_set.extend(whitelisted_peers);

        // If there is not enough non-whitelisted peers, return without disconnecting anyone.
        if tier2.ready.len() - safe_set.len() <= self.config.ideal_connections_hi as usize {
            return;
        }

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
            p.stop(None);
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

        self.state.peer_store.unban(&self.clock);
        if let Err(err) = self.state.peer_store.update_connected_peers_last_seen(&self.clock) {
            error!(target: "network", ?err, "Failed to update peers last seen time.");
        }

        if self.is_outbound_bootstrap_needed() {
            let tier2 = self.state.tier2.load();
            // With some odds - try picking one of the 'NotConnected' peers -- these are the ones that we were able to connect to in the past.
            let prefer_previously_connected_peer =
                thread_rng().gen_bool(PREFER_PREVIOUSLY_CONNECTED_PEER);
            if let Some(peer_info) = self.state.peer_store.unconnected_peer(
                |peer_state| {
                    // Ignore connecting to ourself
                    self.my_peer_id == peer_state.peer_info.id
                    || self.config.node_addr == peer_state.peer_info.addr
                    // Or to peers we are currently trying to connect to
                    || tier2.outbound_handshakes.contains(&peer_state.peer_info.id)
                },
                prefer_previously_connected_peer,
            ) {
                // Start monitor_peers_attempts from start after we discover the first healthy peer
                if !self.started_connect_attempts {
                    self.started_connect_attempts = true;
                    interval = default_interval;
                }
                ctx.spawn(wrap_future({
                    let state = self.state.clone();
                    let clock = self.clock.clone();
                    async move {
                        let result = async {
                            let stream = tcp::Stream::connect(&peer_info).await.context("tcp::Stream::connect()")?;
                            PeerActor::spawn(clock.clone(),stream,None,state.clone()).context("PeerActor::spawn()")?;
                            anyhow::Ok(())
                        }.await;

                        if result.is_err() {
                            tracing::info!(target:"network", ?result, "failed to connect to {peer_info}");
                        }
                        if state.peer_store.peer_connection_attempt(&clock, &peer_info.id, result).is_err() {
                            error!(target: "network", ?peer_info, "Failed to mark peer as failed.");
                        }
                    }
                }));
            }
        }

        // If there are too many active connections try to remove some connections
        self.maybe_stop_active_connection();

        if let Err(err) = self.state.peer_store.remove_expired(&self.clock) {
            error!(target: "network", ?err, "Failed to remove expired peers");
        };

        // Find peers that are not reliable (too much behind) - and make sure that we're not routing messages through them.
        let unreliable_peers = self.unreliable_peers();
        metrics::PEER_UNRELIABLE.set(unreliable_peers.len() as i64);
        self.state.graph.write().set_unreliable_peers(unreliable_peers);

        let new_interval = min(max_interval, interval * EXPONENTIAL_BACKOFF_RATIO);

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.monitor_peers_trigger(ctx, new_interval, (default_interval, max_interval));
            },
        );
    }

    /// Return whether the message is sent or not.
    fn send_message_to_account_or_peer_or_hash(
        &mut self,
        target: &AccountOrPeerIdOrHash,
        msg: RoutedMessageBody,
    ) -> bool {
        let target = match target {
            AccountOrPeerIdOrHash::AccountId(account_id) => {
                return self.state.send_message_to_account(&self.clock, account_id, msg);
            }
            AccountOrPeerIdOrHash::PeerId(it) => PeerIdOrHash::PeerId(it.clone()),
            AccountOrPeerIdOrHash::Hash(it) => PeerIdOrHash::Hash(it.clone()),
        };

        self.state.send_message_to_peer(
            &self.clock,
            self.state.sign_message(&self.clock, RawRoutedMessage { target, body: msg }),
        )
    }

    pub(crate) fn get_network_info(&self) -> NetworkInfo {
        let tier2 = self.state.tier2.load();
        NetworkInfo {
            connected_peers: tier2
                .ready
                .values()
                .map(|cp| ConnectedPeerInfo {
                    full_peer_info: cp.full_peer_info(),
                    received_bytes_per_sec: cp.stats.received_bytes_per_sec.load(Ordering::Relaxed),
                    sent_bytes_per_sec: cp.stats.sent_bytes_per_sec.load(Ordering::Relaxed),
                    last_time_peer_requested: cp
                        .last_time_peer_requested
                        .load()
                        .unwrap_or(self.clock.now()),
                    last_time_received_message: cp.last_time_received_message.load(),
                    connection_established_time: cp.connection_established_time,
                    peer_type: cp.peer_type,
                })
                .collect(),
            num_connected_peers: tier2.ready.len(),
            peer_max_count: self.max_num_peers,
            highest_height_peers: self.highest_height_peers(),
            sent_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.sent_bytes_per_sec.load(Ordering::Relaxed))
                .sum(),
            received_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.received_bytes_per_sec.load(Ordering::Relaxed))
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
        // TODO(gprusak): just spawn a loop.
        let state = self.state.clone();
        ctx.spawn(wrap_future(async move { state.client.network_info(network_info).await }));

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
                self.state.send_message_to_account(
                    &self.clock,
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
                    self.state.sign_message(
                        &self.clock,
                        RawRoutedMessage { target: PeerIdOrHash::Hash(route_back), body },
                    ),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                self.state.disconnect_and_ban(&self.clock, &peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                self.state.broadcast_accounts(vec![announce_account]);
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
                            if self.state.send_message_to_account(
                                &self.clock,
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
                                self.state.sign_message(
                                    &self.clock,
                                    RawRoutedMessage {
                                        target: PeerIdOrHash::PeerId(matching_peer.clone()),
                                        body: RoutedMessageBody::PartialEncodedChunkRequest(
                                            request.clone(),
                                        ),
                                    },
                                ),
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
                    self.state.sign_message(
                        &self.clock,
                        RawRoutedMessage {
                            target: PeerIdOrHash::Hash(route_back),
                            body: RoutedMessageBody::PartialEncodedChunkResponse(response),
                        },
                    ),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                if self.state.send_message_to_account(
                    &self.clock,
                    &account_id,
                    RoutedMessageBody::VersionedPartialEncodedChunk(partial_encoded_chunk.into()),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                if self.state.send_message_to_account(
                    &self.clock,
                    &account_id,
                    RoutedMessageBody::PartialEncodedChunkForward(forward),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ForwardTx(account_id, tx) => {
                if self.state.send_message_to_account(
                    &self.clock,
                    &account_id,
                    RoutedMessageBody::ForwardTx(tx),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::TxStatus(account_id, signer_account_id, tx_hash) => {
                if self.state.send_message_to_account(
                    &self.clock,
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
    fn handle_msg_register_peer(&mut self, msg: RegisterPeer) -> RegisterPeerResponse {
        let _d = delay_detector::DelayDetector::new(|| "consolidate".into());

        let peer_info = &msg.connection.peer_info;
        // Check if this is a blacklisted peer.
        if peer_info.addr.as_ref().map_or(true, |addr| self.state.peer_store.is_blacklisted(addr)) {
            debug!(target: "network", peer_info = ?peer_info, "Dropping connection from blacklisted peer or unknown address");
            return RegisterPeerResponse::Reject(RegisterPeerError::Blacklisted);
        }

        if self.state.peer_store.is_banned(&peer_info.id) {
            debug!(target: "network", id = ?peer_info.id, "Dropping connection from banned peer");
            return RegisterPeerResponse::Reject(RegisterPeerError::Banned);
        }
        if msg.connection.peer_type == PeerType::Inbound {
            if !self.is_inbound_allowed(&peer_info) {
                // TODO(1896): Gracefully drop inbound connection for other peer.
                let tier2 = self.state.tier2.load();
                debug!(target: "network",
                    tier2 = tier2.ready.len(), outgoing_peers = tier2.outbound_handshakes.len(),
                    max_num_peers = self.max_num_peers,
                    "Dropping handshake (network at max capacity)."
                );
                return RegisterPeerResponse::Reject(RegisterPeerError::ConnectionLimitExceeded);
            }
        }
        if let Err(err) = self.register_peer(msg.connection.clone()) {
            return RegisterPeerResponse::Reject(RegisterPeerError::PoolError(err));
        }
        RegisterPeerResponse::Accept
    }

    #[perf]
    fn handle_msg_peers_request(&self, _msg: PeersRequest) -> PeerRequestResult {
        let _d = delay_detector::DelayDetector::new(|| "peers request".into());
        PeerRequestResult {
            peers: self.state.peer_store.healthy_peers(self.config.max_send_peers as usize),
        }
    }

    fn handle_msg_peers_response(&mut self, msg: PeersResponse) {
        let _d = delay_detector::DelayDetector::new(|| "peers response".into());
        if let Err(err) = self.state.peer_store.add_indirect_peers(
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
                PeerManagerMessageResponse::NetworkResponses(
                    self.handle_msg_network_requests(msg, ctx),
                )
            }
            PeerManagerMessageRequest::OutboundTcpConnect(stream) => {
                let peer_addr = stream.peer_addr;
                if let Err(err) =
                    PeerActor::spawn(self.clock.clone(), stream, None, self.state.clone())
                {
                    tracing::info!(target:"network", ?err, ?peer_addr, "spawn_outbound()");
                }
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

    fn handle_peer_to_manager_msg(&mut self, msg: PeerToManagerMsg) -> PeerToManagerMsgResp {
        match msg {
            PeerToManagerMsg::RegisterPeer(msg) => {
                PeerToManagerMsgResp::RegisterPeer(self.handle_msg_register_peer(msg))
            }
            PeerToManagerMsg::PeersRequest(msg) => {
                PeerToManagerMsgResp::PeersRequest(self.handle_msg_peers_request(msg))
            }
            PeerToManagerMsg::PeersResponse(msg) => {
                self.handle_msg_peers_response(msg);
                PeerToManagerMsgResp::Empty
            }
            PeerToManagerMsg::UpdatePeerInfo(peer_info) => {
                if let Err(err) = self.state.peer_store.add_direct_peer(&self.clock, peer_info) {
                    error!(target: "network", ?err, "Fail to update peer store");
                }
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

                    self.state
                        .add_verified_edges_to_routing_table(&self.clock, vec![new_edge.clone()]);
                    PeerToManagerMsgResp::EdgeUpdate(Box::new(new_edge))
                } else {
                    PeerToManagerMsgResp::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            PeerToManagerMsg::ResponseUpdateNonce(edge) => {
                if edge.other(&self.my_peer_id).is_some() {
                    if edge.verify() {
                        self.state
                            .add_verified_edges_to_routing_table(&self.clock, vec![edge.clone()]);
                        PeerToManagerMsgResp::Empty
                    } else {
                        PeerToManagerMsgResp::BanPeer(ReasonForBan::InvalidEdge)
                    }
                } else {
                    PeerToManagerMsgResp::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
        }
    }
}

/// Fetches NetworkInfo, which contains a bunch of stats about the
/// P2P network state. Currently used only in tests.
/// TODO(gprusak): In prod, NetworkInfo is pushed periodically from PeerManagerActor to ClientActor.
/// It would be cleaner to replace the push loop in PeerManagerActor with a pull loop
/// in the ClientActor.
impl Handler<WithSpanContext<GetNetworkInfo>> for PeerManagerActor {
    type Result = NetworkInfo;
    fn handle(
        &mut self,
        msg: WithSpanContext<GetNetworkInfo>,
        _ctx: &mut Self::Context,
    ) -> NetworkInfo {
        let (_span, _msg) = handler_trace_span!(target: "network", msg);
        let _timer = metrics::PEER_MANAGER_MESSAGES_TIME
            .with_label_values(&["GetNetworkInfo"])
            .start_timer();
        self.get_network_info()
    }
}

impl Handler<WithSpanContext<SetChainInfo>> for PeerManagerActor {
    type Result = ();
    fn handle(&mut self, msg: WithSpanContext<SetChainInfo>, ctx: &mut Self::Context) {
        let (_span, info) = handler_trace_span!(target: "network", msg);
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&["SetChainInfo"]).start_timer();
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
        ctx.spawn(wrap_future(async move {
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
        }));
    }
}

impl Handler<WithSpanContext<PeerToManagerMsg>> for PeerManagerActor {
    type Result = PeerToManagerMsgResp;
    fn handle(
        &mut self,
        msg: WithSpanContext<PeerToManagerMsg>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        let msg_type: &str = (&msg.msg).into();
        let (_span, msg) = handler_trace_span!(target: "network", msg, msg_type);
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&[msg_type]).start_timer();
        self.handle_peer_to_manager_msg(msg)
    }
}

impl Handler<WithSpanContext<PeerManagerMessageRequest>> for PeerManagerActor {
    type Result = PeerManagerMessageResponse;
    fn handle(
        &mut self,
        msg: WithSpanContext<PeerManagerMessageRequest>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let msg_type: &str = (&msg.msg).into();
        let (_span, msg) = handler_trace_span!(target: "network", msg, msg_type);
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&[(&msg).into()]).start_timer();
        self.handle_peer_manager_message(msg, ctx)
    }
}

impl Handler<GetDebugStatus> for PeerManagerActor {
    type Result = DebugStatus;
    fn handle(&mut self, msg: GetDebugStatus, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            GetDebugStatus::PeerStore => {
                let mut peer_states_view = self
                    .state
                    .peer_store
                    .load()
                    .iter()
                    .map(|(peer_id, known_peer_state)| KnownPeerStateView {
                        peer_id: peer_id.clone(),
                        status: format!("{:?}", known_peer_state.status),
                        addr: format!("{:?}", known_peer_state.peer_info.addr),
                        first_seen: known_peer_state.first_seen.unix_timestamp(),
                        last_seen: known_peer_state.last_seen.unix_timestamp(),
                        last_attempt: known_peer_state.last_outbound_attempt.clone().map(
                            |(attempt_time, attempt_result)| {
                                let foo = match attempt_result {
                                    Ok(_) => String::from("Ok"),
                                    Err(err) => format!("Error: {:?}", err.as_str()),
                                };
                                (attempt_time.unix_timestamp(), foo)
                            },
                        ),
                    })
                    .collect::<Vec<_>>();

                peer_states_view.sort_by_key(|a| {
                    (
                        -a.last_attempt.clone().map(|(attempt_time, _)| attempt_time).unwrap_or(0),
                        -a.last_seen,
                    )
                });
                DebugStatus::PeerStore(PeerStoreView { peer_states: peer_states_view })
            }
        }
    }
}
