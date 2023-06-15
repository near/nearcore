use crate::client;
use crate::config;
use crate::debug::{DebugStatus, GetDebugStatus};
use crate::network_protocol::{
    AccountOrPeerIdOrHash, Disconnect, Edge, PeerIdOrHash, PeerMessage, Ping, Pong,
    RawRoutedMessage, RoutedMessageBody,
};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::{NetworkState, WhitelistNode};
use crate::peer_manager::peer_store;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::stats::metrics;
use crate::store;
use crate::tcp;
use crate::types::{
    ConnectedPeerInfo, HighestHeightPeerInfo, KnownProducer, NetworkInfo, NetworkRequests,
    NetworkResponses, PeerInfo, PeerManagerMessageRequest, PeerManagerMessageResponse, PeerType,
    SetChainInfo,
};
use actix::fut::future::wrap_future;
use actix::{Actor as _, AsyncContext as _};
use anyhow::Context as _;
use near_async::messaging::Sender;
use near_async::time;
use near_o11y::{handler_debug_span, handler_trace_span, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::block::GenesisId;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::views::{
    ConnectionInfoView, EdgeView, KnownPeerStateView, NetworkGraphView, PeerStoreView,
    RecentOutboundConnectionsView,
};
use rand::seq::IteratorRandom;
use rand::thread_rng;
use rand::Rng;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument as _;

/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The initial waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_INITIAL_DURATION: time::Duration = time::Duration::milliseconds(10);
/// How often should we check wheter local edges match the connection pool.
const FIX_LOCAL_EDGES_INTERVAL: time::Duration = time::Duration::seconds(60);
/// How much time we give fix_local_edges() to resolve the discrepancies, before forcing disconnect.
const FIX_LOCAL_EDGES_TIMEOUT: time::Duration = time::Duration::seconds(6);

/// Number of times to attempt reconnection when trying to re-establish a connection.
const MAX_RECONNECT_ATTEMPTS: usize = 6;

/// How often to report bandwidth stats.
const REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL: time::Duration =
    time::Duration::milliseconds(60_000);

/// If we received more than `REPORT_BANDWIDTH_THRESHOLD_BYTES` of data from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_BYTES: usize = 10_000_000;
/// If we received more than REPORT_BANDWIDTH_THRESHOLD_COUNT` of messages from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_COUNT: usize = 10_000;

/// If a peer is more than these blocks behind (comparing to our current head) - don't route any messages through it.
/// We are updating the list of unreliable peers every MONITOR_PEER_MAX_DURATION (60 seconds) - so the current
/// horizon value is roughly matching this threshold (if the node is 60 blocks behind, it will take it a while to recover).
/// If we set this horizon too low (for example 2 blocks) - we're risking excluding a lot of peers in case of a short
/// network issue.
const UNRELIABLE_PEER_HORIZON: u64 = 60;

/// Due to implementation limits of `Graph` in `near-network`, we support up to 128 client.
pub const MAX_TIER2_PEERS: usize = 128;

/// When picking a peer to connect to, we'll pick from the 'safer peers'
/// (a.k.a. ones that we've been connected to in the past) with these odds.
/// Otherwise, we'd pick any peer that we've heard about.
const PREFER_PREVIOUSLY_CONNECTED_PEER: f64 = 0.6;

/// How often to update the connections in storage.
pub(crate) const UPDATE_CONNECTION_STORE_INTERVAL: time::Duration = time::Duration::minutes(1);
/// How often to poll the NetworkState for closed connections we'd like to re-establish.
pub(crate) const POLL_CONNECTION_STORE_INTERVAL: time::Duration = time::Duration::minutes(1);

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    pub(crate) clock: time::Clock,
    /// Peer information for this node.
    my_peer_id: PeerId,
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,

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
    PeerManagerStarted,
    ServerStarted,
    RoutedMessageDropped,
    AccountsAdded(Vec<AnnounceAccount>),
    EdgesAdded(Vec<Edge>),
    Ping(Ping),
    Pong(Pong),
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
    MessageProcessed(tcp::Tier, PeerMessage),
    // Reported when a reconnect loop is spawned.
    ReconnectLoopSpawned(PeerInfo),
    // Reported when a handshake has been started.
    HandshakeStarted(crate::peer::peer_actor::HandshakeStartedEvent),
    // Reported when a handshake has been successfully completed.
    HandshakeCompleted(crate::peer::peer_actor::HandshakeCompletedEvent),
    // Reported when the TCP connection has been closed.
    ConnectionClosed(crate::peer::peer_actor::ConnectionClosedEvent),
}

impl actix::Actor for PeerManagerActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Periodically push network information to client.
        self.push_network_info_trigger(ctx, self.state.config.push_info_period);

        // Attempt to reconnect to recent outbound connections from storage
        if self.state.config.connect_to_reliable_peers_on_startup {
            tracing::debug!(target: "network", "Reconnecting to reliable peers from storage");
            self.bootstrap_outbound_from_recent_connections(ctx);
        } else {
            tracing::debug!(target: "network", "Skipping reconnection to reliable peers");
        }

        // Periodically starts peer monitoring.
        tracing::debug!(target: "network",
               max_period=?self.state.config.monitor_peers_max_period,
               "monitor_peers_trigger");
        self.monitor_peers_trigger(
            ctx,
            MONITOR_PEERS_INITIAL_DURATION,
            (MONITOR_PEERS_INITIAL_DURATION, self.state.config.monitor_peers_max_period),
        );

        // Periodically fix local edges.
        let clock = self.clock.clone();
        let state = self.state.clone();
        ctx.spawn(wrap_future(async move {
            let mut interval = time::Interval::new(clock.now(), FIX_LOCAL_EDGES_INTERVAL);
            loop {
                interval.tick(&clock).await;
                state.fix_local_edges(&clock, FIX_LOCAL_EDGES_TIMEOUT).await;
            }
        }));

        // Periodically update the connection store.
        let clock = self.clock.clone();
        let state = self.state.clone();
        ctx.spawn(wrap_future(async move {
            let mut interval = time::Interval::new(clock.now(), UPDATE_CONNECTION_STORE_INTERVAL);
            loop {
                interval.tick(&clock).await;
                state.update_connection_store(&clock);
            }
        }));

        // Periodically prints bandwidth stats for each peer.
        self.report_bandwidth_stats_trigger(ctx, REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL);

        self.state.config.event_sink.push(Event::PeerManagerStarted);
    }

    /// Try to gracefully disconnect from connected peers.
    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
        tracing::warn!("PeerManager: stopping");
        self.state.tier2.broadcast_message(Arc::new(PeerMessage::Disconnect(Disconnect {
            remove_from_connection_store: false,
        })));
        actix::Running::Stop
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
        shards_manager_adapter: Sender<ShardsManagerRequestFromNetwork>,
        genesis_id: GenesisId,
    ) -> anyhow::Result<actix::Addr<Self>> {
        let config = config.verify().context("config")?;
        let store = store::Store::from(store);
        let peer_store = peer_store::PeerStore::new(&clock, config.peer_store.clone())
            .context("PeerStore::new")?;
        tracing::debug!(target: "network",
               len = peer_store.len(),
               boot_nodes = config.peer_store.boot_nodes.len(),
               banned = peer_store.count_banned(),
               "Found known peers");
        tracing::debug!(target: "network", blacklist = ?config.peer_store.blacklist, "Blacklist");
        let whitelist_nodes = {
            let mut v = vec![];
            for wn in &config.whitelist_nodes {
                v.push(WhitelistNode::from_peer_info(wn)?);
            }
            v
        };
        let my_peer_id = config.node_id();
        let arbiter = actix::Arbiter::new().handle();
        let clock = clock;
        let state = Arc::new(NetworkState::new(
            &clock,
            store,
            peer_store,
            config,
            genesis_id,
            client,
            shards_manager_adapter,
            whitelist_nodes,
        ));
        arbiter.spawn({
            let arbiter = arbiter.clone();
            let state = state.clone();
            let clock = clock.clone();
            async move {
                // Start server if address provided.
                if let Some(server_addr) = &state.config.node_addr {
                    tracing::debug!(target: "network", at = ?server_addr, "starting public server");
                    let mut listener = match server_addr.listener() {
                        Ok(it) => it,
                        Err(e) => {
                            panic!("failed to start listening on server_addr={server_addr:?} e={e:?}")
                        }
                    };
                    state.config.event_sink.push(Event::ServerStarted);
                    arbiter.spawn({
                        let clock = clock.clone();
                        let state = state.clone();
                        async move {
                            loop {
                                if let Ok(stream) = listener.accept().await {
                                    // Always let the new peer to send a handshake message.
                                    // Only then we can decide whether we should accept a connection.
                                    // It is expected to be reasonably cheap: eventually, for TIER2 network
                                    // we would like to exchange set of connected peers even without establishing
                                    // a proper connection.
                                    tracing::debug!(target: "network", from = ?stream.peer_addr, "got new connection");
                                    if let Err(err) =
                                        PeerActor::spawn(clock.clone(), stream, None, state.clone())
                                    {
                                        tracing::info!(target:"network", ?err, "PeerActor::spawn()");
                                    }
                                }
                            }
                        }
                    });
                }
                if let Some(cfg) = state.config.tier1.clone() {
                    // Connect to TIER1 proxies and broadcast the list those connections periodically.
                    arbiter.spawn({
                        let clock = clock.clone();
                        let state = state.clone();
                        let mut interval = time::Interval::new(clock.now(), cfg.advertise_proxies_interval);
                        async move {
                            loop {
                                interval.tick(&clock).await;
                                state.tier1_request_full_sync();
                                state.tier1_advertise_proxies(&clock).await;
                            }
                        }
                    });
                    // Update TIER1 connections periodically.
                    arbiter.spawn({
                        let clock = clock.clone();
                        let state = state.clone();
                        let mut interval = tokio::time::interval(cfg.connect_interval.try_into().unwrap());
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        async move {
                            loop {
                                interval.tick().await;
                                state.tier1_connect(&clock).await;
                            }
                        }
                    });
                }
                // Periodically poll the connection store for connections we'd like to re-establish
                arbiter.spawn({
                    let clock = clock.clone();
                    let state = state.clone();
                    let arbiter = arbiter.clone();
                    let mut interval = time::Interval::new(clock.now(), POLL_CONNECTION_STORE_INTERVAL);
                    async move {
                        loop {
                            interval.tick(&clock).await;
                            // Poll the NetworkState for all pending reconnect attempts
                            let pending_reconnect = state.poll_pending_reconnect();
                            // Spawn a separate reconnect loop for each pending reconnect attempt
                            for peer_info in pending_reconnect {
                                arbiter.spawn({
                                    let state = state.clone();
                                    let clock = clock.clone();
                                    let peer_info = peer_info.clone();
                                    async move {
                                        state.reconnect(clock, peer_info, MAX_RECONNECT_ATTEMPTS).await;
                                    }
                                });

                                state.config.event_sink.push(Event::ReconnectLoopSpawned(peer_info));
                            }
                        }
                    }
                });
            }
        });
        Ok(Self::start_in_arbiter(&arbiter, move |_ctx| Self {
            my_peer_id: my_peer_id.clone(),
            started_connect_attempts: false,
            state,
            clock,
        }))
    }

    /// Periodically prints bandwidth stats for each peer.
    fn report_bandwidth_stats_trigger(
        &mut self,
        ctx: &mut actix::Context<Self>,
        every: time::Duration,
    ) {
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
                tracing::debug!(target: "bandwidth",
                    ?peer_id,
                    bandwidth_used, msg_received_count, "Peer bandwidth exceeded threshold",
                );
            }
            total_bandwidth_used_by_all_peers += bandwidth_used;
            total_msg_received_count += msg_received_count;
        }

        tracing::info!(
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

        (total_connections < self.state.config.ideal_connections_lo as usize
            || (total_connections < self.state.config.max_num_peers as usize
                && potential_outbound_connections
                    < self.state.config.minimum_outbound_peers as usize))
            && !self.state.config.outbound_disabled
    }

    /// Returns peers close to the highest height
    fn highest_height_peers(&self) -> Vec<HighestHeightPeerInfo> {
        let infos: Vec<HighestHeightPeerInfo> = self
            .state
            .tier2
            .load()
            .ready
            .values()
            .filter_map(|p| p.full_peer_info().into())
            .collect();

        // This finds max height among peers, and returns one peer close to such height.
        let max_height = match infos.iter().map(|i| i.highest_block_height).max() {
            Some(height) => height,
            None => return vec![],
        };
        // Find all peers whose height is within `highest_peer_horizon` from max height peer(s).
        infos
            .into_iter()
            .filter(|i| {
                i.highest_block_height.saturating_add(self.state.config.highest_peer_horizon)
                    >= max_height
            })
            .collect()
    }

    // Get peers that are potentially unreliable and we should avoid routing messages through them.
    // Currently we're picking the peers that are too much behind (in comparison to us).
    fn unreliable_peers(&self) -> HashSet<PeerId> {
        // If chain info is not set, that means we haven't received chain info message
        // from chain yet. Return empty set in that case. This should only last for a short period
        // of time.
        let binding = self.state.chain_info.load();
        let chain_info = if let Some(it) = binding.as_ref() {
            it
        } else {
            return HashSet::new();
        };
        let my_height = chain_info.block.header().height();
        // Find all peers whose height is below `highest_peer_horizon` from max height peer(s).
        // or the ones we don't have height information yet
        self.state
            .tier2
            .load()
            .ready
            .values()
            .filter(|p| {
                p.last_block
                    .load()
                    .as_ref()
                    .map(|x| x.height.saturating_add(UNRELIABLE_PEER_HORIZON) < my_height)
                    .unwrap_or(false)
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
        let whitelisted_peers = filter_peers(&|p| self.state.is_peer_whitelisted(&p.peer_info));
        safe_set.extend(whitelisted_peers);

        // If there is not enough non-whitelisted peers, return without disconnecting anyone.
        if tier2.ready.len() - safe_set.len() <= self.state.config.ideal_connections_hi as usize {
            return;
        }

        // If there is not enough outbound peers, add them to the safe set.
        let outbound_peers = filter_peers(&|p| p.peer_type == PeerType::Outbound);
        if outbound_peers.len() + tier2.outbound_handshakes.len()
            <= self.state.config.minimum_outbound_peers as usize
        {
            safe_set.extend(outbound_peers);
        }

        // If there is not enough archival peers, add them to the safe set.
        if self.state.config.archive {
            let archival_peers = filter_peers(&|p| p.archival);
            if archival_peers.len()
                <= self.state.config.archival_peer_connections_lower_bound as usize
            {
                safe_set.extend(archival_peers);
            }
        }

        // Find all recently active peers.
        let now = self.clock.now();
        let mut active_peers: Vec<Arc<connection::Connection>> = tier2
            .ready
            .values()
            .filter(|p| {
                now - p.last_time_received_message.load()
                    < self.state.config.peer_recent_time_window
            })
            .cloned()
            .collect();

        // Sort by established time.
        active_peers.sort_by_key(|p| p.established_time);
        // Saturate safe set with recently active peers.
        let set_limit = self.state.config.safe_set_size as usize;
        for p in active_peers {
            if safe_set.len() >= set_limit {
                break;
            }
            safe_set.insert(p.peer_info.id.clone());
        }

        // Build valid candidate list to choose the peer to be removed. All peers outside the safe set.
        let candidates = tier2.ready.values().filter(|p| !safe_set.contains(&p.peer_info.id));
        if let Some(p) = candidates.choose(&mut rand::thread_rng()) {
            tracing::debug!(target: "network", id = ?p.peer_info.id,
                tier2_len = tier2.ready.len(),
                ideal_connections_hi = self.state.config.ideal_connections_hi,
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
        ctx: &mut actix::Context<Self>,
        mut interval: time::Duration,
        (default_interval, max_interval): (time::Duration, time::Duration),
    ) {
        let _span = tracing::trace_span!(target: "network", "monitor_peers_trigger").entered();
        let _timer =
            metrics::PEER_MANAGER_TRIGGER_TIME.with_label_values(&["monitor_peers"]).start_timer();

        self.state.peer_store.update(&self.clock);

        if self.is_outbound_bootstrap_needed() {
            let tier2 = self.state.tier2.load();
            // With some odds - try picking one of the 'NotConnected' peers -- these are the ones that we were able to connect to in the past.
            let prefer_previously_connected_peer =
                thread_rng().gen_bool(PREFER_PREVIOUSLY_CONNECTED_PEER);
            if let Some(peer_info) = self.state.peer_store.unconnected_peer(
                |peer_state| {
                    // Ignore connecting to ourself
                    self.my_peer_id == peer_state.peer_info.id
                    || self.state.config.node_addr.as_ref().map(|a|**a) == peer_state.peer_info.addr
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
                            let stream = tcp::Stream::connect(&peer_info, tcp::Tier::T2).await.context("tcp::Stream::connect()")?;
                            PeerActor::spawn_and_handshake(clock.clone(),stream,None,state.clone()).await.context("PeerActor::spawn()")?;
                            anyhow::Ok(())
                        }.await;

                        if result.is_err() {
                            tracing::info!(target:"network", ?result, "failed to connect to {peer_info}");
                        }
                        if state.peer_store.peer_connection_attempt(&clock, &peer_info.id, result).is_err() {
                            tracing::error!(target: "network", ?peer_info, "Failed to store connection attempt.");
                        }
                    }.instrument(tracing::trace_span!(target: "network", "monitor_peers_trigger_connect"))
                }));
            }
        }

        // If there are too many active connections try to remove some connections
        self.maybe_stop_active_connection();

        // Find peers that are not reliable (too much behind) - and make sure that we're not routing messages through them.
        let unreliable_peers = self.unreliable_peers();
        metrics::PEER_UNRELIABLE.set(unreliable_peers.len() as i64);
        self.state.set_unreliable_peers(unreliable_peers);

        let new_interval = min(max_interval, interval * EXPONENTIAL_BACKOFF_RATIO);

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.monitor_peers_trigger(ctx, new_interval, (default_interval, max_interval));
            },
        );
    }

    /// Re-establish each outbound connection in the connection store (single attempt)
    fn bootstrap_outbound_from_recent_connections(&self, ctx: &mut actix::Context<Self>) {
        for conn_info in self.state.connection_store.get_recent_outbound_connections() {
            ctx.spawn(wrap_future({
                let state = self.state.clone();
                let clock = self.clock.clone();
                let peer_info = conn_info.peer_info.clone();
                async move {
                    state.reconnect(clock, peer_info, 1).await;
                }
            }));

            self.state
                .config
                .event_sink
                .push(Event::ReconnectLoopSpawned(conn_info.peer_info.clone()));
        }
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
            AccountOrPeerIdOrHash::Hash(it) => PeerIdOrHash::Hash(*it),
        };

        self.state.send_message_to_peer(
            &self.clock,
            tcp::Tier::T2,
            self.state.sign_message(&self.clock, RawRoutedMessage { target, body: msg }),
        )
    }

    pub(crate) fn get_network_info(&self) -> NetworkInfo {
        let tier1 = self.state.tier1.load();
        let tier2 = self.state.tier2.load();
        let now = self.clock.now();
        let graph = self.state.graph.load();
        let connected_peer = |cp: &Arc<connection::Connection>| ConnectedPeerInfo {
            full_peer_info: cp.full_peer_info(),
            received_bytes_per_sec: cp.stats.received_bytes_per_sec.load(Ordering::Relaxed),
            sent_bytes_per_sec: cp.stats.sent_bytes_per_sec.load(Ordering::Relaxed),
            last_time_peer_requested: cp.last_time_peer_requested.load().unwrap_or(now),
            last_time_received_message: cp.last_time_received_message.load(),
            connection_established_time: cp.established_time,
            peer_type: cp.peer_type,
            nonce: match graph.local_edges.get(&cp.peer_info.id) {
                Some(e) => e.nonce(),
                None => 0,
            },
        };
        NetworkInfo {
            connected_peers: tier2.ready.values().map(connected_peer).collect(),
            tier1_connections: tier1.ready.values().map(connected_peer).collect(),
            num_connected_peers: tier2.ready.len(),
            peer_max_count: self.state.config.max_num_peers,
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
                .account_announcements
                .get_announcements()
                .into_iter()
                .map(|announce_account| KnownProducer {
                    account_id: announce_account.account_id,
                    peer_id: announce_account.peer_id.clone(),
                    // TODO: fill in the address.
                    addr: None,
                    next_hops: self.state.graph.routing_table.view_route(&announce_account.peer_id),
                })
                .collect(),
            tier1_accounts_keys: self.state.accounts_data.load().keys.iter().cloned().collect(),
            tier1_accounts_data: self.state.accounts_data.load().data.values().cloned().collect(),
        }
    }

    fn push_network_info_trigger(&self, ctx: &mut actix::Context<Self>, interval: time::Duration) {
        let _span = tracing::trace_span!(target: "network", "push_network_info_trigger").entered();
        let network_info = self.get_network_info();
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["push_network_info"])
            .start_timer();
        // TODO(gprusak): just spawn a loop.
        let state = self.state.clone();
        ctx.spawn(wrap_future(
            async move { state.client.network_info(network_info).await }.instrument(
                tracing::trace_span!(target: "network", "push_network_info_trigger_future"),
            ),
        ));

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
        ctx: &mut actix::Context<Self>,
    ) -> NetworkResponses {
        let msg_type: &str = msg.as_ref();
        let _span =
            tracing::trace_span!(target: "network", "handle_msg_network_requests", msg_type)
                .entered();
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
                let body = RoutedMessageBody::VersionedStateResponse(response);
                if self.state.send_message_to_peer(
                    &self.clock,
                    tcp::Tier::T2,
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
                let state = self.state.clone();
                ctx.spawn(wrap_future(async move {
                    state.add_accounts(vec![announce_account]).await;
                }));
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
                            let last_block = peer.last_block.load();
                            if (peer.archival || !target.only_archival)
                                && last_block.is_some()
                                && last_block.as_ref().unwrap().height >= target.min_height
                                && peer.tracked_shards.contains(&target.shard_id)
                            {
                                matching_peers.push(peer_id.clone());
                            }
                        }

                        if let Some(matching_peer) = matching_peers.iter().choose(&mut thread_rng())
                        {
                            if self.state.send_message_to_peer(
                                &self.clock,
                                tcp::Tier::T2,
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
                            tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find any matching peer for chunk");
                        }
                    }
                }

                if success {
                    NetworkResponses::NoResponse
                } else {
                    tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find a route for chunk");
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                if self.state.send_message_to_peer(
                    &self.clock,
                    tcp::Tier::T2,
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

    fn handle_peer_manager_message(
        &mut self,
        msg: PeerManagerMessageRequest,
        ctx: &mut actix::Context<Self>,
    ) -> PeerManagerMessageResponse {
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
            PeerManagerMessageRequest::FetchRoutingTable => {
                PeerManagerMessageResponse::FetchRoutingTable(self.state.graph.routing_table.info())
            }
        }
    }
}

impl actix::Handler<WithSpanContext<SetChainInfo>> for PeerManagerActor {
    type Result = ();
    fn handle(&mut self, msg: WithSpanContext<SetChainInfo>, ctx: &mut Self::Context) {
        let (_span, SetChainInfo(info)) = handler_trace_span!(target: "network", msg);
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&["SetChainInfo"]).start_timer();
        // We call self.state.set_chain_info()
        // synchronously, therefore, assuming actix in-order delivery,
        // there will be no race condition between subsequent SetChainInfo
        // calls.
        if !self.state.set_chain_info(info) {
            // We early exit in case the set of TIER1 account keys hasn't changed.
            return;
        }

        let state = self.state.clone();
        let clock = self.clock.clone();
        ctx.spawn(wrap_future(
            async move {
                // This node might have become a TIER1 node due to the change of the key set.
                // If so we should recompute and readvertise the list of proxies.
                // This is mostly important in case a node is its own proxy. In all other cases
                // (when proxies are different nodes) the update of the key set happens asynchronously
                // and this node won't be able to connect to proxies until it happens (and only the
                // connected proxies are included in the advertisement). We run tier1_advertise_proxies
                // periodically in the background anyway to cover those cases.
                state.tier1_advertise_proxies(&clock).await;
            }
            .in_current_span(),
        ));
    }
}

impl actix::Handler<WithSpanContext<PeerManagerMessageRequest>> for PeerManagerActor {
    type Result = PeerManagerMessageResponse;
    fn handle(
        &mut self,
        msg: WithSpanContext<PeerManagerMessageRequest>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        let msg_type: &str = (&msg.msg).into();
        let (_span, msg) = handler_debug_span!(target: "network", msg, msg_type);
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&[(&msg).into()]).start_timer();
        self.handle_peer_manager_message(msg, ctx)
    }
}

impl actix::Handler<GetDebugStatus> for PeerManagerActor {
    type Result = DebugStatus;
    fn handle(&mut self, msg: GetDebugStatus, _ctx: &mut actix::Context<Self>) -> Self::Result {
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
            GetDebugStatus::Graph => DebugStatus::Graph(NetworkGraphView {
                edges: self
                    .state
                    .graph
                    .load()
                    .edges
                    .values()
                    .map(|edge| {
                        let key = edge.key();
                        EdgeView { peer0: key.0.clone(), peer1: key.1.clone(), nonce: edge.nonce() }
                    })
                    .collect(),
                next_hops: (*self.state.graph.routing_table.info().next_hops).clone(),
            }),
            GetDebugStatus::RecentOutboundConnections => {
                DebugStatus::RecentOutboundConnections(RecentOutboundConnectionsView {
                    recent_outbound_connections: self
                        .state
                        .connection_store
                        .get_recent_outbound_connections()
                        .iter()
                        .map(|c| ConnectionInfoView {
                            peer_id: c.peer_info.id.clone(),
                            addr: format!("{:?}", c.peer_info.addr),
                            time_established: c.time_established.unix_timestamp(),
                            time_connected_until: c.time_connected_until.unix_timestamp(),
                        })
                        .collect::<Vec<_>>(),
                })
            }
            GetDebugStatus::Routes => DebugStatus::Routes(self.state.graph_v2.get_debug_view()),
        }
    }
}
