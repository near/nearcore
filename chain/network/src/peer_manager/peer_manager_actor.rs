use crate::client::{
    ClientSenderForNetwork, GetCurrentEpochHeight, SetNetworkInfo, SpiceChunkEndorsementMessage,
    StateRequestHeader, StateRequestPart,
};
use crate::config;
use crate::debug::{DebugStatus, GetDebugStatus};
use crate::network_protocol::{self, T2MessageBody};
use crate::network_protocol::{
    Disconnect, Edge, PeerIdOrHash, PeerMessage, Ping, Pong, RawRoutedMessage, StateHeaderRequest,
    StatePartRequest, StateRequestAck,
};
use crate::network_protocol::{SyncSnapshotHosts, T1MessageBody};
use crate::peer_manager::connected_peers::ConnectedPeerState;
use crate::peer_manager::network_state::{
    NetworkState, PENDING_TIER3_REQUEST_TIMEOUT, WhitelistNode,
};
use crate::peer_manager::network_transport::{NetworkTransport, PeerTransportStats};
use crate::peer_manager::peer_store;
use crate::peer_manager::tcp_transport::TcpTransport;
use crate::routing::GraphSnapshot;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::spice_data_distribution::SpiceDataDistributorSenderForNetwork;
use crate::state_witness::PartialWitnessSenderForNetwork;
use crate::stats::metrics;
use crate::store;
use crate::tcp;
use crate::types::{
    ConnectedPeerInfo, FullPeerInfo, HighestHeightPeerInfo, KnownProducer, NetworkInfo,
    NetworkRequests, NetworkResponses, PeerChainInfo, PeerInfo, PeerManagerMessageRequest,
    PeerManagerMessageResponse, PeerManagerSenderForNetwork, PeerType, SetChainInfo,
    SnapshotHostEvent, SnapshotHostInfo, StateHeaderRequestBody, StatePartRequestBody,
    StateRequestSenderForNetwork, StateSyncEvent, Tier3Request, Tier3RequestBody,
};
use ::time::ext::InstantExt as _;
use anyhow::Context as _;
use itertools::Itertools;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt, FutureSpawnerExt};
use near_async::messaging::{self, CanSendAsync, Sender};
use near_async::tokio::TokioRuntimeHandle;
use near_async::{ActorSystem, time};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::genesis::GenesisId;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::state_sync::{PartIdOrHeader, StateRequestAckBody};
use near_primitives::views::{
    ConnectionInfoView, EdgeView, KnownPeerStateView, NetworkGraphView, PeerStoreView,
    RecentOutboundConnectionsView, SnapshotHostInfoView, SnapshotHostsView,
};
use network_protocol::MAX_SHARDS_PER_SNAPSHOT_HOST_INFO;
use rand::Rng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::thread_rng;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::Instrument as _;

/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The initial waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_INITIAL_DURATION: time::Duration = time::Duration::milliseconds(10);
/// How often should we check whether local edges match the connection pool.
const FIX_LOCAL_EDGES_INTERVAL: time::Duration = time::Duration::seconds(60);
/// How much time we give fix_local_edges() to resolve the discrepancies, before forcing disconnect.
const FIX_LOCAL_EDGES_TIMEOUT: time::Duration = time::Duration::seconds(6);

/// Number of times to attempt reconnection when trying to re-establish a connection.
const MAX_RECONNECT_ATTEMPTS: usize = 6;

/// How often to report bandwidth stats.
const REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL: time::Duration =
    time::Duration::milliseconds(60_000);

/// If a peer's average bytes/sec exceeds this threshold, its bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_BYTES_PER_SEC: u64 = 10_000_000 / 60;
/// If a peer's average messages/sec exceeds this threshold, its bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_COUNT_PER_SEC: u64 = 10_000 / 60;

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

/// The length of time that a Tier3 connection is allowed to idle before it is stopped
const TIER3_IDLE_TIMEOUT: time::Duration = time::Duration::seconds(15);

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    pub(crate) clock: time::Clock,
    /// Handle to spawning futures as well as stopping ourselves for testing.
    pub(crate) handle: TokioRuntimeHandle<Self>,
    /// Peer information for this node.
    my_peer_id: PeerId,
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,

    /// State that is shared between multiple threads (including PeerActors).
    pub(crate) state: Arc<NetworkState>,
    /// Transport for sending/broadcasting messages.
    pub(crate) transport: Arc<dyn NetworkTransport>,
}

/// TEST-ONLY
/// A generic set of events (observable in tests) that the Network may generate.
/// Ideally the tests should observe only public API properties, but until
/// we are at that stage, feel free to add any events that you need to observe.
/// In particular prefer emitting a new event to polling for a state change.
#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::large_enum_variant)]
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

impl messaging::Actor for PeerManagerActor {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        // Periodically push network information to client.
        self.push_network_info_trigger(self.state.config.push_info_period);

        // Attempt to reconnect to recent outbound connections from storage
        if self.state.config.connect_to_reliable_peers_on_startup {
            tracing::debug!(target: "network", "reconnecting to reliable peers from storage");
            self.bootstrap_outbound_from_recent_connections();
        } else {
            tracing::debug!(target: "network", "skipping reconnection to reliable peers");
        }

        // Periodically starts peer monitoring.
        tracing::debug!(target: "network",
               max_period=?self.state.config.monitor_peers_max_period,
               "monitor_peers_trigger");
        self.monitor_peers_trigger(
            MONITOR_PEERS_INITIAL_DURATION,
            (MONITOR_PEERS_INITIAL_DURATION, self.state.config.monitor_peers_max_period),
        );

        // Periodically fix local edges.
        let clock = self.clock.clone();
        let state = self.state.clone();
        let transport = self.transport.clone();
        self.handle.spawn("fix_local_edges loop", async move {
            let mut interval = time::Interval::new(clock.now(), FIX_LOCAL_EDGES_INTERVAL);
            loop {
                interval.tick(&clock).await;
                state.fix_local_edges(&clock, FIX_LOCAL_EDGES_TIMEOUT, transport.clone()).await;
            }
        });

        // Periodically update the connection store.
        let clock = self.clock.clone();
        let state = self.state.clone();
        self.handle.spawn("update_connection_store loop", async move {
            let mut interval = time::Interval::new(clock.now(), UPDATE_CONNECTION_STORE_INTERVAL);
            loop {
                interval.tick(&clock).await;
                state.update_connection_store(&clock);
            }
        });

        // Periodically prints bandwidth stats for each peer.
        self.report_bandwidth_stats_trigger(REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL);

        // Connect to TIER1 proxies and broadcast the list those connections periodically.
        let tier1 = self.state.config.tier1.clone();
        self.handle.spawn("connect to TIER1 proxies", {
            let clock = self.clock.clone();
            let state = self.state.clone();
            let transport = self.transport.clone();
            let mut interval = time::Interval::new(clock.now(), tier1.advertise_proxies_interval);
            async move {
                loop {
                    interval.tick(&clock).await;
                    state.tier1_request_full_sync();
                    state.tier1_advertise_proxies(&clock, transport.as_ref()).await;
                }
            }
        });

        // Update TIER1 connections periodically.
        self.handle.spawn("update TIER1 connections", {
            let clock = self.clock.clone();
            let state = self.state.clone();
            let transport = self.transport.clone();
            let mut interval = tokio::time::interval(tier1.connect_interval.try_into().unwrap());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            async move {
                loop {
                    interval.tick().await;
                    state.tier1_connect(&clock, transport.as_ref()).await;
                }
            }
        });

        // Periodically poll the connection store for connections we'd like to re-establish
        self.handle.spawn("poll connection store for reconnects", {
            let clock = self.clock.clone();
            let transport = self.transport.clone();
            let state = self.state.clone();
            let handle = self.handle.clone();
            let mut interval = time::Interval::new(clock.now(), POLL_CONNECTION_STORE_INTERVAL);
            async move {
                loop {
                    interval.tick(&clock).await;
                    // Poll the NetworkState for all pending reconnect attempts
                    let pending_reconnect = state.poll_pending_reconnect();
                    // Spawn a separate reconnect loop for each pending reconnect attempt
                    for peer_info in pending_reconnect {
                        handle.spawn("reconnect peer", {
                            let state = state.clone();
                            let clock = clock.clone();
                            let peer_info = peer_info.clone();
                            let transport = transport.clone();
                            async move {
                                state
                                    .reconnect(clock, transport, peer_info, MAX_RECONNECT_ATTEMPTS)
                                    .await;
                            }
                        });

                        #[cfg(test)]
                        state.config.event_sink.send(Event::ReconnectLoopSpawned(peer_info));
                    }
                }
            }
        });

        #[cfg(test)]
        self.state.config.event_sink.send(Event::PeerManagerStarted);
    }

    /// Try to gracefully disconnect from connected peers.
    fn stop_actor(&mut self) {
        tracing::debug!(target: "network", "peer manager stopping");
        self.transport.broadcast_message(Arc::new(PeerMessage::Disconnect(Disconnect {
            remove_from_connection_store: false,
        })));
        self.transport.shutdown();
    }
}

/// Project a `ConnectedPeerState` to a `HighestHeightPeerInfo`, keyed by
/// the T2 peer's latest block. Returns `None` if the peer hasn't
/// reported a block yet (the height info is what makes the projection
/// interesting).
fn to_highest_height_peer_info(
    peer_state: &ConnectedPeerState,
    genesis_id: &GenesisId,
) -> Option<HighestHeightPeerInfo> {
    let block = peer_state.block_info.as_ref()?;
    Some(HighestHeightPeerInfo {
        peer_info: peer_state.peer_info.clone(),
        genesis_id: genesis_id.clone(),
        highest_block_height: block.height,
        highest_block_hash: block.hash,
        tracked_shards: peer_state.tracked_shards.clone(),
        archival: peer_state.archival,
    })
}

/// Joins `ConnectedPeerState` (business metadata) with per-peer stats
/// from `transport_info()` (bandwidth, last-seen timestamps) and the
/// routing graph (edge nonce) into a single `ConnectedPeerInfo` for
/// NetworkInfo RPC output.
fn build_connected_peer_info(
    peer_id: &PeerId,
    cp: &ConnectedPeerState,
    stats: &HashMap<PeerId, PeerTransportStats>,
    graph: &GraphSnapshot,
    genesis_id: &GenesisId,
    now: time::Instant,
) -> ConnectedPeerInfo {
    let s = stats.get(peer_id);
    let chain_info = PeerChainInfo {
        genesis_id: genesis_id.clone(),
        last_block: cp.block_info,
        tracked_shards: cp.tracked_shards.clone(),
        archival: cp.archival,
    };
    ConnectedPeerInfo {
        full_peer_info: FullPeerInfo { peer_info: cp.peer_info.clone(), chain_info },
        received_bytes_per_sec: s.map_or(0, |s| s.received_bytes_per_sec),
        sent_bytes_per_sec: s.map_or(0, |s| s.sent_bytes_per_sec),
        last_time_peer_requested: s.and_then(|s| s.last_time_peer_requested).unwrap_or(now),
        last_time_received_message: s.map_or(now, |s| s.last_time_received_message),
        connection_established_time: cp.established_time,
        peer_type: cp.peer_type,
        nonce: graph.local_edges.get(peer_id).map_or(0, |e| e.nonce()),
    }
}

impl PeerManagerActor {
    pub fn spawn(
        clock: time::Clock,
        actor_system: ActorSystem,
        store: Arc<dyn near_store::db::Database>,
        config: config::NetworkConfig,
        client: ClientSenderForNetwork,
        state_request_adapter: StateRequestSenderForNetwork,
        peer_manager_adapter: PeerManagerSenderForNetwork,
        shards_manager_adapter: Sender<ShardsManagerRequestFromNetwork>,
        partial_witness_adapter: PartialWitnessSenderForNetwork,
        spice_data_distributor_adapter: SpiceDataDistributorSenderForNetwork,
        spice_core_writer_adapter: Sender<SpiceChunkEndorsementMessage>,
        genesis_id: GenesisId,
    ) -> anyhow::Result<(TokioRuntimeHandle<Self>, Arc<TcpTransport>)> {
        let config = config.verify().context("config")?;
        let store = store::Store::from(store);
        let peer_store = peer_store::PeerStore::new(&clock, config.peer_store.clone())
            .context("PeerStore::new")?;
        tracing::debug!(target: "network",
               len = peer_store.len(),
               boot_nodes = config.peer_store.boot_nodes.len(),
               banned = peer_store.count_banned(),
               "found known peers");
        tracing::debug!(target: "network", blacklist = ?config.peer_store.blacklist);
        let whitelist_nodes = {
            let mut v = vec![];
            for wn in &config.whitelist_nodes {
                v.push(WhitelistNode::from_peer_info(wn)?);
            }
            v
        };
        let my_peer_id = config.node_id();
        let builder = actor_system.new_tokio_builder();
        let handle = builder.handle();
        let clock = clock;
        let state = Arc::new(NetworkState::new(
            &clock,
            &*handle.future_spawner(),
            store,
            peer_store,
            config,
            genesis_id,
            client,
            state_request_adapter,
            peer_manager_adapter,
            shards_manager_adapter,
            partial_witness_adapter,
            whitelist_nodes,
            spice_data_distributor_adapter,
            spice_core_writer_adapter,
        ));
        if let Some(addr) = state.config.tier3_public_addr {
            tracing::info!(target: "network", %addr, "using configured tier3 public address");
            metrics::TIER3_PUBLIC_ADDR.with_label_values(&[&addr.to_string()]).set(1);
        }
        handle.spawn("PeerManagerActor epoch height fetch", {
            let state = state.clone();
            async move {
                if let Ok(Some(epoch_height)) = state
                    .client
                    .current_epoch_height_request
                    .send_async(GetCurrentEpochHeight)
                    .await
                {
                    state.snapshot_hosts.set_current_epoch_height(epoch_height);
                }
            }
        });
        // Build the transport and start the TCP listener. The listener
        // lives inside TcpTransport so that PMA only ever holds
        // `Arc<dyn NetworkTransport>`, never the concrete type.
        let tcp = TcpTransport::new(state.clone(), clock.clone(), actor_system, handle.clone());
        tcp.start();
        let transport: Arc<dyn NetworkTransport> = tcp.clone();
        builder.spawn_tokio_actor(Self {
            my_peer_id,
            started_connect_attempts: false,
            state,
            transport,
            clock,
            handle: handle.clone(),
        });
        Ok((handle, tcp))
    }

    /// Periodically prints bandwidth stats for each peer.
    fn report_bandwidth_stats_trigger(&self, every: time::Duration) {
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["report_bandwidth_stats"])
            .start_timer();
        let info = self.transport.transport_info();
        let mut total_bytes_per_sec: u64 = 0;
        let mut total_messages_per_sec: u64 = 0;
        for (peer_id, stat) in &info.peer_stats {
            let bytes_per_sec = stat.received_bytes_per_sec;
            let messages_per_sec = stat.received_messages_per_sec;
            if bytes_per_sec > REPORT_BANDWIDTH_THRESHOLD_BYTES_PER_SEC
                || messages_per_sec > REPORT_BANDWIDTH_THRESHOLD_COUNT_PER_SEC
            {
                tracing::debug!(target: "bandwidth",
                    ?peer_id,
                    bytes_per_sec, messages_per_sec,
                    "peer bandwidth exceeded threshold",
                );
            }
            total_bytes_per_sec += bytes_per_sec;
            total_messages_per_sec += messages_per_sec;
        }

        tracing::info!(
            target: "bandwidth",
            total_bytes_per_sec,
            total_messages_per_sec,
        );

        self.handle.clone().run_later(
            "report_bandwidth_stats_trigger",
            every.try_into().unwrap(),
            move |act, _ctx| {
                act.report_bandwidth_stats_trigger(every);
            },
        );
    }

    /// Check if it is needed to create a new outbound connection.
    /// If the number of active connections is less than `ideal_connections_lo` or
    /// (the number of outgoing connections is less than `minimum_outbound_peers`
    ///     and the total connections is less than `max_num_peers`)
    fn is_outbound_bootstrap_needed(&self) -> bool {
        let pending = self.transport.transport_info().pending_outbound.len();
        let t2 = self.state.peers.tier2();
        let t2_count = t2.len();
        let t2_outbound = t2.values().filter(|s| s.peer_type == PeerType::Outbound).count();
        let total_connections = t2_count + pending;
        let potential_outbound_connections = t2_outbound + pending;

        (total_connections < self.state.config.ideal_connections_lo as usize
            || (total_connections < self.state.config.max_num_peers as usize
                && potential_outbound_connections
                    < self.state.config.minimum_outbound_peers as usize))
            && !self.state.config.outbound_disabled
    }

    /// Returns peers close to the highest height.
    fn highest_height_peers(&self) -> Vec<HighestHeightPeerInfo> {
        let genesis_id = self.state.genesis_id.clone();
        let infos: Vec<HighestHeightPeerInfo> = self
            .state
            .peers
            .tier2()
            .values()
            .filter_map(|peer_state| to_highest_height_peer_info(peer_state, &genesis_id))
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
            .peers
            .tier2()
            .into_iter()
            .filter(|(_, s)| {
                s.block_info
                    .as_ref()
                    .map(|x| x.height.saturating_add(UNRELIABLE_PEER_HORIZON) < my_height)
                    .unwrap_or(false)
            })
            .map(|(id, _)| id)
            .collect()
    }

    /// Check if the number of connections (excluding whitelisted ones) exceeds ideal_connections_hi.
    /// If so, constructs a safe set of peers and selects one random peer outside of that set
    /// and sends signal to stop connection to it gracefully.
    ///
    /// Safe set construction process:
    /// 1. Add all whitelisted peers to the safe set.
    /// 2. If the number of outbound connections is less or equal than minimum_outbound_connections,
    ///    add all outbound connections to the safe set.
    /// 3. Find all peers who sent us a message within the last peer_recent_time_window,
    ///    and add them one by one to the safe_set (starting from earliest connection time)
    ///    until safe set has safe_set_size elements.
    fn maybe_stop_active_connection(&self) {
        let info = self.transport.transport_info();
        let stats = &info.peer_stats;
        let t2_peers = self.state.peers.tier2();
        let t2_count = t2_peers.len();

        // Build safe set
        let mut safe_set = HashSet::new();

        // Add whitelisted nodes to the safe set.
        for (id, s) in &t2_peers {
            if self.state.is_peer_whitelisted(&s.peer_info) {
                safe_set.insert(id.clone());
            }
        }

        // If there is not enough non-whitelisted peers, return without disconnecting anyone.
        if t2_count - safe_set.len() <= self.state.config.ideal_connections_hi as usize {
            return;
        }

        // If there is not enough outbound peers, add them to the safe set.
        let outbound_peers = t2_peers
            .iter()
            .filter(|(_, s)| s.peer_type == PeerType::Outbound)
            .map(|(id, _)| id.clone())
            .collect_vec();
        if outbound_peers.len() + info.pending_outbound.len()
            <= self.state.config.minimum_outbound_peers as usize
        {
            safe_set.extend(outbound_peers);
        }

        // If there is not enough archival peers, add them to the safe set.
        if self.state.config.archive {
            let archival_count = t2_peers.iter().filter(|(_, s)| s.archival).count();
            if archival_count <= self.state.config.archival_peer_connections_lower_bound as usize {
                let archival_ids = t2_peers
                    .iter()
                    .filter(|(_, s)| s.archival)
                    .map(|(id, _)| id.clone())
                    .collect_vec();
                safe_set.extend(archival_ids);
            }
        }

        // Find all recently active peers, sorted by established time.
        let now = self.clock.now();
        let mut active_peers: Vec<(&PeerId, time::Instant)> = t2_peers
            .iter()
            .filter_map(|(id, s)| {
                let stat = stats.get(id)?;
                let is_recent = now - stat.last_time_received_message
                    < self.state.config.peer_recent_time_window;
                is_recent.then_some((id, s.established_time))
            })
            .collect();
        active_peers.sort_by_key(|(_, established_time)| *established_time);

        // Saturate safe set with recently active peers.
        let set_limit = self.state.config.safe_set_size as usize;
        for (id, _) in active_peers {
            if safe_set.len() >= set_limit {
                break;
            }
            safe_set.insert(id.clone());
        }

        // Build valid candidate list: all peers outside the safe set.
        let candidates: Vec<&PeerId> =
            t2_peers.keys().filter(|id| !safe_set.contains(*id)).collect();
        if let Some(id) = candidates.choose(&mut rand::thread_rng()) {
            tracing::debug!(target: "network", ?id,
                t2_count,
                ideal_connections_hi = self.state.config.ideal_connections_hi,
                "stopping active connection"
            );
            self.transport.disconnect_peer(id, None);
        }
    }

    /// TIER3 connections are established ad-hoc to transmit individual large messages.
    /// Here we terminate these "single-purpose" connections after an idle timeout.
    ///
    /// When a TIER3 connection is established the intended message is already prepared in-memory,
    /// so there is no concern of the timeout falling in between the handshake and the payload.
    ///
    /// A finer detail is that as long as a TIER3 connection remains open it can be reused to
    /// transmit additional TIER3 payloads intended for the same peer. In such cases the message
    /// can be lost if the timeout is reached precisely while it is in flight. For simplicity we
    /// accept this risk; network requests are understood as unreliable and the requesting node has
    /// retry logic anyway. TODO(saketh): consider if we can improve this in a simple way.
    fn stop_tier3_idle_connections(&self) {
        let now = self.clock.now();
        let info = self.transport.transport_info();
        let t3_peers = self.state.peers.tier3();
        let idle_peers: Vec<PeerId> = t3_peers
            .into_iter()
            .filter_map(|(id, _)| {
                let stat = info.peer_stats.get(&id)?;
                (now - stat.last_time_received_message > TIER3_IDLE_TIMEOUT).then_some(id)
            })
            .collect();
        for peer_id in &idle_peers {
            self.transport.disconnect_peer(peer_id, None);
        }
        // Clean up stale pending Tier3 request entries for peers that never connected back.
        // retain() does a full scan of the map, but this is fine: the map is bounded by the
        // number of in-flight state sync requests (typically tens at most).
        self.state
            .pending_tier3_requests
            .retain(|_, sent_at| now - *sent_at <= PENDING_TIER3_REQUEST_TIMEOUT);
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - un-ban peers that have been banned for awhile,
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
        mut interval: time::Duration,
        (default_interval, max_interval): (time::Duration, time::Duration),
    ) {
        let _span = tracing::trace_span!(target: "network", "monitor_peers_trigger").entered();
        let _timer =
            metrics::PEER_MANAGER_TRIGGER_TIME.with_label_values(&["monitor_peers"]).start_timer();

        self.state.peer_store.update(&self.clock);

        if self.is_outbound_bootstrap_needed() {
            let pending_outbound = self.transport.transport_info().pending_outbound;
            // With some odds - try picking one of the 'NotConnected' peers -- these are the ones that we were able to connect to in the past.
            let prefer_previously_connected_peer =
                thread_rng().gen_bool(PREFER_PREVIOUSLY_CONNECTED_PEER);
            if let Some(peer_info) = self.state.peer_store.unconnected_peer(
                |peer_state| {
                    // Ignore connecting to ourself
                    self.my_peer_id == peer_state.peer_info.id
                    || self.state.config.node_addr.as_ref().map(|a|**a) == peer_state.peer_info.addr
                    // Or to peers we are currently trying to connect to
                    || pending_outbound.contains(&peer_state.peer_info.id)
                },
                prefer_previously_connected_peer,
            ) {
                // Start monitor_peers_attempts from start after we discover the first healthy peer
                if !self.started_connect_attempts {
                    self.started_connect_attempts = true;
                    interval = default_interval;
                }
                self.handle.spawn("monitor_peers_trigger_connect", {
                    let state = self.state.clone();
                    let clock = self.clock.clone();
                    let transport = self.transport.clone();
                    async move {
                        let result = transport
                            .connect_to_peer(&clock, peer_info.clone(), tcp::Tier::T2)
                            .await
                            .map_err(|err| anyhow::anyhow!("connect_to_peer: {err:?}"));

                        if let Err(ref err) = result {
                            tracing::info!(target: "network", %err, %peer_info, "tier2 failed to connect");
                        }
                        state.peer_store.peer_connection_attempt(&clock, &peer_info.id, result);
                    }.instrument(tracing::trace_span!(target: "network", "monitor_peers_trigger_connect"))
                });
            }
        }

        // If there are too many active connections try to remove some connections
        self.maybe_stop_active_connection();

        // Close Tier3 connections which have been idle for too long
        self.stop_tier3_idle_connections();

        // Find peers that are not reliable (too much behind) - and make sure that we're not routing messages through them.
        let unreliable_peers = self.unreliable_peers();
        metrics::PEER_UNRELIABLE.set(unreliable_peers.len() as i64);
        self.state.set_unreliable_peers(unreliable_peers);

        let new_interval = min(max_interval, interval * EXPONENTIAL_BACKOFF_RATIO);

        self.handle.clone().run_later(
            "monitor_peers_trigger",
            interval.try_into().unwrap(),
            move |act, _| {
                act.monitor_peers_trigger(new_interval, (default_interval, max_interval));
            },
        );
    }

    /// Re-establish each outbound connection in the connection store (single attempt)
    fn bootstrap_outbound_from_recent_connections(&self) {
        for conn_info in self.state.connection_store.get_recent_outbound_connections() {
            self.handle.spawn("bootstrap_outbound_from_recent_connections", {
                let state = self.state.clone();
                let clock = self.clock.clone();
                let transport = self.transport.clone();
                let peer_info = conn_info.peer_info.clone();
                async move {
                    state.reconnect(clock, transport, peer_info, 1).await;
                }
            });

            #[cfg(test)]
            self.state
                .config
                .event_sink
                .send(Event::ReconnectLoopSpawned(conn_info.peer_info.clone()));
        }
    }

    pub(crate) fn get_network_info(&self) -> NetworkInfo {
        let now = self.clock.now();
        let graph = self.state.graph.load();
        let genesis_id = self.state.genesis_id.clone();

        let info = self.transport.transport_info();
        let stats = &info.peer_stats;

        let t2_snapshot = self.state.peers.tier2();
        let t1_snapshot = self.state.peers.tier1();

        let build = |peer_id: &PeerId, cp: &ConnectedPeerState| -> ConnectedPeerInfo {
            build_connected_peer_info(peer_id, cp, stats, &graph, &genesis_id, now)
        };
        let t2_infos: Vec<ConnectedPeerInfo> =
            t2_snapshot.iter().map(|(id, s)| build(id, s)).collect();
        let t1_infos: Vec<ConnectedPeerInfo> =
            t1_snapshot.iter().map(|(id, s)| build(id, s)).collect();

        let num_connected = t2_infos.len();
        let sent_total: u64 = t2_infos.iter().map(|p| p.sent_bytes_per_sec).sum();
        let recv_total: u64 = t2_infos.iter().map(|p| p.received_bytes_per_sec).sum();

        NetworkInfo {
            connected_peers: t2_infos,
            tier1_connections: t1_infos,
            num_connected_peers: num_connected,
            peer_max_count: self.state.config.max_num_peers,
            highest_height_peers: self.highest_height_peers(),
            sent_bytes_per_sec: sent_total,
            received_bytes_per_sec: recv_total,
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

    fn push_network_info_trigger(&self, interval: time::Duration) {
        let _span = tracing::trace_span!(target: "network", "push_network_info_trigger").entered();
        let network_info = self.get_network_info();
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["push_network_info"])
            .start_timer();
        // TODO(gprusak): just spawn a loop.
        let state = self.state.clone();
        self.handle.spawn(
            "push_network_info_trigger_future",
            async move {
                state.client.send_async(SetNetworkInfo(network_info).span_wrap()).await.ok();
            }
            .instrument(
                tracing::trace_span!(target: "network", "push_network_info_trigger_future"),
            ),
        );

        self.handle.clone().run_later(
            "push_network_info_trigger",
            interval.try_into().unwrap(),
            move |act, _| {
                act.push_network_info_trigger(interval);
            },
        );
    }

    fn handle_msg_network_requests(&self, msg: NetworkRequests) -> NetworkResponses {
        let msg_type: &str = msg.as_ref();
        let _span =
            tracing::trace_span!(target: "network", "handle_msg_network_requests", msg_type)
                .entered();
        metrics::REQUEST_COUNT_BY_TYPE_TOTAL.with_label_values(&[msg.as_ref()]).inc();
        match msg {
            NetworkRequests::Block { block } => {
                self.transport.broadcast_message(Arc::new(PeerMessage::Block(block)));
                NetworkResponses::NoResponse
            }
            NetworkRequests::OptimisticBlock { chunk_producers, optimistic_block } => {
                // TODO(saketh): the chunk_producers are identified by their validator AccountId,
                // but OptimisticBlock is sent over a direct PeerMessage. Hence we have to perform
                // a conversion here from AccountId to peer connection. Consider reworking this.
                let msg = Arc::new(PeerMessage::OptimisticBlock(optimistic_block));
                for target_account in &*chunk_producers {
                    if let Some(conn) = self.state.get_tier1_proxy_for_account_id(&target_account) {
                        conn.send_message(msg.clone());
                    }
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::Approval { approval_message } => {
                self.state.send_message_to_account(
                    &self.clock,
                    &approval_message.target,
                    T1MessageBody::BlockApproval(approval_message.approval).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                if self.transport.send_message(
                    tcp::Tier::T2,
                    peer_id,
                    Arc::new(PeerMessage::BlockRequest(hash)),
                ) {
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
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, sync_prev_prev_hash } => {
                // The node needs to include its own public address in the request
                // so that the response can be sent over a direct Tier3 connection.
                let Some(addr) = *self.state.my_public_addr.read() else {
                    return NetworkResponses::MyPublicAddrNotKnown;
                };

                // Select a peer which has advertised availability of the desired
                // state snapshot.
                let Some(peer_id) = self
                    .state
                    .snapshot_hosts
                    .select_host_for_header(&sync_prev_prev_hash, shard_id)
                else {
                    tracing::debug!(target: "network", %shard_id, ?sync_hash, "no snapshot hosts available");
                    return NetworkResponses::NoDestinationsAvailable;
                };

                let routed_message = self.state.sign_message(
                    &self.clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(peer_id.clone()),
                        body: T2MessageBody::StateHeaderRequest(StateHeaderRequest {
                            shard_id,
                            sync_hash,
                            addr,
                        })
                        .into(),
                    },
                );

                self.state.pending_tier3_requests.insert(peer_id.clone(), self.clock.now());
                if !self.state.send_message_to_peer(
                    &self.clock,
                    tcp::Tier::T2,
                    routed_message,
                    &*self.transport,
                ) {
                    self.state.pending_tier3_requests.remove(&peer_id);
                    return NetworkResponses::RouteNotFound;
                }
                tracing::debug!(target: "network", %shard_id, ?sync_hash, %peer_id, "requesting state header from host");
                NetworkResponses::SelectedDestination(peer_id)
            }
            NetworkRequests::StateRequestPart {
                shard_id,
                sync_hash,
                sync_prev_prev_hash,
                part_id,
            } => {
                // The node needs to include its own public address in the request
                // so that the response can be sent over a direct Tier3 connection.
                let Some(addr) = *self.state.my_public_addr.read() else {
                    return NetworkResponses::MyPublicAddrNotKnown;
                };

                // Select a peer which has advertised availability of the desired
                // state snapshot.
                let Some(peer_id) = self.state.snapshot_hosts.select_host_for_part(
                    &sync_prev_prev_hash,
                    shard_id,
                    part_id,
                ) else {
                    tracing::debug!(target: "network", %shard_id, ?sync_hash, ?part_id, "no snapshot hosts available");
                    return NetworkResponses::NoDestinationsAvailable;
                };

                let routed_message = self.state.sign_message(
                    &self.clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(peer_id.clone()),
                        body: T2MessageBody::StatePartRequest(StatePartRequest {
                            shard_id,
                            sync_hash,
                            part_id,
                            addr,
                        })
                        .into(),
                    },
                );

                self.state.pending_tier3_requests.insert(peer_id.clone(), self.clock.now());
                if !self.state.send_message_to_peer(
                    &self.clock,
                    tcp::Tier::T2,
                    routed_message,
                    &*self.transport,
                ) {
                    self.state.pending_tier3_requests.remove(&peer_id);
                    return NetworkResponses::RouteNotFound;
                }
                tracing::debug!(target: "network", %shard_id, ?sync_hash, ?part_id, %peer_id, "requesting state part from host");
                NetworkResponses::SelectedDestination(peer_id)
            }
            NetworkRequests::StateRequestAck {
                shard_id,
                sync_hash,
                part_id_or_header,
                body,
                peer_id,
            } => {
                let routed_message = self.state.sign_message(
                    &self.clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(peer_id.clone()),
                        body: T2MessageBody::StateRequestAck(StateRequestAck {
                            shard_id,
                            sync_hash,
                            part_id_or_header,
                            body,
                        })
                        .into(),
                    },
                );

                if !self.state.send_message_to_peer(
                    &self.clock,
                    tcp::Tier::T2,
                    routed_message,
                    &*self.transport,
                ) {
                    return NetworkResponses::RouteNotFound;
                }

                tracing::debug!(target: "network", %shard_id, ?sync_hash, ?part_id_or_header, ?body, %peer_id, "ack state request from host");
                NetworkResponses::NoResponse
            }
            NetworkRequests::SnapshotHostEvent(SnapshotHostEvent::ChainProgressed {
                epoch_height,
            }) => {
                self.state.snapshot_hosts.set_current_epoch_height(epoch_height);
                NetworkResponses::NoResponse
            }
            NetworkRequests::SnapshotHostEvent(SnapshotHostEvent::SnapshotCreated {
                sync_hash,
                mut epoch_height,
                mut shards,
            }) => {
                // Don't send out snapshot host info with empty shards - there's nothing useful to advertise.
                if shards.is_empty() {
                    tracing::trace!(
                        target: "network",
                        "skipping snapshot host info broadcast: no shards available"
                    );
                    return NetworkResponses::NoResponse;
                }

                if shards.len() > MAX_SHARDS_PER_SNAPSHOT_HOST_INFO {
                    tracing::warn!(
                        shards_len = shards.len(),
                        %MAX_SHARDS_PER_SNAPSHOT_HOST_INFO,
                        "peer manager sending out a snapshot host info message with too many shards, list will be truncated, please adjust max_shards_per_snapshot_host_info constant"
                    );

                    // We can's send out more than MAX_SHARDS_PER_SNAPSHOT_HOST_INFO shards because other nodes would
                    // ban us for abusive behavior. Let's truncate the shards vector by choosing a random subset of
                    // MAX_SHARDS_PER_SNAPSHOT_HOST_INFO shard ids. Choosing a random subset slightly increases the chances
                    // that other nodes will have snapshot sync information about all shards from some node.
                    shards = shards
                        .choose_multiple(&mut rand::thread_rng(), MAX_SHARDS_PER_SNAPSHOT_HOST_INFO)
                        .copied()
                        .collect();
                }
                // Sort the shards to keep things tidy
                shards.sort();

                let peer_id = self.state.config.node_id();

                // Hacky workaround for test environments only.
                // When starting a chain from scratch the first two snapshots both have epoch height 1.
                // The epoch height is used as a version number for SnapshotHostInfo and if duplicated,
                // prevents the second snapshot from being advertised as new information to the network.
                // To avoid this problem, we re-index the very first epoch with epoch_height=0.
                if epoch_height == 1 && self.state.snapshot_hosts.get_host_info(&peer_id).is_none()
                {
                    epoch_height = 0;
                }

                // Sign the information about the locally created snapshot using the keys in the
                // network config before broadcasting it
                let snapshot_host_info = Arc::new(SnapshotHostInfo::new(
                    self.state.config.node_id(),
                    sync_hash,
                    epoch_height,
                    shards,
                    &self.state.config.node_key,
                ));

                // Insert our info to our own cache.
                self.state.snapshot_hosts.insert_skip_verify(snapshot_host_info.clone());

                self.transport.broadcast_message(Arc::new(PeerMessage::SyncSnapshotHosts(
                    SyncSnapshotHosts { hosts: vec![snapshot_host_info] },
                )));
                NetworkResponses::NoResponse
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                self.state.disconnect_and_ban(&self.clock, &peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                let state = self.state.clone();
                let transport = self.transport.clone();
                self.handle.spawn("announce_account", async move {
                    state.add_accounts(vec![announce_account], transport).await;
                });
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedChunkRequest { target, request, create_time } => {
                metrics::PARTIAL_ENCODED_CHUNK_REQUEST_DELAY.observe(
                    (self.clock.now().signed_duration_since(create_time)).as_seconds_f64(),
                );
                let mut success = false;

                // Make two attempts to send the message. First following the preference of `prefer_peer`,
                // and if it fails, against the preference.
                for prefer_peer in &[target.prefer_peer, !target.prefer_peer] {
                    if !prefer_peer {
                        if let Some(account_id) = target.account_id.as_ref() {
                            if self.state.send_message_to_account(
                                &self.clock,
                                account_id,
                                T2MessageBody::PartialEncodedChunkRequest(request.clone()).into(),
                                &*self.transport,
                            ) {
                                success = true;
                                break;
                            }
                        }
                    } else {
                        let t2_peers = self.state.peers.tier2();
                        let matching_peers: Vec<PeerId> = t2_peers
                            .into_iter()
                            .filter(|(_, s)| {
                                (s.archival || !target.only_archival)
                                    && s.block_info
                                        .as_ref()
                                        .is_some_and(|b| b.height >= target.min_height)
                                    && s.tracked_shards.contains(&target.shard_id)
                            })
                            .map(|(id, _)| id)
                            .collect();

                        if let Some(matching_peer) = matching_peers.iter().choose(&mut thread_rng())
                        {
                            if self.state.send_message_to_peer(
                                &self.clock,
                                tcp::Tier::T2,
                                self.state.sign_message(
                                    &self.clock,
                                    RawRoutedMessage {
                                        target: PeerIdOrHash::PeerId(matching_peer.clone()),
                                        body: T2MessageBody::PartialEncodedChunkRequest(
                                            request.clone(),
                                        )
                                        .into(),
                                    },
                                ),
                                &*self.transport,
                            ) {
                                success = true;
                                break;
                            }
                        } else {
                            tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "failed to find any matching peer for chunk");
                        }
                    }
                }

                if success {
                    NetworkResponses::NoResponse
                } else {
                    tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "failed to find a route for chunk");
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
                            body: T2MessageBody::PartialEncodedChunkResponse(response).into(),
                        },
                    ),
                    &*self.transport,
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
                    T1MessageBody::VersionedPartialEncodedChunk(Box::new(
                        partial_encoded_chunk.into(),
                    ))
                    .into(),
                    &*self.transport,
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
                    T1MessageBody::PartialEncodedChunkForward(forward).into(),
                    &*self.transport,
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
                    T2MessageBody::ForwardTx(tx).into(),
                    &*self.transport,
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
                    T2MessageBody::TxStatusRequest(signer_account_id, tx_hash).into(),
                    &*self.transport,
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ChunkStateWitnessAck(target, ack) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T2MessageBody::ChunkStateWitnessAck(ack).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::ChunkEndorsement(target, endorsement) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::VersionedChunkEndorsement(endorsement).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple) => {
                let Some(partial_witness) = validator_witness_tuple.first().map(|(_, w)| w) else {
                    return NetworkResponses::NoResponse;
                };
                let part_owners_len = validator_witness_tuple.len();
                let _span = tracing::debug_span!(target: "network",
                    "send partial_encoded_state_witnesses",
                    height = partial_witness.chunk_production_key().height_created,
                    shard_id = %partial_witness.chunk_production_key().shard_id,
                    part_owners_len,
                    tag_witness_distribution = true,
                )
                .entered();

                for (chunk_validator, partial_witness) in validator_witness_tuple {
                    self.state.send_message_to_account(
                        &self.clock,
                        &chunk_validator,
                        T1MessageBody::PartialEncodedStateWitness(partial_witness).into(),
                        &*self.transport,
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedStateWitnessForward(
                chunk_validators,
                partial_witness,
            ) => {
                let _span = tracing::debug_span!(target: "network",
                    "send partial_encoded_state_witness_forward",
                    height = partial_witness.chunk_production_key().height_created,
                    shard_id = %partial_witness.chunk_production_key().shard_id,
                    part_ord = partial_witness.part_ord(),
                    tag_witness_distribution = true,
                )
                .entered();
                for chunk_validator in chunk_validators {
                    self.state.send_message_to_account(
                        &self.clock,
                        &chunk_validator,
                        T1MessageBody::PartialEncodedStateWitnessForward(partial_witness.clone())
                            .into(),
                        &*self.transport,
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::EpochSyncRequest { peer_id } => {
                if self.transport.send_message(
                    tcp::Tier::T2,
                    peer_id,
                    PeerMessage::EpochSyncRequest.into(),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::EpochSyncResponse { peer_id, proof } => {
                if self
                    .state
                    .tier2
                    .send_message(peer_id, PeerMessage::EpochSyncResponse(proof).into())
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ChunkContractAccesses(validators, accesses) => {
                for validator in validators {
                    self.state.send_message_to_account(
                        &self.clock,
                        &validator,
                        T1MessageBody::ChunkContractAccesses(accesses.clone()).into(),
                        &*self.transport,
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::ContractCodeRequest(target, request) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::ContractCodeRequest(request).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::ContractCodeResponse(target, response) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::ContractCodeResponse(response).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedContractDeploys(accounts, deploys) => {
                // Send to last account separately to avoid clone when sending to a single target.
                let (last_account, other_accounts) = accounts.split_last().unwrap();
                for account in other_accounts {
                    self.state.send_message_to_account(
                        &self.clock,
                        &account,
                        T2MessageBody::PartialEncodedContractDeploys(deploys.clone()).into(),
                        &*self.transport,
                    );
                }
                self.state.send_message_to_account(
                    &self.clock,
                    &last_account,
                    T2MessageBody::PartialEncodedContractDeploys(deploys).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::SpicePartialData { partial_data, recipients } => {
                for (partial_data, account) in
                    std::iter::repeat_n(partial_data, recipients.len()).zip(recipients)
                {
                    self.state.send_message_to_account(
                        &self.clock,
                        &account,
                        // TODO(spice): Once spice data distribution retries are implemented
                        // reconsider if sending data over T1 still makes sense.
                        T1MessageBody::SpicePartialData(partial_data).into(),
                        &*self.transport,
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::SpiceChunkEndorsement(target, endorsement) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::SpiceChunkEndorsement(endorsement).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::SpicePartialDataRequest { producer, request } => {
                self.state.send_message_to_account(
                    &self.clock,
                    &producer,
                    T1MessageBody::SpicePartialDataRequest(request).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::SpiceChunkContractAccesses(targets, accesses) => {
                for target in targets {
                    self.state.send_message_to_account(
                        &self.clock,
                        &target,
                        T1MessageBody::SpiceChunkContractAccesses(accesses.clone()).into(),
                        &*self.transport,
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::SpiceContractCodeRequest(target, request) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::SpiceContractCodeRequest(request).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::SpiceContractCodeResponse(target, response) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::SpiceContractCodeResponse(response).into(),
                    &*self.transport,
                );
                NetworkResponses::NoResponse
            }
        }
    }

    fn handle_peer_manager_message(
        &self,
        msg: PeerManagerMessageRequest,
    ) -> PeerManagerMessageResponse {
        match msg {
            PeerManagerMessageRequest::NetworkRequests(msg) => {
                PeerManagerMessageResponse::NetworkResponses(self.handle_msg_network_requests(msg))
            }
            PeerManagerMessageRequest::AdvertiseTier1Proxies => {
                let state = self.state.clone();
                let clock = self.clock.clone();
                let transport = self.transport.clone();
                self.handle.spawn("advertise_tier1_proxies", async move {
                    state.tier1_advertise_proxies(&clock, transport.as_ref()).await;
                });
                PeerManagerMessageResponse::AdvertiseTier1Proxies
            }
            // TEST-ONLY
            PeerManagerMessageRequest::FetchRoutingTable => {
                PeerManagerMessageResponse::FetchRoutingTable(self.state.graph.routing_table.info())
            }
        }
    }
}

impl messaging::Handler<SetChainInfo> for PeerManagerActor {
    fn handle(&mut self, SetChainInfo(info): SetChainInfo) {
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&["SetChainInfo"]).start_timer();
        // We call self.state.set_chain_info()
        // synchronously, therefore, assuming actor in-order delivery,
        // there will be no race condition between subsequent SetChainInfo
        // calls.
        if !self.state.set_chain_info(info) {
            // We early exit in case the set of TIER1 account keys hasn't changed.
            return;
        }

        let state = self.state.clone();
        let clock = self.clock.clone();
        let transport = self.transport.clone();
        self.handle.spawn(
            "handle set_chain_info",
            async move {
                // This node might have become a TIER1 node due to the change of the key set.
                // If so we should recompute and re-advertise the list of proxies.
                // This is mostly important in case a node is its own proxy. In all other cases
                // (when proxies are different nodes) the update of the key set happens asynchronously
                // and this node won't be able to connect to proxies until it happens (and only the
                // connected proxies are included in the advertisement). We run tier1_advertise_proxies
                // periodically in the background anyway to cover those cases.
                state.tier1_advertise_proxies(&clock, transport.as_ref()).await;
            }
            .in_current_span(),
        );
    }
}

impl messaging::Handler<PeerManagerMessageRequest, PeerManagerMessageResponse>
    for PeerManagerActor
{
    fn handle(&mut self, msg: PeerManagerMessageRequest) -> PeerManagerMessageResponse {
        let _timer = metrics::PEER_MANAGER_MESSAGES_TIME
            .with_label_values::<&str>(&[(&msg).into()])
            .start_timer();
        self.handle_peer_manager_message(msg)
    }
}

impl messaging::Handler<PeerManagerMessageRequest> for PeerManagerActor {
    fn handle(&mut self, msg: PeerManagerMessageRequest) {
        messaging::Handler::<PeerManagerMessageRequest, PeerManagerMessageResponse>::handle(
            self, msg,
        );
    }
}

impl messaging::Handler<StateSyncEvent> for PeerManagerActor {
    fn handle(&mut self, msg: StateSyncEvent) {
        let _timer = metrics::PEER_MANAGER_MESSAGES_TIME
            .with_label_values::<&str>(&[(&msg).into()])
            .start_timer();
        match msg {
            StateSyncEvent::StatePartReceived(shard_id, part_id) => {
                self.state.snapshot_hosts.part_received(shard_id, part_id);
            }
        }
    }
}

impl messaging::Handler<Tier3Request> for PeerManagerActor {
    fn handle(&mut self, request: Tier3Request) {
        let _timer = metrics::PEER_MANAGER_TIER3_REQUEST_TIME
            .with_label_values::<&str>(&[(&request.body).into()])
            .start_timer();

        let state = self.state.clone();
        let clock = self.clock.clone();
        let transport = self.transport.clone();
        self.handle.spawn("handle tier3 request",
            async move {
                // Process the request.
                // Unconditionally produce an ack to be sent back over tier2.
                // Optionally produce a response to be sent over tier3.
                let (tier2_ack, maybe_tier3_response) = match request.body {
                    Tier3RequestBody::StateHeader(StateHeaderRequestBody { shard_id, sync_hash }) => {
                        let (ack, response) = match state.state_request_adapter.send_async(StateRequestHeader { shard_id, sync_hash }).await {
                            Ok(Some(client_response)) => {
                                (StateRequestAckBody::WillRespond, Some(PeerMessage::VersionedStateResponse(*client_response.0)))
                            }
                            Ok(None) => {
                                tracing::debug!(target: "network", ?request, "client declined to respond");
                                (StateRequestAckBody::Busy, None)
                            }
                            Err(err) => {
                                tracing::error!(target: "network", ?request, ?err, "client failed to respond");
                                (StateRequestAckBody::Error, None)
                            }
                        };

                        (
                            T2MessageBody::StateRequestAck(StateRequestAck {
                                shard_id,
                                sync_hash,
                                part_id_or_header: PartIdOrHeader::Header,
                                body: ack,
                            }).into(),
                            response
                        )
                    }
                    Tier3RequestBody::StatePart(StatePartRequestBody { shard_id, sync_hash, part_id }) => {
                        let (ack, response) = match state.state_request_adapter.send_async(StateRequestPart { shard_id, sync_hash, part_id }).await {
                            Ok(Some(client_response)) => {
                                (StateRequestAckBody::WillRespond, Some(PeerMessage::VersionedStateResponse(*client_response.0)))
                            }
                            Ok(None) => {
                                tracing::debug!(target: "network", ?request, "client declined to respond");
                                (StateRequestAckBody::Busy, None)
                            }
                            Err(err) => {
                                tracing::error!(target: "network", ?err, ?request, "client failed to respond");
                                (StateRequestAckBody::Error, None)
                            }
                        };

                        (
                            T2MessageBody::StateRequestAck(StateRequestAck {
                                shard_id,
                                sync_hash,
                                part_id_or_header: PartIdOrHeader::Part { part_id },
                                body: ack,
                            }).into(),
                            response
                        )
                    }
                };

                let sender: PeerId = request.peer_info.id.clone();

                // Send an ack for the request
                tracing::debug!(target: "network", ?tier2_ack, %sender, "ack state request from host");
                let routed_message = state.sign_message(
                    &clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(sender.clone()),
                        body: tier2_ack,
                    },
                );
                if !state.send_message_to_peer(&clock, tcp::Tier::T2, routed_message, transport.as_ref()) {
                    tracing::debug!(target: "network", sender = %sender, "failed to route ack");
                }

                let Some(tier3_response) = maybe_tier3_response else {
                    return;
                };

                // Establish a tier3 connection if we don't have one already.
                let already_connected_t3 =
                    state.peers.is_connected_on_tier(&sender, tcp::Tier::T3);
                if !already_connected_t3 {
                    let result = transport
                        .connect_to_peer(&clock, request.peer_info.clone(), tcp::Tier::T3)
                        .await
                        .map_err(|err| anyhow::anyhow!("connect_to_peer: {err:?}"));

                    if let Err(ref err) = result {
                        tracing::debug!(target: "network", ?err, peer_info = %request.peer_info, "tier3 failed to connect");
                    }
                }

                transport.send_message(tcp::Tier::T3, sender, Arc::new(tier3_response));
            }
        );
    }
}

impl messaging::Handler<GetDebugStatus, DebugStatus> for PeerManagerActor {
    fn handle(&mut self, msg: GetDebugStatus) -> DebugStatus {
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
            GetDebugStatus::SnapshotHosts => DebugStatus::SnapshotHosts(SnapshotHostsView {
                hosts: self
                    .state
                    .snapshot_hosts
                    .get_hosts()
                    .iter()
                    .map(|h| SnapshotHostInfoView {
                        peer_id: h.peer_id.clone(),
                        sync_hash: h.sync_hash,
                        epoch_height: h.epoch_height,
                        shards: h.shards.clone().into_iter().map(Into::into).collect(),
                    })
                    .collect::<Vec<_>>(),
            }),
        }
    }
}
