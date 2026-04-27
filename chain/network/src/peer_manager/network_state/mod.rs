use crate::accounts_data::{AccountDataCache, AccountDataError};
use crate::announce_accounts::AnnounceAccountCache;
use crate::client::{
    BlockApproval, BlockHeadersRequest, BlockHeadersResponse, BlockRequest, BlockResponse,
    ChunkEndorsementMessage, ClientSenderForNetwork, EpochSyncRequestMessage,
    EpochSyncResponseMessage, OptimisticBlockMessage, ProcessTxRequest,
    SpiceChunkEndorsementMessage, StateRequestHeader, StateRequestPart, StateResponse,
    StateResponseReceived, TxStatusRequest, TxStatusResponse,
};
use crate::concurrency::demux;
use crate::config;
use crate::network_protocol::{
    Edge, EdgeState, PartialEdgeInfo, PeerIdOrHash, PeerInfo, PeerMessage, RawRoutedMessage,
    RoutedMessage, SignedAccountData, SignedOwnedAccount, SnapshotHostInfo, SyncAccountsData,
    SyncSnapshotHosts, T1MessageBody, T2MessageBody, TieredMessageBody,
};
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::connected_peers::{ConnectedPeerState, ConnectedPeers};
use crate::peer_manager::connection;
use crate::peer_manager::connection_store;
use crate::peer_manager::network_transport::NetworkTransport;
#[cfg(test)]
use crate::peer_manager::peer_manager_actor::Event;
use crate::peer_manager::peer_store;
use crate::private_messages::RegisterPeerError;
use crate::routing::route_back_cache::RouteBackCache;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::snapshot_hosts::{SnapshotHostInfoError, SnapshotHostsCache};
use crate::spice_data_distribution::{
    SpiceChunkContractAccessesMessage, SpiceContractCodeRequestMessage,
    SpiceContractCodeResponseMessage, SpiceDataDistributorSenderForNetwork,
    SpiceIncomingPartialData,
};
use crate::state_witness::{
    ChunkContractAccessesMessage, ChunkStateWitnessAckMessage, ContractCodeRequestMessage,
    ContractCodeResponseMessage, PartialEncodedContractDeploysMessage,
    PartialEncodedStateWitnessForwardMessage, PartialEncodedStateWitnessMessage,
    PartialWitnessSenderForNetwork,
};
use crate::stats::metrics;
use crate::store;
use crate::tcp;
use crate::types::{
    BlockInfo, ChainInfo, PeerManagerSenderForNetwork, PeerType, ReasonForBan,
    StateHeaderRequestBody, StatePartRequestBody, StateRequestSenderForNetwork, Tier3Request,
    Tier3RequestBody,
};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{CanSend, CanSendAsync, Sender};
use near_async::{new_owned_future_spawner, time};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::genesis::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

mod routing;
mod tier1;

/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;

/// Size of LRU cache size of recent routed messages.
/// It should be large enough to detect duplicates (i.e. all messages received during
/// production of 1 block should fit).
const RECENT_ROUTED_MESSAGES_CACHE_SIZE: usize = 10000;

/// How long a peer has to be unreachable, until we prune it from the in-memory graph.
const PRUNE_UNREACHABLE_PEERS_AFTER: time::Duration = time::Duration::hours(1);

/// Remove the edges that were created more that this duration ago.
pub const PRUNE_EDGES_AFTER: time::Duration = time::Duration::minutes(30);

/// How long to wait between reconnection attempts to the same peer
pub(crate) const RECONNECT_ATTEMPT_INTERVAL: time::Duration = time::Duration::seconds(10);

/// How long a pending Tier3 request remains valid. After sending a state sync request over
/// Tier2, we expect the peer to open an inbound Tier3 connection within this window. Entries
/// older than this are cleaned up periodically.
pub(crate) const PENDING_TIER3_REQUEST_TIMEOUT: time::Duration = time::Duration::seconds(60);

impl WhitelistNode {
    pub fn from_peer_info(pi: &PeerInfo) -> anyhow::Result<Self> {
        Ok(Self {
            id: pi.id.clone(),
            addr: if let Some(addr) = pi.addr {
                addr
            } else {
                anyhow::bail!("address is missing");
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
    /// Single-threaded tokio runtime for NetworkState operations
    ops_spawner: Box<dyn FutureSpawner>,
    /// PeerManager config.
    pub config: config::VerifiedConfig,
    /// When network state has been constructed.
    pub created_at: time::Instant,
    /// GenesisId of the chain.
    pub genesis_id: GenesisId,
    pub client: ClientSenderForNetwork,
    pub state_request_adapter: StateRequestSenderForNetwork,
    pub peer_manager_adapter: PeerManagerSenderForNetwork,
    pub shards_manager_adapter: Sender<ShardsManagerRequestFromNetwork>,
    pub partial_witness_adapter: PartialWitnessSenderForNetwork,
    pub spice_data_distributor_adapter: SpiceDataDistributorSenderForNetwork,
    pub spice_core_writer_adapter: Sender<SpiceChunkEndorsementMessage>,

    /// Per-peer metadata + T1 account-key index. Written by
    /// register/unregister and the `handle_peer_message` Block branch.
    pub peers: ConnectedPeers,

    /// Network-related info about the chain.
    pub chain_info: ArcSwap<Option<ChainInfo>>,
    /// Per-peer gossip demuxes for T2 peers. Kept as two separate
    /// maps rather than a struct so `add_accounts_data` and
    /// `add_snapshot_hosts` iterate the map they need directly — no
    /// struct-field extraction step.
    accounts_data_demuxes: Mutex<HashMap<PeerId, demux::Demux<Vec<Arc<SignedAccountData>>, ()>>>,
    snapshot_hosts_demuxes: Mutex<HashMap<PeerId, demux::Demux<Vec<Arc<SnapshotHostInfo>>, ()>>>,

    /// AccountsData for TIER1 accounts.
    pub accounts_data: Arc<AccountDataCache>,
    /// AnnounceAccounts mapping TIER1 account ids to peer ids.
    pub account_announcements: Arc<AnnounceAccountCache>,
    /// Connected peers (inbound and outbound) with their full peer information.
    pub tier2: connection::Pool,
    pub tier1: connection::Pool,
    pub tier3: connection::Pool,
    /// Semaphore limiting inflight inbound handshakes.
    pub inbound_handshake_permits: Arc<tokio::sync::Semaphore>,
    /// The public IP of this node; available after connecting to any one peer.
    pub my_public_addr: Arc<RwLock<Option<std::net::SocketAddr>>>,
    /// Peer store that provides read/write access to peers.
    pub peer_store: peer_store::PeerStore,
    /// Information about state snapshots hosted by network peers.
    pub snapshot_hosts: Arc<SnapshotHostsCache>,
    /// Connection store that provides read/write access to stored connections.
    pub connection_store: connection_store::ConnectionStore,
    /// List of peers to which we should re-establish a connection
    pub pending_reconnect: Mutex<Vec<PeerInfo>>,
    /// A graph of the whole NEAR network.
    pub graph: Arc<crate::routing::Graph>,
    /// Hashes of the body of recently received routed messages.
    /// It allows us to determine whether messages arrived faster over TIER1 or TIER2 network.
    pub recent_routed_messages: Mutex<lru::LruCache<CryptoHash, ()>>,

    /// Hash of messages that requires routing back to respective previous hop.
    pub tier2_route_back: Mutex<RouteBackCache>,
    /// Currently unused, as TIER1 messages do not require a response.
    /// Also TIER1 connections are direct by design (except for proxies),
    /// so routing shouldn't really be needed.
    /// TODO(gprusak): consider removing it altogether.
    pub tier1_route_back: Mutex<RouteBackCache>,

    /// Shared counter across all PeerActors, which counts number of `RoutedMessageBody::ForwardTx`
    /// messages since last block.
    pub txns_since_last_block: AtomicUsize,

    /// Peers from which we expect an inbound Tier3 connection, because we sent them a state
    /// sync request over Tier2. Maps peer_id to the time the request was sent. Entries are
    /// cleaned up after PENDING_TIER3_REQUEST_TIMEOUT.
    pub pending_tier3_requests: DashMap<PeerId, time::Instant>,

    /// Whitelisted nodes, which are allowed to connect even if the connection limit has been
    /// reached.
    whitelist_nodes: Vec<WhitelistNode>,

    /// Mutex which prevents overlapping calls to tier1_advertise_proxies.
    tier1_advertise_proxies_mutex: tokio::sync::Mutex<()>,
    /// Demultiplexer aggregating calls to add_edges(), for V1 routing protocol
    add_edges_demux: demux::Demux<EdgesWithSource, Result<(), ReasonForBan>>,
    /// Mutex serializing calls to set_chain_info(), which mutates a bunch of stuff non-atomically.
    /// TODO(gprusak): make it use synchronization primitives in some more canonical way.
    set_chain_info_mutex: Mutex<()>,
}

#[derive(Debug)]
/// Edges along with their source (constructed locally or received from a remote peer).
/// Self-connected edges are not allowed from remote peers.
pub(crate) enum EdgesWithSource {
    Local(Vec<Edge>),
    Remote { edges: Vec<Edge>, source: PeerId },
}

impl EdgesWithSource {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            EdgesWithSource::Local(edges) | EdgesWithSource::Remote { edges, .. } => {
                edges.is_empty()
            }
        }
    }
}

/// Action to take after processing an incoming routed message.
/// Returned by `NetworkState::process_incoming_routed` for the caller
/// (PeerActor or TestLoopTransport) to execute.
pub(crate) enum RoutedAction {
    /// Message is for us — caller should handle (Ping/Pong synchronously,
    /// others via `handle_peer_message`).
    ForMe(Box<RoutedMessage>),
    /// Not for us — caller should forward via `send_message_to_peer`.
    /// TTL already decremented, num_hops incremented.
    Forward(Box<RoutedMessage>),
    /// Message dropped (TTL expired, etc). Metrics/logging already done.
    Dropped,
}

/// Transport-agnostic per-connection metadata. The caller (PeerActor
/// for TCP, TestLoopTransport for testloop) extracts these fields from
/// whatever connection representation it owns and hands them to the
/// lifecycle methods (`validate_new_connection`, `on_peer_connected`).
pub(crate) struct PeerConnectionInfo {
    pub peer_info: PeerInfo,
    pub tier: tcp::Tier,
    pub peer_type: PeerType,
    pub archival: bool,
    pub tracked_shards: Vec<near_primitives::types::ShardId>,
    /// AccountKey ownership proof — only populated on TIER1 connections.
    pub owned_account: Option<SignedOwnedAccount>,
    pub established_time: time::Instant,
}

/// Minimal peer identity carried through the disconnect path. Only
/// what `on_peer_disconnected` actually reads.
pub(crate) struct PeerDisconnectInfo {
    pub peer_info: PeerInfo,
    pub tier: tcp::Tier,
    pub peer_type: PeerType,
}

impl From<&connection::Connection> for PeerConnectionInfo {
    fn from(conn: &connection::Connection) -> Self {
        Self {
            peer_info: conn.peer_info.clone(),
            tier: conn.tier,
            peer_type: conn.peer_type,
            archival: conn.archival,
            tracked_shards: conn.tracked_shards.clone(),
            owned_account: conn.owned_account.clone(),
            established_time: conn.established_time,
        }
    }
}

impl From<&connection::Connection> for PeerDisconnectInfo {
    fn from(conn: &connection::Connection) -> Self {
        Self { peer_info: conn.peer_info.clone(), tier: conn.tier, peer_type: conn.peer_type }
    }
}

impl NetworkState {
    pub fn new(
        clock: &time::Clock,
        future_spawner: &dyn FutureSpawner,
        store: store::Store,
        peer_store: peer_store::PeerStore,
        config: config::VerifiedConfig,
        genesis_id: GenesisId,
        client: ClientSenderForNetwork,
        state_request_adapter: StateRequestSenderForNetwork,
        peer_manager_adapter: PeerManagerSenderForNetwork,
        shards_manager_adapter: Sender<ShardsManagerRequestFromNetwork>,
        partial_witness_adapter: PartialWitnessSenderForNetwork,
        whitelist_nodes: Vec<WhitelistNode>,
        spice_data_distributor_adapter: SpiceDataDistributorSenderForNetwork,
        spice_core_writer_adapter: Sender<SpiceChunkEndorsementMessage>,
    ) -> Self {
        Self {
            ops_spawner: new_owned_future_spawner("NetworkState ops"),
            graph: crate::routing::Graph::new(
                clock.clone(),
                crate::routing::GraphConfig {
                    node_id: config.node_id(),
                    prune_unreachable_peers_after: PRUNE_UNREACHABLE_PEERS_AFTER,
                    prune_edges_after: Some(PRUNE_EDGES_AFTER),
                    max_edges_per_source: config.routing_graph_max_edges_per_source,
                    max_total_edges: config.routing_graph_max_edges,
                    max_graph_peers: config.routing_graph_max_peers,
                },
            ),
            genesis_id,
            client,
            state_request_adapter,
            peer_manager_adapter,
            shards_manager_adapter,
            partial_witness_adapter,
            peers: ConnectedPeers::new(),
            accounts_data_demuxes: Mutex::new(HashMap::new()),
            snapshot_hosts_demuxes: Mutex::new(HashMap::new()),
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            tier1: connection::Pool::new(config.node_id()),
            tier3: connection::Pool::new(config.node_id()),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            my_public_addr: Arc::new(RwLock::new(config.tier3_public_addr)),
            peer_store,
            snapshot_hosts: Arc::new(SnapshotHostsCache::new(config.snapshot_hosts.clone())),
            connection_store: connection_store::ConnectionStore::new(store.clone()).unwrap(),
            pending_reconnect: Mutex::new(Vec::<PeerInfo>::new()),
            accounts_data: Arc::new(AccountDataCache::new()),
            account_announcements: Arc::new(AnnounceAccountCache::new(store)),
            tier2_route_back: Mutex::new(RouteBackCache::default()),
            tier1_route_back: Mutex::new(RouteBackCache::default()),
            recent_routed_messages: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(RECENT_ROUTED_MESSAGES_CACHE_SIZE).unwrap(),
            )),
            txns_since_last_block: AtomicUsize::new(0),
            pending_tier3_requests: DashMap::new(),
            whitelist_nodes,
            add_edges_demux: demux::Demux::new(
                config.routing_table_update_rate_limit,
                future_spawner,
            ),
            set_chain_info_mutex: Mutex::new(()),
            config,
            created_at: clock.now(),
            tier1_advertise_proxies_mutex: tokio::sync::Mutex::new(()),
            spice_data_distributor_adapter,
            spice_core_writer_adapter,
        }
    }

    /// Spawn a future on the runtime which has the same lifetime as the NetworkState instance.
    /// In particular if the future contains the NetworkState handler, it will be run until
    /// completion. It is safe to self.spawn(...).await.unwrap(), since runtime will be kept alive
    /// by the reference to self.
    ///
    /// It should be used to make the public methods cancellable: you spawn the
    /// noncancellable logic on self.runtime and just await it: in case the call is cancelled,
    /// the noncancellable logic will be run in the background anyway.
    fn spawn<R: 'static + Send>(
        &self,
        description: &'static str,
        fut: impl std::future::Future<Output = R> + 'static + Send,
    ) -> tokio::sync::oneshot::Receiver<R> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.ops_spawner.spawn(description, async move {
            let res = fut.await;
            let _ = tx.send(res);
        });
        rx
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
                tracing::debug!(target: "network", ?err, "failed to save peer data");
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
        if tier2.ready.len() + tier2.outbound_handshakes.len() < self.config.max_num_peers as usize
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

    /// Pure validation for a new connection — no side effects.
    /// Returns Err to reject the connection. If it fails, nothing was
    /// written — no rollback needed.
    pub(crate) fn validate_new_connection(
        &self,
        info: &PeerConnectionInfo,
        edge: &Edge,
    ) -> Result<(), RegisterPeerError> {
        let peer_info = &info.peer_info;
        if peer_info.addr.as_ref().map_or(true, |addr| self.peer_store.is_blacklisted(addr)) {
            tracing::debug!(target: "network", peer_info = ?peer_info, "dropping connection from blacklisted peer or unknown address");
            return Err(RegisterPeerError::Blacklisted);
        }
        if self.peer_store.is_banned(&peer_info.id) {
            tracing::debug!(target: "network", id = ?peer_info.id, "dropping connection from banned peer");
            return Err(RegisterPeerError::Banned);
        }
        match info.tier {
            tcp::Tier::T1 => {
                if info.peer_type == PeerType::Inbound {
                    if !self.config.tier1.enable_inbound {
                        return Err(RegisterPeerError::Tier1InboundDisabled);
                    }
                    // Allow for inbound TIER1 connections only directly from a TIER1 peers.
                    let owned_account =
                        info.owned_account.as_ref().ok_or(RegisterPeerError::NotTier1Peer)?;
                    if !self.accounts_data.load().keys.contains(&owned_account.account_key) {
                        return Err(RegisterPeerError::NotTier1Peer);
                    }
                }
                if !edge.verify() {
                    return Err(RegisterPeerError::InvalidEdge);
                }
            }
            tcp::Tier::T2 => {
                if info.peer_type == PeerType::Inbound {
                    if !self.is_inbound_allowed(peer_info) {
                        // TODO(1896): Gracefully drop inbound connection for other peer.
                        let tier2 = self.tier2.load();
                        tracing::debug!(target: "network",
                            tier2 = tier2.ready.len(), outgoing_peers = tier2.outbound_handshakes.len(),
                            max_num_peers = self.config.max_num_peers,
                            "dropping handshake (network at max capacity)"
                        );
                        return Err(RegisterPeerError::ConnectionLimitExceeded);
                    }
                }
                // TODO(gprusak): consider actually banning the peer for consistency.
                if !edge.verify() {
                    return Err(RegisterPeerError::InvalidEdge);
                }
            }
            tcp::Tier::T3 => {
                if !edge.verify() {
                    return Err(RegisterPeerError::InvalidEdge);
                }
                if info.peer_type == PeerType::Inbound {
                    // Reject inbound Tier3 connections that don't correspond to a
                    // state sync request we sent. We check without removing so that
                    // the entry remains valid for the full timeout window — the peer
                    // may need to open additional T3 connections (e.g. if the first
                    // was idle-closed before a later response is ready).
                    //
                    // Edge verification is done first so that a spoofed peer_id with
                    // an invalid edge cannot influence the pending-request lookup.
                    if !self.pending_tier3_requests.contains_key(&peer_info.id) {
                        return Err(RegisterPeerError::UnexpectedTier3Connection);
                    }
                }
            }
        }
        Ok(())
    }

    /// Post-registration business logic writes. Called AFTER pool_insert
    /// succeeds. Writes to connected_peers (ConnectedPeers handles the
    /// T1 `account_key → peer_id` index internally as a side effect of
    /// `insert`), broadcasts edge (T2), and updates peer_store (T2).
    pub(crate) async fn on_peer_connected(
        self: &Arc<Self>,
        clock: &time::Clock,
        edge: Edge,
        info: PeerConnectionInfo,
        transport: Arc<dyn NetworkTransport>,
    ) {
        let account_key = info.owned_account.as_ref().map(|oa| oa.account_key.clone());
        let peer_id = info.peer_info.id.clone();
        let tier = info.tier;
        let peer_info = info.peer_info.clone();
        self.peers.insert(
            peer_id,
            ConnectedPeerState {
                peer_info: info.peer_info,
                block_info: None,
                tier: info.tier,
                archival: info.archival,
                tracked_shards: info.tracked_shards,
                owned_account_key: account_key,
                peer_type: info.peer_type,
                established_time: info.established_time,
            },
        );
        if tier == tcp::Tier::T2 {
            self.accounts_data_demuxes.lock().insert(
                peer_info.id.clone(),
                demux::Demux::new(
                    self.config.accounts_data_broadcast_rate_limit,
                    &*self.ops_spawner,
                ),
            );
            self.snapshot_hosts_demuxes.lock().insert(
                peer_info.id.clone(),
                demux::Demux::new(
                    self.config.snapshot_hosts_broadcast_rate_limit,
                    &*self.ops_spawner,
                ),
            );
            // Broadcast the edge to other peers. The edge was already verified
            // in validate_new_connection (edge.verify()), so add_edges should
            // never fail for a pre-verified local edge. On master this was done
            // before pool_insert; now done after — the broadcast is independent
            // of whether the peer is in the pool.
            self.add_edges(clock, EdgesWithSource::Local(vec![edge]), transport)
                .await
                .expect("local edge was verified in validate_new_connection");
            self.peer_store.peer_connected(clock, &peer_info);
        }
    }

    /// Post-unregistration cleanup. Removes from connected_peers
    /// (ConnectedPeers clears the T1 `account_key → peer_id` index
    /// internally, only when the removed peer was T1, with a defensive
    /// check against account-key reuse races). For T2: edge removal
    /// broadcast, peer_store, connection_store, pending_reconnect.
    pub(crate) async fn on_peer_disconnected(
        self: &Arc<Self>,
        clock: &time::Clock,
        info: &PeerDisconnectInfo,
        reason: ClosingReason,
        #[cfg(test)] stream_id: tcp::StreamId,
        transport: Arc<dyn NetworkTransport>,
    ) {
        self.peers.remove(info.tier, &info.peer_info.id);

        if info.tier == tcp::Tier::T2 {
            self.accounts_data_demuxes.lock().remove(&info.peer_info.id);
            self.snapshot_hosts_demuxes.lock().remove(&info.peer_info.id);

            let peer_id = info.peer_info.id.clone();

            // If the last edge represents a connection addition, create an edge
            // update for the removal.
            if let Some(edge) = self.graph.load().local_edges.get(&peer_id) {
                if edge.edge_type() == EdgeState::Active {
                    let edge_update =
                        edge.remove_edge(self.config.node_id(), &self.config.node_key);
                    self.add_edges(
                        clock,
                        EdgesWithSource::Local(vec![edge_update.clone()]),
                        transport.clone(),
                    )
                    .await
                    .unwrap();
                }
            }

            // Save the fact that we are disconnecting to the PeerStore.
            let res = match &reason {
                ClosingReason::Ban(ban_reason) => {
                    self.peer_store.peer_ban(clock, &info.peer_info.id, *ban_reason)
                }
                _ => self.peer_store.peer_disconnected(clock, &info.peer_info.id),
            };
            if let Err(err) = res {
                tracing::debug!(target: "network", ?err, "failed to save peer data");
            }

            // Save the fact that we are disconnecting to the ConnectionStore,
            // and push a reconnect attempt, if applicable
            if self.connection_store.connection_closed(&info.peer_info, &info.peer_type, &reason) {
                self.pending_reconnect.lock().push(info.peer_info.clone());
            }
        }

        // Emit after all state changes so tests waiting on
        // `ConnectionClosed` observe the peer_store / connection_store
        // updates. `#[cfg(test)]` keeps stream_id out of production paths.
        #[cfg(test)]
        self.config.event_sink.send(
            crate::peer_manager::peer_manager_actor::Event::ConnectionClosed(
                crate::peer::peer_actor::ConnectionClosedEvent { stream_id, reason },
            ),
        );
    }

    /// Register a direct connection to a new peer. Called after a handshake
    /// completes; commits the peer to the Pool and runs the business-logic
    /// writes (`connected_peers`, edge broadcast, peer_store).
    ///
    /// Builds a new edge between this pair of nodes from the two half-edge
    /// signatures: the local half was created in the handshake exchange and
    /// passed in via `edge`; the peer's half is stored inside `conn`.
    pub async fn register(
        self: &Arc<Self>,
        clock: &time::Clock,
        edge: Edge,
        conn: Arc<connection::Connection>,
        transport: Arc<dyn NetworkTransport>,
    ) -> Result<(), RegisterPeerError> {
        let this = self.clone();
        let clock = clock.clone();
        self.spawn("register_connection", async move {
            let info: PeerConnectionInfo = conn.as_ref().into();
            this.validate_new_connection(&info, &edge)?;
            match conn.tier {
                tcp::Tier::T1 => this.tier1.insert_ready(conn.clone()),
                tcp::Tier::T2 => this.tier2.insert_ready(conn.clone()),
                tcp::Tier::T3 => this.tier3.insert_ready(conn.clone()),
            }
            .map_err(RegisterPeerError::PoolError)?;
            this.on_peer_connected(&clock, edge, info, transport).await;
            Ok(())
        })
        .await
        .unwrap()
    }

    /// Removes the connection from the state.
    ///
    /// Intentionally synchronous and expected to be called from
    /// `PeerActor::stopping`. If it was async there'd be a risk that the
    /// unregister is cancelled before even starting — the cleanup has to
    /// run to completion or we leak Pool / peer_store / peer_gossip state.
    pub fn unregister(
        self: &Arc<Self>,
        clock: &time::Clock,
        conn: &Arc<connection::Connection>,
        #[cfg(test)] stream_id: tcp::StreamId,
        reason: ClosingReason,
        transport: Arc<dyn NetworkTransport>,
    ) {
        let this = self.clone();
        let clock = clock.clone();
        let conn = conn.clone();
        self.spawn("unregister_connection", async move {
            match conn.tier {
                tcp::Tier::T1 => this.tier1.remove(&conn),
                tcp::Tier::T2 => this.tier2.remove(&conn),
                tcp::Tier::T3 => this.tier3.remove(&conn),
            }
            let info: PeerDisconnectInfo = conn.as_ref().into();
            this.on_peer_disconnected(
                &clock,
                &info,
                reason,
                #[cfg(test)]
                stream_id,
                transport,
            )
            .await;
        });
    }

    /// Attempt to connect to the given peer until successful, up to max_attempts times
    pub async fn reconnect(
        self: &Arc<Self>,
        clock: time::Clock,
        transport: Arc<dyn NetworkTransport>,
        peer_info: PeerInfo,
        max_attempts: usize,
    ) {
        let mut interval = time::Interval::new(clock.now(), RECONNECT_ATTEMPT_INTERVAL);
        for _attempt in 0..max_attempts {
            interval.tick(&clock).await;

            let result = transport
                .connect_to_peer(&clock, peer_info.clone(), tcp::Tier::T2)
                .await
                .map_err(|err| anyhow::anyhow!("connect_to_peer: {err:?}"));

            let succeeded = result.is_ok();

            if let Err(ref err) = result {
                tracing::info!(target:"network", %err, %peer_info, "failed to connect");
            }

            // The peer may not be in the peer store; we try to record the connection attempt but
            // ignore any storage errors
            let _ = self.peer_store.peer_connection_attempt(&clock, &peer_info.id, result);

            if succeeded {
                return;
            }
        }
    }

    /// Determine if the given target is referring to us.
    pub fn message_for_me(&self, target: &PeerIdOrHash) -> bool {
        let my_peer_id = self.config.node_id();
        match target {
            PeerIdOrHash::PeerId(peer_id) => &my_peer_id == peer_id,
            PeerIdOrHash::Hash(hash) => self.compare_route_back(*hash, &my_peer_id),
        }
    }

    #[cfg(test)]
    pub fn send_ping(
        &self,
        clock: &time::Clock,
        tier: tcp::Tier,
        nonce: u64,
        target: PeerId,
        transport: &dyn NetworkTransport,
    ) {
        let body = T2MessageBody::Ping(crate::network_protocol::Ping {
            nonce,
            source: self.config.node_id(),
        })
        .into();
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(clock, tier, self.sign_message(clock, msg), transport);
    }

    pub fn send_pong(
        &self,
        clock: &time::Clock,
        tier: tcp::Tier,
        nonce: u64,
        target: CryptoHash,
        transport: &dyn NetworkTransport,
    ) {
        let body = T2MessageBody::Pong(crate::network_protocol::Pong {
            nonce,
            source: self.config.node_id(),
        })
        .into();
        let msg = RawRoutedMessage { target: PeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(clock, tier, self.sign_message(clock, msg), transport);
    }

    pub fn sign_message(&self, clock: &time::Clock, msg: RawRoutedMessage) -> Box<RoutedMessage> {
        Box::new(msg.sign(
            &self.config.node_key,
            self.config.routed_message_ttl,
            Some(clock.now_utc()),
        ))
    }

    /// Route signed message to target peer.
    /// Return whether the message is sent or not.
    pub fn send_message_to_peer(
        &self,
        clock: &time::Clock,
        tier: tcp::Tier,
        msg: Box<RoutedMessage>,
        transport: &dyn NetworkTransport,
    ) -> bool {
        let my_peer_id = self.config.node_id();

        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = msg.target() {
            if target == &my_peer_id {
                tracing::debug!(target: "network", account_id = ?self.config.validator.account_id(), ?my_peer_id, ?msg, "drop signed message to myself");
                metrics::CONNECTED_TO_MYSELF.inc();
                return false;
            }
        }
        match tier {
            tcp::Tier::T1 => {
                let peer_id = match msg.target() {
                    // If a message is a response, we try to load the target from the route back
                    // cache.
                    PeerIdOrHash::Hash(hash) => {
                        match self.tier1_route_back.lock().remove(clock, &hash) {
                            Some(peer_id) => peer_id,
                            None => return false,
                        }
                    }
                    PeerIdOrHash::PeerId(peer_id) => peer_id.clone(),
                };
                return transport.send_message(
                    tcp::Tier::T1,
                    peer_id,
                    Arc::new(PeerMessage::Routed(msg)),
                );
            }
            tcp::Tier::T2 => match self.tier2_find_route(&clock, msg.target()) {
                Ok(peer_id) => {
                    // Remember if we expect a response for this message.
                    if *msg.author() == my_peer_id && msg.expect_response() {
                        tracing::trace!(target: "network", ?msg, "initiate route back");
                        self.tier2_route_back.lock().insert(clock, msg.hash(), my_peer_id);
                    }
                    return transport.send_message(
                        tcp::Tier::T2,
                        peer_id,
                        Arc::new(PeerMessage::Routed(msg)),
                    );
                }
                Err(find_route_error) => {
                    // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                    metrics::MessageDropped::NoRouteFound.inc(msg.body());
                    tracing::debug!(target: "network",
                          account_id = ?self.config.validator.account_id(),
                          to = ?msg.target(),
                          reason = ?find_route_error,
                          known_peers = ?self.graph.routing_table.reachable_peers(),
                          msg = ?msg.body(),
                        "dropping signed message"
                    );
                    return false;
                }
            },
            tcp::Tier::T3 => {
                let peer_id = match msg.target() {
                    PeerIdOrHash::Hash(_) => {
                        debug_assert!(false);
                        return false;
                    }
                    PeerIdOrHash::PeerId(peer_id) => peer_id.clone(),
                };
                return transport.send_message(
                    tcp::Tier::T3,
                    peer_id,
                    Arc::new(PeerMessage::Routed(msg)),
                );
            }
        }
    }

    /// Send message to specific account.
    /// Return whether the message is sent or not.
    /// The message might be sent over TIER1 or TIER2 connection depending on the message type.
    pub fn send_message_to_account(
        self: &Arc<Self>,
        clock: &time::Clock,
        account_id: &AccountId,
        msg: TieredMessageBody,
        transport: &dyn NetworkTransport,
    ) -> bool {
        // If the message is allowed to be sent to self, we handle it directly.
        if self.config.validator.account_id().is_some_and(|id| &id == account_id) {
            // For now, we don't allow some types of messages to be sent to self.
            debug_assert!(msg.allow_sending_to_self());
            let this = self.clone();
            let clock = clock.clone();
            let my_peer_id = self.config.node_id();
            let msg = self.sign_message(
                &clock,
                RawRoutedMessage { target: PeerIdOrHash::PeerId(my_peer_id.clone()), body: msg },
            );
            self.spawn("send_message_to_account", async move {
                let hash = msg.hash();
                this.receive_routed_message(
                    &clock,
                    msg.author().clone(),
                    my_peer_id,
                    hash,
                    msg.body_owned(),
                )
                .await;
            });
            return true;
        }

        let accounts_data = self.accounts_data.load();
        if tcp::Tier::T1.is_allowed_send_routed(&msg) {
            for key in accounts_data.keys_by_id.get(account_id).iter().flat_map(|keys| keys.iter())
            {
                let data = match accounts_data.data.get(key) {
                    Some(data) => data,
                    None => continue,
                };
                let conn = match self.get_tier1_proxy(data) {
                    Some(conn) => conn,
                    None => continue,
                };
                // TODO(gprusak): in case of PartialEncodedChunk, consider stripping everything
                // but the header. This will bound the message size
                conn.send_message(Arc::new(PeerMessage::Routed(self.sign_message(
                    clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(data.peer_id.clone()),
                        body: msg,
                    },
                ))));
                return true;
            }
        }

        let peer_id_from_account_data = accounts_data
            .keys_by_id
            .get(account_id)
            .iter()
            .flat_map(|keys| keys.iter())
            .find_map(|key| accounts_data.data.get(key))
            .map(|data| data.peer_id.clone());
        // Find the target peer_id:
        // - first look it up in self.accounts_data
        // - if missing, fall back to lookup in self.graph.routing_table
        // We want to deprecate self.graph.routing_table.account_owner in the next release.
        let target = if let Some(peer_id) = peer_id_from_account_data {
            metrics::ACCOUNT_TO_PEER_LOOKUPS.with_label_values(&["AccountData"]).inc();
            peer_id
        } else if let Some(peer_id) = self.account_announcements.get_account_owner(account_id) {
            metrics::ACCOUNT_TO_PEER_LOOKUPS.with_label_values(&["AnnounceAccount"]).inc();
            peer_id
        } else {
            // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
            metrics::MessageDropped::UnknownAccount.inc(&msg);
            tracing::debug!(target: "network",
                   account_id = ?self.config.validator.account_id(),
                   to = ?account_id,
                   ?msg,
                   err = "unknown account",
            );
            tracing::trace!(target: "network", known_peers = ?self.account_announcements.get_accounts_keys());
            return false;
        };

        let mut success = false;
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body: msg };
        let msg = self.sign_message(clock, msg);
        for _ in 0..msg.body().message_resend_count() {
            success |= self.send_message_to_peer(clock, tcp::Tier::T2, msg.clone(), transport);
        }
        success
    }

    pub async fn receive_routed_message(
        self: &Arc<Self>,
        clock: &time::Clock,
        msg_author: PeerId,
        prev_hop: PeerId,
        msg_hash: CryptoHash,
        body: TieredMessageBody,
    ) -> Option<TieredMessageBody> {
        match body {
            TieredMessageBody::T1(body) => match *body {
                T1MessageBody::BlockApproval(approval) => {
                    self.client
                        .send_async(BlockApproval(approval, prev_hop).span_wrap())
                        .await
                        .ok();
                    None
                }
                T1MessageBody::VersionedPartialEncodedChunk(chunk) => {
                    self.shards_manager_adapter
                        .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(*chunk));
                    None
                }
                T1MessageBody::PartialEncodedChunkForward(msg) => {
                    self.shards_manager_adapter.send(
                        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(msg),
                    );
                    None
                }
                T1MessageBody::PartialEncodedStateWitness(witness) => {
                    self.partial_witness_adapter.send(PartialEncodedStateWitnessMessage(witness));
                    None
                }
                T1MessageBody::PartialEncodedStateWitnessForward(witness) => {
                    self.partial_witness_adapter
                        .send(PartialEncodedStateWitnessForwardMessage(witness));
                    None
                }
                T1MessageBody::VersionedChunkEndorsement(endorsement) => {
                    self.client.send_async(ChunkEndorsementMessage(endorsement)).await.ok();
                    None
                }
                T1MessageBody::ChunkContractAccesses(accesses) => {
                    self.partial_witness_adapter.send(ChunkContractAccessesMessage(accesses));
                    None
                }
                T1MessageBody::ContractCodeRequest(request) => {
                    self.partial_witness_adapter.send(ContractCodeRequestMessage(request));
                    None
                }
                T1MessageBody::ContractCodeResponse(response) => {
                    self.partial_witness_adapter.send(ContractCodeResponseMessage(response));
                    None
                }
                T1MessageBody::SpicePartialData(spice_partial_data) => {
                    self.spice_data_distributor_adapter
                        .send(SpiceIncomingPartialData { data: spice_partial_data });
                    None
                }
                T1MessageBody::SpiceChunkEndorsement(endorsement) => {
                    self.spice_core_writer_adapter.send(SpiceChunkEndorsementMessage(endorsement));
                    None
                }
                T1MessageBody::SpicePartialDataRequest(request) => {
                    self.spice_data_distributor_adapter.send(request);
                    None
                }
                T1MessageBody::SpiceChunkContractAccesses(accesses) => {
                    self.spice_data_distributor_adapter
                        .send(SpiceChunkContractAccessesMessage(accesses));
                    None
                }
                T1MessageBody::SpiceContractCodeRequest(request) => {
                    self.spice_data_distributor_adapter
                        .send(SpiceContractCodeRequestMessage(request));
                    None
                }
                T1MessageBody::SpiceContractCodeResponse(response) => {
                    self.spice_data_distributor_adapter
                        .send(SpiceContractCodeResponseMessage(response));
                    None
                }
            },
            TieredMessageBody::T2(body) => match *body {
                T2MessageBody::TxStatusRequest(account_id, tx_hash) => self
                    .client
                    .send_async(TxStatusRequest { tx_hash, signer_account_id: account_id })
                    .await
                    .ok()
                    .flatten()
                    .map(|response| {
                        TieredMessageBody::T2(Box::new(T2MessageBody::TxStatusResponse(response)))
                    }),
                T2MessageBody::TxStatusResponse(tx_result) => {
                    self.client.send_async(TxStatusResponse(tx_result.into())).await.ok();
                    None
                }
                T2MessageBody::ForwardTx(transaction) => {
                    self.client
                        .send_async(ProcessTxRequest {
                            transaction,
                            is_forwarded: true,
                            check_only: false,
                        })
                        .await
                        .ok();
                    None
                }
                T2MessageBody::PartialEncodedChunkRequest(request) => {
                    self.shards_manager_adapter.send(
                        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                            partial_encoded_chunk_request: request,
                            route_back: msg_hash,
                        },
                    );
                    None
                }
                T2MessageBody::PartialEncodedChunkResponse(response) => {
                    self.shards_manager_adapter.send(
                        ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                            partial_encoded_chunk_response: response,
                            received_time: clock.now().into(),
                        },
                    );
                    None
                }
                T2MessageBody::ChunkStateWitnessAck(ack) => {
                    self.partial_witness_adapter.send(ChunkStateWitnessAckMessage(ack));
                    None
                }
                T2MessageBody::StateHeaderRequest(request) => {
                    self.peer_manager_adapter.send(Tier3Request {
                        peer_info: PeerInfo {
                            id: msg_author,
                            addr: Some(request.addr),
                            account_id: None,
                        },
                        body: Tier3RequestBody::StateHeader(StateHeaderRequestBody {
                            shard_id: request.shard_id,
                            sync_hash: request.sync_hash,
                        }),
                    });
                    None
                }
                T2MessageBody::StatePartRequest(request) => {
                    self.peer_manager_adapter.send(Tier3Request {
                        peer_info: PeerInfo {
                            id: msg_author,
                            addr: Some(request.addr),
                            account_id: None,
                        },
                        body: Tier3RequestBody::StatePart(StatePartRequestBody {
                            shard_id: request.shard_id,
                            sync_hash: request.sync_hash,
                            part_id: request.part_id,
                        }),
                    });
                    None
                }
                T2MessageBody::PartialEncodedContractDeploys(deploys) => {
                    self.partial_witness_adapter
                        .send(PartialEncodedContractDeploysMessage(deploys));
                    None
                }
                T2MessageBody::StateRequestAck(ack) => {
                    self.client
                        .send_async(
                            StateResponseReceived {
                                peer_id: msg_author,
                                state_response: StateResponse::Ack(ack),
                            }
                            .span_wrap(),
                        )
                        .await
                        .ok();
                    None
                }
                body => {
                    tracing::error!(target: "network", ?body, "peer received unexpected message type");
                    None
                }
            },
        }
    }

    /// Classifies an incoming routed message as for this node, to be
    /// forwarded, or dropped, after per-connection checks (signature
    /// dedup, ForwardTx rate limiting, signature verification) have
    /// passed.
    ///
    /// Records route-back; applies network-wide dedup/metrics for
    /// messages addressed to this node; decrements TTL for messages
    /// that need forwarding.
    ///
    /// Returns a `RoutedAction` for the caller to execute. Ping/Pong
    /// special-casing happens on the caller side.
    pub(crate) fn process_incoming_routed(
        &self,
        clock: &time::Clock,
        from: &PeerId,
        tier: tcp::Tier,
        mut msg: Box<RoutedMessage>,
    ) -> RoutedAction {
        let for_me = self.message_for_me(msg.target());
        if for_me {
            // Network-wide dedup: check if we already received this message
            // (could arrive via both T1 and T2).
            let new_hash = CryptoHash::hash_borsh(msg.body());
            let fastest = self.recent_routed_messages.lock().put(new_hash, ()).is_none();
            metrics::record_routed_msg_metrics(clock, &msg, tier, fastest);
        }

        self.add_route_back(clock, from, tier, &msg);

        if for_me {
            RoutedAction::ForMe(msg)
        } else {
            if msg.decrease_ttl() {
                let num_hops = msg.num_hops_mut();
                *num_hops = num_hops.saturating_add(1);
                RoutedAction::Forward(msg)
            } else {
                #[cfg(test)]
                self.config.event_sink.send(Event::RoutedMessageDropped);
                tracing::debug!(target: "network", ?msg, from = ?from, "message dropped because ttl reached 0");
                metrics::ROUTED_MESSAGE_DROPPED.with_label_values(&[msg.body_variant()]).inc();
                RoutedAction::Dropped
            }
        }
    }

    /// Dispatches an inbound peer message to the appropriate handler.
    ///
    /// Messages handled here are "business logic" messages — TCP-protocol
    /// messages (handshake, peers, gossip, routed forwarding) are handled
    /// by PeerActor directly.
    ///
    /// Returns:
    /// - `Ok(Some(response))` — caller should send response back to peer
    /// - `Ok(None)` — message consumed, no response needed
    /// - `Err(ban_reason)` — caller should ban the peer
    pub async fn handle_peer_message(
        self: &Arc<Self>,
        clock: &time::Clock,
        peer_id: PeerId,
        msg: PeerMessage,
        was_requested: bool,
    ) -> Result<Option<PeerMessage>, ReasonForBan> {
        Ok(match msg {
            PeerMessage::Routed(msg) => {
                let msg_hash = msg.hash();
                self.receive_routed_message(
                    clock,
                    msg.author().clone(),
                    peer_id.clone(),
                    msg_hash,
                    msg.body_owned(),
                )
                .await
                .map(|body| {
                    PeerMessage::Routed(self.sign_message(
                        clock,
                        RawRoutedMessage { target: PeerIdOrHash::Hash(msg_hash), body },
                    ))
                })
            }
            PeerMessage::BlockRequest(hash) => {
                let response = self.client.send_async(BlockRequest(hash)).await;
                response.ok().flatten().map(|block| PeerMessage::Block(block))
            }
            PeerMessage::BlockHeadersRequest(hashes) => {
                let response = self.client.send_async(BlockHeadersRequest(hashes)).await;
                response.ok().flatten().map(PeerMessage::BlockHeaders)
            }
            PeerMessage::Block(block) => {
                // Update connected_peers block_info (monotonic — no-op if
                // the new height is below the stored one).
                let hash = *block.hash();
                let height = block.header().height();
                self.peers.update_block_info(&peer_id, BlockInfo { height, hash });
                self.client
                    .send_async(BlockResponse { block, peer_id, was_requested }.span_wrap())
                    .await
                    .ok();
                None
            }
            PeerMessage::Transaction(transaction) => {
                self.client
                    .send_async(ProcessTxRequest {
                        transaction,
                        is_forwarded: false,
                        check_only: false,
                    })
                    .await
                    .ok();
                None
            }
            PeerMessage::BlockHeaders(headers) => {
                if let Ok(Err(ban_reason)) =
                    self.client.send_async(BlockHeadersResponse(headers, peer_id).span_wrap()).await
                {
                    return Err(ban_reason);
                }
                None
            }
            PeerMessage::Challenge(_) => None,
            PeerMessage::StateRequestHeader(shard_id, sync_hash) => {
                let response = self
                    .state_request_adapter
                    .send_async(StateRequestHeader { shard_id, sync_hash })
                    .await;
                response.ok().flatten().map(|r| PeerMessage::VersionedStateResponse(*r.0))
            }
            PeerMessage::StateRequestPart(shard_id, sync_hash, part_id) => {
                let response = self
                    .state_request_adapter
                    .send_async(StateRequestPart { shard_id, sync_hash, part_id })
                    .await;
                response.ok().flatten().map(|r| PeerMessage::VersionedStateResponse(*r.0))
            }
            PeerMessage::VersionedStateResponse(info) => {
                self.client
                    .send_async(
                        StateResponseReceived {
                            peer_id,
                            state_response: StateResponse::State(info.into()),
                        }
                        .span_wrap(),
                    )
                    .await
                    .ok();
                None
            }
            PeerMessage::EpochSyncRequest => {
                self.client.send(EpochSyncRequestMessage { from_peer: peer_id });
                None
            }
            PeerMessage::EpochSyncResponse(proof) => {
                self.client.send(EpochSyncResponseMessage { from_peer: peer_id, proof });
                None
            }
            PeerMessage::OptimisticBlock(ob) => {
                self.client.send(
                    OptimisticBlockMessage { from_peer: peer_id, optimistic_block: ob }.span_wrap(),
                );
                None
            }
            msg => {
                tracing::error!(target: "network", ?msg, "peer received unexpected type");
                None
            }
        })
    }

    /// Broadcast accounts data to a single peer via its gossip demux.
    /// Deduplicates by account_key, keeping the highest version.
    async fn gossip_accounts_data_to_peer(
        self: Arc<Self>,
        peer_id: PeerId,
        demux: demux::Demux<Vec<Arc<SignedAccountData>>, ()>,
        data: Vec<Arc<SignedAccountData>>,
    ) {
        let res = demux
            .call(data, {
                let this = self.clone();
                let peer_id = peer_id.clone();
                |ds: Vec<Vec<Arc<SignedAccountData>>>| async move {
                    let res = ds.iter().map(|_| ()).collect();
                    let mut sum = HashMap::<_, Arc<SignedAccountData>>::new();
                    for d in ds.into_iter().flatten() {
                        if sum.get(&d.account_key).map_or(true, |old| old.version < d.version) {
                            sum.insert(d.account_key.clone(), d);
                        }
                    }
                    let msg = Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData {
                        incremental: true,
                        requesting_full_sync: false,
                        accounts_data: sum.into_values().collect(),
                    }));
                    this.tier2.send_message(peer_id, msg);
                    res
                }
            })
            .await;
        if res.is_err() {
            tracing::debug!(%peer_id, "peer disconnected while sending sync accounts data");
        }
    }

    /// Broadcast snapshot hosts to a single peer via its gossip demux.
    /// Deduplicates by peer_id, keeping the highest epoch_height.
    async fn gossip_snapshot_hosts_to_peer(
        self: Arc<Self>,
        peer_id: PeerId,
        demux: demux::Demux<Vec<Arc<SnapshotHostInfo>>, ()>,
        data: Vec<Arc<SnapshotHostInfo>>,
    ) {
        let res = demux
            .call(data, {
                let this = self.clone();
                let peer_id = peer_id.clone();
                |ds: Vec<Vec<Arc<SnapshotHostInfo>>>| async move {
                    let res = ds.iter().map(|_| ()).collect();
                    let mut sum = HashMap::<_, Arc<SnapshotHostInfo>>::new();
                    for d in ds.into_iter().flatten() {
                        if sum.get(&d.peer_id).map_or(true, |old| old.epoch_height < d.epoch_height)
                        {
                            sum.insert(d.peer_id.clone(), d);
                        }
                    }
                    let msg = Arc::new(PeerMessage::SyncSnapshotHosts(SyncSnapshotHosts {
                        hosts: sum.into_values().collect(),
                    }));
                    this.tier2.send_message(peer_id, msg);
                    res
                }
            })
            .await;
        if res.is_err() {
            tracing::debug!(%peer_id, "peer disconnected while sending sync snapshot hosts");
        }
    }

    pub async fn add_accounts_data(
        self: &Arc<Self>,
        clock: &time::Clock,
        accounts_data: Vec<Arc<SignedAccountData>>,
    ) -> Option<AccountDataError> {
        let this = self.clone();
        let clock = clock.clone();
        self.spawn("add_accounts_data", async move {
            // Verify and add the new data to the internal state.
            let (new_data, err) = this.accounts_data.clone().insert(&clock, accounts_data).await;
            // Broadcast any new data we have found, even in presence of an error.
            // This will prevent a malicious peer from forcing us to re-verify valid
            // datasets. See accounts_data::Cache documentation for details.
            if new_data.is_empty() {
                return err;
            }
            // Snapshot the demux map in a scoped block so the MutexGuard
            // drops before we start spawning tasks (each `this.spawn`
            // may take unrelated locks).
            let peers: Vec<_> = {
                let guard = this.accounts_data_demuxes.lock();
                guard.iter().map(|(id, demux)| (id.clone(), demux.clone())).collect()
            };
            let tasks: Vec<_> = peers
                .into_iter()
                .map(|(peer_id, demux)| {
                    this.spawn(
                        "send_accounts_data",
                        this.clone().gossip_accounts_data_to_peer(peer_id, demux, new_data.clone()),
                    )
                })
                .collect();
            for t in tasks {
                t.await.unwrap();
            }
            err
        })
        .await
        .unwrap()
    }

    pub async fn add_snapshot_hosts(
        self: &Arc<Self>,
        hosts: Vec<Arc<SnapshotHostInfo>>,
    ) -> Option<SnapshotHostInfoError> {
        let this = self.clone();
        self.spawn("add_snapshot_hosts", async move {
            // Verify and add the new data to the internal state.
            let (new_data, err) = this.snapshot_hosts.clone().insert(hosts).await;
            // Broadcast any valid new data, even if an err was returned.
            // The presence of one invalid entry doesn't invalidate the remaining ones.
            if new_data.is_empty() {
                return err;
            }
            // Snapshot first, drop the guard, then spawn — same
            // reasoning as `add_accounts_data`.
            let peers: Vec<_> = {
                let guard = this.snapshot_hosts_demuxes.lock();
                guard.iter().map(|(id, demux)| (id.clone(), demux.clone())).collect()
            };
            let tasks: Vec<_> = peers
                .into_iter()
                .map(|(peer_id, demux)| {
                    this.spawn(
                        "send_snapshot_hosts",
                        this.clone().gossip_snapshot_hosts_to_peer(
                            peer_id,
                            demux,
                            new_data.clone(),
                        ),
                    )
                })
                .collect();
            for t in tasks {
                t.await.unwrap();
            }
            err
        })
        .await
        .unwrap()
    }

    /// a) there is a peer we should be connected to, but we aren't
    /// b) there is an edge indicating that we should be disconnected from a peer, but we are connected.
    /// Try to resolve the inconsistency.
    /// We call this function every FIX_LOCAL_EDGES_INTERVAL from peer_manager_actor.rs.
    pub async fn fix_local_edges(
        self: &Arc<Self>,
        clock: &time::Clock,
        timeout: time::Duration,
        transport: Arc<dyn NetworkTransport>,
    ) {
        let this = self.clone();
        let clock = clock.clone();
        self.spawn("fix_local_edges", async move {
            let graph = this.graph.load();
            let tier2 = this.tier2.load();
            let mut tasks = vec![];
            for edge in graph.local_edges.values() {
                let edge = edge.clone();
                let node_id = this.config.node_id();
                let other_peer = edge.other(&node_id).unwrap();
                match (tier2.ready.get(other_peer), edge.edge_type()) {
                    // This is an active connection, while the edge indicates it shouldn't.
                    (Some(conn), EdgeState::Removed) => {
                        tasks.push(this.spawn("fix_local_edges", {
                            let this = this.clone();
                            let conn = conn.clone();
                            let clock = clock.clone();
                            async move {
                                conn.send_message(Arc::new(PeerMessage::RequestUpdateNonce(
                                    PartialEdgeInfo::new(
                                        &node_id,
                                        &conn.peer_info.id,
                                        std::cmp::max(
                                            Edge::create_fresh_nonce(&clock),
                                            edge.next(),
                                        ),
                                        &this.config.node_key,
                                    ),
                                )));
                                // TODO(gprusak): here we should synchronically wait for the RequestUpdateNonce
                                // response (with timeout). Until network round trips are implemented, we just
                                // blindly wait for a while, then check again.
                                clock.sleep(timeout).await;
                                match this.graph.load().local_edges.get(&conn.peer_info.id) {
                                    Some(edge) if edge.edge_type() == EdgeState::Active => return,
                                    _ => conn.stop(None),
                                }
                            }
                        }))
                    }
                    // We are not connected to this peer, but routing table contains
                    // information that we do. We should wait and remove that peer
                    // from routing table
                    (None, EdgeState::Active) => tasks.push(this.spawn("fix_local_edges", {
                        let this = this.clone();
                        let clock = clock.clone();
                        let other_peer = other_peer.clone();
                        let transport = transport.clone();
                        async move {
                            // This edge says this is an connected peer, which is currently not in the set of connected peers.
                            // Wait for some time to let the connection begin or broadcast edge removal instead.
                            clock.sleep(timeout).await;
                            if this.tier2.load().ready.contains_key(&other_peer) {
                                return;
                            }
                            // Peer is still not connected after waiting a timeout.
                            // Unwrap is safe, because new_edge is always valid.
                            let new_edge =
                                edge.remove_edge(this.config.node_id(), &this.config.node_key);
                            this.add_edges(
                                &clock,
                                EdgesWithSource::Local(vec![new_edge.clone()]),
                                transport,
                            )
                            .await
                            .unwrap()
                        }
                    })),
                    // OK
                    _ => {}
                }
            }
            for t in tasks {
                let _ = t.await;
            }
        })
        .await
        .unwrap()
    }

    pub fn update_connection_store(self: &Arc<Self>, clock: &time::Clock) {
        self.connection_store.update(clock, &self.tier2.load());
    }

    /// Clears pending_reconnect and returns the cleared values
    pub fn poll_pending_reconnect(&self) -> Vec<PeerInfo> {
        let mut pending_reconnect = self.pending_reconnect.lock();
        let polled = pending_reconnect.clone();
        pending_reconnect.clear();
        return polled;
    }

    /// Collects and returns PeerInfos for all directly connected TIER2 peers.
    pub fn get_direct_peers(self: &Arc<Self>) -> Vec<PeerInfo> {
        return self.tier2.load().ready.values().map(|c| c.peer_info.clone()).collect();
    }

    /// Sets the chain info, and updates the set of TIER1 keys.
    /// Returns true iff the set of TIER1 keys has changed.
    pub fn set_chain_info(self: &Arc<Self>, info: ChainInfo) -> bool {
        let _mutex = self.set_chain_info_mutex.lock();

        // We set state.chain_info and call accounts_data.set_keys
        // synchronously, therefore, assuming actors deliver messages in order, there
        // will be no race condition between subsequent SetChainInfo calls.
        self.chain_info.store(Arc::new(Some(info.clone())));

        // The set of TIER1 accounts has changed, so we might be missing some
        // accounts_data that our peers know about.
        let has_changed = self.accounts_data.set_keys(info.tier1_accounts);
        if has_changed {
            self.tier1_request_full_sync();
        }
        has_changed
    }
}
