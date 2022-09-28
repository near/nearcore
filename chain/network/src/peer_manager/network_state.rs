use crate::accounts_data;
use crate::concurrency::rate;
use crate::config;
use crate::network_protocol::{
    AccountOrPeerIdOrHash, Ping, Pong, RawRoutedMessage, RoutedMessageBody, RoutedMessageV2,
};
use crate::network_protocol::{Edge, PartialEdgeInfo, PeerAddr};
use crate::peer::peer_actor::{PeerActor, StreamConfig};
use crate::peer_manager::connection;
use crate::private_actix::PeerToManagerMsg;
use crate::routing;
use crate::routing::route_back_cache::RouteBackCache;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use crate::time;
use crate::types::{ChainInfo, NetworkClientMessages, PeerMessage};
use crate::types::{NetworkViewClientMessages, PeerIdOrHash};
use actix::Recipient;
use arc_swap::ArcSwap;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use rand::seq::IteratorRandom as _;
use rand::seq::SliceRandom as _;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::{debug, info, trace};

/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: time::Duration = time::Duration::milliseconds(60_000);
/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;

/// Send important messages three times.
/// We send these messages multiple times to reduce the chance that they are lost
const IMPORTANT_MESSAGE_RESENT_COUNT: usize = 3;

pub(crate) struct NetworkState {
    /// PeerManager config.
    pub config: Arc<config::VerifiedConfig>,
    /// GenesisId of the chain.
    pub genesis_id: GenesisId,
    /// Address of the client actor.
    pub client_addr: Recipient<NetworkClientMessages>,
    /// Address of the view client actor.
    pub view_client_addr: Recipient<NetworkViewClientMessages>,
    /// Address of the peer manager actor.
    pub peer_manager_addr: Recipient<PeerToManagerMsg>,
    /// RoutingTableActor, responsible for computing routing table, routing table exchange, etc.
    pub routing_table_addr: actix::Addr<routing::Actor>,

    /// Network-related info about the chain.
    pub chain_info: ArcSwap<ChainInfo>,
    /// AccountsData for TIER1 accounts.
    pub accounts_data: Arc<accounts_data::Cache>,
    /// Connected peers (inbound and outbound) with their full peer information.
    pub tier2: connection::Pool,
    pub tier1: connection::Pool,
    /// Semaphore limiting inflight inbound handshakes.
    pub inbound_handshake_permits: Arc<tokio::sync::Semaphore>,

    /// View of the Routing table. It keeps:
    /// - routing information - how to route messages
    /// - edges adjacent to my_peer_id
    /// - account id
    /// Full routing table (that currently includes information about all edges in the graph) is now inside Routing Table.
    pub routing_table_view: RoutingTableView,

    /// Hash of messages that requires routing back to respective previous hop.
    pub tier1_route_back: Mutex<RouteBackCache>,

    /// Shared counter across all PeerActors, which counts number of `RoutedMessageBody::ForwardTx`
    /// messages sincce last block.
    pub txns_since_last_block: AtomicUsize,

    pub tier1_recv_limiter: rate::Limiter,
}

impl NetworkState {
    pub fn new(
        clock: &time::Clock,
        config: Arc<config::VerifiedConfig>,
        genesis_id: GenesisId,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
        peer_manager_addr: Recipient<PeerToManagerMsg>,
        routing_table_addr: actix::Addr<routing::Actor>,
        routing_table_view: RoutingTableView,
    ) -> Self {
        Self {
            routing_table_addr,
            genesis_id,
            client_addr,
            view_client_addr,
            peer_manager_addr,
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            tier1: connection::Pool::new(config.node_id()),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            accounts_data: Arc::new(accounts_data::Cache::new()),
            routing_table_view,
            tier1_route_back: Mutex::new(RouteBackCache::default()),
            tier1_recv_limiter: rate::Limiter::new(
                clock,
                rate::Limit {
                    qps: (20 * bytesize::MIB) as f64,
                    burst: (40 * bytesize::MIB) as u64,
                },
            ),
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

    pub async fn tier1_daemon_tick(self: &Arc<Self>, clock: &time::Clock, cfg: &config::Tier1) {
        let accounts_data = self.accounts_data.load();
        // Check if our node is currently a TIER1 validator.
        // If so, it should establish TIER1 connections.
        let my_tier1_account_id = match &self.config.validator {
            Some(v)
                if accounts_data
                    .contains_account_key(v.signer.validator_id(), &v.signer.public_key()) =>
            {
                Some(v.signer.validator_id())
            }
            _ => None,
        };
        let mut accounts_by_peer = HashMap::<_, Vec<_>>::new();
        let mut accounts_by_proxy = HashMap::<_, Vec<_>>::new();
        let mut proxies_by_account = HashMap::<_, Vec<_>>::new();
        for d in accounts_data.data.values() {
            proxies_by_account.entry(&d.account_id).or_default().extend(d.peers.iter());
            if let Some(peer_id) = &d.peer_id {
                accounts_by_peer.entry(peer_id).or_default().push(&d.account_id);
            }
            for p in &d.peers {
                accounts_by_proxy.entry(&p.peer_id).or_default().push(&d.account_id);
            }
        }

        let tier1 = self.tier1.load();
        let mut ready: Vec<_> = tier1.ready.values().collect();

        // Browse the connections from oldest to newest.
        ready.sort_unstable_by_key(|c| c.connection_established_time);
        ready.reverse();
        let ready: Vec<&PeerId> = ready.into_iter().map(|c| &c.peer_info.id).collect();

        // Select the oldest TIER1 connection for each account.
        let mut safe = HashMap::<&AccountId, &PeerId>::new();
        // Direct TIER1 connections have priority.
        for peer_id in &ready {
            for account_id in accounts_by_peer.get(peer_id).into_iter().flatten() {
                safe.insert(account_id, peer_id);
            }
        }
        if my_tier1_account_id.is_some() {
            tracing::debug!(target:"test", "I am a validator!");
            // TIER1 nodes can also connect to TIER1 proxies.
            for peer_id in &ready {
                for account_id in accounts_by_proxy.get(peer_id).into_iter().flatten() {
                    safe.insert(account_id, peer_id);
                }
            }
        }
        // Close all other connections, as they are redundant or are no longer TIER1.
        let safe_set: HashSet<_> = safe.values().copied().collect();
        for conn in tier1.ready.values() {
            if !safe_set.contains(&conn.peer_info.id) {
                conn.stop(None);
            }
        }
        if let Some(my_tier1_account_id) = my_tier1_account_id {
            // Try to establish new TIER1 connections to accounts in random order.
            let mut account_ids: Vec<_> = proxies_by_account.keys().copied().collect();
            account_ids.shuffle(&mut rand::thread_rng());
            let mut new_connections = 0;
            for account_id in account_ids {
                tracing::debug!(target:"test","checking {account_id}");
                // Do not connect to yourself.
                if account_id == my_tier1_account_id {
                    continue;
                }
                if new_connections >= cfg.new_connections_per_tick {
                    break;
                }
                if safe.contains_key(account_id) {
                    tracing::debug!(target:"test","TIER1 connection already exists");
                    continue;
                }
                let proxies: Vec<&PeerAddr> =
                    proxies_by_account.get(account_id).into_iter().flatten().map(|x| *x).collect();
                tracing::debug!(target: "test","available proxies: {proxies:?}");
                // It there is an outound connection in progress to a potential proxy, then skip.
                if proxies.iter().any(|p| tier1.outbound_handshakes.contains(&p.peer_id)) {
                    tracing::debug!(target:"test","outbound handshake already in progress");
                    continue;
                }
                // Start a new connection to one of the proxies of the account A, if
                // we are not already connected/connecting to any proxy of A.
                let proxy = proxies.iter().choose(&mut rand::thread_rng());
                if let Some(proxy) = proxy {
                    tracing::debug!(target:"test", "starting connection to {:?}",proxy);
                    new_connections += 1;
                    self.clone()
                        .spawn_outbound(clock.clone(), (*proxy).clone(), connection::Tier::T1)
                        .await;
                }
            }
        }
    }

    pub fn get_tier1_peer(
        &self,
        account_id: &AccountId,
    ) -> Option<(PeerId, Arc<connection::Connection>)> {
        let tier1 = self.tier1.load();
        let accounts_data = self.accounts_data.load();
        for data in accounts_data.by_account.get(account_id)?.values() {
            let peer_id = match &data.peer_id {
                Some(id) => id,
                None => continue,
            };
            if let Some(conn) = tier1.ready.get(peer_id) {
                return Some((peer_id.clone(), conn.clone()));
            }
        }
        return None;
    }

    // Finds a TIER1 connection for the given AccountId.
    // It is expected to perform <10 lookups total on average,
    // so the call latency should be negligible wrt sending a TCP packet.
    // If not, consider precomputing the AccountId -> Connection mapping.
    pub fn get_tier1_proxy(
        &self,
        account_id: &AccountId,
    ) -> Option<(PeerId, Arc<connection::Connection>)> {
        // Prefer direct connections.
        if let Some(res) = self.get_tier1_peer(account_id) {
            return Some(res);
        }
        // In Case there is no direct connection, use a proxy.
        let tier1 = self.tier1.load();
        let accounts_data = self.accounts_data.load();
        for data in accounts_data.by_account.get(account_id)?.values() {
            let peer_id = match &data.peer_id {
                Some(id) => id,
                None => continue,
            };
            for proxy in &data.peers {
                if let Some(conn) = tier1.ready.get(&proxy.peer_id) {
                    return Some((peer_id.clone(), conn.clone()));
                }
            }
        }
        None
    }

    /// Connects peer with given TcpStream.
    /// It will fail (and log) if we have too many connections already,
    /// or if the peer drops the connection in the meantime.
    fn spawn_peer_actor(
        self: Arc<Self>,
        clock: &time::Clock,
        stream: TcpStream,
        stream_cfg: StreamConfig,
    ) {
        if let Err(err) = PeerActor::spawn(clock.clone(), stream, stream_cfg, None, self.clone()) {
            tracing::info!(target:"network", ?err, "PeerActor::spawn()");
        };
    }

    pub async fn spawn_inbound(self: Arc<Self>, clock: &time::Clock, stream: TcpStream) {
        self.spawn_peer_actor(clock, stream, StreamConfig::Inbound);
    }

    pub async fn spawn_outbound(
        self: Arc<Self>,
        clock: time::Clock,
        peer: PeerAddr,
        tier: connection::Tier,
    ) {
        // The `connect` may take several minutes. This happens when the
        // `SYN` packet for establishing a TCP connection gets silently
        // dropped, in which case the default TCP timeout is applied. That's
        // too long for us, so we shorten it to one second.
        //
        // Why exactly a second? It was hard-coded in a library we used
        // before, so we keep it to preserve behavior. Removing the timeout
        // completely was observed to break stuff for real on the testnet.
        let stream = match tokio::time::timeout(
            std::time::Duration::from_secs(1),
            TcpStream::connect(peer.addr),
        )
        .await
        {
            Ok(Ok(it)) => it,
            Ok(Err(err)) => {
                info!(target: "network", addr=?peer.addr, ?err, "Error connecting to");
                return;
            }
            Err(err) => {
                info!(target: "network", addr=?peer.addr, ?err, "Error connecting to");
                return;
            }
        };
        debug!(target: "network", ?peer, "Connecting");
        self.spawn_peer_actor(
            &clock,
            stream,
            StreamConfig::Outbound { peer_id: peer.peer_id, tier },
        );
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

    pub fn send_ping(
        &self,
        clock: &time::Clock,
        tier: connection::Tier,
        nonce: u64,
        target: PeerId,
    ) {
        let body = RoutedMessageBody::Ping(Ping { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(clock, tier, self.sign_message(clock, msg));
    }

    pub fn send_pong(
        &self,
        clock: &time::Clock,
        tier: connection::Tier,
        nonce: u64,
        target: CryptoHash,
    ) {
        let body = RoutedMessageBody::Pong(Pong { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(clock, tier, self.sign_message(clock, msg));
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
    pub fn send_message_to_peer(
        &self,
        clock: &time::Clock,
        tier: connection::Tier,
        msg: Box<RoutedMessageV2>,
    ) -> bool {
        let my_peer_id = self.config.node_id();

        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = &msg.target {
            if target == &my_peer_id {
                debug!(target: "network", account_id = ?self.config.validator.as_ref().map(|v|v.account_id()), ?my_peer_id, ?msg, "Drop signed message to myself");
                metrics::CONNECTED_TO_MYSELF.inc();
                return false;
            }
        }
        match tier {
            connection::Tier::T1 => {
                let peer_id = match &msg.target {
                    PeerIdOrHash::Hash(hash) => {
                        match self.tier1_route_back.lock().remove(clock, hash) {
                            Some(peer_id) => peer_id,
                            None => return false,
                        }
                    }
                    PeerIdOrHash::PeerId(peer_id) => peer_id.clone(),
                };
                return self.tier1.send_message(peer_id, Arc::new(PeerMessage::Routed(msg)));
            }
            connection::Tier::T2 => match self.routing_table_view.find_route(&clock, &msg.target) {
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
            },
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
        if connection::Tier::T1.is_allowed_routed(&msg) {
            if let Some((target, conn)) = self.get_tier1_proxy(account_id) {
                // TODO(gprusak): in case of PartialEncodedChunk, consider stripping everything
                // but the header. This will bound the message size
                conn.send_message(Arc::new(PeerMessage::Routed(self.sign_message(
                    clock,
                    RawRoutedMessage {
                        target: AccountOrPeerIdOrHash::PeerId(target),
                        body: msg.clone(),
                    },
                ))));
            }
        }

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

        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body: msg };
        let msg = self.sign_message(clock, msg);
        if msg.body.is_important() {
            let mut success = false;
            for _ in 0..IMPORTANT_MESSAGE_RESENT_COUNT {
                success |= self.send_message_to_peer(clock, connection::Tier::T2, msg.clone());
            }
            success
        } else {
            self.send_message_to_peer(clock, connection::Tier::T2, msg)
        }
    }

    pub fn add_verified_edges_to_routing_table(&self, edges: Vec<Edge>) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_view.add_local_edges(&edges);
        self.routing_table_addr.do_send(routing::actor::Message::AddVerifiedEdges { edges });
    }
}
