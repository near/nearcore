use crate::accounts_data;
use crate::client;
use crate::config;
use crate::network_protocol::{
    Edge, EdgeState, PartialEdgeInfo, PeerIdOrHash, PeerInfo, PeerMessage, Ping, Pong,
    RawRoutedMessage, RoutedMessageBody, RoutedMessageV2, RoutingTableUpdate,
};
use crate::peer_manager::connection;
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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{debug, trace};

/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;

/// Send important messages three times.
/// We send these messages multiple times to reduce the chance that they are lost
const IMPORTANT_MESSAGE_RESENT_COUNT: usize = 3;

impl WhitelistNode {
    pub fn from_peer_info(pi: &PeerInfo) -> anyhow::Result<Self> {
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
    /// PeerManager config.
    pub config: Arc<config::VerifiedConfig>,
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

    /// Whitelisted nodes, which are allowed to connect even if the connection limit has been
    /// reached.
    whitelist_nodes: Vec<WhitelistNode>,
    /// Maximal allowed number of peer connections.
    /// It is initialized with config.max_num_peers and is mutable
    /// only so that it can be changed in tests.
    /// TODO(gprusak): determine why tests need to change that dynamically
    /// in the first place.
    pub max_num_peers: AtomicU32,
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
        whitelist_nodes: Vec<WhitelistNode>,
    ) -> Self {
        let graph = Arc::new(RwLock::new(routing::GraphWithCache::new(config.node_id())));
        Self {
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
            whitelist_nodes,
            max_num_peers: AtomicU32::new(config.max_num_peers),
            config,
            txns_since_last_block: AtomicUsize::new(0),
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
    pub fn is_inbound_allowed(&self, peer_info: &PeerInfo) -> bool {
        // Check if we have spare inbound connections capacity.
        let tier2 = self.tier2.load();
        if tier2.ready.len() + tier2.outbound_handshakes.len()
            < self.max_num_peers.load(Ordering::Relaxed) as usize
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

    /// Removes the connection from the state.
    pub fn unregister(
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
                self.add_verified_edges_to_routing_table(vec![edge_update.clone()]);
                self.tier2.broadcast_message(Arc::new(PeerMessage::SyncRoutingTable(
                    RoutingTableUpdate::from_edges(vec![edge_update]),
                )));
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

    pub fn add_verified_edges_to_routing_table(&self, edges: Vec<Edge>) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_view.add_local_edges(&edges);
        self.routing_table_addr
            .do_send(routing::actor::Message::AddVerifiedEdges { edges }.with_span_context());
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
}
