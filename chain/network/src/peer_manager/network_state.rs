use crate::accounts_data;
use crate::concurrency::demux;
use crate::config;
use crate::network_protocol::{
    AccountOrPeerIdOrHash, PartialEdgeInfo, PeerIdOrHash, PeerMessage, Ping, Pong,
    RawRoutedMessage, RoutedMessageBody, RoutedMessageV2,
};
use crate::peer_manager::connection;
use crate::private_actix::PeerToManagerMsg;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use crate::time;
use crate::types::{ChainInfo, NetworkClientMessages, NetworkViewClientMessages};
use actix::Recipient;
use arc_swap::ArcSwap;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tracing::{debug, trace};

/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: time::Duration = time::Duration::milliseconds(60_000);
/// Limit number of pending Peer actors to avoid OOM.
pub(crate) const LIMIT_PENDING_PEERS: usize = 60;

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
    /// Address of the peer manager actor.
    pub peer_manager_addr: Recipient<PeerToManagerMsg>,

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
        peer_manager_addr: Recipient<PeerToManagerMsg>,
        routing_table_view: RoutingTableView,
        send_accounts_data_rl: demux::RateLimit,
    ) -> Self {
        Self {
            genesis_id,
            client_addr,
            view_client_addr,
            peer_manager_addr,
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            accounts_data: Arc::new(accounts_data::Cache::new()),
            routing_table_view,
            send_accounts_data_rl,
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
        self.send_message_to_peer(clock, self.sign_message(clock, msg));
    }

    pub fn send_pong(&self, clock: &time::Clock, nonce: u64, target: CryptoHash) {
        let body = RoutedMessageBody::Pong(Pong { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body };
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
