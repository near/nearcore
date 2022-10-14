use crate::accounts_data;
use crate::client;
use crate::concurrency::rate;
use crate::config;
use crate::network_protocol::{
    Edge, EdgeState, PartialEdgeInfo, PeerIdOrHash, PeerMessage, Ping, Pong, RawRoutedMessage,
    RoutedMessageBody, RoutedMessageV2, RoutingTableUpdate,
};
use crate::peer_manager::connection;
use crate::private_actix::{PeerToManagerMsg, ValidateEdgeList};
use crate::routing;
use crate::routing::edge_validator_actor::EdgeValidatorHelper;
use crate::routing::route_back_cache::RouteBackCache;
use crate::routing::routing_table_view::RoutingTableView;
use crate::stats::metrics;
use crate::tcp;
use crate::time;
use crate::types::{ChainInfo, ReasonForBan};
use actix::Recipient;
use arc_swap::ArcSwap;
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

mod tier1;

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
    pub client: client::Client,
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
    /// Fields used for communicating with EdgeValidatorActor
    pub routing_table_exchange_helper: EdgeValidatorHelper,

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
        client: client::Client,
        peer_manager_addr: Recipient<PeerToManagerMsg>,
        routing_table_addr: actix::Addr<routing::Actor>,
        routing_table_view: RoutingTableView,
    ) -> Self {
        Self {
            routing_table_addr,
            genesis_id,
            client,
            peer_manager_addr,
            chain_info: Default::default(),
            tier2: connection::Pool::new(config.node_id()),
            tier1: connection::Pool::new(config.node_id()),
            inbound_handshake_permits: Arc::new(tokio::sync::Semaphore::new(LIMIT_PENDING_PEERS)),
            accounts_data: Arc::new(accounts_data::Cache::new()),
            routing_table_view,
            routing_table_exchange_helper: Default::default(),
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
    pub fn ask_for_more_peers(&self) {
        let msg = Arc::new(PeerMessage::PeersRequest);
        for peer in self.tier2.load().ready.values() {
            peer.send_message(msg.clone());
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

    /// Removes the connection from the state.
    // TODO(gprusak): move PeerManagerActor::unregister logic here as well.
    pub fn unregister(&self, conn: &Arc<connection::Connection>) {
        let peer_id = conn.peer_info.id.clone();
        if conn.tier == tcp::Tier::T1 {
            // There is no banning or routing table for TIER1.
            // Just remove the connection from the network_state.
            self.tier1.remove(conn);
            // TODO(gprusak): move PeerStore to NetworkState!!!.
            return;
        }
        self.tier2.remove(conn);

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

    pub fn send_ping(&self, clock: &time::Clock, tier: tcp::Tier, nonce: u64, target: PeerId) {
        let body = RoutedMessageBody::Ping(Ping { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(clock, tier, self.sign_message(clock, msg));
    }

    pub fn send_pong(&self, clock: &time::Clock, tier: tcp::Tier, nonce: u64, target: CryptoHash) {
        let body = RoutedMessageBody::Pong(Pong { nonce, source: self.config.node_id() });
        let msg = RawRoutedMessage { target: PeerIdOrHash::Hash(target), body };
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
        tier: tcp::Tier,
        msg: Box<RoutedMessageV2>,
    ) -> bool {
        let my_peer_id = self.config.node_id();

        // Check if the message is for myself and don't try to send it in that case.
        if let PeerIdOrHash::PeerId(target) = &msg.target {
            if target == &my_peer_id {
                tracing::debug!(target: "network", account_id = ?self.config.validator.as_ref().map(|v|v.account_id()), ?my_peer_id, ?msg, "Drop signed message to myself");
                metrics::CONNECTED_TO_MYSELF.inc();
                return false;
            }
        }
        match tier {
            tcp::Tier::T1 => {
                tracing::debug!(target:"test", "sending msg over TIER1");
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
            tcp::Tier::T2 => match self.routing_table_view.find_route(&clock, &msg.target) {
                Ok(peer_id) => {
                    // Remember if we expect a response for this message.
                    if msg.author == my_peer_id && msg.expect_response() {
                        tracing::trace!(target: "network", ?msg, "initiate route back");
                        self.routing_table_view.add_route_back(&clock, msg.hash(), my_peer_id);
                    }
                    return self.tier2.send_message(peer_id, Arc::new(PeerMessage::Routed(msg)));
                }
                Err(find_route_error) => {
                    // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                    metrics::MessageDropped::NoRouteFound.inc(&msg.body);

                    tracing::debug!(target: "network",
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
        if tcp::Tier::T1.is_allowed_routed(&msg) {
            tracing::debug!(target:"test", "got TIER1 message to send");
            if let Some((target, conn)) = self.get_tier1_proxy(account_id) {
                tracing::debug!(target:"test", "found TIER1 proxy");
                // TODO(gprusak): in case of PartialEncodedChunk, consider stripping everything
                // but the header. This will bound the message size
                conn.send_message(Arc::new(PeerMessage::Routed(self.sign_message(
                    clock,
                    RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body: msg.clone() },
                ))));
            }
        }

        let target = match self.routing_table_view.account_owner(account_id) {
            Some(peer_id) => peer_id,
            None => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                metrics::MessageDropped::UnknownAccount.inc(&msg);
                tracing::debug!(target: "network",
                       account_id = ?self.config.validator.as_ref().map(|v|v.account_id()),
                       to = ?account_id,
                       ?msg,"Drop message: unknown account",
                );
                tracing::trace!(target: "network", known_peers = ?self.routing_table_view.get_accounts_keys(), "Known peers");
                return false;
            }
        };

        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body: msg };
        let msg = self.sign_message(clock, msg);
        if msg.body.is_important() {
            let mut success = false;
            for _ in 0..IMPORTANT_MESSAGE_RESENT_COUNT {
                success |= self.send_message_to_peer(clock, tcp::Tier::T2, msg.clone());
            }
            success
        } else {
            self.send_message_to_peer(clock, tcp::Tier::T2, msg)
        }
    }

    pub fn add_verified_edges_to_routing_table(&self, edges: Vec<Edge>) {
        if edges.is_empty() {
            return;
        }
        self.routing_table_view.add_local_edges(&edges);
        self.routing_table_addr.do_send(routing::actor::Message::AddVerifiedEdges { edges });
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
        self.routing_table_addr.do_send(routing::actor::Message::ValidateEdgeList(
            ValidateEdgeList {
                source_peer_id: peer_id,
                edges,
                edges_info_shared: self.routing_table_exchange_helper.edges_info_shared.clone(),
                sender: self.routing_table_exchange_helper.edges_to_add_sender.clone(),
            },
        ));
    }

    async fn receive_routed_message(
        &self,
        clock: &time::Clock,
        peer_id: PeerId,
        msg_hash: CryptoHash,
        body: RoutedMessageBody,
    ) -> Result<Option<RoutedMessageBody>, ReasonForBan> {
        Ok(match body {
            RoutedMessageBody::TxStatusRequest(account_id, tx_hash) => self
                .client
                .tx_status_request(account_id, tx_hash)
                .await?
                .map(RoutedMessageBody::TxStatusResponse),
            RoutedMessageBody::TxStatusResponse(tx_result) => {
                self.client.tx_status_response(tx_result).await?;
                None
            }
            RoutedMessageBody::StateRequestHeader(shard_id, sync_hash) => self
                .client
                .state_request_header(shard_id, sync_hash)
                .await?
                .map(RoutedMessageBody::VersionedStateResponse),
            RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id) => self
                .client
                .state_request_part(shard_id, sync_hash, part_id)
                .await?
                .map(RoutedMessageBody::VersionedStateResponse),
            RoutedMessageBody::VersionedStateResponse(info) => {
                self.client.state_response(info).await?;
                None
            }
            RoutedMessageBody::BlockApproval(approval) => {
                self.client.block_approval(approval, peer_id).await?;
                None
            }
            RoutedMessageBody::ForwardTx(transaction) => {
                self.client.transaction(transaction, /*is_forwarded=*/ true).await?;
                None
            }
            RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                self.client.partial_encoded_chunk_request(request, msg_hash).await?;
                None
            }
            RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                self.client.partial_encoded_chunk_response(response, clock.now()).await?;
                None
            }
            RoutedMessageBody::VersionedPartialEncodedChunk(chunk) => {
                self.client.partial_encoded_chunk(chunk).await?;
                None
            }
            RoutedMessageBody::PartialEncodedChunkForward(msg) => {
                self.client.partial_encoded_chunk_forward(msg).await?;
                None
            }
            RoutedMessageBody::ReceiptOutcomeRequest(_) => {
                // Silently ignore for the time being.  We’ve been still
                // sending those messages at protocol version 56 so we
                // need to wait until 59 before we can remove the
                // variant completely.
                None
            }
            body => {
                tracing::error!(target: "network", "Peer received unexpected message type: {:?}", body);
                None
            }
        })
    }

    pub async fn receive_message(
        &self,
        clock: &time::Clock,
        peer_id: PeerId,
        msg: PeerMessage,
        was_requested: bool,
    ) -> Result<Option<PeerMessage>, ReasonForBan> {
        Ok(match msg {
            PeerMessage::Routed(msg) => {
                let msg_hash = msg.hash();
                self.receive_routed_message(clock, peer_id, msg_hash, msg.msg.body).await?.map(
                    |body| {
                        PeerMessage::Routed(self.sign_message(
                            &clock,
                            RawRoutedMessage { target: PeerIdOrHash::Hash(msg_hash), body },
                        ))
                    },
                )
            }
            PeerMessage::BlockRequest(hash) => {
                self.client.block_request(hash).await?.map(PeerMessage::Block)
            }
            PeerMessage::BlockHeadersRequest(hashes) => {
                self.client.block_headers_request(hashes).await?.map(PeerMessage::BlockHeaders)
            }
            PeerMessage::Block(block) => {
                self.client.block(block, peer_id, was_requested).await?;
                None
            }
            PeerMessage::Transaction(transaction) => {
                self.client.transaction(transaction, /*is_forwarded=*/ false).await?;
                None
            }
            PeerMessage::BlockHeaders(headers) => {
                self.client.block_headers(headers, peer_id).await?;
                None
            }
            PeerMessage::Challenge(challenge) => {
                self.client.challenge(challenge).await?;
                None
            }
            msg => {
                tracing::error!(target: "network", "Peer received unexpected type: {:?}", msg);
                None
            }
        })
    }
}
