use crate::accounts_data;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::concurrency::demux;
use crate::network_protocol::{
    Edge, EdgeState, Encoding, OwnedAccount, ParsePeerMessageError, PartialEdgeInfo,
    PeerChainInfoV2, PeerIdOrHash, PeerInfo, PeersResponse, RawRoutedMessage, RoutedMessageBody,
    RoutedMessageV2, RoutingTableUpdate, StateResponseInfo, SyncAccountsData,
};
use crate::peer::stream;
use crate::peer::tracker::Tracker;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::{NetworkState, Event, PRUNE_EDGES_AFTER};
use crate::private_actix::{RegisterPeerError, SendMessage};
use crate::routing::edge::verify_nonce;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::stats::metrics;
use crate::tcp;
use crate::types::{
    BlockInfo, Disconnect, Handshake, HandshakeFailureReason, PeerMessage, PeerType, ReasonForBan,
};
use actix::fut::future::wrap_future;
use actix::{Actor as _, ActorContext as _, ActorFutureExt as _, AsyncContext as _};
use lru::LruCache;
use near_crypto::Signature;
use near_o11y::{handler_debug_span, log_assert, pretty, OpenTelemetrySpanExt, WithSpanContext};
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time;
use near_primitives::types::EpochId;
use near_primitives::utils::DisplayOption;
use near_primitives::version::{
    ProtocolVersion, PEER_MIN_ALLOWED_PROTOCOL_VERSION, PROTOCOL_VERSION,
};
use parking_lot::Mutex;
use std::fmt::Debug;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::Instrument as _;

/// How often to request peers from active peers.
const REQUEST_PEERS_INTERVAL: time::Duration = time::Duration::seconds(60);

/// Maximal allowed UTC clock skew between this node and the peer.
const MAX_CLOCK_SKEW: time::Duration = time::Duration::minutes(30);

/// Maximum number of transaction messages we will accept between block messages.
/// The purpose of this constant is to ensure we do not spend too much time deserializing and
/// dispatching transactions when we should be focusing on consensus-related messages.
const MAX_TRANSACTIONS_PER_BLOCK_MESSAGE: usize = 1000;
/// Limit cache size of 1000 messages
const ROUTED_MESSAGE_CACHE_SIZE: usize = 1000;
/// Duplicated messages will be dropped if routed through the same peer multiple times.
pub(crate) const DROP_DUPLICATED_MESSAGES_PERIOD: time::Duration = time::Duration::milliseconds(50);
/// How often to send the latest block to peers.
const SYNC_LATEST_BLOCK_INTERVAL: time::Duration = time::Duration::seconds(60);
/// How often to perform a full sync of AccountsData with the peer.
const ACCOUNTS_DATA_FULL_SYNC_INTERVAL: time::Duration = time::Duration::minutes(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionClosedEvent {
    pub(crate) stream_id: tcp::StreamId,
    pub(crate) reason: ClosingReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeStartedEvent {
    pub(crate) stream_id: tcp::StreamId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeCompletedEvent {
    pub(crate) stream_id: tcp::StreamId,
    pub(crate) edge: Edge,
    pub(crate) tier: tcp::Tier,
}

pub(crate) HandshakeError {
    #[error("too many inbound connections in connecting state")]
    TooManyInbound,
    #[error("outbound not allowed: {0}")]
    OutboundNotAllowed(#[from] connection::PoolError),
    
    #[error(transparent)]
    Canceled(#[from] ctx::ErrCanceled),
    #[error(transparent)]
    StreamError(#[from] stream::Error),
    
    #[error("rejected by PeerManager: {0:?}")]
    RejectedByPeerManager(RegisterPeerError),
    #[error("Peer clock skew exceeded {MAX_CLOCK_SKEW}")]
    TooLargeClockSkew,
    #[error("owned_account.peer_id doesn't match handshake.sender_peer_id")]
    OwnedAccountMismatch,
}

#[derive(thiserror::Error, Clone, PartialEq, Eq, Debug)]
pub(crate) enum ClosingReason {
    #[error("peer banned: {0:?}")]
    Ban(ReasonForBan),
    /// Read through `tcp::Tier::is_allowed()` to see which message types
    /// are allowed for a connection of each tier.
    #[error("Received a message of type not allowed on this connection.")]
    DisallowedMessage,
    #[error("PeerManager requested to close the connection")]
    PeerManagerRequest,
    #[error("Received DisconnectMessage from peer")]
    DisconnectMessage,
    #[error("PeerActor stopped NOT via PeerActor::stop()")]
    Unknown,
}

impl ClosingReason {
    /// Used upon closing an outbound connection to decide whether to remove it from the ConnectionStore.
    /// If the inbound side is the one closing the connection, it evaluates this function on the closing
    /// reason and includes the result in the Disconnect message.
    pub(crate) fn remove_from_connection_store(&self) -> bool {
        match self {
            ClosingReason::TooManyInbound => false, // outbound may be still be OK
            ClosingReason::OutboundNotAllowed(_) => true, // outbound not allowed
            ClosingReason::Ban(_) => true,          // banned
            ClosingReason::HandshakeFailed => false, // handshake may simply time out
            ClosingReason::RejectedByPeerManager(_) => true, // rejected by peer manager
            ClosingReason::StreamError => false,    // connection issue
            ClosingReason::DisallowedMessage => true, // misbehaving peer
            ClosingReason::PeerManagerRequest => true, // closed intentionally
            ClosingReason::DisconnectMessage => false, // graceful disconnect
            ClosingReason::TooLargeClockSkew => true, // reconnect will fail for the same reason
            ClosingReason::OwnedAccountMismatch => true, // misbehaving peer
            ClosingReason::Unknown => false,        // only happens in tests
        }
    }
}

impl Connection {
    async fn send_message(&self, msg: &PeerMessage) -> anyhow::Result<()> {
        let msg_type: &str = msg.msg_variant();
        let _span = tracing::trace_span!(target: "network", "send_message", msg_type).entered();
        match msg {
            // Temporarily disable this check because now the node needs to send block to its
            // peers to update its height at the peer. In the future we will introduce a new
            // peer message type for that and then we can enable this check again.
            //PeerMessage::Block(b) if self.tracker.lock().has_received(b.hash()) => return,
            PeerMessage::BlockRequest(h) => self.tracker.lock().push_request(*h),
            PeerMessage::SyncAccountsData(d) => metrics::SYNC_ACCOUNTS_DATA
                .with_label_values(&[
                    "sent",
                    metrics::bool_to_str(d.incremental),
                    metrics::bool_to_str(d.requesting_full_sync),
                ])
                .inc(),
            _ => (),
        };
        let bytes = msg.serialize(self.encoding);
        let bytes_len = bytes.len();
        tracing::trace!(target: "network", msg_len = bytes_len);
        self.stream.send(stream::Frame(bytes))?;
        metrics::PEER_MESSAGE_SENT_BY_TYPE_TOTAL.with_label_values(&[msg_type]).inc();
        metrics::PEER_MESSAGE_SENT_BY_TYPE_BYTES
            .with_label_values(&[msg_type])
            .inc_by(bytes_len as u64);
        Ok(())
    }

    // Send full RoutingTable.
    fn sync_routing_table(&self) {
        let mut known_edges: Vec<Edge> =
            self.network_state.graph.load().edges.values().cloned().collect();
        if self.network_state.config.skip_tombstones.is_some() {
            known_edges.retain(|edge| edge.removal_info().is_none());
            metrics::EDGE_TOMBSTONE_SENDING_SKIPPED.inc();
        }
        let known_accounts = self.network_state.graph.routing_table.get_announce_accounts();
        self.send_message_or_log(&PeerMessage::SyncRoutingTable(RoutingTableUpdate::new(
            known_edges,
            known_accounts,
        )));
    }

    async fn receive_routed_message(
        network_state: &NetworkState,
        peer_id: PeerId,
        msg_hash: CryptoHash,
        body: RoutedMessageBody,
    ) -> Result<Option<RoutedMessageBody>, ReasonForBan> {
        let _span = tracing::trace_span!(target: "network", "receive_routed_message").entered();
        Ok(match body {
            RoutedMessageBody::TxStatusRequest(account_id, tx_hash) => network_state
                .client
                .tx_status_request(account_id, tx_hash)
                .await
                .map(|v| RoutedMessageBody::TxStatusResponse(*v)),
            RoutedMessageBody::TxStatusResponse(tx_result) => {
                network_state.client.tx_status_response(tx_result).await;
                None
            }
            RoutedMessageBody::StateRequestHeader(shard_id, sync_hash) => network_state
                .client
                .state_request_header(shard_id, sync_hash)
                .await?
                .map(RoutedMessageBody::VersionedStateResponse),
            RoutedMessageBody::StateRequestPart(shard_id, sync_hash, part_id) => network_state
                .client
                .state_request_part(shard_id, sync_hash, part_id)
                .await?
                .map(RoutedMessageBody::VersionedStateResponse),
            RoutedMessageBody::VersionedStateResponse(info) => {
                network_state.client.state_response(info).await;
                None
            }
            RoutedMessageBody::StateResponse(info) => {
                network_state.client.state_response(StateResponseInfo::V1(info)).await;
                None
            }
            RoutedMessageBody::BlockApproval(approval) => {
                network_state.client.block_approval(approval, peer_id).await;
                None
            }
            RoutedMessageBody::ForwardTx(transaction) => {
                network_state.client.transaction(transaction, /*is_forwarded=*/ true).await;
                None
            }
            RoutedMessageBody::PartialEncodedChunkRequest(request) => {
                network_state.shards_manager_adapter.send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                        partial_encoded_chunk_request: request,
                        route_back: msg_hash,
                    },
                );
                None
            }
            RoutedMessageBody::PartialEncodedChunkResponse(response) => {
                network_state.shards_manager_adapter.send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                        partial_encoded_chunk_response: response,
                        received_time: ctx::time::now(),
                    },
                );
                None
            }
            RoutedMessageBody::VersionedPartialEncodedChunk(chunk) => {
                network_state
                    .shards_manager_adapter
                    .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(chunk));
                None
            }
            RoutedMessageBody::PartialEncodedChunkForward(msg) => {
                network_state
                    .shards_manager_adapter
                    .send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(msg));
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

    fn receive_message(
        &self,
        ctx: &mut actix::Context<Self>,
        conn: &connection::Connection,
        msg: PeerMessage,
    ) {
        let _span = tracing::trace_span!(target: "network", "receive_message").entered();
        // This is a fancy way to clone the message iff event_sink is non-null.
        // If you have a better idea on how to achieve that, feel free to improve this.
        let message_processed_event = self
            .network_state
            .config
            .event_sink
            .delayed_push(|| Event::MessageProcessed(conn.tier, msg.clone()));
        let was_requested = match &msg {
            PeerMessage::Block(block) => {
                self.network_state.txns_since_last_block.store(0, Ordering::Release);
                let hash = *block.hash();
                let height = block.header().height();
                conn.last_block.rcu(|last_block| {
                    if last_block.is_none() || last_block.unwrap().height <= height {
                        Arc::new(Some(BlockInfo { height, hash }))
                    } else {
                        last_block.clone()
                    }
                });
                let mut tracker = self.tracker.lock();
                tracker.push_received(hash);
                tracker.has_request(&hash)
            }
            _ => false,
        };
        let res = match msg {
            PeerMessage::Routed(msg) => {
                let msg_hash = msg.hash();
                Self::receive_routed_message(&network_state, peer_id, msg_hash, msg.msg.body).await?.map(
                    |body| {
                        PeerMessage::Routed(network_state.sign_message(
                            RawRoutedMessage { target: PeerIdOrHash::Hash(msg_hash), body },
                        ))
                    },
                )
            }
            PeerMessage::BlockRequest(hash) => {
                network_state.client.block_request(hash).await.map(|b|PeerMessage::Block(*b))
            }
            PeerMessage::BlockHeadersRequest(hashes) => {
                network_state.client.block_headers_request(hashes).await.map(PeerMessage::BlockHeaders)
            }
            PeerMessage::Block(block) => {
                network_state.client.block(block, peer_id, was_requested).await;
                None
            }
            PeerMessage::Transaction(transaction) => {
                network_state.client.transaction(transaction, /*is_forwarded=*/ false).await;
                None
            }
            PeerMessage::BlockHeaders(headers) => {
                network_state.client.block_headers(headers, peer_id).await?;
                None
            }
            PeerMessage::Challenge(challenge) => {
                network_state.client.challenge(challenge).await;
                None
            }
            msg => {
                tracing::error!(target: "network", "Peer received unexpected type: {:?}", msg);
                None
            }
        };
        match res {
            // TODO(gprusak): make sure that for routed messages we drop routeback info correctly.
            Ok(Some(resp)) => self.send_message_or_log(&resp);
            Ok(None) => {}
            Err(ban_reason) => return ClosingReason::Ban(ban_reason);
        }
    }

    fn add_route_back(&self, conn: &connection::Connection, msg: &RoutedMessageV2) {
        if !msg.expect_response() {
            return;
        }
        tracing::trace!(target: "network", route_back = ?msg.clone(), "Received peer message that requires response");
        let from = &conn.peer_info.id;
        match conn.tier {
            tcp::Tier::T1 => self.network_state.tier1_route_back.lock().insert(
                msg.hash(),
                from.clone(),
            ),
            tcp::Tier::T2 => self.network_state.graph.routing_table.add_route_back(
                msg.hash(),
                from.clone(),
            ),
        }
    }

    fn handle_msg_ready(
        &mut self,
        ctx: &mut actix::Context<Self>,
        conn: Arc<connection::Connection>,
        peer_msg: PeerMessage,
    ) {
        let _span = tracing::trace_span!(
            target: "network",
            "handle_msg_ready")
        .entered();

        match peer_msg.clone() {
            PeerMessage::Disconnect(d) => {
                tracing::debug!(target: "network", "Disconnect signal. Me: {:?} Peer: {:?}", self.my_node_info.id, self.other_peer_id());

                if d.remove_from_connection_store {
                    self.network_state
                        .connection_store
                        .remove_from_connection_store(self.other_peer_id().unwrap())
                }

                self.stop(ctx, ClosingReason::DisconnectMessage);
            }
            PeerMessage::Tier1Handshake(_) | PeerMessage::Tier2Handshake(_) => {
                // Received handshake after already have seen handshake from this peer.
                tracing::debug!(target: "network", "Duplicate handshake from {}", self.peer_info);
            }
            PeerMessage::PeersRequest => {
                let peers = self
                    .network_state
                    .peer_store
                    .healthy_peers(self.network_state.config.max_send_peers as usize);
                let direct_peers = self.network_state.get_direct_peers();
                if !peers.is_empty() || !direct_peers.is_empty() {
                    tracing::debug!(target: "network", "Peers request from {}: sending {} peers and {} direct peers.", self.peer_info, peers.len(), direct_peers.len());
                    self.send_message_or_log(&PeerMessage::PeersResponse(PeersResponse {
                        peers,
                        direct_peers,
                    }));
                }
                self.network_state
                    .config
                    .event_sink
                    .push(Event::MessageProcessed(conn.tier, peer_msg));
            }
            PeerMessage::PeersResponse(PeersResponse { peers, direct_peers }) => {
                tracing::debug!(target: "network", "Received peers from {}: {} peers and {} direct peers.", self.peer_info, peers.len(), direct_peers.len());
                let node_id = self.network_state.config.node_id();
                self.network_state.peer_store.add_indirect_peers(
                    &self.clock,
                    peers.into_iter().filter(|peer_info| peer_info.id != node_id),
                );
                // Direct peers of the responding peer are still indirect peers for this node.
                // However, we may treat them with more trust in the future.
                self.network_state.peer_store.add_indirect_peers(
                    &self.clock,
                    direct_peers.into_iter().filter(|peer_info| peer_info.id != node_id),
                );
                self.network_state
                    .config
                    .event_sink
                    .push(Event::MessageProcessed(conn.tier, peer_msg));
            }
            PeerMessage::RequestUpdateNonce(edge_info) => {
                s.spawn(async {
                    let peer_id = &conn.peer_info.id;
                    match network_state.graph.load().local_edges.get(peer_id) {
                        Some(cur_edge)
                            if cur_edge.edge_type() == EdgeState::Active
                                && cur_edge.nonce() >= edge_info.nonce =>
                        {
                            // Found a newer local edge, so just send it to the peer.
                            conn.send_message(Arc::new(PeerMessage::SyncRoutingTable(
                                RoutingTableUpdate::from_edges(vec![cur_edge.clone()]),
                            )));
                        }
                        // Sign the edge and broadcast it to everyone (finalize_edge does both).
                        _ => {
                            if let Err(ban_reason) = network_state
                                .finalize_edge(&clock, peer_id.clone(), edge_info)
                                .await
                            {
                                conn.stop(Some(ban_reason));
                            }
                        }
                    };
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                });
            }
            PeerMessage::SyncRoutingTable(rtu) => {
                s.spawn(async {
                    Self::handle_sync_routing_table(&network_state, conn.clone(), rtu)
                        .await;
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                }));
            }
            PeerMessage::SyncAccountsData(msg) => {
                metrics::SYNC_ACCOUNTS_DATA
                    .with_label_values(&[
                        "received",
                        metrics::bool_to_str(msg.incremental),
                        metrics::bool_to_str(msg.requesting_full_sync),
                    ])
                    .inc();
                let network_state = self.network_state.clone();
                // In case a full sync is requested, immediately send what we got.
                // It is a microoptimization: we do not send back the data we just received.
                if msg.requesting_full_sync {
                    self.send_message_or_log(&PeerMessage::SyncAccountsData(SyncAccountsData {
                        requesting_full_sync: false,
                        incremental: false,
                        accounts_data: network_state
                            .accounts_data
                            .load()
                            .data
                            .values()
                            .cloned()
                            .collect(),
                    }));
                }
                // Early exit, if there is no data in the message.
                if msg.accounts_data.is_empty() {
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                    return;
                }
                let network_state = self.network_state.clone();
                s.spawn(async {
                    if let Some(err) =
                        network_state.add_accounts_data(msg.accounts_data).await
                    {
                        conn.stop(Some(match err {
                            accounts_data::Error::InvalidSignature => {
                                ReasonForBan::InvalidSignature
                            }
                            accounts_data::Error::DataTooLarge => ReasonForBan::Abusive,
                            accounts_data::Error::SingleAccountMultipleData => {
                                ReasonForBan::Abusive
                            }
                        }));
                    }
                    network_state
                        .config
                        .event_sink
                        .push(Event::MessageProcessed(conn.tier, peer_msg));
                }));
            }
            PeerMessage::Routed(mut msg) => {
                tracing::trace!(
                    target: "network",
                    "Received routed message from {} to {:?}.",
                    self.peer_info,
                    msg.target);
                let for_me = network_state.message_for_me(&msg.target);
                if for_me {
                    // Check if we have already received this message.
                    let fastest = network_state
                        .recent_routed_messages
                        .lock()
                        .put(CryptoHash::hash_borsh(&msg.body), ())
                        .is_none();
                    // Register that the message has been received.
                    metrics::record_routed_msg_metrics(&msg, conn.tier, fastest);
                }

                // Drop duplicated messages routed within DROP_DUPLICATED_MESSAGES_PERIOD ms
                let key = (msg.author.clone(), msg.target.clone(), msg.signature.clone());
                let now = ctx::time::now();
                if let Some(&t) = self.routed_message_cache.get(&key) {
                    if now <= t + DROP_DUPLICATED_MESSAGES_PERIOD {
                        metrics::MessageDropped::Duplicate.inc(&msg.body);
                        self.network_state.config.event_sink.push(Event::RoutedMessageDropped);
                        tracing::debug!(target: "network", "Dropping duplicated message from {} to {:?}", msg.author, msg.target);
                        return;
                    }
                }
                if let RoutedMessageBody::ForwardTx(_) = &msg.body {
                    // Check whenever we exceeded number of transactions we got since last block.
                    // If so, drop the transaction.
                    let r = self.network_state.txns_since_last_block.load(Ordering::Acquire);
                    // TODO(gprusak): this constraint doesn't take into consideration such
                    // parameters as number of nodes or number of shards. Reconsider why do we need
                    // this and whether this is really the right way of handling it.
                    if r > MAX_TRANSACTIONS_PER_BLOCK_MESSAGE {
                        metrics::MessageDropped::TransactionsPerBlockExceeded.inc(&msg.body);
                        return;
                    }
                    self.network_state.txns_since_last_block.fetch_add(1, Ordering::AcqRel);
                }
                self.routed_message_cache.put(key, now);

                if !msg.verify() {
                    // Received invalid routed message from peer.
                    return ClosingReason::Ban(ReasonForBan::InvalidSignature);
                }

                self.add_route_back(&conn, msg.as_ref());
                if for_me {
                    // Handle Ping and Pong message if they are for us without sending to client.
                    // i.e. Return false in case of Ping and Pong
                    match &msg.body {
                        RoutedMessageBody::Ping(ping) => {
                            self.network_state.send_pong(
                                conn.tier,
                                ping.nonce,
                                msg.hash(),
                            );
                            // TODO(gprusak): deprecate Event::Ping/Pong in favor of
                            // MessageProcessed.
                            self.network_state.config.event_sink.push(Event::Ping(ping.clone()));
                            self.network_state
                                .config
                                .event_sink
                                .push(Event::MessageProcessed(conn.tier, PeerMessage::Routed(msg)));
                        }
                        RoutedMessageBody::Pong(pong) => {
                            self.network_state.config.event_sink.push(Event::Pong(pong.clone()));
                            self.network_state
                                .config
                                .event_sink
                                .push(Event::MessageProcessed(conn.tier, PeerMessage::Routed(msg)));
                        }
                        _ => self.receive_message(ctx, &conn, PeerMessage::Routed(msg)),
                    }
                } else {
                    if msg.decrease_ttl() {
                        self.network_state.send_message_to_peer(conn.tier, msg);
                    } else {
                        self.network_state.config.event_sink.push(Event::RoutedMessageDropped);
                        tracing::warn!(target: "network", ?msg, from = ?conn.peer_info.id, "Message dropped because TTL reached 0.");
                        metrics::ROUTED_MESSAGE_DROPPED
                            .with_label_values(&[msg.body_variant()])
                            .inc();
                    }
                }
            }
            msg => self.receive_message(ctx, &conn, msg),
        }
    }

    async fn handle_sync_routing_table(
        network_state: &Arc<NetworkState>,
        conn: Arc<connection::Connection>,
        rtu: RoutingTableUpdate,
    ) {
        let _span = tracing::trace_span!(target: "network", "handle_sync_routing_table").entered();
        if let Err(ban_reason) = network_state.add_edges(rtu.edges).await {
            return Some(ban_reason);
        }
        // For every announce we received, we fetch the last announce with the same account_id
        // that we already broadcasted. Client actor will both verify signatures of the received announces
        // as well as filter out those which are older than the fetched ones (to avoid overriding
        // a newer announce with an older one).
        let old = network_state
            .graph
            .routing_table
            .get_broadcasted_announces(rtu.accounts.iter().map(|a| &a.account_id));
        let accounts: Vec<(AnnounceAccount, Option<EpochId>)> = rtu
            .accounts
            .into_iter()
            .map(|aa| {
                let id = aa.account_id.clone();
                (aa, old.get(&id).map(|old| old.epoch_id.clone()))
            })
            .collect();
        match network_state.client.announce_account(accounts).await {
            Err(ban_reason) => return Some(ban_reason),
            Ok(accounts) => network_state.add_accounts(accounts).await,
        }
    }
}

impl actix::Actor for PeerActor {
    type Context = actix::Context<PeerActor>;

    fn started(&mut self, ctx: &mut Self::Context) {
        tracing::debug!(target: "network", "{:?}: Peer {:?} {:?} started", self.my_node_info.id, self.peer_addr, self.peer_type);
        // Set Handshake timeout for stopping actor if peer is not ready after given period of time.

        near_performance_metrics::actix::run_later(
            ctx,
            self.network_state.config.handshake_timeout.try_into().unwrap(),
            move |act, ctx| match act.peer_status {
                PeerStatus::Connecting { .. } => {
                    tracing::info!(target: "network", "Handshake timeout expired for {}", act.peer_info);
                    act.stop(ctx, ClosingReason::HandshakeFailed);
                }
                _ => {}
            },
        );

        // If outbound peer, initiate handshake.
        if let PeerStatus::Connecting(_, ConnectingStatus::Outbound { handshake_spec, .. }) =
            &self.peer_status
        {
            self.send_handshake(handshake_spec.clone());
        }
        self.network_state
            .config
            .event_sink
            .push(Event::HandshakeStarted(HandshakeStartedEvent { stream_id: self.stream_id }));
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // closing_reason may be None in case the whole actix system is stopped.
        // It happens a lot in tests.
        metrics::PEER_CONNECTIONS_TOTAL.dec();
        match &self.closing_reason {
            None => {
                // Due to Actix semantics, sometimes closing reason may be not set.
                // But it is only expected to happen in tests.
                tracing::error!(target:"network", "closing reason not set. This should happen only in tests.");
            }
            Some(reason) => {
                tracing::info!(target: "network", "{:?}: Peer {} disconnected, reason: {reason}", self.my_node_info.id, self.peer_info);

                // If we are on the inbound side of the connection, set a flag in the disconnect
                // message advising the outbound side whether to attempt to re-establish the connection.
                let remove_from_connection_store =
                    self.peer_type == PeerType::Inbound && reason.remove_from_connection_store();

                self.send_message_or_log(&PeerMessage::Disconnect(Disconnect {
                    remove_from_connection_store,
                }));
            }
        }

        network_state.unregister(
            &conn,
            self.stream_id,
            self.closing_reason.clone().unwrap_or(ClosingReason::Unknown),
        );
    }
}

impl PeerActor {
    fn handle_stream_err(&mut self, err: stream::Error) {
        let expected = match &err {
            stream::Error::Recv(stream::RecvError::MessageTooLarge { .. }) => {
                self.stop(ctx, ClosingReason::Ban(ReasonForBan::Abusive));
                true
            }
            // It is expected in a sense that the peer might be just slow.
            stream::Error::Send(stream::SendError::QueueOverflow { .. }) => true,
            stream::Error::Recv(stream::RecvError::IO(err))
            | stream::Error::Send(stream::SendError::IO(err)) => match err.kind() {
                // Connection has been closed.
                io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::BrokenPipe
                // libc::ETIIMEDOUT = 110, translates to io::ErrorKind::TimedOut.
                | io::ErrorKind::TimedOut => true,
                // When stopping tokio runtime, an "IO driver has terminated" is sometimes
                // returned.
                io::ErrorKind::Other => true,
                // It is unexpected in a sense that stream got broken in an unexpected way.
                // In case you encounter an error that was actually to be expected,
                // please add it here and document.
                _ => false,
            },
        };
        log_assert!(expected, "unexpected closing reason: {err}");
        tracing::info!(target: "network", ?err, "Closing connection to {}", self.peer_info);
        return ClosingReason::StreamError;
    }
}

impl PeerActor {
    #[perf]
    async fn handle(&mut self, stream::Frame(msg): stream::Frame) {
        let _span = tracing::debug_span!(
            target: "network",
            "handle",
            handler = "bytes",
            actor = "PeerActor",
            msg_len = msg.len(),
            peer = %self.peer_info)
        .entered();

        let mut peer_msg = match self.parse_message(&msg) {
            Ok(msg) => msg,
            Err(err) => {
                tracing::debug!(target: "network", "Received invalid data {} from {}: {}", pretty::AbbrBytes(&msg), self.peer_info, err);
                return;
            }
        };

        tracing::trace!(target: "network", "Received message: {}", peer_msg);

        {
            let labels = [peer_msg.msg_variant()];
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_TOTAL.with_label_values(&labels).inc();
            metrics::PEER_MESSAGE_RECEIVED_BY_TYPE_BYTES
                .with_label_values(&labels)
                .inc_by(msg.len() as u64);
        }
        if self.closing_reason.is_some() {
            tracing::warn!(target: "network", "Received {} from closing connection {:?}. Ignoring", peer_msg, self.peer_type);
            return;
        }
        conn.last_time_received_message.store(self.clock.now());
        // Check if the message type is allowed given the TIER of the connection:
        // TIER1 connections are reserved exclusively for BFT consensus messages.
        if !conn.tier.is_allowed(&peer_msg) {
            tracing::warn!(target: "network", "Received {} on {:?} connection, disconnecting",peer_msg.msg_variant(),conn.tier);
            // TODO(gprusak): this is abusive behavior. Consider banning for it.
            self.stop(ctx, ClosingReason::DisallowedMessage);
            return;
        }

        // Optionally, ignore any received tombstones after startup. This is to
        // prevent overload from too much accumulated deleted edges.
        //
        // We have similar code to skip sending tombstones, here we handle the
        // case when our peer doesn't use that logic yet.
        if let Some(skip_tombstones) = self.network_state.config.skip_tombstones {
            if let PeerMessage::SyncRoutingTable(routing_table) = &mut peer_msg {
                if conn.established_time + skip_tombstones > self.clock.now() {
                    routing_table
                        .edges
                        .retain(|edge| edge.edge_type() == EdgeState::Active);
                    metrics::EDGE_TOMBSTONE_RECEIVING_SKIPPED.inc();
                }
            }
        }
        // Handle the message.
        self.handle_msg_ready(ctx, conn.clone(), peer_msg);
    }
}
