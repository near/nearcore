use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use actix::actors::resolver::{ConnectAddr, Resolver};
use actix::io::FramedWrite;
use actix::{
    Actor, ActorContext, ActorFuture, Addr, AsyncContext, Context, ContextFutureSpawner, Handler,
    Recipient, Running, StreamHandler, SystemService, WrapFuture,
};
use chrono::offset::TimeZone;
use chrono::{DateTime, Utc};
use futures::{future, Stream, StreamExt};
use rand::{thread_rng, Rng};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, info, trace, warn};

use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::utils::from_timestamp;
use near_store::Store;

use crate::codec::Codec;
use crate::metrics;
use crate::peer::Peer;
use crate::peer_store::PeerStore;
use crate::routing::{Edge, EdgeInfo, EdgeType, ProcessEdgeResult, RoutingTable};
use crate::types::{
    AccountOrPeerIdOrHash, AnnounceAccount, Ban, BlockedPorts, Consolidate, ConsolidateResponse,
    FullPeerInfo, InboundTcpConnect, KnownPeerStatus, NetworkInfo, NetworkViewClientMessages,
    NetworkViewClientResponses, OutboundTcpConnect, PeerId, PeerIdOrHash, PeerList,
    PeerManagerRequest, PeerMessage, PeerRequest, PeerResponse, PeerType, PeersRequest,
    PeersResponse, Ping, Pong, QueryPeerStats, RawRoutedMessage, ReasonForBan, RoutedMessage,
    RoutedMessageBody, RoutedMessageFrom, SendMessage, StopSignal, SyncData, Unregister,
};
use crate::types::{
    NetworkClientMessages, NetworkConfig, NetworkRequests, NetworkResponses, PeerInfo,
};
use futures::task::Poll;
use std::net::SocketAddr;
use std::pin::Pin;

/// How often to request peers from active peers.
const REQUEST_PEERS_SECS: i64 = 60;
/// How much time to wait (in milliseconds) after we send update nonce request before disconnecting.
/// This number should be large to handle pair of nodes with high latency.
const WAIT_ON_TRY_UPDATE_NONCE: u64 = 6_000;
/// If we see an edge between us and other peer, but this peer is not a current connection, wait this
/// timeout and in case it didn't become an active peer, broadcast edge removal update.
const WAIT_PEER_BEFORE_REMOVE: u64 = 6_000;

macro_rules! unwrap_or_error(($obj: expr, $error: expr) => (match $obj {
    Ok(result) => result,
    Err(err) => {
        error!(target: "network", "{}: {}", $error, err);
        return;
    }
}));

/// Contains information relevant to an active peer.
struct ActivePeer {
    addr: Addr<Peer>,
    full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    sent_bytes_per_sec: u64,
    /// Last time requested peers.
    last_time_peer_requested: DateTime<Utc>,
}

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    /// Networking configuration.
    config: NetworkConfig,
    /// Peer information for this node.
    peer_id: PeerId,
    /// Address of the client actor.
    client_addr: Recipient<NetworkClientMessages>,
    /// Address of the view client actor.
    view_client_addr: Recipient<NetworkViewClientMessages>,
    /// Peer store that provides read/write access to peers.
    peer_store: PeerStore,
    /// Set of outbound connections that were not consolidated yet.
    outgoing_peers: HashSet<PeerId>,
    /// Active peers (inbound and outbound) with their full peer information.
    active_peers: HashMap<PeerId, ActivePeer>,
    /// Routing table to keep track of account id
    routing_table: RoutingTable,
    /// Monitor peers attempts, used for fast checking in the beginning with exponential backoff.
    monitor_peers_attempts: u64,
    /// Active peers we have sent new edge update, but we haven't received response so far.
    pending_update_nonce_request: HashMap<PeerId, u64>,
}

impl PeerManagerActor {
    pub fn new(
        store: Arc<Store>,
        config: NetworkConfig,
        client_addr: Recipient<NetworkClientMessages>,
        view_client_addr: Recipient<NetworkViewClientMessages>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let peer_store = PeerStore::new(store.clone(), &config.boot_nodes)?;
        debug!(target: "network", "Found known peers: {} (boot nodes={})", peer_store.len(), config.boot_nodes.len());
        debug!(target: "network", "Blacklist: {:?}", config.blacklist);

        let me = config.public_key.clone().into();
        Ok(PeerManagerActor {
            peer_id: config.public_key.clone().into(),
            config,
            client_addr,
            view_client_addr,
            peer_store,
            active_peers: HashMap::default(),
            outgoing_peers: HashSet::default(),
            routing_table: RoutingTable::new(me, store),
            monitor_peers_attempts: 0,
            pending_update_nonce_request: HashMap::new(),
        })
    }

    fn num_active_peers(&self) -> usize {
        self.active_peers.len()
    }

    fn is_blacklisted(&self, addr: &SocketAddr) -> bool {
        if let Some(blocked_ports) = self.config.blacklist.get(&addr.ip()) {
            match blocked_ports {
                BlockedPorts::All => true,
                BlockedPorts::Some(ports) => ports.contains(&addr.port()),
            }
        } else {
            false
        }
    }

    /// Register a direct connection to a new peer. This will be called after successfully
    /// establishing a connection with another peer. It become part of the active peers.
    ///
    /// To build new edge between this pair of nodes both signatures are required.
    /// Signature from this node is passed in `edge_info`
    /// Signature from the other node is passed in `full_peer_info.edge_info`.
    fn register_peer(
        &mut self,
        full_peer_info: FullPeerInfo,
        edge_info: EdgeInfo,
        peer_type: PeerType,
        addr: Addr<Peer>,
        ctx: &mut Context<Self>,
    ) {
        debug!(target: "network", "Consolidated connection with {:?}", full_peer_info);

        if self.outgoing_peers.contains(&full_peer_info.peer_info.id) {
            self.outgoing_peers.remove(&full_peer_info.peer_info.id);
        }
        unwrap_or_error!(
            self.peer_store.peer_connected(&full_peer_info),
            "Failed to save peer data"
        );

        let target_peer_id = full_peer_info.peer_info.id.clone();

        let new_edge = Edge::new(
            self.peer_id.clone(),   // source
            target_peer_id.clone(), // target
            edge_info.nonce,
            edge_info.signature,
            full_peer_info.edge_info.signature.clone(),
        );

        self.active_peers.insert(
            target_peer_id.clone(),
            ActivePeer {
                addr: addr.clone(),
                full_peer_info,
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                last_time_peer_requested: Utc.timestamp(0, 0),
            },
        );

        self.process_edge(ctx, new_edge.clone());

        // TODO(MarX, #1363): Implement sync service. Right now all edges and known validators
        //  are sent during handshake.
        let known_edges = self.routing_table.get_edges();
        let known_accounts = self.routing_table.get_announce_accounts();
        let wait_for_sync = 1;

        // Start syncing network point of view. Wait until both parties are connected before start
        // sending messages.
        ctx.run_later(Duration::from_secs(wait_for_sync), move |act, ctx| {
            let _ = addr.do_send(SendMessage {
                message: PeerMessage::Sync(SyncData {
                    edges: known_edges,
                    accounts: known_accounts,
                }),
            });

            // Ask for peers list on connection.
            let _ = addr.do_send(SendMessage { message: PeerMessage::PeersRequest });
            if let Some(active_peer) = act.active_peers.get_mut(&target_peer_id) {
                active_peer.last_time_peer_requested = Utc::now();
            }

            if peer_type == PeerType::Outbound {
                // Only broadcast new message from the outbound endpoint.
                // Wait a time out before broadcasting this new edge to let the other party finish handshake.
                act.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::Sync(SyncData::edge(new_edge)) },
                );
            }
        });
    }

    fn remove_active_peer(&mut self, ctx: &mut Context<Self>, peer_id: &PeerId) {
        // If the last edge we have with this peer represent a connection addition, create the edge
        // update the represents the connection removal.
        self.active_peers.remove(&peer_id);

        if let Some(edge) = self.routing_table.get_edge(self.peer_id.clone(), peer_id.clone()) {
            if edge.edge_type() == EdgeType::Added {
                let edge_update = edge.remove_edge(self.peer_id.clone(), &self.config.secret_key);
                self.process_edge(ctx, edge_update.clone());
                self.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::Sync(SyncData::edge(edge_update)) },
                );
            }
        }
    }

    /// Remove a peer from the active peer set. If the peer doesn't belong to the active peer set
    /// data from ongoing connection established is removed.
    fn unregister_peer(&mut self, ctx: &mut Context<Self>, peer_id: PeerId) {
        // If this is an unconsolidated peer because failed / connected inbound, just delete it.
        if self.outgoing_peers.contains(&peer_id) {
            self.outgoing_peers.remove(&peer_id);
            return;
        }
        self.remove_active_peer(ctx, &peer_id);
        unwrap_or_error!(self.peer_store.peer_disconnected(&peer_id), "Failed to save peer data");
    }

    /// Add peer to ban list.
    fn ban_peer(&mut self, ctx: &mut Context<Self>, peer_id: &PeerId, ban_reason: ReasonForBan) {
        info!(target: "network", "Banning peer {:?} for {:?}", peer_id, ban_reason);
        self.remove_active_peer(ctx, peer_id);
        unwrap_or_error!(self.peer_store.peer_ban(peer_id, ban_reason), "Failed to save peer data");
    }

    /// Connects peer with given TcpStream and optional information if it's outbound.
    fn connect_peer(
        &mut self,
        recipient: Addr<Self>,
        stream: TcpStream,
        peer_type: PeerType,
        peer_info: Option<PeerInfo>,
        edge_info: Option<EdgeInfo>,
    ) {
        let peer_id = self.peer_id.clone();
        let account_id = self.config.account_id.clone();
        let server_addr = self.config.addr;
        let handshake_timeout = self.config.handshake_timeout;
        let client_addr = self.client_addr.clone();
        let view_client_addr = self.view_client_addr.clone();
        Peer::create(move |ctx| {
            let server_addr = server_addr.unwrap_or_else(|| stream.local_addr().unwrap());
            let remote_addr = stream.peer_addr().unwrap();
            let (read, write) = tokio::io::split(stream);

            // TODO: check if peer is banned or known based on IP address and port.
            Peer::add_stream(
                FramedRead::new(read, Codec::new())
                    .take_while(|x| match x {
                        Ok(_) => future::ready(true),
                        Err(e) => {
                            warn!(target: "network", "Peer stream error: {:?}", e);
                            future::ready(false)
                        }
                    })
                    .map(Result::unwrap),
                ctx,
            );
            Peer::new(
                PeerInfo { id: peer_id, addr: Some(server_addr), account_id },
                remote_addr,
                peer_info,
                peer_type,
                FramedWrite::new(write, Codec::new(), ctx),
                handshake_timeout,
                recipient,
                client_addr,
                view_client_addr,
                edge_info,
            )
        });
    }

    fn is_outbound_bootstrap_needed(&self) -> bool {
        self.active_peers.len() + self.outgoing_peers.len() < self.config.max_peer as usize
            && !self.config.outbound_disabled
    }

    fn is_inbound_allowed(&self) -> bool {
        self.active_peers.len() + self.outgoing_peers.len() < self.config.max_peer as usize
    }

    /// Returns single random peer with the most weight.
    fn most_weight_peers(&self) -> Vec<FullPeerInfo> {
        // This finds max of weight and height and returns such peer.
        let max_weight_and_score = match self
            .active_peers
            .values()
            .map(|active_peers| {
                (
                    active_peers.full_peer_info.chain_info.weight_and_score,
                    active_peers.full_peer_info.chain_info.height,
                )
            })
            .max()
        {
            Some((weight_and_score, _)) => weight_and_score,
            None => return vec![],
        };
        // Find all peers whose height is within `most_weighted_peer_horizon` from max weight peer(s).
        self.active_peers
            .values()
            .filter_map(|active_peer| {
                if max_weight_and_score.beyond_threshold(
                    &active_peer.full_peer_info.chain_info.weight_and_score,
                    self.config.most_weighted_peer_horizon,
                ) {
                    None
                } else {
                    Some(active_peer.full_peer_info.clone())
                }
            })
            .collect::<Vec<_>>()
    }

    /// Returns bytes sent/received across all peers.
    fn get_total_bytes_per_sec(&self) -> (u64, u64) {
        let sent_bps = self.active_peers.values().map(|x| x.sent_bytes_per_sec).sum();
        let received_bps = self.active_peers.values().map(|x| x.received_bytes_per_sec).sum();
        (sent_bps, received_bps)
    }

    /// Get a random peer we are not connected to from the known list.
    fn sample_random_peer(&self, ignore_list: &HashSet<PeerId>) -> Option<PeerInfo> {
        let unconnected_peers = self.peer_store.unconnected_peers(ignore_list);
        let index = thread_rng().gen_range(0, std::cmp::max(unconnected_peers.len(), 1));

        unconnected_peers
            .iter()
            .enumerate()
            .filter_map(|(i, v)| if i == index { Some(v.clone()) } else { None })
            .next()
    }

    /// Query current peers for more peers.
    fn query_active_peers_for_more_peers(&mut self, ctx: &mut Context<Self>) {
        let mut requests = vec![];
        let msg = SendMessage { message: PeerMessage::PeersRequest };
        for (_, active_peer) in self.active_peers.iter_mut() {
            if Utc::now().signed_duration_since(active_peer.last_time_peer_requested).num_seconds()
                > REQUEST_PEERS_SECS
            {
                active_peer.last_time_peer_requested = Utc::now();
                requests.push(active_peer.addr.send(msg.clone()));
            }
        }
        future::try_join_all(requests)
            .into_actor(self)
            .map(|x, _, _| {
                let _ignore = x.map_err(|e| error!("Failed sending broadcast message: {}", e));
            })
            .spawn(ctx);
    }

    /// Add an edge update to the routing table and return if it is a new edge update.
    fn process_edge(&mut self, ctx: &mut Context<Self>, edge: Edge) -> bool {
        let ProcessEdgeResult { new_edge, schedule_computation } =
            self.routing_table.process_edge(edge);

        if let Some(duration) = schedule_computation {
            ctx.run_later(duration, |act, _ctx| act.routing_table.update());
        }

        new_edge
    }

    fn wait_peer_or_remove(&mut self, ctx: &mut Context<Self>, edge: Edge) {
        // This edge says this is an active peer, which is currently not in the set of active peers.
        // Wait for some time to let the connection begin or broadcast edge removal instead.

        ctx.run_later(Duration::from_millis(WAIT_PEER_BEFORE_REMOVE), move |act, ctx| {
            let other = edge.other(&act.peer_id).unwrap();
            if !act.active_peers.contains_key(&other) {
                // Peer is still not active after waiting a timeout.
                let new_edge = edge.remove_edge(act.peer_id.clone(), &act.config.secret_key);
                act.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::Sync(SyncData::edge(new_edge)) },
                );
            }
        });
    }

    fn try_update_nonce(&mut self, ctx: &mut Context<Self>, edge: Edge, other: PeerId) {
        let nonce = edge.next_nonce();

        if let Some(last_nonce) = self.pending_update_nonce_request.get(&other) {
            if *last_nonce >= nonce {
                // We already tried to update an edge with equal or higher nonce.
                return;
            }
        }

        self.send_message(
            ctx,
            &other.clone(),
            PeerMessage::RequestUpdateNonce(EdgeInfo::new(
                self.peer_id.clone(),
                other.clone(),
                nonce,
                &self.config.secret_key,
            )),
        );

        self.pending_update_nonce_request.insert(other.clone(), nonce);

        ctx.run_later(Duration::from_millis(WAIT_ON_TRY_UPDATE_NONCE), move |act, _ctx| {
            if let Some(cur_nonce) = act.pending_update_nonce_request.get(&other) {
                if *cur_nonce == nonce {
                    if let Some(peer) = act.active_peers.get(&other) {
                        // Send disconnect signal to this peer if we haven't edge update.
                        peer.addr.do_send(PeerManagerRequest::UnregisterPeer);
                    }
                    act.pending_update_nonce_request.remove(&other);
                }
            }
        });
    }

    /// Periodically query peer actors for latest weight and traffic info.
    fn monitor_peer_stats(&mut self, ctx: &mut Context<Self>) {
        for (peer_id, active_peer) in self.active_peers.iter() {
            let peer_id1 = peer_id.clone();
            active_peer
                .addr
                .send(QueryPeerStats {})
                .into_actor(self)
                .map(|result, _, _| result.map_err(|err| error!("Failed sending message: {}", err)))
                .map(move |res, act, _| {
                    let _ignore = res.map(|res| {
                        if res.is_abusive {
                            trace!(target: "network", "Banning peer {} for abuse ({} sent, {} recv)", peer_id1, res.message_counts.0, res.message_counts.1);
                            // TODO(MarX, #1586): Ban peer if we found them abusive. Fix issue with heavy
                            //  network traffic that flags honest peers.
                            // Send ban signal to peer instance. It should send ban signal back and stop the instance.
                            // if let Some(active_peer) = act.active_peers.get(&peer_id1) {
                            //     active_peer.addr.do_send(PeerManagerRequest::BanPeer(ReasonForBan::Abusive));
                            // }
                        } else if let Some(active_peer) = act.active_peers.get_mut(&peer_id1) {
                            active_peer.full_peer_info.chain_info = res.chain_info;
                            active_peer.sent_bytes_per_sec = res.sent_bytes_per_sec;
                            active_peer.received_bytes_per_sec = res.received_bytes_per_sec;
                        }
                    });
                })
                .spawn(ctx);
        }

        ctx.run_later(self.config.peer_stats_period, move |act, ctx| {
            act.monitor_peer_stats(ctx);
        });
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - unban peers that have been banned for awhile,
    ///  - remove expired peers,
    fn monitor_peers(&mut self, ctx: &mut Context<Self>) {
        let mut to_unban = vec![];
        for (peer_id, peer_state) in self.peer_store.iter() {
            if let KnownPeerStatus::Banned(_, last_banned) = peer_state.status {
                let interval = unwrap_or_error!(
                    (Utc::now() - from_timestamp(last_banned)).to_std(),
                    "Failed to convert time"
                );
                if interval > self.config.ban_window {
                    info!(target: "network", "Monitor peers: unbanned {} after {:?}.", peer_id, interval);
                    to_unban.push(peer_id.clone());
                }
            }
        }
        for peer_id in to_unban {
            unwrap_or_error!(self.peer_store.peer_unban(&peer_id), "Failed to unban a peer");
        }

        if self.is_outbound_bootstrap_needed() {
            if let Some(peer_info) = self.sample_random_peer(&self.outgoing_peers) {
                self.outgoing_peers.insert(peer_info.id.clone());
                ctx.notify(OutboundTcpConnect { peer_info });
            } else {
                self.query_active_peers_for_more_peers(ctx);
            }
        }

        unwrap_or_error!(
            self.peer_store.remove_expired(&self.config),
            "Failed to remove expired peers"
        );

        // Reschedule the bootstrap peer task, starting of as quick as possible with exponential backoff.
        let wait = Duration::from_millis(cmp::min(
            self.config.bootstrap_peers_period.as_millis() as u64,
            10 << self.monitor_peers_attempts,
        ));
        self.monitor_peers_attempts = cmp::min(13, self.monitor_peers_attempts + 1);
        ctx.run_later(wait, move |act, ctx| {
            act.monitor_peers(ctx);
        });
    }

    /// Broadcast message to all active peers.
    fn broadcast_message(&self, ctx: &mut Context<Self>, msg: SendMessage) {
        // TODO(MarX, #1363): Implement smart broadcasting. (MST)

        let requests: Vec<_> =
            self.active_peers.values().map(|peer| peer.addr.send(msg.clone())).collect();

        future::try_join_all(requests)
            .into_actor(self)
            .map(|res, _, _| res.map_err(|e| error!("Failed sending broadcast message: {}", e)))
            .map(|_, _, _| ())
            .spawn(ctx);
    }

    fn announce_account(&mut self, ctx: &mut Context<Self>, announce_account: AnnounceAccount) {
        debug!(target: "network", "{:?} Account announce: {:?}", self.config.account_id, announce_account);
        if !self.routing_table.contains_account(&announce_account) {
            self.routing_table.add_account(announce_account.clone());
            self.broadcast_message(
                ctx,
                SendMessage { message: PeerMessage::Sync(SyncData::account(announce_account)) },
            );
        }
    }

    /// Send message to peer that belong to our active set
    /// Return whether the message is sent or not.
    fn send_message(
        &mut self,
        ctx: &mut Context<Self>,
        peer_id: &PeerId,
        message: PeerMessage,
    ) -> bool {
        if let Some(active_peer) = self.active_peers.get(&peer_id) {
            active_peer
                .addr
                .send(SendMessage { message })
                .into_actor(self)
                .map(|res, _, _| res.map_err(|e| error!("Failed sending message: {}", e)))
                .map(|_, _, _| ())
                .spawn(ctx);
            true
        } else {
            debug!(target: "network",
                   "Sending message to: {} (which is not an active peer) Active Peers: {:?}\n{:?}",
                   peer_id,
                   self.active_peers.keys(),
                   message
            );
            false
        }
    }

    /// Return whether the message is sent or not.
    fn send_message_to_account_or_peer_or_hash(
        &mut self,
        ctx: &mut Context<Self>,
        target: &AccountOrPeerIdOrHash,
        msg: RoutedMessageBody,
    ) -> bool {
        match target {
            AccountOrPeerIdOrHash::AccountId(account_id) => {
                self.send_message_to_account(ctx, &account_id, msg)
            }
            peer_or_hash @ AccountOrPeerIdOrHash::PeerId(_)
            | peer_or_hash @ AccountOrPeerIdOrHash::Hash(_) => self.send_message_to_peer(
                ctx,
                RawRoutedMessage { target: peer_or_hash.clone(), body: msg },
            ),
        }
    }

    /// Route signed message to target peer.
    /// Return whether the message is sent or not.
    fn send_signed_message_to_peer(&mut self, ctx: &mut Context<Self>, msg: RoutedMessage) -> bool {
        match self.routing_table.find_route(&msg.target) {
            Ok(peer_id) => {
                // Remember if we expect a response for this message.
                if msg.author == self.peer_id && msg.expect_response() {
                    self.routing_table.add_route_back(msg.hash(), self.peer_id.clone());
                }

                self.send_message(ctx, &peer_id, PeerMessage::Routed(msg))
            }
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                near_metrics::inc_counter(&metrics::DROP_MESSAGE_UNREACHABLE_PEER);
                debug!(target: "network", "{:?} Drop signed message to {:?} Reason {:?}. Known peers: {:?} Message {:?}",
                      self.config.account_id,
                      msg.target,
                      find_route_error,
                      self.routing_table.peer_forwarding.keys(),
                      msg,
                );
                false
            }
        }
    }

    /// Route message to target peer.
    /// Return whether the message is sent or not.
    fn send_message_to_peer(&mut self, ctx: &mut Context<Self>, msg: RawRoutedMessage) -> bool {
        let msg = self.sign_routed_message(msg);
        self.send_signed_message_to_peer(ctx, msg)
    }

    /// Send message to specific account.
    /// Return whether the message is sent or not.
    fn send_message_to_account(
        &mut self,
        ctx: &mut Context<Self>,
        account_id: &AccountId,
        msg: RoutedMessageBody,
    ) -> bool {
        let target = match self.routing_table.account_owner(&account_id) {
            Ok(peer_id) => peer_id,
            Err(find_route_error) => {
                // TODO(MarX, #1369): Message is dropped here. Define policy for this case.
                near_metrics::inc_counter(&metrics::DROP_MESSAGE_UNKNOWN_ACCOUNT);
                debug!(target: "network", "{:?} Drop message to {} Reason {:?}. Known peers: {:?} Message {:?}",
                       self.config.account_id,
                       account_id,
                       find_route_error,
                       self.routing_table.get_accounts_keys(),
                       msg,
                );
                return false;
            }
        };

        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body: msg };
        self.send_message_to_peer(ctx, msg)
    }

    fn sign_routed_message(&self, msg: RawRoutedMessage) -> RoutedMessage {
        msg.sign(self.peer_id.clone(), &self.config.secret_key, self.config.routed_message_ttl)
    }

    // Determine if the given target is referring to us.
    fn message_for_me(&mut self, target: &PeerIdOrHash) -> bool {
        match target {
            PeerIdOrHash::PeerId(peer_id) => peer_id == &self.peer_id,
            PeerIdOrHash::Hash(hash) => {
                self.routing_table.compare_route_back(hash.clone(), &self.peer_id)
            }
        }
    }

    fn propose_edge(&self, peer1: PeerId, with_nonce: Option<u64>) -> EdgeInfo {
        let key = Edge::key(self.peer_id.clone(), peer1.clone());

        // When we create a new edge we increase the latest nonce by 2 in case we miss a removal
        // proposal from our partner.
        let nonce = with_nonce.unwrap_or_else(|| {
            self.routing_table
                .get_edge(self.peer_id.clone(), peer1)
                .map_or(1, |edge| edge.next_nonce())
        });

        EdgeInfo::new(key.0, key.1, nonce, &self.config.secret_key)
    }

    // Ping pong useful functions.

    fn send_ping(&mut self, ctx: &mut Context<Self>, nonce: usize, target: PeerId) {
        let body =
            RoutedMessageBody::Ping(Ping { nonce: nonce as u64, source: self.peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target), body };
        self.send_message_to_peer(ctx, msg);
    }

    fn send_pong(&mut self, ctx: &mut Context<Self>, nonce: usize, target: CryptoHash) {
        let body =
            RoutedMessageBody::Pong(Pong { nonce: nonce as u64, source: self.peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body };
        self.send_message_to_peer(ctx, msg);
    }

    fn handle_ping(&mut self, ctx: &mut Context<Self>, ping: Ping, hash: CryptoHash) {
        self.send_pong(ctx, ping.nonce as usize, hash);
        self.routing_table.add_ping(ping);
    }

    fn handle_pong(&mut self, _ctx: &mut Context<Self>, pong: Pong) {
        self.routing_table.add_pong(pong);
    }

    pub(crate) fn get_network_info(&mut self) -> NetworkInfo {
        let (sent_bytes_per_sec, received_bytes_per_sec) = self.get_total_bytes_per_sec();
        NetworkInfo {
            active_peers: self
                .active_peers
                .values()
                .map(|a| a.full_peer_info.clone())
                .collect::<Vec<_>>(),
            num_active_peers: self.num_active_peers(),
            peer_max_count: self.config.max_peer,
            most_weight_peers: self.most_weight_peers(),
            sent_bytes_per_sec,
            received_bytes_per_sec,
            known_producers: self.routing_table.get_accounts_keys(),
        }
    }

    fn push_network_info(&mut self, ctx: &mut Context<Self>) {
        let network_info = self.get_network_info();

        let _ = self.client_addr.do_send(NetworkClientMessages::NetworkInfo(network_info));
        ctx.run_later(self.config.push_info_period, move |act, ctx| {
            act.push_network_info(ctx);
        });
    }
}

// TODO Incoming needs someone to own TcpListener, temporary workaround until there is a better way
pub struct IncomingCrutch {
    listener: TcpListener,
}

impl Stream for IncomingCrutch {
    type Item = std::io::Result<TcpStream>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Stream::poll_next(std::pin::Pin::new(&mut self.listener.incoming()), cx)
    }
}

impl Actor for PeerManagerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start server if address provided.
        if let Some(server_addr) = self.config.addr {
            // TODO: for now crashes if server didn't start.
            ctx.spawn(TcpListener::bind(server_addr).into_actor(self).then(
                move |listener, act, ctx| {
                    let listener = listener.unwrap();
                    let incoming = IncomingCrutch { listener };
                    info!(target: "stats", "Server listening at {}@{}", act.peer_id, server_addr);
                    ctx.add_message_stream(
                        incoming.filter_map(|x| future::ready(x.map(InboundTcpConnect::new).ok())),
                    );
                    actix::fut::ready(())
                },
            ));
        }

        // Periodically push network information to client
        self.push_network_info(ctx);

        // Start peer monitoring.
        self.monitor_peers(ctx);

        // Start active peer stats querying.
        self.monitor_peer_stats(ctx);
    }

    /// Try to gracefully disconnect from active peers.
    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        let msg = SendMessage { message: PeerMessage::Disconnect };

        for (_, active_peer) in self.active_peers.iter() {
            active_peer.addr.do_send(msg.clone());
        }

        Running::Stop
    }
}

impl Handler<NetworkRequests> for PeerManagerActor {
    type Result = NetworkResponses;

    fn handle(&mut self, msg: NetworkRequests, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            NetworkRequests::Block { block } => {
                self.broadcast_message(ctx, SendMessage { message: PeerMessage::Block(block) });
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockHeaderAnnounce { header, approval_message } => {
                if let Some(approval_message) = approval_message {
                    self.send_message_to_account(
                        ctx,
                        &approval_message.target,
                        RoutedMessageBody::BlockApproval(approval_message.approval),
                    );
                }
                self.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::BlockHeaderAnnounce(header) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                if self.send_message(ctx, &peer_id, PeerMessage::BlockRequest(hash)) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if self.send_message(ctx, &peer_id, PeerMessage::BlockHeadersRequest(hashes)) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateRequest { shard_id, sync_hash, need_header, parts, target } => {
                if self.send_message_to_account_or_peer_or_hash(
                    ctx,
                    &target,
                    RoutedMessageBody::StateRequest(shard_id, sync_hash, need_header, parts),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateResponse { route_back, response } => {
                if self.send_message_to_peer(
                    ctx,
                    RawRoutedMessage {
                        target: AccountOrPeerIdOrHash::Hash(route_back),
                        body: RoutedMessageBody::StateResponse(response),
                    },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                if let Some(peer) = self.active_peers.get(&peer_id) {
                    let _ = peer.addr.do_send(PeerManagerRequest::BanPeer(ban_reason));
                } else {
                    warn!(target: "network", "Try to ban a disconnected peer: {:?}", peer_id);
                    // Call `ban_peer` in peer manager to trigger action that persists information
                    // of ban in disk.
                    self.ban_peer(ctx, &peer_id, ban_reason);
                }

                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                self.announce_account(ctx, announce_account);
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedChunkRequest { account_id, request } => {
                if self.send_message_to_account(
                    ctx,
                    &account_id,
                    RoutedMessageBody::PartialEncodedChunkRequest(request),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkResponse { route_back, partial_encoded_chunk } => {
                if self.send_message_to_peer(
                    ctx,
                    RawRoutedMessage {
                        target: AccountOrPeerIdOrHash::Hash(route_back),
                        body: RoutedMessageBody::PartialEncodedChunk(partial_encoded_chunk),
                    },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                if self.send_message_to_account(
                    ctx,
                    &account_id,
                    RoutedMessageBody::PartialEncodedChunk(partial_encoded_chunk),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ForwardTx(account_id, tx) => {
                if self.send_message_to_account(ctx, &account_id, RoutedMessageBody::ForwardTx(tx))
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::TxStatus(account_id, signer_account_id, tx_hash) => {
                if self.send_message_to_account(
                    ctx,
                    &account_id,
                    RoutedMessageBody::TxStatusRequest(signer_account_id, tx_hash),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::Query { account_id, path, data, id } => {
                if self.send_message_to_account(
                    ctx,
                    &account_id,
                    RoutedMessageBody::QueryRequest { path, data, id },
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ReceiptOutComeRequest(account_id, receipt_id) => {
                if self.send_message_to_account(
                    ctx,
                    &account_id,
                    RoutedMessageBody::ReceiptOutcomeRequest(receipt_id),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::FetchRoutingTable => {
                NetworkResponses::RoutingTableInfo(self.routing_table.info())
            }
            NetworkRequests::Sync { peer_id, sync_data } => {
                // Process edges and add new edges to the routing table. Also broadcast new edges.
                let SyncData { edges, accounts } = sync_data;

                if !edges.iter().all(|edge| edge.verify()) {
                    return NetworkResponses::BanPeer(ReasonForBan::InvalidEdge);
                }

                // Filter known accounts before validating them.
                let new_accounts = accounts
                    .into_iter()
                    .filter_map(|announce_account| {
                        if let Some(current_announce_account) =
                            self.routing_table.get_announce(&announce_account.account_id)
                        {
                            if announce_account.epoch_id == current_announce_account.epoch_id {
                                None
                            } else {
                                Some((
                                    announce_account,
                                    Some(current_announce_account.epoch_id.clone()),
                                ))
                            }
                        } else {
                            Some((announce_account, None))
                        }
                    })
                    .collect();

                // Ask client to validate accounts before accepting them.
                self.view_client_addr
                    .send(NetworkViewClientMessages::AnnounceAccount(new_accounts))
                    .into_actor(self)
                    .then(move |response, act, ctx| {
                        match response {
                        Ok(NetworkViewClientResponses::Ban { ban_reason }) => {
                            if let Some(active_peer) = act.active_peers.get(&peer_id) {
                                active_peer.addr.do_send(PeerManagerRequest::BanPeer(ban_reason));
                            }
                        }
                        Ok(NetworkViewClientResponses::AnnounceAccount(accounts)) => {
                            // Filter known edges.
                            let me = act.peer_id.clone();

                            let new_edges: Vec<_> = edges
                                .into_iter()
                                .filter( |edge| {
                                    if let Some(cur_edge) = act.routing_table.get_edge(edge.peer0.clone(), edge.peer1.clone()){
                                        if cur_edge.nonce >= edge.nonce {
                                            // We have newer update. Drop this.
                                            return false;
                                        }
                                    }
                                    // Add new edge update to the routing table.
                                    act.process_edge(ctx,edge.clone());
                                    if let Some(other) = edge.other(&me) {
                                        // We belong to this edge.
                                        return if act.active_peers.contains_key(&other) {
                                            // This is an active connection.
                                            match edge.edge_type() {
                                                EdgeType::Added => true,
                                                EdgeType::Removed => {
                                                    // Try to update the nonce, and in case it fails removes the peer.
                                                    act.try_update_nonce(ctx, edge.clone(), other);
                                                    false
                                                }
                                            }
                                        } else {
                                            match edge.edge_type() {
                                                EdgeType::Added => {
                                                    act.wait_peer_or_remove(ctx, edge.clone());
                                                    false
                                                }
                                                EdgeType::Removed => true
                                            }
                                        };
                                    } else {

                                    true
                                    }

                                })
                                .collect();

                            // Add accounts to the routing table.
                            debug!(target: "network", "{:?} Received new accounts: {:?}", act.config.account_id, accounts);
                            for account in accounts.iter() {
                                act.routing_table.add_account(account.clone());
                            }

                            let new_data = SyncData { edges: new_edges, accounts };

                            if !new_data.is_empty() {
                                act.broadcast_message(
                                    ctx,
                                    SendMessage { message: PeerMessage::Sync(new_data) },
                                )
                            };
                        }
                        _ => {
                            debug!(target: "network", "Received invalid account confirmation from client.");
                        }
                    }
                        actix::fut::ready(())
                    })
                    .spawn(ctx);

                NetworkResponses::NoResponse
            }
            NetworkRequests::Challenge(challenge) => {
                // TODO(illia): smarter routing?
                self.broadcast_message(
                    ctx,
                    SendMessage { message: PeerMessage::Challenge(challenge) },
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::RequestUpdateNonce(peer_id, edge_info) => {
                if Edge::partial_verify(self.peer_id.clone(), peer_id.clone(), &edge_info) {
                    if let Some(cur_edge) =
                        self.routing_table.get_edge(self.peer_id.clone(), peer_id.clone())
                    {
                        if cur_edge.edge_type() == EdgeType::Added
                            && cur_edge.nonce >= edge_info.nonce
                        {
                            return NetworkResponses::EdgeUpdate(cur_edge);
                        }
                    }

                    let new_edge = Edge::build_with_secret_key(
                        self.peer_id.clone(),
                        peer_id.clone(),
                        edge_info.nonce,
                        &self.config.secret_key,
                        edge_info.signature,
                    );

                    self.process_edge(ctx, new_edge.clone());
                    NetworkResponses::EdgeUpdate(new_edge)
                } else {
                    NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            NetworkRequests::ResponseUpdateNonce(edge) => {
                if edge.contains_peer(&self.peer_id) && edge.verify() {
                    if self.process_edge(ctx, edge.clone()) {
                        let other = edge.other(&self.peer_id).unwrap();
                        if let Some(nonce) = self.pending_update_nonce_request.get(&other) {
                            if edge.nonce >= *nonce {
                                self.pending_update_nonce_request.remove(&other);
                            }
                        }
                    }
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::BanPeer(ReasonForBan::InvalidEdge)
                }
            }
            NetworkRequests::PingTo(nonce, target) => {
                self.send_ping(ctx, nonce, target);
                NetworkResponses::NoResponse
            }
            NetworkRequests::FetchPingPongInfo => {
                let (pings, pongs) = self.routing_table.fetch_ping_pong();
                NetworkResponses::PingPongInfo { pings, pongs }
            }
        }
    }
}

impl Handler<InboundTcpConnect> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: InboundTcpConnect, ctx: &mut Self::Context) {
        if self.is_inbound_allowed() {
            self.connect_peer(ctx.address(), msg.stream, PeerType::Inbound, None, None);
        } else {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network", "Inbound connection dropped (network at max capacity).");
        }
    }
}

impl Handler<OutboundTcpConnect> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: OutboundTcpConnect, ctx: &mut Self::Context) {
        if let Some(addr) = msg.peer_info.addr {
            Resolver::from_registry()
                .send(ConnectAddr(addr))
                .into_actor(self)
                .then(move |res, act, ctx| match res {
                    Ok(res) => match res {
                        Ok(stream) => {
                            debug!(target: "network", "Connecting to {}", msg.peer_info);
                            let edge_info = act.propose_edge(msg.peer_info.id.clone(), None);

                            act.connect_peer(
                                ctx.address(),
                                stream,
                                PeerType::Outbound,
                                Some(msg.peer_info),
                                Some(edge_info),
                            );
                            actix::fut::ready(())
                        }
                        Err(err) => {
                            info!(target: "network", "Error connecting to {}: {}", addr, err);
                            act.outgoing_peers.remove(&msg.peer_info.id);
                            actix::fut::ready(())
                        }
                    },
                    Err(err) => {
                        info!(target: "network", "Error connecting to {}: {}", addr, err);
                        act.outgoing_peers.remove(&msg.peer_info.id);
                        actix::fut::ready(())
                    }
                })
                .wait(ctx);
        } else {
            warn!(target: "network", "Trying to connect to peer with no public address: {:?}", msg.peer_info);
        }
    }
}

impl Handler<Consolidate> for PeerManagerActor {
    type Result = ConsolidateResponse;

    fn handle(&mut self, msg: Consolidate, ctx: &mut Self::Context) -> Self::Result {
        // Check if this is a blacklisted peer.
        if msg.peer_info.addr.as_ref().map_or(true, |addr| self.is_blacklisted(addr)) {
            debug!(target: "network", "Dropping connection from blacklisted peer or unknown address: {:?}", msg.peer_info);
            return ConsolidateResponse::Reject;
        }

        // We already connected to this peer.
        if self.active_peers.contains_key(&msg.peer_info.id) {
            debug!(target: "network", "Dropping handshake (Active Peer). {:?} {:?}", self.peer_id, msg.peer_info.id);
            return ConsolidateResponse::Reject;
        }
        // This is incoming connection but we have this peer already in outgoing.
        // This only happens when both of us connect at the same time, break tie using higher peer id.
        if msg.peer_type == PeerType::Inbound && self.outgoing_peers.contains(&msg.peer_info.id) {
            // We pick connection that has lower id.
            if msg.peer_info.id > self.peer_id {
                debug!(target: "network", "Dropping handshake (Tied). {:?} {:?}", self.peer_id, msg.peer_info.id);
                return ConsolidateResponse::Reject;
            }
        }

        if msg.peer_type == PeerType::Inbound && !self.is_inbound_allowed() {
            // TODO(1896): Gracefully drop inbound connection for other peer.
            debug!(target: "network", "Inbound connection dropped (network at max capacity).");
            return ConsolidateResponse::Reject;
        }

        if msg.other_edge_info.nonce == 0 {
            debug!(target: "network", "Invalid nonce. It must be greater than 0. nonce={}", msg.other_edge_info.nonce);
            return ConsolidateResponse::Reject;
        }

        let last_edge = self.routing_table.get_edge(self.peer_id.clone(), msg.peer_info.id.clone());
        let last_nonce = last_edge.as_ref().map_or(0, |edge| edge.nonce);

        // Check that the received nonce is greater than the current nonce of this connection.
        if last_nonce >= msg.other_edge_info.nonce {
            debug!(target: "network", "Too low nonce. ({} <= {}) {:?} {:?}", msg.other_edge_info.nonce, last_nonce, self.peer_id, msg.peer_info.id);
            // If the check fails don't allow this connection.
            return ConsolidateResponse::InvalidNonce(last_edge.unwrap());
        }

        let require_response = msg.this_edge_info.is_none();

        let edge_info = msg.this_edge_info.clone().unwrap_or_else(|| {
            self.propose_edge(msg.peer_info.id.clone(), Some(msg.other_edge_info.nonce))
        });

        let edge_info_response = if require_response { Some(edge_info.clone()) } else { None };

        // TODO: double check that address is connectable and add account id.
        self.register_peer(
            FullPeerInfo {
                peer_info: msg.peer_info,
                chain_info: msg.chain_info,
                edge_info: msg.other_edge_info,
            },
            edge_info,
            msg.peer_type,
            msg.actor,
            ctx,
        );

        return ConsolidateResponse::Accept(edge_info_response);
    }
}

impl Handler<Unregister> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: Unregister, ctx: &mut Self::Context) {
        self.unregister_peer(ctx, msg.peer_id);
    }
}

impl Handler<Ban> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: Ban, ctx: &mut Self::Context) {
        self.ban_peer(ctx, &msg.peer_id, msg.ban_reason);
    }
}

impl Handler<PeersRequest> for PeerManagerActor {
    type Result = PeerList;

    fn handle(&mut self, _msg: PeersRequest, _ctx: &mut Self::Context) -> Self::Result {
        PeerList { peers: self.peer_store.healthy_peers(self.config.max_send_peers) }
    }
}

impl Handler<PeersResponse> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: PeersResponse, _ctx: &mut Self::Context) {
        self.peer_store.add_peers(
            msg.peers.into_iter().filter(|peer_info| peer_info.id != self.peer_id).collect(),
        );
    }
}

/// "Return" true if this message is for this peer and should be sent to the client.
/// Otherwise try to route this message to the final receiver and return false.
impl Handler<RoutedMessageFrom> for PeerManagerActor {
    type Result = bool;

    fn handle(&mut self, msg: RoutedMessageFrom, ctx: &mut Self::Context) -> Self::Result {
        let RoutedMessageFrom { mut msg, from } = msg;

        if msg.expect_response() {
            self.routing_table.add_route_back(msg.hash(), from.clone());
        }

        if self.message_for_me(&msg.target) {
            // Handle Ping and Pong message if they are for us without sending to client.
            // i.e. Return false in case of Ping and Pong
            match &msg.body {
                RoutedMessageBody::Ping(ping) => self.handle_ping(ctx, ping.clone(), msg.hash()),
                RoutedMessageBody::Pong(pong) => self.handle_pong(ctx, pong.clone()),
                _ => return true,
            }

            false
        } else {
            if msg.decrease_ttl() {
                self.send_signed_message_to_peer(ctx, msg);
            } else {
                warn!(target: "network", "Message dropped because TTL reached 0. Message: {:?} From: {:?}", msg, from);
            }
            false
        }
    }
}

impl Handler<RawRoutedMessage> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: RawRoutedMessage, ctx: &mut Self::Context) {
        if let AccountOrPeerIdOrHash::AccountId(target) = msg.target {
            self.send_message_to_account(ctx, &target, msg.body);
        } else {
            self.send_message_to_peer(ctx, msg);
        }
    }
}

impl Handler<PeerRequest> for PeerManagerActor {
    type Result = PeerResponse;

    fn handle(&mut self, msg: PeerRequest, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            PeerRequest::UpdateEdge((peer, nonce)) => {
                PeerResponse::UpdatedEdge(self.propose_edge(peer, Some(nonce)))
            }
            PeerRequest::RouteBack(body, target) => {
                self.send_message_to_peer(
                    ctx,
                    RawRoutedMessage { target: AccountOrPeerIdOrHash::Hash(target), body },
                );
                PeerResponse::NoResponse
            }
        }
    }
}

impl Handler<StopSignal> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, _msg: StopSignal, ctx: &mut Self::Context) -> Self::Result {
        debug!(target: "network", "Receive Stop Signal. Me: {:?}", self.peer_id);
        ctx.stop();
    }
}
