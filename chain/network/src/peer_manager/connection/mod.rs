use crate::concurrency::arc_mutex::ArcMutex;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::concurrency::demux;
use crate::network_protocol::{
    Edge, PeerInfo, PeerMessage, RoutingTableUpdate, SignedAccountData, SyncAccountsData,
};
use crate::peer::peer_actor;
use crate::peer::peer_actor::PeerActor;
use crate::private_actix::SendMessage;
use crate::stats::metrics;
use crate::time;
use crate::types::{BlockInfo, FullPeerInfo, PeerChainInfo, PeerType, ReasonForBan};
use arc_swap::ArcSwap;
use near_o11y::WithSpanContextExt;
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::types::ShardId;
use std::collections::{hash_map::Entry, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(crate) struct Stats {
    /// Number of messages received since the last reset of the counter.
    pub received_messages: AtomicU64,
    /// Number of bytes received since the last reset of the counter.
    pub received_bytes: AtomicU64,
    /// Avg received bytes/s, based on the last few minutes of traffic.
    pub received_bytes_per_sec: AtomicU64,
    /// Avg sent bytes/s, based on the last few minutes of traffic.
    pub sent_bytes_per_sec: AtomicU64,

    /// Number of messages in the buffer to send.
    pub messages_to_send: AtomicU64,
    /// Number of bytes (sum of message sizes) in the buffer to send.
    pub bytes_to_send: AtomicU64,
}

/// Contains information relevant to a connected peer.
pub(crate) struct Connection {
    // TODO(gprusak): addr should be internal, so that Connection will become an API of the
    // PeerActor.
    pub addr: actix::Addr<PeerActor>,

    pub peer_info: PeerInfo,
    pub edge: Edge,
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
    pub last_block: ArcSwap<Option<BlockInfo>>,

    /// Who started connection. Inbound (other) or Outbound (us).
    pub peer_type: PeerType,
    /// Time where the connection was established.
    pub connection_established_time: time::Instant,

    /// Last time requested peers.
    pub last_time_peer_requested: AtomicCell<Option<time::Instant>>,
    /// Last time we received a message from this peer.
    pub last_time_received_message: AtomicCell<time::Instant>,
    /// Connection stats
    pub stats: Arc<Stats>,
    /// prometheus gauge point guard.
    pub _peer_connections_metric: metrics::GaugePoint,

    /// A helper data structure for limiting reading, reporting stats.
    pub send_accounts_data_demux: demux::Demux<Vec<Arc<SignedAccountData>>, ()>,
    pub send_routing_table_update_demux: demux::Demux<Arc<RoutingTableUpdate>, ()>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("peer_info", &self.peer_info)
            .field("edge", &self.edge)
            .field("peer_type", &self.peer_type)
            .field("connection_established_time", &self.connection_established_time)
            .finish()
    }
}

impl Connection {
    pub fn full_peer_info(&self) -> FullPeerInfo {
        let chain_info = PeerChainInfo {
            genesis_id: self.genesis_id.clone(),
            last_block: self.last_block.load().as_ref().clone(),
            tracked_shards: self.tracked_shards.clone(),
            archival: self.archival,
        };
        FullPeerInfo { peer_info: self.peer_info.clone(), chain_info }
    }

    pub fn stop(&self, ban_reason: Option<ReasonForBan>) {
        self.addr.do_send(peer_actor::Stop { ban_reason }.with_span_context());
    }

    // TODO(gprusak): embed Stream directly in Connection,
    // so that we can skip actix queue when sending messages.
    pub fn send_message(&self, msg: Arc<PeerMessage>) {
        let msg_kind = msg.msg_variant().to_string();
        tracing::trace!(target: "network", ?msg_kind, "Send message");
        self.addr.do_send(SendMessage { message: msg }.with_span_context());
    }

    async fn send_routing_table_update_inner(
        self: Arc<Self>,
        rtus: Vec<Arc<RoutingTableUpdate>>,
    ) -> Vec<()> {
        self.send_message(Arc::new(PeerMessage::SyncRoutingTable(RoutingTableUpdate {
            edges: Edge::deduplicate(
                rtus.iter().map(|rtu| rtu.edges.iter()).flatten().cloned().collect(),
            ),
            accounts: rtus.iter().flat_map(|rtu| rtu.accounts.iter()).cloned().collect(),
        })));
        rtus.iter().map(|_| ()).collect()
    }

    pub fn send_routing_table_update(
        self: &Arc<Self>,
        rtu: Arc<RoutingTableUpdate>,
    ) -> impl Future<Output = ()> {
        let this = self.clone();
        async move {
            let res = this
                .send_routing_table_update_demux
                .call(rtu, {
                    let this = this.clone();
                    move |rtus| this.send_routing_table_update_inner(rtus)
                })
                .await;
            if res.is_err() {
                tracing::info!(
                    "peer {} disconnected, while sending SyncRoutingTable",
                    this.peer_info.id
                );
            }
        }
    }

    pub fn send_accounts_data(
        self: &Arc<Self>,
        data: Vec<Arc<SignedAccountData>>,
    ) -> impl Future<Output = ()> {
        let this = self.clone();
        async move {
            let res = this
                .send_accounts_data_demux
                .call(data, {
                    let this = this.clone();
                    |ds: Vec<Vec<Arc<SignedAccountData>>>| async move {
                        let res = ds.iter().map(|_| ()).collect();
                        let mut sum = HashMap::<_, Arc<SignedAccountData>>::new();
                        for d in ds.into_iter().flatten() {
                            match sum.entry((d.epoch_id.clone(), d.account_id.clone())) {
                                Entry::Occupied(mut x) => {
                                    if x.get().timestamp < d.timestamp {
                                        x.insert(d);
                                    }
                                }
                                Entry::Vacant(x) => {
                                    x.insert(d);
                                }
                            }
                        }
                        let msg = Arc::new(PeerMessage::SyncAccountsData(SyncAccountsData {
                            incremental: true,
                            requesting_full_sync: false,
                            accounts_data: sum.into_values().collect(),
                        }));
                        this.send_message(msg);
                        res
                    }
                })
                .await;
            if res.is_err() {
                tracing::info!(
                    "peer {} disconnected, while sending SyncAccountsData",
                    this.peer_info.id
                );
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct PoolSnapshot {
    pub me: PeerId,
    /// Connections which have completed the handshake and are ready
    /// for transmitting messages.
    pub ready: im::HashMap<PeerId, Arc<Connection>>,
    /// Set of started outbound connections, which are not ready yet.
    /// We need to keep those to prevent a deadlock when 2 peers try
    /// to connect to each other at the same time.
    ///
    /// The procedure of establishing a connections should look as follows:
    /// 1. Peer A decides to connect to peer B.
    /// 2. Peer A gets an OutboundHandshakePermit by calling pool.start_outbound(B).
    /// 3. Peer A connects to peer B.
    /// 4. Peer B accepts the connection by calling pool.insert_ready(<connection to A>).
    /// 5. Peer B notifies A that it has accepted the connection.
    /// 6. Peer A accepts the connection by calling pool.insert_ready(<connection to B>).
    /// 7. Peer A drops the OutboundHandshakePermit.
    ///
    /// In case any of these steps fails the connection and the OutboundHandshakePermit
    /// should be dropped.
    ///
    /// Now imagine that A and B try to connect to each other at the same time:
    /// a. Peer A executes 1,2,3.
    /// b. Peer B executes 1,2,3.
    /// c. Both A and B try to execute 4 and exactly one of these calls will succeed: the tie
    ///    is broken by comparing PeerIds: connection from smaller to bigger takes priority.
    ///    WLOG let us assume that A < B.
    /// d. Peer A rejects connection from B, peer B accepts connection from A and notifies A.
    /// e. A continues with 6,7, B just drops the connection and the permit.
    ///
    /// Now imagine a different interleaving:
    /// a. Peer B executes 1,2,3 and A accepts the connection (i.e. 4)
    /// b. Peer A executes 1 and then attempts 2.
    /// In this scenario A will fail to obtain a permit, because it has already accepted a
    /// connection from B.
    pub outbound_handshakes: im::HashSet<PeerId>,
}

pub(crate) struct OutboundHandshakePermit(PeerId, Weak<ArcMutex<PoolSnapshot>>);

impl OutboundHandshakePermit {
    pub fn peer_id(&self) -> &PeerId {
        &self.0
    }
}

impl fmt::Debug for OutboundHandshakePermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.peer_id().fmt(f)
    }
}

impl Drop for OutboundHandshakePermit {
    fn drop(&mut self) {
        if let Some(pool) = self.1.upgrade() {
            pool.update(|pool| {
                pool.outbound_handshakes.remove(&self.0);
            });
        }
    }
}

#[derive(Clone)]
pub(crate) struct Pool(Arc<ArcMutex<PoolSnapshot>>);

#[derive(thiserror::Error, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PoolError {
    #[error("already connected to this peer")]
    AlreadyConnected,
    #[error("already started another outbound connection to this peer")]
    AlreadyStartedConnecting,
    #[error("loop connections are not allowed")]
    LoopConnection,
}

impl Pool {
    pub fn new(me: PeerId) -> Pool {
        Self(Arc::new(ArcMutex::new(PoolSnapshot {
            me,
            ready: im::HashMap::new(),
            outbound_handshakes: im::HashSet::new(),
        })))
    }

    pub fn load(&self) -> Arc<PoolSnapshot> {
        self.0.load()
    }

    pub fn insert_ready(&self, peer: Arc<Connection>) -> Result<(), PoolError> {
        self.0.update(move |pool| {
            let id = &peer.peer_info.id;
            if id == &pool.me {
                return Err(PoolError::LoopConnection);
            }
            if pool.ready.contains_key(id) {
                return Err(PoolError::AlreadyConnected);
            }
            match peer.peer_type {
                PeerType::Inbound => {
                    if pool.outbound_handshakes.contains(id) && id >= &pool.me {
                        return Err(PoolError::AlreadyStartedConnecting);
                    }
                }
                PeerType::Outbound => {
                    // This is a bug, if an outbound permit is not kept
                    // until insert_ready is called.
                    // TODO(gprusak): in fact, we can make insert_ready
                    // consume the outbound permit to additionally ensure
                    // that permit is dropped properly. However we will still
                    // need a runtime check to verify that the permit comes
                    // from the same Pool instance and is for the right PeerId.
                    if !pool.outbound_handshakes.contains(id) {
                        panic!("bug detected: OutboundHandshakePermit dropped before calling Pool.insert_ready()")
                    }
                }
            }
            pool.ready.insert(id.clone(), peer);
            Ok(())
        })
    }

    pub fn start_outbound(&self, peer_id: PeerId) -> Result<OutboundHandshakePermit, PoolError> {
        self.0.update(move |pool| {
            if peer_id == pool.me {
                return Err(PoolError::LoopConnection);
            }
            if pool.ready.contains_key(&peer_id) {
                return Err(PoolError::AlreadyConnected);
            }
            if pool.outbound_handshakes.contains(&peer_id) {
                return Err(PoolError::AlreadyStartedConnecting);
            }
            pool.outbound_handshakes.insert(peer_id.clone());
            Ok(OutboundHandshakePermit(peer_id, Arc::downgrade(&self.0)))
        })
    }

    pub fn remove(&self, peer_id: &PeerId) {
        self.0.update(|pool| {
            pool.ready.remove(peer_id);
        });
    }

    /// Send message to peer that belongs to our active set
    /// Return whether the message is sent or not.
    pub fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        let pool = self.load();
        if let Some(peer) = pool.ready.get(&peer_id) {
            peer.send_message(msg);
            return true;
        }
        tracing::debug!(target: "network",
           to = ?peer_id,
           num_connected_peers = pool.ready.len(),
           ?msg,
           "Failed sending message: peer not connected"
        );
        false
    }

    /// Broadcast message to all ready peers.
    pub fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        metrics::BROADCAST_MESSAGES.with_label_values(&[msg.msg_variant()]).inc();
        for peer in self.load().ready.values() {
            peer.send_message(msg.clone());
        }
    }
}
