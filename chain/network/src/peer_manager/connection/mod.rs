use crate::concurrency::arc_mutex::ArcMutex;
use crate::concurrency::atomic_cell::AtomicCell;
use crate::concurrency::demux;
use crate::network_protocol::{
    PeerInfo, PeerMessage, RoutedMessageBody, SignedAccountData, SignedOwnedAccount,
    SyncAccountsData,
};
use crate::peer::peer_actor;
use crate::peer::peer_actor::PeerActor;
use crate::private_actix::SendMessage;
use crate::stats::metrics;
use crate::tcp;
use crate::types::{BlockInfo, FullPeerInfo, PeerChainInfo, PeerType, ReasonForBan};
use arc_swap::ArcSwap;
use near_async::time;
use near_crypto::PublicKey;
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

impl tcp::Tier {
    /// Checks if the given message type is allowed on a connection of the given Tier.
    /// TIER1 is reserved exclusively for BFT consensus messages.
    /// Each validator establishes a lot of TIER1 connections, so bandwidth shouldn't be
    /// wasted on broadcasting or periodic state syncs on TIER1 connections.
    pub(crate) fn is_allowed(self, msg: &PeerMessage) -> bool {
        match msg {
            PeerMessage::Tier1Handshake(_) => self == tcp::Tier::T1,
            PeerMessage::Tier2Handshake(_) => self == tcp::Tier::T2,
            PeerMessage::HandshakeFailure(_, _) => true,
            PeerMessage::LastEdge(_) => true,
            PeerMessage::Routed(msg) => self.is_allowed_routed(&msg.body),
            _ => self == tcp::Tier::T2,
        }
    }

    pub(crate) fn is_allowed_routed(self, body: &RoutedMessageBody) -> bool {
        match body {
            RoutedMessageBody::BlockApproval(..) => true,
            RoutedMessageBody::VersionedPartialEncodedChunk(..) => true,
            _ => self == tcp::Tier::T2,
        }
    }
}

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
    // TODO(gprusak): add rate limiting on TIER1 connections for defence in-depth.
    pub tier: tcp::Tier,
    // TODO(gprusak): addr should be internal, so that Connection will become an API of the
    // PeerActor.
    pub addr: actix::Addr<PeerActor>,

    pub peer_info: PeerInfo,
    /// AccountKey ownership proof.
    pub owned_account: Option<SignedOwnedAccount>,
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
    pub established_time: time::Instant,

    /// Last time requested peers.
    pub last_time_peer_requested: AtomicCell<Option<time::Instant>>,
    /// Last time we received a message from this peer.
    pub last_time_received_message: AtomicCell<time::Instant>,
    /// Connection stats
    pub stats: Arc<Stats>,
    /// prometheus gauge point guard.
    pub _peer_connections_metric: metrics::GaugePoint,

    /// Demultiplexer for the calls to send_accounts_data().
    pub send_accounts_data_demux: demux::Demux<Vec<Arc<SignedAccountData>>, ()>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("peer_info", &self.peer_info)
            .field("peer_type", &self.peer_type)
            .field("established_time", &self.established_time)
            .finish()
    }
}

impl Connection {
    pub fn full_peer_info(&self) -> FullPeerInfo {
        let chain_info = PeerChainInfo {
            genesis_id: self.genesis_id.clone(),
            last_block: *self.last_block.load().as_ref(),
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
                            match sum.entry(d.account_key.clone()) {
                                Entry::Occupied(mut x) => {
                                    if x.get().version < d.version {
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
    /// Index on `ready` by Connection.owned_account.account_key.
    /// We allow only 1 connection to a peer with the given account_key,
    /// as it is an invalid setup to have 2 nodes acting as the same validator.
    pub ready_by_account_key: im::HashMap<PublicKey, Arc<Connection>>,
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
    /// Inbound end of the loop connection. The outbound end is added to the `ready` set.
    pub loop_inbound: Option<Arc<Connection>>,
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
            pool.update(|mut pool| {
                pool.outbound_handshakes.remove(&self.0);
                ((), pool)
            });
        }
    }
}

#[derive(Clone)]
pub(crate) struct Pool(Arc<ArcMutex<PoolSnapshot>>);

#[derive(thiserror::Error, Clone, Debug, PartialEq, Eq)]
pub(crate) enum PoolError {
    #[error("already connected to this peer")]
    AlreadyConnected,
    #[error("already connected to peer {peer_id} with the same account key {account_key}")]
    AlreadyConnectedAccount { peer_id: PeerId, account_key: PublicKey },
    #[error("already started another outbound connection to this peer")]
    AlreadyStartedConnecting,
    #[error("loop connections are not allowed")]
    UnexpectedLoopConnection,
    #[error("OutboundHandshakePermit dropped before calling Pool.insert_ready()")]
    PermitDropped,
}

impl Pool {
    pub fn new(me: PeerId) -> Pool {
        Self(Arc::new(ArcMutex::new(PoolSnapshot {
            loop_inbound: None,
            me,
            ready: im::HashMap::new(),
            ready_by_account_key: im::HashMap::new(),
            outbound_handshakes: im::HashSet::new(),
        })))
    }

    pub fn load(&self) -> Arc<PoolSnapshot> {
        self.0.load()
    }

    pub fn insert_ready(&self, peer: Arc<Connection>) -> Result<(), PoolError> {
        self.0.try_update(move |mut pool| {
            let id = peer.peer_info.id.clone();
            // We support loopback connections for the purpose of
            // validating our own external IP. This is the only case
            // in which we allow 2 connections in a pool to have the same
            // PeerId. The outbound connection is added to the
            // `ready` set, the inbound connection is put into dedicated `loop_inbound` field.
            if id == pool.me && peer.peer_type == PeerType::Inbound {
                if pool.loop_inbound.is_some() {
                    return Err(PoolError::AlreadyConnected);
                }
                // Detect a situation in which a different node tries to connect
                // to us with the same PeerId. This can happen iff the node key
                // has been stolen (or just copied over by mistake).
                if !pool.ready.contains_key(&id) && !pool.outbound_handshakes.contains(&id) {
                    return Err(PoolError::UnexpectedLoopConnection);
                }
                pool.loop_inbound = Some(peer);
                return Ok(((), pool));
            }
            match peer.peer_type {
                PeerType::Inbound => {
                    if pool.outbound_handshakes.contains(&id) && id >= pool.me {
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
                    if !pool.outbound_handshakes.contains(&id) {
                        return Err(PoolError::PermitDropped);
                    }
                }
            }
            if pool.ready.insert(id.clone(), peer.clone()).is_some() {
                return Err(PoolError::AlreadyConnected);
            }
            if let Some(owned_account) = &peer.owned_account {
                // Only 1 connection per account key is allowed.
                // Having 2 peers use the same account key is an invalid setup,
                // which violates the BFT consensus anyway.
                // TODO(gprusak): an incorrectly closed TCP connection may remain in ESTABLISHED
                // state up to minutes/hours afterwards. This may cause problems in
                // case a validator is restarting a node after crash and the new node has the same
                // peer_id/account_key/IP:port as the old node. What is the desired behavior is
                // such a case? Linux TCP implementation supports:
                // TCP_USER_TIMEOUT - timeout for ACKing the sent data
                // TCP_KEEPIDLE - idle connection time after which a KEEPALIVE is sent
                // TCP_KEEPINTVL - interval between subsequent KEEPALIVE probes
                // TCP_KEEPCNT - number of KEEPALIVE probes before closing the connection.
                // If it ever becomes a problem, we can eiter:
                // 1. replace TCP with sth else, like QUIC.
                // 2. use some lower level API than tokio::net to be able to set the linux flags.
                // 3. implement KEEPALIVE equivalent manually on top of TCP to resolve conflicts.
                // 4. allow overriding old connections by new connections, but that will require
                //    a deeper thought to make sure that the connections will be eventually stable
                //    and that incorrect setups will be detectable.
                if let Some(conn) = pool
                    .ready_by_account_key
                    .insert(owned_account.account_key.clone(), peer.clone())
                {
                    // Unwrap is safe, because pool.ready_by_account_key is an index on connections
                    // with owned_account present.
                    let err = PoolError::AlreadyConnectedAccount {
                        peer_id: conn.peer_info.id.clone(),
                        account_key: conn.owned_account.as_ref().unwrap().account_key.clone(),
                    };
                    // We duplicate the error logging here, because returning an error
                    // from insert_ready is expected (pool may regularly reject connections),
                    // however conflicting connections with the same account key indicate an
                    // incorrect validator setup, so we log it here as a warn!, rather than just
                    // info!.
                    tracing::warn!(target:"network", "Pool::register({id}): {err}");
                    metrics::ALREADY_CONNECTED_ACCOUNT.inc();
                    return Err(err);
                }
            }
            Ok(((), pool))
        })
    }

    /// Reserves an OutboundHandshakePermit for the given peer_id.
    /// It should be called before attempting to connect to this peer.
    /// The returned permit shouldn't be dropped until insert_ready for this
    /// outbound connection is called.
    ///
    /// This is required to resolve race conditions in case 2 nodes try to connect
    /// to each other at the same time.
    ///
    /// NOTE: Pool supports loop connections (i.e. connections in which both ends are the same
    /// node) for the purpose of verifying one's own public IP.
    // TODO(gprusak): simplify this flow.
    pub fn start_outbound(&self, peer_id: PeerId) -> Result<OutboundHandshakePermit, PoolError> {
        self.0.try_update(move |mut pool| {
            if pool.ready.contains_key(&peer_id) {
                return Err(PoolError::AlreadyConnected);
            }
            if pool.outbound_handshakes.contains(&peer_id) {
                return Err(PoolError::AlreadyStartedConnecting);
            }
            pool.outbound_handshakes.insert(peer_id.clone());
            Ok((OutboundHandshakePermit(peer_id, Arc::downgrade(&self.0)), pool))
        })
    }

    pub fn remove(&self, conn: &Arc<Connection>) {
        self.0.update(|mut pool| {
            match pool.ready.entry(conn.peer_info.id.clone()) {
                im::hashmap::Entry::Occupied(e) if Arc::ptr_eq(e.get(), conn) => {
                    e.remove_entry();
                }
                _ => {}
            }
            if let Some(owned_account) = &conn.owned_account {
                match pool.ready_by_account_key.entry(owned_account.account_key.clone()) {
                    im::hashmap::Entry::Occupied(e) if Arc::ptr_eq(e.get(), conn) => {
                        e.remove_entry();
                    }
                    _ => {}
                }
            }
            ((), pool)
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
