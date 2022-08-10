use crate::concurrency::demux;
use crate::network_protocol::PeerMessage;
use crate::network_protocol::{SignedAccountData, SyncAccountsData};
use crate::peer::peer_actor::PeerActor;
use crate::private_actix::SendMessage;
use crate::stats::metrics;
use crate::types::FullPeerInfo;
use arc_swap::ArcSwap;
use near_network_primitives::time;
use near_network_primitives::types::{
    PartialEdgeInfo, PeerChainInfoV2, PeerInfo, PeerManagerRequest, PeerManagerRequestWithContext,
    PeerType, ReasonForBan,
};
use near_primitives::network::PeerId;
use near_rate_limiter::ThrottleController;
use std::collections::{hash_map::Entry, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

fn update<T: Clone>(p: &ArcSwap<T>, mut f: impl FnMut(&mut T)) {
    p.rcu(|v| {
        let mut v = (**v).clone();
        f(&mut v);
        Arc::new(v)
    });
}

pub(crate) struct Stats {
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
}

// AtomicCell narrows down a Mutex API to load/store calls.
pub(crate) struct AtomicCell<T>(Mutex<T>);

impl<T: Clone> AtomicCell<T> {
    pub fn new(v: T) -> Self {
        Self(Mutex::new(v))
    }
    pub fn load(&self) -> T {
        self.0.lock().unwrap().clone()
    }
    pub fn store(&self, v: T) {
        *self.0.lock().unwrap() = v;
    }
}

/// Contains information relevant to a connected peer.
pub(crate) struct ConnectedPeer {
    // TODO(gprusak): addr should be internal, so that ConnectedPeer will become an API of the
    // PeerActor.
    pub addr: actix::Addr<PeerActor>,

    pub peer_info: PeerInfo,
    pub partial_edge_info: PartialEdgeInfo,
    pub initial_chain_info: PeerChainInfoV2,
    pub chain_height: AtomicU64,

    /// Who started connection. Inbound (other) or Outbound (us).
    pub peer_type: PeerType,
    /// Time where the connection was established.
    pub connection_established_time: time::Instant,

    /// Last time requested peers.
    pub last_time_peer_requested: AtomicCell<time::Instant>,
    /// Last time we received a message from this peer.
    pub last_time_received_message: AtomicCell<time::Instant>,
    /// Connection stats
    pub stats: ArcSwap<Stats>,
    /// prometheus gauge point guard.
    pub _metric_point: metrics::GaugePoint<metrics::Connection>,

    /// A helper data structure for limiting reading, reporting stats.
    pub throttle_controller: ThrottleController,
    pub send_accounts_data_demux: demux::Demux<Vec<Arc<SignedAccountData>>, ()>,
}

impl fmt::Debug for ConnectedPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("ConnectedPeer")
            .field("peer_info", &self.peer_info)
            .field("partial_edge_info", &self.partial_edge_info)
            .field("peer_type", &self.peer_type)
            .field("connection_established_time", &self.connection_established_time)
            .finish()
    }
}

impl ConnectedPeer {
    pub fn full_peer_info(&self) -> FullPeerInfo {
        let mut chain_info = self.initial_chain_info.clone();
        chain_info.height = self.chain_height.load(Ordering::Relaxed);
        FullPeerInfo {
            peer_info: self.peer_info.clone(),
            chain_info,
            partial_edge_info: self.partial_edge_info.clone(),
        }
    }

    pub fn ban(&self, ban_reason: ReasonForBan) {
        self.addr.do_send(PeerManagerRequestWithContext {
            msg: PeerManagerRequest::BanPeer(ban_reason),
            context: Span::current().context(),
        });
    }

    pub fn unregister(&self) {
        self.addr.do_send(PeerManagerRequestWithContext {
            msg: PeerManagerRequest::UnregisterPeer,
            context: Span::current().context(),
        });
    }

    pub fn send_message(&self, msg: Arc<PeerMessage>) {
        let msg_kind = msg.msg_variant().to_string();
        tracing::trace!(target: "network", ?msg_kind, "Send message");
        self.addr.do_send(SendMessage { message: msg, context: Span::current().context() });
    }

    pub fn send_accounts_data(
        self: &Arc<Self>,
        data: Vec<Arc<SignedAccountData>>,
    ) -> impl Future<Output = ()> {
        let this = self.clone();
        self.send_accounts_data_demux.call(
            data,
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
            },
        )
    }
}

#[derive(Default)]
pub(crate) struct ConnectedPeers(ArcSwap<im::HashMap<PeerId, Arc<ConnectedPeer>>>);

impl ConnectedPeers {
    pub fn read(&self) -> Arc<im::HashMap<PeerId, Arc<ConnectedPeer>>> {
        self.0.load_full()
    }

    pub fn insert(&self, peer: Arc<ConnectedPeer>) {
        let id = peer.peer_info.id.clone();
        update(&self.0, |peers| {
            peers.insert(id.clone(), peer.clone());
        });
    }

    pub fn remove(&self, peer_id: &PeerId) {
        update(&self.0, |peers| {
            peers.remove(peer_id);
        });
    }

    /// Send message to peer that belong to our active set
    /// Return whether the message is sent or not.
    pub fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        let connected_peers = self.read();
        if let Some(peer) = connected_peers.get(&peer_id) {
            peer.send_message(msg);
            return true;
        }
        tracing::debug!(target: "network",
           to = ?peer_id,
           num_connected_peers = connected_peers.len(),
           ?msg,
           "Failed sending message: peer not connected"
        );
        false
    }

    /// Broadcast message to all active peers.
    pub fn broadcast_message(&self, msg: Arc<PeerMessage>) {
        metrics::BROADCAST_MESSAGES.with_label_values(&[msg.msg_variant()]).inc();
        for peer in self.read().values() {
            peer.send_message(msg.clone());
        }
    }
}
