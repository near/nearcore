use crate::concurrency::demux;
use crate::network_protocol::{Encoding, PeerMessage};
use crate::network_protocol::{SignedAccountData, SyncAccountsData};
use crate::peer::peer_actor::PeerActor;
use crate::private_actix::SendMessage;
use crate::types::{FullPeerInfo, PeerStatsResult};
use arc_swap::ArcSwap;
use near_network_primitives::time;
use near_network_primitives::types::{ReasonForBan,PeerType,PeerManagerRequest,PeerManagerRequestWithContext};
use near_primitives::network::PeerId;
use near_rate_limiter::ThrottleController;
use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::sync::Arc;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use crate::stats::metrics;

fn update<T:Clone>(p:&ArcSwap<T>, mut f: impl FnMut(&mut T)) {
    p.rcu(|v|{
        let mut v = (**v).clone();
        f(&mut v);
        Arc::new(v)
    });
}

/// Contains information relevant to a connected peer.
#[derive(Clone)]
pub(crate) struct ConnectedPeer {
    pub addr: actix::Addr<PeerActor>,
    pub full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
    /// Last time requested peers.
    pub last_time_peer_requested: time::Instant,
    /// Last time we received a message from this peer.
    pub last_time_received_message: time::Instant,
    /// Time where the connection was established.
    pub connection_established_time: time::Instant,
    /// Who started connection. Inbound (other) or Outbound (us).
    pub peer_type: PeerType,
    /// A helper data structure for limiting reading, reporting stats.
    pub throttle_controller: ThrottleController,
    /// Encoding used for communication.
    pub encoding: Option<Encoding>,
    pub send_accounts_data_demux: demux::Demux<Vec<Arc<SignedAccountData>>, ()>,
}

pub(crate) struct ConnectedPeerHandle(ArcSwap<ConnectedPeer>);

impl ConnectedPeer {
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

    pub fn send_message(&self,msg:Arc<PeerMessage>) {
        let msg_kind = msg.msg_variant().to_string();
        tracing::trace!(target: "network", ?msg_kind, "Send message");
        // Sending may fail in case a peer connection is closed in the meantime.
        if let Err(err) = self.addr.try_send(SendMessage { message:msg, context: Span::current().context() }) {
            tracing::warn!("peer mailbox error: {err}");
        }
    }

    pub fn send_accounts_data(
        self:&Arc<Self>,
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

impl ConnectedPeerHandle {
    pub fn new(peer:ConnectedPeer) -> Self { Self(ArcSwap::new(Arc::new(peer))) }

    pub fn read(&self) -> Arc<ConnectedPeer> { self.0.load_full() }

    pub fn set_last_time_peer_requested(&self, t: time::Instant) {
        update(&self.0,|p| p.last_time_peer_requested = t);
    }

    pub fn set_last_time_received_message(&self, t: time::Instant) {
        update(&self.0,|p| p.last_time_received_message = t);
    }

    pub fn set_peer_stats(&self, stats: PeerStatsResult) {
        update(&self.0,|p| {
            p.full_peer_info.chain_info = stats.chain_info.clone();
            p.sent_bytes_per_sec = stats.sent_bytes_per_sec;
            p.received_bytes_per_sec = stats.received_bytes_per_sec;
            p.encoding = stats.encoding;
        });
    }
}

#[derive(Default)]
pub(crate) struct ConnectedPeers(ArcSwap<im::HashMap<PeerId, Arc<ConnectedPeerHandle>>>);

impl ConnectedPeers {
    pub fn read(&self) -> Arc<im::HashMap<PeerId, Arc<ConnectedPeerHandle>>> {
        self.0.load_full()
    }

    pub fn insert(&self, peer: ConnectedPeer) -> Arc<ConnectedPeerHandle> {
        let id = peer.full_peer_info.peer_info.id.clone();
        let peer = Arc::new(ConnectedPeerHandle::new(peer.clone()));
        update(&self.0,|peers|{ peers.insert(id.clone(), peer.clone()); });
        peer
    }

    pub fn remove(&self, peer_id: &PeerId) {
        update(&self.0,|peers|{ peers.remove(peer_id); });
    }

    /// Send message to peer that belong to our active set
    /// Return whether the message is sent or not.
    pub fn send_message(&self, peer_id: PeerId, msg: Arc<PeerMessage>) -> bool {
        let connected_peers = self.read();
        if let Some(peer) = connected_peers.get(&peer_id) {
            peer.read().send_message(msg);
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
            peer.read().send_message(msg.clone());
        }
    }
}
