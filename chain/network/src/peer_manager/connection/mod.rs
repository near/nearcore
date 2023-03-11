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
use near_crypto::PublicKey;
use near_o11y::WithSpanContextExt;
use near_primitives::block::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::time;
use near_primitives::types::ShardId;
use std::collections::{hash_map::Entry, HashMap};
use std::fmt;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

mod background;
mod stream;

#[cfg(test)]
mod tests;

mod stream;

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

/// Contains information relevant to a connected peer.
pub(crate) struct Connection {
    // TODO(gprusak): add rate limiting on TIER1 connections for defence in-depth.
    pub tier: tcp::Tier,

    pub stream: scope::Service<stream::SharedFrameSender>,

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

    /// Time where the connection was established.
    pub established_time: time::Instant,

    /// Last time requested peers.
    pub last_time_peer_requested: AtomicCell<Option<time::Instant>>,
    /// Last time we received a message from this peer.
    pub last_time_received_message: AtomicCell<time::Instant>,
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

