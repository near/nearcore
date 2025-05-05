use super::StateSyncDownloadSource;
use super::task_tracker::TaskHandle;
use crate::metrics;
use crate::sync::state::util::increment_download_count;
use futures::FutureExt;
use futures::future::BoxFuture;
use near_async::messaging::AsyncSender;
use near_async::time::{Clock, Duration};
use near_chain::BlockHeader;
use near_network::types::{
    NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_sync::{ShardStateSyncResponse, ShardStateSyncResponseHeader};
use near_primitives::types::ShardId;
use near_store::{DBCol, Store};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::select;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// Logic to download state sync headers and parts from peers.
pub(super) struct StateSyncDownloadSourcePeer {
    pub clock: Clock,
    pub store: Store,
    pub request_sender: AsyncSender<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    pub request_timeout: Duration,
    pub state: Arc<Mutex<StateSyncDownloadSourcePeerSharedState>>,
}

#[derive(Default)]
pub(super) struct StateSyncDownloadSourcePeerSharedState {
    highest_height_peers: Vec<PeerId>,
    /// Tracks pending requests we have sent to peers. The requests are indexed by
    /// (shard ID, sync hash, part ID or header), and the value is the peer ID we
    /// expect the response from, as well as a channel sender to complete the future
    /// waiting for the response.
    pending_requests: HashMap<PendingPeerRequestKey, PendingPeerRequestValue>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct PendingPeerRequestKey {
    shard_id: ShardId,
    sync_hash: CryptoHash,
    kind: PartIdOrHeader,
}

struct PendingPeerRequestValue {
    peer_id: Option<PeerId>, // present for headers, not for parts
    sender: oneshot::Sender<ShardStateSyncResponse>,
}

impl StateSyncDownloadSourcePeerSharedState {
    pub fn receive_peer_message(
        &mut self,
        peer_id: PeerId,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        data: ShardStateSyncResponse,
    ) -> Result<(), near_chain::Error> {
        let key = PendingPeerRequestKey {
            shard_id,
            sync_hash,
            kind: match data.part_id() {
                Some(part_id) => PartIdOrHeader::Part { part_id },
                None => PartIdOrHeader::Header,
            },
        };

        let Some(request) = self.pending_requests.get(&key) else {
            tracing::debug!(target: "sync", "Received {:?} expecting {:?}", key, self.pending_requests.keys());
            return Err(near_chain::Error::Other("Unexpected state response".to_owned()));
        };

        if request.peer_id.as_ref().is_some_and(|expecting_peer_id| expecting_peer_id != &peer_id) {
            return Err(near_chain::Error::Other(
                "Unexpected state response (wrong sender)".to_owned(),
            ));
        }

        let value = self.pending_requests.remove(&key).unwrap();
        let _ = value.sender.send(data);
        Ok(())
    }

    /// Sets the peers that are eligible for querying state sync headers/parts.
    pub fn set_highest_peers(&mut self, peers: Vec<PeerId>) {
        self.highest_height_peers = peers;
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum PartIdOrHeader {
    Part { part_id: u64 },
    Header,
}

impl StateSyncDownloadSourcePeer {
    async fn try_download(
        clock: Clock,
        request_sender: AsyncSender<PeerManagerMessageRequest, PeerManagerMessageResponse>,
        key: PendingPeerRequestKey,
        store: Store,
        state: Arc<Mutex<StateSyncDownloadSourcePeerSharedState>>,
        cancel: CancellationToken,
        request_timeout: Duration,
        handle: Arc<TaskHandle>,
    ) -> Result<ShardStateSyncResponse, near_chain::Error> {
        handle.set_status("Preparing request");

        // Sender/receiver pair used to await for the peer's response.
        let (sender, receiver) = oneshot::channel();

        // Peers advertise their snapshots by the prev prev hash of the sync hash.
        // We compute it here to pass as part of the network request.
        // TODO(saketh): it would be nice to migrate the network layer to the same hash.
        let prev_hash = *store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, key.sync_hash.as_bytes())?
            .ok_or_else(|| {
                near_chain::Error::DBNotFoundErr(format!("No block header {}", key.sync_hash))
            })?
            .prev_hash();
        let prev_prev_hash = *store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, prev_hash.as_bytes())?
            .ok_or_else(|| {
                near_chain::Error::DBNotFoundErr(format!("No block header {}", prev_hash))
            })?
            .prev_hash();

        let network_request = match &key.kind {
            PartIdOrHeader::Part { part_id } => {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::StateRequestPart {
                    shard_id: key.shard_id,
                    sync_hash: key.sync_hash,
                    sync_prev_prev_hash: prev_prev_hash,
                    part_id: *part_id,
                })
            }
            PartIdOrHeader::Header => {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::StateRequestHeader {
                    shard_id: key.shard_id,
                    sync_hash: key.sync_hash,
                    sync_prev_prev_hash: prev_prev_hash,
                })
            }
        };

        let deadline = clock.now() + request_timeout;
        let typ = match &key.kind {
            PartIdOrHeader::Part { .. } => "part",
            PartIdOrHeader::Header => "header",
        };

        let _timer = metrics::STATE_SYNC_P2P_REQUEST_DELAY
            .with_label_values(&[&key.shard_id.to_string(), &typ])
            .start_timer();

        handle.set_status("Sending network request");
        let network_response = match request_sender.send_async(network_request).await {
            Ok(response) => response.as_network_response(),
            Err(e) => {
                increment_download_count(key.shard_id, typ, "network", "failed_to_send");
                return Err(near_chain::Error::Other(format!("Failed to send request: {}", e)));
            }
        };

        let request_sent_to_peer = match network_response {
            NetworkResponses::SelectedDestination(peer_id) => peer_id,
            NetworkResponses::NoDestinationsAvailable => {
                increment_download_count(key.shard_id, typ, "network", "no_hosts_available");
                return Err(near_chain::Error::Other("No hosts available".to_owned()));
            }
            NetworkResponses::RouteNotFound => {
                increment_download_count(key.shard_id, typ, "network", "route_not_found");
                return Err(near_chain::Error::Other("Route not found".to_owned()));
            }
            NetworkResponses::MyPublicAddrNotKnown => {
                increment_download_count(key.shard_id, typ, "network", "my_public_addr_not_known");
                return Err(near_chain::Error::Other("Awaiting IP self-discovery".to_owned()));
            }
            NetworkResponses::NoResponse => {
                increment_download_count(key.shard_id, typ, "network", "no_response");
                return Err(near_chain::Error::Other("No response".to_owned()));
            }
        };

        let state_value = PendingPeerRequestValue { peer_id: Some(request_sent_to_peer), sender };

        // Ensures that the key is removed from the map of pending requests when this scope exits,
        // whether on success or timeout.
        let _remove_key_upon_drop = RemoveKeyUponDrop { key: key.clone(), state: state.clone() };
        {
            let mut state_lock = state.lock();
            state_lock.pending_requests.insert(key.clone(), state_value);
        }

        handle.set_status("Waiting for peer response");
        select! {
            _ = clock.sleep_until(deadline) => {
                increment_download_count(key.shard_id, typ, "network", "timeout");
                Err(near_chain::Error::Other("Timeout".to_owned()))
            }
            _ = cancel.cancelled() => {
                increment_download_count(key.shard_id, typ, "network", "cancelled");
                Err(near_chain::Error::Other("Cancelled".to_owned()))
            }
            result = receiver => {
                match result {
                    Ok(result) => {
                        increment_download_count(key.shard_id, typ, "network", "success");
                        Ok(result)
                    }
                    Err(_) => {
                        increment_download_count(key.shard_id, typ, "network", "sender_dropped");
                        Err(near_chain::Error::Other("Sender dropped".to_owned()))
                    },
                }
            }
        }
    }
}

// Simple RAII structure to remove a key from the pending requests map.
struct RemoveKeyUponDrop {
    key: PendingPeerRequestKey,
    state: Arc<Mutex<StateSyncDownloadSourcePeerSharedState>>,
}

impl Drop for RemoveKeyUponDrop {
    fn drop(&mut self) {
        let mut state_lock = self.state.lock();
        state_lock.pending_requests.remove(&self.key);
    }
}

impl StateSyncDownloadSource for StateSyncDownloadSourcePeer {
    fn download_shard_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<ShardStateSyncResponseHeader, near_chain::Error>> {
        let key = PendingPeerRequestKey { shard_id, sync_hash, kind: PartIdOrHeader::Header };
        let fut = Self::try_download(
            self.clock.clone(),
            self.request_sender.clone(),
            key,
            self.store.clone(),
            self.state.clone(),
            cancel,
            self.request_timeout,
            handle,
        );
        fut.map(|response| {
            response.and_then(|response| {
                response
                    .take_header()
                    .ok_or_else(|| near_chain::Error::Other("Expected header".to_owned()))
            })
        })
        .instrument(tracing::debug_span!("StateSyncDownloadSourcePeer::download_shard_header"))
        .boxed()
    }

    fn download_shard_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Vec<u8>, near_chain::Error>> {
        let key =
            PendingPeerRequestKey { shard_id, sync_hash, kind: PartIdOrHeader::Part { part_id } };
        let fut = Self::try_download(
            self.clock.clone(),
            self.request_sender.clone(),
            key,
            self.store.clone(),
            self.state.clone(),
            cancel,
            self.request_timeout,
            handle,
        );
        fut.map(|response| {
            response.and_then(|response| {
                response
                    .take_part()
                    .ok_or_else(|| near_chain::Error::Other("Expected part".to_owned()))
                    .map(|(_, part)| part)
            })
        })
        .instrument(tracing::debug_span!("StateSyncDownloadSourcePeer::download_shard_part"))
        .boxed()
    }
}
