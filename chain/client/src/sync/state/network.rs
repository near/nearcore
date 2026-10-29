use super::StateSyncDownloadSource;
use super::task_tracker::TaskHandle;
use crate::metrics;
use crate::sync::state::util::increment_download_count;
use futures::FutureExt;
use futures::future::BoxFuture;
use near_async::messaging::AsyncSender;
use near_async::time::{Clock, Duration};
use near_chain::BlockHeader;
use near_network::client::StateRequestUpdate;
use near_network::types::{
    NetworkRequests, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_part::{StatePart, StatePartIndex};
use near_primitives::state_sync::{
    PartOrHeader, ShardStateSyncResponse, ShardStateSyncResponseHeader, StateRequestAckBody,
};
use near_primitives::types::ShardId;
use near_store::{DBCol, Store};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::mem;
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
    part_or_header: PartOrHeader,
}

struct PendingPeerRequestValue {
    /// The peer the request was sent to, once the network has named it.
    peer_id: Option<PeerId>,
    /// Messages about the request which arrived before the network named the peer, with their
    /// senders. They are handled once it has.
    early_messages: Vec<(PeerId, StateRequestUpdate)>,
    sender: oneshot::Sender<ShardStateSyncResponse>,
}

impl StateSyncDownloadSourcePeerSharedState {
    pub fn receive_peer_message(
        &mut self,
        peer_id: PeerId,
        msg: StateRequestUpdate,
    ) -> Result<(), near_chain::Error> {
        let shard_id = msg.shard_id();
        let part_or_header = msg.part_or_header();

        match msg {
            StateRequestUpdate::Ack(ref ack) => {
                metrics::STATE_SYNC_PEER_MSGS
                    .with_label_values::<&str>(&[
                        shard_id.to_string().as_str(),
                        part_or_header.into(),
                        ack.body.into(),
                    ])
                    .inc();
            }
            StateRequestUpdate::State(_) => {
                metrics::STATE_SYNC_PEER_MSGS
                    .with_label_values::<&str>(&[
                        shard_id.to_string().as_str(),
                        part_or_header.into(),
                        "state",
                    ])
                    .inc();
            }
            StateRequestUpdate::NotSent { .. } => {
                metrics::STATE_SYNC_PEER_MSGS
                    .with_label_values::<&str>(&[
                        shard_id.to_string().as_str(),
                        part_or_header.into(),
                        "not_sent",
                    ])
                    .inc();
            }
        };

        let key = PendingPeerRequestKey { shard_id, sync_hash: msg.sync_hash(), part_or_header };
        self.handle_peer_message(&key, peer_id, msg)
    }

    /// Records the peer the network sent the request for `key` to, and handles the messages about
    /// the request which arrived before it did.
    fn set_peer(&mut self, key: &PendingPeerRequestKey, peer_id: PeerId) {
        let Some(request) = self.pending_requests.get_mut(key) else {
            return;
        };
        request.peer_id = Some(peer_id);
        for (sender, msg) in mem::take(&mut request.early_messages) {
            if let Err(err) = self.handle_peer_message(key, sender, msg) {
                tracing::error!(target: "sync", ?err, "error applying state sync response");
            }
        }
    }

    fn handle_peer_message(
        &mut self,
        key: &PendingPeerRequestKey,
        peer_id: PeerId,
        msg: StateRequestUpdate,
    ) -> Result<(), near_chain::Error> {
        let Some(request) = self.pending_requests.get_mut(key) else {
            tracing::debug!(target: "sync", ?key, %peer_id, "unexpected state response, request may have timed out");
            return Ok(());
        };

        let Some(expected_peer_id) = &request.peer_id else {
            // The network can report on the request, e.g. that it could not send it, before
            // `try_download` learns which peer it was sent to. Keep the message until then.
            request.early_messages.push((peer_id, msg));
            return Ok(());
        };

        if *expected_peer_id != peer_id {
            return Err(near_chain::Error::Other(
                "Unexpected state response (wrong sender)".to_owned(),
            ));
        }

        match msg {
            StateRequestUpdate::Ack(ack) => {
                match ack.body {
                    StateRequestAckBody::Busy | StateRequestAckBody::Error => {
                        // We received a message indicating that the peer won't send a response.
                        // Removing the entry from pending_requests drops the sender channel
                        // so that the node will stop awaiting the state response.
                        let _ = self.pending_requests.remove(key).unwrap();
                    }
                    StateRequestAckBody::WillRespond => {
                        // Do nothing with positive responses for now.
                        // In a future release we could implement a separate deadline
                        // for receiving WillRespond.
                    }
                }
            }
            StateRequestUpdate::State(state) => {
                let value = self.pending_requests.remove(key).unwrap();
                let _ = value.sender.send(state.take_state_response());
            }
            StateRequestUpdate::NotSent { .. } => {
                // The request never reached the peer. As for a negative ack, removing the entry
                // drops the sender channel, so that the request is retried without waiting for
                // it to time out.
                let _ = self.pending_requests.remove(key).unwrap();
            }
        };

        Ok(())
    }
}

/// The hash a peer's snapshot serving `sync_hash` is advertised under, which is what
/// `snapshot_hash_for_block` names for the snapshot a host takes for that sync hash. Spice
/// snapshots the state the sync block's own chunks left behind, so the snapshot sits at the sync
/// block; otherwise the host snapshots at the sync block's prev block, keyed by its prev prev.
fn snapshot_hash_for_sync_hash(
    store: &Store,
    sync_hash: &CryptoHash,
) -> Result<CryptoHash, near_chain::Error> {
    let header = |hash: &CryptoHash| {
        store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, hash.as_bytes())
            .ok_or_else(|| near_chain::Error::DBNotFoundErr(format!("No block header {hash}")))
    };
    let sync_header = header(sync_hash)?;
    if sync_header.is_spice() {
        return Ok(*sync_hash);
    }
    Ok(*header(sync_header.prev_hash())?.prev_hash())
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

        // Peers advertise their snapshots by the block the snapshot was taken at. We compute it
        // here to pass as part of the network request.
        // TODO(saketh): it would be nice to migrate the network layer to the same hash. Under
        // spice it already is the sync hash; before spice it is still the prev prev block.
        let snapshot_hash = snapshot_hash_for_sync_hash(&store, &key.sync_hash)?;

        let network_request = match &key.part_or_header {
            PartOrHeader::Part { part_idx } => {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::StateRequestPart {
                    shard_id: key.shard_id,
                    sync_hash: key.sync_hash,
                    snapshot_hash,
                    part_idx: *part_idx,
                })
            }
            PartOrHeader::Header => {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::StateRequestHeader {
                    shard_id: key.shard_id,
                    sync_hash: key.sync_hash,
                    snapshot_hash,
                })
            }
        };

        let deadline = clock.now() + request_timeout;
        let typ = match &key.part_or_header {
            PartOrHeader::Part { .. } => "part",
            PartOrHeader::Header => "header",
        };

        let _timer = metrics::STATE_SYNC_P2P_REQUEST_DELAY
            .with_label_values(&[key.shard_id.to_string().as_str(), &typ])
            .start_timer();

        // Register the request before sending it: the network may report on it, e.g. that it could
        // not send it, before we process its reply naming the peer.
        let state_value = PendingPeerRequestValue { peer_id: None, early_messages: vec![], sender };

        // Ensures that the key is removed from the map of pending requests when this scope exits,
        // whether on success or timeout.
        let _remove_key_upon_drop = RemoveKeyUponDrop { key: key.clone(), state: state.clone() };
        {
            let mut state_lock = state.lock();
            state_lock.pending_requests.insert(key.clone(), state_value);
        }

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

        tracing::debug!(target: "sync", ?key, ?request_sent_to_peer, "p2p request sent");

        state.lock().set_peer(&key, request_sent_to_peer.clone());

        handle.set_status("Waiting for peer response");
        select! {
            _ = clock.sleep_until(deadline) => {
                increment_download_count(key.shard_id, typ, "network", "timeout");
                tracing::debug!(target: "sync", ?key, ?request_sent_to_peer, "p2p request timed out");
                Err(near_chain::Error::Other("Timeout".to_owned()))
            }
            _ = cancel.cancelled() => {
                increment_download_count(key.shard_id, typ, "network", "cancelled");
                Err(near_chain::Error::Other("Cancelled".to_owned()))
            }
            result = receiver => {
                match result {
                    Ok(result) => {
                        tracing::debug!(target: "sync", ?key, ?request_sent_to_peer, "p2p request succeeded");
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
        let key =
            PendingPeerRequestKey { shard_id, sync_hash, part_or_header: PartOrHeader::Header };
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
        part_idx: StatePartIndex,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<StatePart, near_chain::Error>> {
        let key = PendingPeerRequestKey {
            shard_id,
            sync_hash,
            part_or_header: PartOrHeader::Part { part_idx },
        };
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

#[cfg(test)]
mod tests {
    use super::{
        StateSyncDownloadSourcePeer, StateSyncDownloadSourcePeerSharedState,
        snapshot_hash_for_sync_hash,
    };
    use crate::sync::state::StateSyncDownloadSource;
    use crate::sync::state::task_tracker::TaskTracker;
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use near_async::messaging::AsyncSender;
    use near_async::time::{Clock, Duration, FakeClock};
    use near_chain::state_snapshot_actor::snapshot_hash_for_block;
    use near_chain::test_utils::get_chain_with_genesis;
    use near_chain::{BlockHeader, ChainStoreAccess as _};
    use near_chain_configs::test_genesis::TestGenesisBuilder;
    use near_crypto::{KeyType, SecretKey};
    use near_network::client::StateRequestUpdate;
    use near_network::types::{
        NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
    };
    use near_primitives::hash::CryptoHash;
    use near_primitives::network::PeerId;
    use near_primitives::state_part::StatePart;
    use near_primitives::state_sync::PartOrHeader;
    use near_primitives::types::ShardId;
    use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
    use near_store::{DBCol, Store};
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    /// A store holding `len` headers on top of a test genesis, and the headers, genesis first.
    fn header_chain(protocol_version: ProtocolVersion, len: usize) -> (Store, Vec<BlockHeader>) {
        let genesis = TestGenesisBuilder::new()
            .genesis_time_from_clock(&Clock::real())
            .protocol_version(protocol_version)
            .build();
        let chain = get_chain_with_genesis(Clock::real(), genesis);
        let store = chain.chain_store().store();
        let mut headers = vec![chain.genesis().clone()];
        for _ in 0..len {
            let prev = headers.last().unwrap();
            let mut header = prev.clone();
            header.set_prev_hash(*prev.hash());
            header.set_height(prev.height() + 1);
            header.set_prev_height(prev.height());
            header.init();
            let mut store_update = store.store_update();
            store_update.insert_ser(DBCol::BlockHeader, header.hash().as_ref(), &header);
            store_update.commit();
            headers.push(header);
        }
        (store, headers)
    }

    fn non_spice_protocol_version() -> ProtocolVersion {
        PROTOCOL_VERSION.min(ProtocolFeature::Spice.protocol_version() - 1)
    }

    #[test]
    fn test_snapshot_hash_for_sync_hash_is_sync_prev_prev() {
        let (store, headers) = header_chain(non_spice_protocol_version(), 4);
        let sync_hash = headers[4].hash();
        assert_eq!(snapshot_hash_for_sync_hash(&store, sync_hash).unwrap(), *headers[2].hash());
    }

    /// The host snapshots the sync block's prev block and advertises it under
    /// `snapshot_hash_for_block`; a requester has to ask for that same hash to find the host.
    #[test]
    fn test_snapshot_hash_agrees_between_host_and_requester() {
        let (store, headers) = header_chain(non_spice_protocol_version(), 4);
        let sync_hash = headers[4].hash();
        let host = snapshot_hash_for_block(&headers[3]);
        assert_eq!(snapshot_hash_for_sync_hash(&store, sync_hash).unwrap(), host);
    }

    #[test]
    fn test_snapshot_hash_for_sync_hash_missing_header() {
        let (store, _) = header_chain(non_spice_protocol_version(), 1);
        let err = snapshot_hash_for_sync_hash(&store, &CryptoHash::hash_bytes(b"unknown"));
        assert!(matches!(err, Err(near_chain::Error::DBNotFoundErr(_))), "{err:?}");
    }

    #[cfg(feature = "protocol_feature_spice")]
    #[test]
    fn test_snapshot_hash_for_sync_hash_spice_is_sync_hash() {
        let (store, headers) = header_chain(ProtocolFeature::Spice.protocol_version(), 4);
        assert!(headers[4].is_spice());
        let sync_hash = headers[4].hash();
        assert_eq!(snapshot_hash_for_sync_hash(&store, sync_hash).unwrap(), *sync_hash);
    }

    /// Under spice the host snapshots the sync block itself.
    #[cfg(feature = "protocol_feature_spice")]
    #[test]
    fn test_snapshot_hash_agrees_between_host_and_requester_spice() {
        let (store, headers) = header_chain(ProtocolFeature::Spice.protocol_version(), 4);
        let sync_hash = headers[4].hash();
        let host = snapshot_hash_for_block(&headers[4]);
        assert_eq!(snapshot_hash_for_sync_hash(&store, sync_hash).unwrap(), host);
    }

    fn peer(seed: &str) -> PeerId {
        PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed).public_key())
    }

    /// A source downloading from peers over a fake network. For each request, `network` is given
    /// the source's shared state, e.g. to deliver messages, and returns the peer it sends the
    /// request to. Time never passes, so requests can't time out.
    fn peer_source<F>(store: Store, network: F) -> StateSyncDownloadSourcePeer
    where
        F: Fn(&mut StateSyncDownloadSourcePeerSharedState) -> PeerId + Send + Sync + 'static,
    {
        let state = Arc::new(Mutex::new(StateSyncDownloadSourcePeerSharedState::default()));
        let request_sender = AsyncSender::from_fn({
            let state = state.clone();
            move |_: PeerManagerMessageRequest| -> PeerManagerMessageResponse {
                NetworkResponses::SelectedDestination(network(&mut state.lock())).into()
            }
        });
        StateSyncDownloadSourcePeer {
            clock: FakeClock::default().clock(),
            store,
            request_sender,
            request_timeout: Duration::seconds(10),
            state,
        }
    }

    /// Starts downloading part 0 of shard 0 at `sync_hash`.
    fn download_part(
        source: &StateSyncDownloadSourcePeer,
        sync_hash: CryptoHash,
    ) -> BoxFuture<'_, Result<StatePart, near_chain::Error>> {
        let handle = TaskTracker::new(1).get_handle("part").now_or_never().unwrap();
        source.download_shard_part(ShardId::new(0), sync_hash, 0, handle, CancellationToken::new())
    }

    /// The network's report that it could not send the request for part 0 of shard 0.
    fn part_not_sent(sync_hash: CryptoHash) -> StateRequestUpdate {
        StateRequestUpdate::NotSent {
            shard_id: ShardId::new(0),
            sync_hash,
            part_or_header: PartOrHeader::Part { part_idx: 0 },
        }
    }

    /// A download stops waiting as soon as the network reports its request as not sent, even if
    /// the report arrives before the network's reply naming the peer.
    #[test]
    fn test_not_sent_request_stops_waiting() {
        let (store, headers) = header_chain(non_spice_protocol_version(), 2);
        let sync_hash = *headers[2].hash();
        let source = peer_source(store, move |state| {
            state.receive_peer_message(peer("host"), part_not_sent(sync_hash)).unwrap();
            peer("host")
        });
        let result = download_part(&source, sync_hash).now_or_never();
        assert!(result.expect("download should stop waiting").is_err());
    }

    /// A report that a request to another host was not sent, e.g. from an earlier attempt which
    /// timed out, leaves the download waiting for its own host, whether the report arrives before
    /// or after the network names the host.
    #[test]
    fn test_not_sent_from_other_host_is_ignored() {
        let (store, headers) = header_chain(non_spice_protocol_version(), 2);
        let sync_hash = *headers[2].hash();
        let source = peer_source(store, move |state| {
            state.receive_peer_message(peer("other"), part_not_sent(sync_hash)).unwrap();
            peer("host")
        });
        let mut download = download_part(&source, sync_hash);
        assert!((&mut download).now_or_never().is_none());
        let report =
            source.state.lock().receive_peer_message(peer("other"), part_not_sent(sync_hash));
        assert!(report.is_err());
        assert!(download.now_or_never().is_none());
    }
}
