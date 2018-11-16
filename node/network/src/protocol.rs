use io::{NetSyncIo, SyncIo};
use message::*;
use parking_lot::RwLock;
use primitives::traits::{Block, Decode, Encode, GenericResult};
use rand::{seq, thread_rng};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::time;
/// network protocol
use substrate_network_libp2p::{NodeIndex, ProtocolId, Severity};

/// time to wait (secs) for a request
const REQUEST_WAIT: u64 = 60;

/// current version of the protocol
pub(crate) const CURRENT_VERSION: u32 = 1;

pub struct ProtocolConfig {
    // config information goes here
    pub protocol_id: ProtocolId,
}

impl ProtocolConfig {
    pub fn new(protocol_id: ProtocolId) -> ProtocolConfig {
        ProtocolConfig { protocol_id }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        ProtocolConfig::new(ProtocolId::default())
    }
}

pub(crate) struct PeerInfo {
    // information about connected peers
    request_timestamp: Option<time::Instant>,
}

pub trait Transaction: Send + Sync + Serialize + DeserializeOwned + Debug + 'static {}
impl<T> Transaction for T where T: Send + Sync + Serialize + DeserializeOwned + Debug + 'static {}

#[allow(dead_code)]
pub struct Protocol<T> {
    // TODO: add more fields when we need them
    config: ProtocolConfig,
    // peers that are in the handshaking process
    handshaking_peers: RwLock<HashMap<NodeIndex, time::Instant>>,
    // info about peers
    peer_info: RwLock<HashMap<NodeIndex, PeerInfo>>,
    // callbacks
    tx_callback: fn(T) -> GenericResult,
}

impl<T: Transaction> Protocol<T> {
    pub fn new(config: ProtocolConfig, tx_callback: fn(T) -> GenericResult) -> Protocol<T> {
        Protocol {
            config,
            handshaking_peers: RwLock::new(HashMap::new()),
            peer_info: RwLock::new(HashMap::new()),
            tx_callback,
        }
    }

    pub fn on_peer_connected<B: Block>(&self, net_sync: &mut NetSyncIo, peer: NodeIndex) {
        self.handshaking_peers
            .write()
            .insert(peer, time::Instant::now());
        // use this placeholder for now. Change this when block storage is ready
        let status = Status::default();
        let message: Message<T, B> = Message::new(MessageBody::Status(status));
        self.send_message(net_sync, peer, &message);
    }

    pub fn on_peer_disconnected(&self, peer: NodeIndex) {
        self.handshaking_peers.write().remove(&peer);
        self.peer_info.write().remove(&peer);
    }

    pub fn sample_peers(&self, num_to_sample: usize) -> Vec<usize> {
        let mut rng = thread_rng();
        let peer_info = self.peer_info.read();
        let owned_peers = peer_info.keys().cloned();
        seq::sample_iter(&mut rng, owned_peers, num_to_sample).unwrap()
    }

    fn on_transaction_message(&self, tx: T) {
        //TODO: communicate to consensus
        let _ = (self.tx_callback)(tx);
    }

    fn on_status_message(&self, net_sync: &mut NetSyncIo, peer: NodeIndex, status: &Status) {
        if status.version != CURRENT_VERSION {
            debug!(target: "sync", "Version mismatch");
            net_sync.report_peer(
                peer,
                Severity::Bad(&format!(
                    "Peer uses incompatible version {}",
                    status.version
                )),
            );
            return;
        }
        let peer_info = PeerInfo {
            request_timestamp: None,
        };
        self.peer_info.write().insert(peer, peer_info);
        self.handshaking_peers.write().remove(&peer);
    }

    fn on_block_request(
        &self,
        _net_sync: &mut NetSyncIo,
        _peer: NodeIndex,
        _request: &BlockRequest,
    ) {
        unimplemented!();
    }

    fn on_block_response<B: Block>(
        &self,
        _net_sync: &mut NetSyncIo,
        _peer: NodeIndex,
        _response: &BlockResponse<B>,
    ) {
        unimplemented!()
    }

    pub fn on_message<B: Block>(&self, net_sync: &mut NetSyncIo, peer: NodeIndex, data: &[u8]) {
        let message: Message<T, B> = match Decode::decode(data) {
            Some(m) => m,
            _ => {
                debug!("cannot decode message: {:?}", data);
                net_sync.report_peer(peer, Severity::Bad("invalid message format"));
                return;
            }
        };
        match message.body {
            MessageBody::Transaction(tx) => self.on_transaction_message(tx),
            MessageBody::Status(status) => self.on_status_message(net_sync, peer, &status),
            MessageBody::BlockRequest(request) => self.on_block_request(net_sync, peer, &request),
            MessageBody::BlockResponse(response) => {
                self.on_block_response(net_sync, peer, &response)
            }
        }
    }

    pub fn send_message<B: Block>(
        &self,
        net_sync: &mut NetSyncIo,
        node_index: NodeIndex,
        message: &Message<T, B>,
    ) {
        match Encode::encode(message) {
            Some(data) => {
                net_sync.send(node_index, data);
            }
            _ => {
                // this should never happen
                error!("FATAL: cannot encode message: {:?}", message);
                return;
            }
        };
    }

    pub fn maintain_peers(&self, net_sync: &mut NetSyncIo) {
        let cur_time = time::Instant::now();
        let mut aborting = Vec::new();
        let peer_info = self.peer_info.read();
        let handshaking_peers = self.handshaking_peers.read();
        for (peer, time_stamp) in peer_info
            .iter()
            .filter_map(|(id, info)| info.request_timestamp.as_ref().map(|x| (id, x)))
            .chain(handshaking_peers.iter())
        {
            if (cur_time - *time_stamp).as_secs() > REQUEST_WAIT {
                trace!(target: "sync", "Timeout {}", *peer);
                aborting.push(*peer);
            }
        }
        for peer in aborting {
            net_sync.report_peer(peer, Severity::Timeout);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use primitives::types;
    use MockBlock;

    impl<T: Transaction> Protocol<T> {
        fn _on_message<B: Block>(&self, data: &[u8]) -> Message<T, B> {
            match Decode::decode(data) {
                Some(m) => m,
                _ => panic!("cannot decode message: {:?}", data),
            }
        }
    }

    #[test]
    fn test_serialization() {
        let tx = types::SignedTransaction::new(0, types::TransactionBody::new(0, 0, 0, 0));
        let message: Message<_, MockBlock> = Message::new(MessageBody::Transaction(tx));
        let config = ProtocolConfig::default();
        let callback = |_| Ok(());
        let protocol = Protocol::new(config, callback);
        let decoded = protocol._on_message(&Encode::encode(&message).unwrap());
        assert_eq!(message, decoded);
    }

}
