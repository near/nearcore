use message::{Message, MessageBody, Status};
use parking_lot::{Mutex, RwLock};
use primitives::traits::{Decode, Encode};
use rand::{seq, thread_rng};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time;
/// network protocol
use substrate_network_libp2p::{NodeIndex, ProtocolId, Service as NetworkService};

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

pub trait Transaction: Send + Sync + Serialize + DeserializeOwned + Debug + Clone + 'static {}
impl<T> Transaction for T where T: Send + Sync + Serialize + DeserializeOwned + Debug + Clone + 'static {}

pub struct Protocol {
    // TODO: add more fields when we need them
    pub(crate) config: ProtocolConfig,
    // peers that are in the handshaking process
    pub(crate) handshaking_peers: RwLock<HashMap<NodeIndex, time::Instant>>,
    // info about peers
    pub(crate) peer_info: RwLock<HashMap<NodeIndex, PeerInfo>>,
}

impl Protocol  {
    pub fn new(config: ProtocolConfig) -> Protocol {
        Protocol {
            config,
            handshaking_peers: RwLock::new(HashMap::new()),
            peer_info: RwLock::new(HashMap::new()),
        }
    }

    pub fn on_peer_connected<T: Transaction>(&self, network: &Arc<Mutex<NetworkService>>, peer: NodeIndex) {
        self.handshaking_peers.write().insert(peer, time::Instant::now());
        let status = Status {
            version: CURRENT_VERSION,
        };
        let message: Message<T> = Message::new(MessageBody::Status(status));
        self.send_message(network, peer, message);
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
    
    #[allow(unused_variables)]
    fn on_transaction_message<T: Transaction>(&self, tx: T) {
        //TODO: communicate to consensus
        unimplemented!("TODO: on_transaction_message");
    }

    fn on_status_message(&self, peer: NodeIndex, status: &Status) {
        if status.version != CURRENT_VERSION {
            debug!(target: "sync", "Version mismatch");
            return;
        }
        let peer_info = PeerInfo {
            request_timestamp: None,
        };
        self.peer_info.write().insert(peer, peer_info);
        self.handshaking_peers.write().remove(&peer);
    }

    pub fn on_message<T: Transaction>(&self, peer: NodeIndex, data: &[u8]) {
        let message: Message<T> = match Decode::decode(data) {
            Some(m) => m,
            _ => {
                error!("cannot decode message: {:?}", data);
                return;
            }
        };
        match message.body {
            MessageBody::Transaction(tx) => self.on_transaction_message(tx),
            MessageBody::Status(status) => self.on_status_message(peer, &status),
        }
    }

    pub fn send_message<T: Transaction>(&self, network: &Arc<Mutex<NetworkService>>, node_index: NodeIndex, message: Message<T>) {
        let data = match Encode::encode(&message) {
            Some(d) => d,
            _ => {
                error!("cannot encode message: {:?}", message);
                return;
            }
        };
        network
            .lock()
            .send_custom_message(node_index, self.config.protocol_id, data);
    }

    pub fn maintain_peers(&self, network: &Arc<Mutex<NetworkService>>) {
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
            network.lock().drop_node(peer);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use primitives::types;
    use transaction_pool::Pool;

    impl Protocol {
        fn _on_message<T: Transaction>(&self, data: &[u8]) -> Message<T> {
            match Decode::decode(data) {
                Some(m) => m,
                _ => panic!("cannot decode message: {:?}", data),
            }
        }
    }

    #[test]
    fn test_serialization() {
        let tx = types::SignedTransaction::new(0, types::TransactionBody::new(0, 0, 0, 0));
        let message = Message::new(MessageBody::Transaction(tx));
        let config = ProtocolConfig::default();
        let protocol = Protocol::new(config);
        let decoded = protocol._on_message(&Encode::encode(&message).unwrap());
        assert_eq!(message, decoded);
    }

}
