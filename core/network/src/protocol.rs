/// network protocol

use substrate_network_libp2p::{Service, NodeIndex, ProtocolId};
use primitives::{types, traits::{Encode, Decode}};
use message::{self, Message};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::collections::HashMap;
use rand::{thread_rng, sample};

pub struct ProtocolConfig {
    // config information goes here
    pub version: String,
    pub protocol_id: ProtocolId,
}

impl ProtocolConfig {
    pub fn new(version: &'static str, protocol_id: ProtocolId) -> ProtocolConfig {
        ProtocolConfig {
            version: version.to_string(),
            protocol_id
        }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        ProtocolConfig::new("1.0", ProtocolId::default())
    }
}

struct PeerInfo {
    // information about connected peers
}

impl PeerInfo {
    pub fn new() -> PeerInfo {
        PeerInfo {}
    }
}

pub struct Protocol {
    // TODO: add more fields when we need them
    config: ProtocolConfig,
    peers: RwLock<HashMap<NodeIndex, PeerInfo>>,
}

impl Protocol {
    pub fn new(config: ProtocolConfig) -> Protocol {
        Protocol {
            config,
            peers: RwLock::new(HashMap::new())
        }
    }

    pub fn on_peer_connected(&self, peer: NodeIndex) {
        self.peers.write().insert(peer, PeerInfo::new());
    }

    pub fn on_peer_disconnected(&self, peer: NodeIndex) {
        self.peers.write().remove(&peer);
    }

    pub fn sample_peers(&self, num_to_sample: usize) -> Vec<usize> {
        let mut rng = thread_rng();
        let peers = self.peers.read();
        //let mut owned_peers = Vec::new();
        //for peer in peers.keys() {
        //    owned_peers.push(*peer);
        //}
        let owned_peers = peers.keys().map(|x| *x);
        sample(&mut rng, owned_peers, num_to_sample)
    }
    
    fn on_transaction_message(&self, tx: types::SignedTransaction) {
        debug!("TODO: transaction message");
    }

    pub fn on_message(&self, network: &Arc<Mutex<Service>>, data: &[u8]) {
        let message: Message = match Decode::decode(data) {
            Some(m) => m,
            _ => {
                error!("cannot decode message: {:?}", data);
                return;
            }
        };
        match message.body {
            message::MessageBody::TransactionMessage(tx) => self.on_transaction_message(tx)
        }
    }

    pub fn send_message(&self, network: &Arc<Mutex<Service>>, node_index: NodeIndex, message: Message) {
        let data = match Encode::encode(&message) {
            Some(d) => d,
            _ => {
                error!("cannot encode message: {:?}", message);
                return
            }
        };
        network.lock().send_custom_message(node_index, self.config.protocol_id, data);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    impl Protocol {
        fn _on_message(&self, data: &[u8]) -> Message {
            match Decode::decode(data) {
                Some(m) => m,
                _ => panic!("cannot decode message: {:?}", data)
            }
        }
    }

    #[test]
    fn test_serialization() {
        let tx = types::SignedTransaction::new(0, 0, types::TransactionBody::new(0, 0, 0, 0));
        let message = message::Message::new(
            0, 0, "shard0", message::MessageBody::TransactionMessage(tx)
        );
        let config = ProtocolConfig::default();
        let protocol = Protocol::new(config);
        let decoded = protocol._on_message(&Encode::encode(&message).unwrap());
        assert_eq!(message, decoded);
    }

}