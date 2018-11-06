use libp2p::{Multiaddr, secio};
use substrate_network_libp2p::{Secret, PeerId, NetworkConfiguration};
use message::{Message, MessageBody};
use primitives::{types, traits::{Encode, Decode}};

pub fn parse_addr(addr: &str) -> Multiaddr {
        addr.parse().expect("cannot parse address")
    }

pub fn test_config(addr: &str, boot_nodes: Vec<String>) -> NetworkConfiguration {
    let mut config = NetworkConfiguration::new();
    config.listen_addresses = vec![parse_addr(addr)];
    config.boot_nodes = boot_nodes;
    config
}

pub fn test_config_with_secret(addr: &str, boot_nodes: Vec<String>, secret: Secret)
    -> NetworkConfiguration {
        let mut config = test_config(addr, boot_nodes);
        config.use_secret = Some(secret);
        config
    }

pub fn create_secret() -> Secret {
    let mut secret: Secret = [0; 32];
    secret[31] = 1;
    secret
}

pub fn raw_key_to_peer_id(raw_key: Secret) -> PeerId {
    let secret_key = secio::SecioKeyPair::secp256k1_raw_key(&raw_key)
        .expect("key with correct len should always be valid");
    secret_key.to_peer_id()
}

pub fn raw_key_to_peer_id_str(raw_key: Secret) -> String {
    let peer_id = raw_key_to_peer_id(raw_key);
    peer_id.to_base58()
}

pub fn fake_message() -> Message {
    let tx = types::SignedTransaction::new(0, 0, types::TransactionBody::new(0, 0, 0, 0));
    Message::new(0, 0, "shard0", MessageBody::TransactionMessage(tx))
}