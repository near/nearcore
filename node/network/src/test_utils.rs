#![allow(dead_code)]

use super::MockBlock;
use libp2p::{secio, Multiaddr};
use message::{Message, MessageBody};
use primitives::types;
use protocol::{ProtocolConfig, Transaction};
use rand::Rng;
use service::Service;
use substrate_network_libp2p::{NetworkConfiguration, PeerId, Secret};

pub fn parse_addr(addr: &str) -> Multiaddr {
    addr.parse().expect("cannot parse address")
}

pub fn test_config(addr: &str, boot_nodes: Vec<String>) -> NetworkConfiguration {
    let mut config = NetworkConfiguration::new();
    config.listen_addresses = vec![parse_addr(addr)];
    config.boot_nodes = boot_nodes;
    config
}

pub fn test_config_with_secret(
    addr: &str,
    boot_nodes: Vec<String>,
    secret: Secret,
) -> NetworkConfiguration {
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

pub fn fake_tx_message() -> Message<types::SignedTransaction, MockBlock> {
    let tx = types::SignedTransaction::new(0, types::TransactionBody::new(0, 0, 0, 0));
    Message::new(MessageBody::Transaction(tx))
}

pub fn init_logger(debug: bool) {
    let mut builder = env_logger::Builder::new();
    if debug {
        builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
    }
    builder.filter(None, log::LevelFilter::Info);
    builder.try_init().unwrap_or(());
}

pub fn create_test_services<T: Transaction>(num_services: u32) -> Vec<Service<T>> {
    let base_address = "/ip4/127.0.0.1/tcp/".to_string();
    let base_port = rand::thread_rng().gen_range(30000, 60000);
    let mut addresses = Vec::new();
    for i in 0..num_services {
        let port = base_port + i;
        addresses.push(base_address.clone() + &port.to_string());
    }
    // spin up a root service that does not have bootnodes and
    // have other services have this service as their boot node
    // may want to abstract this out to enable different configurations
    let secret = create_secret();
    let root_config = test_config_with_secret(&addresses[0], vec![], secret);
    let tx_callback = |_| Ok(());
    let root_service =
        Service::new::<MockBlock>(ProtocolConfig::default(), root_config, tx_callback).unwrap();
    let boot_node = addresses[0].clone() + "/p2p/" + &raw_key_to_peer_id_str(secret);
    let mut services = vec![root_service];
    for i in 1..num_services {
        let config = test_config(&addresses[i as usize], vec![boot_node.clone()]);
        let service =
            Service::new::<MockBlock>(ProtocolConfig::default(), config, tx_callback).unwrap();
        services.push(service);
    }
    services
}
