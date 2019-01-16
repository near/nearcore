#![allow(unused)]

extern crate storage;

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::Stream;
use futures::sync::mpsc::channel;
use futures::{future, Future};
use libp2p::{secio, Multiaddr};
use parking_lot::RwLock;
use rand::Rng;
use substrate_network_libp2p::{
    start_service, NetworkConfiguration, PeerId, ProtocolId, RegisteredProtocol, Secret,
    Service as NetworkService,
};
use tokio::timer::Interval;

use self::storage::test_utils::create_memory_db;
use crate::error::Error;
use crate::message::Message;
use crate::protocol::{Protocol, ProtocolConfig, CURRENT_VERSION};
use beacon::authority::{AuthorityConfig, AuthorityStake};
use beacon::types::{BeaconBlockChain, SignedBeaconBlock, SignedBeaconBlockHeader};
use chain::{SignedBlock, SignedHeader};
use primitives::hash::{hash_struct, CryptoHash};
use primitives::signature::get_key_pair;
use primitives::traits::GenericResult;
use primitives::types;
use client::test_utils::get_client;

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

pub fn special_secret() -> Secret {
    let mut secret: Secret = [1; 32];
    secret[31] = 0;
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

pub fn fake_tx_message() -> Message {
    let tx = types::SignedTransaction::empty();
    Message::Transaction(Box::new(tx))
}

pub fn init_logger(debug: bool) {
    let mut builder = env_logger::Builder::new();
    if debug {
        builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
        builder.filter(Some("network"), log::LevelFilter::Debug);
    }
    builder.filter(None, log::LevelFilter::Info);
    builder.try_init().unwrap_or(());
}

pub fn default_network_service() -> NetworkService {
    let net_config = NetworkConfiguration::default();
    let version = [CURRENT_VERSION as u8];
    let registered = RegisteredProtocol::new(ProtocolId::default(), &version);
    start_service(net_config, Some(registered)).unwrap()
}

pub fn get_noop_network_task() -> impl Future<Item = (), Error = ()> {
    Interval::new_interval(Duration::from_secs(1)).for_each(|_| Ok(())).then(|_| Ok(()))
}

pub fn get_test_protocol() -> Protocol {
    let (block_tx, _) = channel(1024);
    let (transaction_tx, _) = channel(1024);
    let (message_tx, _) = channel(1024);
    let (gossip_tx, _) = channel(1024);
    let client = Arc::new(get_client());
    Protocol::new(ProtocolConfig::default(), client, block_tx, transaction_tx, message_tx, gossip_tx)
}

pub fn get_test_authority_config(
    num_authorities: u32,
    epoch_length: u64,
    num_seats_per_slot: u64,
) -> AuthorityConfig {
    let mut initial_authorities = vec![];
    for i in 0..num_authorities {
        let (public_key, _) = get_key_pair();
        initial_authorities.push(AuthorityStake {
            account_id: i.to_string(),
            public_key,
            amount: 100,
        });
    }
    AuthorityConfig { initial_proposals: initial_authorities, epoch_length, num_seats_per_slot }
}
