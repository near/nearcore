#![allow(unused)]

use client::{chain::Chain, Client};
use error::Error;
use futures::{future, Future};
use futures::stream::Stream;
use libp2p::{Multiaddr, secio};
use message::{Message, MessageBody};
use primitives::hash::CryptoHash;
use primitives::traits::{Block, GenericResult, Header};
use primitives::types;
use protocol::{CURRENT_VERSION, ProtocolConfig, ProtocolHandler};
use rand::Rng;
use service::Service;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use substrate_network_libp2p::{
    NetworkConfiguration, PeerId, ProtocolId, RegisteredProtocol,
    Secret, Service as NetworkService, start_service,
};
use tokio::timer::Interval;

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

pub fn fake_tx_message() -> Message<MockBlock, MockBlockHeader> {
    let tx = types::SignedTransaction::empty();
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

#[derive(Default, Clone, Copy)]
pub struct MockProtocolHandler {}

impl ProtocolHandler for MockProtocolHandler {
    fn handle_transaction(&self, t: types::SignedTransaction) -> GenericResult {
        println!("{:?}", t);
        Ok(())
    }
}

pub fn create_test_services(num_services: u32) -> Vec<Service<MockBlock, MockProtocolHandler>> {
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
    let handler = MockProtocolHandler::default();
    let mock_client = Arc::new(MockClient::default());
    let root_service =
        Service::new(ProtocolConfig::default(), root_config, handler, mock_client.clone()).unwrap();
    let boot_node = addresses[0].clone() + "/p2p/" + &raw_key_to_peer_id_str(secret);
    let mut services = vec![root_service];
    for i in 1..num_services {
        let config = test_config(&addresses[i as usize], vec![boot_node.clone()]);
        let service =
            Service::new(ProtocolConfig::default(), config, handler, mock_client.clone()).unwrap();
        services.push(service);
    }
    services
}

pub fn default_network_service() -> NetworkService {
    let net_config = NetworkConfiguration::default();
    let version = [CURRENT_VERSION as u8];
    let registered = RegisteredProtocol::new(ProtocolId::default(), &version);
    start_service(net_config, Some(registered)).unwrap()
}

pub fn get_noop_network_task() -> impl Future<Item=(), Error=()> {
    Interval::new_interval(Duration::from_secs(1))
        .for_each(|_| Ok(()))
        .then(|_| Ok(()))
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MockBlockHeader {}
#[derive(Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MockBlock {}

impl Header for MockBlockHeader {
    fn hash(&self) -> CryptoHash {
        CryptoHash::default()
    }

    fn index(&self) -> u64 {
        0
    }
    fn parent_hash(&self) -> CryptoHash {
        CryptoHash::default()
    }
}

impl Block for MockBlock {
    type Header = MockBlockHeader;
    type Body = ();
    fn header(&self) -> &Self::Header {
        &MockBlockHeader {}
    }
    fn body(&self) -> &Self::Body {
        &()
    }
    fn deconstruct(self) -> (Self::Header, Self::Body) {
        (MockBlockHeader {}, ())
    }
    fn new(_header: Self::Header, _body: Self::Body) -> Self {
        MockBlock {}
    }
    fn hash(&self) -> CryptoHash {
        CryptoHash::default()
    }
}

#[derive(Default)]
pub struct MockClient {
    pub block: MockBlock,
}

pub struct MockHandler {
    pub client: Arc<Client>,
}

impl ProtocolHandler for MockHandler {
    fn handle_transaction(&self, t: types::SignedTransaction) -> GenericResult {
        self.client.handle_signed_transaction(t)
    }
}

impl Chain<MockBlock> for MockClient {
    fn get_block(&self, id: &types::BlockId) -> Option<MockBlock> {
        Some(self.block.clone())
    }
    fn get_header(&self, id: &types::BlockId) -> Option<MockBlockHeader> {
        Some(self.block.header().clone())
    }
    fn best_hash(&self) -> CryptoHash {
        CryptoHash::default()
    }
    fn best_index(&self) -> u64 {
        0
    }
    fn genesis_hash(&self) -> CryptoHash {
        CryptoHash::default()
    }
    fn import_blocks(&self, blocks: Vec<MockBlock>) {}

    fn prod_block(&self) -> MockBlock {
        MockBlock {}
    }
}
