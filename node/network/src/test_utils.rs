#![allow(unused)]

use libp2p::{secio, Multiaddr};
use substrate_network_libp2p::{NetworkConfiguration, PeerId, Secret};

use client::Client;
use error::Error;
use message::{Message, MessageBody};
use primitives::hash::CryptoHash;
use primitives::traits::{Block, Header};
use primitives::types;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MockBlockHeader {}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MockBlock {}

impl Header for MockBlockHeader {
    fn hash(&self) -> CryptoHash {
        CryptoHash { 0: [0; 32] }
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
        CryptoHash { 0: [0; 32] }
    }
}

pub struct MockClient {}

impl Client<MockBlock> for MockClient {
    fn get_block(&self, id: &types::BlockId) -> Result<MockBlock, Error> {
        Ok(MockBlock {})
    }
    fn get_header(&self, id: &types::BlockId) -> Result<MockBlockHeader, Error> {
        Ok(MockBlockHeader {})
    }
}
