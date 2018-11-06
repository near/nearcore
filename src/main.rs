extern crate env_logger;
#[macro_use]
extern crate log;
extern crate network;
extern crate substrate_network_libp2p;
extern crate libp2p;
extern crate futures;
extern crate tokio;
extern crate storage;
extern crate primitives;

mod example;

use env_logger::Builder;
use substrate_network_libp2p::ProtocolId;
use network::{service::Service, protocol::ProtocolConfig, test_utils::*};

pub fn main() {
    let mut storage = storage::Storage::new("storage/db/".to_string());
    let mut builder = Builder::new();
    builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();
    // start network service
    let addr = "/ip4/127.0.0.1/tcp/30000";
    let net_config = test_config_with_secret(addr, vec![], create_secret());
    let service = match Service::new(ProtocolConfig::default(), net_config, ProtocolId::default()) {
        Ok(s) => s,
        Err(e) => panic!("Error in starting network service: {:?}", e)
    };
}