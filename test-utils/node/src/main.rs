#![allow(unused_mut,unused_variables)]

extern crate env_logger;
extern crate log;
extern crate network;
extern crate substrate_network_libp2p;
extern crate libp2p;
extern crate futures;
extern crate tokio;
extern crate storage;
extern crate primitives;
extern crate parking_lot;
#[macro_use]
extern crate clap;

use env_logger::Builder;
use substrate_network_libp2p::ProtocolId;
use network::{
    service::Service, protocol::ProtocolConfig, test_utils::*, transaction_pool::Pool,
};
use parking_lot::Mutex;
use std::sync::Arc;
use primitives::types;
use clap::{Arg, App};

fn create_addr(host: &str, port: &str) -> String {
    format!("/ip4/{}/tcp/{}", host, port)
}

pub fn main() {
    let mut builder = Builder::new();
    builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();

    // parse command line arguments for now. Will need to switch to use config file in the future.
    let matches = App::new("Client")
        .arg(Arg::with_name("host")
            .long("host")
            .takes_value(true))
        .arg(Arg::with_name("port")
            .long("port")
            .takes_value(true))
        .arg(Arg::with_name("is_root")
            .long("is_root")
            .required(true)
            .takes_value(true))
        .arg(Arg::with_name("root_port")
            .long("root_port")
            .required(true)
            .takes_value(true))
        .get_matches();
    let host = matches.value_of("host").unwrap_or("127.0.0.1");
    let port = matches.value_of("port").unwrap_or("30000");
    let is_root = value_t!(matches, "is_root", bool).unwrap();
    let root_port = matches.value_of("root_port").unwrap();

    let mut storage = storage::Storage::new(format!("storage/db-{}/", port));

    // start network service
    let addr = create_addr(host, port);
    let root_addr = create_addr(host, root_port);
    let net_config;
    if is_root {
        net_config = test_config_with_secret(&addr, vec![], create_secret());
    } else {
        let boot_node = root_addr + "/p2p/" + &raw_key_to_peer_id_str(create_secret());
        println!("boot node: {}", boot_node);
        net_config = test_config(&addr, vec![boot_node]);
    }
    let tx_pool = Arc::new(Mutex::new(Pool::new() as Pool<types::SignedTransaction>));
    let service = match Service::new(ProtocolConfig::default(), net_config, ProtocolId::default(), tx_pool) {
        Ok(s) => s,
        Err(e) => panic!("Error in starting network service: {:?}", e)
    };
}
