extern crate env_logger;
extern crate futures;
extern crate log;
extern crate network;
extern crate parking_lot;
extern crate storage;
extern crate tokio;
#[macro_use]
extern crate clap;
extern crate client;
extern crate primitives;

use clap::{App, Arg};
use client::Client;
use env_logger::Builder;
use network::service::generate_service_task;
use network::{protocol::ProtocolConfig, service::Service, test_utils::*};
use primitives::types::SignedTransaction;
use std::sync::Arc;

fn create_addr(host: &str, port: &str) -> String {
    format!("/ip4/{}/tcp/{}", host, port)
}

pub fn main() {
    let mut builder = Builder::new();
    builder.filter(Some("sub-libp2p"), log::LevelFilter::Debug);
    builder.filter(Some("sync"), log::LevelFilter::Debug);
    builder.filter(Some("main"), log::LevelFilter::Debug);
    builder.filter(None, log::LevelFilter::Info);
    builder.init();

    // parse command line arguments for now. Will need to switch to use config file in the future.
    let matches = App::new("Client")
        .arg(Arg::with_name("host").long("host").takes_value(true))
        .arg(Arg::with_name("port").long("port").takes_value(true))
        .arg(
            Arg::with_name("is_root")
                .long("is_root")
                .required(true)
                .takes_value(true),
        ).arg(
            Arg::with_name("root_port")
                .long("root_port")
                .required(true)
                .takes_value(true),
        ).get_matches();
    let host = matches.value_of("host").unwrap_or("127.0.0.1");
    let port = matches.value_of("port").unwrap_or("30000");
    let is_root = value_t!(matches, "is_root", bool).unwrap();
    let root_port = matches.value_of("root_port").unwrap();

    // start network service
    let addr = create_addr(host, port);
    let root_addr = create_addr(host, root_port);
    let net_config = if is_root {
        test_config_with_secret(&addr, vec![], special_secret())
    } else {
        let boot_node = root_addr + "/p2p/" + &raw_key_to_peer_id_str(special_secret());
        println!("boot node: {}", boot_node);
        test_config(&addr, vec![boot_node])
    };
    let storage = storage::Storage::new(&format!("storage/db-{}/", port));
    let client = Arc::new(Client::new(storage));
    let protocol_config = if is_root {
        ProtocolConfig::new_with_default_id(special_secret())
    } else {
        ProtocolConfig::default()
    };
    let service = Service::new(
        protocol_config,
        net_config,
        MockProtocolHandler::default(),
        client.clone(),
    ).unwrap();
    let task = generate_service_task::<_, MockProtocolHandler, SignedTransaction>(
        service.network.clone(),
        service.protocol.clone(),
    );
    tokio::run(task);
}
