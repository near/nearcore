extern crate beacon;
#[macro_use]
extern crate clap;
extern crate client;
extern crate env_logger;
extern crate futures;
extern crate log;
extern crate network;
extern crate node_cli;
extern crate parking_lot;
extern crate primitives;
extern crate service;
extern crate storage;
extern crate tokio;

use beacon::types::BeaconBlockHeader;
use clap::{App, Arg};
use client::Client;
use env_logger::Builder;
use futures::{Future, Stream};
use network::service::generate_service_task;
use network::{protocol::ProtocolConfig, service::Service, test_utils::*};
use node_cli::chain_spec::get_default_chain_spec;
use primitives::signer::InMemorySigner;
use primitives::types::{SignedTransaction, TransactionBody};
use service::network_handler::NetworkHandler;
use std::sync::Arc;
use std::time::Duration;
use tokio::timer::Interval;

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
        .arg(Arg::with_name("is_root").long("is_root").required(true).takes_value(true))
        .arg(Arg::with_name("root_port").long("root_port").required(true).takes_value(true))
        .get_matches();
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
    let chain_spec = get_default_chain_spec().unwrap();
    let storage = Arc::new(storage::test_utils::create_memory_db());
    let signer = Arc::new(InMemorySigner::new());
    let client = Arc::new(Client::new(&chain_spec, storage, signer));
    let protocol_config = if is_root {
        ProtocolConfig::new_with_default_id(special_secret())
    } else {
        ProtocolConfig::default()
    };
    let network_handler = NetworkHandler { client: client.clone() };
    let service =
        Service::new(protocol_config, net_config, network_handler, client.clone()).unwrap();
    let task = generate_service_task::<_, _, BeaconBlockHeader>(
        service.network.clone(),
        service.protocol.clone(),
    );
    // produce some fake transactions once in a while
    let tx_period = Duration::from_millis(1000);
    let fake_tx_task = Interval::new_interval(tx_period)
        .for_each({
            let client = client.clone();
            move |_| {
                let tx_body = TransactionBody {
                    nonce: 1,
                    amount: 1,
                    sender: 1,
                    receiver: 2,
                    method_name: String::new(),
                    args: vec![],
                };
                let tx = SignedTransaction::new(123, tx_body);
                client.receive_transaction(tx);
                Ok(())
            }
        }).map_err(|_| ());
    let task = task.select(fake_tx_task).then(|_| Ok(()));
    tokio::run(task);
}
