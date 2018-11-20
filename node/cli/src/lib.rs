extern crate client;
extern crate network;
extern crate primitives;
extern crate service;

use client::Client;
use network::protocol::ProtocolConfig;
use network::service::NetworkConfiguration;
use network::service::Service as NetworkService;
use network::MockBlock;
use primitives::types::SignedTransaction;
use service::network_handler::NetworkHandler;
use service::run_service;
use std::sync::Arc;

pub fn run() {
    let client = Arc::new(Client::default());
    let network_handler = NetworkHandler {
        client: client.clone(),
    };
    let network = NetworkService::new::<MockBlock>(
        ProtocolConfig::default(),
        NetworkConfiguration::default(),
        network_handler,
    ).unwrap();
    run_service::<SignedTransaction, NetworkHandler>(client.clone(), &network);
}
