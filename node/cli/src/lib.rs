extern crate client;
extern crate network;
extern crate primitives;
extern crate service;
extern crate storage;

use client::Client;
use network::protocol::ProtocolConfig;
use network::service::NetworkConfiguration;
use network::service::Service as NetworkService;
use network::test_utils::MockBlock;
use network::test_utils::MockClient;
use primitives::types::SignedTransaction;
use service::network_handler::NetworkHandler;
use service::run_service;
use std::sync::Arc;
use storage::Storage;

pub fn run() {
    let storage = Storage::new("storage/db/");
    let client = Arc::new(Client::new(storage));
    let network_handler = NetworkHandler {
        client: client.clone(),
    };
    let network_client = Arc::new(MockClient::default());
    let network = NetworkService::new(
        ProtocolConfig::default(),
        NetworkConfiguration::default(),
        network_handler,
        network_client,
    ).unwrap();
    run_service::<MockBlock, SignedTransaction, NetworkHandler>(client.clone(), &network);
}
