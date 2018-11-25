extern crate beacon;
extern crate client;
extern crate network;
extern crate primitives;
extern crate service;
extern crate storage;

use beacon::types::BeaconBlockHeader;
use client::Client;
use network::protocol::ProtocolConfig;
use network::service::NetworkConfiguration;
use network::service::Service as NetworkService;
use service::network_handler::NetworkHandler;
use service::run_service;
use std::sync::Arc;
use storage::{DiskStorage, Storage};

pub fn run() {
    // TODO: add argument parsing into service/config.rs.
    let storage: Arc<Storage> = Arc::new(DiskStorage::new("storage/db/"));
    let client = Arc::new(Client::new(storage));
    let network_handler = NetworkHandler {
        client: client.clone(),
    };
    let network = NetworkService::new(
        ProtocolConfig::default(),
        NetworkConfiguration::default(),
        network_handler,
        client.clone(),
    ).unwrap();
    run_service::<_, _, BeaconBlockHeader>(client.clone(), &network);
}
