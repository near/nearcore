extern crate beacon;
extern crate clap;
extern crate client;
extern crate network;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate service;
extern crate storage;

use beacon::types::BeaconBlockHeader;
use clap::{App, Arg};
use client::Client;
use network::protocol::ProtocolConfig;
use network::service::NetworkConfiguration;
use network::service::Service as NetworkService;
use service::network_handler::NetworkHandler;
use service::run_service;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use storage::{DiskStorage, Storage};

mod chain_spec;

fn get_storage_path(base_path: &Path) -> PathBuf {
    let mut path = base_path.to_owned();
    path.push("storage/db");
    path
}

fn start_service(base_path: &Path) {
    let storage_path = get_storage_path(base_path);
    let storage: Arc<Storage> = Arc::new(
        DiskStorage::new(&storage_path.to_string_lossy())
    );
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

pub fn run() {
    let matches = App::new("near")
        .arg(
            Arg::with_name("base_path")
                .short("b")
                .long("base-path")
                .value_name("PATH")
                .help("Sets a custom config file")
                .takes_value(true),
        ).get_matches();

    let base_path = matches
        .value_of("base_path")
        .map(|x| Path::new(x))
        .unwrap_or_else(|| Path::new("."));

    start_service(base_path);
}
