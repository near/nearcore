extern crate beacon;
extern crate clap;
extern crate client;
extern crate network;
extern crate node_runtime;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate service;
extern crate storage;
#[macro_use]
extern crate log;

use beacon::types::BeaconBlockHeader;
use chain_spec::{deserialize_chain_spec, get_default_chain_spec};
use clap::{App, Arg};
use client::Client;
use network::protocol::ProtocolConfig;
use network::service::{
    generate_service_task, NetworkConfiguration, Service as NetworkService
};
use primitives::signer::InMemorySigner;
use primitives::traits::{GenericResult, Signer};
use network::network_handler::NetworkHandler;
use service::run_service;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub mod chain_spec;

fn get_storage_path(base_path: &Path) -> PathBuf {
    let mut path = base_path.to_owned();
    path.push("storage/db");
    path
}

fn start_service(base_path: &Path, chain_spec_path: Option<&Path>) -> GenericResult {
    let chain_spec = match chain_spec_path {
        Some(path) => {
            let mut file = File::open(path).expect("could not open chain spec file");

            let mut contents = String::new();
            file.read_to_string(&mut contents).expect("could not read from chain spec file");

            deserialize_chain_spec(&contents)
        }
        None => get_default_chain_spec(),
    }.unwrap();

    let signer = Arc::new(InMemorySigner::new());
    info!("Public key: {}", signer.public_key());
    let storage_path = get_storage_path(base_path);
    let storage = Arc::new(storage::open_database(&storage_path.to_string_lossy()));
    let client = Arc::new(Client::new(&chain_spec, storage, signer));
    let network_handler = NetworkHandler { client: client.clone() };
    let network = NetworkService::new(
        ProtocolConfig::default(),
        NetworkConfiguration::default(),
        network_handler,
        client.clone(),
    ).unwrap();
    let network_task = generate_service_task::<_, _, BeaconBlockHeader>(
        network.network.clone(),
        network.protocol.clone(),
    );
    let produce_blocks_interval = Duration::from_secs(2);
    run_service(&client, network_task, produce_blocks_interval)
}

pub fn run() {
    let matches = App::new("near")
        .arg(
            Arg::with_name("base_path")
                .short("b")
                .long("base-path")
                .value_name("PATH")
                .help("Sets a base path for persisted files")
                .takes_value(true),
        ).arg(
            Arg::with_name("chain_spec_file")
                .short("c")
                .long("chain-spec-file")
                .value_name("CHAIN_SPEC_FILE")
                .help("Sets a file location to read a custom chain spec")
                .takes_value(true),
        ).get_matches();

    let base_path =
        matches.value_of("base_path").map(|x| Path::new(x)).unwrap_or_else(|| Path::new("."));

    let chain_spec_path = matches.value_of("chain_spec_file").map(|x| Path::new(x));

    start_service(base_path, chain_spec_path).unwrap();
}
