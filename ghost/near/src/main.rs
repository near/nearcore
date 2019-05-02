use std::sync::Arc;

use actix::{Actor, AsyncContext, System};
use chrono::{DateTime, Utc};
use log::LevelFilter;

use near_chain::test_utils::KeyValueRuntime;
use near_chain::{Block, BlockHeader, BlockStatus, Chain, Provenance, RuntimeAdapter};
use near_client::{BlockProducer, ClientActor, ClientConfig};
use near_network::{test_utils::convert_boot_nodes, NetworkConfig, PeerInfo, PeerManagerActor};
use near_store::test_utils::create_test_store;

use clap::{App, Arg};
use near::{start_with_config, NearConfig};

fn init_logging(verbose: bool) {
    if verbose {
        env_logger::Builder::new()
            .filter_module("tokio_reactor", LevelFilter::Info)
            .filter(None, LevelFilter::Debug)
            .init();
    } else {
        env_logger::Builder::new()
            .filter_module("tokio_reactor", LevelFilter::Info)
            .filter(Some("info"), LevelFilter::Info)
            .filter(None, LevelFilter::Warn)
            .init();
    }
}

fn main() {
    let matches = App::new("Nearmint")
        .args(&[Arg::with_name("verbose")
            .long("verbose")
            .short("verbose")
            .help("Verbose logging")
            .takes_value(false)])
        .get_matches();

    init_logging(matches.is_present("verbose"));
    // TODO: implement flags parsing here and reading config from NEARHOME env or base-dir flag.
    let genesis_timestamp = Utc::now();
    let near = NearConfig::new(genesis_timestamp.clone(), "test", 25123);

    let system = System::new("NEAR");
    start_with_config(near);
    system.run().unwrap();
}
