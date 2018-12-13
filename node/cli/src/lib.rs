extern crate beacon;
extern crate beacon_chain_handler;
extern crate chain;
extern crate clap;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate log;
extern crate network;
extern crate node_rpc;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate shard;
extern crate storage;
extern crate tokio;

use clap::{App, Arg};
use std::path::PathBuf;
use std::str::FromStr;

pub mod chain_spec;
pub mod service;
pub mod test_utils;

pub fn get_service_config() -> service::ServiceConfig {
    let default_p2p_port = service::DEFAULT_P2P_PORT.to_string();
    let default_rpc_port = service::DEFAULT_RPC_PORT.to_string();
    let default_log_level = service::DEFAULT_LOG_LEVEL.to_string().to_lowercase();
    let matches = App::new("near")
        .arg(
            Arg::with_name("base_path")
                .short("b")
                .long("base-path")
                .value_name("PATH")
                .help("Sets a base path for persisted files.")
                .default_value(service::DEFAULT_BASE_PATH)
                .takes_value(true),
        ).arg(
            Arg::with_name("chain_spec_file")
                .short("c")
                .long("chain-spec-file")
                .value_name("CHAIN_SPEC_FILE")
                .help("Sets a file location to read a custom chain spec.")
                .takes_value(true),
        ).arg(
            Arg::with_name("p2p_port")
                .short("p")
                .long("p2p_port")
                .value_name("P2P_PORT")
                .help("Sets the p2p protocol TCP port.")
                .default_value(&default_p2p_port)
                .takes_value(true),
        ).arg(
            Arg::with_name("rpc_port")
                .short("r")
                .long("rpc_port")
                .value_name("RPC_PORT")
                .help("Sets the rpc protocol TCP port.")
                .default_value(&default_rpc_port)
                .takes_value(true),
        ).arg(
            Arg::with_name("test_node_index")
                .long("test-node-index")
                .value_name("TEST_NODE_INDEX")
                .help(
                    "Used as a seed for generating a node ID.\
                     This should only be used for deterministically \
                     creating node ID's during tests.",
                )
                .takes_value(true),
        ).arg(
           Arg::with_name("log_level")
                .short("l")
                .long("log-level")
                .value_name("LOG_LEVEL")
                .help("Set log level. Can override specific targets with RUST_LOG.")
                .possible_values(&[
                    &log::LevelFilter::Debug.to_string().to_lowercase(),
                    &log::LevelFilter::Info.to_string().to_lowercase(),
                    &log::LevelFilter::Warn.to_string().to_lowercase(),
                ])
                .default_value(&default_log_level)
                .takes_value(true),
        ).get_matches();

    let base_path = matches
        .value_of("base_path")
        .map(PathBuf::from)
        .unwrap();

    let chain_spec_path = matches
        .value_of("chain_spec_file")
        .map(PathBuf::from);

    let p2p_port = matches
        .value_of("p2p_port")
        .map(|x| x.parse::<u16>().unwrap())
        .unwrap();

    let rpc_port = matches
        .value_of("rpc_port")
        .map(|x| x.parse::<u16>().unwrap())
        .unwrap();

    let test_node_index = matches
        .value_of("test_node_index")
        .map(|x| x.parse::<u32>().unwrap());

    let log_level = matches
        .value_of("log_level")
        .map(log::LevelFilter::from_str)
        .unwrap()
        .unwrap();

    service::ServiceConfig {
        base_path,
        chain_spec_path,
        log_level,
        p2p_port,
        rpc_port,
        test_node_index,
    }
}

pub fn run() {
    let config = get_service_config();
    service::start_service(
        config,
        test_utils::spawn_pasthrough_consensus,
    );
}
