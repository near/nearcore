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

pub mod chain_spec;
pub mod service;
pub mod test_utils;

pub fn run() {
    let matches = App::new("near")
        .arg(
            Arg::with_name("base_path")
                .short("b")
                .long("base-path")
                .value_name("PATH")
                .help("Sets a base path for persisted files.")
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
                .takes_value(true),
        ).arg(
            Arg::with_name("rpc_port")
                .short("r")
                .long("rpc_port")
                .value_name("RPC_PORT")
                .help("Sets the rpc protocol TCP port.")
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
        ).get_matches();

    let base_path = matches
        .value_of("base_path")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));

    let chain_spec_path = matches
        .value_of("chain_spec_file")
        .map(PathBuf::from);

    let p2p_port = matches
        .value_of("p2p_port")
        .map(|x| x.parse::<u16>().unwrap());

    let rpc_port = matches
        .value_of("rpc_port")
        .map(|x| x.parse::<u16>().unwrap());

    let test_node_index = matches
        .value_of("test_node_index")
        .map(|x| x.parse::<u32>().unwrap());

    let config = service::ServiceConfig {
        base_path,
        chain_spec_path,
        p2p_port,
        rpc_port,
        test_node_index,
    };
    service::start_service(
        config,
        test_utils::spawn_pasthrough_consensus,
    );
}
