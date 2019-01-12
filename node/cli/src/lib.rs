extern crate beacon;
extern crate beacon_chain_handler;
extern crate chain;
extern crate clap;
extern crate client;
extern crate consensus;
extern crate env_logger;
extern crate futures;
extern crate network;
extern crate node_http;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate serde;
extern crate serde_derive;
#[cfg(test)]
extern crate serde_json;
extern crate shard;
extern crate storage;
extern crate tokio;
extern crate txflow;

use clap::{App, Arg};
use std::path::PathBuf;
use std::str::FromStr;
use txflow::txflow_task::beacon_witness_selector::BeaconWitnessSelector;
use primitives::types::ChainPayload;
use client::{chain_spec, ClientConfig, DEFAULT_LOG_LEVEL, DEFAULT_BASE_PATH};

pub mod service;

use service::NetworkConfig;

pub fn get_service_configs() -> (NetworkConfig, ClientConfig) {
    let default_p2p_port = service::DEFAULT_P2P_PORT.to_string();
    let default_rpc_port = service::DEFAULT_RPC_PORT.to_string();
    let default_log_level = DEFAULT_LOG_LEVEL.to_string().to_lowercase();
    let matches = App::new("near")
        .arg(
            Arg::with_name("base_path")
                .short("d")
                .long("base-path")
                .value_name("PATH")
                .help("Specify a base path for persisted files.")
                .default_value(DEFAULT_BASE_PATH)
                .takes_value(true),
        ).arg(
            Arg::with_name("chain_spec_file")
                .short("c")
                .long("chain-spec-file")
                .value_name("CHAIN_SPEC")
                .help("Specify a file location to read a custom chain spec.")
                .takes_value(true),
        ).arg(
            Arg::with_name("rpc_port")
                .short("r")
                .long("rpc_port")
                .value_name("PORT")
                .help("Specify the rpc protocol TCP port.")
                .default_value(&default_rpc_port)
                .takes_value(true),
        ).arg(
            Arg::with_name("p2p_port")
                .short("p")
                .long("p2p_port")
                .value_name("PORT")
                .help("Specify the p2p protocol TCP port.")
                .default_value(&default_p2p_port)
                .takes_value(true),
        ).arg(
            Arg::with_name("boot_node")
                .short("b")
                .long("boot-node")
                .value_name("URL")
                .help("Specify a list of boot nodes.")
                .multiple(true)
                .takes_value(true),
        ).arg(
            Arg::with_name("test_network_key_seed")
                .long("test-network-key-seed")
                .value_name("TEST_NETWORK_KEY_SEED")
                .help(
                    "Specify a seed for generating a node ID.\
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
        ).arg(
            Arg::with_name("account_id")
            .value_name("ACCOUNT_ID")
            .help("Set the account id of the node")
            .takes_value(true)
                // TODO(#282): Remove default account id from here.
            .default_value("alice.near")
        ).arg(
            Arg::with_name("public_key")
              .short("k")
              .long("public-key")
              .value_name("PUBLIC_KEY")
              .help("Sets public key to sign with, \
                         can be omitted with 1 file in keystore")
              .takes_value(true)
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

    let test_network_key_seed = matches
        .value_of("test_network_key_seed")
        .map(|x| x.parse::<u32>().unwrap());

    let log_level = matches
        .value_of("log_level")
        .map(log::LevelFilter::from_str)
        .unwrap()
        .unwrap();

    let boot_nodes = matches
        .values_of("boot_node")
        .unwrap_or_else(clap::Values::default)
        .map(String::from)
        .collect();

    let account_id = matches
        .value_of("account_id")
        .map(String::from)
        .unwrap();

    let public_key = matches
        .value_of("public_key")
        .map(String::from);

    (NetworkConfig {
        p2p_port,
        rpc_port,
        boot_nodes,
        test_network_key_seed,
    },
    ClientConfig {
        base_path,
        account_id,
        public_key,
        chain_spec_path,
        log_level,
    })
}

pub fn run() {
    let (network_cfg, client_cfg) = get_service_configs();
    service::start_service(
        network_cfg,
        client_cfg,
        txflow::txflow_task::spawn_task::<ChainPayload, BeaconWitnessSelector>
    );
}
