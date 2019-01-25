use clap::{Arg, ArgMatches};
use std::net::SocketAddr;

use crate::ClientConfig;
use ::primitives::{hash::hash_struct, types::PeerId};

const DEFAULT_ADDR: &str = "127.0.0.1:3000";
// const NETWORK_CONFIG_PATH: &str = "storage";

#[derive(Clone)]
pub struct NetworkConfig {
    pub listen_addr: SocketAddr,
    pub peer_id: PeerId,
    pub boot_nodes: Vec<SocketAddr>,
}

impl NetworkConfig {
    pub fn new(addr: &str, peer_id: PeerId, boot_nodes: Vec<String>) -> Self {
        let listen_addr = addr.parse::<SocketAddr>().expect("Cannot parse address");
        let boot_nodes = boot_nodes.iter().map(|n| n.parse::<SocketAddr>().expect("Cannot parse address in boot nodes")).collect();
        NetworkConfig {
            listen_addr,
            peer_id,
            boot_nodes
        }
    }
}

pub fn get_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name("addr")
            .long("addr")
            .value_name("ADDR")
            .help("Address that network service listens on")
            .default_value(DEFAULT_ADDR)
            .takes_value(true),
        Arg::with_name("boot_node")
            .short("b")
            .long("boot-node")
            .value_name("BOOT_NODE")
            .help("Specify a list of boot nodes.")
            .multiple(true)
            .takes_value(true),
        Arg::with_name("test_network_key_seed")
            .long("test-network-key-seed")
            .value_name("TEST_NETWORK_KEY_SEED")
            .help(
                "Specify a seed for generating a node ID.\
                 This should only be used for deterministically \
                 creating node ID's during tests.",
            )
            .takes_value(true),
    ]
}

pub fn from_matches(client_config: &ClientConfig, matches: &ArgMatches) -> NetworkConfig {
    let addr = matches.value_of("addr").unwrap();
    let test_network_key_seed =
        matches.value_of("test_network_key_seed").map(|x| x.parse::<u32>().unwrap());

    let mut boot_nodes: Vec<_> = matches
        .values_of("boot_node")
        .unwrap_or_else(clap::Values::default)
        .map(String::from)
        .collect();
    if boot_nodes.is_empty() {
        boot_nodes = client_config.chain_spec.boot_nodes.to_vec();
    } else if !client_config.chain_spec.boot_nodes.is_empty() {
        // TODO(#222): Maybe return an error here instead of panicking.
        panic!("Boot nodes cannot be specified when chain spec has the boot nodes.");
    }
    NetworkConfig::new(
        addr,
        hash_struct(&test_network_key_seed),
        boot_nodes,
    )
}
