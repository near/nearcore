use clap::{Arg, ArgMatches};
use std::iter;
use std::net::Ipv4Addr;
use std::mem;

use crate::ClientConfig;
use substrate_network_libp2p::{NetworkConfiguration, Protocol, Secret};
use libp2p::Multiaddr;

const DEFAULT_P2P_PORT: &str = "30333";
const NETWORK_CONFIG_PATH: &str = "storage";

pub type NetworkConfig = NetworkConfiguration;

pub fn get_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name("p2p_port")
            .short("p")
            .long("p2p_port")
            .value_name("PORT")
            .help("Specify the p2p protocol TCP port.")
            .default_value(DEFAULT_P2P_PORT)
            .takes_value(true),
        Arg::with_name("boot_node")
            .short("b")
            .long("boot-node")
            .value_name("URL")
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

fn get_multiaddr(ip_addr: Ipv4Addr, port: u16) -> Multiaddr {
    iter::once(Protocol::Ip4(ip_addr)).chain(iter::once(Protocol::Tcp(port))).collect()
}

pub fn get_test_secret_from_network_key_seed(test_network_key_seed: u32) -> Secret {
    // 0 is an invalid secret so we increment all values by 1
    let bytes: [u8; 4] = unsafe { mem::transmute(test_network_key_seed + 1) };

    let mut array = [0; 32];
    for (count, b) in bytes.iter().enumerate() {
        array[array.len() - count - 1] = *b;
    }
    array
}

pub fn from_matches(client_config: &ClientConfig, matches: &ArgMatches) -> NetworkConfig {
    let p2p_port = matches.value_of("p2p_port").map(|x| x.parse::<u16>().unwrap()).unwrap();
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
    let mut network_config = NetworkConfiguration::new();
    let mut network_config_path = client_config.base_path.to_owned();
    network_config_path.push(NETWORK_CONFIG_PATH);
    network_config.net_config_path = Some(network_config_path.to_string_lossy().to_string());
    network_config.boot_nodes = boot_nodes;
    network_config.listen_addresses =
        vec![get_multiaddr(Ipv4Addr::UNSPECIFIED, p2p_port)];

    network_config.use_secret =
        test_network_key_seed.map(get_test_secret_from_network_key_seed);
    network_config
}
