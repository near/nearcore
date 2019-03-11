use std::mem;
use std::net::SocketAddr;
use std::time::Duration;

use clap::{Arg, ArgMatches};

use crate::ClientConfig;
use primitives::network::PeerAddr;
use primitives::{hash::hash, types::PeerId};

const DEFAULT_RECONNECT_DELAY_MS: &str = "50";
const DEFAULT_GOSSIP_INTERVAL_MS: &str = "50";
const DEFAULT_GOSSIP_SAMPLE_SIZE: &str = "10";
// const NETWORK_CONFIG_PATH: &str = "storage";

#[derive(Clone)]
pub struct NetworkConfig {
    pub listen_addr: Option<SocketAddr>,
    pub peer_id: PeerId,
    pub boot_nodes: Vec<PeerAddr>,
    pub reconnect_delay: Duration,
    pub gossip_interval: Duration,
    pub gossip_sample_size: usize,
}

pub fn get_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name("addr")
            .long("addr")
            .value_name("ADDR")
            .help("Address that network service listens on")
            .takes_value(true),
        Arg::with_name("boot_nodes")
            .short("b")
            .long("boot-nodes")
            .value_name("BOOT_NODES")
            .help(
                "Specify a list of boot node. In the form:\
                    --boot_nodes <ip1>:<port1>/<node-id1>
                    --boot_nodes <ip2>:<port2>/<node-id2>
            ",
            )
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
        Arg::with_name("reconnect_delay_ms")
            .long("reconnect-delay-ms")
            .value_name("RECONNECT_DELAY_MS")
            .help("Delay in ms before we (re)connect to a known peer.")
            .default_value(DEFAULT_RECONNECT_DELAY_MS)
            .takes_value(true),
        Arg::with_name("gossip_interval_ms")
            .long("gossip-interval-ms")
            .value_name("GOSSIP_INTERVAL_MS")
            .help("Delay in ms between gossiping peers info with known peers.")
            .default_value(DEFAULT_GOSSIP_INTERVAL_MS)
            .takes_value(true),
        Arg::with_name("tx_gossip_interval_ms")
            .long("tx-gossip-interval-ms")
            .value_name("TRANSACTIONS GOSSIP_INTERVAL_MS")
            .help("Delay in ms between gossiping transactions to peers.")
            .default_value(DEFAULT_GOSSIP_INTERVAL_MS)
            .takes_value(true),
        Arg::with_name("gossip_sample_size")
            .long("gossip-sample-size")
            .value_name("GOSSIP_SAMPLE_SIZE")
            .help("Delay in ms between gossiping peers info with known peers.")
            .default_value(DEFAULT_GOSSIP_SAMPLE_SIZE)
            .takes_value(true),
    ]
}

pub fn get_peer_id_from_seed(seed: u32) -> PeerId {
    let bytes: [u8; 4] = unsafe { mem::transmute(seed) };

    let mut array = [0; 32];
    for (count, b) in bytes.iter().enumerate() {
        array[array.len() - count - 1] = *b;
    }
    hash(&array)
}

pub fn from_matches(client_config: &ClientConfig, matches: &ArgMatches) -> NetworkConfig {
    let listen_addr =
        matches.value_of("addr").map(|value| value.parse::<SocketAddr>().expect("Cannot parse address"));
    let test_network_key_seed =
        matches.value_of("test_network_key_seed").map(|x| x.parse::<u32>().unwrap()).unwrap_or(0);

    let parsed_boot_nodes =
        matches.values_of("boot_nodes").unwrap_or_else(clap::Values::default).map(String::from);
    let mut boot_nodes: Vec<_> = parsed_boot_nodes
        .map(|addr_id| {
            PeerAddr::parse(&addr_id).expect("Cannot parse address")
        })
        .clone()
        .collect();

    let reconnect_delay_ms =
        matches.value_of("reconnect_delay_ms").map(|x| x.parse::<u64>().unwrap()).unwrap();
    let gossip_interval_ms =
        matches.value_of("gossip_interval_ms").map(|x| x.parse::<u64>().unwrap()).unwrap();
    let gossip_sample_size =
        matches.value_of("gossip_sample_size").map(|x| x.parse::<usize>().unwrap()).unwrap();

    if boot_nodes.is_empty() {
        boot_nodes = client_config.chain_spec.boot_nodes.to_vec();
    } else if !client_config.chain_spec.boot_nodes.is_empty() {
        // TODO(#222): Maybe return an error here instead of panicking.
        panic!("Boot nodes cannot be specified when chain spec has the boot nodes.");
    }
    let peer_id = get_peer_id_from_seed(test_network_key_seed);
    if listen_addr.is_some() {
        println!("To boot from this node: {}/{}", listen_addr.unwrap(), String::from(&peer_id));
    }
    NetworkConfig {
        listen_addr,
        peer_id,
        boot_nodes,
        reconnect_delay: Duration::from_millis(reconnect_delay_ms),
        gossip_interval: Duration::from_millis(gossip_interval_ms),
        gossip_sample_size,
    }
}
