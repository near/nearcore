use rand;
use std::convert::TryFrom;
use std::mem;
use std::net::SocketAddr;
use std::time::Duration;

use clap::{Arg, ArgMatches};

use primitives::hash::{hash, CryptoHash};
use primitives::network::PeerAddr;
use primitives::types::PeerId;

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
    pub proxy_handlers: Vec<ProxyHandlerType>,
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
        Arg::with_name("proxy_handlers")
            .long("proxy-handlers")
            .value_name("PROXY_HANDLERS")
            .help(
                "Specify a list of proxy handler for the node. In the form:\
                    --proxy_handler identifier
            ",
            )
            .multiple(true)
            .takes_value(true),
    ]
}

pub fn get_peer_id_from_seed(seed: Option<u32>) -> PeerId {
    if let Some(seed) = seed {
        let bytes: [u8; 4] = unsafe { mem::transmute(seed) };

        let mut array = [0; 32];
        for (count, b) in bytes.iter().enumerate() {
            array[array.len() - count - 1] = *b;
        }
        hash(&array)
    } else {
        let array: [u8; 32] = rand::random();
        // array is 32 bytes so we can safely unwrap here.
        CryptoHash::try_from(array.as_ref()).unwrap()
    }
}

pub fn from_matches(matches: &ArgMatches) -> NetworkConfig {
    let listen_addr = matches
        .value_of("addr")
        .map(|value| value.parse::<SocketAddr>().expect("Cannot parse address"));
    let test_network_key_seed =
        matches.value_of("test_network_key_seed").map(|x| x.parse::<u32>().unwrap());

    let parsed_boot_nodes =
        matches.values_of("boot_nodes").unwrap_or_else(clap::Values::default).map(String::from);
    let boot_nodes: Vec<_> = parsed_boot_nodes
        .map(|addr_id| PeerAddr::parse(&addr_id).expect("Cannot parse address"))
        .clone()
        .collect();

    let reconnect_delay_ms =
        matches.value_of("reconnect_delay_ms").map(|x| x.parse::<u64>().unwrap()).unwrap();
    let gossip_interval_ms =
        matches.value_of("gossip_interval_ms").map(|x| x.parse::<u64>().unwrap()).unwrap();
    let gossip_sample_size =
        matches.value_of("gossip_sample_size").map(|x| x.parse::<usize>().unwrap()).unwrap();

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
        proxy_handlers: vec![],
    }
}

/// Proxy Handlers that can be used in nodes from config file.
///
/// TODO(#795): Network Config Proxy
/// * Populate ProxyHandlerType with all proxy_handler we want to have builtin.
/// * get_handler_type is a map-like function that build ProxyHandlerType (with fixed parameters) from identifier (string).
/// * Parse config to accept proxy handlers. (This haven't been tested)
/// * Pass proper arguments to NetworkConfig constructor
/// * Build proxy_handlers properly from network configs.
#[derive(Clone)]
pub enum ProxyHandlerType {
    Debug,
    Dropout(f64),
}

impl ProxyHandlerType {
    #[allow(dead_code)]
    fn get_handler_type(handler_id: String) -> Option<ProxyHandlerType> {
        match handler_id.as_ref() {
            "debug" => Some(ProxyHandlerType::Debug),

            "dropout" => Some(ProxyHandlerType::Dropout(0.5f64)),

            _ => None,
        }
    }
}
