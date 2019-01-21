extern crate clap;
extern crate network;
extern crate client;
extern crate node_runtime;
extern crate serde_json;

use clap::{App, Arg};

fn main() {
    let matches = App::new("chain-spec-builder")
        .arg(
            Arg::with_name("boot_node")
                .short("b")
                .long("boot-node")
                .help(
                    "Specify a list of boot nodes in the format of \
                    '{host_name},{port},{test_network_key_seed}'.")
                .multiple(true)
                .takes_value(true),
        ).get_matches();

    let boot_nodes: Vec<String> = matches
        .values_of("boot_node")
        .unwrap_or_else(clap::Values::default)
        .map(|x| {
            let d: Vec<_> = x.split(',').collect();
            if d.len() != 3 {
                let message = "boot node must be in \
                '{host_name},{port},{test_network_key_seed}' format";
                panic!("{}", message);
            }
            let host = &d[0];
            let port: &str = &d[1];
            let port = port.parse::<u16>().unwrap();

            let test_network_key_seed: &str = &d[2];
            let test_network_key_seed = test_network_key_seed.parse::<u32>().unwrap();
            let secret = configs::network::get_test_secret_from_network_key_seed(
                test_network_key_seed
            );
            let key = network::test_utils::raw_key_to_peer_id(secret);

            format!("/dns4/{}/tcp/{}/p2p/{}", host, port, key.to_base58())
        })
        .collect();

    let (mut chain_spec, _) = node_runtime::test_utils::generate_test_chain_spec();
    chain_spec.boot_nodes = boot_nodes;
    let serialized = configs::chain_spec::serialize_chain_spec(chain_spec);
    println!("{}", serialized);
}
