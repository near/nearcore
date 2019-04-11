use clap::{App, Arg};
use primitives::crypto::signer::InMemorySigner;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use testlib::node::{Node, NodeConfig};

const NUMBER_OF_NODES_ERR: &str =
    "Number of addresses, public keys, and account ids should be the same";

fn parse_args() -> Vec<(Arc<InMemorySigner>, SocketAddr)> {
    let matches = App::new("Near Load Tester")
        .arg(
            Arg::with_name("key_files_path")
                .short("k")
                .long("key-files-path")
                .value_name("KEY_FILES_PATH")
                .help("The path to the folder with key files.")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("addresses")
                .short("a")
                .long("addresses")
                .value_name("ADDRESSES")
                .help(
                    "A list of addresses of the nodes. In the form:\
                     --boot_nodes <ip1>:<port1> <ip2>:<port2>",
                )
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("public_keys")
                .short("k")
                .long("public-keys")
                .value_name("PUBLIC_KEYS")
                .help("Public keys of the nodes we are connecting two.")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("account_ids")
                .short("a")
                .long("account-ids")
                .value_name("ACCOUNT_IDS")
                .help("Account ids of the corresponding nodes")
                .multiple(true)
                .takes_value(true),
        )
        .get_matches();

    let key_files_path: PathBuf = matches.value_of("key_files_path").map(PathBuf::from).unwrap();
    let parsed_addrs =
        matches.values_of("addresses").unwrap_or_else(clap::Values::default).map(String::from);
    let addrs: Vec<_> = parsed_addrs
        .map(|addr_id| SocketAddr::from_str(&addr_id).expect("Cannot parse address"))
        .clone()
        .collect();
    let public_keys: Vec<String> =
        matches.values_of("public_keys").unwrap_or_else(clap::Values::default).map(String::from).collect();
    let account_ids: Vec<String> =
        matches.values_of("account_ids").unwrap_or_else(clap::Values::default).map(String::from).collect();

    assert_eq!(addrs.len(), public_keys.len(), "{}", NUMBER_OF_NODES_ERR);
    assert_eq!(account_ids.len(), public_keys.len(), "{}", NUMBER_OF_NODES_ERR);
    let mut res = vec![];
    for i in 0..addrs.len() {
        let signer = Arc::new(InMemorySigner::from_key_file(
            account_ids[i].clone(),
            key_files_path.as_path(),
            Some(public_keys[i].clone()),
        ));
        res.push((signer, addrs[i].clone()));
    }
    res
}

fn connect_nodes(args: Vec<(Arc<InMemorySigner>, SocketAddr)>) -> Vec<Arc<RwLock<dyn Node>>> {
    args.into_iter()
        .map(|(signer, addr)| Node::new_sharable(NodeConfig::Remote { signer, addr }))
        .collect()
}

fn main() {
    let args = parse_args();
    let _nodes = connect_nodes(args);
    // TODO: Start observer and transactions executor, add transactions executor args once #851
    // and #852 are merged.
}
