//! A tool that does load testing of the remotely running nodes.
//! Usage example:
//! ```
//! loadtester --key-files-path /tmp/keys --addresses 35.235.94.193:3030 35.236.51.32:3030 35.236.103.128:3030 --public-keys CTVkQMjLyr4QzoXrTDVzfCUp95sCJPwLJZ34JTiekxMV EJ1DMa6s2ngC5GtZb3Z2DZzat2xFZ34j15VLY37dcdXX 3DToePHssYc75SsxZgzgVLwXE8XQXKjdpdL7CT7D34UE --account-ids near.1 near.2 near.3
//! ```
//! where `/tmp/keys` contains keys of the accounts of the nodes that we are testing.
use clap::{App, Arg};
use env_logger::Builder;
use primitives::crypto::signer::InMemorySigner;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use testlib::node::{Node, NodeConfig};
use testlib::nodes_monitor::NodesMonitor;
use testlib::transactions_executor::{Executor, TrafficType};
use testlib::transactions_generator::TransactionType;

const NUMBER_OF_NODES_ERR: &str =
    "Number of addresses, public keys, and account ids should be the same";

fn parse_args() -> Vec<(Arc<InMemorySigner>, SocketAddr)> {
    let matches = App::new("Near Load Tester")
        .arg(
            Arg::with_name("key_files_path")
                .long("key-files-path")
                .value_name("KEY_FILES_PATH")
                .help("The path to the folder with key files.")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("addresses")
                .long("addresses")
                .value_name("ADDRESSES")
                .help(
                    "A list of addresses of the nodes. In the form: --boot_nodes <ip1>:<port1> <ip2>:<port2>",
                )
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("public_keys")
                .long("public-keys")
                .value_name("PUBLIC_KEYS")
                .help("Public keys of the nodes we are connecting two.")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("account_ids")
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
    let public_keys: Vec<String> = matches
        .values_of("public_keys")
        .unwrap_or_else(clap::Values::default)
        .map(String::from)
        .collect();
    let account_ids: Vec<String> = matches
        .values_of("account_ids")
        .unwrap_or_else(clap::Values::default)
        .map(String::from)
        .collect();

    assert_eq!(addrs.len(), public_keys.len(), "{}", NUMBER_OF_NODES_ERR);
    assert_eq!(account_ids.len(), public_keys.len(), "{}", NUMBER_OF_NODES_ERR);
    let mut res = vec![];
    for i in 0..addrs.len() {
        let signer = Arc::new(InMemorySigner::from_key_file(
            account_ids[i].clone(),
            key_files_path.as_path(),
            Some(public_keys[i].clone()),
        ));
        res.push((signer, addrs[i]));
    }
    res
}

fn connect_nodes(args: Vec<(Arc<InMemorySigner>, SocketAddr)>) -> Vec<Arc<RwLock<dyn Node>>> {
    args.into_iter()
        .map(|(signer, addr)| Node::new_sharable(NodeConfig::Remote { signer, addr }))
        .collect()
}

fn configure_logging(log_level: log::LevelFilter) {
    let internal_targets = vec!["observer"];
    let mut builder = Builder::from_default_env();
    internal_targets.iter().for_each(|internal_targets| {
        builder.filter(Some(internal_targets), log_level);
    });
    builder.default_format_timestamp_nanos(true);
    builder.try_init().unwrap();
}

fn main() {
    configure_logging(log::LevelFilter::Debug);
    let args = parse_args();
    let nodes = connect_nodes(args);
    let nodes_monitor =
        Arc::new(NodesMonitor::new(nodes.to_vec(), Duration::from_secs(1), Duration::from_secs(1)));
    nodes_monitor.start();
    // Wait for all nodes to start running.
    while !nodes_monitor.all_nodes_running() {}
    println!("All nodes started.");

    // Start the monitor.
    {
        let nodes_monitor = nodes_monitor.clone();
        thread::spawn(move || loop {
            let tps = nodes_monitor
                .average_tps(Duration::from_secs(10))
                .map(|d| format!("{}", d))
                .unwrap_or_else(|| "Pending".to_owned());
            println!("TPS: {}", tps);
            thread::sleep(Duration::from_secs(1));
        });
    }

    // Start the executor.
    let handle =
        Executor::spawn(nodes, TransactionType::Monetary, None, None, 700, TrafficType::Regular);
    handle.join().unwrap();
}
