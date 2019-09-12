use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use clap::{App, Arg};
use env_logger::Builder;

use near::{get_default_home, load_config};
use node_runtime::StateRecord;
use remote_node::RemoteNode;

use crate::transactions_executor::Executor;
use crate::transactions_generator::TransactionType;

pub mod remote_node;
pub mod sampler;
pub mod stats;
pub mod transactions_executor;
pub mod transactions_generator;

#[allow(dead_code)]
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

    let default_home = get_default_home();
    let matches = App::new("loadtester")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory to load genesis config (default \"~/.near\")")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("num-nodes")
                .default_value("1")
                .help("Number of local nodes to test")
                .takes_value(true),
        )
        .get_matches();
    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let num_nodes =
        matches.value_of("num-nodes").map(|v| v.parse().expect("Must be integer")).unwrap_or(1);

    let config = load_config(home_dir);

    let accounts: Vec<_> = config.genesis_config.records[0]
        .iter()
        .filter_map(|r| match r {
            StateRecord::Account { account_id, .. } => Some(account_id.clone()),
            _ => None,
        })
        .collect();

    let addrs = (0..num_nodes).map(|i| format!("127.0.0.1:{}", 3030 + i)).collect::<Vec<_>>();

    let num_nodes = addrs.len();
    let accounts_per_node = accounts.len() / num_nodes;
    let mut nodes = vec![];
    for (i, addr) in addrs.iter().enumerate() {
        let node = RemoteNode::new(
            SocketAddr::from_str(addr).unwrap(),
            &accounts[(i * accounts_per_node)..((i + 1) * accounts_per_node)],
        );
        nodes.push(node);
    }

    // Start the executor.
    let handle = Executor::spawn(nodes, Some(Duration::from_secs(60)), 700, TransactionType::Set);
    handle.join().unwrap();
}
