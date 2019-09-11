use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use std::path::PathBuf;

use env_logger::Builder;

use near::config::GenesisConfig;
use remote_node::RemoteNode;

use crate::transactions_executor::Executor;
use crate::transactions_generator::TransactionType;
use node_runtime::StateRecord;

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
    let genesis_config = GenesisConfig::from_file(&PathBuf::from("/tmp/local-near/node0/genesis.json"));
    let accounts: Vec<_> = genesis_config
        .records[0]
        .iter()
        .filter_map(|r| match r {
            StateRecord::Account { account_id, .. } => Some(account_id.clone()),
            _ => None,
        })
        .collect();

    let addrs = [
        "127.0.0.1:3030",
        "127.0.0.1:3031",
//        "127.0.0.1:3032",
//        "127.0.0.1:3033",
//        "127.0.0.1:3034",
//        "127.0.0.1:3035",
//        "127.0.0.1:3036",
//        "127.0.0.1:3037",
//        "127.0.0.1:3038",
//        "127.0.0.1:3039",
    ];

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
    let handle = Executor::spawn(nodes, Some(Duration::from_secs(10)), 700, TransactionType::Set);
    handle.join().unwrap();
}
