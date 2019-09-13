#[macro_use]
extern crate clap;

use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use clap::{crate_version, App, Arg};
use env_logger::Builder;

use git_version::git_version;
use near_primitives::types::Version;
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
    let version =
        Version { version: crate_version!().to_string(), build: git_version!().to_string() };

    let matches = App::new("NEAR Protocol loadtester")
        .version(format!("{} (build {})", version.version, version.build).as_str())
        .arg(
            Arg::with_name("n")
                .short("n")
                .takes_value(true)
                .default_value("400")
                .help("Number of accounts to create"),
        )
        .arg(
            Arg::with_name("prefix")
                .long("prefix")
                .takes_value(true)
                .default_value("near")
                .help("Prefix the account names (account results in {prefix}.0, {prefix}.1, ...)"),
        )
        .arg(
            Arg::with_name("addr")
                .long("addr")
                .takes_value(true)
                .default_value("127.0.0.1:3030")
                .help("Socket address of one of the node in network"),
        )
        .arg(
            Arg::with_name("tps")
                .long("tps")
                .takes_value(true)
                .default_value("2000")
                .help("Transaction per second to generate"),
        )
        .arg(
            Arg::with_name("duration")
                .long("duration")
                .takes_value(true)
                .default_value("10")
                .help("Duration of load test in seconds"),
        )
        .arg(
            Arg::with_name("type")
                .long("type")
                .takes_value(true)
                .default_value("set")
                .possible_values(&["set", "send_money", "heavy_storage"])
                .help("Transaction type"),
        )
        .get_matches();

    let n = value_t_or_exit!(matches, "n", u64);
    let prefix = value_t_or_exit!(matches, "prefix", String);
    let addr = value_t_or_exit!(matches, "addr", String);
    let tps = value_t_or_exit!(matches, "tps", u64);
    let duration = value_t_or_exit!(matches, "duration", u64);
    let transaction_type = value_t_or_exit!(matches, "type", TransactionType);

    let node = RemoteNode::new(SocketAddr::from_str(&addr).unwrap(), &[]);

    let peer_addrs = node.read().unwrap().peer_node_addrs().unwrap();

    let accounts = node.read().unwrap().ensure_create_accounts(&prefix, n).unwrap();

    let num_nodes = peer_addrs.len() + 1;
    let accounts_per_node = accounts.len() / num_nodes;
    node.write().unwrap().update_accounts(&accounts[0..accounts_per_node]);
    let mut nodes = vec![node];
    for (i, addr) in peer_addrs.iter().enumerate() {
        let node = RemoteNode::new(
            SocketAddr::from_str(addr).unwrap(),
            &accounts[((i+1) * accounts_per_node)..((i + 2) * accounts_per_node)],
        );
        nodes.push(node);
    }

    // Start the executor.
    let handle = Executor::spawn(nodes, Some(Duration::from_secs(duration)), tps, transaction_type);
    handle.join().unwrap();
}
