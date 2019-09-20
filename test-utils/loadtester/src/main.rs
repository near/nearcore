#[macro_use]
extern crate clap;

use log::info;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::{crate_version, App, Arg, SubCommand};
use env_logger::Builder;

use git_version::git_version;
use near::config::create_testnet_configs;
use near::get_default_home;
use near_crypto::Signer;
use near_primitives::types::Version;
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
    let internal_targets = vec!["loadtester"];
    let mut builder = Builder::from_default_env();
    internal_targets.iter().for_each(|internal_targets| {
        builder.filter(Some(internal_targets), log_level);
    });
    builder.default_format_timestamp_nanos(true);
    builder.try_init().unwrap();
}

fn main() {
    let version =
        Version { version: crate_version!().to_string(), build: git_version!().to_string() };
    let default_home = get_default_home();

    let matches = App::new("NEAR Protocol loadtester")
        .version(format!("{} (build {})", version.version, version.build).as_str())
        .arg(
            Arg::with_name("v")
                 .short("v")
                 .multiple(true)
                 .help("Sets the level of verbosity"))
        .subcommand(SubCommand::with_name("run").about("Run loadtester")
            .arg(
                Arg::with_name("accounts")
                    .long("accounts")
                    .takes_value(true)
                    .default_value("400")
                    .help("Number of accounts to use"),
            )
            .arg(
                Arg::with_name("prefix")
                    .long("prefix")
                    .takes_value(true)
                    .default_value("near")
                    .help("Prefix the account names (account results in {prefix}.0, {prefix}.1, ...)"),
            )
            // Will be able to self discover all available rpc endpoint once nearcore expose it.
            // .arg(
            //     Arg::with_name("addr")
            //         .long("addr")
            //         .takes_value(true)
            //         .default_value("127.0.0.1:3030")
            //         .help("Socket address of one of the node in network"),
            // )
            .arg(
                Arg::with_name("addrs")
                .long("addrs")
                .takes_value(true)
                .multiple(true)
                .help("Socket addresses of nodes to test in network"))
            .arg(
                Arg::with_name("tps")
                    .long("tps")
                    .takes_value(true)
                    .default_value("2000")
                    .help("Transaction per second to generate"))
            .arg(
                Arg::with_name("duration")
                    .long("duration")
                    .takes_value(true)
                    .default_value("10")
                    .help("Duration of load test in seconds"))
            .arg(
                Arg::with_name("type")
                    .long("type")
                    .takes_value(true)
                    .default_value("set")
                    .possible_values(&["set", "send_money", "heavy_storage"])
                    .help("Transaction type")))
        .subcommand(SubCommand::with_name("create_genesis").about("Create genesis file of many accounts for launch a network")
            .arg(
                Arg::with_name("accounts")
                .long("accounts")
                .takes_value(true)
                .default_value("400")
                .help("Number of accounts to create"))
            .arg(
                Arg::with_name("validators")
                .long("validators")
                .takes_value(true)
                .default_value("4")
                .help("Number of validators to create"))
            .arg(
                Arg::with_name("prefix")
                    .long("prefix")
                    .takes_value(true)
                    .default_value("near")
                    .help("Prefix the account names (account results in {prefix}.0, {prefix}.1, ...)"))
            .arg(
                Arg::with_name("home")
                    .long("home")
                    .takes_value(true)
                    .default_value(&default_home)))
        .get_matches();

    match matches.occurrences_of("v") {
        0 => configure_logging(log::LevelFilter::Error),
        1 => configure_logging(log::LevelFilter::Warn),
        2 => configure_logging(log::LevelFilter::Info),
        3 | _ => configure_logging(log::LevelFilter::Debug),
    }

    match matches.subcommand() {
        ("create_genesis", Some(args)) => create_genesis(args),
        ("run", Some(args)) => run(args),
        _ => unreachable!(),
    }
}

pub const CONFIG_FILENAME: &str = "config.json";

fn create_genesis(matches: &clap::ArgMatches) {
    let n = value_t_or_exit!(matches, "accounts", u64) as usize;
    let v = value_t_or_exit!(matches, "validators", u64) as usize;
    let prefix = value_t_or_exit!(matches, "prefix", String);
    let dir_buf = value_t_or_exit!(matches, "home", PathBuf);
    let dir = dir_buf.as_path();

    let (mut configs, signers, network_signers, genesis_config) =
        create_testnet_configs(v, n - v, &format!("{}.", prefix), false);
    for i in 0..v {
        let node_dir = dir.join(format!("{}.{}", prefix, i));
        fs::create_dir_all(node_dir.clone()).expect("Failed to create directory");

        signers[i].write_to_file(&node_dir.join(configs[i].validator_key_file.clone()));
        network_signers[i].write_to_file(&node_dir.join(configs[i].node_key_file.clone()));

        genesis_config.write_to_file(&node_dir.join(configs[i].genesis_file.clone()));
        configs[i].consensus.min_num_peers = 0;
        configs[i].write_to_file(&node_dir.join(CONFIG_FILENAME));
        info!(target: "loadtester", "Generated node key, validator key, genesis file in {}", node_dir.to_str().unwrap());
    }
}

fn run(matches: &clap::ArgMatches) {
    let n = value_t_or_exit!(matches, "accounts", u64);
    let prefix = value_t_or_exit!(matches, "prefix", String);
    // let addr = value_t_or_exit!(matches, "addr", String);
    let addrs = values_t_or_exit!(matches, "addrs", String);
    let addr = &addrs[0];
    let tps = value_t_or_exit!(matches, "tps", u64);
    let duration = value_t_or_exit!(matches, "duration", u64);
    let transaction_type = value_t_or_exit!(matches, "type", TransactionType);

    let node = RemoteNode::new(SocketAddr::from_str(&addr).unwrap(), &[]);

    // let peer_addrs = node.read().unwrap().peer_node_addrs().unwrap();
    let peer_addrs = &addrs[1..];

    let accounts: Vec<_> = (0..n).map(|i| format!("{}.{}", &prefix, i)).collect();

    let num_nodes = peer_addrs.len() + 1;
    let accounts_per_node = accounts.len() / num_nodes;
    node.write().unwrap().update_accounts(&accounts[0..accounts_per_node]);
    let mut nodes = vec![node];
    for (i, addr) in peer_addrs.iter().enumerate() {
        let node = RemoteNode::new(
            SocketAddr::from_str(addr).unwrap(),
            &accounts[((i + 1) * accounts_per_node)..((i + 2) * accounts_per_node)],
        );
        nodes.push(node);
    }

    // Start the executor.
    let handle = Executor::spawn(nodes, Some(Duration::from_secs(duration)), tps, transaction_type);
    handle.join().unwrap();
}
