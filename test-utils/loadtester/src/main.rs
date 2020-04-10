#[macro_use]
extern crate clap;

use std::time::Duration;

use clap::{crate_version, App, AppSettings, Arg, SubCommand};
use env_logger::Builder;

use futures::future;
use git_version::git_version;
use near_primitives::types::Version;
use remote_node::RemoteNode;

use crate::account::account_k;
use crate::transactions_executor::Executor;
use crate::transactions_generator::TransactionType;

pub mod account;
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
    builder.format_timestamp_nanos();
    builder.try_init().unwrap();
}

use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio::runtime::Runtime;

fn main() {
    let version =
        Version { version: crate_version!().to_string(), build: git_version!().to_string() };

    let matches = App::new("NEAR Protocol loadtester")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .version(format!("{} (build {})", version.version, version.build).as_str())
        .arg(Arg::with_name("v")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity"))
        .subcommand(SubCommand::with_name("run").about("Run loadtester")
            .arg(Arg::with_name("accounts")
                .long("accounts")
                .takes_value(true)
                .default_value("10240")
                .help("Number of accounts to use"))
            .arg(Arg::with_name("postfix")
                .long("postfix")
                .takes_value(true)
                .default_value("near")
                .help("Postfix the account names (account results in word_in_dict_{postfix}, ...)"))
            .arg(Arg::with_name("addr")
                .long("addr")
                .takes_value(true)
                .help("Rpc address of node to test in network"))
            .arg(Arg::with_name("tps")
                .long("tps")
                .takes_value(true)
                .default_value("100")
                .help("Transaction per second to generate"))
            .arg(Arg::with_name("duration")
                .long("duration")
                .takes_value(true)
                .default_value("10")
                .help("Duration of load test in seconds"))
            .arg(Arg::with_name("type")
                .long("type")
                .takes_value(true)
                .default_value("set")
                .possible_values(&["set", "send_money", "heavy_storage"])
                .help("Transaction type")))
        .get_matches();

    match matches.occurrences_of("v") {
        0 => configure_logging(log::LevelFilter::Error),
        1 => configure_logging(log::LevelFilter::Warn),
        2 => configure_logging(log::LevelFilter::Info),
        _ => configure_logging(log::LevelFilter::Debug),
    }

    match matches.subcommand() {
        ("run", Some(args)) => run(args),
        _ => unreachable!(),
    }
}

pub const CONFIG_FILENAME: &str = "config.json";

fn run(matches: &clap::ArgMatches<'_>) {
    let n = value_t_or_exit!(matches, "accounts", u64);
    let tps = value_t_or_exit!(matches, "tps", u64);
    let duration = value_t_or_exit!(matches, "duration", u64);
    let transaction_type = value_t_or_exit!(matches, "type", TransactionType);

    let addr = value_t_or_exit!(matches, "addr", String);

    let accounts: Vec<_> = (0..n).map(|i| account_k(i as usize)).collect();

    let node = RemoteNode::new(&addr, &accounts);
    node.write().unwrap().update_accounts(&accounts);
    let nodes = vec![node];

    // Start the executor.
    Executor::spawn(nodes, Some(Duration::from_secs(duration)), tps, transaction_type);
    // handle.join().unwrap();
}
