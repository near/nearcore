use std::fs;
use std::path::Path;

use actix::System;
use clap::{App, Arg, SubCommand};
use log::{info, LevelFilter};

use near::config::init_testnet_configs;
use near::{get_store_path, init_configs, load_configs, start_with_config};

fn init_logging(verbose: bool) {
    if verbose {
        env_logger::Builder::new()
            .filter_module("tokio_reactor", LevelFilter::Info)
            .filter(None, LevelFilter::Debug)
            .init();
    } else {
        env_logger::Builder::new()
            .filter_module("tokio_reactor", LevelFilter::Info)
            .filter(Some("near"), LevelFilter::Info)
            .filter(Some("info"), LevelFilter::Info)
            .filter(None, LevelFilter::Warn)
            .init();
    }
}

fn get_default_home() -> String {
    match std::env::var("NEAR_HOME") {
        Ok(home) => home,
        Err(_) => match dirs::home_dir() {
            Some(mut home) => {
                home.push(".near");
                home.as_path().to_str().unwrap().to_string()
            }
            None => "".to_string(),
        },
    }
}

fn main() {
    let default_home = get_default_home();
    let matches = App::new("NEAR Protocol Node v0.1")
        .arg(Arg::with_name("verbose").long("verbose").help("Verbose logging").takes_value(false))
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("init").about("Initializes NEAR configuration")
            .arg(Arg::with_name("chain-id").long("chain-id").takes_value(true).help("Chain ID, by default creates new random"))
            .arg(Arg::with_name("account-id").long("account-id").takes_value(true).help("Account ID for initial validator"))
        )
        .subcommand(SubCommand::with_name("testnet").about("Setups testnet configuration with all necessary files (validator key, node key, genesis and config)")
            .arg(Arg::with_name("v").long("v").takes_value(true).help("Number of validators to initialize the testnet with (default 4)"))
            .arg(Arg::with_name("n").long("n").takes_value(true).help("Number of non-validators to initialize the testnet with (default 0)"))
            .arg(Arg::with_name("prefix").long("prefix").takes_value(true).help("Prefix the directory name for each node with (node results in node0, node1, ...) (default \"node\")"))
        )
        .subcommand(SubCommand::with_name("run").about("Runs NEAR node"))
        .subcommand(SubCommand::with_name("unsafe_reset_data").about("(unsafe) Remove all the data, effectively resetting node to genesis state (keeps genesis and config)"))
        .get_matches();

    init_logging(matches.is_present("verbose"));

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();

    // TODO: implement flags parsing here and reading config from NEARHOME env or base-dir flag.
    match matches.subcommand() {
        ("init", Some(args)) => {
            // TODO: Check if `home` exists. If exists check what networks we already have there.
            let chain_id = args.value_of("chain-id");
            let account_id = args.value_of("account-id");
            init_configs(home_dir, chain_id, account_id);
        }
        ("testnet", Some(args)) => {
            let num_validators = args
                .value_of("v")
                .map(|x| x.parse().expect("Failed to parse number of validators"))
                .unwrap_or(4);
            let num_non_validators = args
                .value_of("n")
                .map(|x| x.parse().expect("Failed to parse number of non-validators"))
                .unwrap_or(0);
            let prefix = args.value_of("prefix").unwrap_or("node");
            init_testnet_configs(home_dir, num_validators, num_non_validators, prefix);
        }
        ("run", Some(_args)) => {
            // Load configs from home.
            let system = System::new("NEAR");
            let (near_config, genesis_config, block_producer) = load_configs(home_dir);
            start_with_config(home_dir, genesis_config, near_config, Some(block_producer));
            system.run().unwrap();
        }
        ("unsafe_reset_data", Some(_args)) => {
            let store_path = get_store_path(home_dir);
            info!(target: "near", "Removing all data from {}", store_path);
            fs::remove_dir_all(store_path).expect("Removing data failed");
        }
        (_, _) => unreachable!(),
    }
}
