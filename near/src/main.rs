use std::fs;
use std::path::Path;

use actix::System;
use clap::{App, Arg, SubCommand};
use log::{info, LevelFilter};

use near::config::init_testnet_configs;
use near::{get_store_path, init_configs, load_config, start_with_config};

fn init_logging(verbose: bool) {
    if verbose {
        env_logger::Builder::new()
            .filter_module("tokio_reactor", LevelFilter::Info)
            .filter_module("cranelift_codegen", LevelFilter::Warn)
            .filter_module("cranelift_wasm", LevelFilter::Warn)
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
            .arg(Arg::with_name("test-seed").long("test-seed").takes_value(true).help("Specify private key generated from seed (TESTING ONLY)"))
        )
        .subcommand(SubCommand::with_name("testnet").about("Setups testnet configuration with all necessary files (validator key, node key, genesis and config)")
            .arg(Arg::with_name("v").long("v").takes_value(true).help("Number of validators to initialize the testnet with (default 4)"))
            .arg(Arg::with_name("n").long("n").takes_value(true).help("Number of non-validators to initialize the testnet with (default 0)"))
            .arg(Arg::with_name("prefix").long("prefix").takes_value(true).help("Prefix the directory name for each node with (node results in node0, node1, ...) (default \"node\")"))
        )
        .subcommand(SubCommand::with_name("run").about("Runs NEAR node")
            .arg(Arg::with_name("produce-empty-blocks").long("produce-empty-blocks").help("Set this to false to only produce blocks when there are txs or receipts (default true)").default_value("true").takes_value(true))
        )
        .subcommand(SubCommand::with_name("unsafe_reset_data").about("(unsafe) Remove all the data, effectively resetting node to genesis state (keeps genesis and config)"))
        .subcommand(SubCommand::with_name("unsafe_reset_all").about("(unsafe) Remove all the config, keys, data and effectively removing all information about the network"))
        .get_matches();

    init_logging(matches.is_present("verbose"));

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();

    match matches.subcommand() {
        ("init", Some(args)) => {
            // TODO: Check if `home` exists. If exists check what networks we already have there.
            let chain_id = args.value_of("chain-id");
            let account_id = args.value_of("account-id");
            let test_seed = args.value_of("test-seed");
            init_configs(home_dir, chain_id, account_id, test_seed);
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
        ("run", Some(args)) => {
            // Load configs from home.
            let mut near_config = load_config(home_dir);
            // Override some parameters from command line.
            if let Some(produce_empty_blocks) = args
                .value_of("produce-empty-blocks")
                .map(|x| x.parse().expect("Failed to parse boolean"))
            {
                near_config.client_config.produce_empty_blocks = produce_empty_blocks;
            }

            let system = System::new("NEAR");
            start_with_config(home_dir, near_config);
            system.run().unwrap();
        }
        ("unsafe_reset_data", Some(_args)) => {
            let store_path = get_store_path(home_dir);
            info!(target: "near", "Removing all data from {}", store_path);
            fs::remove_dir_all(store_path).expect("Removing data failed");
        }
        ("unsafe_reset_all", Some(_args)) => {
            info!(target: "near", "Removing all data and config from {}", home_dir.to_str().unwrap());
            fs::remove_dir_all(home_dir).expect("Removing data and config failed.");
        }
        (_, _) => unreachable!(),
    }
}
