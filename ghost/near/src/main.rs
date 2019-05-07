use std::path::Path;

use actix::System;
use clap::{App, Arg, SubCommand};
use log::LevelFilter;

use near::{start_with_config, GenesisConfig, NearConfig, init_config};
use near_client::BlockProducer;

fn init_logging(verbose: bool) {
    if verbose {
        env_logger::Builder::new()
            .filter_module("tokio_reactor", LevelFilter::Info)
            .filter(None, LevelFilter::Debug)
            .init();
    } else {
        env_logger::Builder::new()
            .filter_module("tokio_reactor", LevelFilter::Info)
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
        .subcommand(SubCommand::with_name("init").about("Initializes NEAR configuration"))
        .subcommand(SubCommand::with_name("run").about("Runs NEAR node"))
        .get_matches();

    init_logging(matches.is_present("verbose"));

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();

    // TODO: implement flags parsing here and reading config from NEARHOME env or base-dir flag.
    if let Some(_matches) = matches.subcommand_matches("init") {
        // TODO: Check if `home` exists. If exists check what networks we already have there.
        init_config(home_dir, None, None);
    } else if let Some(_matches) = matches.subcommand_matches("run") {
        // Load configs from home.
        let system = System::new("NEAR");
        let genesis_config = GenesisConfig::test(vec!["alice.near"]);
        let mut near = NearConfig::test("alice.near", 25123);
        near.client_config.skip_sync_wait = true;
        let block_producer = BlockProducer::test("alice.near");
        start_with_config(home_dir, genesis_config, near, Some(block_producer));
        system.run().unwrap();
    }
}
