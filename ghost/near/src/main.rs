use actix::System;
use clap::{App, Arg, SubCommand};
use log::LevelFilter;

use near::{start_with_config, NearConfig};

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

fn main() {
    let matches = App::new("NEAR Protocol Node v0.1")
        .arg(Arg::with_name("verbose")
            .long("verbose")
            .help("Verbose logging")
            .takes_value(false))
        .arg(Arg::with_name("home").long("home").help("Directory for config and data (default \"~/.near\")").takes_value(true))
        .subcommand(SubCommand::with_name("init").about("Initializes NEAR configuration"))
        .subcommand(SubCommand::with_name("run").about("Runs NEAR node"))
        .get_matches();

    init_logging(matches.is_present("verbose"));

    // TODO: implement flags parsing here and reading config from NEARHOME env or base-dir flag.
    if let Some(_matches) = matches.subcommand_matches("init") {
        // Check if `home` exists. If exists check what networks we already have there.
//    let genesis_timestamp = Utc::now();
//    let near = NearConfig::new(genesis_timestamp.clone(), "alice.near", 25123);
    } else if let Some(_matches) = matches.subcommand_matches("run") {
        // Load configs from home.
        let system = System::new("NEAR");
//    start_with_config(near);
        system.run().unwrap();
    }
}
