use std::path::Path;
use std::process;

use ansi_term::Color::{Green, Red};
use clap::{App, Arg, SubCommand};

use near_logger_utils::init_integration_logger;
use near_store::{create_store, StoreValidator};
use neard::{get_default_home, get_store_path, load_config};

fn main() {
    init_integration_logger();

    let default_home = get_default_home();
    let matches = App::new("store-validator")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .subcommand(SubCommand::with_name("validate"))
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let near_config = load_config(home_dir);

    let store = create_store(&get_store_path(&home_dir));

    let mut store_validator = StoreValidator::default();
    store_validator.validate(&*store, &near_config.genesis.config);

    if store_validator.tests_done() == 0 {
        println!("{}", Red.bold().paint("No conditions has been validated"));
        process::exit(1);
    }
    println!(
        "Conditions validated: {}",
        Green.bold().paint(store_validator.tests_done().to_string())
    );
    for error in store_validator.errors.iter() {
        println!("{}: {}", Red.bold().paint(&error.col.to_string()), error.msg);
    }
    if store_validator.is_failed() {
        println!("Errors found: {}", Red.bold().paint(store_validator.failed().to_string()));
        process::exit(1);
    } else {
        println!("{}", Green.bold().paint("No errors found"));
    }
}
