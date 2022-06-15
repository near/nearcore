use std::path::Path;
use std::process;
use std::sync::Arc;

use ansi_term::Color::{Green, Red, White, Yellow};
use clap::{Arg, Command};

use near_chain::store_validator::StoreValidator;
use near_chain::RuntimeAdapter;
use near_chain_configs::{GenesisValidationMode, DEFAULT_GC_NUM_EPOCHS_TO_KEEP};
use near_logger_utils::init_integration_logger;
use near_store::create_store;
use nearcore::{get_default_home, get_store_path, load_config, TrackedConfig};

fn main() {
    init_integration_logger();

    let default_home = get_default_home();
    let matches = Command::new("store-validator")
        .arg(
            Arg::new("home")
                .long("home")
                .default_value_os(default_home.as_os_str())
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .subcommand(Command::new("validate"))
        .get_matches();

    let home_dir = matches.value_of("home").map(Path::new).unwrap();
    let near_config = load_config(home_dir, GenesisValidationMode::Full)
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

    let store = create_store(&get_store_path(home_dir));

    let runtime_adapter: Arc<dyn RuntimeAdapter> = Arc::new(nearcore::NightshadeRuntime::new(
        home_dir,
        store.clone(),
        &near_config.genesis,
        TrackedConfig::from_config(&near_config.client_config),
        None,
        None,
        None,
        DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
        Default::default(),
    ));

    let mut store_validator = StoreValidator::new(
        near_config.validator_signer.as_ref().map(|x| x.validator_id().clone()),
        near_config.genesis.config,
        runtime_adapter.clone(),
        store,
        false,
    );
    store_validator.validate();

    if store_validator.tests_done() == 0 {
        println!("{}", Red.bold().paint("No conditions has been validated"));
        process::exit(1);
    }
    println!(
        "{} {}",
        White.bold().paint("Conditions validated:"),
        Green.bold().paint(store_validator.tests_done().to_string())
    );
    for error in store_validator.errors.iter() {
        println!(
            "{}  {}  {}",
            Red.bold().paint(&error.col),
            Yellow.bold().paint(&error.key),
            error.err
        );
    }
    if store_validator.is_failed() {
        println!("Errors found: {}", Red.bold().paint(store_validator.num_failed().to_string()));
        process::exit(1);
    } else {
        println!("{}", Green.bold().paint("No errors found"));
    }
    let gc_counters = store_validator.get_gc_counters();
    for (col, count) in gc_counters {
        println!("{} {}", White.bold().paint(col), count);
    }
}
