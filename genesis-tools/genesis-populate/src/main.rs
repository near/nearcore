use std::path::Path;
use std::sync::Arc;

use clap::{App, Arg};

use near_store::create_store;
use neard::{get_default_home, get_store_path, load_config};

use genesis_populate::GenesisBuilder;

fn main() {
    let default_home = get_default_home();
    let matches = App::new("Genesis populator")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .arg(Arg::with_name("additional-accounts-num").long("additional-accounts-num").required(true).takes_value(true).help("Number of additional accounts per shard to add directly to the trie (TESTING ONLY)"))
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let additional_accounts_num = matches
        .value_of("additional-accounts-num")
        .map(|x| x.parse::<u64>().expect("Failed to parse number of additional accounts."))
        .unwrap();
    let near_config = load_config(home_dir);

    let store = create_store(&get_store_path(home_dir));
    GenesisBuilder::from_config_and_store(home_dir, Arc::new(near_config.genesis), store)
        .add_additional_accounts(additional_accounts_num)
        .add_additional_accounts_contract(near_test_contracts::tiny_contract().to_vec())
        .print_progress()
        .build()
        .unwrap()
        .dump_state()
        .unwrap();
}
