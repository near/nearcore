use clap::{App, Arg};
use near::get_default_home;
use std::path::Path;

pub mod csv_parser;
pub mod csv_to_json_configs;
pub mod serde_with;

fn main() {
    let default_home = get_default_home();
    let matches = App::new("Genesis CSV to JSON.")
        .about("Converts accounts listed in CSV format to JSON configs")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .arg(Arg::with_name("chain-id").long("chain-id").takes_value(true))
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let chain_id = matches.value_of("chain-id").expect("Chain id is requried");
    csv_to_json_configs::csv_to_json_configs(home_dir, chain_id.to_string());
}
