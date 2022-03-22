use clap::{Arg, Command};
use near_primitives::types::ShardId;
use nearcore::get_default_home;
use std::collections::HashSet;
use std::path::Path;

pub mod csv_parser;
pub mod csv_to_json_configs;
pub mod serde_with;

fn main() {
    let default_home = get_default_home();
    let matches = Command::new("Genesis CSV to JSON.")
        .about("Converts accounts listed in CSV format to JSON configs")
        .arg(
            Arg::new("home")
                .long("home")
                .default_value_os(default_home.as_os_str())
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .arg(Arg::new("chain-id").long("chain-id").takes_value(true))
        .arg(
            Arg::new("tracked-shards")
                .long("tracked-shards")
                .takes_value(true)
                .help("Set of shards that this node wants to track (default empty)"),
        )
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    let chain_id = matches.value_of("chain-id").expect("Chain id is requried");
    let tracked_shards: HashSet<ShardId> = match matches.value_of("tracked-shards") {
        Some(s) => {
            if s.is_empty() {
                HashSet::default()
            } else {
                s.split(',').map(|v| v.parse::<ShardId>().unwrap()).collect()
            }
        }
        None => HashSet::default(),
    };
    csv_to_json_configs::csv_to_json_configs(
        home_dir,
        chain_id.to_string(),
        tracked_shards.into_iter().collect(),
    );
}
