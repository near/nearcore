use clap::{Arg, Command};
use near_primitives::types::ShardId;
use nearcore::get_default_home;
use std::collections::HashSet;
use std::path::PathBuf;

pub mod csv_parser;
pub mod csv_to_json_configs;
pub mod serde_with;

fn main() {
    let matches = Command::new("Genesis CSV to JSON.")
        .about("Converts accounts listed in CSV format to JSON configs")
        .arg(
            Arg::new("home")
                .long("home")
                .default_value(get_default_home().into_os_string())
                .value_parser(clap::value_parser!(PathBuf))
                .help("Directory for config and data (default \"~/.near\")")
                .action(clap::ArgAction::Set),
        )
        .arg(Arg::new("chain-id").long("chain-id").action(clap::ArgAction::Set))
        .arg(
            Arg::new("tracked-shards")
                .long("tracked-shards")
                .action(clap::ArgAction::Set)
                .help("Set of shards that this node wants to track (default empty)"),
        )
        .get_matches();

    let home_dir = matches.get_one::<PathBuf>("home").unwrap();
    let chain_id = matches.get_one::<String>("chain-id").expect("Chain id is requried");
    let tracked_shards: HashSet<ShardId> = match matches.get_one::<String>("tracked-shards") {
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
