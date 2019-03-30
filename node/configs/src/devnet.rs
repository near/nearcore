use clap::{Arg, ArgMatches};
use std::time::Duration;

const DEVNET_BLOCK_PERIOD_MS: &str = "100";

pub struct DevNetConfig {
    /// How often devnet produces blocks.
    pub block_period: Duration,
    /// Path to storage to replay blocks from.
    pub replay_storage: Option<String>,
}

pub fn get_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name("test_block_period")
            .long("test-block-period")
            .value_name("TEST_BLOCK_PERIOD")
            .help("Sets the block production period for devnet, in milliseconds")
            .takes_value(true)
            .default_value(DEVNET_BLOCK_PERIOD_MS),
        Arg::with_name("replay_storage")
            .long("replay-storage")
            .value_name("REPLAY_PATH")
            .help("Specify a replay path to re-evaluate blockchain.")
            .takes_value(true),
    ]
}

pub fn from_matches(matches: &ArgMatches) -> DevNetConfig {
    let block_period = matches
        .value_of("test_block_period")
        .map(|x| Duration::from_millis(x.parse::<u64>().unwrap()))
        .unwrap();
    let replay_storage = matches.value_of("replay_storage").map(std::string::ToString::to_string);
    DevNetConfig { block_period, replay_storage }
}
