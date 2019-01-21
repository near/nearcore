use clap::{Arg, ArgMatches};
use std::time::Duration;

const DEVNET_BLOCK_PERIOD_MS: &str = "100";

pub struct DevNetConfig {
    /// how often devnet produces blocks
    pub block_period: Duration,
}

pub fn get_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![Arg::with_name("test_block_period")
        .long("test-block-period")
        .value_name("TEST_BLOCK_PERIOD")
        .help("Sets the block production period for devnet, in milliseconds")
        .takes_value(true)
        .default_value(DEVNET_BLOCK_PERIOD_MS)]
}

pub fn from_matches(matches: &ArgMatches) -> DevNetConfig {
    let block_period = matches
        .value_of("test_block_period")
        .map(|x| Duration::from_millis(x.parse::<u64>().unwrap()))
        .unwrap();
    DevNetConfig { block_period }
}
