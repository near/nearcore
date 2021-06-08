use clap::Clap;
use near_primitives::types::{NumSeats, NumShards};
use std::path::Path;

#[derive(Clap)]
pub(super) struct TestnetCmd {
    /// Number of non-validators to initialize the testnet with.
    #[clap(long = "n", default_value = "0")]
    non_validators: NumSeats,
    /// Prefix the directory name for each node with (node results in node0, node1, ...)
    #[clap(long, default_value = "node")]
    prefix: String,
    /// Number of shards to initialize the testnet with.
    #[clap(long, default_value = "4")]
    shards: NumShards,
    /// Number of validators to initialize the testnet with.
    #[clap(long = "v", default_value = "4")]
    validators: NumSeats,
}

impl TestnetCmd {
    pub(super) fn run(self, home_dir: &Path) {
        nearcore::config::init_testnet_configs(
            home_dir,
            self.shards,
            self.validators,
            self.non_validators,
            &self.prefix,
            false,
        );
    }
}
