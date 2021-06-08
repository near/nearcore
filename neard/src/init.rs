use clap::Clap;
use near_primitives::types::NumShards;
use std::path::Path;

#[derive(Clap)]
pub(super) struct InitCmd {
    /// Download the verified NEAR genesis file automatically.
    #[clap(long)]
    download_genesis: bool,
    /// Makes block production fast (TESTING ONLY).
    #[clap(long)]
    fast: bool,
    /// Account ID for the validator key.
    #[clap(long)]
    account_id: Option<String>,
    /// Chain ID, by default creates new random.
    #[clap(long)]
    chain_id: Option<String>,
    /// Specify a custom download URL for the genesis-file.
    #[clap(long)]
    download_genesis_url: Option<String>,
    /// Genesis file to use when initializing testnet (including downloading).
    #[clap(long)]
    genesis: Option<String>,
    /// Number of shards to initialize the chain with.
    #[clap(long, default_value = "1")]
    num_shards: NumShards,
    /// Specify private key generated from seed (TESTING ONLY).
    #[clap(long)]
    test_seed: Option<String>,
}

impl InitCmd {
    pub(super) fn run(self, home_dir: &Path) {
        // TODO: Check if `home` exists. If exists check what networks we already have there.
        if (self.download_genesis || self.download_genesis_url.is_some()) && self.genesis.is_some()
        {
            panic!(
                    "Please specify a local genesis file or download the NEAR genesis or specify your own."
                );
        }

        nearcore::init_configs(
            home_dir,
            self.chain_id.as_deref(),
            self.account_id.as_deref(),
            self.test_seed.as_deref(),
            self.num_shards,
            self.fast,
            self.genesis.as_deref(),
            self.download_genesis,
            self.download_genesis_url.as_deref(),
        );
    }
}
