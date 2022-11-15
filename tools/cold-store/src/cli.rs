use near_store::NodeStorage;

use clap::Parser;
use std::path::Path;

#[derive(Parser)]
pub struct ColdStoreCommand {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Parser)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
enum SubCommand {
    Open,
}

impl ColdStoreCommand {
    pub fn run(self, home_dir: &Path) {
        let near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::Full,
        )
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        match self.subcmd {
            SubCommand::Open => check_open(home_dir, near_config),
        }
    }
}

fn check_open(home_dir: &Path, near_config: nearcore::config::NearConfig) {
    let opener = NodeStorage::opener(
        home_dir,
        &near_config.config.store,
        near_config.config.cold_store.as_ref(),
    );
    let store = opener.open().unwrap_or_else(|e| panic!("Error opening storage: {:#}", e));
    assert!(store.has_cold());
}
