use near_primitives::block::Tip;
use near_store::{DBCol, NodeStorage, Temperature, FINAL_HEAD_KEY, HEAD_KEY};

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
    /// Open NodeStorage and check that is has cold storage.
    Open,
    /// Open NodeStorage and print cold head, hot head and hot final head.
    Head,
}

impl ColdStoreCommand {
    pub fn run(self, home_dir: &Path) {
        let near_config = nearcore::config::load_config(
            &home_dir,
            near_chain_configs::GenesisValidationMode::Full,
        )
        .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        );
        let store = opener.open().unwrap_or_else(|e| panic!("Error opening storage: {:#}", e));
        match self.subcmd {
            SubCommand::Open => check_open(&store),
            SubCommand::Head => print_heads(&store),
        }
    }
}

fn check_open(store: &NodeStorage) {
    assert!(store.has_cold());
}

fn print_heads(store: &NodeStorage) {
    println!(
        "HOT HEAD is at {:#?}",
        store.get_store(Temperature::Hot).get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)
    );
    println!(
        "HOT FINAL HEAD is at {:#?}",
        store.get_store(Temperature::Hot).get_ser::<Tip>(DBCol::BlockMisc, FINAL_HEAD_KEY)
    );
    println!(
        "COLD HEAD is at {:#?}",
        store.get_store(Temperature::Cold).get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY)
    );
}
