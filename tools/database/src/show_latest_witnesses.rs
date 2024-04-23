use std::path::PathBuf;
use std::rc::Rc;

use clap::Parser;
use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_store::{Mode, NodeStorage};
use nearcore::load_config;

#[derive(Parser)]
pub(crate) struct ShowLatestWitnessesCommand {
    /// Block height (required)
    #[arg(long)]
    height: u64,

    /// Shard id (optional)
    #[arg(long)]
    shard_id: Option<u64>,

    /// Pretty-print using the "{:#?}" formatting.
    #[arg(long)]
    pretty: bool,

    /// Print the raw &[u8], can be pasted into rust code
    #[arg(long)]
    binary: bool,
}

impl ShowLatestWitnessesCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let near_config = load_config(home, GenesisValidationMode::Full).unwrap();
        let node_storage = NodeStorage::opener(
            home,
            near_config.client_config.archive,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
        )
        .open_in_mode(Mode::ReadOnly)
        .unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store =
            Rc::new(ChainStore::new(store, near_config.genesis.config.genesis_height, false));

        let witnesses = chain_store.get_latest_witnesses(self.height, self.shard_id).unwrap();
        println!("Found {} witnesses:", witnesses.len());
        for (i, witness) in witnesses.iter().enumerate() {
            println!(
                "#{} (height: {}, shard_id: {}):",
                i,
                witness.chunk_header.height_created(),
                witness.chunk_header.shard_id()
            );
            if self.pretty {
                println!("{:#?}", witness);
            } else if self.binary {
                println!("{:?}", borsh::to_vec(witness).unwrap());
            } else {
                println!("{:?}", witness);
            }
        }

        Ok(())
    }
}
