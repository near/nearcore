use std::path::PathBuf;
use std::rc::Rc;

use clap::Parser;
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManager;
use near_store::{NodeStorage, Store};
use nearcore::open_storage;

#[derive(Parser)]
pub(crate) struct AnalyseGasUsageCommand;

impl AnalyseGasUsageCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        // Create a ChainStore and EpochManager that will be used to read blockchain data.
        let mut near_config =
            nearcore::config::load_config(home, near_chain_configs::GenesisValidationMode::Full)
                .unwrap();
        let node_storage: NodeStorage = open_storage(&home, &mut near_config).unwrap();
        let store: Store =
            node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let chain_store = Rc::new(ChainStore::new(
            store.clone(),
            near_config.genesis.config.genesis_height,
            false,
        ));
        let _epoch_manager =
            EpochManager::new_from_genesis_config(store, &near_config.genesis.config).unwrap();

        println!(
            "Analysing gas usage, the tip of the blockchain is: {:#?}",
            chain_store.head().unwrap()
        );

        // TODO: Implement the analysis

        Ok(())
    }
}
