use std::path::PathBuf;
use std::rc::Rc;

use clap::Parser;
use near_chain::{Block, ChainStore};
use near_epoch_manager::EpochManager;
use near_store::{NodeStorage, Store};
use nearcore::open_storage;

use crate::block_iterators::LastNBlocksIterator;

#[derive(Parser)]
pub(crate) struct AnalyseGasUsageCommand {
    /// Analyse the last N blocks in the blockchain
    #[arg(long)]
    last_blocks: Option<u64>,
}

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
        let epoch_manager =
            EpochManager::new_from_genesis_config(store, &near_config.genesis.config).unwrap();

        // Create an iterator over the blocks that should be analysed
        let last_blocks_to_analyse: u64 = self.last_blocks.unwrap_or(100);
        let blocks_iter = LastNBlocksIterator::new(last_blocks_to_analyse, chain_store.clone());

        // Analyse
        analyse_gas_usage(blocks_iter, &chain_store, &epoch_manager);

        Ok(())
    }
}

fn analyse_gas_usage(
    blocks_iter: impl Iterator<Item = Block>,
    _chain_store: &ChainStore,
    _epoch_manager: &EpochManager,
) {
    for block in blocks_iter {
        println!("Analysing block with height: {}", block.header().height());
        // TODO: Analyse
    }
}
