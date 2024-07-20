use crate::utils::{prepare_memtrie_state_trimming, write_state_column};
use clap::Parser;
use near_chain_configs::{GenesisConfig, GenesisValidationMode};
use near_store::{DBCol, Store};
use nearcore::{load_config, open_storage};
use std::path::PathBuf;

/// For developers only. Aggressively trims the database for testing purposes.
#[derive(Parser)]
pub(crate) struct AggressiveTrimmingCommand {
    #[clap(long)]
    obliterate_disk_trie: bool,
}

impl AggressiveTrimmingCommand {
    pub(crate) fn run(&self, home: &PathBuf) -> anyhow::Result<()> {
        let mut near_config = load_config(home, GenesisValidationMode::UnsafeFast).unwrap();
        let node_storage = open_storage(&home, &mut near_config).unwrap();
        let store = node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        if self.obliterate_disk_trie {
            Self::obliterate_disk_trie(store, &near_config.genesis.config)?;
        }
        Ok(())
    }

    /// Delete the entire State column except those that are needed to load memtrie.
    /// This is used to TEST that we are able to rely on memtries only to run a node.
    /// It is NOT safe for production. Do not trim your nodes like this. It will break your node.
    fn obliterate_disk_trie(store: Store, genesis_config: &GenesisConfig) -> anyhow::Result<()> {
        let state_needed =
            prepare_memtrie_state_trimming(store.clone(), genesis_config, true)?.state_entries;

        // Now that we've read all the non-inlined keys into memory, delete the State column, and
        // write back these values.
        let mut update = store.store_update();
        update.delete_all(DBCol::State);
        update.commit()?;

        tracing::info!("Overwriting State column with only important keys...");
        write_state_column(&store, state_needed)?;
        tracing::info!("Done writing State column.");

        Ok(())
    }
}
