use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_store::{Mode, NodeStorage};
use nearcore::load_config;
use nearcore::NightshadeRuntime;
use std::path::Path;

#[derive(clap::Parser)]
pub struct UndoBlockCommand {}

impl UndoBlockCommand {
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let store_opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            None,
        );

        let storage = store_opener.open_in_mode(Mode::ReadWrite).unwrap();
        let store = storage.get_hot_store();

        let runtime = NightshadeRuntime::from_config(home_dir, store.clone(), &near_config);

        let mut chain_store = ChainStore::new(
            store,
            near_config.genesis.config.genesis_height,
            near_config.client_config.save_trie_changes,
        );

        crate::undo_block(&mut chain_store, &*runtime)
    }
}
