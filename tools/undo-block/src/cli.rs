use chrono::Utc;
use near_chain::types::LatestKnown;
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate};
use near_chain_configs::GenesisValidationMode;
use near_primitives::block::Tip;
use near_primitives::utils::to_timestamp;
use near_store::{Mode, NodeStorage};
use nearcore::load_config;
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

        let cold_config: Option<&near_store::StoreConfig> = near_config.config.cold_store.as_ref();
        let store_opener = NodeStorage::opener(
            home_dir,
            near_config.config.archive,
            &near_config.config.store,
            cold_config,
        );

        let storage = store_opener.open_in_mode(Mode::ReadWrite).unwrap();
        let store = storage.get_hot_store();

        let mut chain_store = ChainStore::new(
            store,
            near_config.genesis.config.genesis_height,
            near_config.client_config.save_trie_changes,
        );

        let current_head = chain_store.head()?;
        let current_head_hash = current_head.last_block_hash;
        let prev_hash = current_head.prev_block_hash;
        let prev_header = chain_store.get_block_header(&prev_hash)?;
        let prev_tip = Tip::from_header(&prev_header);

        tracing::info!(target: "neard", "trying to update head from block hash {} to block hash {}", current_head_hash, prev_hash);
        tracing::info!(target: "neard", "current head height is {}, trying to update to height {}", current_head.height, prev_tip.height);

        // stop if it's already the final block
        if chain_store.final_head()?.height >= current_head.height {
            return Err(anyhow::anyhow!("Cannot revert past final block"));
        }

        let mut chain_store_update = ChainStoreUpdate::new(&mut chain_store);

        // clear block data for current head
        chain_store_update.clear_block_data_undo_block(current_head_hash)?;

        chain_store_update.save_head(&prev_tip)?;

        chain_store_update.commit()?;

        chain_store.save_latest_known(LatestKnown {
            height: prev_tip.height,
            seen: to_timestamp(Utc::now()),
        })?;

        let new_chain_store_head = chain_store.head()?;
        let new_chain_store_header_head = chain_store.header_head()?;

        tracing::info!(target: "neard", "The current chain store shows head is at height {}", new_chain_store_head.height);
        tracing::info!(target: "neard", "The current chain store shows header head is at height {}", new_chain_store_header_head.height);
        Ok(())
    }
}
