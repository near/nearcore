use chrono::Utc;
use near_chain::types::LatestKnown;
use near_chain::{ChainStore, ChainStoreAccess, ChainStoreUpdate, RuntimeWithEpochManagerAdapter};
use near_chain_configs::GenesisValidationMode;
use near_primitives::block::Tip;
use near_primitives::utils::to_timestamp;
use near_store::{Mode, NodeStorage};
use nearcore::load_config;
use nearcore::NightshadeRuntime;
use std::path::Path;
use std::sync::Arc;

#[derive(clap::Parser)]
pub struct ResetHeadToPrevCommand {
    // store_temperature: Temperature,
}

impl ResetHeadToPrevCommand {
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
            store.clone(),
            near_config.genesis.config.genesis_height,
            near_config.client_config.save_trie_changes,
        );

        let runtime_adapter: Arc<dyn RuntimeWithEpochManagerAdapter> =
            Arc::new(NightshadeRuntime::from_config(home_dir, store, &near_config));

        let head = chain_store.head().unwrap();
        let current_final_head = chain_store.final_head().unwrap();
        // let head_height = head.height;
        let head_hash = head.last_block_hash;
        // let head_block = chain_store.get_block(&head_hash);
        let prev_hash = head.prev_block_hash;
        let prev_header = chain_store.get_block_header(&prev_hash).unwrap();
        let prev_tip = Tip::from_header(&prev_header);
        // let prev_block = chain_store.get_block(&prev_hash).unwrap();

        tracing::info!(target: "neard", "trying to update head from block hash {} to block hash {}", head_hash, prev_hash);
        tracing::info!(target: "neard", "current head height is {}, trying to update to height {}", head.height, prev_tip.height);
        let mut chain_store_update = ChainStoreUpdate::new(&mut chain_store);

        // chain_store_update.dec_block_refcount(&prev_hash)?;

        // clear block data for current head
        chain_store_update.clear_block_data_head_reset(runtime_adapter.as_ref(), head_hash)?;

        chain_store_update.save_head(&prev_tip)?;
        if current_final_head.height >= prev_tip.height {
            chain_store_update.save_final_head(&prev_tip)?;
        }

        // update tail if tail is higher than the new head
        if chain_store_update.tail().unwrap() > prev_tip.height {
            chain_store_update.update_tail(prev_tip.height)?;
        }

        chain_store_update.commit()?;

        chain_store.save_latest_known(LatestKnown {
            height: prev_tip.height,
            seen: to_timestamp(Utc::now()),
        })?;

        let new_chain_store_head = chain_store.head().unwrap();
        let new_chain_store_header_head = chain_store.header_head().unwrap();

        tracing::info!(target: "neard", "The current chain store shows head is at height {}", new_chain_store_head.height);
        tracing::info!(target: "neard", "The current chain store shows header head is at height {}", new_chain_store_header_head.height);
        //tracing::info!(target: "neard", "The current chain store shows head is at block hash {}", new_chain_store_head.last_block_hash);
        Ok(())
    }
}
