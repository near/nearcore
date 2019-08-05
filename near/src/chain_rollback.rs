use std::path::Path;
use crate::config::GenesisConfig;
use near_primitives::types::{BlockIndex};
use near_chain::{ChainStore, ChainStoreAccess, Tip};
use near_store::create_store;

pub fn chain_rollback_to(height: BlockIndex, data_dir: &str) -> Result<(), String> {
    let mut chain_store = ChainStore::new(create_store(data_dir));
    let mut store_update = chain_store.store_update();
    let block_header = {
        let block_hash = store_update.get_block_hash_by_height(height).map_err(|e| format!("{}", e))?;
        store_update.get_block_header(&block_hash).map_err(|e| format!("{}", e))?.clone()
    };
    let tip = Tip::from_header(&block_header);
    store_update.save_head(&tip);
    store_update.save_sync_head(&tip);
    Ok(store_update.commit().map_err(|e| format!("{}", e))?)
}
