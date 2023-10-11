use crate::utils::{flat_head, flat_head_state_root, open_rocksdb};
use near_epoch_manager::EpochManager;
use near_primitives::block::Tip;
use near_primitives::block_header::BlockHeader;
use near_primitives::types::ShardId;
use near_store::trie::mem::loading::load_trie_from_flat_state;
use near_store::{DBCol, ShardUId, HEAD_KEY};
use nearcore::NearConfig;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Command to load an in-memory trie for research purposes.
#[derive(clap::Parser)]
pub struct LoadMemTrieCommand {
    #[clap(long)]
    shard_id: ShardId,
}

impl LoadMemTrieCommand {
    pub fn run(&self, near_config: NearConfig, home: &Path) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home, near_store::Mode::ReadOnly)?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        let genesis_config = &near_config.genesis.config;
        // Note: this is not necessarily correct; it's just an estimate of the shard layout,
        // so that users of this tool doesn't have to specify the full shard UID.
        let head =
            store.get_ser::<Tip>(DBCol::BlockMisc, HEAD_KEY).unwrap().unwrap().last_block_hash;
        let block_header = store
            .get_ser::<BlockHeader>(DBCol::BlockHeader, &borsh::to_vec(&head).unwrap())?
            .ok_or_else(|| anyhow::anyhow!("Block header not found"))?;
        let epoch_manager =
            EpochManager::new_from_genesis_config(store.clone(), &genesis_config).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id()).unwrap();

        let shard_uid = ShardUId::from_shard_id_and_layout(self.shard_id, &shard_layout);
        let state_root = flat_head_state_root(&store, &shard_uid);
        let flat_head_height = flat_head(&store, &shard_uid).height;

        let _trie = load_trie_from_flat_state(
            &store,
            shard_uid,
            state_root,
            flat_head_height,
            64 * 1024 * 1024 * 1024,
        )?;
        println!(
            "Loaded trie for shard {} at height {}, press Ctrl-C to exit.",
            self.shard_id, flat_head_height
        );
        std::thread::sleep(Duration::from_secs(10000000000));
        Ok(())
    }
}
