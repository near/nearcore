use std::path::Path;

use near_chain::types::RuntimeAdapter;
use near_chain::{ChainStore, ChainStoreAccess};
use near_epoch_manager::EpochManagerAdapter;
use near_store::Store;
use nearcore::NearConfig;
use node_runtime::bootstrap_congestion_info;

use crate::util::load_trie;

#[derive(clap::Parser)]
pub enum CongestionControlCmd {
    /// Print the congestion information.
    Print,
    /// Run the congestion info bootstrapping.
    Bootstrap,
}

impl CongestionControlCmd {
    pub(crate) fn run(&self, home_dir: &Path, near_config: NearConfig, store: Store) {
        match self {
            CongestionControlCmd::Print => self.run_print(&near_config, store),
            CongestionControlCmd::Bootstrap => self.run_bootstrap(home_dir, &near_config, store),
        }
    }

    fn run_print(&self, near_config: &NearConfig, store: Store) {
        let chain_store = ChainStore::new(
            store.clone(),
            near_config.genesis.config.genesis_height,
            near_config.client_config.save_trie_changes,
        );
        let head = chain_store.head().unwrap();
        let block = chain_store.get_block(&head.last_block_hash).unwrap();

        for chunk_header in block.chunks().iter() {
            let congestion_info = chunk_header.congestion_info();
            println!("{:?} - {:?}", chunk_header.shard_id(), congestion_info);
        }
    }

    fn run_bootstrap(&self, home_dir: &Path, near_config: &NearConfig, store: Store) {
        let (epoch_manager, runtime, state_roots, block_header) =
            load_trie(store, home_dir, near_config);

        let prev_hash = block_header.prev_hash();

        for (shard_id, state_root) in state_roots.iter().enumerate() {
            let shard_id = shard_id as u64;
            let trie = runtime.get_trie_for_shard(shard_id, prev_hash, *state_root, true).unwrap();
            let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();
            let protocol_config = runtime.get_protocol_config(&epoch_id).unwrap();
            let runtime_config = protocol_config.runtime_config;

            let congestion_info =
                bootstrap_congestion_info(&trie, &runtime_config, shard_id).unwrap();

            println!("{:?} - {:?}", shard_id, congestion_info);
        }
    }
}
