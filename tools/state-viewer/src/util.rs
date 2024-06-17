use std::{path::Path, sync::Arc};

use near_chain::{types::Tip, Block, BlockHeader, ChainStore, ChainStoreAccess};
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::types::{BlockHeight, StateRoot};
use near_store::{ShardUId, Store};
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};

pub enum LoadTrieMode {
    /// Load latest state
    Latest,
    /// Load prev state at some height
    Height(BlockHeight),
    /// Load the prev state of the last final block from some height
    LastFinalFromHeight(BlockHeight),
}

pub fn load_trie(
    store: Store,
    home_dir: &Path,
    near_config: &NearConfig,
) -> (Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, Vec<StateRoot>, BlockHeader) {
    load_trie_stop_at_height(store, home_dir, near_config, LoadTrieMode::Latest)
}

pub fn load_trie_stop_at_height(
    store: Store,
    home_dir: &Path,
    near_config: &NearConfig,
    mode: LoadTrieMode,
) -> (Arc<EpochManagerHandle>, Arc<NightshadeRuntime>, Vec<StateRoot>, BlockHeader) {
    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &near_config.genesis.config);
    let runtime =
        NightshadeRuntime::from_config(home_dir, store, near_config, epoch_manager.clone());
    let runtime = runtime.expect("could not create the transaction runtime");

    let head = chain_store.head().unwrap();
    let block = match mode {
        LoadTrieMode::LastFinalFromHeight(height) => {
            get_last_final_from_height(height, &head, &chain_store)
        }
        LoadTrieMode::Height(height) => {
            let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
            chain_store.get_block(&block_hash).unwrap()
        }
        LoadTrieMode::Latest => chain_store.get_block(&head.last_block_hash).unwrap(),
    };
    let shard_layout = epoch_manager.get_shard_layout(&block.header().epoch_id()).unwrap();
    let mut state_roots = vec![];
    for chunk in block.chunks().iter() {
        let shard_uid = ShardUId::from_shard_id_and_layout(chunk.shard_id(), &shard_layout);
        let chunk_extra = chain_store.get_chunk_extra(&head.last_block_hash, &shard_uid).unwrap();
        let state_root = *chunk_extra.state_root();
        state_roots.push(state_root);
    }

    (epoch_manager, runtime, state_roots, block.header().clone())
}

/// find the first final block whose height is at least `height`.
fn get_last_final_from_height(height: u64, head: &Tip, chain_store: &ChainStore) -> Block {
    let mut cur_height = height + 1;
    loop {
        if cur_height >= head.height {
            panic!("No final block with height >= {} exists", height);
        }
        let cur_block_hash = match chain_store.get_block_hash_by_height(cur_height) {
            Ok(hash) => hash,
            Err(_) => {
                cur_height += 1;
                continue;
            }
        };
        let last_final_block_hash =
            *chain_store.get_block_header(&cur_block_hash).unwrap().last_final_block();
        let last_final_block = chain_store.get_block(&last_final_block_hash).unwrap();
        if last_final_block.header().height() >= height {
            break last_final_block;
        } else {
            cur_height += 1;
            continue;
        }
    }
}
