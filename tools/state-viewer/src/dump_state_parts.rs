use crate::epoch_info::iterate_and_filter;
use clap::Subcommand;
use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_epoch_manager::EpochManager;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::state_part::PartId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives::types::EpochId;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, EpochHeight, ShardId};
use near_store::Store;
use nearcore::{NearConfig, NightshadeRuntime};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

#[derive(Subcommand, Debug, Clone)]
pub(crate) enum EpochSelection {
    /// Current epoch.
    Current,
    /// Fetch the given epoch.
    EpochId { epoch_id: String },
    /// Fetch epochs at the given height.
    EpochHeight { epoch_height: EpochHeight },
    /// Fetch an epoch containing the given block hash.
    BlockHash { block_hash: String },
    /// Fetch an epoch containing the given block height.
    BlockHeight { block_height: BlockHeight },
}

impl EpochSelection {
    pub fn to_epoch_id(
        &self,
        store: Store,
        chain_store: &ChainStore,
        epoch_manager: &EpochManager,
    ) -> EpochId {
        match self {
            EpochSelection::Current => {
                epoch_manager.get_epoch_id(&chain_store.head().unwrap().last_block_hash).unwrap()
            }
            EpochSelection::EpochId { epoch_id } => {
                EpochId(CryptoHash::from_str(&epoch_id).unwrap())
            }
            EpochSelection::EpochHeight { epoch_height } => {
                // Fetch epochs at the given height.
                // There should only be one epoch at a given height. But this is a debug tool, let's check
                // if there are multiple epochs at a given height.
                let epoch_ids = iterate_and_filter(store, |epoch_info| {
                    epoch_info.epoch_height() == *epoch_height
                });
                assert_eq!(epoch_ids.len(), 1, "{:#?}", epoch_ids);
                epoch_ids[0].clone()
            }
            EpochSelection::BlockHash { block_hash } => {
                let block_hash = CryptoHash::from_str(&block_hash).unwrap();
                epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
            EpochSelection::BlockHeight { block_height } => {
                // Fetch an epoch containing the given block height.
                let block_hash = chain_store.get_block_hash_by_height(*block_height).unwrap();
                epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
        }
    }
}

/// Returns block hash of the last block of an epoch preceding the given `epoch_info`.
fn get_prev_hash_of_epoch(
    epoch_info: &EpochInfo,
    chain_store: &ChainStore,
    epoch_manager: &EpochManager,
) -> CryptoHash {
    let head = chain_store.head().unwrap();
    let mut cur_block_info = epoch_manager.get_block_info(&head.last_block_hash).unwrap();
    // EpochManager doesn't have an API that maps EpochId to Blocks, and this function works
    // around that limitation by iterating over the epochs.
    // This workaround is acceptable because:
    // 1) Extending EpochManager's API is a major change.
    // 2) This use case is not critical at all.
    loop {
        let cur_epoch_info = epoch_manager.get_epoch_info(cur_block_info.epoch_id()).unwrap();
        let cur_epoch_height = cur_epoch_info.epoch_height();
        assert!(
            cur_epoch_height >= epoch_info.epoch_height(),
            "cur_block_info: {:#?}, epoch_info.epoch_height: {}",
            cur_block_info,
            epoch_info.epoch_height()
        );
        let epoch_first_block_info =
            epoch_manager.get_block_info(cur_block_info.epoch_first_block()).unwrap();
        let prev_epoch_last_block_info =
            epoch_manager.get_block_info(epoch_first_block_info.prev_hash()).unwrap();

        if cur_epoch_height == epoch_info.epoch_height() {
            return *prev_epoch_last_block_info.hash();
        }

        cur_block_info = prev_epoch_last_block_info;
    }
}

pub(crate) fn dump_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_id: Option<u64>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    output_dir: &Path,
) {
    let runtime_adapter: Arc<dyn RuntimeAdapter> =
        Arc::new(NightshadeRuntime::from_config(home_dir, store.clone(), &near_config));
    let mut epoch_manager =
        EpochManager::new_from_genesis_config(store.clone(), &near_config.genesis.config)
            .expect("Failed to start Epoch Manager");
    let mut chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        near_config.client_config.save_trie_changes,
    );

    let epoch_id = epoch_selection.to_epoch_id(store, &mut chain_store, &mut epoch_manager);
    let epoch = runtime_adapter.get_epoch_info(&epoch_id).unwrap();
    let sync_prev_hash = get_prev_hash_of_epoch(&epoch, &mut chain_store, &mut epoch_manager);
    let sync_prev_block = chain_store.get_block(&sync_prev_hash).unwrap();

    assert!(runtime_adapter.is_next_block_epoch_start(&sync_prev_hash).unwrap());
    assert!(
        shard_id < sync_prev_block.chunks().len() as u64,
        "shard_id: {}, #shards: {}",
        shard_id,
        sync_prev_block.chunks().len()
    );
    let state_root = sync_prev_block.chunks()[shard_id as usize].prev_state_root();
    let state_root_node =
        runtime_adapter.get_state_root_node(shard_id, &sync_prev_hash, &state_root).unwrap();

    let num_parts = get_num_state_parts(state_root_node.memory_usage);
    tracing::info!(
        target: "dump-state-parts",
        epoch_height = epoch.epoch_height(),
        epoch_id = ?epoch_id.0,
        shard_id,
        num_parts,
        ?sync_prev_hash,
        "Dumping state as seen at the beginning of the specified epoch.",
    );

    std::fs::create_dir_all(output_dir).unwrap();
    for part_id in if let Some(part_id) = part_id { part_id..part_id + 1 } else { 0..num_parts } {
        let now = Instant::now();
        assert!(part_id < num_parts, "part_id: {}, num_parts: {}", part_id, num_parts);
        let state_part = runtime_adapter
            .obtain_state_part(
                shard_id,
                &sync_prev_hash,
                &state_root,
                PartId::new(part_id, num_parts),
            )
            .unwrap();
        let filename = output_dir.join(format!("state_part_{:06}", part_id));
        let len = state_part.len();
        std::fs::write(&filename, state_part).unwrap();
        tracing::info!(
            target: "dump-state-parts",
            part_id,
            part_length = len,
            ?filename,
            elapsed_sec = now.elapsed().as_secs_f64());
    }
}
