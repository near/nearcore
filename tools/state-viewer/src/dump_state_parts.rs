use near_chain::{ChainStore, ChainStoreAccess, RuntimeAdapter};
use near_primitives::state_part::PartId;
use near_primitives::syncing::get_num_state_parts;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ShardId;
use near_store::Store;
use nearcore::{NearConfig, NightshadeRuntime};
use std::path::Path;
use std::sync::Arc;

pub(crate) fn dump_state_parts(
    sync_prev_hash: CryptoHash,
    shard_id: ShardId,
    part_id: Option<u64>,
    home_dir: &Path,
    near_config: NearConfig,
    store: Store,
    output_dir: &Path,
) {
    let runtime_adapter: Arc<dyn RuntimeAdapter> =
        Arc::new(NightshadeRuntime::from_config(home_dir, store.clone(), &near_config));

    let chain_store = ChainStore::new(
        store.clone(),
        near_config.genesis.config.genesis_height,
        !near_config.client_config.archive,
    );

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

    std::fs::create_dir_all(output_dir).unwrap();
    for part_id in if let Some(part_id) = part_id { part_id..part_id + 1 } else { 0..num_parts } {
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
        std::fs::write(filename.clone(), state_part).unwrap();
        tracing::debug!(
            "part_id: {}, result length: {}, wrote {}",
            part_id,
            len,
            filename.display()
        );
    }
}
