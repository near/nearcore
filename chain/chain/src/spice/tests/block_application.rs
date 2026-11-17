use super::core::{build_block, process_block, setup};
use crate::Chain;
use crate::spice::block_application::{apply_block_postprocessing, apply_shard_postprocessing};
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::adapter::{StoreAdapter as _, StoreUpdateAdapter as _};
use near_store::flat::{
    BlockInfo, FlatStateChanges, FlatStateDelta, FlatStateDeltaMetadata, FlatStorageStatus,
};
use std::sync::Arc;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_postprocessing_follows_the_final_execution_head_only_for_applied_shards() {
    let (chain, blocks, shard_uids) = setup_with_flat_storage_and_memtries();
    let genesis = &blocks[0];
    let &[applied_shard, lagging_shard, ..] = shard_uids.as_slice() else {
        panic!("the test needs an applied shard and a lagging one");
    };
    // Both shards start with their flat head and their memtrie root at genesis.
    for shard_uid in [applied_shard, lagging_shard] {
        assert_eq!(flat_head(&chain, shard_uid), *genesis.hash());
        assert_eq!(num_memtrie_roots(&chain, shard_uid), 1);
    }

    let block = blocks.last().unwrap();
    let last_final_block = chain.get_block_header(block.header().last_final_block()).unwrap();
    // The final execution head `block` sets is far enough to drop the genesis memtrie roots.
    assert!(last_final_block.prev_height().unwrap() > genesis.header().height());
    // The applied shard has applied that head.
    let mut store_update = chain.chain_store().store().store_update();
    store_update.chunk_store_update().set_chunk_extra(
        last_final_block.hash(),
        &applied_shard,
        &ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
    );
    store_update.commit();

    apply_block_postprocessing(
        chain.runtime_adapter.as_ref(),
        chain.epoch_manager.as_ref(),
        &chain.chain_store().chain_store(),
        block,
    )
    .unwrap();

    let final_execution_head = chain.chain_store().spice_final_execution_head().unwrap();
    assert_eq!(&final_execution_head.last_block_hash, block.header().last_final_block());
    // A shard that has applied the new final execution head follows it: its flat head moves to the
    // block before the head, and its memtrie roots below the head are dropped.
    assert_eq!(flat_head(&chain, applied_shard), final_execution_head.prev_block_hash);
    assert_eq!(num_memtrie_roots(&chain, applied_shard), 0);
    // A shard that has not, like one still being caught up for the next epoch, keeps its flat head
    // and every root, such as the one a state sync loaded.
    assert_eq!(flat_head(&chain, lagging_shard), *genesis.hash());
    assert_eq!(num_memtrie_roots(&chain, lagging_shard), 1);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_shard_postprocessing_follows_a_final_block() {
    let (chain, blocks, shard_uids) = setup_with_flat_storage_and_memtries();
    let &[shard_uid, other_shard_uid, ..] = shard_uids.as_slice() else {
        panic!("the test needs two shards");
    };
    let final_block = blocks[2].header();

    apply_shard_postprocessing(
        chain.runtime_adapter.as_ref(),
        &chain.chain_store().chain_store(),
        shard_uid,
        final_block,
    )
    .unwrap();

    // The shard follows the final block: its flat head moves to the block before it, and its
    // memtrie roots up to that block are dropped.
    assert_eq!(flat_head(&chain, shard_uid), *final_block.prev_hash());
    assert_eq!(num_memtrie_roots(&chain, shard_uid), 0);
    // The other shards are left alone.
    assert_eq!(flat_head(&chain, other_shard_uid), *blocks[0].hash());
    assert_eq!(num_memtrie_roots(&chain, other_shard_uid), 1);
}

/// A chain of a few blocks whose shards all have their flat head and a memtrie root at genesis, and
/// a flat state delta for every block after it.
fn setup_with_flat_storage_and_memtries() -> (Chain, Vec<Arc<Block>>, Vec<ShardUId>) {
    const NUM_BLOCKS_AFTER_GENESIS: usize = 5;
    let (mut chain, _) = setup();
    let mut blocks = vec![chain.genesis_block()];
    for _ in 0..NUM_BLOCKS_AFTER_GENESIS {
        let block = build_block(&chain, blocks.last().unwrap(), vec![]);
        process_block(&mut chain, block.clone());
        blocks.push(block);
    }
    chain.init_flat_storage().unwrap();
    let shard_uids: Vec<ShardUId> = chain
        .epoch_manager
        .get_shard_layout(blocks[0].header().epoch_id())
        .unwrap()
        .shard_uids()
        .collect();
    let flat_storage_manager = chain.runtime_adapter.get_flat_storage_manager();
    let tries = chain.runtime_adapter.get_tries();
    for &shard_uid in &shard_uids {
        tries.load_memtrie(&shard_uid, None, false).unwrap();
        let flat_storage = flat_storage_manager.get_flat_storage_for_shard(shard_uid).unwrap();
        for block in &blocks[1..] {
            let header = block.header();
            let metadata = FlatStateDeltaMetadata {
                block: BlockInfo {
                    hash: *header.hash(),
                    height: header.height(),
                    prev_hash: *header.prev_hash(),
                },
                prev_block_with_changes: None,
            };
            let delta = FlatStateDelta { metadata, changes: FlatStateChanges::default() };
            flat_storage.add_delta(delta).unwrap().commit();
        }
    }
    (chain, blocks, shard_uids)
}

fn flat_head(chain: &Chain, shard_uid: ShardUId) -> CryptoHash {
    match chain.runtime_adapter.get_flat_storage_manager().get_flat_storage_status(shard_uid) {
        FlatStorageStatus::Ready(status) => status.flat_head.hash,
        status => panic!("flat storage of {shard_uid} is not ready: {status:?}"),
    }
}

fn num_memtrie_roots(chain: &Chain, shard_uid: ShardUId) -> usize {
    chain.runtime_adapter.get_tries().get_memtries(shard_uid).unwrap().read().num_roots()
}
