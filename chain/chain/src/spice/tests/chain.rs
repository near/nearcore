use crate::Chain;
use crate::spice::chain::SpiceChainReader;
use crate::spice::tests::core::{build_block, process_block, setup};
use assert_matches::assert_matches;
use near_chain_configs::{MutableConfigValue, TrackedShardsConfig};
use near_chain_primitives::Error;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::hash::CryptoHash;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::adapter::StoreAdapter as _;
use std::sync::Arc;

fn spice_chain_reader(
    chain: &Chain,
    tracked_shards_config: TrackedShardsConfig,
) -> SpiceChainReader {
    let shard_tracker = ShardTracker::new(
        tracked_shards_config,
        chain.epoch_manager.clone(),
        MutableConfigValue::new(None, "validator_signer"),
    );
    SpiceChainReader::new(
        chain.chain_store().chain_store(),
        chain.epoch_manager.clone(),
        shard_tracker,
    )
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_block_is_not_executed_until_every_tracked_shard_has_chunk_extra() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let reader = spice_chain_reader(&chain, TrackedShardsConfig::AllShards);

    let shard_uids = chain.epoch_manager.shard_uids(block.header().epoch_id()).unwrap();
    let (last_shard_uid, other_shard_uids) = shard_uids.split_last().unwrap();
    let chunk_extra = Arc::new(ChunkExtra::new_with_only_state_root(&CryptoHash::default()));
    let mut store_update = chain.chain_store.store_update();
    for shard_uid in other_shard_uids {
        store_update.save_chunk_extra(block.hash(), shard_uid, chunk_extra.clone());
    }
    store_update.commit().unwrap();
    assert_matches!(reader.check_block_executed(block.header()), Err(Error::DBNotFoundErr(_)));

    let mut store_update = chain.chain_store.store_update();
    store_update.save_chunk_extra(block.hash(), last_shard_uid, chunk_extra);
    store_update.commit().unwrap();
    reader.check_block_executed(block.header()).unwrap();
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_block_is_executed_when_no_shards_are_tracked() {
    let (mut chain, _core_reader) = setup();
    let genesis = chain.genesis_block();
    let block = build_block(&chain, &genesis, vec![]);
    process_block(&mut chain, block.clone());
    let reader = spice_chain_reader(&chain, TrackedShardsConfig::NoShards);

    reader.check_block_executed(block.header()).unwrap();
}
