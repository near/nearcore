mod all_stake_fallback;
mod core;
mod core_writer_actor;
mod header_chunk_endorsements;

use crate::Chain;
use crate::test_utils::{get_chain_with_genesis, get_fake_next_block_chunk_headers};
use near_async::time::Clock;
use near_chain_configs::Genesis;
use near_primitives::block::Block;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::test_utils::{
    TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
};
use near_primitives::types::{NumShards, ProtocolVersion, ShardId};
use std::sync::Arc;

/// Saves the block and records it in the epoch manager the way block postprocessing
/// does, so epoch lookups keyed on its hash resolve, without running block
/// processing.
pub(crate) fn save_and_record_block(
    chain: &mut Chain,
    block: &Arc<Block>,
    protocol_version: ProtocolVersion,
) {
    let mut store_update = chain.chain_store.store_update();
    store_update.save_block(block.clone());
    store_update.save_block_header(block.header().clone()).unwrap();
    let block_info = BlockInfo::from_header(block.header(), 0, protocol_version);
    let epoch_manager_update = chain
        .epoch_manager
        .add_validator_proposals(block_info, *block.header().random_value())
        .unwrap();
    store_update.merge(epoch_manager_update.into());
    store_update.commit().unwrap();
}

/// A chain with a pre-spice genesis, for the activation-boundary tests. Its blocks
/// are fabricated with [`add_pre_spice_block`], not processed.
pub(crate) fn setup_pre_spice_chain(num_shards: NumShards) -> Chain {
    let mut genesis =
        Genesis::test_sharded(Clock::real(), vec!["test1".parse().unwrap()], 1, num_shards);
    genesis.config.protocol_version = pre_spice_protocol_version();
    get_chain_with_genesis(Clock::real(), genesis)
}

/// Fabricates, saves and records the next pre-spice block: shards in
/// `new_chunk_shards` get a new (fake) chunk header, every other shard carries the
/// previous block's header, i.e. its chunk is missing.
pub(crate) fn add_pre_spice_block(
    chain: &mut Chain,
    prev_block: &Block,
    new_chunk_shards: &[ShardId],
) -> Arc<Block> {
    let epoch_manager = chain.epoch_manager.clone();
    let new_chunks = get_fake_next_block_chunk_headers(prev_block, epoch_manager.as_ref());
    let chunks: Vec<_> = prev_block
        .chunks()
        .iter_raw()
        .zip(new_chunks)
        .map(
            |(carried, new)| {
                if new_chunk_shards.contains(&new.shard_id()) { new } else { carried.clone() }
            },
        )
        .collect();
    let signer = Arc::new(create_test_signer("test1"));
    let block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
        .chunks(chunks)
        .protocol_version(pre_spice_protocol_version())
        .build();
    save_and_record_block(chain, &block, pre_spice_protocol_version());
    block
}
