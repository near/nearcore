// A merkle path into `chunk_headers_root` proves that a chunk is in the block, not which shard it
// belongs to. `set_state_header` binds `chunk` and `prev_chunk_header` to the requested shard by
// checking the path against the shard's index.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::drop_condition::DropCondition;
use crate::setup::env::TestLoopEnv;
use near_async::time::Duration;
use near_chain::Chain;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::merklize;
use near_primitives::sharding::ChunkHashHeight;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{BlockHeight, ShardId, ShardIndex};
use std::collections::HashMap;

fn await_sync_hash(env: &mut TestLoopEnv) -> CryptoHash {
    env.node_runner(0).run_until_new_epoch();
    env.node_runner(0).run_until(
        |node| node.client().chain.get_sync_hash(&node.head().last_block_hash).unwrap().is_some(),
        Duration::seconds(20),
    );
    let node = env.node(0);
    node.client().chain.get_sync_hash(&node.head().last_block_hash).unwrap().unwrap()
}

/// Every merkle path and the state root node stay valid for the other shard, so only the chunk's
/// index in the block tells the two chunks apart.
fn replace_chunk_with_other_shard_chunk(
    header: &mut ShardStateSyncResponseHeader,
    other_shard_header: ShardStateSyncResponseHeader,
) {
    let (ShardStateSyncResponseHeader::V2(header), ShardStateSyncResponseHeader::V2(other)) =
        (header, other_shard_header)
    else {
        panic!("expected V2 state sync headers");
    };
    header.chunk = other.chunk;
    header.chunk_proof = other.chunk_proof;
    header.state_root_node = other.state_root_node;
}

fn replace_prev_chunk_with_chunk_of_shard(
    header: &mut ShardStateSyncResponseHeader,
    block_before_chunk: &Block,
    shard_index: ShardIndex,
) -> BlockHeight {
    let ShardStateSyncResponseHeader::V2(header) = header else {
        panic!("expected V2 state sync header");
    };
    let (_, chunk_proofs) = merklize(
        &block_before_chunk
            .chunks()
            .iter()
            .map(|chunk| ChunkHashHeight(chunk.chunk_hash().clone(), chunk.height_included()))
            .collect::<Vec<ChunkHashHeight>>(),
    );
    let chunk_header = block_before_chunk.chunks().get(shard_index).unwrap().clone();
    let chunk_height_included = chunk_header.height_included();
    header.prev_chunk_header = Some(chunk_header);
    header.prev_chunk_proof = Some(chunk_proofs[shard_index].clone());
    chunk_height_included
}

/// Check 4g requires the receipt proofs to end at the prev chunk's height, so a newer prev chunk
/// needs fewer of them. Returns how many were removed.
fn remove_incoming_receipts_proofs_at_or_below_height(
    chain: &Chain,
    header: &mut ShardStateSyncResponseHeader,
    height: BlockHeight,
) -> usize {
    let ShardStateSyncResponseHeader::V2(header) = header else {
        panic!("expected V2 state sync header");
    };
    let num_proofs = header.incoming_receipts_proofs.len();
    let num_proofs_above_height = header
        .incoming_receipts_proofs
        .iter()
        .take_while(|response| chain.get_block_header(&response.0).unwrap().height() > height)
        .count();
    header.incoming_receipts_proofs.truncate(num_proofs_above_height);
    header.root_proofs.truncate(num_proofs_above_height);
    num_proofs - num_proofs_above_height
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_set_state_header_rejects_chunk_from_other_shard() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .num_shards(2)
        .epoch_length(10)
        .track_all_shards()
        .build();

    let sync_hash = await_sync_hash(&mut env);
    let synced_shard_id = ShardId::new(0);
    let other_shard_id = ShardId::new(1);
    let server = &env.node(0).client().chain.state_sync_adapter;
    let genuine_header = server.compute_state_response_header(synced_shard_id, sync_hash).unwrap();
    let other_shard_header =
        server.compute_state_response_header(other_shard_id, sync_hash).unwrap();
    assert_ne!(genuine_header.synced_state_root(), other_shard_header.synced_state_root());

    let syncing_node_adapter = &env.node(1).client().chain.state_sync_adapter;
    syncing_node_adapter
        .set_state_header(synced_shard_id, sync_hash, genuine_header.clone())
        .unwrap();

    let mut modified_header = genuine_header;
    replace_chunk_with_other_shard_chunk(&mut modified_header, other_shard_header);

    let rejection = syncing_node_adapter
        .set_state_header(synced_shard_id, sync_hash, modified_header)
        .unwrap_err();
    assert!(rejection.to_string().contains("failed: chunk isn't included into block"));
}

#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_set_state_header_rejects_prev_chunk_from_other_shard() {
    init_test_logger();
    let synced_shard_id = ShardId::new(0);
    let other_shard_id = ShardId::new(1);
    // The synced shard misses its chunk at offset 2 of every epoch, so the block before its sync
    // chunk holds a newer chunk of the other shard than of the synced shard.
    let chunks_produced_by_height = HashMap::from([(synced_shard_id, vec![true, true, false])]);
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .num_shards(2)
        .epoch_length(10)
        .track_all_shards()
        .delay_warmup()
        .build()
        .drop(DropCondition::ChunksProducedByHeight(chunks_produced_by_height))
        .warmup();

    let sync_hash = await_sync_hash(&mut env);
    let node = env.node(0);
    let chain = &node.client().chain;
    let epoch_id = *chain.get_block_header(&sync_hash).unwrap().epoch_id();
    let shard_layout = node.client().epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let other_shard_index = shard_layout.get_shard_index(other_shard_id).unwrap();
    let genuine_header =
        chain.state_sync_adapter.compute_state_response_header(synced_shard_id, sync_hash).unwrap();
    let genuine_prev_chunk_height_included =
        genuine_header.cloned_prev_chunk_header().unwrap().height_included();

    let chunk_block_header =
        chain.get_block_header_by_height(genuine_header.chunk_height_included().unwrap()).unwrap();
    let block_before_chunk = chain.get_block(chunk_block_header.prev_hash()).unwrap();
    let mut modified_header = genuine_header.clone();
    let other_shard_chunk_height_included = replace_prev_chunk_with_chunk_of_shard(
        &mut modified_header,
        &block_before_chunk,
        other_shard_index,
    );
    assert!(other_shard_chunk_height_included > genuine_prev_chunk_height_included);
    let num_removed_proofs = remove_incoming_receipts_proofs_at_or_below_height(
        chain,
        &mut modified_header,
        other_shard_chunk_height_included,
    );
    assert!(num_removed_proofs > 0);

    let syncing_node_adapter = &env.node(1).client().chain.state_sync_adapter;
    syncing_node_adapter.set_state_header(synced_shard_id, sync_hash, genuine_header).unwrap();

    let rejection = syncing_node_adapter
        .set_state_header(synced_shard_id, sync_hash, modified_header)
        .unwrap_err();
    assert!(rejection.to_string().contains("failed: prev_chunk isn't included into block"));
}
