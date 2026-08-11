use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients_with_rpc};
use crate::utils::node::TestLoopNode;
#[cfg(feature = "test_features")]
use borsh::BorshDeserialize;
use near_async::time::Duration;
use near_chain::spice::core::get_last_certified_block_header;
#[cfg(feature = "test_features")]
use near_client::NetworkAdversarialMessage;
#[cfg(feature = "test_features")]
use near_client::client_actor::AdvProduceChunksMode;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::{ChunkHash, EncodedShardChunk};
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::{get_block_shard_id, get_block_shard_id_rev};
use near_store::DBCol;
#[cfg(feature = "test_features")]
use std::collections::HashMap;

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_garbage_collection() {
    init_test_logger();

    let num_producers = 2;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients_with_rpc(&validators_spec);

    let epoch_length = 5;
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .epoch_length(epoch_length)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .gc_num_epochs_to_keep(1)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build();

    // We want to make sure that gc runs at least once and it doesn't trigger any asserts.
    env.rpc_runner().run_until(|node| node.tail() >= epoch_length, Duration::seconds(20));
}

// TODO(spice-resharding): Add a test for witness GC during resharding.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_garbage_collection_witnesses() {
    init_test_logger();

    let num_producers = 2;
    let num_validators = 0;
    let validators_spec = create_validators_spec(num_producers, num_validators);
    let clients = validators_spec_clients_with_rpc(&validators_spec);

    let epoch_length = 5;
    let shard_layout = ShardLayout::multi_shard(2, 0);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(shard_layout.clone())
        .epoch_length(epoch_length)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .gc_num_epochs_to_keep(1)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .delay_warmup()
        .build();

    // We delay endorsements to simulate slow execution validation causing execution to lag behind.
    let execution_delay = 4;
    env.delay_endorsements_propagation(execution_delay);
    env = env.warmup();

    // Use a chunk producer node (not RPC) since only chunk producers store witnesses.
    env.node_runner(0).run_until(
        |node| {
            let chain_store = &node.client().chain.chain_store;
            let final_head = chain_store.final_head().unwrap();
            get_last_certified_block_header(chain_store, &final_head.last_block_hash)
                .map_or(0, |header| header.height())
                >= 10
        },
        Duration::seconds(20),
    );
    let shard_tracker = env.node(0).client().shard_tracker.clone();
    let tracked_shards: Vec<_> = shard_layout
        .shard_ids()
        // This gets tracked shards for genesis, but it should not change during the test.
        .filter(|shard_id| shard_tracker.cares_about_shard(&CryptoHash::default(), *shard_id))
        .collect();
    assert_witness_gc_invariant(&env.node(0), &tracked_shards);
}

/// Verifies witness GC invariant, as seen from the final head: witnesses of certified blocks are
/// gone, witnesses of uncertified blocks are still there.
fn assert_witness_gc_invariant(node: &TestLoopNode, tracked_shards: &[ShardId]) {
    let chain_store = &node.client().chain.chain_store;
    let final_head = chain_store.final_head().unwrap();
    let last_certified_height =
        get_last_certified_block_header(chain_store, &final_head.last_block_hash).unwrap().height();
    let execution_head = chain_store.spice_execution_head().unwrap();
    let store = node.store();

    for (key, _) in store.iter(DBCol::witnesses()) {
        let (block_hash, shard_id) = get_block_shard_id_rev(&key).unwrap();
        let block_height = chain_store.get_block_height(&block_hash).unwrap();
        assert!(
            // Note we allow 1 block difference here since GC is async.
            block_height > last_certified_height - 1,
            "witness at height {block_height} shard {shard_id} should have been GC'd (last_certified_height = {last_certified_height})"
        );
    }

    for height in (last_certified_height + 1)..=execution_head.height {
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        for &shard_id in tracked_shards {
            assert!(
                store.get(DBCol::witnesses(), &get_block_shard_id(&block_hash, shard_id)).is_some(),
                "witness at height {height} shard {shard_id} should exist"
            );
        }
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chunk_certifying_block_index_is_collected_with_its_block() {
    init_test_logger();

    let epoch_length = 5;
    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(1)
        .build();

    env.node_runner(0).run_until(|node| node.tail() >= 2 * epoch_length, Duration::seconds(30));

    let node = env.node(0);
    let chain_store = &node.client().chain.chain_store;
    let tail = node.tail();
    let mut indexed_chunks = 0;
    for (key, _) in node.store().iter(DBCol::chunk_certifying_block()) {
        let (block_hash, shard_id) = get_block_shard_id_rev(&key).unwrap();
        let block = chain_store.get_block(&block_hash).unwrap_or_else(|_| {
            panic!(
                "chunk ({block_hash}, {shard_id}) is indexed but its block is collected, tail {tail}"
            )
        });
        assert!(block.header().height() >= tail);
        indexed_chunks += 1;
    }
    assert!(indexed_chunks > 0, "expected the index to hold chunks of retained blocks");
}

#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_invalid_chunks_are_collected() {
    init_test_logger();

    let mut env =
        TestLoopBuilder::new().validators(4, 0).epoch_length(5).gc_num_epochs_to_keep(1).build();

    let (malicious_node, honest_node) = (0, 1);
    env.node_runner(malicious_node).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithCorruptedTxRoot,
        ),
    );

    let mut seen_invalid_chunks: HashMap<ChunkHash, BlockHeight> = HashMap::new();
    env.node_runner(honest_node).run_until(
        |node| {
            seen_invalid_chunks.extend(stored_invalid_chunks(node));
            gc_passed_an_invalid_chunk(node, &seen_invalid_chunks)
        },
        Duration::seconds(60),
    );

    let node = env.node(honest_node);
    let chunk_tail = node.chunk_tail();
    let hashes_below_chunk_tail: Vec<_> = seen_invalid_chunks
        .iter()
        .filter(|(_, height)| **height < chunk_tail)
        .map(|(chunk_hash, _)| chunk_hash)
        .collect();
    assert!(!hashes_below_chunk_tail.is_empty(), "gc did not pass any invalid chunk");
    for chunk_hash in hashes_below_chunk_tail {
        for col in [DBCol::InvalidChunks, DBCol::PartialChunks, DBCol::Chunks] {
            assert!(!node.store().exists(col, chunk_hash.as_ref()));
        }
    }

    for height in stored_invalid_chunks(&node).into_values() {
        assert!(height >= chunk_tail, "invalid chunk at height {height}, chunk_tail {chunk_tail}");
    }
    for (key, _) in node.store().iter(DBCol::invalid_chunk_hashes_by_height()) {
        let height = BlockHeight::try_from_slice(&key).unwrap();
        assert!(height >= chunk_tail, "index row at height {height}, chunk_tail {chunk_tail}");
    }
}

#[cfg(feature = "test_features")]
fn gc_passed_an_invalid_chunk(
    node: &TestLoopNode,
    seen_invalid_chunks: &HashMap<ChunkHash, BlockHeight>,
) -> bool {
    let chunk_tail = node.chunk_tail();
    seen_invalid_chunks.values().any(|height| *height < chunk_tail)
}

#[cfg(feature = "test_features")]
fn stored_invalid_chunks(node: &TestLoopNode) -> HashMap<ChunkHash, BlockHeight> {
    node.store()
        .iter(DBCol::InvalidChunks)
        .map(|(_, value)| {
            let chunk: EncodedShardChunk = borsh::from_slice(&value).unwrap();
            (chunk.chunk_hash().clone(), chunk.height_created())
        })
        .collect()
}
