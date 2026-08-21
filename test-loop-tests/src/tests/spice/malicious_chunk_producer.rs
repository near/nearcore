use crate::setup::builder::{ArchivalKind, TestLoopBuilder};
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::near_chain_primitives::Error;
use near_chain_configs::TrackedShardsConfig;
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Tip;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::types::{BlockHeight, Gas};
use near_primitives::utils::{get_endorsements_key_prefix, get_spice_invalid_chunk_key_rev};
use near_store::DBCol;
use near_store::db::{COLD_HEAD_KEY, Database};

/// Test that a malicious chunk producer sending chunks with corrupted tx_root
/// triggers the invalid chunk path, and under SPICE the chain still progresses
/// using empty chunks.
#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_chain_with_malicious_chunk_producer() {
    init_test_logger();

    let num_producers = 4;
    let num_validators = 0;
    let mut env = TestLoopBuilder::new().validators(num_producers, num_validators).build();

    let (malicious_node, honest_node) = (0, 1);
    env.node_runner(malicious_node).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithCorruptedTxRoot,
        ),
    );

    // Run for enough blocks that the malicious node is scheduled as chunk
    // producer at least once. With 4 producers and 1 shard, each producer gets
    // roughly 1 in 4 slots.
    env.node_runner(honest_node).run_for_number_of_blocks(10);

    // Verify that at least one invalid chunk was persisted as evidence, that the
    // block at that height includes a chunk for the shard, and that the chunk
    // extra shows zero gas usage (proving an empty chunk was executed).
    let node = env.node(honest_node);
    let chain_store = node.client().chain.chain_store();
    let epoch_manager = &node.client().epoch_manager;
    let mut invalid_chunk_count = 0;
    for (_, value) in node.store().iter(DBCol::spice_invalid_chunks()) {
        let chunk: EncodedShardChunk = borsh::from_slice(&value).unwrap();
        let header = chunk.cloned_header();
        let shard_id = header.shard_id();
        let height = header.height_created();
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        let block = chain_store.get_block(&block_hash).unwrap();
        let chunk_hash = header.chunk_hash();
        assert!(
            block.chunks().iter_raw().any(|ch| ch.chunk_hash() == chunk_hash),
            "height {height}: block should contain the chunk hash from the invalid chunk",
        );
        // Verify execution produced zero gas usage (empty chunk).
        let epoch_id = epoch_manager.get_epoch_id(&block_hash).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let chunk_extra = chain_store.get_chunk_extra(&block_hash, &shard_uid).unwrap();
        assert_eq!(chunk_extra.gas_used(), Gas::ZERO, "empty chunk should use zero gas");

        // Verify that the invalid chunk was not stored as a valid ShardChunk in DBCol::Chunks.
        assert_matches!(chain_store.get_chunk(chunk_hash), Err(Error::ChunkMissing(_)));
        invalid_chunk_count += 1;
    }
    assert!(invalid_chunk_count > 0, "expected at least one invalid chunk stored as evidence");
}

/// Test that a node joining late can block-sync past malicious chunks that
/// were replaced with empty chunks. The syncing node must independently
/// detect the malicious chunks (via validate_chunk_proofs failure) and
/// replace them with empty chunks, which requires that peers have the
/// malicious parts persisted in DBCol::PartialChunks.
#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_block_sync_with_malicious_chunks() {
    init_test_logger();

    let num_producers = 4;
    let num_validators = 0;
    let cache_horizon = 3;
    let mut env = TestLoopBuilder::new()
        .validators(num_producers, num_validators)
        .config_modifier(move |config, _| {
            config.chunks_cache_height_horizon = cache_horizon;
        })
        .build();

    let malicious_node = 0;
    let honest_node = 1;
    env.node_runner(malicious_node).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithCorruptedTxRoot,
        ),
    );

    // Run past the cache horizon so that early chunks are evicted from the
    // in-memory cache and peers must serve parts from DBCol::PartialChunks.
    env.node_runner(honest_node).run_for_number_of_blocks(cache_horizon as usize + 10);

    // Sanity check: honest node detected at least one malicious chunk.
    assert!(env.node(honest_node).store().iter(DBCol::spice_invalid_chunks()).count() > 0);

    // Add a late-joining non-validator node that will block-sync.
    // It must track all shards so it actually fetches and validates chunks.
    let sync_account = create_account_id("sync_node");
    let new_node_state = env
        .node_state_builder()
        .account_id(&sync_account)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
        })
        .build();
    env.add_node("sync_node", new_node_state);
    let sync_node_idx = env.node_datas.len() - 1;

    // New node should catch up.
    let honest_height = env.node(honest_node).head().height;
    env.node_runner(sync_node_idx).run_until_head_height(honest_height);

    // The syncing node should have independently detected every malicious
    // chunk that the honest node detected.
    let sync_store = env.node_for_account(&sync_account).store();
    let honest_store = env.node(honest_node).store();
    for (key, _) in honest_store.iter(DBCol::spice_invalid_chunks()) {
        assert!(
            sync_store.exists(DBCol::spice_invalid_chunks(), key.as_ref()),
            "syncing node missing invalid chunk that the honest node detected",
        );
    }
}

/// Test that validators endorse chunks containing a `proof_of_invalid_chunk`
/// (witness produced when a malicious chunk producer sends a corrupted chunk
/// body). Verifies the full end-to-end flow by checking persisted endorsements.
#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_witness_validation_with_invalid_chunk() {
    init_test_logger();

    let num_producers = 4;
    let num_validators = 2;
    let mut env = TestLoopBuilder::new().validators(num_producers, num_validators).build();

    let (malicious_node, honest_node) = (0, 1);
    env.node_runner(malicious_node).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithCorruptedTxRoot,
        ),
    );

    // Run for enough blocks that the malicious node is scheduled as chunk
    // producer at least once. With 4 producers and 1 shard, each producer gets
    // roughly 1 in 4 slots.
    env.node_runner(honest_node).run_for_number_of_blocks(10);

    let node = env.node(honest_node);
    let chain_store = node.client().chain.chain_store();
    let epoch_manager = &node.client().epoch_manager;
    let mut invalid_chunk_count = 0;
    for (_, value) in node.store().iter(DBCol::spice_invalid_chunks()) {
        let chunk: EncodedShardChunk = borsh::from_slice(&value).unwrap();
        let header = chunk.cloned_header();
        let shard_id = header.shard_id();
        let height = header.height_created();
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        let epoch_id = epoch_manager.get_epoch_id(&block_hash).unwrap();

        // Verify all assigned validators endorsed by scanning persisted endorsements.
        let assignments =
            epoch_manager.get_chunk_validator_assignments(&epoch_id, shard_id, height).unwrap();
        let prefix = get_endorsements_key_prefix(&block_hash, shard_id);
        let stored_count = node.store().iter_prefix(DBCol::endorsements(), &prefix).count();
        assert_eq!(
            assignments.len(),
            stored_count,
            "block at height {height}: expected {} endorsements but got {stored_count}",
            assignments.len(),
        );

        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let chunk_extra = chain_store.get_chunk_extra(&block_hash, &shard_uid).unwrap();
        assert_eq!(
            chunk_extra.gas_used(),
            Gas::ZERO,
            "block at height {height}: empty chunk should use zero gas",
        );

        invalid_chunk_count += 1;
    }
    assert!(invalid_chunk_count > 0, "expected at least one invalid chunk stored as evidence");
}

/// A malicious chunk's encoded body is the only record of what the producer sent: it gets no
/// `DBCol::Chunks` row, and `DBCol::PartialChunks` is not archived. Cold storage must keep it.
#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_invalid_chunk_copied_to_cold_storage() {
    init_test_logger();

    let epoch_length = 5;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .epoch_length(epoch_length)
        .enable_archival_node(ArchivalKind::Cold)
        .build();

    let malicious_node = 0;
    env.node_runner(malicious_node).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithCorruptedTxRoot,
        ),
    );

    env.archival_runner()
        .run_until(|node| !stored_invalid_chunk_keys(node).is_empty(), Duration::seconds(60));

    let invalid_chunk_keys = stored_invalid_chunk_keys(&env.archival_node());
    let last_invalid_chunk_height = invalid_chunk_keys
        .iter()
        .map(|key| get_spice_invalid_chunk_key_rev(key).unwrap().0)
        .max()
        .unwrap();

    env.archival_runner().run_until(
        |node| cold_head_height(node) > last_invalid_chunk_height,
        Duration::seconds(60),
    );

    let archival_idx = env.archival_data_idx();
    let cold_store_sender = env.node_datas[archival_idx].cold_store_sender.as_ref().unwrap();
    let cold_store_actor = env.test_loop.data.get(&cold_store_sender.actor_handle());
    let cold_db = cold_store_actor.get_cold_db();

    for key in &invalid_chunk_keys {
        let (height_created, chunk_hash) = get_spice_invalid_chunk_key_rev(key).unwrap();
        assert!(
            cold_db.get_raw_bytes(DBCol::spice_invalid_chunks(), key).is_some(),
            "invalid chunk {chunk_hash:?} at height {height_created} missing from cold storage",
        );
    }
}

#[cfg(feature = "test_features")]
fn stored_invalid_chunk_keys(node: &TestLoopNode) -> Vec<Box<[u8]>> {
    node.store().iter(DBCol::spice_invalid_chunks()).map(|(key, _)| key).collect()
}

#[cfg(feature = "test_features")]
fn cold_head_height(node: &TestLoopNode) -> BlockHeight {
    node.store().get_ser::<Tip>(DBCol::BlockMisc, COLD_HEAD_KEY).map_or(0, |tip| tip.height)
}
