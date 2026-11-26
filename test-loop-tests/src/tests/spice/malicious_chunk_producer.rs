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
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::types::{Balance, BlockHeight, Gas, ShardId};
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
    let sync_node_idx = env.add_node("sync_node", new_node_state);

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

/// Under SPICE an invalid chunk has no `ShardChunk` in `DBCol::Chunks`. A node tracking every
/// shard must still drop a transaction from its pool once a block includes it, even when that
/// block carries an invalid chunk for an earlier shard.
#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_tx_leaves_pool_next_to_invalid_chunk() {
    init_test_logger();

    let num_producers = 8;
    let sender = create_account_id("usera");
    let receiver = create_account_id("userb");
    let mut env = TestLoopBuilder::new()
        .validators(num_producers, 0)
        .num_shards(2)
        .track_all_shards()
        .add_user_accounts([&sender, &receiver], Balance::from_near(1_000))
        .build();

    // Accounts sort after the boundary account, so they live in the last shard.
    let observer_node = env.node(0);
    let epoch_id = observer_node.head().epoch_id;
    let epoch_manager = observer_node.client().epoch_manager.clone();
    let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let invalid_shard_id = shard_layout.shard_ids().next().unwrap();
    let tx_shard_uid = shard_layout.account_id_to_shard_uid(&sender);
    assert_ne!(tx_shard_uid.shard_id(), invalid_shard_id);
    let tx_shard_producers = epoch_manager
        .get_epoch_chunk_producers_for_shard(&epoch_id, tx_shard_uid.shard_id())
        .unwrap();
    let invalid_shard_producers =
        epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, invalid_shard_id).unwrap();
    let malicious = invalid_shard_producers
        .iter()
        .find(|account| !tx_shard_producers.contains(account))
        .expect("a producer of only the first shard")
        .clone();
    let observer = tx_shard_producers[0].clone();
    let malicious_idx = env.account_data_idx(&malicious);
    env.node_runner(malicious_idx).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithCorruptedTxRoot,
        ),
    );

    let mut rounds_next_to_invalid_chunk = 0;
    for _ in 0..24 {
        let tx = env.node_for_account(&observer).tx_send_money(
            &sender,
            &receiver,
            Balance::from_millinear(1),
        );
        let tx_hash = tx.get_hash();
        // A chunk producer only keeps transactions it was handed itself, so every producer of
        // the shard gets one. The observer then holds a copy whichever of them includes it.
        for producer in &tx_shard_producers {
            env.node_for_account(producer).submit_tx(tx.clone());
        }
        env.runner_for_account(&observer).run_until(
            |node| tx_in_shard_chunk(node, tx_shard_uid.shard_id(), tx_hash),
            Duration::seconds(30),
        );

        let node = env.node_for_account(&observer);
        let block = node.head_block();
        let included_at = block.header().height();
        let invalid_next_to_tx = block.chunks().iter_new().any(|header| {
            header.shard_id() == invalid_shard_id
                && node
                    .client()
                    .chain
                    .chain_store()
                    .is_invalid_chunk(header.height_created(), header.chunk_hash())
                    .is_some()
        });
        if invalid_next_to_tx {
            rounds_next_to_invalid_chunk += 1;
        }
        let pooled = node
            .client()
            .chunk_producer
            .sharded_tx_pool
            .lock()
            .get_pool_iterator(tx_shard_uid)
            .map_or(0, |pool| pool.drain().len());
        assert_eq!(
            pooled, 0,
            "tx {tx_hash} included at height {included_at} is still in the pool of {observer} \
             (invalid chunk in the same block: {invalid_next_to_tx})",
        );
    }
    assert!(
        rounds_next_to_invalid_chunk > 0,
        "no transaction was included in a block with an invalid chunk, so the test proves nothing",
    );
}

/// Whether the chunk of `shard_id` in the head block of `node` carries `tx_hash`.
#[cfg(feature = "test_features")]
fn tx_in_shard_chunk(node: &TestLoopNode, shard_id: ShardId, tx_hash: CryptoHash) -> bool {
    let block = node.head_block();
    block.chunks().iter_new().filter(|header| header.shard_id() == shard_id).any(|header| {
        let chunk = node.client().chain.get_chunk(header.chunk_hash()).unwrap();
        chunk.to_transactions().iter().any(|tx| tx.get_hash() == tx_hash)
    })
}
