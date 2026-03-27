use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use assert_matches::assert_matches;
use near_chain::ChainStoreAccess;
use near_chain::near_chain_primitives::Error;
use near_chain_configs::TrackedShardsConfig;
use near_client::NetworkAdversarialMessage;
use near_client::client_actor::AdvProduceChunksMode;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::EncodedShardChunk;
use near_primitives::types::AccountId;
use near_primitives::types::Gas;
use near_store::DBCol;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    for (_, value) in node.store().iter(DBCol::InvalidChunks) {
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
    assert!(env.node(honest_node).store().iter(DBCol::InvalidChunks).count() > 0);

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
    for (key, _) in honest_store.iter(DBCol::InvalidChunks) {
        assert!(
            sync_store.exists(DBCol::InvalidChunks, key.as_ref()),
            "syncing node missing invalid chunk that the honest node detected",
        );
    }
}

/// Test that validators endorse chunks containing an `proof_of_invalid_chunk`
/// (witness produced when a malicious chunk producer sends a corrupted chunk
/// body). Verifies the full end-to-end flow: witnesses are self-contained,
/// validators can pre-validate and execute them, and ALL assigned validators
/// endorse the result. Also verifies that gas_used is zero for blocks with
/// invalid chunks (empty chunk was executed).
#[cfg(feature = "test_features")]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_witness_validation_with_invalid_chunk() {
    init_test_logger();

    let num_producers = 4;
    let num_validators = 2;
    let mut env = TestLoopBuilder::new().validators(num_producers, num_validators).build();

    // Collect handles before mutably borrowing env.test_loop.
    let handles: Vec<_> =
        env.node_datas.iter().map(|nd| nd.peer_manager_sender.actor_handle()).collect();

    // Track which validators endorsed each block hash.
    let endorsements_by_block: Arc<Mutex<HashMap<CryptoHash, HashSet<AccountId>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    for handle in handles {
        let map = endorsements_by_block.clone();
        let peer_actor = env.test_loop.data.get_mut(&handle);
        peer_actor.register_override_handler(Box::new(move |request| {
            if let NetworkRequests::SpiceChunkEndorsement(_, endorsement) = &request {
                map.lock()
                    .entry(*endorsement.block_hash())
                    .or_default()
                    .insert(endorsement.account_id().clone());
            }
            Some(request)
        }));
    }

    let (malicious_node, honest_node) = (0, 1);
    env.node_runner(malicious_node).send_adversarial_message(
        NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::ProduceWithCorruptedTxRoot,
        ),
    );

    // Run long enough for the malicious node to be scheduled as chunk producer.
    env.node_runner(honest_node).run_for_number_of_blocks(10);

    // Verify that each block containing an invalid chunk was endorsed by ALL
    // assigned validators, and that gas_used is zero (empty chunk executed).
    let node = env.node(honest_node);
    let chain_store = node.client().chain.chain_store();
    let epoch_manager = &node.client().epoch_manager;
    let endorsements = endorsements_by_block.lock();
    let mut invalid_chunk_count = 0;
    for (_, value) in node.store().iter(DBCol::InvalidChunks) {
        let chunk: EncodedShardChunk = borsh::from_slice(&value).unwrap();
        let header = chunk.cloned_header();
        let shard_id = header.shard_id();
        let height = header.height_created();
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        let epoch_id = epoch_manager.get_epoch_id(&block_hash).unwrap();

        // Assert ALL assigned validators endorsed.
        let assignments =
            epoch_manager.get_chunk_validator_assignments(&epoch_id, shard_id, height).unwrap();
        let expected = assignments.len();
        let block_endorsers = endorsements.get(&block_hash).unwrap_or_else(|| {
            panic!("block at height {height} with invalid chunk received no endorsements")
        });
        let received = block_endorsers.len();
        assert_eq!(
            expected, received,
            "block at height {height}: expected {expected} endorsements but got {received}",
        );
        for validator in assignments.ordered_chunk_validators() {
            assert!(
                block_endorsers.contains(&validator),
                "validator {validator} did not endorse block at height {height}",
            );
        }

        // Assert gas_used == Gas::ZERO (empty chunk was executed).
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
