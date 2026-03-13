use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::TrackedShardsConfig;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{EncodedShardChunk, ShardChunkHeader, ShardChunkHeaderV3};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::Gas;
use near_store::DBCol;

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
    let epoch_manager = env.node(malicious_node).client().epoch_manager.clone();
    let peer_manager_handle = env.node_datas[malicious_node].peer_manager_sender.actor_handle();
    let peer_manager = env.test_loop.data.get_mut(&peer_manager_handle);

    // Intercept all chunk messages from this node and corrupt the tx_root in
    // the header. The parts and encoded_merkle_root remain valid so individual
    // part validation passes and RS decode succeeds, but validate_chunk_proofs
    // will fail because tx_root doesn't match the actual chunk body.
    peer_manager.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
        match request {
            NetworkRequests::PartialEncodedChunkMessage {
                account_id,
                mut partial_encoded_chunk,
            } => {
                let header = partial_encoded_chunk.header;
                let epoch_id =
                    epoch_manager.get_epoch_id_from_prev_block(header.prev_block_hash()).unwrap();
                let chunk_producer_info = epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        shard_id: header.shard_id(),
                        epoch_id,
                        height_created: header.height_created(),
                    })
                    .unwrap();
                let signer = create_test_signer(chunk_producer_info.account_id().as_str());

                // Replace tx_root with garbage so validate_chunk_proofs fails.
                let bad_tx_root = CryptoHash::hash_bytes(b"malicious");
                let new_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new_for_spice(
                    *header.prev_block_hash(),
                    *header.encoded_merkle_root(),
                    header.encoded_length(),
                    header.height_created(),
                    header.shard_id(),
                    *header.prev_outgoing_receipts_root(),
                    bad_tx_root,
                    &signer,
                ));
                partial_encoded_chunk.header = new_header;
                Some(NetworkRequests::PartialEncodedChunkMessage {
                    account_id,
                    partial_encoded_chunk,
                })
            }
            _ => Some(request),
        }
    }));

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
        invalid_chunk_count += 1;
    }
    assert!(invalid_chunk_count > 0, "expected at least one invalid chunk stored as evidence");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
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

    let (malicious_node, honest_node) = (0, 1);
    let epoch_manager = env.node(malicious_node).client().epoch_manager.clone();
    let peer_manager_handle = env.node_datas[malicious_node].peer_manager_sender.actor_handle();
    let peer_manager = env.test_loop.data.get_mut(&peer_manager_handle);

    // Intercept all chunk messages from this node and corrupt the tx_root in
    // the header. The parts and encoded_merkle_root remain valid so individual
    // part validation passes and RS decode succeeds, but validate_chunk_proofs
    // will fail because tx_root doesn't match the actual chunk body.
    peer_manager.register_override_handler(Box::new(move |request| -> Option<NetworkRequests> {
        match request {
            NetworkRequests::PartialEncodedChunkMessage {
                account_id,
                mut partial_encoded_chunk,
            } => {
                let header = partial_encoded_chunk.header;
                let epoch_id =
                    epoch_manager.get_epoch_id_from_prev_block(header.prev_block_hash()).unwrap();
                let chunk_producer_info = epoch_manager
                    .get_chunk_producer_info(&ChunkProductionKey {
                        shard_id: header.shard_id(),
                        epoch_id,
                        height_created: header.height_created(),
                    })
                    .unwrap();
                let signer = create_test_signer(chunk_producer_info.account_id().as_str());

                let bad_tx_root = CryptoHash::hash_bytes(b"malicious");
                let new_header = ShardChunkHeader::V3(ShardChunkHeaderV3::new_for_spice(
                    *header.prev_block_hash(),
                    *header.encoded_merkle_root(),
                    header.encoded_length(),
                    header.height_created(),
                    header.shard_id(),
                    *header.prev_outgoing_receipts_root(),
                    bad_tx_root,
                    &signer,
                ));
                partial_encoded_chunk.header = new_header;
                Some(NetworkRequests::PartialEncodedChunkMessage {
                    account_id,
                    partial_encoded_chunk,
                })
            }
            _ => Some(request),
        }
    }));

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

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
