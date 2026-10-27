use crate::setup::builder::TestLoopBuilder;
use assert_matches::assert_matches;
use near_chain::Error;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::{ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV3};
use near_primitives::types::SpiceChunkId;
use near_store::adapter::StoreAdapter as _;

/// Spice records the epoch's first block as the sync hash, rather than waiting for two new
/// chunks in every shard: every spice block carries a chunk for every shard, so the epoch's
/// first block is already a well-defined anchor for all of them.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_sync_hash_is_the_epochs_first_block() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).epoch_length(10).build();

    let genesis_height = env.node(0).client().chain.genesis().height();
    env.node_runner(0).run_until_certified(genesis_height + 25);

    let node = env.node(0);
    let chain = &node.client().chain;
    let head = chain.head().unwrap();
    let epoch_manager = &node.client().epoch_manager;
    assert!(
        chain.get_block_header(&head.last_block_hash).unwrap().is_spice(),
        "the test must run on a spice chain",
    );

    // Every epoch that still has a recorded sync hash must name that epoch's first block.
    let mut num_epochs_with_sync_hash = 0;
    let mut block_hash = head.last_block_hash;
    while let Some(sync_hash) = chain.get_sync_hash(&block_hash).unwrap() {
        let epoch_first_block =
            *epoch_manager.get_block_info(&block_hash).unwrap().epoch_first_block();
        assert_eq!(sync_hash, epoch_first_block);
        num_epochs_with_sync_hash += 1;
        block_hash = *chain.get_block_header(&epoch_first_block).unwrap().prev_hash();
    }
    // `remove_old_epochs` keeps sync hashes only for the head's epoch and the one before it.
    assert_eq!(num_epochs_with_sync_hash, 2);
}

/// The served spice state sync header hands out the state the sync block's chunk left behind,
/// and proves it by carrying the body of the block that certified that chunk: the body hashes
/// to the `block_body_hash` of a header the syncing node already holds from header sync, and
/// commits the chunk's execution result whole.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_header_proves_the_post_state_root() {
    const SHARDS_COUNT: usize = 2;
    init_test_logger();

    let mut env = TestLoopBuilder::new()
        .validators(2, 0)
        .num_shards(SHARDS_COUNT)
        .track_all_shards()
        .epoch_length(10)
        .build();

    let genesis_height = env.node(0).client().chain.genesis().height();
    env.node_runner(0).run_until_certified(genesis_height + 25);

    let node = env.node(0);
    let chain = &node.client().chain;
    let syncing_node = env.node(1);
    let head = chain.head().unwrap();
    let sync_hash = chain
        .get_sync_hash(&head.last_block_hash)
        .unwrap()
        .expect("the head epoch must have a sync hash by now");
    let sync_header = chain.get_block_header(&sync_hash).unwrap();
    let shard_ids = node.client().epoch_manager.shard_ids(sync_header.epoch_id()).unwrap();
    assert_eq!(shard_ids.len(), SHARDS_COUNT);

    for shard_id in shard_ids {
        let response = chain.state_sync_adapter.get_state_response_header(shard_id, sync_hash);
        let header = match response {
            Ok(ShardStateSyncResponseHeader::V3(header)) => header,
            other => panic!("spice must serve a V3 state sync header, got {other:?}"),
        };
        let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };

        // The certifying block sits above the sync block, and is the one the chain recorded
        // as certifying this chunk.
        let certifying = chain.get_block_header(&header.certifying_block_hash).unwrap();
        assert!(certifying.height() > sync_header.height());
        assert_eq!(
            node.store().chain_store().get_chunk_certifying_block(&chunk_id),
            Some(header.certifying_block_hash),
        );

        // Another node accepts the served header.
        syncing_node
            .client()
            .chain
            .state_sync_adapter
            .set_state_header(shard_id, sync_hash, ShardStateSyncResponseHeader::V3(header.clone()))
            .expect("valid header should pass the validation");

        // Wrong `state_root` is rejected
        assert_matches!(
            syncing_node.client().chain.state_sync_adapter.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(
                    ShardStateSyncResponseHeaderV3 {
                        state_root: CryptoHash::default(),
                        ..header.clone()
                    }
                ),
            ),
            Err(Error::Other(msg)) if msg.contains("state_root does not match")
        );

        let sync_block = chain.get_block(&sync_hash).unwrap();
        let other_body = sync_block.body().expect("a spice block has a versioned body").clone();

        // Wrong `certifying_block_body` is rejected
        assert_matches!(
            syncing_node.client().chain.state_sync_adapter.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(
                    ShardStateSyncResponseHeaderV3 {
                        certifying_block_body: other_body,
                        ..header.clone()
                    }
                ),
            ),
            Err(Error::Other(msg)) if msg.contains("body does not match")
        );

        // `certifying_block_body` which is not final yet is rejected
        assert_matches!(
            syncing_node.client().chain.state_sync_adapter.set_state_header(
                shard_id,
                sync_hash,
                ShardStateSyncResponseHeader::V3(
                    ShardStateSyncResponseHeaderV3 {
                        certifying_block_hash: head.last_block_hash,
                        certifying_block_body:
                            chain.get_block(&head.last_block_hash).unwrap().body().unwrap().clone(),
                        ..header.clone()
                    }
                ),
            ),
            Err(Error::Other(msg)) if msg.contains("certifying block does not follow the sync block")
        );
    }
}
