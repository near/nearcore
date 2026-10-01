use crate::setup::builder::TestLoopBuilder;
use crate::utils::node::TestLoopNode;
use near_o11y::testonly::init_test_logger;
use near_primitives::block_body::{BlockBody, SpiceCoreStatement, SpiceCoreStatements};
use near_primitives::hash::CryptoHash;
use near_primitives::state_sync::{ShardStateSyncResponseHeader, ShardStateSyncResponseHeaderV3};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{ChunkExecutionResult, EpochId, ShardId, SpiceChunkId};
use near_store::adapter::StoreAdapter as _;

/// The first block of the epoch `block_hash` belongs to, walking back over the canonical chain.
fn first_block_of_epoch(node: &TestLoopNode, block_hash: CryptoHash) -> CryptoHash {
    let chain = &node.client().chain;
    let mut header = chain.get_block_header(&block_hash).unwrap();
    let epoch_id = *header.epoch_id();
    loop {
        let prev = chain.get_block_header(header.prev_hash()).unwrap();
        if prev.epoch_id() != &epoch_id || prev.is_genesis() {
            return *header.hash();
        }
        header = prev;
    }
}

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
    assert!(
        chain.get_block_header(&head.last_block_hash).unwrap().is_spice(),
        "the test must run on a spice chain",
    );

    // Every epoch that still has a recorded sync hash must name that epoch's first block, and
    // there must be at least one such epoch by now. Older epochs are dropped from the column.
    let mut checked = 0;
    let mut seen_epochs: Vec<EpochId> = Vec::new();
    let mut hash = head.last_block_hash;
    loop {
        let header = chain.get_block_header(&hash).unwrap();
        if !seen_epochs.contains(header.epoch_id()) {
            seen_epochs.push(*header.epoch_id());
            if let Some(sync_hash) = chain.get_sync_hash(&hash).unwrap() {
                assert_eq!(
                    sync_hash,
                    first_block_of_epoch(&node, hash),
                    "sync hash for epoch {:?} must be its first block",
                    header.epoch_id(),
                );
                checked += 1;
            }
        }
        let prev_hash = *header.prev_hash();
        if prev_hash == CryptoHash::default() {
            break;
        }
        hash = prev_hash;
    }
    assert!(checked > 0, "expected at least one epoch with a recorded sync hash");
}

/// The served spice state sync header hands out the state the sync block's chunk left behind,
/// together with the body of the block that certified that chunk - a body the syncing node can
/// authenticate against a block header it already holds from header sync.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_header_carries_the_certifying_block_body() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).epoch_length(10).build();

    let genesis_height = env.node(0).client().chain.genesis().height();
    env.node_runner(0).run_until_certified(genesis_height + 25);

    let node = env.node(0);
    let chain = &node.client().chain;
    let sync_hash = sync_hash_at_head(&node);
    let sync_header = chain.get_block_header(&sync_hash).unwrap();
    let shard_ids = node.client().epoch_manager.shard_ids(sync_header.epoch_id()).unwrap();
    assert!(!shard_ids.is_empty());

    for shard_id in shard_ids {
        let header = spice_state_header(&node, shard_id, sync_hash);
        let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };

        // The certifying block is the one the chain recorded for the chunk, and it sits above
        // the sync block.
        assert_eq!(
            node.store().chain_store().get_chunk_certifying_block(&chunk_id),
            Some(header.certifying_block_hash),
        );
        let certifying = chain.get_block_header(&header.certifying_block_hash).unwrap();
        assert!(certifying.height() > sync_header.height());

        // Its header commits the body, and the body commits the execution result served.
        assert_eq!(certifying.block_body_hash(), Some(header.certifying_block_body.compute_hash()));
        let committed = header
            .certifying_block_body
            .spice_core_statements()
            .iter_execution_results()
            .find(|(id, _)| *id == &chunk_id)
            .map(|(_, result)| result);
        assert_eq!(committed, Some(&header.execution_result));

        // The node accepts the header it serves.
        chain
            .state_sync_adapter
            .set_state_header(shard_id, sync_hash, ShardStateSyncResponseHeader::V3(header))
            .unwrap();
    }
}

/// A syncing node must reject a spice state sync header whose execution result is not the one
/// the chain certified, however the tampering is dressed up.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_header_rejects_tampering() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).epoch_length(10).build();

    let genesis_height = env.node(0).client().chain.genesis().height();
    env.node_runner(0).run_until_certified(genesis_height + 25);

    let node = env.node(0);
    let chain = &node.client().chain;
    let sync_hash = sync_hash_at_head(&node);
    let shard_id = node
        .client()
        .epoch_manager
        .shard_ids(chain.get_block_header(&sync_hash).unwrap().epoch_id())
        .unwrap()[0];
    let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };
    let header = spice_state_header(&node, shard_id, sync_hash);
    let state_root = *header.execution_result.chunk_extra.state_root();
    let tampered_result = ChunkExecutionResult {
        chunk_extra: ChunkExtra::new_with_only_state_root(&state_root),
        outgoing_receipts_root: header.execution_result.outgoing_receipts_root,
    };
    assert_ne!(tampered_result, header.execution_result);

    let rejects = |tampered: ShardStateSyncResponseHeaderV3| {
        chain
            .state_sync_adapter
            .set_state_header(shard_id, sync_hash, ShardStateSyncResponseHeader::V3(tampered))
            .is_err()
    };

    // The same state root but a different `ChunkExtra`, which the body does not commit.
    let mut tampered = header.clone();
    tampered.execution_result = tampered_result.clone();
    assert!(rejects(tampered), "an execution result the body does not commit must be rejected");

    // A body rewritten to commit the tampered result no longer matches the header's hash.
    let mut tampered = header.clone();
    tampered.certifying_block_body =
        with_execution_result(&header.certifying_block_body, &chunk_id, &tampered_result);
    tampered.execution_result = tampered_result;
    assert!(rejects(tampered), "a body that does not match its header must be rejected");

    // A certifying block that does not descend from the sync block.
    let mut tampered = header;
    tampered.certifying_block_hash = sync_hash;
    assert!(rejects(tampered), "a certifying block below the sync block must be rejected");
}

fn sync_hash_at_head(node: &TestLoopNode) -> CryptoHash {
    let chain = &node.client().chain;
    let head = chain.head().unwrap();
    chain
        .get_sync_hash(&head.last_block_hash)
        .unwrap()
        .expect("the head epoch must have a sync hash by now")
}

fn spice_state_header(
    node: &TestLoopNode,
    shard_id: ShardId,
    sync_hash: CryptoHash,
) -> ShardStateSyncResponseHeaderV3 {
    let response =
        node.client().chain.state_sync_adapter.get_state_response_header(shard_id, sync_hash);
    match response {
        Ok(ShardStateSyncResponseHeader::V3(header)) => header,
        other => panic!("spice must serve a V3 state sync header, got {other:?}"),
    }
}

/// `body` with the execution result for `chunk_id` replaced by `result`.
fn with_execution_result(
    body: &BlockBody,
    chunk_id: &SpiceChunkId,
    result: &ChunkExecutionResult,
) -> BlockBody {
    let statements = body
        .spice_core_statements()
        .iter()
        .map(|statement| match statement {
            SpiceCoreStatement::ChunkExecutionResult { chunk_id: id, .. } if id == chunk_id => {
                SpiceCoreStatement::ChunkExecutionResult {
                    chunk_id: id.clone(),
                    execution_result: result.clone(),
                }
            }
            other => other.clone(),
        })
        .collect();
    BlockBody::new_for_spice(
        body.chunks().to_vec(),
        body.vrf_value().clone(),
        body.vrf_proof().clone(),
        SpiceCoreStatements::new(statements),
    )
}
