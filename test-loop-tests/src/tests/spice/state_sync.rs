use crate::setup::builder::TestLoopBuilder;
use crate::utils::node::TestLoopNode;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::verify_path;
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::types::{EpochId, SpiceChunkId};
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
/// and proves its root against the `chunk_execution_root` of the block that certified that
/// chunk - a block header the syncing node already holds from header sync.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_header_proves_the_post_state_root() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).epoch_length(10).build();

    let genesis_height = env.node(0).client().chain.genesis().height();
    env.node_runner(0).run_until_certified(genesis_height + 25);

    let node = env.node(0);
    let chain = &node.client().chain;
    let head = chain.head().unwrap();
    let sync_hash = chain
        .get_sync_hash(&head.last_block_hash)
        .unwrap()
        .expect("the head epoch must have a sync hash by now");
    let sync_header = chain.get_block_header(&sync_hash).unwrap();
    let shard_ids = node.client().epoch_manager.shard_ids(sync_header.epoch_id()).unwrap();
    assert!(!shard_ids.is_empty());

    for shard_id in shard_ids {
        let response = chain.state_sync_adapter.get_state_response_header(shard_id, sync_hash);
        let header = match response {
            Ok(ShardStateSyncResponseHeader::V3(header)) => header,
            other => panic!("spice must serve a V3 state sync header, got {other:?}"),
        };
        let proof = &header.state_root_proof;

        // The leaf names the chunk that was asked for.
        let chunk_id = SpiceChunkId { block_hash: sync_hash, shard_id };
        assert_eq!(proof.roots.chunk_id(), &chunk_id);

        // The committing block sits above the sync block, and its header commits the leaf.
        let committing = chain.get_block_header(&proof.committing_block_hash).unwrap();
        assert!(committing.height() > sync_header.height());
        let chunk_execution_root =
            committing.chunk_execution_root().expect("a spice header commits execution roots");
        assert!(verify_path(chunk_execution_root, &proof.proof, &proof.roots));

        // It is the block the chain recorded as certifying the chunk, and the execution result
        // the header carries is the one behind the proven leaf.
        assert_eq!(
            node.store().chain_store().get_chunk_certifying_block(&chunk_id),
            Some(proof.committing_block_hash),
        );
        assert_eq!(header.execution_result.chunk_extra.state_root(), proof.roots.state_root());
        assert_eq!(
            &header.execution_result.outgoing_receipts_root,
            proof.roots.outgoing_receipts_root(),
        );
    }
}
