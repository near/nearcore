use crate::setup::builder::TestLoopBuilder;
use crate::setup::network_rules::Action;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{
    PartialEncodedChunkWithArcReceipts, ShardChunkHeader, ShardChunkHeaderV3,
};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{Balance, BlockHeight, Gas, ShardId};

/// Partial chunk for `(height, shard_id)` with an *unknown* `prev_block_hash`,
/// zero parts, and a throwaway signer whose producer signature does not verify.
fn make_unknown_prev_chunk(
    height: BlockHeight,
    shard_id: ShardId,
) -> PartialEncodedChunkWithArcReceipts {
    let unknown_prev =
        CryptoHash::hash_bytes(format!("unknown-prev-{height}-{shard_id}").as_bytes());
    let signer = create_test_signer("unknown_prev_chunk_producer");
    let header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
        unknown_prev,
        Default::default(),
        Default::default(),
        Default::default(),
        0,
        height,
        shard_id,
        Gas::ZERO,
        Gas::from_gas(1000),
        Balance::ZERO,
        CryptoHash::default(),
        CryptoHash::default(),
        vec![],
        Default::default(),
        BandwidthRequests::empty(),
        None,
        &signer,
    ));
    PartialEncodedChunkWithArcReceipts {
        header,
        parts: Vec::new(),
        prev_outgoing_receipts: Vec::new(),
        prev_prev_block_hash: None,
        epoch_id: None,
    }
}

/// End-to-end: for the target shard, an unknown-prev chunk is injected into each
/// recipient just before the genuine `PartialEncodedChunkMessage`. Its producer
/// signature fails at arrival, so it is dropped instead of preempting the genuine
/// chunk; the target shard is present at every height, like the non-targeted one.
#[test]
fn test_unknown_prev_chunk_does_not_skip_chunk() {
    init_test_logger();
    let target_shard = ShardId::new(0);
    let mut env = TestLoopBuilder::new().num_shards(2).chunk_producer_per_shard().build();

    // Every chunk message for the target shard goes out behind an injected copy for the same
    // recipient, carrying the unknown-prev chunk: both are routed the same way, so the
    // injected one arrives first.
    let mut injections = Vec::new();
    for index in 0..env.node_datas.len() {
        let injection = env.network_rule(
            index,
            move |request| {
                matches!(request, NetworkRequests::PartialEncodedChunkMessage { partial_encoded_chunk, .. }
                    if partial_encoded_chunk.header.shard_id() == target_shard)
            },
            Action::Replace(Box::new(|genuine| {
                let NetworkRequests::PartialEncodedChunkMessage {
                    account_id,
                    partial_encoded_chunk,
                } = &genuine
                else {
                    unreachable!("the rule matches partial chunk messages only");
                };
                let header = &partial_encoded_chunk.header;
                let preempting = NetworkRequests::PartialEncodedChunkMessage {
                    account_id: account_id.clone(),
                    partial_encoded_chunk: make_unknown_prev_chunk(
                        header.height_created(),
                        header.shard_id(),
                    ),
                };
                vec![preempting, genuine]
            })),
        );
        injections.push(injection);
    }

    let start_height = env.node(0).head().height;
    let window = 12;
    let end_height = start_height + window;
    env.node_runner(0).run_until_head_height(end_height);
    assert!(
        injections.iter().any(|injection| injection.matched() > 0),
        "no chunk message for the target shard was sent"
    );

    // chunk_mask is indexed by shard index, not shard id.
    let node = env.node(0);
    let head = node.head();
    let shard_layout = node.client().epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
    let target_index = shard_layout.get_shard_index(target_shard).unwrap();

    let mut target_skipped = 0;
    for height in (start_height + 1)..=end_height {
        let mask =
            node.client().chain.get_block_by_height(height).unwrap().header().chunk_mask().to_vec();
        for (index, present) in mask.iter().enumerate() {
            if index != target_index {
                // A non-target shard going missing would be unrelated breakage.
                assert!(present, "non-targeted shard {index} skipped at height {height}: {mask:?}");
            }
        }
        if !mask[target_index] {
            target_skipped += 1;
        }
    }
    // The injected chunk is unvalidated, so it never preempts the genuine one.
    assert_eq!(
        target_skipped, 0,
        "the targeted shard's chunk must not be skipped once unvalidated chunks cannot claim the slot"
    );
}
