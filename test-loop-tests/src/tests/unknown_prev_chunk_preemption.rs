use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::HandlerResult;
use near_async::messaging::{IntoSender, Sender};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{
    PartialEncodedChunk, PartialEncodedChunkV2, ShardChunkHeader, ShardChunkHeaderV3,
};
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, Balance, BlockHeight, Gas, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::HashMap;

/// Partial chunk for `(height, shard_id)` with an *unknown* `prev_block_hash`,
/// zero parts, and a throwaway signer whose producer signature does not verify.
fn make_unknown_prev_chunk(height: BlockHeight, shard_id: ShardId) -> PartialEncodedChunk {
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
        PROTOCOL_VERSION,
    ));
    PartialEncodedChunk::V2(PartialEncodedChunkV2 {
        header,
        parts: Vec::new(),
        prev_outgoing_receipts: Vec::new(),
    })
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

    // No-delay senders so an injected chunk is processed before the genuine one,
    // which the default handler delivers with NETWORK_DELAY.
    let shards_manager_senders: HashMap<AccountId, Sender<ShardsManagerRequestFromNetwork>> = env
        .node_datas
        .iter()
        .map(|data| (data.account_id.clone(), data.shards_manager_sender.clone().into_sender()))
        .collect();

    for node_data in &env.node_datas {
        let peer_actor_handle = node_data.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        let shards_manager_senders = shards_manager_senders.clone();
        peer_actor.register_override_handler(Box::new(move |request| -> HandlerResult {
            if let NetworkRequests::PartialEncodedChunkMessage {
                account_id: ref to,
                partial_encoded_chunk: ref genuine,
            } = request
            {
                let header = &genuine.header;
                if header.shard_id() == target_shard {
                    let preempting =
                        make_unknown_prev_chunk(header.height_created(), header.shard_id());
                    if let Some(sender) = shards_manager_senders.get(to) {
                        sender.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                            preempting,
                        ));
                    }
                }
            }
            // Pass the genuine chunk through unchanged.
            HandlerResult::Unhandled(request)
        }));
    }

    let start_height = env.node(0).client().chain.head().unwrap().height;
    let window = 12;
    let end_height = start_height + window;
    env.node_runner(0).run_until_head_height(end_height);

    // chunk_mask is indexed by shard index, not shard id.
    let node = env.node(0);
    let head = node.client().chain.head().unwrap();
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
