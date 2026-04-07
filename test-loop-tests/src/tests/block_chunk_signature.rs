use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_crypto::Signature;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::block_body::BlockBody;
use near_primitives::sharding::ShardChunkHeader;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

const TIMEOUT_SECONDS: i64 = 5;

#[test]
#[ignore] // TODO: convert override handler to transport filter (iteration 24-26)
fn block_chunk_signature_rejection() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).skip_warmup().build();

    let _mutated_blocks = Arc::new(AtomicUsize::new(0));

    // TODO(iteration 24-26): convert to transport message filter.
    // Override handlers are not supported with the real PeerManagerActor.
    // for node_data in &env.node_datas {
    //     let peer_actor = env.test_loop.data.get_mut(&node_data.peer_manager_sender.actor_handle());
    //     peer_actor.register_override_handler(...);
    // }

    env.test_loop.run_for(Duration::seconds(TIMEOUT_SECONDS));

    let height0 = env.node(0).head().height;
    let height1 = env.node(1).head().height;

    assert!(height0 <= 3, "expected node0 to stall after signature tampering");
    assert!(height1 <= 3, "expected node1 to stall after signature tampering");
}

#[allow(dead_code)] // TODO(iteration 24-26): will be used after transport filter conversion
fn first_chunk_mut(block: &mut Block) -> Option<&mut ShardChunkHeader> {
    match block {
        Block::BlockV4(block) => match &mut block.body {
            BlockBody::V1(body) => body.chunks.get_mut(0),
            BlockBody::V2(body) => body.chunks.get_mut(0),
            BlockBody::V3(body) => body.chunks.get_mut(0),
        },
        _ => None,
    }
}

#[allow(dead_code)] // TODO(iteration 24-26): will be used after transport filter conversion
fn zero_chunk_signature(chunk: &mut ShardChunkHeader) -> bool {
    match chunk {
        ShardChunkHeader::V1(header) => replace_with_zero(&mut header.signature),
        ShardChunkHeader::V2(header) => replace_with_zero(&mut header.signature),
        ShardChunkHeader::V3(header) => replace_with_zero(&mut header.signature),
    }
}

#[allow(dead_code)] // TODO(iteration 24-26): will be used after transport filter conversion
fn replace_with_zero(signature: &mut Signature) -> bool {
    let zero_signature = Signature::empty(signature.key_type());
    if *signature == zero_signature {
        false
    } else {
        *signature = zero_signature;
        true
    }
}
