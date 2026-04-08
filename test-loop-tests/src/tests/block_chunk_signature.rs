use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_crypto::Signature;
use near_network::types::PeerMessage;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::block_body::BlockBody;
use near_primitives::sharding::ShardChunkHeader;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const TIMEOUT_SECONDS: i64 = 5;

#[test]
fn block_chunk_signature_rejection() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(2, 0).skip_warmup().build();

    let mutated_blocks = Arc::new(AtomicUsize::new(0));

    {
        env.shared_state.network_shared_state.register_message_filter(move |_from, _to, msg| {
            if let PeerMessage::Block(block) = msg {
                let mut block_clone = (**block).clone();
                if let Some(chunk) = first_chunk_mut(&mut block_clone) {
                    if zero_chunk_signature(chunk) {
                        let previous = mutated_blocks.fetch_add(1, Ordering::SeqCst);
                        assert!(
                            previous < 2,
                            "expected at most two mutated blocks before a ban kicks in"
                        );
                        return Some(PeerMessage::Block(Arc::new(block_clone)));
                    }
                }
            }
            Some(msg.clone())
        });
    }

    env.test_loop.run_for(Duration::seconds(TIMEOUT_SECONDS));

    let height0 = env.node(0).head().height;
    let height1 = env.node(1).head().height;

    assert!(height0 <= 3, "expected node0 to stall after signature tampering");
    assert!(height1 <= 3, "expected node1 to stall after signature tampering");
}

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

fn zero_chunk_signature(chunk: &mut ShardChunkHeader) -> bool {
    match chunk {
        ShardChunkHeader::V1(header) => replace_with_zero(&mut header.signature),
        ShardChunkHeader::V2(header) => replace_with_zero(&mut header.signature),
        ShardChunkHeader::V3(header) => replace_with_zero(&mut header.signature),
    }
}

fn replace_with_zero(signature: &mut Signature) -> bool {
    let zero_signature = Signature::empty(signature.key_type());
    if *signature == zero_signature {
        false
    } else {
        *signature = zero_signature;
        true
    }
}
