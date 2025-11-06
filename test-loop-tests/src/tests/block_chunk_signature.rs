use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use near_async::time::Duration;
use near_crypto::Signature;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::block_body::BlockBody;
use near_primitives::sharding::ShardChunkHeader;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

const TIMEOUT_SECONDS: i64 = 5;

#[test]
fn block_chunk_signature_rejection() {
    init_test_logger();

    let validators_spec = create_validators_spec(2, 0);
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder().validators_spec(validators_spec).build();
    let mut builder = TestLoopBuilder::new().skip_warmup();
    builder = builder.genesis(genesis).epoch_config_store_from_genesis().clients(clients);
    let mut env = builder.build();

    let mutated_blocks = Arc::new(AtomicUsize::new(0));

    for node_data in &env.node_datas {
        let mutated_blocks = mutated_blocks.clone();
        let peer_actor_handle = node_data.peer_manager_sender.actor_handle();
        let peer_actor = env.test_loop.data.get_mut(&peer_actor_handle);
        peer_actor.register_override_handler(Box::new(move |request| match request {
            NetworkRequests::Block { block } => {
                let mut block_clone = (*block).clone();
                if let Some(chunk) = first_chunk_mut(&mut block_clone) {
                    if zero_chunk_signature(chunk) {
                        let previous = mutated_blocks.fetch_add(1, Ordering::SeqCst);
                        assert!(
                            previous < 2,
                            "Expected at most two mutated blocks before a ban kicks in"
                        );
                        return Some(NetworkRequests::Block { block: Arc::new(block_clone) });
                    }
                }
                Some(NetworkRequests::Block { block })
            }
            other => Some(other),
        }));
    }

    env.test_loop.run_for(Duration::seconds(TIMEOUT_SECONDS));

    let node0 = TestLoopNode::from(&env.node_datas[0]);
    let node1 = TestLoopNode::from(&env.node_datas[1]);
    let test_loop_data = env.test_loop_data();
    let height0 = node0.head(test_loop_data).height;
    let height1 = node1.head(test_loop_data).height;

    assert!(height0 <= 3, "expected node0 to stall after signature tampering");
    assert!(height1 <= 3, "expected node1 to stall after signature tampering");

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
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
