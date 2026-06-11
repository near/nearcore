//! Integration smoke test for prev_prev-anchored V2 partial witness validation
//! across two test-loop nodes. A V2 witness part arriving BEFORE its prev block
//! is signature-verified against the anchor (two blocks back) and assembled
//! immediately — no deferral, no block-notification replay (the
//! `PendingV2WitnessCache` machinery is gone). The decoded witness waits in the
//! orphan-witness pool and is validated when the held block is released, so the
//! chain keeps including chunks afterwards. Nightly-gated: the V2 wire path
//! needs `ProtocolFeature::EarlyKickout`.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::peer_manager_actor::HandlerResult;
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_chain::Block;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_network::client::BlockResponse;
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance};
use parking_lot::Mutex;
use std::sync::Arc;

/// Two clients: account0 produces blocks/chunks, account1 is the chunk
/// validator that receives V2 witnesses.
const PRODUCER: &str = "account0";
const RECEIVER: &str = "account1";

fn make_env() -> TestLoopEnv {
    let accounts = [PRODUCER, RECEIVER];
    let clients = accounts.iter().map(|s| s.parse::<AccountId>().unwrap()).collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(&[PRODUCER], &[RECEIVER]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(50)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&clients, Balance::from_near(1_000_000))
        .genesis_height(10_000)
        .build();
    let epoch_config_store =
        TestEpochConfigBuilder::from_genesis(&genesis).build_store_for_genesis_protocol_version();
    TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
}

/// Capture-and-release for `NetworkRequests::Block`. `target_holds` blocks
/// are held back from receivers and stashed in `held` for the test to
/// replay later. `captured` counts how many were intercepted total so the
/// override handler stops trapping once the budget is met (preventing
/// re-capture when the test pops `held` to release).
struct BlockHolder {
    held: Vec<Block>,
    captured: usize,
    target_holds: usize,
}

impl BlockHolder {
    fn new(target_holds: usize) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self { held: Vec::new(), captured: 0, target_holds }))
    }
}

/// Register the same hold/intercept on every node's `PeerManager`. The
/// producer's PM is what fires `NetworkRequests::Block` (broadcast); the
/// receiver's PM is what fires `NetworkRequests::BlockRequest` (catch-up
/// sync). Without dropping both, receiver back-fills the held block via the
/// BlockRequest path and the witness-before-block window collapses.
fn register_block_holder(env: &mut TestLoopEnv, holder: &Arc<Mutex<BlockHolder>>) {
    let node_handles: Vec<_> =
        env.node_datas.iter().map(|n| n.peer_manager_sender.actor_handle()).collect();
    for handle in node_handles {
        let holder = holder.clone();
        let peer_actor = env.test_loop.data.get_mut(&handle);
        peer_actor.register_override_handler(Box::new(move |request| match &request {
            NetworkRequests::Block { block } => {
                let mut h = holder.lock();
                if h.captured < h.target_holds {
                    h.captured += 1;
                    h.held.push(block.as_ref().clone());
                    HandlerResult::Handled(NetworkResponses::NoResponse)
                } else {
                    HandlerResult::Unhandled(request)
                }
            }
            NetworkRequests::BlockRequest { .. } => {
                let h = holder.lock();
                if !h.held.is_empty() {
                    HandlerResult::Handled(NetworkResponses::NoResponse)
                } else {
                    HandlerResult::Unhandled(request)
                }
            }
            _ => HandlerResult::Unhandled(request),
        }));
    }
}

#[test]
// Spice distributes witnesses via its own data-distribution path, not the
// anchored V2 partial-witness path this test drives.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_v2_witness_verifies_without_prev_block() {
    init_test_logger();
    let mut env = make_env();

    let producer_peer_id = env
        .node_datas
        .iter()
        .find(|n| n.account_id.as_str() == PRODUCER)
        .expect("producer node datas")
        .peer_id
        .clone();
    let receiver_client_sender = env
        .node_datas
        .iter()
        .find(|n| n.account_id.as_str() == RECEIVER)
        .expect("receiver node datas")
        .client_sender
        .clone();

    let holder = BlockHolder::new(1);
    register_block_holder(&mut env, &holder);

    // Run until a block is held back from the receiver. While the block is
    // held, the producer keeps distributing V2 witness parts for the next
    // height; the receiver verifies them against the prev_prev anchor (which
    // it has) even though the prev block is missing.
    env.test_loop.run_until(|_| holder.lock().held.len() == 1, Duration::seconds(5));
    let held_height = holder.lock().held[0].header().height();

    // Give the receiver time to process witness parts for held_height + 1
    // while the prev block is still missing. The assembled witness lands in
    // the orphan pool; nothing is deferred at the partial-witness layer.
    env.test_loop.run_for(Duration::seconds(2));

    // Release the held block directly to the receiver client.
    let held_block = {
        let mut h = holder.lock();
        h.held.remove(0)
    };
    receiver_client_sender.send(
        BlockResponse {
            block: Arc::new(held_block),
            peer_id: producer_peer_id,
            was_requested: false,
        }
        .span_wrap(),
    );

    // The chain must keep including chunks past the held height: that requires
    // the receiver (the only chunk validator) to have endorsed chunks whose
    // witness parts arrived before their prev block.
    let client_handle = env
        .node_datas
        .iter()
        .find(|n| n.account_id.as_str() == RECEIVER)
        .expect("receiver node datas")
        .client_sender
        .actor_handle();
    env.test_loop.run_until(
        |data| {
            let client = &data.get(&client_handle).client;
            let Ok(head) = client.chain.head() else {
                return false;
            };
            if head.height <= held_height + 2 {
                return false;
            }
            let head_block = client.chain.get_block(&head.last_block_hash).unwrap();
            head_block.header().chunk_mask().iter().all(|included| *included)
        },
        Duration::seconds(20),
    );

    drop(env);
}
