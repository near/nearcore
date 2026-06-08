//! Integration smoke test for the V2 partial witness defer/replay path, exercising
//! `PartialWitnessActor`'s `PendingV2WitnessCache` end-to-end across two test-loop nodes: a V2
//! witness arriving before its `prev_block` defers, and the BlockNotification scan resolves and
//! dispatches it once the block lands. The oracle reads the receiver actor's OWN cache via
//! `pending_cache_bucket_count`, not the process-global `PARTIAL_WITNESS_PENDING_CACHE_SIZE` gauge
//! (every node writes the gauge). Nightly-gated: the V2 wire path needs `ProtocolFeature::EarlyKickout`.
//!
//! A prior multi-notification `Requeue` test was dropped (its `cache > 0` oracle was a false
//! positive); pinning it needs actor-local cache-key inspection, deferred to follow-up.

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
/// BlockRequest path and the defer collapses immediately.
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
// Spice distributes witnesses via its own data-distribution path, not the V2
// partial-witness pending cache this test drives, so the defer never fires.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_v2_init_emit_defer_then_replay() {
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
    // Receiver actor's OWN cache, not the process-global gauge (every node writes
    // it, zeroed each block). `pending_cache_bucket_count` = distinct
    // `prev_block_hash` buckets; fine for `> 0` / `== 0`.
    let receiver_pw_handle = env
        .node_datas
        .iter()
        .find(|n| n.account_id.as_str() == RECEIVER)
        .expect("receiver node datas")
        .partial_witness_sender
        .actor_handle();
    let holder = BlockHolder::new(1);
    register_block_holder(&mut env, &holder);

    // Run until receiver buffers >= 1 V2 witness.
    env.test_loop.run_until(
        |data| data.get(&receiver_pw_handle).pending_cache_bucket_count() > 0,
        Duration::seconds(5),
    );
    assert!(
        env.test_loop.data.get(&receiver_pw_handle).pending_cache_bucket_count() > 0,
        "receiver should have deferred at least one V2 witness",
    );
    let held_block = {
        let mut h = holder.lock();
        assert_eq!(h.held.len(), 1, "exactly one block held");
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

    // Wait for receiver cache to drain. Proves BlockNotification scan ran +
    // resolved (prev now = released block → producer-DB lookup → Ready → dispatch).
    env.test_loop.run_until(
        |data| data.get(&receiver_pw_handle).pending_cache_bucket_count() == 0,
        Duration::seconds(5),
    );
    assert_eq!(
        env.test_loop.data.get(&receiver_pw_handle).pending_cache_bucket_count(),
        0,
        "pending cache must drain after block notification",
    );

    drop(env);
}
