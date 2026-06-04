//! Integration smoke test for the V2 partial witness defer/replay path. It
//! exercises `PartialWitnessActor`'s `PendingV2WitnessCache` end-to-end across
//! two test-loop nodes: a V2 witness arriving at a receiver that is missing its
//! `prev_block` defers (pending-cache gauge rises), and after the block lands
//! the BlockNotification scan resolves and dispatches it (gauge drains to 0).
//! Nightly-gated because the V2 wire path only activates once
//! `ProtocolFeature::EarlyKickout` is reachable (`NIGHTLY_PROTOCOL_VERSION >= 152`).
//!
//! Scope: this proves the actor wiring (BlockResponse → BlockNotification → cache
//! scan → dispatch) runs, which no unit test reaches. It does NOT pin exactly-once
//! dispatch or the multi-notification `Requeue` liveness story — those live in
//! `pre_check_replay` unit tests (`partial_witness_actor_tests.rs`). A prior
//! two-block "survives two notifications" test was dropped: its `cache > 0` oracle
//! was a false positive (orphan-block witnesses repopulate the cache, so it passed
//! even with the requeue path broken). Pinning it correctly needs actor-local
//! cache-key inspection, deferred to follow-up.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::peer_manager_actor::HandlerResult;
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_chain::Block;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::metrics::PARTIAL_WITNESS_PENDING_CACHE_SIZE;
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

fn make_env() -> crate::setup::env::TestLoopEnv {
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

/// Read `near_partial_witness_pending_cache_size` (no labels).
fn pending_cache_size() -> i64 {
    PARTIAL_WITNESS_PENDING_CACHE_SIZE.get()
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
fn register_block_holder(
    env: &mut crate::setup::env::TestLoopEnv,
    holder: &Arc<Mutex<BlockHolder>>,
) {
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
    let holder = BlockHolder::new(1);
    register_block_holder(&mut env, &holder);

    // Run the loop until the receiver has buffered at least one V2 witness in
    // the pending cache. The gauge is a process-global IntGauge; in a 2-node
    // single-process test-loop only the receiver's actor writes it.
    env.test_loop.run_until(|_data| pending_cache_size() > 0, Duration::seconds(5));
    assert!(pending_cache_size() > 0, "receiver should have deferred at least one V2 witness");
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

    // Wait for the pending cache to drain back to zero. Drain proves the
    // BlockNotification scan ran and resolved this witness (its prev is now the
    // just-released block, so the producer-DB lookup succeeds → Ready → dispatch).
    env.test_loop.run_until(|_data| pending_cache_size() == 0, Duration::seconds(5));
    assert_eq!(pending_cache_size(), 0, "pending cache must drain after block notification");

    drop(env);
}
