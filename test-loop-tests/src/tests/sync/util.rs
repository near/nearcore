use crate::setup::peer_manager_actor::HandlerResult;
use crate::setup::state::{NodeExecutionData, SharedState};
use crate::utils::node::TestLoopNode;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::CanSendAsync;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_client::QueryError;
use near_network::client::{BlockHeadersRequest, BlockHeadersResponse};
use near_network::types::{NetworkRequests, NetworkResponses};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_primitives::types::AccountId;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

/// Epoch sync horizon used across sync tests. Explicitly configured on each
/// test's syncing node to avoid depending on the production default.
pub const TEST_EPOCH_SYNC_HORIZON: u64 = 2;

/// Height past the epoch sync horizon for far-horizon tests.
/// Nodes at genesis need to be this far behind to trigger far-horizon sync.
pub fn far_horizon_height(epoch_length: u64) -> u64 {
    (TEST_EPOCH_SYNC_HORIZON + 3) * epoch_length
}

/// Set up sync status tracking for a node. Returns the history vector.
///
/// Installs an `every_event_callback` on the test loop that records each
/// distinct sync status transition. Consecutive identical statuses are
/// deduplicated.
///
/// Note: Overwrites any previously installed `every_event_callback`.
pub fn track_sync_status(
    test_loop: &mut TestLoopV2,
    node_datas: &[NodeExecutionData],
    node_idx: usize,
) -> Rc<RefCell<Vec<String>>> {
    let history: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let history_clone = history.clone();
    let node_handle = node_datas[node_idx].client_sender.actor_handle();
    test_loop.set_every_event_callback(move |test_loop_data| {
        let client = &test_loop_data.get(&node_handle).client;
        let status = client.sync_handler.sync_status.as_variant_name();
        let mut h = history_clone.borrow_mut();
        if h.last().map(|s| s.as_str()) != Some(status) {
            h.push(status.to_string());
        }
    });
    history
}

/// Verify that a synced node's account balances match the source validators.
///
/// For each account, queries the balance on a source node (any node except
/// the synced one) and compares with the synced node's view.
///
/// The synced node must track all shards (`TrackedShardsConfig::AllShards`).
/// If any account query returns `UnavailableShard` on the synced node, the
/// function panics — callers must ensure all shards are tracked.
///
/// TODO(sync-v2): support querying accounts from the correct node based on shard
/// tracking, so we don't need AllShards on the synced node.
pub fn verify_balances_on_synced_node(
    test_loop_data: &TestLoopData,
    node_datas: &[NodeExecutionData],
    synced_node_idx: usize,
    accounts: &[AccountId],
) {
    let synced_node =
        TestLoopNode { data: test_loop_data, node_data: &node_datas[synced_node_idx] };
    let source_nodes: Vec<_> = node_datas
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != synced_node_idx)
        .map(|(_, nd)| nd)
        .collect();

    for account in accounts {
        // Get reference balance from any source node that tracks the shard.
        let reference_balance = source_nodes
            .iter()
            .find_map(|nd| {
                let node = TestLoopNode { data: test_loop_data, node_data: nd };
                match node.view_account_query(account) {
                    Ok(view) => Some(view.amount),
                    Err(QueryError::UnavailableShard { .. }) => None,
                    Err(err) => panic!("unexpected query error for {account}: {err:?}"),
                }
            })
            .unwrap_or_else(|| panic!("no source node tracks shard for {account}"));

        let synced_view = synced_node
            .view_account_query(account)
            .unwrap_or_else(|err| panic!("query failed for {account} on synced node: {err:?}"));
        assert_eq!(
            synced_view.amount, reference_balance,
            "balance mismatch for {account} on synced node"
        );
    }
    tracing::debug!(total = accounts.len(), "balance verification complete");
}

/// Expected V2 far-horizon sync status sequence.
const FAR_HORIZON_SYNC_SEQUENCE: &[&str] =
    &["AwaitingPeers", "NoSync", "EpochSync", "HeaderSync", "StateSync", "BlockSync", "NoSync"];

/// Expected V2 near-horizon sync status sequence.
const NEAR_HORIZON_SYNC_SEQUENCE: &[&str] = &["AwaitingPeers", "NoSync", "BlockSync", "NoSync"];

/// Assert that the sync status history matches the expected V2 far-horizon sequence.
pub fn assert_far_horizon_sync_sequence(history: &[String]) {
    let expected: Vec<String> =
        FAR_HORIZON_SYNC_SEQUENCE.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(history, expected.as_slice(), "unexpected sync status history");
}

/// Assert that the sync status history matches the expected V2 near-horizon sequence.
pub fn assert_near_horizon_sync_sequence(history: &[String]) {
    let expected: Vec<String> =
        NEAR_HORIZON_SYNC_SEQUENCE.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(history, expected.as_slice(), "unexpected sync status history");
}

/// Run until two nodes have equal head heights (30s timeout).
pub fn run_until_synced(
    test_loop: &mut TestLoopV2,
    node_datas: &[NodeExecutionData],
    syncing_node_idx: usize,
    source_node_idx: usize,
) {
    let syncing_handle = node_datas[syncing_node_idx].client_sender.actor_handle();
    let source_handle = node_datas[source_node_idx].client_sender.actor_handle();
    test_loop.run_until(
        |data| {
            let syncing_h = data.get(&syncing_handle).client.chain.head().unwrap().height;
            let source_h = data.get(&source_handle).client.chain.head().unwrap().height;
            syncing_h == source_h
        },
        Duration::seconds(30),
    );
}

/// Restrict a node to only communicate with a single source peer.
///
/// Blocks bidirectional traffic between `new_node_idx` and every node
/// except `source_node_idx`. Calls `allow_all_requests()` first to clear
/// any previous restrictions.
pub fn restrict_to_single_peer(
    shared_state: &SharedState,
    node_datas: &[NodeExecutionData],
    new_node_idx: usize,
    source_node_idx: usize,
) {
    let new_peer = node_datas[new_node_idx].peer_id.clone();
    shared_state.network_shared_state.allow_all_requests();
    for (i, data) in node_datas.iter().enumerate() {
        if i != new_node_idx && i != source_node_idx {
            shared_state
                .network_shared_state
                .disallow_requests(data.peer_id.clone(), new_peer.clone());
            shared_state
                .network_shared_state
                .disallow_requests(new_peer.clone(), data.peer_id.clone());
        }
    }
}

/// Throttle header sync on a node by limiting the number of headers returned
/// per request.
///
/// Registers an override handler on the node's peer manager that intercepts
/// outgoing `BlockHeadersRequest` messages, routes them to the peer's
/// ViewClient (same as the default handler), but truncates the response to
/// `max_headers` before sending it back.
///
/// This is needed because `MAX_BLOCK_HEADERS = 512` far exceeds typical test
/// chain lengths (~27 headers), causing header sync to complete in a single
/// event. With truncation, header sync takes multiple events, creating
/// observable intermediate states for `run_until` conditions.
pub fn throttle_header_sync(
    test_loop: &mut TestLoopV2,
    shared_state: &SharedState,
    node_data: &NodeExecutionData,
    max_headers: usize,
) {
    let network_shared_state = shared_state.network_shared_state.clone();
    let account_id = node_data.account_id.clone();
    let future_spawner: Arc<dyn FutureSpawner> =
        Arc::new(test_loop.future_spawner(&node_data.identifier));

    node_data.register_override_handler(
        &mut test_loop.data,
        Box::new(move |request| match request {
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                let my_peer_id = network_shared_state.account_to_peer_id(&account_id);
                let responder = network_shared_state
                    .senders_for_peer(&peer_id, &my_peer_id)
                    .client_sender
                    .clone();
                let future = network_shared_state
                    .senders_for_peer(&my_peer_id, &peer_id)
                    .view_client_sender
                    .send_async(BlockHeadersRequest(hashes));
                future_spawner.spawn("throttled header response", async move {
                    let response = future.await.unwrap().unwrap();
                    let truncated = response.into_iter().take(max_headers).collect();
                    let future =
                        responder.send_async(BlockHeadersResponse(truncated, peer_id).span_wrap());
                    drop(future);
                });
                HandlerResult::Handled(NetworkResponses::NoResponse)
            }
            other => HandlerResult::Unhandled(other),
        }),
    );
}
