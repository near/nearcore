use crate::setup::state::{NodeExecutionData, SharedState};
use crate::utils::node::TestLoopNode;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_client::QueryError;
use near_primitives::types::AccountId;
use std::cell::RefCell;
use std::rc::Rc;

/// Epoch sync horizon used across sync tests. Explicitly configured on each
/// test's syncing node to avoid depending on the production default.
pub const TEST_EPOCH_SYNC_HORIZON: u64 = 2;

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
    tracing::info!(total = accounts.len(), "balance verification complete");
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
