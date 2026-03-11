use crate::setup::state::{NodeExecutionData, SharedState};
use crate::utils::node::TestLoopNode;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_client::QueryError;
use near_primitives::types::AccountId;
use std::cell::RefCell;
use std::rc::Rc;

/// Set up sync status tracking for a node. Returns the history vector.
///
/// Installs an `every_event_callback` on the test loop that records each
/// distinct sync status transition. Consecutive identical statuses are
/// deduplicated.
///
/// **NOTE**: Overwrites any previously installed `every_event_callback`.
/// If tracking multiple nodes sequentially, call this again after the
/// first node finishes syncing.
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
        let header_head_height = client.chain.header_head().unwrap().height;
        let head_height = client.chain.head().unwrap().height;
        tracing::info!(
            ?client.sync_handler.sync_status,
            ?header_head_height,
            ?head_height,
            "new node sync status"
        );
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
/// the synced one) and compares with the synced node's view. Accounts on
/// shards not tracked by the synced node are skipped.
///
/// Migrated from state_sync.py balance consistency assertions.
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
            .expect(&format!("no source node tracks shard for {account}"));

        match synced_node.view_account_query(account) {
            Ok(view) => assert_eq!(
                view.amount, reference_balance,
                "balance mismatch for {account} on synced node"
            ),
            Err(QueryError::UnavailableShard { .. }) => continue,
            Err(err) => panic!("unexpected query error for {account} on synced node: {err:?}"),
        }
    }
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
