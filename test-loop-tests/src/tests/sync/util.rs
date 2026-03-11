use crate::setup::state::{NodeExecutionData, SharedState};
use near_async::test_loop::TestLoopV2;
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
