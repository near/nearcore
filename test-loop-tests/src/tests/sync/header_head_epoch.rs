//! The client reports the epoch of its latest block header to the network, which uses it to
//! bound the epochs of the snapshot host infos it keeps.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::node::TestLoopNode;
use near_async::time::Duration;
use near_network::types::{NetworkRequests, SnapshotHostEvent};
use near_o11y::testonly::init_test_logger;
use near_primitives::types::EpochHeight;
use std::cell::RefCell;
use std::rc::Rc;

/// Records the epochs a node reports to its network.
fn record_reported_epochs(env: &mut TestLoopEnv, node_idx: usize) -> Rc<RefCell<Vec<EpochHeight>>> {
    let reported = Rc::new(RefCell::new(Vec::new()));
    let observed = reported.clone();
    let peer_manager =
        env.test_loop.data.get_mut(&env.node_datas[node_idx].peer_manager_sender.actor_handle());
    peer_manager.register_observer(Box::new(move |request| {
        if let NetworkRequests::SnapshotHostEvent(SnapshotHostEvent::ChainProgressed {
            epoch_height,
        }) = request
        {
            observed.borrow_mut().push(*epoch_height);
        }
    }));
    reported
}

fn head_epoch_height(node: &TestLoopNode) -> EpochHeight {
    node.client().epoch_manager.get_epoch_info(&node.head().epoch_id).unwrap().epoch_height()
}

// Scenario: a validator runs through several epochs.
//
// Assertions:
//   - It reports each epoch once, in order, from the genesis epoch to its current one.
#[test]
fn test_header_head_epoch_reported_to_network() {
    init_test_logger();

    let epoch_length = 10;
    let mut env =
        TestLoopBuilder::new().validators(4, 0).epoch_length(epoch_length).delay_warmup().build();
    let genesis_epoch = head_epoch_height(&env.node(0));
    // The genesis epoch is reported during warmup, so recording has to start before it.
    let reported = record_reported_epochs(&mut env, 0);
    let mut env = env.warmup();

    env.node_runner(0).run_until_head_height(4 * epoch_length);
    // A report can trail the epoch change by up to one periodic sync check.
    let last_reported = reported.clone();
    env.node_runner(0).run_until(
        |node| last_reported.borrow().last() == Some(&head_epoch_height(node)),
        Duration::seconds(5),
    );
    let expected: Vec<_> = (genesis_epoch..=head_epoch_height(&env.node(0))).collect();
    assert_eq!(*reported.borrow(), expected);
}
