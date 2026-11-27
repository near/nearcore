//! Network rules: what a node does with the requests it sends, switched on and off while the
//! test runs.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::network_rules::{Action, NetworkRequestsMatcher};
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use parking_lot::Mutex;
use std::sync::Arc;

fn approvals() -> NetworkRequestsMatcher {
    NetworkRequestsMatcher::new(|request| matches!(request, NetworkRequests::Approval { .. }))
}

/// A rule that counts the approvals reaching it: a rule sees a request after the rules added
/// before it had their say.
fn count_approvals(counter: &Arc<Mutex<u64>>) -> Action {
    let counter = counter.clone();
    Action::Observe(Box::new(move |_| *counter.lock() += 1))
}

/// Node 0 drops the approvals it sends from now on; the rule counts what it dropped, and once
/// it is lifted the approvals go out again.
#[test]
fn test_drop_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let dropped = env.node_network(0).drop_messages(approvals());
    let passed = Arc::new(Mutex::new(0));
    env.node_network(0).add_rule(approvals(), count_approvals(&passed));

    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(dropped.matched() > 0, "node 0 sent no approval to drop");
    assert_eq!(*passed.lock(), 0, "a dropped approval went on to the next rule");

    dropped.lift();
    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(*passed.lock() > 0, "node 0 sent no approval once the drop was lifted");
}

/// Node 0 sends its approvals a second late. A request that goes out later skips the rules on
/// its way, so the rule after the delay never sees one, and the chain goes on regardless.
#[test]
fn test_delay_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let delayed = env.node_network(0).add_rule(approvals(), Action::delay(Duration::seconds(1)));
    let passed = Arc::new(Mutex::new(0));
    env.node_network(0).add_rule(approvals(), count_approvals(&passed));

    env.node_runner(1).run_for_number_of_blocks(10);
    assert!(delayed.matched() > 0, "node 0 sent no approval to delay");
    assert_eq!(*passed.lock(), 0, "a delayed approval went through the rules again on its way out");
}

/// Node 0 delays its approvals, and an observer added before the delay sees every one of
/// them: rules run in the order they were added, so an observer added first sees everything
/// the node sends.
#[test]
fn test_observe_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let observed = Arc::new(Mutex::new(0));
    let observed_clone = observed.clone();
    env.node_network(0).observe_messages(approvals(), move |_| *observed_clone.lock() += 1);
    let delayed = env.node_network(0).add_rule(approvals(), Action::delay(Duration::seconds(1)));

    env.node_runner(1).run_for_number_of_blocks(10);
    assert!(delayed.matched() > 0, "node 0 sent no approval to delay");
    assert_eq!(*observed.lock(), delayed.matched(), "the observer missed a delayed approval");
}
