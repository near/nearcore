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

/// A rule that counts the requests reaching it: a rule sees a request after the rules added
/// before it had their say.
fn count_requests(counter: &Arc<Mutex<u64>>) -> Action {
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
    env.node_network(0).add_outgoing_rule(approvals(), count_requests(&passed));

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

    let delayed =
        env.node_network(0).add_outgoing_rule(approvals(), Action::delay(Duration::seconds(1)));
    let passed = Arc::new(Mutex::new(0));
    env.node_network(0).add_outgoing_rule(approvals(), count_requests(&passed));

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
    let delayed =
        env.node_network(0).add_outgoing_rule(approvals(), Action::delay(Duration::seconds(1)));

    env.node_runner(1).run_for_number_of_blocks(10);
    assert!(delayed.matched() > 0, "node 0 sent no approval to delay");
    assert_eq!(*observed.lock(), delayed.matched(), "the observer missed a delayed approval");
}

fn block_broadcasts() -> NetworkRequestsMatcher {
    NetworkRequestsMatcher::new(|request| matches!(request, NetworkRequests::Block { .. }))
}

/// An incoming rule is asked once for each request on its way to its node, from any sender:
/// node 3's rule sees every block the other nodes broadcast, and none node 3 broadcasts.
#[test]
fn test_incoming_rule_sees_each_sender() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let received = Arc::new(Mutex::new(0));
    env.node_network(3).add_incoming_rule(block_broadcasts(), count_requests(&received));
    let sent: Vec<_> = (0..3)
        .map(|index| env.node_network(index).observe_messages(block_broadcasts(), |_| {}))
        .collect();

    env.node_runner(0).run_for_number_of_blocks(10);
    let sent: u64 = sent.iter().map(|rule| rule.matched()).sum();
    assert!(sent > 0, "nodes 0 to 2 broadcast no block");
    assert_eq!(*received.lock(), sent, "node 3's rule missed a broadcast or saw its own");
}

/// Node 3 gets no block broadcast while its incoming rule holds them; the other nodes get
/// every one. Once the rule is lifted, the held blocks go out right away.
#[test]
fn test_hold_incoming_messages() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let held =
        env.node_network(3).add_incoming_rule(block_broadcasts(), Action::hold_until_lifted());
    env.node_runner(0).run_for_number_of_blocks(5);
    assert!(held.matched() > 0, "no block broadcast reached node 3's rule");

    held.lift();
    env.node_runner(0).run_for_number_of_blocks(3);
    assert_eq!(env.node(3).head().height, env.node(0).head().height);
}

/// A rule between two nodes: node 3 gets no block node 0 broadcasts, and every block nodes 1
/// and 2 broadcast. The other nodes get node 0's blocks as usual.
#[test]
fn test_rule_between_nodes() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();
    let to = env.node_datas[3].account_id.clone();

    let dropped = env.node_network(0).add_rule_to(&to, block_broadcasts(), Action::Drop);
    let received = Arc::new(Mutex::new(0));
    env.node_network(3).add_incoming_rule(block_broadcasts(), count_requests(&received));
    let sent: Vec<_> = (0..3)
        .map(|index| env.node_network(index).observe_messages(block_broadcasts(), |_| {}))
        .collect();

    env.node_runner(1).run_for_number_of_blocks(10);
    assert!(sent[0].matched() > 0, "node 0 broadcast no block");
    assert_eq!(dropped.matched(), sent[0].matched(), "a block of node 0 reached node 3");
    let sent_by_others = sent[1].matched() + sent[2].matched();
    assert_eq!(*received.lock(), sent_by_others, "a block of node 1 or 2 missed node 3");
}
