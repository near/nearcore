//! Network rules: what a node does with the requests it sends, switched on and off while the
//! test runs. See `src/setup/network_rules.rs`. The examples act on the block approvals one of
//! four equal validators sends: the other three hold enough stake for the chain to go on
//! without them, so the chain is not affected by what happens to them.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::network_rules::Action;
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_o11y::testonly::init_test_logger;
use parking_lot::Mutex;
use std::sync::Arc;

fn is_approval(request: &NetworkRequests) -> bool {
    matches!(request, NetworkRequests::Approval { .. })
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

    let dropped = env.drop_messages_from(0, is_approval);
    let passed = Arc::new(Mutex::new(0));
    env.network_rule(0, is_approval, count_approvals(&passed));

    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(dropped.matched() > 0, "node 0 sent no approval to drop");
    assert_eq!(*passed.lock(), 0, "a dropped approval went on to the next rule");

    dropped.lift();
    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(*passed.lock() > 0, "node 0 sent no approval once the drop was lifted");
}

/// Node 0 sends every approval twice: the rule after it sees both copies.
#[test]
fn test_replace_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let duplicated = env.network_rule(
        0,
        is_approval,
        Action::Replace(Box::new(|request| {
            let NetworkRequests::Approval { approval_message } = &request else {
                unreachable!("the rule matches approvals only");
            };
            let copy = NetworkRequests::Approval { approval_message: approval_message.clone() };
            vec![request, copy]
        })),
    );
    let passed = Arc::new(Mutex::new(0));
    env.network_rule(0, is_approval, count_approvals(&passed));

    env.node_runner(1).run_for_number_of_blocks(5);
    assert!(duplicated.matched() > 0, "node 0 sent no approval to duplicate");
    assert_eq!(*passed.lock(), 2 * duplicated.matched(), "the copies did not go on in pairs");
}

/// Node 0 sends its approvals a second late. A request that goes out later skips the rules on
/// its way, so the rule after the delay never sees one, and the chain goes on regardless.
#[test]
fn test_delay_messages_from_node() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().validators(4, 0).build();

    let delayed = env.network_rule(0, is_approval, Action::Delay(Duration::seconds(1)));
    let passed = Arc::new(Mutex::new(0));
    env.network_rule(0, is_approval, count_approvals(&passed));

    env.node_runner(1).run_for_number_of_blocks(10);
    assert!(delayed.matched() > 0, "node 0 sent no approval to delay");
    assert_eq!(*passed.lock(), 0, "a delayed approval went through the rules again on its way out");
}
