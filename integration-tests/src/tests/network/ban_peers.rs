pub use crate::tests::network::runner::*;
use std::time::Duration;

/// Check we don't try to connect to a banned peer and we don't accept
/// incoming connection from it.
#[test]
fn dont_connect_to_banned_peer() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2)
        .enable_outbound()
        .use_boot_nodes(vec![0, 1])
        .ban_window(Duration::from_secs(60));

    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    runner.push_action(ban_peer(0, 1));
    // It needs to wait a large timeout so we are sure both peer don't establish a connection.
    runner.push(Action::Wait(Duration::from_millis(1000)));

    runner.push(Action::CheckRoutingTable(0, vec![]));
    runner.push(Action::CheckRoutingTable(1, vec![]));

    start_test(runner)
}

/// Check two peers are able to connect again after one peers is banned and unbanned.
#[test]
fn connect_to_unbanned_peer() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2)
        .enable_outbound()
        .use_boot_nodes(vec![0, 1])
        .ban_window(Duration::from_secs(2));

    // Check both peers are connected
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    // Ban peer 1
    runner.push_action(ban_peer(0, 1));

    runner.push(Action::Wait(Duration::from_millis(1000)));
    // During two seconds peer is banned so no connection is possible.
    runner.push(Action::CheckRoutingTable(0, vec![]));
    runner.push(Action::CheckRoutingTable(1, vec![]));

    // After two seconds peer is unbanned and they should be able to connect again.
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    start_test(runner)
}
