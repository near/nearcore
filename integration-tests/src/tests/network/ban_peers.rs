use crate::tests::network::runner::*;
use near_network::time;

/// Check we don't try to connect to a banned peer and we don't accept
/// incoming connection from it.
#[test]
fn dont_connect_to_banned_peer() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2)
        .enable_outbound()
        .use_boot_nodes(vec![0, 1])
        .ban_window(time::Duration::seconds(60));

    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    runner.push_action(ban_peer(0, 1));
    // It needs to wait a large timeout so we are sure both peer don't establish a connection.
    runner.push(Action::Wait(time::Duration::milliseconds(1000)));

    runner.push(Action::CheckRoutingTable(0, vec![]));
    runner.push(Action::CheckRoutingTable(1, vec![]));

    start_test(runner)
}
