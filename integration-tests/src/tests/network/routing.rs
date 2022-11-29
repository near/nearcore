use crate::tests::network::runner::*;
use near_network::time;

#[test]
fn account_propagation() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::CheckAccountId(1, vec![0, 1]));
    runner.push(Action::AddEdge { from: 0, to: 2, force: true });
    runner.push(Action::CheckAccountId(2, vec![0, 1]));

    start_test(runner)
}

/// Check that two archival nodes keep connected after network rebalance. Nodes 0 and 1 are archival nodes, others aren't.
/// Initially connect 2, 3, 4 to 0. Then connect 1 to 0, this connection should persist, even after other nodes tries
/// to connect to node 0 again.
///
/// Do four rounds where 2, 3, 4 tries to connect to 0 and check that connection between 0 and 1 was never dropped.
#[test]
// TODO(#5389) fix this test, ignoring for now to unlock merging
fn archival_node() -> anyhow::Result<()> {
    let mut runner = Runner::new(5, 5)
        .max_num_peers(3)
        .ideal_connections(2, 2)
        .safe_set_size(1)
        .minimum_outbound_peers(0)
        .set_as_archival(0)
        .set_as_archival(1);

    runner.push(Action::AddEdge { from: 2, to: 0, force: true });
    runner.push(Action::Wait(time::Duration::milliseconds(50)));
    runner.push(Action::AddEdge { from: 3, to: 0, force: true });
    runner.push(Action::Wait(time::Duration::milliseconds(50)));
    runner.push(Action::AddEdge { from: 4, to: 0, force: true });
    runner.push(Action::Wait(time::Duration::milliseconds(50)));
    runner.push_action(check_expected_connections(0, Some(2), Some(2)));

    runner.push(Action::AddEdge { from: 1, to: 0, force: true });
    runner.push(Action::Wait(time::Duration::milliseconds(50)));
    runner.push_action(check_expected_connections(0, Some(2), Some(2)));
    runner.push_action(check_direct_connection(0, 1));

    for _step in 0..4 {
        runner.push(Action::AddEdge { from: 2, to: 0, force: true });
        runner.push(Action::Wait(time::Duration::milliseconds(50)));
        runner.push(Action::AddEdge { from: 3, to: 0, force: true });
        runner.push(Action::Wait(time::Duration::milliseconds(50)));
        runner.push(Action::AddEdge { from: 4, to: 0, force: true });
        runner.push(Action::Wait(time::Duration::milliseconds(50)));
        runner.push_action(check_expected_connections(0, Some(2), Some(2)));
        runner.push_action(check_direct_connection(0, 1));
    }

    start_test(runner)
}
