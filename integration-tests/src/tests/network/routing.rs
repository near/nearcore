use crate::tests::network::runner::*;
use near_network::time;

#[test]
fn simple() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 1);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    start_test(runner)
}

#[test]
fn from_boot_nodes() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 1).use_boot_nodes(vec![0]).enable_outbound();

    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    start_test(runner)
}

#[test]
fn three_nodes_path() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));

    start_test(runner)
}

#[test]
fn three_nodes_star() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));
    runner.push(Action::AddEdge { from: 0, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![0])]));

    start_test(runner)
}

#[test]
fn join_components() -> anyhow::Result<()> {
    let mut runner = Runner::new(4, 4);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 2, to: 3, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));
    runner.push(Action::CheckRoutingTable(2, vec![(3, vec![3])]));
    runner.push(Action::CheckRoutingTable(3, vec![(2, vec![2])]));
    runner.push(Action::AddEdge { from: 0, to: 2, force: true });
    runner.push(Action::AddEdge { from: 3, to: 1, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2]), (3, vec![1, 2])]));
    runner.push(Action::CheckRoutingTable(3, vec![(1, vec![1]), (2, vec![2]), (0, vec![1, 2])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (3, vec![3]), (2, vec![0, 3])]));
    runner.push(Action::CheckRoutingTable(2, vec![(0, vec![0]), (3, vec![3]), (1, vec![0, 3])]));

    start_test(runner)
}

#[test]
fn account_propagation() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::CheckAccountId(1, vec![0, 1]));
    runner.push(Action::AddEdge { from: 0, to: 2, force: true });
    runner.push(Action::CheckAccountId(2, vec![0, 1]));

    start_test(runner)
}

#[test]
fn ping_simple() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::PingTo { source: 0, nonce: 0, target: 1 });
    runner.push(Action::CheckPingPong(1, vec![Ping { nonce: 0, source: 0 }], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![Pong { nonce: 0, source: 1 }]));

    start_test(runner)
}

#[test]
/// Crate 3 nodes connected in a line and try to use Ping.
fn ping_jump() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 2);

    // Add edges
    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    // Check routing tables and wait for `PeerManager` to update it's routing table
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(0, vec![1]), (1, vec![1])]));

    // Try Pinging from node 0 to 2
    runner.push(Action::PingTo { source: 0, nonce: 0, target: 2 });
    // Check whenever Node 2 got message from 0
    runner.push(Action::CheckPingPong(2, vec![Ping { nonce: 0, source: 0 }], vec![]));
    // Check whenever Node 0 got reply from 2.
    runner.push(Action::CheckPingPong(0, vec![], vec![Pong { nonce: 0, source: 2 }]));

    start_test(runner)
}

/// Test routed messages are not dropped if have enough TTL.
/// Spawn three nodes and connect them in a line:
///
/// 0 ---- 1 ---- 2
///
/// Set routed message ttl to 2, so routed message can't pass through more than 2 edges.
/// Send Ping from 0 to 2. It should arrive since there are only 2 edges from 0 to 2.
/// Check Ping arrive at node 2 and later Pong arrive at node 0.
#[test]
fn test_dont_drop_after_ttl() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 1).routed_message_ttl(2);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::PingTo { source: 0, nonce: 0, target: 2 });
    runner.push(Action::CheckPingPong(2, vec![Ping { nonce: 0, source: 0 }], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![Pong { nonce: 0, source: 2 }]));

    start_test(runner)
}

/// Test routed messages are dropped if don't have enough TTL.
/// Spawn three nodes and connect them in a line:
///
/// 0 ---- 1 ---- 2
///
/// Set routed message ttl to 1, so routed message can't pass through more than 1 edges.
/// Send Ping from 0 to 2. It should not arrive since there are 2 edges from 0 to 2.
/// Check none of Ping and Pong arrived.
#[test]
fn test_drop_after_ttl() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 1).routed_message_ttl(1);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::PingTo { source: 0, nonce: 0, target: 2 });
    runner.push(Action::Wait(time::Duration::milliseconds(100)));
    runner.push(Action::CheckPingPong(2, vec![], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![]));

    start_test(runner)
}

#[test]
fn simple_remove() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 3);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));
    runner.push(Action::Stop(1));
    runner.push(Action::CheckRoutingTable(0, vec![]));
    runner.push(Action::CheckRoutingTable(2, vec![]));

    start_test(runner)
}

#[test]
fn square() -> anyhow::Result<()> {
    let mut runner = Runner::new(4, 4);

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::AddEdge { from: 2, to: 3, force: true });
    runner.push(Action::AddEdge { from: 3, to: 0, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (3, vec![3]), (2, vec![1, 3])]));
    runner.push(Action::Stop(1));
    runner.push(Action::CheckRoutingTable(0, vec![(3, vec![3]), (2, vec![3])]));
    runner.push(Action::CheckRoutingTable(2, vec![(3, vec![3]), (0, vec![3])]));
    runner.push(Action::CheckRoutingTable(3, vec![(2, vec![2]), (0, vec![0])]));

    start_test(runner)
}

#[test]
fn blacklist_01() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2).add_to_blacklist(0, Some(1)).use_boot_nodes(vec![0]);

    runner.push(Action::Wait(time::Duration::milliseconds(100)));
    runner.push(Action::CheckRoutingTable(1, vec![]));
    runner.push(Action::CheckRoutingTable(0, vec![]));

    start_test(runner)
}

#[test]
fn blacklist_10() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2).add_to_blacklist(1, Some(0)).use_boot_nodes(vec![0]);

    runner.push(Action::Wait(time::Duration::milliseconds(100)));
    runner.push(Action::CheckRoutingTable(1, vec![]));
    runner.push(Action::CheckRoutingTable(0, vec![]));

    start_test(runner)
}

#[test]
fn blacklist_all() -> anyhow::Result<()> {
    let mut runner = Runner::new(2, 2).add_to_blacklist(0, None).use_boot_nodes(vec![0]);

    runner.push(Action::Wait(time::Duration::milliseconds(100)));
    runner.push(Action::CheckRoutingTable(1, vec![]));
    runner.push(Action::CheckRoutingTable(0, vec![]));

    start_test(runner)
}

/// Spawn 4 nodes with max peers required equal 2. Connect first three peers in a triangle.
/// Try to connect peer3 to peer0 and see it fail since first three peer are at max capacity.
#[test]
fn max_num_peers_limit() -> anyhow::Result<()> {
    let mut runner = Runner::new(4, 4).max_num_peers(2).ideal_connections(2, 2).enable_outbound();

    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![0])]));
    runner.push(Action::AddEdge { from: 3, to: 0, force: false });
    runner.push(Action::Wait(time::Duration::milliseconds(100)));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![0])]));
    runner.push(Action::CheckRoutingTable(3, vec![]));

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

/// Spawn 3 nodes, add edges to form 0---1---2 line. Then send 2 pings from 0 to 2, one should be dropped.
#[test]
fn test_dropping_routing_messages() -> anyhow::Result<()> {
    let mut runner = Runner::new(3, 3).max_num_peers(2).ideal_connections(2, 2).enable_outbound();

    runner.push(Action::SetOptions { target: 0, max_num_peers: Some(1) });
    runner.push(Action::SetOptions { target: 1, max_num_peers: Some(2) });
    runner.push(Action::SetOptions { target: 2, max_num_peers: Some(1) });
    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(0, vec![1]), (1, vec![1])]));

    // Send two identical messages. One will be dropped, because the delay between them was less than 100ms.
    runner.push(Action::PingTo { source: 0, nonce: 0, target: 2 });
    runner.push(Action::PingTo { source: 0, nonce: 0, target: 2 });
    runner.push(Action::CheckPingPong(2, vec![Ping { nonce: 0, source: 0 }], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![Pong { nonce: 0, source: 2 }]));

    // Send two identical messages but in 300ms interval so they don't get dropped.
    runner.push(Action::PingTo { source: 0, nonce: 1, target: 2 });
    runner.push(Action::Wait(time::Duration::milliseconds(300)));
    runner.push(Action::PingTo { source: 0, nonce: 1, target: 2 });
    runner.push(Action::CheckPingPong(
        2,
        vec![
            Ping { nonce: 0, source: 0 },
            Ping { nonce: 1, source: 0 },
            Ping { nonce: 1, source: 0 },
        ],
        vec![],
    ));
    runner.push(Action::CheckPingPong(
        0,
        vec![],
        vec![
            Pong { nonce: 0, source: 2 },
            Pong { nonce: 1, source: 2 },
            Pong { nonce: 1, source: 2 },
        ],
    ));

    start_test(runner)
}
