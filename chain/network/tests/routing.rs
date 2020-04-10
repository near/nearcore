pub use runner::*;

mod runner;

#[test]
fn simple() {
    let mut runner = Runner::new(2, 1);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    start_test(runner);
}

#[test]
fn from_boot_nodes() {
    let mut runner = Runner::new(2, 1).use_boot_nodes(vec![0]).enable_outbound();

    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));

    start_test(runner);
}

#[test]
fn three_nodes_path() {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));

    start_test(runner);
}

#[test]
fn three_nodes_star() {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));
    runner.push(Action::AddEdge(0, 2));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![0])]));

    start_test(runner);
}

#[test]
fn join_components() {
    let mut runner = Runner::new(4, 4);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(2, 3));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0])]));
    runner.push(Action::CheckRoutingTable(2, vec![(3, vec![3])]));
    runner.push(Action::CheckRoutingTable(3, vec![(2, vec![2])]));
    runner.push(Action::AddEdge(0, 2));
    runner.push(Action::AddEdge(3, 1));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2]), (3, vec![1, 2])]));
    runner.push(Action::CheckRoutingTable(3, vec![(1, vec![1]), (2, vec![2]), (0, vec![1, 2])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (3, vec![3]), (2, vec![0, 3])]));
    runner.push(Action::CheckRoutingTable(2, vec![(0, vec![0]), (3, vec![3]), (1, vec![0, 3])]));

    start_test(runner);
}

#[test]
fn account_propagation() {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::CheckAccountId(1, vec![0, 1]));
    runner.push(Action::AddEdge(0, 2));
    runner.push(Action::CheckAccountId(2, vec![0, 1]));

    start_test(runner);
}

#[test]
fn ping_simple() {
    let mut runner = Runner::new(2, 2);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1])]));
    runner.push(Action::PingTo(0, 0, 1));
    runner.push(Action::CheckPingPong(1, vec![(0, 0)], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![(0, 1)]));

    start_test(runner);
}

#[test]
fn ping_jump() {
    let mut runner = Runner::new(3, 2);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::PingTo(0, 0, 2));
    runner.push(Action::CheckPingPong(2, vec![(0, 0)], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![(0, 2)]));

    start_test(runner);
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
fn test_dont_drop_after_ttl() {
    let mut runner = Runner::new(3, 1).routed_message_ttl(2);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::PingTo(0, 0, 2));
    runner.push(Action::CheckPingPong(2, vec![(0, 0)], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![(0, 2)]));

    start_test(runner);
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
fn test_drop_after_ttl() {
    let mut runner = Runner::new(3, 1).routed_message_ttl(1);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::PingTo(0, 0, 2));
    runner.push(Action::Wait(100));
    runner.push(Action::CheckPingPong(2, vec![], vec![]));
    runner.push(Action::CheckPingPong(0, vec![], vec![]));

    start_test(runner);
}

#[test]
fn simple_remove() {
    let mut runner = Runner::new(3, 3);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![1])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![1])]));
    runner.push(Action::Stop(1));
    runner.push(Action::CheckRoutingTable(0, vec![]));
    runner.push(Action::CheckRoutingTable(2, vec![]));

    start_test(runner);
}

#[test]
fn square() {
    let mut runner = Runner::new(4, 4);

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::AddEdge(2, 3));
    runner.push(Action::AddEdge(3, 0));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (3, vec![3]), (2, vec![1, 3])]));
    runner.push(Action::Stop(1));
    runner.push(Action::CheckRoutingTable(0, vec![(3, vec![3]), (2, vec![3])]));
    runner.push(Action::CheckRoutingTable(2, vec![(3, vec![3]), (0, vec![3])]));
    runner.push(Action::CheckRoutingTable(3, vec![(2, vec![2]), (0, vec![0])]));

    start_test(runner);
}

#[test]
fn blacklist_01() {
    let mut runner = Runner::new(2, 2).add_to_blacklist(0, Some(1)).use_boot_nodes(vec![0]);

    runner.push(Action::Wait(100));
    runner.push(Action::CheckRoutingTable(1, vec![]));
    runner.push(Action::CheckRoutingTable(0, vec![]));

    start_test(runner);
}

#[test]
fn blacklist_10() {
    let mut runner = Runner::new(2, 2).add_to_blacklist(1, Some(0)).use_boot_nodes(vec![0]);

    runner.push(Action::Wait(100));
    runner.push(Action::CheckRoutingTable(1, vec![]));
    runner.push(Action::CheckRoutingTable(0, vec![]));

    start_test(runner);
}

#[test]
fn blacklist_all() {
    let mut runner = Runner::new(2, 2).add_to_blacklist(0, None).use_boot_nodes(vec![0]);

    runner.push(Action::Wait(100));
    runner.push(Action::CheckRoutingTable(1, vec![]));
    runner.push(Action::CheckRoutingTable(0, vec![]));

    start_test(runner);
}

/// Spawn 4 nodes with max peers required equal 2. Connect first three peers in a triangle.
/// Try to connect peer3 to peer0 and see it fail since first three peer are at max capacity.
#[test]
fn max_peer_limit() {
    let mut runner = Runner::new(4, 4).max_peer(2).enable_outbound();

    runner.push(Action::AddEdge(0, 1));
    runner.push(Action::AddEdge(1, 2));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![0])]));
    runner.push(Action::AddEdge(3, 0));
    runner.push(Action::Wait(100));
    runner.push(Action::CheckRoutingTable(0, vec![(1, vec![1]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(1, vec![(0, vec![0]), (2, vec![2])]));
    runner.push(Action::CheckRoutingTable(2, vec![(1, vec![1]), (0, vec![0])]));
    runner.push(Action::CheckRoutingTable(3, vec![]));

    start_test(runner);
}
