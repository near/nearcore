use crate::tests::network::runner::*;
use near_async::time;
use std::cmp::min;

/// Check that a node is able to connect to the network, even if the number
/// of active peers of every node is high.
///
/// Spawn a network with `num_node` nodes and with top connections allowed
/// for every peer `max_num_peers`. Wait until every peer has `expected_connections`
/// active peers and start new node. Wait until new node is connected.
pub fn connect_at_max_capacity(
    num_node: usize,
    ideal_lo: u32,
    ideal_hi: u32,
    max_num_peers: u32,
    expected_connections: usize,
    extra_nodes: usize,
) -> anyhow::Result<()> {
    // Use at most 4 boot nodes
    let total_boot_nodes = min(num_node, 4);

    let boot_nodes = (0..total_boot_nodes).collect::<Vec<_>>();

    let mut runner = Runner::new(num_node + extra_nodes, num_node)
        .enable_outbound()
        .max_num_peers(max_num_peers)
        .use_boot_nodes(boot_nodes)
        .safe_set_size(1)
        .minimum_outbound_peers(0)
        .ideal_connections(ideal_lo, ideal_hi);

    let total_nodes = num_node + extra_nodes;

    // Stop all extra nodes
    for node_id in num_node..total_nodes {
        runner.push(Action::Stop(node_id));
    }

    // Wait until all running nodes have expected connections
    for node_id in 0..num_node {
        runner.push_action(check_expected_connections(node_id, Some(expected_connections), None));
    }

    // Restart stopped nodes
    for node_id in num_node..total_nodes {
        runner.push_action(restart(node_id));
    }

    // Wait until all nodes have expected connections
    for node_id in 0..total_nodes {
        runner.push_action(check_expected_connections(node_id, Some(expected_connections), None));
    }

    start_test(runner)
}

/// Check that two nodes are able to connect if they only know themselves from boot list.
#[test]
fn simple_network() -> anyhow::Result<()> {
    connect_at_max_capacity(2, 1, 1, 1, 1, 0)
}

/// Start 4 nodes and connect them all with each other.
/// Create new node, it should be able to connect since max allowed peers is 4.
#[test]
fn connect_on_almost_full_network() -> anyhow::Result<()> {
    connect_at_max_capacity(4, 1, 3, 4, 1, 1)
}

/// Start 4 nodes and connect them all with each other.
/// Create new node, it should be able to connect even if max allowed peers is 3.
#[test]
fn connect_on_full_network() -> anyhow::Result<()> {
    connect_at_max_capacity(5, 2, 3, 4, 2, 1)
}

#[test]
fn connect_whitelisted() -> anyhow::Result<()> {
    let nodes = 4;
    let validators = 2;
    let mut runner = Runner::new(nodes, validators)
        .enable_outbound()
        .safe_set_size(1)
        .minimum_outbound_peers(0)
        .max_num_peers(2)
        .ideal_connections(2, 2)
        // 3 is whitelisted by 0.
        .add_to_whitelist(0, 3);
    // Fill completely the capacity of the first 3 nodes.
    runner.push(Action::AddEdge { from: 0, to: 1, force: true });
    runner.push(Action::AddEdge { from: 1, to: 2, force: true });
    runner.push(Action::AddEdge { from: 2, to: 0, force: true });
    // Try to establish an extra non-whitelisted connection.
    runner.push(Action::AddEdge { from: 3, to: 1, force: false });
    runner.push(Action::AddEdge { from: 3, to: 2, force: false });
    // Establish an extra whitelisted connection.
    runner.push(Action::AddEdge { from: 3, to: 0, force: true });
    // Wait for the topology to stabilize.
    // - 3 shouldn't be able to establish a connection to 1,2
    // - 1 shouldn't drop connections to 0,2,3, even though the
    //   connection limit is 2, since 0<->3 connection doesn't
    //   count towards this limit.
    runner.push(Action::Wait(time::Duration::milliseconds(200)));
    runner.push_action(assert_expected_peers(0, vec![1, 2, 3]));
    runner.push_action(assert_expected_peers(1, vec![0, 2]));
    runner.push_action(assert_expected_peers(2, vec![0, 1]));
    runner.push_action(assert_expected_peers(3, vec![0]));
    start_test(runner)
}
