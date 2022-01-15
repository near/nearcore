pub use crate::tests::network::runner::*;
use std::cmp::min;

/// Check that a node is able to connect to the network, even if the number
/// of active peers of every node is high.
///
/// Spawn a network with `num_node` nodes and with top connections allowed
/// for every peer `max_num_peers`. Wait until every peer has `expected_connections`
/// active peers and start new node. Wait until new node is connected.
pub fn connect_at_max_capacity(
    num_node: usize,
    ideal_lo: usize,
    ideal_hi: usize,
    max_num_peers: usize,
    expected_connections: usize,
    extra_nodes: usize,
) {
    // Use at most 4 boot nodes
    let total_boot_nodes = min(num_node, 4);

    let boot_nodes = (0..total_boot_nodes).collect::<Vec<_>>();

    let mut runner = Runner::new(num_node + extra_nodes, num_node)
        .enable_outbound()
        .max_num_peers(max_num_peers)
        .use_boot_nodes(boot_nodes)
        .safe_set_size(0)
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

    start_test(runner);
}

/// Check that two nodes are able to connect if they only know themselves from boot list.
#[test]
fn simple_network() {
    connect_at_max_capacity(2, 1, 1, 1, 1, 0);
}

/// Start 4 nodes and connect them all with each other.
/// Create new node, it should be able to connect since max allowed peers is 4.
#[test]
fn connect_on_almost_full_network() {
    connect_at_max_capacity(4, 1, 3, 4, 1, 1);
}

/// Start 4 nodes and connect them all with each other.
/// Create new node, it should be able to connect even if max allowed peers is 3.
#[test]
fn connect_on_full_network() {
    connect_at_max_capacity(5, 2, 3, 4, 2, 1);
}
