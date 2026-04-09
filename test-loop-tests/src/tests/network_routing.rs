use crate::setup::builder::TestLoopBuilder;
use near_network::routing::NextHopTable;
use near_o11y::testonly::init_test_logger;
use std::collections::HashMap;
use std::sync::Arc;

/// Test multi-hop routing: 3 nodes where node 0 and node 2 are not directly
/// connected. Messages must route through node 1.
#[test]
fn test_multi_hop_message_routing() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().validators(3, 0).build();

    // After build (which populates fully-connected routing tables), modify
    // routing tables to force a linear topology: 0 <-> 1 <-> 2
    // Node 0 reaches node 2 via node 1.
    // Node 2 reaches node 0 via node 1.
    // Node 1 reaches both directly.
    let peer0 = env.node_datas[0].peer_id.clone();
    let peer1 = env.node_datas[1].peer_id.clone();
    let peer2 = env.node_datas[2].peer_id.clone();

    let node_network_states = env.shared_state.node_network_states.lock();

    // Node 0: can reach 1 directly, reaches 2 via 1.
    if let Some(state) = node_network_states.get(&peer0) {
        let mut hops: NextHopTable = HashMap::new();
        hops.insert(peer1.clone(), vec![peer1.clone()]);
        hops.insert(peer2.clone(), vec![peer1.clone()]); // route through 1
        state.graph.routing_table.update(Arc::new(hops), Arc::new(HashMap::new()));
    }

    // Node 1: can reach both directly.
    if let Some(state) = node_network_states.get(&peer1) {
        let mut hops: NextHopTable = HashMap::new();
        hops.insert(peer0.clone(), vec![peer0.clone()]);
        hops.insert(peer2.clone(), vec![peer2.clone()]);
        state.graph.routing_table.update(Arc::new(hops), Arc::new(HashMap::new()));
    }

    // Node 2: can reach 1 directly, reaches 0 via 1.
    if let Some(state) = node_network_states.get(&peer2) {
        let mut hops: NextHopTable = HashMap::new();
        hops.insert(peer1.clone(), vec![peer1.clone()]);
        hops.insert(peer0, vec![peer1]); // route through 1
        state.graph.routing_table.update(Arc::new(hops), Arc::new(HashMap::new()));
    }

    drop(node_network_states);

    // Run for several blocks to verify the network functions with multi-hop routing.
    env.node_runner(0).run_until_head_height(10);
}
