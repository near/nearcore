use crate::routing::{GraphConfigV2, GraphV2};
use crate::test_utils::random_peer_id;
use crate::types::Edge;
use near_primitives::network::PeerId;
use std::collections::HashMap;

// Creates a GraphV2 instance, populates its EdgeCache with the edges in the given SPT,
// then has the graph calculate distances within the tree from the root.
// Verifies that the calculated distances match those in `expected`.
fn verify_calculate_distances(expected: HashMap<PeerId, i32>, root: PeerId, spt: Vec<Edge>) {
    let graph = GraphV2::new(GraphConfigV2 { node_id: root.clone(), prune_edges_after: None });
    let mut inner = graph.inner.lock();

    for edge in &spt {
        inner.edge_cache.insert_active_edge(edge);
    }

    let calculated = inner.calculate_distances(&root, &spt).unwrap();

    // Check that expected distances match the calculated ones
    for (node, expected_distance) in &expected {
        let id = inner.edge_cache.get_id(node);
        assert_eq!(*expected_distance, calculated[id as usize]);
    }

    // Make sure there are no unexpected entries in `calculated_distances`
    assert_eq!(
        calculated.len(),
        expected.len() + calculated.iter().map(|d| if *d == -1 { 1 } else { 0 }).sum::<usize>()
    );
}

#[test]
fn calculate_distances0() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);

    // Test behavior of distance calculation on an empty tree
    verify_calculate_distances(HashMap::from([(node0.clone(), 0)]), node0.clone(), vec![]);

    // Test behavior of distance calculation on a simple tree 0--1
    verify_calculate_distances(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]),
        node0.clone(),
        vec![edge0],
    );
}
