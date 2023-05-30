use crate::routing::{GraphConfigV2, GraphV2};
use crate::test_utils::expected_routing_tables;
use crate::test_utils::random_peer_id;
use crate::testonly::make_rng;
use crate::types::Edge;
use near_primitives::network::PeerId;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashMap;

// Creates a GraphV2 instance, populates its edge_cache with the edges in the given SPT,
// then has the graph calculate distances within the tree from the root.
//
// Verifies that the calculated distances match those in `expected`.
//
// When populating the edge_cache, may intersperse some extraneous edges to make sure
// that unrelated state in the cache does not interfere with distance calculations.
fn verify_calculate_distances(
    expected: Option<HashMap<PeerId, i32>>,
    root: PeerId,
    spt: Vec<Edge>,
) {
    let mut rng = make_rng(921853233);
    let rng = &mut rng;

    for num_extraneous_edges in 0..3 {
        let graph = GraphV2::new(GraphConfigV2 { node_id: root.clone(), prune_edges_after: None });
        let mut inner = graph.inner.lock();

        // Insert both the SPT's edges and the extraneous edges in a random order
        {
            let mut edges = spt.clone();
            for _ in 0..num_extraneous_edges {
                edges.push(Edge::make_fake_edge(
                    random_peer_id(),
                    random_peer_id(),
                    rng.gen::<u64>(),
                ));
            }

            edges.shuffle(rng);
            for edge in &edges {
                inner.edge_cache.insert_active_edge(edge);
            }
        }

        let calculated = inner.calculate_distances(&root, &spt);
        match expected {
            Some(ref expected) => {
                let calculated = calculated.unwrap();

                // Check that expected distances match the calculated ones
                for (node, expected_distance) in expected {
                    let id = inner.edge_cache.get_id(node);
                    assert_eq!(*expected_distance, calculated[id as usize]);
                }

                // Make sure there are no unexpected entries in `calculated_distances`
                assert_eq!(
                    calculated.len(),
                    expected.len()
                        + calculated.iter().map(|d| if *d == -1 { 1 } else { 0 }).sum::<usize>()
                );
            }
            None => {
                assert_eq!(None, calculated);
            }
        }
    }
}

#[test]
fn calculate_distances() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 123);
    let edge2 = Edge::make_fake_edge(node0.clone(), node2.clone(), 123);

    // Test behavior of distance calculation on an empty tree
    verify_calculate_distances(Some(HashMap::from([(node0.clone(), 0)])), node0.clone(), vec![]);

    // Test behavior of distance calculation on a simple tree 0--1
    verify_calculate_distances(
        Some(HashMap::from([(node0.clone(), 0), (node1.clone(), 1)])),
        node0.clone(),
        vec![edge0.clone()],
    );

    // Distance calculation should reject a tree which doesn't contain the root
    verify_calculate_distances(None, node0.clone(), vec![edge1.clone()]);

    // Test behavior of distance calculation on a line graph 0--1--2
    verify_calculate_distances(
        Some(HashMap::from([(node0.clone(), 0), (node1.clone(), 1), (node2.clone(), 2)])),
        node0.clone(),
        vec![edge0.clone(), edge1.clone()],
    );
    verify_calculate_distances(
        Some(HashMap::from([(node0.clone(), 1), (node1.clone(), 0), (node2, 1)])),
        node1,
        vec![edge0.clone(), edge1.clone()],
    );

    // Distance calculation rejects non-trees
    verify_calculate_distances(None, node0, vec![edge0, edge1, edge2]);
}

#[test]
fn calculate_next_hops() {
    let node0 = random_peer_id();
    let graph = GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None });

    // Test behavior on a node with no peers
    assert_eq!((HashMap::new(), HashMap::from([(node0.clone(), 0)])), graph.compute_next_hops());

    // Add a peer node1
    let node1 = random_peer_id();
    let edge01 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    assert!(graph.update_shortest_path_tree(node1.clone(), vec![edge01.clone()]));

    let (next_hops, distance) = graph.compute_next_hops();
    assert!(expected_routing_tables(&next_hops, &[(node1.clone(), vec![node1.clone()])]));
    assert_eq!(distance, HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]));

    // Add another peer node2 advertising a node3 behind it
    let node2 = random_peer_id();
    let node3 = random_peer_id();
    let edge02 = Edge::make_fake_edge(node0.clone(), node2.clone(), 123);
    let edge23 = Edge::make_fake_edge(node2.clone(), node3.clone(), 123);
    assert!(graph.update_shortest_path_tree(node2.clone(), vec![edge02.clone(), edge23]));

    let (next_hops, distance) = graph.compute_next_hops();
    assert!(expected_routing_tables(
        &next_hops,
        &[
            (node1.clone(), vec![node1.clone()]),
            (node2.clone(), vec![node2.clone()]),
            (node3.clone(), vec![node2.clone()]),
        ]
    ));
    assert_eq!(
        distance,
        HashMap::from([
            (node0.clone(), 0),
            (node1.clone(), 1),
            (node2.clone(), 1),
            (node3.clone(), 2)
        ])
    );

    // Update the SPT for node1, also advertising node3 behind it
    let edge13 = Edge::make_fake_edge(node1.clone(), node3.clone(), 123);
    assert!(graph.update_shortest_path_tree(node1.clone(), vec![edge01, edge13]));

    let (next_hops, distance) = graph.compute_next_hops();
    assert!(expected_routing_tables(
        &next_hops,
        &[
            (node1.clone(), vec![node1.clone()]),
            (node2.clone(), vec![node2.clone()]),
            (node3.clone(), vec![node1.clone(), node2.clone()]),
        ]
    ));
    assert_eq!(
        distance,
        HashMap::from([
            (node0.clone(), 0),
            (node1.clone(), 1),
            (node2.clone(), 1),
            (node3.clone(), 2)
        ])
    );

    // Update the SPT for node2, removing the route to node3
    assert!(graph.update_shortest_path_tree(node2.clone(), vec![edge02]));

    let (next_hops, distance) = graph.compute_next_hops();
    assert!(expected_routing_tables(
        &next_hops,
        &[
            (node1.clone(), vec![node1.clone()]),
            (node2.clone(), vec![node2.clone()]),
            (node3.clone(), vec![node1.clone()]),
        ]
    ));
    assert_eq!(distance, HashMap::from([(node0, 0), (node1, 1), (node2, 1), (node3, 2)]));
}
