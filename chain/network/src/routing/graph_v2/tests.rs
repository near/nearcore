use crate::network_protocol;
use crate::network_protocol::AdvertisedRoute;
use crate::routing::{GraphConfigV2, GraphV2, NetworkTopologyChange};
use crate::test_utils::expected_routing_tables;
use crate::test_utils::random_peer_id;
use crate::types::Edge;
use near_primitives::network::PeerId;
use std::collections::HashMap;
use std::sync::Arc;

// Calls `calculate_tree_distances` on the given `root` and `edges`.
// Verifies that the calculated distances and first steps match those in `expected`.
fn verify_calculate_tree_distances(
    expected: Option<HashMap<PeerId, (i32, Option<PeerId>)>>,
    root: PeerId,
    edges: Vec<Edge>,
) {
    let graph = GraphV2::new(GraphConfigV2 { node_id: root.clone(), prune_edges_after: None });
    let mut inner = graph.inner.lock();

    let calculated = inner.calculate_tree_distances(&root, &edges);
    match expected {
        Some(ref expected) => {
            let (distance, first_step) = calculated.unwrap();

            // Check for the expected entries
            for (node, (expected_distance, expected_first_step)) in expected {
                let id = inner.edge_cache.get_id(node) as usize;

                // Expected distance should match the calculated one
                assert_eq!(*expected_distance, distance[id]);

                // Expected first step should match the calculated one
                match expected_first_step {
                    Some(expected_first_step) => {
                        assert!(
                            first_step[id] == inner.edge_cache.get_id(&expected_first_step) as i32
                        );
                    }
                    None => {
                        assert!(first_step[id] == -1);
                    }
                }
            }

            // Make sure there are no unexpected entries
            let mut calculated_reachable_nodes = 0;
            for id in 0..inner.edge_cache.max_id() {
                if distance[id] != -1 || first_step[id] != -1 {
                    calculated_reachable_nodes += 1;
                }
            }
            assert_eq!(calculated_reachable_nodes, expected.len());
        }
        None => {
            assert_eq!(None, calculated);
        }
    }
}

#[test]
fn calculate_tree_distances() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 123);
    let edge2 = Edge::make_fake_edge(node0.clone(), node2.clone(), 123);

    // Test behavior of distance calculation on an empty tree
    verify_calculate_tree_distances(
        Some(HashMap::from([(node0.clone(), (0, None))])),
        node0.clone(),
        vec![],
    );

    // Test behavior of distance calculation on a simple tree 0--1
    verify_calculate_tree_distances(
        Some(HashMap::from([
            (node0.clone(), (0, None)),
            (node1.clone(), (1, Some(node1.clone()))),
        ])),
        node0.clone(),
        vec![edge0.clone()],
    );

    // Distance calculation should reject a tree which doesn't contain the root
    verify_calculate_tree_distances(None, node0.clone(), vec![edge1.clone()]);

    // Test behavior of distance calculation on a line graph 0--1--2
    verify_calculate_tree_distances(
        Some(HashMap::from([
            (node0.clone(), (0, None)),
            (node1.clone(), (1, Some(node1.clone()))),
            (node2.clone(), (2, Some(node1.clone()))),
        ])),
        node0.clone(),
        vec![edge0.clone(), edge1.clone()],
    );
    verify_calculate_tree_distances(
        Some(HashMap::from([
            (node0.clone(), (1, Some(node0.clone()))),
            (node1.clone(), (0, None)),
            (node2.clone(), (1, Some(node2))),
        ])),
        node1,
        vec![edge0.clone(), edge1.clone()],
    );

    // Distance calculation rejects non-trees
    verify_calculate_tree_distances(None, node0, vec![edge0, edge1, edge2]);
}

#[test]
fn compute_next_hops() {
    let node0 = random_peer_id();
    let graph = GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None });

    // Test behavior on a node with no peers
    assert_eq!((HashMap::new(), HashMap::from([(node0.clone(), 0)])), graph.compute_next_hops());

    // Add a peer node1; 0--1
    let node1 = random_peer_id();
    let edge01 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    assert!(graph.update_distance_vector(
        node1.clone(),
        vec![
            AdvertisedRoute { destination: node1.clone(), length: 0 },
            AdvertisedRoute { destination: node0.clone(), length: 1 }
        ],
        vec![edge01.clone()]
    ));

    let (next_hops, distance) = graph.compute_next_hops();
    assert!(expected_routing_tables(&next_hops, &[(node1.clone(), vec![node1.clone()])]));
    assert_eq!(distance, HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]));

    // Add another peer node2 advertising a node3 behind it; 0--2--3
    let node2 = random_peer_id();
    let node3 = random_peer_id();
    let edge02 = Edge::make_fake_edge(node0.clone(), node2.clone(), 123);
    let edge23 = Edge::make_fake_edge(node2.clone(), node3.clone(), 123);
    assert!(graph.update_distance_vector(
        node2.clone(),
        vec![
            AdvertisedRoute { destination: node2.clone(), length: 0 },
            AdvertisedRoute { destination: node0.clone(), length: 1 },
            AdvertisedRoute { destination: node3.clone(), length: 1 },
        ],
        vec![edge02.clone(), edge23]
    ));

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

    // Update the SPT for node1, also advertising node3 behind it; 0--1--3
    let edge13 = Edge::make_fake_edge(node1.clone(), node3.clone(), 123);
    assert!(graph.update_distance_vector(
        node1.clone(),
        vec![
            AdvertisedRoute { destination: node1.clone(), length: 0 },
            AdvertisedRoute { destination: node0.clone(), length: 1 },
            AdvertisedRoute { destination: node3.clone(), length: 1 },
        ],
        vec![edge01, edge13]
    ));

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

    // Update the SPT for node2, removing the route to node3; 0--2
    assert!(graph.update_distance_vector(
        node2.clone(),
        vec![
            AdvertisedRoute { destination: node2.clone(), length: 0 },
            AdvertisedRoute { destination: node0.clone(), length: 1 },
        ],
        vec![edge02]
    ));

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

#[test]
fn compute_next_hops_discard_loop() {
    let node0 = random_peer_id();
    let graph = GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None });

    // Add a peer node1 which advertises node2 via node0; 2--0--1
    let node1 = random_peer_id();
    let node2 = random_peer_id();
    let edge01 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge02 = Edge::make_fake_edge(node0.clone(), node2.clone(), 123);
    assert!(graph.update_distance_vector(
        node1.clone(),
        vec![
            AdvertisedRoute { destination: node1.clone(), length: 0 },
            AdvertisedRoute { destination: node0.clone(), length: 1 },
            AdvertisedRoute { destination: node2, length: 2 },
        ],
        vec![edge01, edge02]
    ));

    // node2 should be ignored because the advertised route to it goes back through the local node
    let (next_hops, distance) = graph.compute_next_hops();
    assert!(expected_routing_tables(&next_hops, &[(node1.clone(), vec![node1.clone()])]));
    assert_eq!(distance, HashMap::from([(node0, 0), (node1, 1)]));
}

#[tokio::test]
async fn test_process_network_event() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let graph =
        Arc::new(GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None }));

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 456);

    // Process a new connection 0--1
    let distance_vector = graph
        .process_network_event(NetworkTopologyChange::PeerConnected(node1.clone(), edge0.clone()))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]),
        &distance_vector,
    );

    // Receive a DistanceVector from node1 with node2 behind it; 0--1--2
    let distance_vector = graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedRoutes(
            network_protocol::DistanceVector {
                root: node1.clone(),
                routes: vec![
                    AdvertisedRoute { destination: node1.clone(), length: 0 },
                    AdvertisedRoute { destination: node0.clone(), length: 1 },
                    AdvertisedRoute { destination: node2.clone(), length: 1 },
                ],
                edges: vec![edge0.clone(), edge1.clone()],
            },
        ))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1), (node2.clone(), 2)]),
        &distance_vector,
    );

    // Process a local update (nonce refresh) to the connection 0--1
    let edge0_refreshed = Edge::make_fake_edge(node0.clone(), node1.clone(), 789);
    let distance_vector = graph
        .process_network_event(NetworkTopologyChange::PeerConnected(node1.clone(), edge0_refreshed))
        .await;
    // This update doesn't trigger a broadcast because node0's available routes haven't changed
    assert_eq!(None, distance_vector);
    // node0's locally stored DistanceVector should have the route to node2
    let distance_vector = graph.inner.lock().my_distance_vector.clone();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1), (node2.clone(), 2)]),
        &distance_vector,
    );

    // Process disconnection of node1
    let distance_vector = graph
        .process_network_event(NetworkTopologyChange::PeerDisconnected(node1.clone()))
        .await
        .unwrap();
    graph.verify_own_distance_vector(HashMap::from([(node0.clone(), 0)]), &distance_vector);
}
