use crate::network_protocol;
use crate::network_protocol::AdvertisedPeerDistance;
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
    expected: Option<HashMap<PeerId, (u32, Option<PeerId>)>>,
    root: PeerId,
    edges: Vec<Edge>,
) {
    let graph = GraphV2::new(GraphConfigV2 { node_id: random_peer_id(), prune_edges_after: None });
    let mut inner = graph.inner.lock();

    let calculated = inner.calculate_tree_distances(&root, &edges);
    match expected {
        Some(ref expected) => {
            let (distance, first_step) = calculated.unwrap();

            // Check for the expected entries
            for (node, (expected_distance, expected_first_step)) in expected {
                let id = inner.edge_cache.get_id(node) as usize;

                // Map the expected first step to its internal label
                let expected_first_step =
                    expected_first_step.as_ref().map(|peer_id| inner.edge_cache.get_id(&peer_id));

                // Expected distance should match the calculated one
                assert_eq!(*expected_distance, distance[id].unwrap());

                // Expected first step should match the calculated one
                assert_eq!(expected_first_step, first_step[id]);
            }

            // Make sure there are no unexpected entries
            let mut calculated_reachable_nodes = 0;
            for id in 0..inner.edge_cache.max_id() {
                if distance[id].is_some() || first_step[id].is_some() {
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
    // Test again from root 1 in 0--1--2
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
            AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
            AdvertisedPeerDistance { destination: node0.clone(), distance: 1 }
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
            AdvertisedPeerDistance { destination: node2.clone(), distance: 0 },
            AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
            AdvertisedPeerDistance { destination: node3.clone(), distance: 1 },
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
            AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
            AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
            AdvertisedPeerDistance { destination: node3.clone(), distance: 1 },
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
            AdvertisedPeerDistance { destination: node2.clone(), distance: 0 },
            AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
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
            AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
            AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
            AdvertisedPeerDistance { destination: node2, distance: 2 },
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
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerConnected(node1.clone(), edge0.clone()))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]),
        &distance_vector_update,
    );

    // Receive a DistanceVector from node1 with node2 behind it; 0--1--2
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 1 },
                ],
                edges: vec![edge0.clone(), edge1.clone()],
            },
        ))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1), (node2.clone(), 2)]),
        &distance_vector_update,
    );

    // Process a local update (nonce refresh) to the connection 0--1
    let edge0_refreshed = Edge::make_fake_edge(node0.clone(), node1.clone(), 789);
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerConnected(node1.clone(), edge0_refreshed))
        .await;
    // This update doesn't trigger a broadcast because node0's available routes haven't changed
    assert_eq!(None, distance_vector_update);
    // node0's locally stored DistanceVector should have the route to node2
    let distance_vector_update = graph.inner.lock().my_distance_vector.clone();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1), (node2.clone(), 2)]),
        &distance_vector_update,
    );

    // Process disconnection of node1
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerDisconnected(node1.clone()))
        .await
        .unwrap();
    graph.verify_own_distance_vector(HashMap::from([(node0.clone(), 0)]), &distance_vector_update);
}

#[tokio::test]
async fn test_process_network_event_idempotent() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();

    let graph =
        Arc::new(GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None }));

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);

    // Process a new connection 0--1
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerConnected(node1.clone(), edge0.clone()))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]),
        &distance_vector_update,
    );
    // Process the same event without error
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerConnected(node1.clone(), edge0.clone()))
        .await;
    // This update doesn't trigger a broadcast because node0's available routes haven't changed
    assert_eq!(None, distance_vector_update);

    // Process disconnection of node1
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerDisconnected(node1.clone()))
        .await
        .unwrap();
    graph.verify_own_distance_vector(HashMap::from([(node0.clone(), 0)]), &distance_vector_update);
    // Process the same event without error
    let distance_vector_update =
        graph.process_network_event(NetworkTopologyChange::PeerDisconnected(node1.clone())).await;
    // This update doesn't trigger a broadcast because node0's available routes haven't changed
    assert_eq!(None, distance_vector_update);
}

#[tokio::test]
async fn test_receive_distance_vector_before_processing_local_connection() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let graph =
        Arc::new(GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None }));

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 456);

    // Receive a DistanceVector from node1 with node2 behind it; 0--1--2
    // The local node has not processed a NetworkTopologyChange::PeerConnected event
    // for node1, but it should handle this DistanceVector correctly anyway.
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 1 },
                ],
                edges: vec![edge0.clone(), edge1.clone()],
            },
        ))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1), (node2.clone(), 2)]),
        &distance_vector_update,
    );
}

#[tokio::test]
async fn test_receive_invalid_distance_vector() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let graph =
        Arc::new(GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None }));

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 456);

    graph
        .process_invalid_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 1 },
                ],
                // Missing edge
                edges: vec![edge1.clone()],
            },
        ))
        .await;

    graph
        .process_invalid_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                // Missing route shown by edges
                distances: vec![
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
                ],
                edges: vec![edge0.clone(), edge1.clone()],
            },
        ))
        .await;

    graph
        .process_invalid_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
                    // Route length is shorter than shown by edges
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 0 },
                ],
                edges: vec![edge0.clone(), edge1.clone()],
            },
        ))
        .await;
}

#[tokio::test]
async fn receive_distance_vector_without_route_to_local_node() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let graph =
        Arc::new(GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None }));

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 456);

    // Broadcasting a distance vector which doesn't have a route to the receiving node
    // is valid behavior, but it doesn't provide the receiving node any routes.
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![
                    // No route to the receiving node node0
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 1 },
                ],
                edges: vec![edge1.clone()],
            },
        ))
        .await;
    assert_eq!(None, distance_vector_update);

    // Let node0 realize it has a direct connection to node1
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerConnected(node1.clone(), edge0))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]),
        &distance_vector_update,
    );

    // Now the same advertised routes from the tree tree 1--2 can be handled by node0,
    // which will combine it with the direct edge 0--1 to produce 0--1--2.
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![
                    // No route to the receiving node node0
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 1 },
                ],
                edges: vec![edge1.clone()],
            },
        ))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1), (node2.clone(), 2)]),
        &distance_vector_update,
    );

    // node0 should also be able to handle node1's default DistanceVector with no edges
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![AdvertisedPeerDistance { destination: node1.clone(), distance: 0 }],
                edges: vec![],
            },
        ))
        .await
        .unwrap();
    graph.verify_own_distance_vector(
        HashMap::from([(node0.clone(), 0), (node1.clone(), 1)]),
        &distance_vector_update,
    );
}

/// This test produces a situation in which it is not possible for node0 to construct a spanning
/// tree which is exactly consistent with its distance vector.
///
/// node0 ends up with a distance of 3 to node4 and a distance of 1 to node2.
/// For either destination, node0 knows a chain of signed edges producing the claimed distance:
///     0--1--2--4
///     0--2
/// However, it is not possible to construct a tree containing both of these chains.
///
/// We handle this by allowing the node to construct a spanning tree which achieves all of its
/// claimed distances _or better_. In this case, 0--2--4 is valid.
///
/// The situation arises as a result of inconsistent states of node1 and node2:
///     - node1 is telling us that node2 has a connection to node4
///     - node2 is telling us that it has no connection to node4
///
/// It is not the responsibility of node0 to decide who is right; perhaps the connection was lost
/// and node1 hasn't realized it yet, or perhaps the connection is newly formed and we haven't received
/// an update from node2 yet (note that direct latency for 0--2 may be worse than the latency 0--1--2).
///
/// Instead, node0 trusts its peers to have the exact distances which they claim, and does not try to
/// deduce anything from the spanning trees they provide other than verifying the claimed distances.
#[tokio::test]
async fn inconsistent_peers() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();
    let node3 = random_peer_id();
    let node4 = random_peer_id();

    let graph =
        Arc::new(GraphV2::new(GraphConfigV2 { node_id: node0.clone(), prune_edges_after: None }));

    let edge01 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge02 = Edge::make_fake_edge(node0.clone(), node2.clone(), 123);
    let edge12 = Edge::make_fake_edge(node1.clone(), node2.clone(), 123);
    let edge13 = Edge::make_fake_edge(node1.clone(), node3.clone(), 123);
    let edge24 = Edge::make_fake_edge(node2.clone(), node4.clone(), 123);

    // Receive a DistanceVector from node1 with routes to 2, 3, 4 behind it
    //    0 -- 1 -- 3
    //          \
    //           2 -- 4
    graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node1.clone(),
                distances: vec![
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node3.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node4.clone(), distance: 2 },
                ],
                edges: vec![edge01.clone(), edge12.clone(), edge13.clone(), edge24.clone()],
            },
        ))
        .await;

    // Receive a DistanceVector from node2 with routes to 1, 3 behind it
    //           1 -- 3
    //          /
    //    0 -- 2
    //
    // Notably, node2 does not advertise a route to node 4
    let distance_vector_update = graph
        .process_network_event(NetworkTopologyChange::PeerAdvertisedDistances(
            network_protocol::DistanceVector {
                root: node2.clone(),
                distances: vec![
                    AdvertisedPeerDistance { destination: node2.clone(), distance: 0 },
                    AdvertisedPeerDistance { destination: node0.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node1.clone(), distance: 1 },
                    AdvertisedPeerDistance { destination: node3.clone(), distance: 2 },
                ],
                edges: vec![edge02.clone(), edge12.clone(), edge13.clone()],
            },
        ))
        .await
        .unwrap();

    // Best available advertised route to each destination
    let expected_routes = HashMap::from([
        (node0.clone(), 0),
        (node1.clone(), 1),
        (node2.clone(), 1),
        (node3.clone(), 2),
        (node4.clone(), 3),
    ]);

    // There is no set of edges which produces a tree exactly consistent with `expected_routes`,
    // but we should be able to construct a valid DistanceVector anyway
    graph.verify_own_distance_vector(expected_routes, &distance_vector_update);
}
