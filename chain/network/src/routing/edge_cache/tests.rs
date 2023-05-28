use crate::routing::edge_cache::*;
use crate::test_utils::random_peer_id;
use crate::testonly::make_rng;
use rand::Rng;
use std::collections::HashSet;

#[test]
fn test_has_edge_nonce_or_newer() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node0.clone(), node1.clone(), 456);

    let mut ec = EdgeCache::new();

    // Initially empty
    assert!(!ec.has_edge_nonce_or_newer(&edge0));
    assert!(!ec.has_edge_nonce_or_newer(&edge1));

    // Write the older nonce
    ec.write_verified_nonce(&edge0);
    assert!(ec.has_edge_nonce_or_newer(&edge0));
    assert!(!ec.has_edge_nonce_or_newer(&edge1));

    // Write the newer nonce
    ec.write_verified_nonce(&edge1);
    assert!(ec.has_edge_nonce_or_newer(&edge0));
    assert!(ec.has_edge_nonce_or_newer(&edge1));
}

#[test]
fn test_update_active_edge_nonce() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node0.clone(), node1.clone(), 456);

    assert_eq!(edge0.key(), edge1.key());
    let key = edge0.key();

    let mut ec = EdgeCache::new();

    // First insert with the older nonce
    ec.insert_active_edge(&edge0);
    assert_eq!(Some(123), ec.get_nonce_for_active_edge(key));

    // Insert another copy of the same edge with a newer nonce
    ec.insert_active_edge(&edge1);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(key));

    // Insert with the older nonce again; should not overwrite
    ec.insert_active_edge(&edge0);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(key));

    // Remove a copy; should still remember it
    ec.remove_active_edge(key);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(key));

    // Remove another copy; should still remember it
    ec.remove_active_edge(key);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(key));

    // Remove final copy
    ec.remove_active_edge(key);
    assert_eq!(None, ec.get_nonce_for_active_edge(key));
}

#[test]
fn test_p2id_mapping() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();
    let node3 = random_peer_id();

    // Set up a simple line graph 0--1--2--3
    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 456);
    let edge2 = Edge::make_fake_edge(node2.clone(), node3.clone(), 789);

    let mut ec = EdgeCache::new();

    // Insert and remove a single edge
    ec.insert_active_edge(&edge0);
    ec.check_mapping(vec![node0.clone(), node1.clone()]);
    ec.remove_active_edge(&edge0.key());
    ec.check_mapping(vec![]);

    // Insert all edges
    ec.insert_active_edge(&edge0);
    ec.insert_active_edge(&edge1);
    ec.insert_active_edge(&edge2);
    ec.check_mapping(vec![node0.clone(), node1.clone(), node2.clone(), node3.clone()]);

    // Remove edge1; all nodes still active
    ec.remove_active_edge(edge1.key());
    ec.check_mapping(vec![node0.clone(), node1.clone(), node2.clone(), node3.clone()]);

    // Remove edge0; node0 and node1 will no longer be active
    ec.remove_active_edge(edge0.key());
    ec.check_mapping(vec![node2.clone(), node3.clone()]);

    // Insert edge1; reactivates only node1
    ec.insert_active_edge(&edge1);
    ec.check_mapping(vec![node1.clone(), node2.clone(), node3.clone()]);

    // Remove edge2; deactivates only node2
    ec.remove_active_edge(edge2.key());
    ec.check_mapping(vec![node1.clone(), node2.clone()]);

    // Remove edge1; no nodes active
    ec.remove_active_edge(edge1.key());
    ec.check_mapping(vec![]);
}

#[test]
fn test_reuse_ids() {
    let max_node_ct = 5;

    let mut rng = make_rng(921853233);
    let rng = &mut rng;

    let mut ec = EdgeCache::new();

    // Run multiple iterations of inserting and deleting sets of edges to the same cache
    for _ in 0..25 {
        // Generate some random PeerIds; should have at least 2 so we can make some edges
        let node_ct = rng.gen::<usize>() % (max_node_ct - 2) + 2;
        let peer_ids: Vec<PeerId> = (0..node_ct).map(|_| random_peer_id()).collect();

        // Generate some random edges
        let edge_ct = rng.gen::<usize>() % 10 + 0;
        let edges: Vec<Edge> = (0..edge_ct)
            .map(|_| {
                // Generate two distinct indices at random in (0..node_ct)
                let peer0 = rng.gen::<usize>() % (node_ct - 1);
                let peer1 = peer0 + 1 + (rng.gen::<usize>() % (node_ct - peer0 - 1));

                // Make an edge with the chosen nodes and a random nonce
                Edge::make_fake_edge(
                    peer_ids[peer0].clone(),
                    peer_ids[peer1].clone(),
                    rng.gen::<u64>(),
                )
            })
            .collect();

        let mut active = HashSet::<PeerId>::new();
        for e in &edges {
            // Insert the edge to the EdgeCache
            ec.insert_active_edge(e);

            // Update our record of active nodes
            let (peer0, peer1) = e.key().clone();
            active.insert(peer0);
            active.insert(peer1);

            ec.check_mapping(Vec::from_iter(active.clone()));

            // u32 ids should be reused across iterations
            assert!(ec.max_id() <= max_node_ct);
        }

        for e in &edges {
            ec.remove_active_edge(e.key());
        }
    }
}
