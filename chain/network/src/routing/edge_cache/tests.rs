use crate::routing::edge_cache::*;
use crate::test_utils::random_peer_id;
use crate::testonly::make_rng;
use rand::Rng;
use std::collections::HashSet;

#[test]
fn has_edge_nonce_or_newer() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node0.clone(), node1, 456);

    let mut ec = EdgeCache::new(node0);

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
fn update_active_edge_nonce() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();

    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node0.clone(), node1, 456);

    assert_eq!(edge0.key(), edge1.key());
    let key: EdgeKey = edge0.key().into();

    let mut ec = EdgeCache::new(node0);

    // First insert with the older nonce
    ec.insert_active_edge(&edge0);
    assert_eq!(Some(123), ec.get_nonce_for_active_edge(&key));

    // Insert another copy of the same edge with a newer nonce
    ec.insert_active_edge(&edge1);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(&key));

    // Insert with the older nonce again; should not overwrite
    ec.insert_active_edge(&edge0);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(&key));

    // Remove a copy; should still remember it
    ec.remove_active_edge(&key);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(&key));

    // Remove another copy; should still remember it
    ec.remove_active_edge(&key);
    assert_eq!(Some(456), ec.get_nonce_for_active_edge(&key));

    // Remove final copy
    ec.remove_active_edge(&key);
    assert_eq!(None, ec.get_nonce_for_active_edge(&key));
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

    // Set up the EdgeCache with node0 as the local node
    let mut ec = EdgeCache::new(node0.clone());
    ec.check_mapping(vec![node0.clone()]);

    // Insert and remove a single edge
    ec.insert_active_edge(&edge0);
    ec.check_mapping(vec![node0.clone(), node1.clone()]);
    ec.remove_active_edge(&edge0.key().into());
    ec.check_mapping(vec![node0.clone()]);

    // Insert all edges (0--1--2--3)
    ec.insert_active_edge(&edge0);
    ec.insert_active_edge(&edge1);
    ec.insert_active_edge(&edge2);
    ec.check_mapping(vec![node0.clone(), node1.clone(), node2.clone(), node3.clone()]);

    // Remove edge1; all nodes are still active (0--1  2--3)
    ec.remove_active_edge(&edge1.key().into());
    ec.check_mapping(vec![node0.clone(), node1.clone(), node2.clone(), node3.clone()]);

    // Remove edge0; node1 will no longer be active (0  1  2--3)
    ec.remove_active_edge(&edge0.key().into());
    ec.check_mapping(vec![node0.clone(), node2.clone(), node3.clone()]);

    // Insert edge1; reactivates node1 (0  1--2--3)
    ec.insert_active_edge(&edge1);
    ec.check_mapping(vec![node0.clone(), node1.clone(), node2.clone(), node3]);

    // Remove edge2; deactivates only node2 (0  1--2  3)
    ec.remove_active_edge(&edge2.key().into());
    ec.check_mapping(vec![node0.clone(), node1, node2]);

    // Remove edge1; only the local node should remain mapped (0  1  2  3)
    ec.remove_active_edge(&edge1.key().into());
    ec.check_mapping(vec![node0]);
}

#[test]
fn reuse_ids() {
    let max_node_ct = 5;

    let mut rng = make_rng(921853233);
    let rng = &mut rng;

    let local_node_id = random_peer_id();
    let mut ec = EdgeCache::new(local_node_id.clone());

    // Run multiple iterations of inserting and deleting sets of edges
    for _ in 0..25 {
        // Generate some random PeerIds; should have at least 2 so we can make some edges
        let node_ct = rng.gen::<usize>() % (max_node_ct - 2) + 2;
        let mut peer_ids: Vec<PeerId> = (1..node_ct).map(|_| random_peer_id()).collect();
        peer_ids.push(local_node_id.clone());

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

        let mut active = HashSet::<PeerId>::from([local_node_id.clone()]);
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
            ec.remove_active_edge(&e.key().into());
        }
    }
}

#[test]
fn free_unused_after_create_for_tree() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let edge = Edge::make_fake_edge(node1.clone(), node2.clone(), 123);

    // Initialize the edge cache and check that just the local node has an id
    let mut ec = EdgeCache::new(node0.clone());
    ec.check_mapping(vec![node0.clone()]);

    // Create and check ids for the tree 1--2
    ec.create_ids_for_tree(&node1, &vec![edge]);
    ec.check_mapping_external(&vec![node0.clone(), node1, node2]);

    // Free unused ids
    ec.free_unused_ids();
    ec.check_mapping(vec![node0]);
}

#[test]
fn overwrite_shortest_path_tree() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();

    let mut ec = EdgeCache::new(node0.clone());

    let edge0 = Edge::make_fake_edge(node0, node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 123);

    // Write an SPT for node1 advertising node2 behind it; 0--1--2
    ec.update_tree(&node1, &vec![edge0.clone(), edge1.clone()]);

    assert!(ec.is_active(&edge1));
    assert!(ec.p2id.contains_key(&node2));

    // Now write an SPT for node1 without the connection to node2; 0--1  2
    ec.update_tree(&node1, &vec![edge0]);

    // edge1 should have been pruned from node0's `active_edges` map
    assert!(!ec.is_active(&edge1));
    // node2 should have been pruned from node0's `p2id` mapping
    assert!(!ec.p2id.contains_key(&node2));
}

fn assert_eq_unordered(a: Vec<Edge>, b: Vec<Edge>) {
    for x in &a {
        assert!(b.contains(x));
    }
    for x in &b {
        assert!(a.contains(x));
    }
}

#[test]
fn test_construct_shortest_path_tree() {
    let node0 = random_peer_id();
    let node1 = random_peer_id();
    let node2 = random_peer_id();
    let node3 = random_peer_id();

    // Set up a simple line graph 0--1--2--3
    let edge0 = Edge::make_fake_edge(node0.clone(), node1.clone(), 123);
    let edge1 = Edge::make_fake_edge(node1.clone(), node2.clone(), 456);
    let edge2 = Edge::make_fake_edge(node2.clone(), node3.clone(), 789);

    // Set up the EdgeCache with node0 as the local node
    let mut ec = EdgeCache::new(node0.clone());
    ec.check_mapping(vec![node0.clone()]);

    // Insert the edges to the cache
    ec.insert_active_edge(&edge0);
    ec.insert_active_edge(&edge1);
    ec.insert_active_edge(&edge2);

    // Construct tree 0--1--2--3
    assert_eq_unordered(
        ec.construct_spanning_tree(&HashMap::from([
            (node0.clone(), 0),
            (node1.clone(), 1),
            (node2.clone(), 2),
            (node3.clone(), 3),
        ]))
        .unwrap(),
        vec![edge0.clone(), edge1.clone(), edge2.clone()],
    );

    // Add direct edges to node2 and node3
    let edge02 = Edge::make_fake_edge(node0.clone(), node2.clone(), 123);
    let edge03 = Edge::make_fake_edge(node0.clone(), node3.clone(), 456);

    ec.insert_active_edge(&edge02);
    ec.insert_active_edge(&edge03);

    // Construct tree 0--{1,2,3}
    assert_eq_unordered(
        ec.construct_spanning_tree(&HashMap::from([
            (node0, 0),
            (node1, 1),
            (node2, 1),
            (node3, 1),
        ]))
        .unwrap(),
        vec![edge0, edge02, edge03],
    );
}
