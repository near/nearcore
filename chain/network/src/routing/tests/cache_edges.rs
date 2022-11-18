use crate::network_protocol::testonly as data;
use crate::network_protocol::Edge;
//use crate::network_protocol::EDGE_MIN_TIMESTAMP_NONCE;
use crate::routing;
use crate::store;
use crate::store::testonly::Component;
use crate::testonly::make_rng;
use crate::time;
use near_crypto::Signature;
use near_primitives::network::PeerId;
use std::collections::{HashMap,HashSet};
use std::sync::Arc;

fn edge(p0: &PeerId, p1: &PeerId, nonce: u64) -> Edge {
    Edge::new(p0.clone(), p1.clone(), nonce, Signature::default(), Signature::default())
}

struct RoutingTableTest {
    me: PeerId,
    rng: crate::testonly::Rng,
    db: Arc<dyn near_store::db::Database>,
}

impl RoutingTableTest {
    // Gets a new random PeerId.
    pub fn make_peer(&mut self) -> PeerId {
        data::make_peer_id(&mut self.rng)
    }

    fn new() -> Self {
        let mut rng = make_rng(87927345);
        let me = data::make_peer_id(&mut rng);
        let db = near_store::db::TestDB::new();
        Self { me, rng, db }
    }

    fn new_graph(&self) -> routing::GraphWithCache {
        routing::GraphWithCache::new(routing::GraphConfig{
            node_id: self.me.clone(),
            prune_unreachable_peers_after: time::Duration::seconds(3),
            prune_edges_after: None,
        }, store::Store::from(self.db.clone()))
    }

    fn check(&mut self, g: &routing::GraphWithCache, want_mem: &[Edge], want_db: &[Component]) {
        let store = store::Store::from(self.db.clone());
        let got_mem = g.load();
        let got_mem : HashMap<_,_> = got_mem.iter().collect();
        let mut want_mem_map = HashMap::new();
        for e in want_mem {
            if want_mem_map.insert(e.key(), e).is_some() {
                panic!("want_mem: multiple entries for {:?}", e.key());
            }
        }
        assert_eq!(got_mem, want_mem_map);

        let got_db: HashSet<_> = store.list_components().into_iter().map(|c| c.normal()).collect();
        let want_db: HashSet<_> = want_db.iter().map(|c| c.clone().normal()).collect();
        assert_eq!(got_db, want_db);
    }
}

#[test]
fn empty() {
    let mut test = RoutingTableTest::new();
    let g = test.new_graph();
    test.check(&g, &[], &[]);
}

const SEC: time::Duration = time::Duration::seconds(1);

#[tokio::test]
async fn one_edge() {
    let clock = time::FakeClock::default();
    let mut test = RoutingTableTest::new();
    let g = test.new_graph();
    let p1 = test.make_peer();
    let e1 = edge(&test.me, &p1, 1);
    let e1v2 = edge(&test.me, &p1, 2);

    // Add an active edge.
    // Update RT with pruning. NOOP, since p1 is reachable.
    g.update_routing_table(&clock.clock(), vec![e1.clone()]).await;
    test.check(&g, &[e1.clone()], &[]);

    // Override with an inactive edge.
    g.update_routing_table(&clock.clock(), vec![e1v2.clone()]).await;
    test.check(&g, &[e1v2.clone()], &[]);

    // After 2s, update RT with pruning unreachable for 3s.
    // NOOP, since p1 is unreachable for 2s.
    clock.advance(2 * SEC);
    g.update_routing_table(&clock.clock(), vec![]).await;
    test.check(&g, &[e1v2.clone()], &[]);

    // Update RT with pruning unreachable for 1s. p1 should be moved to DB.
    clock.advance(2 * SEC);
    g.update_routing_table(&clock.clock(), vec![]).await;
    test.check(&g, &[], &[Component { edges: vec![e1v2.clone()], peers: vec![p1.clone()] }]);
}

#[tokio::test]
async fn load_component() {
    let clock = time::FakeClock::default();
    let mut test = RoutingTableTest::new();
    let g = test.new_graph();
    let p1 = test.make_peer();
    let p2 = test.make_peer();
    let e1 = edge(&test.me, &p1, 2);
    let e2 = edge(&test.me, &p2, 2);
    let e3 = edge(&p1, &p2, 1);
    let e1v2 = edge(&test.me, &p1, 3);

    // There is an active edge between p1,p2, but neither is reachable from me().
    // They should be pruned.
    g.update_routing_table(&clock.clock(), vec![e1.clone(), e2.clone(), e3.clone()]).await;
    test.check(
        &g,
        &[],
        &[Component {
            edges: vec![e1.clone(), e2.clone(), e3.clone()],
            peers: vec![p1.clone(), p2.clone()],
        }],
    );

    // Add an active edge from me() to p1. This should trigger loading the whole component from DB.
    g.update_routing_table(&clock.clock(), vec![e1v2.clone()]).await;
    test.check(&g, &[e1v2, e2, e3], &[]);
}

#[tokio::test]
async fn components_nonces_are_tracked_in_storage() {
    let clock = time::FakeClock::default();
    let mut test = RoutingTableTest::new();
    let g = test.new_graph();
    
    // Add an inactive edge and prune it.
    let p1 = test.make_peer();
    let e1 = edge(&test.me, &p1, 2);
    g.update_routing_table(&clock.clock(), vec![e1.clone()]).await;
    test.check(&g, &[], &[Component { edges: vec![e1.clone()], peers: vec![p1.clone()] }]);

    // Add an active unreachable edge, which also should get pruned.
    let p2 = test.make_peer();
    let p3 = test.make_peer();
    let e23 = edge(&p2, &p3, 2);
    g.update_routing_table(&clock.clock(), vec![e23.clone()]).await;
    test.check(
        &g,
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component { edges: vec![e23.clone()], peers: vec![p2.clone(), p3.clone()] },
        ],
    );

    // Spawn a new graph with the same storage.
    // Add another inactive edge and prune it. The previously created component shouldn't get
    // overwritten, but rather a new one should be created.
    // This verifies that the last_component_nonce (which indicates which component IDs have been
    // already utilized) is persistently stored in DB.
    let g = test.new_graph();
    let p4 = test.make_peer();
    let e4 = edge(&test.me, &p4, 2);
    g.update_routing_table(&clock.clock(), vec![e4.clone()]).await;
    test.check(
        &g,
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component { edges: vec![e23.clone()], peers: vec![p2.clone(), p3.clone()] },
            Component { edges: vec![e4.clone()], peers: vec![p4.clone()] },
        ],
    );

    // Add an active edge between unreachable nodes, which will merge 2 components in DB.
    let e34 = edge(&p3, &p4, 1);
    g.update_routing_table(&clock.clock(), vec![e34.clone()]).await;
    test.check(
        &g,
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component {
                edges: vec![e4.clone(), e23.clone(), e34.clone()],
                peers: vec![p2.clone(), p3.clone(), p4.clone()],
            },
        ],
    );
}

/*fn to_active_nonce(value: u64) -> u64 {
    if value % 2 == 1 {
        return value;
    }
    return value + 1;
}*/

/*
#[test]
fn expired_edges() {
    let clock = time::FakeClock::default();
    clock.set_utc(*EDGE_MIN_TIMESTAMP_NONCE + time::Duration::days(2));
    let mut test = RoutingTableTest::new();
    let mut g = test.new_graph();
    let p1 = test.make_peer();
    let p2 = test.make_peer();
    let current_odd_nonce = to_active_nonce(clock.now_utc().unix_timestamp() as u64);

    let e1 = edge(&test.me, &p1, current_odd_nonce);

    let old_e2 = edge(&test.me, &p2, current_odd_nonce - 100);
    let still_old_e2 = edge(&test.me, &p2, current_odd_nonce - 90);
    let fresh_e2 = edge(&test.me, &p2, current_odd_nonce);

    // Add an active edge.
    actor.add_verified_edges(vec![e1.clone(), old_e2.clone()]);
    test.check(&[e1.clone(), old_e2.clone()], &[]);

    // Update RT with pruning. e1 should stay - as it is fresh, but old_e2 should be removed.
    actor.update_routing_table(
        Some(test.clock.now()),
        Some(test.clock.now_utc().checked_sub(time::Duration::seconds(10)).unwrap()),
    );
    test.check(&[e1.clone()], &[]);

    // Adding 'still old' edge to e2 should fail (as it is older than the last prune_edges_older_than)
    actor.add_verified_edges(vec![still_old_e2.clone()]);
    test.check(&[e1.clone()], &[]);

    // But adding the fresh edge should work.
    actor.add_verified_edges(vec![fresh_e2.clone()]);
    test.check(&[e1.clone(), fresh_e2.clone()], &[]);

    // Advance 20 seconds:
    test.clock.advance(20 * SEC);

    // Now the edge is 'too old' and should be removed.
    actor.update_routing_table(
        Some(test.clock.now()),
        Some(test.clock.now_utc().checked_sub(time::Duration::seconds(10)).unwrap()),
    );
    test.check(&[], &[]);

    // let's create a removal edge
    let e1v2 =
        edge(&test.me(), &p1, to_active_nonce(test.clock.now_utc().unix_timestamp() as u64) + 1);
    actor.add_verified_edges(vec![e1v2.clone()]);
    test.check(&[e1v2.clone()], &[]);

    actor.update_routing_table(
        None,
        Some(test.clock.now_utc().checked_sub(time::Duration::seconds(10)).unwrap()),
    );
    test.check(&[e1v2.clone()], &[]);

    // And now it should disappear

    // Advance 20 seconds:
    test.clock.advance(20 * SEC);

    // Now the edge is 'too old' and should be removed.
    actor.update_routing_table(
        Some(test.clock.now()),
        Some(test.clock.now_utc().checked_sub(time::Duration::seconds(10)).unwrap()),
    );
    test.check(&[], &[]);
}
*/
