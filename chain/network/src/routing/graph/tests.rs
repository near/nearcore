use super::{Graph, GraphConfig};
use crate::network_protocol::testonly as data;
use crate::network_protocol::Edge;
use crate::network_protocol::EDGE_MIN_TIMESTAMP_NONCE;
use crate::store;
use crate::store::testonly::Component;
use crate::testonly::make_rng;
use crate::time;
use near_crypto::Signature;
use near_primitives::network::PeerId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl Graph {
    async fn check(&self, want_mem: &[Edge], want_db: &[Component]) {
        let got_mem = self.load();
        let got_mem: HashMap<_, _> = got_mem.edges.iter().collect();
        let mut want_mem_map = HashMap::new();
        for e in want_mem {
            if want_mem_map.insert(e.key(), e).is_some() {
                panic!("want_mem: multiple entries for {:?}", e.key());
            }
        }
        assert_eq!(got_mem, want_mem_map);

        let got_db: HashSet<_> =
            self.inner.lock().store.list_components().into_iter().map(|c| c.normal()).collect();
        let want_db: HashSet<_> = want_db.iter().map(|c| c.clone().normal()).collect();
        assert_eq!(got_db, want_db);
    }
}

fn edge(p0: &PeerId, p1: &PeerId, nonce: u64) -> Edge {
    Edge::new(p0.clone(), p1.clone(), nonce, Signature::default(), Signature::default())
}

fn store() -> store::Store {
    store::Store::from(near_store::db::TestDB::new())
}

#[tokio::test]
async fn empty() {
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let cfg = GraphConfig {
        node_id: data::make_peer_id(rng),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let g = Graph::new(cfg, store());
    g.check(&[], &[]).await;
}

const SEC: time::Duration = time::Duration::seconds(1);

#[tokio::test]
async fn one_edge() {
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let cfg = GraphConfig {
        node_id: data::make_peer_id(rng),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let g = Arc::new(Graph::new(cfg.clone(), store()));

    let p1 = data::make_peer_id(rng);
    let e1 = edge(&cfg.node_id, &p1, 1);
    let e1v2 = edge(&cfg.node_id, &p1, 2);

    // Add an active edge.
    // Update RT with pruning. NOOP, since p1 is reachable.
    g.update(&clock.clock(), vec![e1.clone()]).await;
    g.check(&[e1.clone()], &[]).await;

    // Override with an inactive edge.
    g.update(&clock.clock(), vec![e1v2.clone()]).await;
    g.check(&[e1v2.clone()], &[]).await;

    // After 2s, update RT with pruning unreachable for 3s.
    // NOOP, since p1 is unreachable for 2s.
    clock.advance(2 * SEC);
    g.update(&clock.clock(), vec![]).await;
    g.check(&[e1v2.clone()], &[]).await;

    // Update RT with pruning unreachable for 1s. p1 should be moved to DB.
    clock.advance(2 * SEC);
    g.update(&clock.clock(), vec![]).await;
    g.check(&[], &[Component { edges: vec![e1v2.clone()], peers: vec![p1.clone()] }]).await;
}

#[tokio::test]
async fn load_component() {
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let cfg = GraphConfig {
        node_id: data::make_peer_id(rng),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let g = Arc::new(Graph::new(cfg.clone(), store()));

    let p1 = data::make_peer_id(rng);
    let p2 = data::make_peer_id(rng);
    let e1 = edge(&cfg.node_id, &p1, 2);
    let e2 = edge(&cfg.node_id, &p2, 2);
    let e3 = edge(&p1, &p2, 1);
    let e1v2 = edge(&cfg.node_id, &p1, 3);

    // There is an active edge between p1,p2, but neither is reachable from me().
    // They should be pruned.
    g.update(&clock.clock(), vec![e1.clone(), e2.clone(), e3.clone()]).await;
    g.check(
        &[],
        &[Component {
            edges: vec![e1.clone(), e2.clone(), e3.clone()],
            peers: vec![p1.clone(), p2.clone()],
        }],
    )
    .await;

    // Add an active edge from me() to p1. This should trigger loading the whole
    // component from DB.
    g.update(&clock.clock(), vec![e1v2.clone()]).await;
    g.check(&[e1v2, e2, e3], &[]).await;
}

#[tokio::test]
async fn components_nonces_are_tracked_in_storage() {
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let cfg = GraphConfig {
        node_id: data::make_peer_id(rng),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let store = store();
    let g = Arc::new(Graph::new(cfg.clone(), store.clone()));

    // Add an inactive edge and prune it.
    let p1 = data::make_peer_id(rng);
    let e1 = edge(&cfg.node_id, &p1, 2);
    g.update(&clock.clock(), vec![e1.clone()]).await;
    g.check(&[], &[Component { edges: vec![e1.clone()], peers: vec![p1.clone()] }]).await;

    // Add an active unreachable edge, which also should get pruned.
    let p2 = data::make_peer_id(rng);
    let p3 = data::make_peer_id(rng);
    let e23 = edge(&p2, &p3, 2);
    g.update(&clock.clock(), vec![e23.clone()]).await;
    g.check(
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component { edges: vec![e23.clone()], peers: vec![p2.clone(), p3.clone()] },
        ],
    )
    .await;

    // Spawn a new graph with the same storage.
    // Add another inactive edge and prune it. The previously created component
    // shouldn't get overwritten, but rather a new one should be created.
    // This verifies that the last_component_nonce (which indicates which component
    // IDs have been already utilized) is persistently stored in DB.
    let g = Arc::new(Graph::new(cfg.clone(), store));
    let p4 = data::make_peer_id(rng);
    let e4 = edge(&cfg.node_id, &p4, 2);
    g.update(&clock.clock(), vec![e4.clone()]).await;
    g.check(
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component { edges: vec![e23.clone()], peers: vec![p2.clone(), p3.clone()] },
            Component { edges: vec![e4.clone()], peers: vec![p4.clone()] },
        ],
    )
    .await;

    // Add an active edge between unreachable nodes, which will merge 2 components
    // in DB.
    let e34 = edge(&p3, &p4, 1);
    g.update(&clock.clock(), vec![e34.clone()]).await;
    g.check(
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component {
                edges: vec![e4.clone(), e23.clone(), e34.clone()],
                peers: vec![p2.clone(), p3.clone(), p4.clone()],
            },
        ],
    )
    .await;
}

fn to_active_nonce(t: time::Utc) -> u64 {
    let value = t.unix_timestamp() as u64;
    if value % 2 == 1 {
        return value;
    }
    return value + 1;
}

#[tokio::test]
async fn expired_edges() {
    let clock = time::FakeClock::default();
    clock.set_utc(*EDGE_MIN_TIMESTAMP_NONCE + time::Duration::days(2));
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let cfg = GraphConfig {
        node_id: data::make_peer_id(rng),
        prune_unreachable_peers_after: time::Duration::hours(100),
        prune_edges_after: Some(110 * SEC),
    };
    let g = Arc::new(Graph::new(cfg.clone(), store()));

    let p1 = data::make_peer_id(rng);
    let p2 = data::make_peer_id(rng);

    let now = clock.now_utc();
    let e1 = edge(&cfg.node_id, &p1, to_active_nonce(now));
    let old_e2 = edge(&cfg.node_id, &p2, to_active_nonce(now - 100 * SEC));
    let still_old_e2 = edge(&cfg.node_id, &p2, to_active_nonce(now - 90 * SEC));
    let fresh_e2 = edge(&cfg.node_id, &p2, to_active_nonce(now));

    // Add an active edge.
    g.update(&clock.clock(), vec![e1.clone(), old_e2.clone()]).await;
    g.check(&[e1.clone(), old_e2.clone()], &[]).await;
    // Update RT with pruning. e1 should stay - as it is fresh, but old_e2 should be
    // removed.
    clock.advance(40 * SEC);
    g.update(&clock.clock(), vec![]).await;
    g.check(&[e1.clone()], &[]).await;

    // Adding 'still old' edge to e2 should fail (as it is older than the last
    // prune_edges_older_than)
    g.update(&clock.clock(), vec![still_old_e2.clone()]).await;
    g.check(&[e1.clone()], &[]).await;

    // But adding the fresh edge should work.
    g.update(&clock.clock(), vec![fresh_e2.clone()]).await;
    g.check(&[e1.clone(), fresh_e2.clone()], &[]).await;

    // Advance so that the edge is 'too old' and should be removed.
    clock.advance(100 * SEC);
    g.update(&clock.clock(), vec![]).await;
    g.check(&[], &[]).await;

    // Let's create a removal edge.
    let e1v2 = edge(&cfg.node_id, &p1, to_active_nonce(clock.now_utc()) + 1);
    g.update(&clock.clock(), vec![e1v2.clone()]).await;
    g.check(&[e1v2.clone()], &[]).await;

    // Advance time a bit. The edge should stay.
    clock.advance(20 * SEC);
    g.update(&clock.clock(), vec![]).await;
    g.check(&[e1v2.clone()], &[]).await;

    // Advance time a lot. The edge should be pruned.
    clock.advance(100 * SEC);
    g.update(&clock.clock(), vec![]).await;
    g.check(&[], &[]).await;
}
