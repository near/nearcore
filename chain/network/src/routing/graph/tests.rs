use super::{Graph, GraphConfig};
use crate::network_protocol::testonly as data;
use crate::network_protocol::Edge;
use crate::network_protocol::EDGE_MIN_TIMESTAMP_NONCE;
use crate::store;
use crate::store::testonly::Component;
use crate::testonly::make_rng;
use near_async::time;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

impl Graph {
    async fn simple_update(self: &Arc<Self>, clock: &time::Clock, edges: Vec<Edge>) {
        assert_eq!(vec![true], self.update(clock, vec![edges]).await.1);
    }

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

fn store() -> store::Store {
    store::Store::from(near_store::db::TestDB::new())
}

fn peer_id(key: &SecretKey) -> PeerId {
    PeerId::new(key.public_key())
}

#[tokio::test]
async fn empty() {
    init_test_logger();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = GraphConfig {
        node_id: peer_id(&node_key),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let g = Graph::new(cfg, store());
    g.check(&[], &[]).await;
}

const SEC: time::Duration = time::Duration::seconds(1);

#[tokio::test]
async fn one_edge() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = GraphConfig {
        node_id: peer_id(&node_key),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let g = Arc::new(Graph::new(cfg.clone(), store()));

    let p1 = data::make_secret_key(rng);
    let e1 = data::make_edge(&node_key, &p1, 1);
    let e1v2 = e1.remove_edge(peer_id(&p1), &p1);

    tracing::info!(target:"test", "Add an active edge. Update RT with pruning.");
    // NOOP, since p1 is reachable.
    g.simple_update(&clock.clock(), vec![e1.clone()]).await;
    g.check(&[e1.clone()], &[]).await;

    tracing::info!(target:"test", "Override with an inactive edge.");
    g.simple_update(&clock.clock(), vec![e1v2.clone()]).await;
    g.check(&[e1v2.clone()], &[]).await;

    tracing::info!(target:"test", "After 2s, simple_update RT with pruning unreachable for 3s.");
    // NOOP, since p1 is unreachable for 2s.
    clock.advance(2 * SEC);
    g.simple_update(&clock.clock(), vec![]).await;
    g.check(&[e1v2.clone()], &[]).await;

    tracing::info!(target:"test", "Update RT with pruning unreachable for 1s.");
    // p1 should be moved to DB.
    clock.advance(2 * SEC);
    g.simple_update(&clock.clock(), vec![]).await;
    g.check(&[], &[Component { edges: vec![e1v2.clone()], peers: vec![peer_id(&p1)] }]).await;
}

#[tokio::test]
async fn load_component() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = GraphConfig {
        node_id: peer_id(&node_key),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let g = Arc::new(Graph::new(cfg.clone(), store()));

    let p1 = data::make_secret_key(rng);
    let p2 = data::make_secret_key(rng);
    let e1 = data::make_edge_tombstone(&node_key, &p1);
    let e2 = data::make_edge_tombstone(&node_key, &p2);
    let e3 = data::make_edge(&p1, &p2, 1);
    let e1v2 = data::make_edge(&node_key, &p1, e1.nonce() + 1);

    // There is an active edge between p1,p2, but neither is reachable from me().
    // They should be pruned.
    g.simple_update(&clock.clock(), vec![e1.clone(), e2.clone(), e3.clone()]).await;
    g.check(
        &[],
        &[Component {
            edges: vec![e1.clone(), e2.clone(), e3.clone()],
            peers: vec![peer_id(&p1), peer_id(&p2)],
        }],
    )
    .await;

    // Add an active edge from me() to p1. This should trigger loading the whole component from DB.
    g.simple_update(&clock.clock(), vec![e1v2.clone()]).await;
    g.check(&[e1v2, e2, e3], &[]).await;
}

#[tokio::test]
async fn components_nonces_are_tracked_in_storage() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = GraphConfig {
        node_id: peer_id(&node_key),
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
    };
    let store = store();
    let g = Arc::new(Graph::new(cfg.clone(), store.clone()));

    tracing::info!(target:"test", "Add an inactive edge and prune it.");
    let p1 = data::make_secret_key(rng);
    let e1 = data::make_edge_tombstone(&node_key, &p1);
    g.simple_update(&clock.clock(), vec![e1.clone()]).await;
    g.check(&[], &[Component { edges: vec![e1.clone()], peers: vec![peer_id(&p1)] }]).await;

    tracing::info!(target:"test", "Add an active unreachable edge, which also should get pruned.");
    let p2 = data::make_secret_key(rng);
    let p3 = data::make_secret_key(rng);
    let e23 = data::make_edge(&p2, &p3, 3);
    g.simple_update(&clock.clock(), vec![e23.clone()]).await;
    g.check(
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![peer_id(&p1)] },
            Component { edges: vec![e23.clone()], peers: vec![peer_id(&p2), peer_id(&p3)] },
        ],
    )
    .await;

    // Spawn a new graph with the same storage.
    // Add another inactive edge and prune it. The previously created component shouldn't get
    // overwritten, but rather a new one should be created.
    // This verifies that the last_component_nonce (which indicates which component IDs have been
    // already utilized) is persistently stored in DB.
    let g = Arc::new(Graph::new(cfg.clone(), store));
    let p4 = data::make_secret_key(rng);
    let e4 = data::make_edge_tombstone(&node_key, &p4);
    g.simple_update(&clock.clock(), vec![e4.clone()]).await;
    g.check(
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![peer_id(&p1)] },
            Component { edges: vec![e23.clone()], peers: vec![peer_id(&p2), peer_id(&p3)] },
            Component { edges: vec![e4.clone()], peers: vec![peer_id(&p4)] },
        ],
    )
    .await;

    // Add an active edge between unreachable nodes, which will merge 2 components in DB.
    let e34 = data::make_edge(&p3, &p4, 1);
    g.simple_update(&clock.clock(), vec![e34.clone()]).await;
    g.check(
        &[],
        &[
            Component { edges: vec![e1.clone()], peers: vec![peer_id(&p1)] },
            Component {
                edges: vec![e4.clone(), e23.clone(), e34.clone()],
                peers: vec![peer_id(&p2), peer_id(&p3), peer_id(&p4)],
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
    init_test_logger();
    let clock = time::FakeClock::default();
    clock.set_utc(*EDGE_MIN_TIMESTAMP_NONCE + time::Duration::days(2));
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = GraphConfig {
        node_id: peer_id(&node_key),
        prune_unreachable_peers_after: time::Duration::hours(100),
        prune_edges_after: Some(110 * SEC),
    };
    let g = Arc::new(Graph::new(cfg.clone(), store()));

    let p1 = data::make_secret_key(rng);
    let p2 = data::make_secret_key(rng);

    let now = clock.now_utc();
    let e1 = data::make_edge(&node_key, &p1, to_active_nonce(now));
    let old_e2 = data::make_edge(&node_key, &p2, to_active_nonce(now - 100 * SEC));
    let still_old_e2 = data::make_edge(&node_key, &p2, to_active_nonce(now - 90 * SEC));
    let fresh_e2 = data::make_edge(&node_key, &p2, to_active_nonce(now));

    tracing::info!(target:"test", "Add an active edge.");
    g.simple_update(&clock.clock(), vec![e1.clone(), old_e2.clone()]).await;
    g.check(&[e1.clone(), old_e2.clone()], &[]).await;
    tracing::info!(target:"test", "Update RT with pruning.");
    // e1 should stay - as it is fresh, but old_e2 should be removed.
    clock.advance(40 * SEC);
    g.simple_update(&clock.clock(), vec![]).await;
    g.check(&[e1.clone()], &[]).await;

    tracing::info!(target:"test", "Adding 'still old' edge to e2 should fail.");
    // (as it is older than the last prune_edges_older_than)
    g.simple_update(&clock.clock(), vec![still_old_e2.clone()]).await;
    g.check(&[e1.clone()], &[]).await;

    tracing::info!(target:"test", "But adding the fresh edge should work.");
    g.simple_update(&clock.clock(), vec![fresh_e2.clone()]).await;
    g.check(&[e1.clone(), fresh_e2.clone()], &[]).await;

    tracing::info!(target:"test", "Advance so that the edge is 'too old' and should be removed.");
    clock.advance(100 * SEC);
    g.simple_update(&clock.clock(), vec![]).await;
    g.check(&[], &[]).await;

    tracing::info!(target:"test", "Let's create a removal edge.");
    let e1v2 = data::make_edge(&node_key, &p1, to_active_nonce(clock.now_utc()))
        .remove_edge(peer_id(&p1), &p1);
    g.simple_update(&clock.clock(), vec![e1v2.clone()]).await;
    g.check(&[e1v2.clone()], &[]).await;

    // Advance time a bit. The edge should stay.
    clock.advance(20 * SEC);
    g.simple_update(&clock.clock(), vec![]).await;
    g.check(&[e1v2.clone()], &[]).await;

    // Advance time a lot. The edge should be pruned.
    clock.advance(100 * SEC);
    g.simple_update(&clock.clock(), vec![]).await;
    g.check(&[], &[]).await;
}
