use super::{Graph, GraphConfig};
use crate::config::{
    DEFAULT_ROUTING_GRAPH_MAX_EDGES, DEFAULT_ROUTING_GRAPH_MAX_EDGES_PER_SOURCE,
    DEFAULT_ROUTING_GRAPH_MAX_PEERS,
};
use crate::network_protocol::Edge;
use crate::network_protocol::testonly as data;
use crate::peer_manager::network_state::EdgesWithSource;
use crate::testonly::make_rng;
use near_async::time;
use near_crypto::SecretKey;
use near_o11y::testonly::init_test_logger;
use near_primitives::network::PeerId;
use std::collections::HashMap;
use std::sync::Arc;

/// Default test config with generous limits so existing tests aren't affected.
fn test_graph_config(node_id: PeerId) -> GraphConfig {
    GraphConfig {
        node_id,
        prune_unreachable_peers_after: time::Duration::seconds(3),
        prune_edges_after: None,
        max_edges_per_source: DEFAULT_ROUTING_GRAPH_MAX_EDGES_PER_SOURCE,
        max_total_edges: DEFAULT_ROUTING_GRAPH_MAX_EDGES,
        max_graph_peers: DEFAULT_ROUTING_GRAPH_MAX_PEERS,
    }
}

impl Graph {
    async fn simple_update(self: &Arc<Self>, edges: Vec<Edge>) {
        let edges = EdgesWithSource::Local(edges);
        assert_eq!(vec![true], self.update(vec![edges]).await.1);
    }

    async fn remote_update(self: &Arc<Self>, source: PeerId, edges: Vec<Edge>) -> Vec<bool> {
        let edges = EdgesWithSource::Remote { edges, source };
        self.update(vec![edges]).await.1
    }

    fn check(&self, want_mem: &[Edge]) {
        let got_mem = self.load();
        let got_mem: HashMap<_, _> = got_mem.edges.iter().collect();
        let mut want_mem_map = HashMap::new();
        for e in want_mem {
            if want_mem_map.insert(e.key(), e).is_some() {
                panic!("want_mem: multiple entries for {:?}", e.key());
            }
        }
        assert_eq!(got_mem, want_mem_map);
    }
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
    let cfg = test_graph_config(peer_id(&node_key));
    let clock = time::FakeClock::default();
    let g = Graph::new(clock.clock(), cfg);
    g.check(&[]);
}

const SEC: time::Duration = time::Duration::seconds(1);

#[tokio::test]
async fn one_edge() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = test_graph_config(peer_id(&node_key));
    let g = Arc::new(Graph::new(clock.clock(), cfg.clone()));

    let p1 = data::make_secret_key(rng);
    let e1 = data::make_edge(&node_key, &p1, 1);
    let e1v2 = e1.remove_edge(peer_id(&p1), &p1);

    tracing::info!(target:"test", "add an active edge, update rt with pruning");
    // NOOP, since p1 is reachable.
    g.simple_update(vec![e1.clone()]).await;
    g.check(&[e1.clone()]);

    tracing::info!(target:"test", "override with an inactive edge");
    g.simple_update(vec![e1v2.clone()]).await;
    g.check(&[e1v2.clone()]);

    tracing::info!(target:"test", "after 2s, simple_update rt with pruning unreachable for 3s");
    // NOOP, since p1 is unreachable for 2s.
    clock.advance(2 * SEC);
    g.simple_update(vec![]).await;
    g.check(&[e1v2.clone()]);

    tracing::info!(target:"test", "update rt with pruning unreachable for 1s");
    // p1 should be moved to DB.
    clock.advance(2 * SEC);
    g.simple_update(vec![]).await;
    g.check(&[]);
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
    clock.set_utc(time::Utc::UNIX_EPOCH + time::Duration::days(2));
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = GraphConfig {
        prune_unreachable_peers_after: time::Duration::hours(100),
        prune_edges_after: Some(110 * SEC),
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg.clone()));

    let p1 = data::make_secret_key(rng);
    let p2 = data::make_secret_key(rng);

    let now = clock.now_utc();
    let e1 = data::make_edge(&node_key, &p1, to_active_nonce(now));
    let old_e2 = data::make_edge(&node_key, &p2, to_active_nonce(now - 100 * SEC));
    let still_old_e2 = data::make_edge(&node_key, &p2, to_active_nonce(now - 90 * SEC));
    let fresh_e2 = data::make_edge(&node_key, &p2, to_active_nonce(now));

    tracing::info!(target:"test", "add an active edge");
    g.simple_update(vec![e1.clone(), old_e2.clone()]).await;
    g.check(&[e1.clone(), old_e2.clone()]);
    tracing::info!(target:"test", "update rt with pruning");
    // e1 should stay - as it is fresh, but old_e2 should be removed.
    clock.advance(40 * SEC);
    g.simple_update(vec![]).await;
    g.check(&[e1.clone()]);

    tracing::info!(target:"test", "adding 'still old' edge to e2 should fail");
    // (as it is older than the last prune_edges_older_than)
    g.simple_update(vec![still_old_e2.clone()]).await;
    g.check(&[e1.clone()]);

    tracing::info!(target:"test", "but adding the fresh edge should work");
    g.simple_update(vec![fresh_e2.clone()]).await;
    g.check(&[e1.clone(), fresh_e2.clone()]);

    tracing::info!(target:"test", "advance so that the edge is 'too old' and should be removed");
    clock.advance(100 * SEC);
    g.simple_update(vec![]).await;
    g.check(&[]);

    tracing::info!(target:"test", "let's create a removal edge");
    let e1v2 = data::make_edge(&node_key, &p1, to_active_nonce(clock.now_utc()))
        .remove_edge(peer_id(&p1), &p1);
    g.simple_update(vec![e1v2.clone()]).await;
    g.check(&[e1v2.clone()]);

    // Advance time a bit. The edge should stay.
    clock.advance(20 * SEC);
    g.simple_update(vec![]).await;
    g.check(&[e1v2.clone()]);

    // Advance time a lot. The edge should be pruned.
    clock.advance(100 * SEC);
    g.simple_update(vec![]).await;
    g.check(&[]);
}

// ======================== Routing graph limit tests ========================
//
// All limit tests create edges adjacent to `node_key` so the introduced peers
// are reachable from the graph source and won't be pruned by the
// prune_unreachable_peers pass that runs after every graph update.
//
// Invariant: limit drops are non-punitive — `ok` (the bool returned by
// update()) must be `true` even when edges are dropped by caps. Only
// signature/self-loop failures set `ok = false`.

#[tokio::test]
async fn source_cap_enforced() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let max_per_source = 5;
    let cfg = GraphConfig {
        max_edges_per_source: max_per_source,
        max_total_edges: 1_000,
        max_graph_peers: 1_000,
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    let source_key = data::make_secret_key(rng);
    let source = peer_id(&source_key);

    // Send more edges than the per-source cap. Edges connect to node_key
    // so they are reachable and won't be pruned.
    let edges: Vec<Edge> = (0..max_per_source + 5)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, 1)
        })
        .collect();
    g.remote_update(source, edges).await;

    let snapshot = g.load();
    assert_eq!(
        snapshot.edges.len(),
        max_per_source,
        "expected exactly {} edges (per-source cap), got {}",
        max_per_source,
        snapshot.edges.len()
    );
}

#[tokio::test]
async fn source_cap_allows_updates() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let max_per_source = 3;
    let cfg = GraphConfig {
        max_edges_per_source: max_per_source,
        max_total_edges: 1_000,
        max_graph_peers: 1_000,
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    let source_key = data::make_secret_key(rng);
    let source = peer_id(&source_key);

    // Create edge keys that we'll update later.
    let k1 = data::make_secret_key(rng);
    let e1 = data::make_edge(&node_key, &k1, 1);

    let k2 = data::make_secret_key(rng);
    let e2 = data::make_edge(&node_key, &k2, 1);

    let k3 = data::make_secret_key(rng);
    let e3 = data::make_edge(&node_key, &k3, 1);

    // Fill source to cap.
    g.remote_update(source.clone(), vec![e1.clone(), e2.clone(), e3.clone()]).await;
    let snapshot = g.load();
    assert_eq!(snapshot.edges.len(), 3);

    // Update existing edge (higher nonce) — should succeed even though source is at cap.
    let e1_updated = data::make_edge(&node_key, &k1, 3);
    g.remote_update(source, vec![e1_updated.clone()]).await;
    let snapshot = g.load();
    // Edge count stays the same (update, not new key).
    assert_eq!(snapshot.edges.len(), 3);
    // Verify the edge was actually updated.
    assert_eq!(snapshot.edges.get(e1.key()).unwrap().nonce(), 3);
}

#[tokio::test]
async fn source_cap_accumulates_across_messages() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let max_per_source = 5;
    let cfg = GraphConfig {
        max_edges_per_source: max_per_source,
        max_total_edges: 1_000,
        max_graph_peers: 1_000,
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    let source_key = data::make_secret_key(rng);
    let source = peer_id(&source_key);

    // First message: send 3 edges (under cap).
    let edges1: Vec<Edge> = (0..3)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, 1)
        })
        .collect();
    g.remote_update(source.clone(), edges1).await;
    assert_eq!(g.load().edges.len(), 3, "first batch should be fully accepted");

    // Second message: send 3 more edges from the same source (total would be 6 > cap=5).
    let edges2: Vec<Edge> = (0..3)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, 1)
        })
        .collect();
    g.remote_update(source, edges2).await;
    assert_eq!(
        g.load().edges.len(),
        max_per_source,
        "source cap should accumulate across messages: expected {}, got {}",
        max_per_source,
        g.load().edges.len()
    );
}

#[tokio::test]
async fn global_edge_cap() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let max_total = 10;
    let cfg = GraphConfig {
        max_edges_per_source: 100,
        max_total_edges: max_total,
        max_graph_peers: 1_000,
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    // Send edges from multiple sources exceeding global cap.
    for _ in 0..3 {
        let source_key = data::make_secret_key(rng);
        let source = peer_id(&source_key);
        let edges: Vec<Edge> = (0..5)
            .map(|_| {
                let k = data::make_secret_key(rng);
                data::make_edge(&node_key, &k, 1)
            })
            .collect();
        g.remote_update(source, edges).await;
    }

    let snapshot = g.load();
    assert_eq!(
        snapshot.edges.len(),
        max_total,
        "expected exactly {} edges (global cap), got {}",
        max_total,
        snapshot.edges.len()
    );
}

#[tokio::test]
async fn global_peer_cap() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    // Each edge (node_key, random_k) adds 1 new peer (the random_k side).
    // node_key itself is the BFS source and already in the graph = 1 peer.
    // With max_graph_peers=4, we can add at most 3 edges (3 new peers + 1 = 4).
    let cfg = GraphConfig {
        max_edges_per_source: 100,
        max_total_edges: 1_000,
        max_graph_peers: 4,
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    let source_key = data::make_secret_key(rng);
    let source = peer_id(&source_key);

    let edges: Vec<Edge> = (0..10)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, 1)
        })
        .collect();
    g.remote_update(source, edges).await;

    let snapshot = g.load();
    // 3 edges fit (3 new peers + 1 existing = 4 = max), 4th edge would need 5 > 4.
    assert_eq!(
        snapshot.edges.len(),
        3,
        "expected exactly 3 edges (peer cap), got {}",
        snapshot.edges.len()
    );
}

#[tokio::test]
async fn local_edges_obey_global_caps() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let max_total = 5;
    let cfg = GraphConfig {
        max_edges_per_source: 100,
        max_total_edges: max_total,
        max_graph_peers: 1_000,
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    // Local edges should also obey max_total_edges.
    let edges: Vec<Edge> = (0..max_total + 5)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, 1)
        })
        .collect();
    g.simple_update(edges).await;

    let snapshot = g.load();
    assert_eq!(
        snapshot.edges.len(),
        max_total,
        "expected exactly {} edges (global cap for local edges), got {}",
        max_total,
        snapshot.edges.len()
    );
}

/// Limit-triggered drops must NOT set ok=false (which would map to
/// ReasonForBan::InvalidEdge). Only signature/self-loop failures do that.
#[tokio::test]
async fn limit_drops_are_non_punitive() {
    init_test_logger();
    let clock = time::FakeClock::default();
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let cfg = GraphConfig {
        max_edges_per_source: 3,
        max_total_edges: 5,
        max_graph_peers: 4,
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    let source_key = data::make_secret_key(rng);
    let source = peer_id(&source_key);

    // Send 10 edges — many will be dropped by various caps.
    let edges: Vec<Edge> = (0..10)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, 1)
        })
        .collect();
    let oks = g.remote_update(source, edges).await;

    // ok must be true: limit drops are non-punitive.
    assert_eq!(oks, vec![true], "limit drops must not set ok=false");

    // Some edges should have been dropped (graph can't hold all 10).
    let snapshot = g.load();
    assert!(snapshot.edges.len() < 10);
}

/// After filling and then pruning a source's edges, the source should be
/// able to introduce new edge keys again (budget recovery).
#[tokio::test]
async fn source_budget_recovery_after_prune() {
    init_test_logger();
    let clock = time::FakeClock::default();
    clock.set_utc(time::Utc::UNIX_EPOCH + time::Duration::days(2));
    let mut rng = make_rng(87927345);
    let rng = &mut rng;
    let node_key = data::make_secret_key(rng);
    let max_per_source = 3;
    let cfg = GraphConfig {
        max_edges_per_source: max_per_source,
        max_total_edges: 1_000,
        max_graph_peers: 1_000,
        prune_unreachable_peers_after: time::Duration::hours(100),
        prune_edges_after: Some(time::Duration::seconds(60)),
        ..test_graph_config(peer_id(&node_key))
    };
    let g = Arc::new(Graph::new(clock.clock(), cfg));

    let source_key = data::make_secret_key(rng);
    let source = peer_id(&source_key);

    // Fill source to cap with 3 edges.
    let now = clock.now_utc();
    let edges: Vec<Edge> = (0..max_per_source)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, to_active_nonce(now))
        })
        .collect();
    g.remote_update(source.clone(), edges).await;
    assert_eq!(g.load().edges.len(), 3);

    // A 4th edge should be rejected (source cap reached).
    let extra_key = data::make_secret_key(rng);
    let extra_edge = data::make_edge(&node_key, &extra_key, to_active_nonce(now));
    g.remote_update(source.clone(), vec![extra_edge]).await;
    assert_eq!(g.load().edges.len(), 3, "4th edge should be rejected");

    // Advance time so old edges get pruned.
    clock.advance(time::Duration::seconds(120));
    // Trigger a pruning pass by sending an empty local update.
    g.simple_update(vec![]).await;
    assert_eq!(g.load().edges.len(), 0, "all old edges should be pruned");

    // Now the same source should be able to introduce new edge keys again.
    let fresh_now = clock.now_utc();
    let new_edges: Vec<Edge> = (0..max_per_source)
        .map(|_| {
            let k = data::make_secret_key(rng);
            data::make_edge(&node_key, &k, to_active_nonce(fresh_now))
        })
        .collect();
    g.remote_update(source, new_edges).await;
    assert_eq!(
        g.load().edges.len(),
        max_per_source,
        "source should be able to introduce new keys after budget recovery"
    );
}
