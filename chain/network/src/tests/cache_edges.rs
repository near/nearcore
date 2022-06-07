use crate::routing;
use crate::store;
use crate::store::testonly::Component;
use crate::tests::data;
use crate::tests::util;
use near_crypto::Signature;
use near_network_primitives::time;
use near_network_primitives::types::Edge;
use near_primitives::network::PeerId;
use near_store::test_utils::create_test_store;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

fn edge(p0: &PeerId, p1: &PeerId, nonce: u64) -> Edge {
    Edge::new(p0.clone(), p1.clone(), nonce, Signature::default(), Signature::default())
}

struct RoutingTableTest {
    clock: time::FakeClock,
    rng: util::Rng,
    store: store::Store,
    graph: Arc<RwLock<routing::GraphWithCache>>,
    // This is the system runner attached to the given test's system thread.
    // Allows to create actors within the test.
    _system: actix::SystemRunner,
}

impl Drop for RoutingTableTest {
    fn drop(&mut self) {
        actix::System::current().stop();
    }
}

impl RoutingTableTest {
    // Gets a new random PeerId.
    pub fn make_peer(&mut self) -> PeerId {
        data::make_peer_id(&mut self.rng)
    }

    fn me(&self) -> PeerId {
        self.graph.read().my_peer_id()
    }

    fn new() -> Self {
        let mut rng = util::make_rng(87927345);
        let clock = time::FakeClock::default();
        let me = data::make_peer_id(&mut rng);
        let store = store::Store::new(create_test_store());

        let graph = Arc::new(RwLock::new(routing::GraphWithCache::new(me.clone())));
        Self { rng, clock, graph, store, _system: actix::System::new() }
    }

    fn new_actor(&self) -> routing::actor::Actor {
        routing::actor::Actor::new(self.clock.clock(), self.store.clone(), self.graph.clone())
    }

    fn check(&mut self, want_mem: Vec<Edge>, want_db: Vec<Component>) {
        let got_mem: HashMap<_, _> = self.graph.read().edges().clone().into_iter().collect();
        let want_mem: HashMap<_, _> = want_mem.into_iter().map(|e| (e.key().clone(), e)).collect();
        assert_eq!(got_mem, want_mem);

        let got_db: HashSet<_> =
            self.store.list_components().into_iter().map(|c| c.normal()).collect();
        let want_db: HashSet<_> = want_db.into_iter().map(|c| c.normal()).collect();
        assert_eq!(got_db, want_db);
    }
}

#[test]
fn empty() {
    let mut test = RoutingTableTest::new();
    test.check([].into(), [].into());
}

const SEC: time::Duration = time::Duration::seconds(1);

#[test]
fn one_edge() {
    let mut test = RoutingTableTest::new();
    let mut actor = test.new_actor();
    let p1 = test.make_peer();
    let e1 = edge(&test.me(), &p1, 1);
    let e1v2 = edge(&test.me(), &p1, 2);

    // Add an active edge.
    actor.add_verified_edges(vec![e1.clone()]);
    test.check([e1.clone()].into(), [].into());

    // Update RT with pruning. NOOP, since p1 is reachable.
    actor.update_routing_table(Some(test.clock.now()));
    test.check(vec![e1.clone()], vec![]);

    // Override with an inactive edge.
    actor.add_verified_edges(vec![e1v2.clone()]);
    test.check([e1v2.clone()].into(), [].into());

    // After 2s, update RT without pruning.
    test.clock.advance(2 * SEC);
    actor.update_routing_table(None);
    test.check(vec![e1v2.clone()], vec![]);

    // Update RT with pruning unreachable for 3s. NOOP, since p1 is unreachable for 2s.
    actor.update_routing_table(Some(test.clock.now() - 3 * SEC));
    test.check(vec![e1v2.clone()], vec![]);

    // Update RT with pruning unreachable for 1s. p1 should be moved to DB.
    actor.update_routing_table(Some(test.clock.now() - SEC));
    test.check(vec![], vec![Component { edges: vec![e1v2.clone()], peers: vec![p1.clone()] }]);
}

#[test]
fn load_component() {
    let mut test = RoutingTableTest::new();
    let mut actor = test.new_actor();
    let p1 = test.make_peer();
    let p2 = test.make_peer();
    let e1 = edge(&test.me(), &p1, 2);
    let e2 = edge(&test.me(), &p2, 2);
    let e3 = edge(&p1, &p2, 1);
    let e1v2 = edge(&test.me(), &p1, 3);

    // There is an active edge between p1,p2, but neither is reachable from me().
    // They should be pruned.
    actor.add_verified_edges(vec![e1.clone(), e2.clone(), e3.clone()]);
    actor.update_routing_table(Some(test.clock.now()));
    test.check(
        vec![],
        vec![Component {
            edges: vec![e1.clone(), e2.clone(), e3.clone()],
            peers: vec![p1.clone(), p2.clone()],
        }],
    );

    // Add an active edge from me() to p1. This should trigger loading the whole component from DB.
    actor.add_verified_edges(vec![e1v2.clone()]);
    actor.update_routing_table(Some(test.clock.now()));
    test.check(vec![e1v2, e2, e3], vec![]);
}

#[test]
fn components_nonces_are_tracked_in_storage() {
    let mut test = RoutingTableTest::new();

    // Start the actor, add an inactive edge and prune it.
    let mut actor = test.new_actor();
    let p1 = test.make_peer();
    let e1 = edge(&test.me(), &p1, 2);
    actor.add_verified_edges(vec![e1.clone()]);
    actor.update_routing_table(Some(test.clock.now()));
    test.check(vec![], vec![Component { edges: vec![e1.clone()], peers: vec![p1.clone()] }]);

    // Add an active unreachable edge, which also should get pruned.
    let p2 = test.make_peer();
    let p3 = test.make_peer();
    let e23 = edge(&p2, &p3, 2);
    actor.add_verified_edges(vec![e23.clone()]);
    actor.update_routing_table(Some(test.clock.now()));
    test.check(
        vec![],
        vec![
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component { edges: vec![e23.clone()], peers: vec![p2.clone(), p3.clone()] },
        ],
    );

    // Restart the actor.
    // Add another inactive edge and prune it. The previously created component shouldn't get
    // overwritten, but rather a new one should be created.
    // This verifies that the last_component_nonce (which indicates which component IDs have been
    // already utilized) is persistently stored in DB.
    let mut actor = test.new_actor();
    let p4 = test.make_peer();
    let e4 = edge(&test.me(), &p4, 2);
    actor.add_verified_edges(vec![e4.clone()]);
    actor.update_routing_table(Some(test.clock.now()));
    test.check(
        vec![],
        vec![
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component { edges: vec![e23.clone()], peers: vec![p2.clone(), p3.clone()] },
            Component { edges: vec![e4.clone()], peers: vec![p4.clone()] },
        ],
    );

    // Add an active edge between unreachable nodes, which will merge 2 components in DB.
    let e34 = edge(&p3, &p4, 1);
    actor.add_verified_edges(vec![e34.clone()]);
    actor.update_routing_table(Some(test.clock.now()));
    test.check(
        vec![],
        vec![
            Component { edges: vec![e1.clone()], peers: vec![p1.clone()] },
            Component {
                edges: vec![e4.clone(), e23.clone(), e34.clone()],
                peers: vec![p2.clone(), p3.clone(), p4.clone()],
            },
        ],
    );
}
