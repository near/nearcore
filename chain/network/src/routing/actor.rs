use crate::network_protocol::Edge;
use crate::private_actix::StopMsg;
use crate::routing;
use crate::stats::metrics;
use crate::store;
use crate::time;
use actix::{
    Actor as _, ActorContext as _, Context, 
    Running,
};
use near_o11y::{
    handler_debug_span, handler_trace_span, OpenTelemetrySpanExt, WithSpanContext,
};
use near_performance_metrics_macros::perf;
use near_primitives::network::PeerId;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;

/// Actor that maintains routing table information.
///
/// We store the following information
///   - routing information (where a message should be send to reach given peer)
///
/// We use store for following reasons:
///   - store removed edges to disk
///   - we currently don't store active edges to disk
pub(crate) struct Actor {
    clock: time::Clock,
    my_peer_id: PeerId,
    // routing::Actor is the only actor which is allowed to update the Graph.
    // Other actors (namely PeerManagerActor) are supposed to have read-only access to it.
    // Graph is locked for writes only to update a bunch of edges.
    // You must not put expensive computations under the lock and/or DB access.
    // TODO: Reimplement GraphWithCache to GraphWithCache(Arc<RwLock<GraphWithCacheInner>>),
    // to enforce the constraint above.
    graph: Arc<RwLock<routing::GraphWithCache>>,
    store: store::Store,
    /// Last time a peer was reachable.
    peer_reachable_at: HashMap<PeerId, time::Instant>,
}

impl Actor {
    pub(super) fn new(
        clock: time::Clock,
        store: store::Store,
        graph: Arc<RwLock<routing::GraphWithCache>>,
    ) -> Self {
        let my_peer_id = graph.read().my_peer_id();
        Self {
            clock,
            my_peer_id,
            graph,
            store,
            peer_reachable_at: Default::default(),
        }
    }

    pub fn spawn(
        clock: time::Clock,
        store: store::Store,
        graph: Arc<RwLock<routing::GraphWithCache>>,
    ) -> actix::Addr<Self> {
        let arbiter = actix::Arbiter::new();
        Actor::start_in_arbiter(&arbiter.handle(), |_| Self::new(clock, store, graph))
    }

    /// Add several edges to the current view of the network.
    /// These edges are assumed to have been verified at this point.
    /// Each edge actually represents a "version" of an edge, identified by Edge.nonce.
    /// Returns a list of edges (versions) which were not previously observed. Requires DB access.
    ///
    /// Everytime we remove an edge we store all edges removed at given time to disk.
    /// If new edge comes comes that is adjacent to a peer that has been previously removed,
    /// we will try to re-add edges previously removed from disk.
    pub fn add_verified_edges(&mut self, edges: Vec<Edge>) -> Vec<Edge> {
        let total = edges.len();
        // load the components BEFORE graph.update_edges
        // so that result doesn't contain edges we already have in storage.
        // It is especially important for initial full sync with peers, because
        // we broadcast all the returned edges to all connected peers.
        for edge in &edges {
            let key = edge.key();
            self.load_component(&key.0);
            self.load_component(&key.1);
        }
        let edges = self.graph.write().update_edges(edges);
        // Update metrics after edge update
        metrics::EDGE_UPDATES.inc_by(total as u64);
        metrics::EDGE_ACTIVE.set(self.graph.read().total_active_edges() as i64);
        metrics::EDGE_TOTAL.set(self.graph.read().edges().len() as i64);
        edges
    }

    /// If peer_id is not in memory check if it is on disk in bring it back on memory.
    ///
    /// Note: here an advanced example, which shows what's happening.
    /// Let's say we have a full graph fully connected with nodes `A, B, C, D`.
    /// Step 1 ) `A`, `B` get removed.
    /// We store edges belonging to `A` and `B`: `<A,B>, <A,C>, <A, D>, <B, C>, <B, D>`
    /// into component 1 let's call it `C_1`.
    /// And mapping from `A` to `C_1`, and from `B` to `C_1`
    ///
    /// Note that `C`, `D` is still active.
    ///
    /// Step 2) 'C' gets removed.
    /// We stored edges <C, D> into component 2 `C_2`.
    /// And a mapping from `C` to `C_2`.
    ///
    /// Note that `D` is still active.
    ///
    /// Step 3) An active edge gets added from `D` to `A`.
    /// We will load `C_1` and try to re-add all edges belonging to `C_1`.
    /// We will add `<A,B>, <A,C>, <A, D>, <B, C>, <B, D>`
    ///
    /// Important note: `C_1` also contains an edge from `A` to `C`, though `C` was removed in `C_2`.
    /// - 1) We will not load edges belonging to `C_2`, even though we are adding an edges from `A` to deleted `C`.
    /// - 2) We will not delete mapping from `C` to `C_2`, because `C` doesn't belong to `C_1`.
    /// - 3) Later, `C` will be deleted, because we will figure out it's not reachable.
    /// New component `C_3` will be created.
    /// And mapping from `C` to `C_2` will be overridden by mapping from `C` to `C_3`.
    /// And therefore `C_2` component will become unreachable.
    /// TODO(gprusak): this whole algorithm seems to be leaking stuff to storage and never cleaning up.
    /// What is the point of it? What does it actually gives us?
    fn load_component(&mut self, peer_id: &PeerId) {
        if *peer_id == self.my_peer_id || self.peer_reachable_at.contains_key(peer_id) {
            return;
        }
        let edges = match self.store.pop_component(peer_id) {
            Ok(edges) => edges,
            Err(e) => {
                warn!("self.store.pop_component({}): {}", peer_id, e);
                return;
            }
        };
        self.graph.write().update_edges(edges);
    }

    /// Prunes peers unreachable since <unreachable_since> (and their adjacent edges)
    /// from the in-mem graph and stores them in DB.
    /// # Returns
    /// List of edges removed.
    pub(crate) fn prune_unreachable_peers(
        &mut self,
        unreachable_since: time::Instant,
    ) -> Vec<Edge> {
        let _d = delay_detector::DelayDetector::new(|| "pruning unreachable peers".into());

        // Select peers to prune.
        let mut peers = HashSet::new();
        for (k, _) in self.graph.read().edges() {
            for peer_id in [&k.0, &k.1] {
                if self
                    .peer_reachable_at
                    .get(peer_id)
                    .map(|t| t < &unreachable_since)
                    .unwrap_or(true)
                {
                    peers.insert(peer_id.clone());
                }
            }
        }
        if peers.is_empty() {
            return vec![];
        }

        // Prune peers from peer_reachable_at.
        for peer_id in &peers {
            self.peer_reachable_at.remove(&peer_id);
        }

        // Prune edges from graph.
        let edges = self.graph.write().remove_adjacent_edges(&peers);

        // Store the pruned data in DB.
        if let Err(e) = self.store.push_component(&peers, &edges) {
            warn!("self.store.push_component(): {}", e);
        }
        edges
    }

    /// update_routing_table
    /// 1. recomputes the routing table (if needed)
    /// 2. bumps peer_reachable_at to now() for peers which are still reachable.
    /// 3. prunes peers which are unreachable `prune_unreachable_since`.
    /// Returns the new routing table and the pruned edges - adjacent to the pruned peers.
    /// Should be called periodically.
    pub fn update_routing_table(
        &mut self,
        prune_unreachable_since: Option<time::Instant>,
        prune_edges_older_than: Option<time::Utc>,
    ) -> (Arc<routing::NextHopTable>, Vec<Edge>) {
        if let Some(prune_edges_older_than) = prune_edges_older_than {
            self.graph.write().prune_old_edges(prune_edges_older_than)
        }
        let next_hops = self.graph.read().next_hops();
        // Update peer_reachable_at.
        let now = self.clock.now();
        self.peer_reachable_at.insert(self.my_peer_id.clone(), now);
        for peer in next_hops.keys() {
            self.peer_reachable_at.insert(peer.clone(), now);
        }
        // Do not prune if there are edges to validate in flight.
        // TODO:
        /*if self.edge_validator_requests_in_progress != 0 {
            prune_unreachable_since = None;
        }*/
        let pruned_edges = match prune_unreachable_since {
            None => vec![],
            Some(t) => self.prune_unreachable_peers(t),
        };
        (next_hops, pruned_edges)
    }
}

impl actix::Actor for Actor {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Self::Context) {}
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        Running::Stop
    }
    fn stopped(&mut self, _ctx: &mut Self::Context) {
        actix::Arbiter::current().stop();
    }
}

impl actix::Handler<WithSpanContext<StopMsg>> for Actor {
    type Result = ();
    fn handle(&mut self, msg: WithSpanContext<StopMsg>, ctx: &mut Self::Context) -> Self::Result {
        let (_span, _msg) = handler_debug_span!(target: "network", msg);
        ctx.stop();
    }
}

/// Messages for `RoutingTableActor`
#[derive(actix::Message, Debug, strum::IntoStaticStr)]
#[rtype(result = "Response")]
pub(crate) enum Message {
    /// Add verified edges to routing table actor and update stats.
    /// Each edge contains signature of both peers.
    /// We say that the edge is "verified" if and only if we checked that the `signature0` and
    /// `signature1` is valid.
    AddVerifiedEdges { edges: Vec<Edge> },
    /// Request routing table update and maybe prune edges.
    RoutingTableUpdate {
        prune_unreachable_since: Option<time::Instant>,
        prune_edges_older_than: Option<time::Utc>,
    },
}

#[derive(actix::MessageResponse, Debug)]
pub enum Response {
    Empty,
    AddVerifiedEdges(Vec<Edge>),
    RoutingTableUpdate {
        /// PeerManager maintains list of local edges. We will notify `PeerManager`
        /// to remove those edges.
        pruned_edges: Vec<Edge>,
        /// Active PeerId that are part of the shortest path to each PeerId.
        next_hops: Arc<routing::NextHopTable>,
    },
}

impl actix::Handler<WithSpanContext<Message>> for Actor {
    type Result = Response;

    #[perf]
    fn handle(&mut self, msg: WithSpanContext<Message>, _ctx: &mut Self::Context) -> Self::Result {
        let msg_type: &str = (&msg.msg).into();
        let (_span, msg) = handler_trace_span!(target: "network", msg, msg_type);
        let _timer =
            metrics::ROUTING_TABLE_MESSAGES_TIME.with_label_values(&[msg_type]).start_timer();
        match msg {
            // Adds verified edges to the graph. Accesses DB.
            Message::AddVerifiedEdges { edges } => {
                Response::AddVerifiedEdges(self.add_verified_edges(edges))
            }
            // Recalculates the routing table.
            Message::RoutingTableUpdate { prune_unreachable_since, prune_edges_older_than } => {
                let (next_hops, pruned_edges) =
                    self.update_routing_table(prune_unreachable_since, prune_edges_older_than);
                Response::RoutingTableUpdate { pruned_edges, next_hops }
            }
        }
    }
}
