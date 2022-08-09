use crate::private_actix::{StopMsg, ValidateEdgeList};
use crate::routing;
use crate::routing::edge_validator_actor::EdgeValidatorActor;
use crate::stats::metrics;
use crate::store;
use actix::{
    ActorContext as _, ActorFutureExt, Addr, Context, ContextFutureSpawner as _, Running,
    WrapFuture as _,
};
use near_network_primitives::time::{self, Utc};
use near_network_primitives::types::Edge;
use near_performance_metrics_macros::perf;
use near_primitives::network::PeerId;
use near_rate_limiter::{ActixMessageResponse, ActixMessageWrapper, ThrottleToken};
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
    /// List of Peers to ban
    peers_to_ban: Vec<PeerId>,
    /// EdgeValidatorActor, which is responsible for validating edges.
    edge_validator_pool: Addr<EdgeValidatorActor>,
    /// Number of edge validations in progress; We will not update routing table as long as
    /// this number is non zero.
    edge_validator_requests_in_progress: u64,
}

impl Actor {
    pub fn new(
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
            peers_to_ban: Default::default(),
            edge_validator_requests_in_progress: 0,
            edge_validator_pool: actix::SyncArbiter::start(4, || EdgeValidatorActor {}),
        }
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
        mut prune_unreachable_since: Option<time::Instant>,
        prune_edges_older_than: Option<Utc>,
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
        if self.edge_validator_requests_in_progress != 0 {
            prune_unreachable_since = None;
        }
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
}

impl actix::Handler<StopMsg> for Actor {
    type Result = ();
    fn handle(&mut self, _: StopMsg, ctx: &mut Self::Context) -> Self::Result {
        self.edge_validator_pool.do_send(StopMsg {});
        ctx.stop();
    }
}

/// Messages for `RoutingTableActor`
#[derive(actix::Message, Debug)]
#[rtype(result = "Response")]
pub enum Message {
    /// Gets list of edges to validate from another peer.
    /// Those edges will be filtered, by removing existing edges, and then
    /// those edges will be sent to `EdgeValidatorActor`.
    ValidateEdgeList(ValidateEdgeList),
    /// Add verified edges to routing table actor and update stats.
    /// Each edge contains signature of both peers.
    /// We say that the edge is "verified" if and only if we checked that the `signature0` and
    /// `signature1` is valid.
    AddVerifiedEdges { edges: Vec<Edge> },
    /// Request routing table update and maybe prune edges.
    RoutingTableUpdate {
        prune_unreachable_since: Option<time::Instant>,
        prune_edges_older_than: Option<Utc>,
    },
    /// TEST-ONLY Remove edges.
    AdvRemoveEdges(Vec<Edge>),
}

#[derive(actix::MessageResponse, Debug)]
pub enum Response {
    Empty,
    AddVerifiedEdgesResponse(Vec<Edge>),
    RoutingTableUpdateResponse {
        /// PeerManager maintains list of local edges. We will notify `PeerManager`
        /// to remove those edges.
        local_edges_to_remove: Vec<PeerId>,
        /// Active PeerId that are part of the shortest path to each PeerId.
        next_hops: Arc<routing::NextHopTable>,
        /// List of peers to ban for sending invalid edges.
        peers_to_ban: Vec<PeerId>,
    },
}

impl actix::Handler<Message> for Actor {
    type Result = Response;

    #[perf]
    fn handle(&mut self, msg: Message, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            // Schedules edges for validation.
            Message::ValidateEdgeList(mut msg) => {
                self.edge_validator_requests_in_progress += 1;
                msg.edges.retain(|x| !self.graph.read().has(x));
                let peer_id = msg.source_peer_id.clone();
                self.edge_validator_pool
                    .send(msg)
                    .into_actor(self)
                    .map(move |res, act, _| {
                        act.edge_validator_requests_in_progress -= 1;
                        if let Ok(false) = res {
                            act.peers_to_ban.push(peer_id);
                        }
                    })
                    .spawn(ctx);
                Response::Empty
            }
            // Adds verified edges to the graph. Accesses DB.
            Message::AddVerifiedEdges { edges } => {
                Response::AddVerifiedEdgesResponse(self.add_verified_edges(edges))
            }
            // Recalculates the routing table.
            Message::RoutingTableUpdate { prune_unreachable_since, prune_edges_older_than } => {
                let (next_hops, pruned_edges) =
                    self.update_routing_table(prune_unreachable_since, prune_edges_older_than);
                Response::RoutingTableUpdateResponse {
                    local_edges_to_remove: pruned_edges
                        .iter()
                        .filter_map(|e| e.other(&self.my_peer_id))
                        .cloned()
                        .collect(),
                    next_hops,
                    peers_to_ban: std::mem::take(&mut self.peers_to_ban),
                }
            }
            // TEST-ONLY
            Message::AdvRemoveEdges(edges) => {
                for edge in edges {
                    self.graph.write().remove_edge(edge.key());
                }
                Response::Empty
            }
        }
    }
}

impl actix::Handler<ActixMessageWrapper<Message>> for Actor {
    type Result = ActixMessageResponse<Response>;

    fn handle(
        &mut self,
        msg: ActixMessageWrapper<Message>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        // Unpack throttle controller
        let (msg, throttle_token) = msg.take();

        let result = self.handle(msg, ctx);

        // TODO(#5155) Add support for DeepSizeOf to result
        ActixMessageResponse::new(
            result,
            ThrottleToken::new(throttle_token.throttle_controller().cloned(), 0),
        )
    }
}
