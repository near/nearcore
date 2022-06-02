use crate::private_actix::{StopMsg, ValidateEdgeList};
use crate::routing::edge_validator_actor::EdgeValidatorActor;
use crate::routing::graph::Graph;
use crate::routing::routing_table_view::SAVE_PEERS_MAX_TIME;
use crate::stats::metrics;
use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, Context, ContextFutureSpawner, Handler, Running,
    SyncArbiter, WrapFuture,
};
use near_network_primitives::types::{Edge, EdgeState};
use near_performance_metrics_macros::perf;
use near_primitives::borsh::BorshSerialize;
use near_primitives::network::PeerId;
use near_primitives::utils::index_to_bytes;
use near_rate_limiter::{ActixMessageResponse, ActixMessageWrapper, ThrottleToken};
use near_store::DBCol;
use near_store::{Store, StoreUpdate};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

/// `Prune` enum is to specify how often should we prune edges.
#[derive(Debug, Eq, PartialEq)]
pub enum Prune {
    /// Prune once per hour - default.
    OncePerHour,
    /// Prune right now - for testing purposes.
    Now,
    /// Don't prune at all - this happens in case we are in the middle of adding new edges.
    Disable,
}

/// RoutingTableActor that maintains routing table information. We currently have only one
/// instance of this actor.
///
/// We store the following information
///   - list of all known edges
///   - helper data structure for exchanging routing table
///   - routing information (where a message should be send to reach given peer)
///
/// We use store for following reasons:
///   - store removed edges to disk
///   - we currently don't store active edges to disk
pub struct RoutingTableActor {
    /// Data structure with all edges. It's guaranteed that `peer.0` < `peer.1`.
    pub edges_info: HashMap<(PeerId, PeerId), Edge>,
    /// Data structure used for exchanging routing tables.
    pub peer_ibf_set: crate::routing::ibf_peer_set::IbfPeerSet,
    /// Current view of the network represented by undirected graph.
    /// Nodes are Peers and edges are active connections.
    pub raw_graph: Graph,
    /// Active PeerId that are part of the shortest path to each PeerId.
    pub peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
    /// Last time a peer was reachable through active edges.
    pub peer_last_time_reachable: HashMap<PeerId, Instant>,
    /// Everytime a group of peers becomes unreachable at the same time; We store edges belonging to
    /// them in components. We remove all of those edges from memory, and save them to database,
    /// If any of them become reachable again, we re-add whole component.
    ///
    /// To store components, we have following column in the DB.
    /// DBCol::LastComponentNonce -> stores component_nonce: u64, which is the lowest nonce that
    ///                          hasn't been used yet. If new component gets created it will use
    ///                          this nonce.
    /// DBCol::ComponentEdges     -> Mapping from `component_nonce` to list of edges
    /// DBCol::PeerComponent      -> Mapping from `peer_id` to last component nonce if there
    ///                          exists one it belongs to.
    store: Store,
    /// First component nonce id that hasn't been used. Used for creating new components.
    pub next_available_component_nonce: u64,
    /// True if edges were changed and we need routing table recalculation.
    pub needs_routing_table_recalculation: bool,
    /// EdgeValidatorActor, which is responsible for validating edges.
    edge_validator_pool: Addr<EdgeValidatorActor>,
    /// Number of edge validations in progress; We will not update routing table as long as
    /// this number is non zero.
    edge_validator_requests_in_progress: u64,
    /// List of Peers to ban
    peers_to_ban: Vec<PeerId>,
}

impl RoutingTableActor {
    pub fn new(my_peer_id: PeerId, store: Store) -> Self {
        let component_nonce = store
            .get_ser::<u64>(DBCol::LastComponentNonce, &[])
            .unwrap_or(None)
            .map_or(0, |nonce| nonce + 1);
        let edge_validator_pool = SyncArbiter::start(4, || EdgeValidatorActor {});
        Self {
            edges_info: Default::default(),
            peer_ibf_set: Default::default(),
            raw_graph: Graph::new(my_peer_id),
            peer_forwarding: Default::default(),
            peer_last_time_reachable: Default::default(),
            store,
            next_available_component_nonce: component_nonce,
            needs_routing_table_recalculation: Default::default(),
            edge_validator_pool,
            edge_validator_requests_in_progress: Default::default(),
            peers_to_ban: Default::default(),
        }
    }

    pub fn remove_edges(&mut self, edges: &[Edge]) {
        for edge in edges.iter() {
            self.remove_edge(edge);
        }
    }

    pub fn remove_edge(&mut self, edge: &Edge) {
        #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
        self.peer_ibf_set.remove_edge(&edge.to_simple_edge());

        let key = edge.key();
        if self.edges_info.remove(key).is_some() {
            self.raw_graph.remove_edge(&edge.key().0, &edge.key().1);
            self.needs_routing_table_recalculation = true;
        }
    }

    /// `add_verified_edge` adds edges, for which we already that theirs signatures
    /// are valid (`signature0`, `signature`).
    fn add_verified_edge(&mut self, edge: Edge) -> bool {
        let key = edge.key();
        if !self.is_edge_newer(key, edge.nonce()) {
            // We already have a newer information about this edge. Discard this information.
            false
        } else {
            self.needs_routing_table_recalculation = true;
            match edge.edge_type() {
                EdgeState::Active => {
                    self.raw_graph.add_edge(&key.0, &key.1);
                }
                EdgeState::Removed => {
                    self.raw_graph.remove_edge(&key.0, &key.1);
                }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            self.peer_ibf_set.add_edge(&edge.to_simple_edge());
            self.edges_info.insert(key.clone(), edge);
            true
        }
    }

    /// Add several edges to the current view of the network.
    /// These edges are assumed to have been verified at this point.
    /// Return list of edges added.
    ///
    /// Everytime we remove an edge we store all edges removed at given time to disk.
    /// If new edge comes comes that is adjacent to a peer that has been previously removed,
    /// we will try to re-add edges previously removed from disk.
    pub fn add_verified_edges_to_routing_table(&mut self, mut edges: Vec<Edge>) -> Vec<Edge> {
        if edges.is_empty() {
            return Vec::new();
        }

        let total = edges.len();
        edges.retain(|edge| {
            let key = edge.key();

            self.fetch_edges_for_peer_from_disk(&key.0);
            self.fetch_edges_for_peer_from_disk(&key.1);

            self.add_verified_edge(edge.clone())
        });
        self.needs_routing_table_recalculation = true;

        // Update metrics after edge update
        metrics::EDGE_UPDATES.inc_by(total as u64);
        metrics::EDGE_ACTIVE.set(self.raw_graph.total_active_edges() as i64);

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
    fn fetch_edges_for_peer_from_disk(&mut self, other_peer_id: &PeerId) {
        if other_peer_id == self.my_peer_id()
            || self.peer_last_time_reachable.contains_key(other_peer_id)
        {
            return;
        }

        let my_peer_id = self.my_peer_id().clone();

        // Get the "row" (a.k.a nonce) at which we've stored a given peer in the past (when we pruned it).
        if let Ok(component_nonce) = self.component_nonce_from_peer(other_peer_id) {
            let mut update = self.store.store_update();

            // Load all edges that were persisted in database in the cell - and add them to the current graph.
            if let Ok(edges) = self.get_and_remove_component_edges(component_nonce, &mut update) {
                for edge in edges {
                    for &peer_id in vec![&edge.key().0, &edge.key().1].iter() {
                        if peer_id == &my_peer_id
                            || self.peer_last_time_reachable.contains_key(peer_id)
                        {
                            continue;
                        }

                        // `edge = (peer_id, other_peer_id)` belongs to component that we loaded from database.
                        if let Ok(cur_nonce) = self.component_nonce_from_peer(peer_id) {
                            // If `peer_id` belongs to current component
                            if cur_nonce == component_nonce {
                                // Mark it as reachable and delete from database.
                                self.peer_last_time_reachable
                                    .insert(peer_id.clone(), Instant::now() - SAVE_PEERS_MAX_TIME);
                                update.delete(
                                    DBCol::PeerComponent,
                                    peer_id.try_to_vec().unwrap().as_ref(),
                                );
                            }
                        }
                    }
                    self.add_verified_edge(edge);
                }
            }

            if let Err(e) = update.commit() {
                warn!(target: "network", "Error removing network component from store. {:?}", e);
            }
        } else {
            self.peer_last_time_reachable.insert(other_peer_id.clone(), Instant::now());
        }
    }

    fn my_peer_id(&self) -> &PeerId {
        self.raw_graph.my_peer_id()
    }

    /// Recalculate routing table and update list of reachable peers.
    pub fn recalculate_routing_table(&mut self) {
        let _d = delay_detector::DelayDetector::new(|| "routing table update".into());
        let _routing_table_recalculation =
            metrics::ROUTING_TABLE_RECALCULATION_HISTOGRAM.start_timer();

        trace!(target: "network", "Update routing table.");

        self.peer_forwarding = Arc::new(self.raw_graph.calculate_distance());

        let now = Instant::now();
        for peer in self.peer_forwarding.keys() {
            self.peer_last_time_reachable.insert(peer.clone(), now);
        }

        metrics::ROUTING_TABLE_RECALCULATIONS.inc();
        metrics::PEER_REACHABLE.set(self.peer_forwarding.len() as i64);
    }

    /// If pruning is enabled we will remove unused edges and store them to disk.
    ///
    /// # Returns
    /// List of edges removed.
    pub fn prune_edges(
        &mut self,
        prune: Prune,
        prune_edges_not_reachable_for: Duration,
    ) -> Vec<Edge> {
        if prune == Prune::Disable {
            return Vec::new();
        }

        let _d = delay_detector::DelayDetector::new(|| "pruning edges".into());

        let edges_to_remove = self.prune_unreachable_edges_and_save_to_db(
            prune == Prune::Now,
            prune_edges_not_reachable_for,
        );
        self.remove_edges(&edges_to_remove);
        edges_to_remove
    }

    fn prune_unreachable_edges_and_save_to_db(
        &mut self,
        force_pruning: bool,
        prune_edges_not_reachable_for: Duration,
    ) -> Vec<Edge> {
        let now = Instant::now();
        // Save nodes on disk and remove from memory only if elapsed time from oldest peer
        // is greater than `SAVE_PEERS_MAX_TIME`
        let do_pruning = force_pruning
            || (self.peer_last_time_reachable.values())
                .any(|&last_time| now.saturating_duration_since(last_time) >= SAVE_PEERS_MAX_TIME);
        if !do_pruning {
            return Vec::new();
        }

        // We compute routing graph every one second; we mark every node that was reachable during that time.
        // All nodes not reachable for at last 1 hour(SAVE_PEERS_AFTER_TIME) will be moved to disk.
        // TODO - use drain_filter once it becomes stable - #59618
        let peers_to_remove = (self.peer_last_time_reachable.iter())
            .filter(|(_, &last_time)| {
                now.saturating_duration_since(last_time) >= prune_edges_not_reachable_for
            })
            .map(|(peer_id, _)| peer_id.clone())
            .collect::<HashSet<_>>();

        debug!(target: "network", "try_save_edges: We are going to remove {} peers", peers_to_remove.len());

        let mut update = self.store.store_update();

        // Sets mapping from `peer_id` to `component nonce` in DB. This is later used to find
        // component that the edge belonged to.
        for peer_id in peers_to_remove.iter() {
            let _ = update.set_ser(
                DBCol::PeerComponent,
                peer_id.try_to_vec().unwrap().as_ref(),
                &self.next_available_component_nonce,
            );

            self.peer_last_time_reachable.remove(peer_id);
        }

        // TODO - use drain_filter once it becomes stable - #59618
        let edges_to_remove = (self.edges_info.values())
            .filter(|edge| {
                peers_to_remove.contains(&edge.key().0) || peers_to_remove.contains(&edge.key().1)
            })
            .cloned()
            .collect();

        let _ = update.set_ser(
            DBCol::ComponentEdges,
            &index_to_bytes(self.next_available_component_nonce),
            &edges_to_remove,
        );

        self.next_available_component_nonce += 1;
        // Stores next available nonce.
        let _ =
            update.set_ser(DBCol::LastComponentNonce, &[], &self.next_available_component_nonce);

        if let Err(e) = update.commit() {
            warn!(target: "network", "Error storing network component to store. {:?}", e);
        }
        edges_to_remove
    }

    /// Checks whenever given edge is newer than the one we already have.
    pub fn is_edge_newer(&self, key: &(PeerId, PeerId), nonce: u64) -> bool {
        self.edges_info.get(key).map_or(0, |x| x.nonce()) < nonce
    }

    /// Get edges stored in DB under `DBCol::PeerComponent` column at `peer_id` key.
    fn component_nonce_from_peer(&self, peer_id: &PeerId) -> Result<u64, ()> {
        match self
            .store
            .get_ser::<u64>(DBCol::PeerComponent, peer_id.try_to_vec().unwrap().as_ref())
        {
            Ok(Some(nonce)) => Ok(nonce),
            _ => Err(()),
        }
    }

    /// Get all edges that were stored at a given "row" (a.k.a. component_nonce) in the store (and also remove them).
    fn get_and_remove_component_edges(
        &self,
        component_nonce: u64,
        update: &mut StoreUpdate,
    ) -> Result<Vec<Edge>, ()> {
        let enc_nonce = index_to_bytes(component_nonce);

        let res = match self.store.get_ser::<Vec<Edge>>(DBCol::ComponentEdges, enc_nonce.as_ref()) {
            Ok(Some(edges)) => Ok(edges),
            _ => Err(()),
        };

        update.delete(DBCol::ComponentEdges, enc_nonce.as_ref());

        res
    }

    pub fn get_all_edges(&self) -> Vec<Edge> {
        self.edges_info.iter().map(|x| x.1.clone()).collect()
    }
}

impl Actor for RoutingTableActor {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {}

    /// Try to gracefully disconnect from active peers.
    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        Running::Stop
    }
}

impl Handler<StopMsg> for RoutingTableActor {
    type Result = ();
    fn handle(&mut self, _: StopMsg, ctx: &mut Self::Context) -> Self::Result {
        self.edge_validator_pool.do_send(StopMsg {});
        ctx.stop();
    }
}

impl RoutingTableActor {
    #[perf]
    fn handle_validate_edge_list(
        &mut self,
        msg: ValidateEdgeList,
        ctx: &mut Context<RoutingTableActor>,
    ) -> bool {
        self.edge_validator_requests_in_progress += 1;
        let mut msg = msg;
        msg.edges.retain(|x| self.is_edge_newer(x.key(), x.nonce()));
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

        true
    }
}

/// Messages for `RoutingTableActor`
#[derive(actix::Message, Debug)]
#[rtype(result = "RoutingTableMessagesResponse")]
pub enum RoutingTableMessages {
    /// Add verified edges to routing table actor and update stats.
    /// Each edge contains signature of both peers.
    /// We say that the edge is "verified" if and only if we checked that the `signature0` and
    /// `signature1` is valid.
    AddVerifiedEdges { edges: Vec<Edge> },
    /// Remove edges for unit tests
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AdvRemoveEdges(Vec<Edge>),
    /// Get `RoutingTable` for debugging purposes.
    RequestRoutingTable,
    /// Add `PeerId` and generate `IbfSet`.
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AddPeerIfMissing(PeerId, Option<u64>),
    /// Remove `PeerId` from `IbfSet`
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    RemovePeer(PeerId),
    /// Do new routing table exchange algorithm.
    ProcessIbfMessage { peer_id: PeerId, ibf_msg: crate::types::RoutingVersion2 },
    /// Start new routing table sync.
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    StartRoutingTableSync { seed: u64 },
    /// Request routing table update and maybe prune edges.
    RoutingTableUpdate { prune: Prune, prune_edges_not_reachable_for: Duration },
    /// Gets list of edges to validate from another peer.
    /// Those edges will be filtered, by removing existing edges, and then
    /// those edges will be sent to `EdgeValidatorActor`.
    ValidateEdgeList(ValidateEdgeList),
}

#[derive(actix::MessageResponse, Debug)]
pub enum RoutingTableMessagesResponse {
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AddPeerResponse {
        seed: u64,
    },
    Empty,
    ProcessIbfMessageResponse {
        ibf_msg: Option<crate::types::RoutingVersion2>,
    },
    RequestRoutingTableResponse {
        edges_info: Vec<Edge>,
    },
    AddVerifiedEdgesResponse(Vec<Edge>),
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    StartRoutingTableSyncResponse(crate::types::RoutingSyncV2),
    RoutingTableUpdateResponse {
        /// PeerManager maintains list of local edges. We will notify `PeerManager`
        /// to remove those edges.
        local_edges_to_remove: Vec<PeerId>,
        /// Active PeerId that are part of the shortest path to each PeerId.
        peer_forwarding: Arc<HashMap<PeerId, Vec<PeerId>>>,
        /// List of peers to ban for sending invalid edges.
        peers_to_ban: Vec<PeerId>,
    },
}

impl RoutingTableActor {
    pub fn exchange_routing_tables_using_ibf(
        &self,
        peer_id: &PeerId,
        ibf_set: &crate::routing::IbfSet<near_network_primitives::types::SimpleEdge>,
        ibf_level: crate::routing::ibf_peer_set::ValidIBFLevel,
        ibf_vec: &[crate::routing::ibf::IbfBox],
        seed: u64,
    ) -> (Vec<near_network_primitives::types::SimpleEdge>, Vec<u64>, u64) {
        let ibf = ibf_set.get_ibf(ibf_level);

        let mut new_ibf = crate::routing::ibf::Ibf::from_vec(ibf_vec, seed ^ (ibf_level.0 as u64));

        if !new_ibf.merge(&ibf.data, seed ^ (ibf_level.0 as u64)) {
            tracing::error!(target: "network", "exchange routing tables failed with peer {}", peer_id);
            return (Default::default(), Default::default(), 0);
        }

        let (edge_hashes, unknown_edges_count) = new_ibf.try_recover();
        let (known, unknown_edges) = self.peer_ibf_set.split_edges_for_peer(peer_id, &edge_hashes);

        (known, unknown_edges, unknown_edges_count)
    }
}

impl Handler<RoutingTableMessages> for RoutingTableActor {
    type Result = RoutingTableMessagesResponse;

    #[perf]
    fn handle(&mut self, msg: RoutingTableMessages, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            RoutingTableMessages::ValidateEdgeList(validate_edge_list) => {
                self.handle_validate_edge_list(validate_edge_list, ctx);
                RoutingTableMessagesResponse::Empty
            }
            RoutingTableMessages::AddVerifiedEdges { edges } => {
                RoutingTableMessagesResponse::AddVerifiedEdgesResponse(
                    self.add_verified_edges_to_routing_table(edges),
                )
            }
            RoutingTableMessages::RoutingTableUpdate {
                mut prune,
                prune_edges_not_reachable_for,
            } => {
                if prune == Prune::OncePerHour && self.edge_validator_requests_in_progress != 0 {
                    prune = Prune::Disable;
                }

                let edges_removed = if self.needs_routing_table_recalculation || prune == Prune::Now
                {
                    self.needs_routing_table_recalculation = false;
                    self.recalculate_routing_table();
                    self.prune_edges(prune, prune_edges_not_reachable_for)
                } else {
                    Vec::new()
                };
                RoutingTableMessagesResponse::RoutingTableUpdateResponse {
                    local_edges_to_remove: (edges_removed.iter())
                        .filter_map(|e| e.other(self.my_peer_id()))
                        .cloned()
                        .collect(),
                    peer_forwarding: self.peer_forwarding.clone(),
                    peers_to_ban: std::mem::take(&mut self.peers_to_ban),
                }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::StartRoutingTableSync { seed } => {
                RoutingTableMessagesResponse::StartRoutingTableSyncResponse(
                    crate::types::RoutingSyncV2::Version2(crate::types::RoutingVersion2 {
                        known_edges: self.edges_info.len() as u64,
                        seed,
                        edges: Default::default(),
                        routing_state: crate::types::RoutingState::InitializeIbf,
                    }),
                )
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::AdvRemoveEdges(edges) => {
                for edge in edges.iter() {
                    self.remove_edge(edge);
                }
                RoutingTableMessagesResponse::Empty
            }
            RoutingTableMessages::RequestRoutingTable => {
                RoutingTableMessagesResponse::RequestRoutingTableResponse {
                    edges_info: self.edges_info.iter().map(|(_k, v)| v.clone()).collect(),
                }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::AddPeerIfMissing(peer_id, ibf_set) => {
                let seed = self.peer_ibf_set.add_peer(peer_id, ibf_set, &mut self.edges_info);
                RoutingTableMessagesResponse::AddPeerResponse { seed }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::RemovePeer(peer_id) => {
                self.peer_ibf_set.remove_peer(&peer_id);
                RoutingTableMessagesResponse::Empty
            }
            RoutingTableMessages::ProcessIbfMessage { peer_id, ibf_msg } => {
                match ibf_msg.routing_state {
                    crate::types::RoutingState::PartialSync(partial_sync) => {
                        if let Some(ibf_set) = self.peer_ibf_set.get(&peer_id) {
                            let seed = ibf_msg.seed;
                            let (edges_for_peer, unknown_edge_hashes, unknown_edges_count) = self
                                .exchange_routing_tables_using_ibf(
                                    &peer_id,
                                    ibf_set,
                                    partial_sync.ibf_level,
                                    &partial_sync.ibf,
                                    ibf_msg.seed,
                                );

                            let edges_for_peer = edges_for_peer
                                .iter()
                                .filter_map(|x| self.edges_info.get(x.key()).cloned())
                                .collect();
                            // Prepare message
                            let ibf_msg = if unknown_edges_count == 0
                                && !unknown_edge_hashes.is_empty()
                            {
                                crate::types::RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: edges_for_peer,
                                    routing_state: crate::types::RoutingState::RequestMissingEdges(
                                        unknown_edge_hashes,
                                    ),
                                }
                            } else if unknown_edges_count == 0 && unknown_edge_hashes.is_empty() {
                                crate::types::RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: edges_for_peer,
                                    routing_state: crate::types::RoutingState::Done,
                                }
                            } else if let Some(new_ibf_level) = partial_sync.ibf_level.inc() {
                                let ibf_vec = ibf_set.get_ibf_vec(new_ibf_level);
                                crate::types::RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: edges_for_peer,
                                    routing_state: crate::types::RoutingState::PartialSync(
                                        crate::types::PartialSync {
                                            ibf_level: new_ibf_level,
                                            ibf: ibf_vec.clone(),
                                        },
                                    ),
                                }
                            } else {
                                crate::types::RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: self.edges_info.iter().map(|x| x.1.clone()).collect(),
                                    routing_state: crate::types::RoutingState::RequestAllEdges,
                                }
                            };
                            RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                                ibf_msg: Some(ibf_msg),
                            }
                        } else {
                            tracing::error!(target: "network", "Peer not found {}", peer_id);
                            RoutingTableMessagesResponse::Empty
                        }
                    }
                    crate::types::RoutingState::InitializeIbf => {
                        self.peer_ibf_set.add_peer(
                            peer_id.clone(),
                            Some(ibf_msg.seed),
                            &mut self.edges_info,
                        );
                        if let Some(ibf_set) = self.peer_ibf_set.get(&peer_id) {
                            let seed = ibf_set.get_seed();
                            let ibf_vec =
                                ibf_set.get_ibf_vec(crate::routing::ibf_peer_set::MIN_IBF_LEVEL);
                            RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                                ibf_msg: Some(crate::types::RoutingVersion2 {
                                    known_edges: self.edges_info.len() as u64,
                                    seed,
                                    edges: Default::default(),
                                    routing_state: crate::types::RoutingState::PartialSync(
                                        crate::types::PartialSync {
                                            ibf_level: crate::routing::ibf_peer_set::MIN_IBF_LEVEL,
                                            ibf: ibf_vec.clone(),
                                        },
                                    ),
                                }),
                            }
                        } else {
                            tracing::error!(target: "network", "Peer not found {}", peer_id);
                            RoutingTableMessagesResponse::Empty
                        }
                    }
                    crate::types::RoutingState::RequestMissingEdges(requested_edges) => {
                        let seed = ibf_msg.seed;
                        let (edges_for_peer, _) =
                            self.peer_ibf_set.split_edges_for_peer(&peer_id, &requested_edges);

                        let edges_for_peer = edges_for_peer
                            .iter()
                            .filter_map(|x| self.edges_info.get(x.key()).cloned())
                            .collect();

                        let ibf_msg = crate::types::RoutingVersion2 {
                            known_edges: self.edges_info.len() as u64,
                            seed,
                            edges: edges_for_peer,
                            routing_state: crate::types::RoutingState::Done,
                        };
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                            ibf_msg: Some(ibf_msg),
                        }
                    }
                    crate::types::RoutingState::RequestAllEdges => {
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                            ibf_msg: Some(crate::types::RoutingVersion2 {
                                known_edges: self.edges_info.len() as u64,
                                seed: ibf_msg.seed,
                                edges: self.get_all_edges(),
                                routing_state: crate::types::RoutingState::Done,
                            }),
                        }
                    }
                    crate::types::RoutingState::Done => {
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse { ibf_msg: None }
                    }
                }
            }
        }
    }
}

impl Handler<ActixMessageWrapper<RoutingTableMessages>> for RoutingTableActor {
    type Result = ActixMessageResponse<RoutingTableMessagesResponse>;

    fn handle(
        &mut self,
        msg: ActixMessageWrapper<RoutingTableMessages>,
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

pub fn start_routing_table_actor(peer_id: PeerId, store: Store) -> Addr<RoutingTableActor> {
    RoutingTableActor::new(peer_id, store).start()
}
