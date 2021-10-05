use std::collections::HashMap;

use actix::dev::MessageResponse;
use actix::{Actor, Handler, Message, SyncContext, System};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use tracing::error;

use near_performance_metrics_macros::perf;
use near_primitives::network::PeerId;

#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::ibf::{Ibf, IbfBox};
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::ibf_peer_set::IbfPeerSet;
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::ibf_set::IbfSet;
use crate::routing::Edge;
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::routing::{SimpleEdge, ValidIBFLevel, MIN_IBF_LEVEL};
use crate::types::StopMsg;
#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
use crate::types::{PartialSync, RoutingState, RoutingVersion2};

/// Actor that maintains routing table information.
/// TODO (PIOTR, #4859) Finish moving routing table computation to new thread.
#[derive(Default)]
pub struct RoutingTableActor {
    /// Data structures with all edges.
    edges: HashMap<(PeerId, PeerId), Edge>,
    /// Data structure used for exchanging routing tables.
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    pub peer_ibf_set: IbfPeerSet,
}

#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
impl RoutingTableActor {
    pub fn split_edges_for_peer(
        &self,
        peer_id: &PeerId,
        unknown_edges: &[u64],
    ) -> (Vec<SimpleEdge>, Vec<u64>) {
        self.peer_ibf_set.split_edges_for_peer(peer_id, unknown_edges)
    }
}

impl Handler<StopMsg> for RoutingTableActor {
    type Result = ();
    fn handle(&mut self, _: StopMsg, _ctx: &mut Self::Context) -> Self::Result {
        System::current().stop();
    }
}

impl Actor for RoutingTableActor {
    type Context = SyncContext<Self>;
}

#[derive(Debug)]
pub enum RoutingTableMessages {
    AddEdges(Vec<Edge>),
    RemoveEdges(Vec<Edge>),
    RequestRoutingTable,
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AddPeerIfMissing(PeerId, Option<u64>),
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    RemovePeer(PeerId),
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    ProcessIbfMessage {
        peer_id: PeerId,
        ibf_msg: RoutingVersion2,
    },
}

impl Message for RoutingTableMessages {
    type Result = RoutingTableMessagesResponse;
}

#[derive(MessageResponse, Debug)]
pub enum RoutingTableMessagesResponse {
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    AddPeerResponse {
        seed: u64,
    },
    Empty,
    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
    ProcessIbfMessageResponse {
        ibf_msg: Option<RoutingVersion2>,
    },
    RequestRoutingTableResponse {
        edges_info: Vec<Edge>,
    },
}

#[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
impl RoutingTableActor {
    pub fn exchange_routing_tables_using_ibf(
        &self,
        peer_id: &PeerId,
        ibf_set: &IbfSet<SimpleEdge>,
        ibf_level: ValidIBFLevel,
        ibf_vec: &[IbfBox],
        seed: u64,
    ) -> (Vec<SimpleEdge>, Vec<u64>, u64) {
        let ibf = ibf_set.get_ibf(ibf_level);

        let mut new_ibf = Ibf::from_vec(ibf_vec.clone(), seed ^ (ibf_level.0 as u64));

        if !new_ibf.merge(&ibf.data, seed ^ (ibf_level.0 as u64)) {
            error!(target: "network", "exchange routing tables failed with peer {}", peer_id);
            return (Default::default(), Default::default(), 0);
        }

        let (edge_hashes, unknown_edges_count) = new_ibf.try_recover();
        let (known, unknown_edges) = self.split_edges_for_peer(&peer_id, &edge_hashes);

        (known, unknown_edges, unknown_edges_count)
    }
}

impl Handler<RoutingTableMessages> for RoutingTableActor {
    type Result = RoutingTableMessagesResponse;

    #[perf]
    fn handle(&mut self, msg: RoutingTableMessages, _ctx: &mut Self::Context) -> Self::Result {
        match msg {
            RoutingTableMessages::AddEdges(edges) => {
                for edge in edges.iter() {
                    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
                    {
                        let se = edge.to_simple_edge();
                        self.peer_ibf_set.add_edge(&se);
                    }
                    self.edges.insert((edge.peer0.clone(), edge.peer1.clone()), edge.clone());
                }
                RoutingTableMessagesResponse::Empty
            }
            RoutingTableMessages::RemoveEdges(edges) => {
                for edge in edges.iter() {
                    #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
                    self.peer_ibf_set.remove_edge(&edge.to_simple_edge());

                    self.edges.remove(&(edge.peer0.clone(), edge.peer1.clone()));
                }
                RoutingTableMessagesResponse::Empty
            }
            RoutingTableMessages::RequestRoutingTable => {
                RoutingTableMessagesResponse::RequestRoutingTableResponse {
                    edges_info: self.edges.iter().map(|(_k, v)| v.clone()).collect(),
                }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::AddPeerIfMissing(peer_id, ibf_set) => {
                let seed = self.peer_ibf_set.add_peer(peer_id.clone(), ibf_set, &mut self.edges);
                RoutingTableMessagesResponse::AddPeerResponse { seed }
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::RemovePeer(peer_id) => {
                self.peer_ibf_set.remove_peer(&peer_id);
                RoutingTableMessagesResponse::Empty
            }
            #[cfg(feature = "protocol_feature_routing_exchange_algorithm")]
            RoutingTableMessages::ProcessIbfMessage { peer_id, ibf_msg } => {
                match ibf_msg.routing_state {
                    RoutingState::PartialSync(partial_sync) => {
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
                                .filter_map(|x| self.edges.get(&x.key()).cloned())
                                .collect();
                            // Prepare message
                            let ibf_msg = if unknown_edges_count == 0
                                && unknown_edge_hashes.len() > 0
                            {
                                RoutingVersion2 {
                                    known_edges: self.edges.len() as u64,
                                    seed,
                                    edges: edges_for_peer,
                                    routing_state: RoutingState::RequestMissingEdges(
                                        unknown_edge_hashes,
                                    ),
                                }
                            } else if unknown_edges_count == 0 && unknown_edge_hashes.len() == 0 {
                                RoutingVersion2 {
                                    known_edges: self.edges.len() as u64,
                                    seed,
                                    edges: edges_for_peer,
                                    routing_state: RoutingState::Done,
                                }
                            } else {
                                if let Some(new_ibf_level) = partial_sync.ibf_level.inc() {
                                    let ibf_vec = ibf_set.get_ibf_vec(new_ibf_level);
                                    RoutingVersion2 {
                                        known_edges: self.edges.len() as u64,
                                        seed,
                                        edges: edges_for_peer,
                                        routing_state: RoutingState::PartialSync(PartialSync {
                                            ibf_level: new_ibf_level,
                                            ibf: ibf_vec,
                                        }),
                                    }
                                } else {
                                    RoutingVersion2 {
                                        known_edges: self.edges.len() as u64,
                                        seed,
                                        edges: self.edges.iter().map(|x| x.1.clone()).collect(),
                                        routing_state: RoutingState::RequestAllEdges,
                                    }
                                }
                            };
                            RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                                ibf_msg: Some(ibf_msg),
                            }
                        } else {
                            error!(target: "network", "Peer not found {}", peer_id);
                            RoutingTableMessagesResponse::Empty
                        }
                    }
                    RoutingState::InitializeIbf => {
                        self.peer_ibf_set.add_peer(
                            peer_id.clone(),
                            Some(ibf_msg.seed),
                            &mut self.edges,
                        );
                        if let Some(ibf_set) = self.peer_ibf_set.get(&peer_id) {
                            let seed = ibf_set.get_seed();
                            let ibf_vec = ibf_set.get_ibf_vec(MIN_IBF_LEVEL);
                            RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                                ibf_msg: Some(RoutingVersion2 {
                                    known_edges: self.edges.len() as u64,
                                    seed,
                                    edges: Default::default(),
                                    routing_state: RoutingState::PartialSync(PartialSync {
                                        ibf_level: MIN_IBF_LEVEL,
                                        ibf: ibf_vec,
                                    }),
                                }),
                            }
                        } else {
                            error!(target: "network", "Peer not found {}", peer_id);
                            RoutingTableMessagesResponse::Empty
                        }
                    }
                    RoutingState::RequestMissingEdges(requested_edges) => {
                        let seed = ibf_msg.seed;
                        let (edges_for_peer, _) =
                            self.split_edges_for_peer(&peer_id, &requested_edges);

                        let edges_for_peer = edges_for_peer
                            .iter()
                            .filter_map(|x| self.edges.get(&x.key()).cloned())
                            .collect();

                        let ibf_msg = RoutingVersion2 {
                            known_edges: self.edges.len() as u64,
                            seed,
                            edges: edges_for_peer,
                            routing_state: RoutingState::Done,
                        };
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                            ibf_msg: Some(ibf_msg),
                        }
                    }
                    RoutingState::RequestAllEdges => {
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse {
                            ibf_msg: Some(RoutingVersion2 {
                                known_edges: self.edges.len() as u64,
                                seed: ibf_msg.seed,
                                edges: self.edges.iter().map(|x| x.1.clone()).collect(),
                                routing_state: RoutingState::Done,
                            }),
                        }
                    }
                    RoutingState::Done => {
                        RoutingTableMessagesResponse::ProcessIbfMessageResponse { ibf_msg: None }
                    }
                }
            }
        }
    }
}
