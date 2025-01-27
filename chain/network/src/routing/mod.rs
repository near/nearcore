mod bfs;
pub(crate) mod edge;
#[cfg(feature = "distance_vector_routing")]
mod edge_cache;
mod graph;
#[cfg(feature = "distance_vector_routing")]
mod graph_v2;
pub(crate) mod route_back_cache;
pub mod routing_table_view;

pub(crate) use graph::{DistanceTable, Graph, GraphConfig, NextHopTable};
#[cfg(feature = "distance_vector_routing")]
pub(crate) use graph_v2::{GraphConfigV2, GraphV2, NetworkTopologyChange};
