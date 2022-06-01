use near_network_primitives::types::{Edge, SimpleEdge};
use near_primitives::network::PeerId;
use serde::Deserialize;

#[cfg_attr(feature = "test_features", derive(Deserialize))]
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
pub struct SetRoutingTableRequest {
    pub add_edges: Option<Vec<Edge>>,
    pub remove_edges: Option<Vec<SimpleEdge>>,
    pub prune_edges: Option<bool>,
}

#[derive(Deserialize)]
pub struct SetAdvOptionsRequest {
    pub disable_edge_signature_verification: Option<bool>,
    pub disable_edge_propagation: Option<bool>,
    pub disable_edge_pruning: Option<bool>,
}

#[derive(Deserialize)]
pub struct StartRoutingTableSyncRequest {
    pub peer_id: PeerId,
}
