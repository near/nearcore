use crate::errors::RpcError;
use near_network::routing::{Edge, SimpleEdge};
use near_primitives::network::PeerId;
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
pub struct SetRoutingTableRequest {
    pub add_edges: Option<Vec<Edge>>,
    pub remove_edges: Option<Vec<SimpleEdge>>,
    pub prune_edges: Option<bool>,
}

impl SetRoutingTableRequest {
    pub fn parse(value: Option<Value>) -> Result<Self, RpcError> {
        if let Some(value) = value {
            serde_json::from_value(value)
                .map_err(|err| RpcError::parse_error(format!("Error {:?}", err)))
        } else {
            Err(RpcError::parse_error("Require at least one parameter".to_owned()))
        }
    }
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
