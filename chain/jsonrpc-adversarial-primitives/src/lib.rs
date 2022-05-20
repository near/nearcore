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

#[cfg(feature = "test_features")]
impl SetRoutingTableRequest {
    pub fn parse(
        value: Option<serde_json::Value>,
    ) -> Result<Self, near_jsonrpc_primitives::errors::RpcError> {
        if let Some(value) = value {
            serde_json::from_value(value).map_err(|err| {
                near_jsonrpc_primitives::errors::RpcError::parse_error(format!("Error {:?}", err))
            })
        } else {
            Err(near_jsonrpc_primitives::errors::RpcError::parse_error(
                "Require at least one parameter".to_owned(),
            ))
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
