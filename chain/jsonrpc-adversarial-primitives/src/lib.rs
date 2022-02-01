#[cfg(feature = "ser_de")]
use near_jsonrpc_primitives::errors::RpcError;
use near_network_primitives::types::{Edge, SimpleEdge};
use near_primitives::network::PeerId;
#[cfg(feature = "ser_de")]
use serde::Deserialize;
#[cfg(feature = "ser_de")]
use serde_json::Value;

#[cfg_attr(feature = "ser_de", derive(Deserialize))]
#[cfg_attr(feature = "deepsize_feature", derive(deepsize::DeepSizeOf))]
pub struct SetRoutingTableRequest {
    pub add_edges: Option<Vec<Edge>>,
    pub remove_edges: Option<Vec<SimpleEdge>>,
    pub prune_edges: Option<bool>,
}

#[cfg(feature = "ser_de")]
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

#[cfg_attr(feature = "ser_de", derive(Deserialize))]
pub struct SetAdvOptionsRequest {
    pub disable_edge_signature_verification: Option<bool>,
    pub disable_edge_propagation: Option<bool>,
    pub disable_edge_pruning: Option<bool>,
}

#[cfg_attr(feature = "ser_de", derive(Deserialize))]
pub struct StartRoutingTableSyncRequest {
    pub peer_id: PeerId,
}
