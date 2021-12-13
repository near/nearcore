use near_network_primitives::types::{KnownProducer, PeerInfo};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcNetworkInfoResponse {
    pub active_peers: Vec<PeerInfo>,
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcNetworkInfoError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
}

impl From<near_client_primitives::types::NetworkInfoResponse> for RpcNetworkInfoResponse {
    fn from(network_info_response: near_client_primitives::types::NetworkInfoResponse) -> Self {
        Self {
            active_peers: network_info_response.connected_peers,
            num_active_peers: network_info_response.num_connected_peers,
            peer_max_count: network_info_response.peer_max_count,
            sent_bytes_per_sec: network_info_response.sent_bytes_per_sec,
            received_bytes_per_sec: network_info_response.received_bytes_per_sec,
            known_producers: network_info_response.known_producers,
        }
    }
}

impl From<actix::MailboxError> for RpcNetworkInfoError {
    fn from(error: actix::MailboxError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl From<String> for RpcNetworkInfoError {
    fn from(error_message: String) -> Self {
        Self::InternalError { error_message }
    }
}

impl From<RpcNetworkInfoError> for crate::errors::RpcError {
    fn from(error: RpcNetworkInfoError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcNetworkInfoError: {:?}", err),
                )
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}
