use near_network_primitives::types::{KnownProducer, PeerInfo};
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcPeerInfo {
    pub id: PeerId,
    pub addr: Option<SocketAddr>,
    pub account_id: Option<AccountId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcKnownProducer {
    pub account_id: AccountId,
    pub addr: Option<SocketAddr>,
    pub peer_id: PeerId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcNetworkInfoResponse {
    pub active_peers: Vec<RpcPeerInfo>,
    pub num_active_peers: usize,
    pub peer_max_count: u32,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<RpcKnownProducer>,
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcNetworkInfoError {
    #[error("Internal error: {error_message}")]
    InternalError { error_message: String },
}

impl From<PeerInfo> for RpcPeerInfo {
    fn from(peer_info: PeerInfo) -> Self {
        Self { id: peer_info.id, addr: peer_info.addr, account_id: peer_info.account_id }
    }
}

impl From<KnownProducer> for RpcKnownProducer {
    fn from(known_producer: KnownProducer) -> Self {
        Self {
            account_id: known_producer.account_id,
            addr: known_producer.addr,
            peer_id: known_producer.peer_id,
        }
    }
}

impl From<near_client_primitives::types::NetworkInfoResponse> for RpcNetworkInfoResponse {
    fn from(network_info_response: near_client_primitives::types::NetworkInfoResponse) -> Self {
        Self {
            active_peers: network_info_response
                .connected_peers
                .iter()
                .map(|pi| pi.clone().into())
                .collect(),
            num_active_peers: network_info_response.num_connected_peers,
            peer_max_count: network_info_response.peer_max_count,
            sent_bytes_per_sec: network_info_response.sent_bytes_per_sec,
            received_bytes_per_sec: network_info_response.received_bytes_per_sec,
            known_producers: network_info_response
                .known_producers
                .iter()
                .map(|kp| kp.clone().into())
                .collect(),
        }
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
