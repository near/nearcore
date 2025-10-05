use super::{RpcFrom, RpcInto};
use near_async::messaging::AsyncSendError;
use near_client_primitives::types::{NetworkInfoResponse, PeerInfo};
use near_jsonrpc_primitives::types::network_info::{
    RpcNetworkInfoError, RpcNetworkInfoResponse, RpcPeerInfo,
};

impl RpcFrom<AsyncSendError> for RpcNetworkInfoError {
    fn rpc_from(error: AsyncSendError) -> Self {
        Self::InternalError { error_message: error.to_string() }
    }
}

impl RpcFrom<PeerInfo> for RpcPeerInfo {
    fn rpc_from(peer_info: PeerInfo) -> Self {
        Self { id: peer_info.id, addr: peer_info.addr, account_id: peer_info.account_id }
    }
}

impl RpcFrom<NetworkInfoResponse> for RpcNetworkInfoResponse {
    fn rpc_from(network_info_response: NetworkInfoResponse) -> Self {
        Self {
            active_peers: network_info_response
                .connected_peers
                .iter()
                .map(|pi| pi.clone().rpc_into())
                .collect(),
            num_active_peers: network_info_response.num_connected_peers,
            peer_max_count: network_info_response.peer_max_count,
            sent_bytes_per_sec: network_info_response.sent_bytes_per_sec,
            received_bytes_per_sec: network_info_response.received_bytes_per_sec,
        }
    }
}

impl RpcFrom<String> for RpcNetworkInfoError {
    fn rpc_from(error_message: String) -> Self {
        Self::InternalError { error_message }
    }
}
