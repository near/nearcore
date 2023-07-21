#[cfg(feature = "debug_types")]
use near_client_primitives::debug::{
    DebugBlockStatusData, EpochInfoView, TrackedShardsView, ValidatorStatus,
};
#[cfg(feature = "debug_types")]
use near_primitives::views::{
    CatchupStatusView, ChainProcessingInfo, NetworkGraphView, NetworkRoutesView, PeerStoreView,
    RecentOutboundConnectionsView, RequestedStatePartsView, SyncStatusView,
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcStatusResponse {
    #[serde(flatten)]
    pub status_response: near_primitives::views::StatusResponse,
}

#[cfg(feature = "debug_types")]
#[derive(serde::Serialize, Debug)]
pub enum DebugStatusResponse {
    SyncStatus(SyncStatusView),
    CatchupStatus(Vec<CatchupStatusView>),
    TrackedShards(TrackedShardsView),
    // List of epochs - in descending order (next epoch is first).
    EpochInfo(Vec<EpochInfoView>),
    // Detailed information about blocks.
    BlockStatus(DebugBlockStatusData),
    // Detailed information about the validator (approvals, block & chunk production etc.)
    ValidatorStatus(ValidatorStatus),
    PeerStore(PeerStoreView),
    ChainProcessingStatus(ChainProcessingInfo),
    // The state parts already requested.
    RequestedStateParts(Vec<RequestedStatePartsView>),
    NetworkGraph(NetworkGraphView),
    RecentOutboundConnections(RecentOutboundConnectionsView),
    Routes(NetworkRoutesView),
}

#[cfg(feature = "debug_types")]
#[derive(Debug, serde::Serialize)]
pub struct RpcDebugStatusResponse {
    pub status_response: DebugStatusResponse,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RpcHealthResponse;

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcStatusError {
    #[error("Node is syncing")]
    NodeIsSyncing,
    #[error("No blocks for {elapsed:?}")]
    NoNewBlocks { elapsed: std::time::Duration },
    #[error("Epoch Out Of Bounds {epoch_id:?}")]
    EpochOutOfBounds { epoch_id: near_primitives::types::EpochId },
    #[error("The node reached its limits. Try again later. More details: {error_message}")]
    InternalError { error_message: String },
}

impl From<RpcStatusError> for crate::errors::RpcError {
    fn from(error: RpcStatusError) -> Self {
        let error_data = match serde_json::to_value(error) {
            Ok(value) => value,
            Err(err) => {
                return Self::new_internal_error(
                    None,
                    format!("Failed to serialize RpcStateChangesError: {:?}", err),
                )
            }
        };
        Self::new_internal_or_handler_error(Some(error_data.clone()), error_data)
    }
}
