//! Mock-side delayed sender adapters for `TestLoopPeerManagerActor`.
//!
//! Each `From<&NodeExecutionData>` impl wraps the destination actor's
//! `TestLoopSender` with `with_delay(NETWORK_DELAY)` so messages routed
//! through the mock PMA observe a uniform 10ms transit delay. The mock
//! consumes these inside its `*ForTestLoopNetwork` typed senders.
//!
//! The real-PMA path applies network delay at the transport layer
//! instead (see `setup/testloop_transport/transport.rs::NETWORK_DELAY`),
//! and uses un-delayed senders directly from `NodeExecutionData`.

use super::peer_manager_actor::{
    ChunkEndorsementSenderForTestLoopNetwork, ClientSenderForTestLoopNetwork,
    SpiceDataDistributorSenderForTestLoopNetwork, TestLoopNetworkBlockInfo,
    TxRequestHandleSenderForTestLoopNetwork, ViewClientSenderForTestLoopNetwork,
};
use crate::setup::state::NodeExecutionData;
use near_async::messaging::{IntoMultiSender, IntoSender, Sender};
use near_async::time::Duration;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::PartialWitnessSenderForNetwork;
use near_network::types::StateRequestSenderForNetwork;

pub const NETWORK_DELAY: Duration = Duration::milliseconds(10);

impl From<&NodeExecutionData> for ClientSenderForTestLoopNetwork {
    fn from(data: &NodeExecutionData) -> ClientSenderForTestLoopNetwork {
        data.client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&NodeExecutionData> for ViewClientSenderForTestLoopNetwork {
    fn from(data: &NodeExecutionData) -> ViewClientSenderForTestLoopNetwork {
        data.view_client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&NodeExecutionData> for StateRequestSenderForNetwork {
    fn from(data: &NodeExecutionData) -> StateRequestSenderForNetwork {
        data.state_request_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&NodeExecutionData> for PartialWitnessSenderForNetwork {
    fn from(data: &NodeExecutionData) -> PartialWitnessSenderForNetwork {
        data.partial_witness_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&NodeExecutionData> for Sender<ShardsManagerRequestFromNetwork> {
    fn from(data: &NodeExecutionData) -> Sender<ShardsManagerRequestFromNetwork> {
        data.shards_manager_sender.clone().with_delay(NETWORK_DELAY).into_sender()
    }
}

impl From<&NodeExecutionData> for TxRequestHandleSenderForTestLoopNetwork {
    fn from(data: &NodeExecutionData) -> TxRequestHandleSenderForTestLoopNetwork {
        data.rpc_handler_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&NodeExecutionData> for ChunkEndorsementSenderForTestLoopNetwork {
    fn from(data: &NodeExecutionData) -> ChunkEndorsementSenderForTestLoopNetwork {
        data.chunk_endorsement_handler_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&NodeExecutionData> for Sender<TestLoopNetworkBlockInfo> {
    fn from(data: &NodeExecutionData) -> Sender<TestLoopNetworkBlockInfo> {
        data.legacy_mock_pma_sender
            .as_ref()
            .expect("legacy_mock_pma_sender required for mock PMA block-info push")
            .clone()
            .with_delay(NETWORK_DELAY)
            .into_sender()
    }
}

impl From<&NodeExecutionData> for SpiceDataDistributorSenderForTestLoopNetwork {
    fn from(data: &NodeExecutionData) -> SpiceDataDistributorSenderForTestLoopNetwork {
        data.spice_data_distributor_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&NodeExecutionData> for Sender<SpiceChunkEndorsementMessage> {
    fn from(data: &NodeExecutionData) -> Sender<SpiceChunkEndorsementMessage> {
        data.spice_core_writer_sender.clone().with_delay(NETWORK_DELAY).into_sender()
    }
}
