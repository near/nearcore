use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use near_async::messaging::{IntoMultiSender, IntoSender, Sender};
use near_async::test_loop::data::TestLoopDataHandle;
use near_async::test_loop::sender::TestLoopSender;
use near_async::time::Duration;
use near_chain_configs::{ClientConfig, Genesis};
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::client_actor::ClientActorInner;
use near_client::{PartialWitnessActor, ViewClientActorInner};
use near_jsonrpc::ViewClientSenderForRpc;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::PartialWitnessSenderForNetwork;
use near_network::test_loop::{
    ClientSenderForTestLoopNetwork, TestLoopNetworkSharedState, TestLoopPeerManagerActor,
    ViewClientSenderForTestLoopNetwork,
};
use near_parameters::RuntimeConfigStore;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::network::PeerId;
use near_primitives::types::AccountId;
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_store::Store;
use nearcore::state_sync::StateSyncDumper;
use tempfile::TempDir;

use super::builder::DropConditionKind;
use super::env::TestLoopChunksStorage;

const NETWORK_DELAY: Duration = Duration::milliseconds(10);

/// This is the state associate with the test loop environment.
/// This state is shared across all nodes and none of it belongs to a specific node.
pub struct SharedState {
    pub genesis: Genesis,
    /// Directory of the current test. This is automatically deleted once tempdir goes out of scope.
    pub tempdir: TempDir,
    pub epoch_config_store: EpochConfigStore,
    pub runtime_config_store: Option<RuntimeConfigStore>,
    /// Shared state across all the network actors. It handles the mapping between AccountId,
    /// PeerId, and the route back CryptoHash, so that individual network actors can do routing.
    pub network_shared_state: TestLoopNetworkSharedState,
    pub upgrade_schedule: ProtocolUpgradeVotingSchedule,
    /// Stores all chunks ever observed on chain. Used by drop conditions to simulate network drops.
    pub chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    /// List of drop conditions that apply to all nodes in the network.
    pub drop_condition_kinds: Vec<DropConditionKind>,
    pub load_memtries_for_tracked_shards: bool,
    /// Flag to indicate if warmup is pending. This is used to ensure that warmup is only done once.
    pub warmup_pending: Arc<AtomicBool>,
}

/// This is the state associated with each node in the test loop environment before being built.
/// The setup_client function will be called for each node to build the node and return TestData
pub struct NodeState {
    pub account_id: AccountId,
    pub client_config: ClientConfig,
    pub store: Store,
    pub split_store: Option<Store>,
}

/// This is the state associated with each node in the test loop environment after being built.
/// This state is specific to each node and is not shared across nodes.
/// We can access each of the individual actors and senders from this state.
#[derive(Clone)]
pub struct TestData {
    pub account_id: AccountId,
    pub peer_id: PeerId,
    pub client_sender: TestLoopSender<ClientActorInner>,
    pub view_client_sender: TestLoopSender<ViewClientActorInner>,
    pub shards_manager_sender: TestLoopSender<ShardsManagerActor>,
    pub partial_witness_sender: TestLoopSender<PartialWitnessActor>,
    pub peer_manager_sender: TestLoopSender<TestLoopPeerManagerActor>,
    pub state_sync_dumper_handle: TestLoopDataHandle<StateSyncDumper>,
}

impl From<&TestData> for AccountId {
    fn from(data: &TestData) -> AccountId {
        data.account_id.clone()
    }
}

impl From<&TestData> for PeerId {
    fn from(data: &TestData) -> PeerId {
        data.peer_id.clone()
    }
}

impl From<&TestData> for ClientSenderForTestLoopNetwork {
    fn from(data: &TestData) -> ClientSenderForTestLoopNetwork {
        data.client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&TestData> for ViewClientSenderForRpc {
    fn from(data: &TestData) -> ViewClientSenderForRpc {
        data.view_client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&TestData> for ViewClientSenderForTestLoopNetwork {
    fn from(data: &TestData) -> ViewClientSenderForTestLoopNetwork {
        data.view_client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&TestData> for PartialWitnessSenderForNetwork {
    fn from(data: &TestData) -> PartialWitnessSenderForNetwork {
        data.partial_witness_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
    }
}

impl From<&TestData> for Sender<ShardsManagerRequestFromNetwork> {
    fn from(data: &TestData) -> Sender<ShardsManagerRequestFromNetwork> {
        data.shards_manager_sender.clone().with_delay(NETWORK_DELAY).into_sender()
    }
}
