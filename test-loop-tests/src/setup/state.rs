use std::mem;
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
pub struct SharedState {
    pub genesis: Genesis,
    pub tempdir: TempDir,
    pub epoch_config_store: EpochConfigStore,
    pub runtime_config_store: Option<RuntimeConfigStore>,
    pub network_shared_state: TestLoopNetworkSharedState,
    pub upgrade_schedule: ProtocolUpgradeVotingSchedule,
    pub chunks_storage: Arc<Mutex<TestLoopChunksStorage>>,
    pub drop_condition_kinds: Vec<DropConditionKind>,
    pub load_memtries_for_tracked_shards: bool,
    pub warmup_pending: Arc<AtomicBool>,
}

impl SharedState {
    /// Function to move the tempdir out of the shared state.
    /// We can probably get rid of this once epoch_sync tests move to not building a new TestLoop env.
    pub fn move_tempdir(&mut self) -> TempDir {
        let temp_dir = mem::replace(&mut self.tempdir, tempfile::tempdir().unwrap());
        temp_dir
    }
}

pub struct NodeState {
    pub account_id: AccountId,
    pub client_config: ClientConfig,
    pub store: Store,
    pub split_store: Option<Store>,
}

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
