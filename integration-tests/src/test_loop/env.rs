use near_async::messaging::{IntoMultiSender, IntoSender, Sender};
use near_async::test_loop::data::TestLoopDataHandle;
use near_async::test_loop::sender::TestLoopSender;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::client_actor::ClientActorInner;
use near_client::PartialWitnessActor;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::PartialWitnessSenderForNetwork;
use near_network::test_loop::ClientSenderForTestLoopNetwork;
use near_primitives::types::AccountId;
use nearcore::state_sync::StateSyncDumper;

const NETWORK_DELAY: Duration = Duration::milliseconds(10);

pub struct TestLoopEnv {
    pub test_loop: TestLoopV2,
    pub datas: Vec<TestData>,
}

pub struct TestData {
    pub account_id: AccountId,
    pub client_sender: TestLoopSender<ClientActorInner>,
    pub shards_manager_sender: TestLoopSender<ShardsManagerActor>,
    pub partial_witness_sender: TestLoopSender<PartialWitnessActor>,
    pub state_sync_dumper_handle: TestLoopDataHandle<StateSyncDumper>,
}

impl From<&TestData> for AccountId {
    fn from(data: &TestData) -> AccountId {
        data.account_id.clone()
    }
}

impl From<&TestData> for ClientSenderForTestLoopNetwork {
    fn from(data: &TestData) -> ClientSenderForTestLoopNetwork {
        data.client_sender.clone().with_delay(NETWORK_DELAY).into_multi_sender()
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
