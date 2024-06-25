use near_async::messaging::{IntoMultiSender, IntoSender, Sender};
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
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

impl TestLoopEnv {
    /// Reach block with height `genesis_height + 3`. Check that it can be done
    /// within 5 seconds. Ensure that all clients have block
    /// `genesis_height + 2` and it has all chunks.
    /// Needed because for smaller heights blocks may not get all chunks and/or
    /// approvals.
    pub fn warmup(self) -> Self {
        let Self { mut test_loop, datas } = self;

        let client_handle = datas[0].client_sender.actor_handle();
        let genesis_height = test_loop.data.get(&client_handle).client.chain.genesis().height();
        test_loop.run_until(
            |test_loop_data| {
                let client_actor = test_loop_data.get(&client_handle);
                client_actor.client.chain.head().unwrap().height == genesis_height + 3
            },
            Duration::seconds(5),
        );
        for idx in 0..datas.len() {
            let client_handle = datas[idx].client_sender.actor_handle();
            let event = move |test_loop_data: &mut TestLoopData| {
                let client_actor = test_loop_data.get(&client_handle);
                let block =
                    client_actor.client.chain.get_block_by_height(genesis_height + 2).unwrap();
                let num_shards = block.header().chunk_mask().len();
                assert_eq!(block.header().chunk_mask(), vec![true; num_shards]);
            };
            test_loop.send_adhoc_event("assertions".to_owned(), Box::new(event));
        }
        test_loop.run_instant();

        Self { test_loop, datas }
    }

    /// Used to finish off remaining events that are still in the loop. This can be necessary if the
    /// destructor of some components wait for certain condition to become true. Otherwise, the
    /// destructors may end up waiting forever. This also helps avoid a panic when destructing
    /// TestLoop itself, as it asserts that all events have been handled.
    pub fn shutdown_and_drain_remaining_events(mut self, timeout: Duration) {
        // State sync dumper is not an Actor, handle stopping separately.
        for node_data in self.datas {
            self.test_loop.data.get_mut(&node_data.state_sync_dumper_handle).stop();
        }

        self.test_loop.shutdown_and_drain_remaining_events(timeout);
    }
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
