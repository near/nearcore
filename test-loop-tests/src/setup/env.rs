use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use std::sync::atomic::Ordering;
use tempfile::TempDir;

use super::drop_condition::DropCondition;
use super::state::{SharedState, TestData};

pub struct TestLoopEnv {
    pub test_loop: TestLoopV2,
    pub node_datas: Vec<TestData>,
    pub shared_state: SharedState,
}

impl TestLoopEnv {
    /// The function is used to add a new network drop condition to the test loop environment.
    /// While adding a new drop_condition, we iterate through all the nodes and register the
    /// drop_condition with the node's peer_manager_actor.
    ///
    /// Additionally, we store the drop_condition in the shared_state.
    /// While adding a new node to the environment, we can iterate through all the drop_conditions
    /// and register them with the new node's peer_manager_actor.
    pub fn drop(mut self, drop_condition: DropCondition) -> Self {
        for data in self.node_datas.iter() {
            data.register_drop_condition(
                &mut self.test_loop.data,
                self.shared_state.chunks_storage.clone(),
                &drop_condition,
            );
        }
        self.shared_state.drop_conditions.push(drop_condition);
        self
    }

    /// Reach block with height `genesis_height + 3`. Check that it can be done
    /// within 5 seconds. Ensure that all clients have block
    /// `genesis_height + 2` and it has all chunks.
    /// Needed because for smaller heights blocks may not get all chunks and/or
    /// approvals.
    pub fn warmup(self) -> Self {
        let Self { mut test_loop, node_datas: datas, shared_state } = self;

        // This may happen if you're calling warmup twice or have set skip_warmup in builder.
        assert!(shared_state.warmup_pending.load(Ordering::Relaxed), "warmup already done");
        shared_state.warmup_pending.store(false, Ordering::Relaxed);

        let client_handle = datas[0].client_sender.actor_handle();
        let genesis_height = test_loop.data.get(&client_handle).client.chain.genesis().height();
        test_loop.run_until(
            |test_loop_data| {
                let client_actor = test_loop_data.get(&client_handle);
                client_actor.client.chain.head().unwrap().height == genesis_height + 4
            },
            Duration::seconds(5),
        );
        for idx in 0..datas.len() {
            let client_handle = datas[idx].client_sender.actor_handle();
            let event = move |test_loop_data: &mut TestLoopData| {
                let client_actor = test_loop_data.get(&client_handle);
                let block =
                    client_actor.client.chain.get_block_by_height(genesis_height + 3).unwrap();
                let num_shards = block.header().chunk_mask().len();
                assert_eq!(block.header().chunk_mask(), vec![true; num_shards]);
            };
            test_loop.send_adhoc_event("assertions".to_owned(), Box::new(event));
        }
        test_loop.run_instant();
        Self { test_loop, node_datas: datas, shared_state }
    }

    pub fn kill_node(&mut self, identifier: &str) {
        self.test_loop.remove_events_with_identifier(identifier);
    }

    /// Used to finish off remaining events that are still in the loop. This can be necessary if the
    /// destructor of some components wait for certain condition to become true. Otherwise, the
    /// destructors may end up waiting forever. This also helps avoid a panic when destructing
    /// TestLoop itself, as it asserts that all events have been handled.
    ///
    /// Returns the test loop data dir, if the caller wishes to reuse it for another test loop.
    pub fn shutdown_and_drain_remaining_events(mut self, timeout: Duration) -> TempDir {
        // State sync dumper is not an Actor, handle stopping separately.
        for node_data in self.node_datas {
            self.test_loop.data.get_mut(&node_data.state_sync_dumper_handle).stop();
        }

        self.test_loop.shutdown_and_drain_remaining_events(timeout);
        self.shared_state.tempdir
    }
}
