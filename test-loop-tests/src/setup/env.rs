use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_client::SetNetworkInfo;
use near_network::types::NetworkInfo;
use near_store::adapter::StoreAdapter;
use std::sync::atomic::Ordering;

use super::builder::setup_client;
use super::drop_condition::DropCondition;
use super::state::{NodeState, SharedState, TestData};

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

    /// Function to stop a node in test loop environment.
    /// Calling this function immediately stops all events with the given identifier.
    /// This function returns the NodeState of the stopped node which can be used to restart the node.
    ///
    /// Note that other nodes may still continue to queue network events into the peer
    /// manager actor of the stopped node but this would not be processed.
    pub fn kill_node(&mut self, identifier: &str) -> NodeState {
        // Make test_loop ignore all events from this node.
        self.test_loop.remove_events_with_identifier(identifier);

        // Build node_state
        let node_data = self
            .node_datas
            .iter()
            .find(|data| data.identifier == identifier)
            .expect("Node with identifier not found");

        let account_id = node_data.account_id.clone();
        let client_actor = self.test_loop.data.get(&node_data.client_sender.actor_handle());
        let client_config = client_actor.client.config.clone();
        let store = client_actor.client.chain.chain_store.store();
        let split_store = if client_config.archive {
            let view_client_actor =
                self.test_loop.data.get(&node_data.view_client_sender.actor_handle());
            Some(view_client_actor.chain.chain_store.store())
        } else {
            None
        };

        NodeState { account_id, client_config, store, split_store }
    }

    /// Function to restart a node in test loop environment. This function takes in the new_identifier
    /// and node_state of the stopped node as input.
    ///
    /// As long as the account_id in the node_state matches the account_id of the stopped node,
    /// this function automatically takes care of properly redirecting all network messages to the new node.
    ///
    /// Additionally, we set the NetworkInfo for this node which is required for state sync to work.
    pub fn restart_node(&mut self, new_identifier: &str, node_state: NodeState) {
        // get the HeightHeightPeerInfo from all nodes
        let highest_height_peers = self
            .node_datas
            .iter()
            .filter(|data| data.account_id != node_state.account_id)
            .map(|data| data.get_highest_height_peer_info(&self.test_loop.data))
            .collect_vec();

        // setup_client handles adding the account_id and peer_id details to network_shared_state
        let node_data =
            setup_client(new_identifier, &mut self.test_loop, node_state, &self.shared_state);

        // Note: TestLoopEnv does not currently propagate the network info to other peers. This is because
        // the networking layer is completely mocked out. So in order to allow the new node to sync, we
        // need to manually propagate the network info to the new node.
        node_data
            .client_sender
            .send(SetNetworkInfo(NetworkInfo { highest_height_peers, ..NetworkInfo::default() }));

        // Finally push node_data into node_datas
        self.node_datas.push(node_data);
    }

    /// Function to add a new node in test loop environment. This function takes in the identifier
    /// and node_state of the new node as input.
    ///
    /// We set the NetworkInfo for this node which is required for state sync to work.
    pub fn add_node(&mut self, identifier: &str, node_state: NodeState) {
        // Logically this function is the same as restart_node
        self.restart_node(identifier, node_state);
    }

    /// Used to finish off remaining events that are still in the loop. This can be necessary if the
    /// destructor of some components wait for certain condition to become true. Otherwise, the
    /// destructors may end up waiting forever. This also helps avoid a panic when destructing
    /// TestLoop itself, as it asserts that all events have been handled.
    pub fn shutdown_and_drain_remaining_events(mut self, timeout: Duration) {
        // State sync dumper is not an Actor, handle stopping separately.
        for node_data in self.node_datas {
            self.test_loop.data.get_mut(&node_data.state_sync_dumper_handle).stop();
        }

        self.test_loop.shutdown_and_drain_remaining_events(timeout);
    }
}
