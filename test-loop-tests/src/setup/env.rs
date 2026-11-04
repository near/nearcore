use super::builder::NodeStateBuilder;
use super::drop_condition::DropCondition;
use super::network_rules::{Action, RuleHandle};
use super::setup::setup_client;
use super::state::{NodeExecutionData, NodeSetupState, SharedState};
use crate::utils::account::{archival_account_id, rpc_account_id};
use crate::utils::network::chunk_parts_matcher;
use crate::utils::node::{NodeRunner, TestLoopNode, TestLoopNodeMut};
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_network::types::NetworkRequests;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::types::AccountId;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use near_store::archive::cloud_storage::CloudStorage;
use near_store::db::ColdDB;
use near_store::test_utils::TestNodeStorage;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub struct TestLoopEnv {
    pub test_loop: TestLoopV2,
    pub node_datas: Vec<NodeExecutionData>,
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
        self.add_drop_condition(drop_condition);
        self
    }

    /// Same as [`Self::drop`], but borrows the environment and returns the condition's
    /// handle: one switch over its rule on every account, the accounts of nodes added later
    /// included, whose count covers them all. Lifting it ends the condition everywhere.
    pub fn add_drop_condition(&mut self, drop_condition: DropCondition) -> RuleHandle {
        let handle = RuleHandle::default();
        // Rules are keyed by account and a restarted node is a new entry with the account of
        // the old one, so each account gets the rule once, through its latest entry.
        let mut seen = HashSet::new();
        for data in self.node_datas.iter().rev() {
            if !seen.insert(&data.account_id) {
                continue;
            }
            data.register_drop_condition(
                &self.test_loop.data,
                self.shared_state.chunks_storage.clone(),
                &self.shared_state.network_rules,
                &drop_condition,
                &handle,
            );
        }
        self.shared_state.drop_conditions.push((drop_condition, handle.clone()));
        handle
    }

    /// From now on `action` applies to every request node `index` sends that `matches`, until
    /// the handle is lifted. The rule is keyed by the node's account, so it stays on a node
    /// that is killed and restarted. See [`super::network_rules`].
    pub fn network_rule(
        &self,
        index: usize,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
        action: Action,
    ) -> RuleHandle {
        self.network_rule_for_account(&self.node_datas[index].account_id, matches, action)
    }

    /// [`Self::network_rule`] for the node of `account_id`.
    pub fn network_rule_for_account(
        &self,
        account_id: &AccountId,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
        action: Action,
    ) -> RuleHandle {
        self.shared_state.network_rules.add(account_id.clone(), matches, action)
    }

    /// [`Self::network_rule`] under an existing handle, shared with the rules added under it
    /// before: one switch over the same rule on several nodes, or a rule whose action lifts
    /// its own handle once it has done its job.
    pub fn network_rule_with_handle(
        &self,
        index: usize,
        handle: &RuleHandle,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
        action: Action,
    ) {
        let account_id = self.node_datas[index].account_id.clone();
        self.shared_state.network_rules.add_with_handle(handle, account_id, None, matches, action);
    }

    /// [`Self::network_rule`] on the messages that carry the parts of the chunks `selects`: the
    /// parts a producer pushes, the parts owners forward, and the request and response pair a
    /// node falls back to. See [`crate::utils::network::chunk_parts_matcher`].
    pub fn chunk_parts_rule(
        &self,
        index: usize,
        selects: impl Fn(&ShardChunkHeader) -> bool + Send + Sync + 'static,
        action: Action,
    ) -> RuleHandle {
        let matches = chunk_parts_matcher(self.shared_state.chunks_storage.clone(), selects);
        self.network_rule(index, matches, action)
    }

    /// From now on `observe` reports every request node `index` sends that `matches`, as
    /// the node sent it.
    pub fn observe_messages_from(
        &self,
        index: usize,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
        observe: impl Fn(&NetworkRequests) + Send + Sync + 'static,
    ) -> RuleHandle {
        let account_id = self.node_datas[index].account_id.clone();
        self.shared_state.network_rules.observe_first(account_id, matches, observe)
    }

    /// From now on node `index` drops every request it sends that `matches`.
    pub fn drop_messages_from(
        &self,
        index: usize,
        matches: impl Fn(&NetworkRequests) -> bool + Send + Sync + 'static,
    ) -> RuleHandle {
        self.network_rule(index, matches, Action::Drop)
    }

    /// Reach block with height `genesis_height + 3`. Check that it can be done
    /// within 5 seconds. Ensure that all clients have block
    /// `genesis_height + 2` and it has all chunks.
    /// Needed because for smaller heights blocks may not get all chunks and/or
    /// approvals.
    pub fn warmup(mut self) -> Self {
        // This may happen if you're calling warmup twice or have set skip_warmup in builder.
        assert!(self.shared_state.warmup_pending.load(Ordering::Relaxed), "warmup already done");
        self.shared_state.warmup_pending.store(false, Ordering::Relaxed);

        let client = self.node(0).client();
        let max_block_production_delay = client.config.max_block_production_delay.get();
        let genesis_height = client.chain.genesis().height();
        let client_handle = self.node_datas[0].client_sender.actor_handle();
        self.test_loop.run_until(
            |test_loop_data| {
                let client_actor = test_loop_data.get(&client_handle);
                client_actor.client.chain.head().unwrap().height == genesis_height + 4
            },
            max_block_production_delay * 5,
        );
        for idx in 0..self.node_datas.len() {
            let client_handle = self.node_datas[idx].client_sender.actor_handle();
            let event = move |test_loop_data: &mut TestLoopData| {
                let client_actor = test_loop_data.get(&client_handle);
                let block =
                    client_actor.client.chain.get_block_by_height(genesis_height + 3).unwrap();
                let num_shards = block.header().chunk_mask().len();
                assert_eq!(block.header().chunk_mask(), vec![true; num_shards]);
            };
            self.test_loop.send_adhoc_event("assertions".to_owned(), Box::new(event));
        }
        self.test_loop.run_instant();
        self
    }

    /// Function to stop a node in test loop environment.
    /// Calling this function immediately stops all events with the given identifier.
    /// This function returns the NodeState of the stopped node which can be used to restart the node.
    ///
    /// Note that other nodes may still continue to queue network events into the peer
    /// manager actor of the stopped node but this would not be processed.
    pub fn kill_node(&self, identifier: &str) -> NodeSetupState {
        // Make test_loop ignore all events from this node.
        self.test_loop.event_denylist().lock().push(identifier.to_string());

        // Build node_state
        let node_data = self
            .node_datas
            .iter()
            .find(|data| data.identifier == identifier)
            .expect("Node with identifier not found");

        let account_id = node_data.account_id.clone();
        let client_actor = self.test_loop.data.get(&node_data.client_sender.actor_handle());
        let client_config = client_actor.client.config.clone();
        let hot_store = client_actor.client.chain.chain_store.store();

        let (split_store, cold_db) = match self.get_split_store_and_cold_db(node_data) {
            Some((split_store, cold_db)) => (Some(split_store), Some(cold_db)),
            None => (None, None),
        };
        let cloud_storage = self.get_cloud_storage(node_data);
        let storage = TestNodeStorage { hot_store, split_store, cold_db, cloud_storage };

        NodeSetupState { account_id, client_config, storage, validator_signer: None }
    }

    fn get_split_store_and_cold_db(
        &self,
        node_data: &NodeExecutionData,
    ) -> Option<(Store, Arc<ColdDB>)> {
        let Some(cold_store_sender) = &node_data.cold_store_sender else {
            return None;
        };
        let cold_store_actor = self.test_loop.data.get(&cold_store_sender.actor_handle());
        let cold_db = cold_store_actor.get_cold_db();
        let view_client_actor =
            self.test_loop.data.get(&node_data.view_client_sender.actor_handle());
        let split_store = view_client_actor.chain.chain_store.store();
        Some((split_store, cold_db))
    }

    fn get_cloud_storage(&self, node_data: &NodeExecutionData) -> Option<Arc<CloudStorage>> {
        let cloud_storage = self.test_loop.data.get(&node_data.cloud_storage_sender);
        cloud_storage.clone()
    }

    /// Function to restart a node in test loop environment. This function takes in the new_identifier
    /// and node_state of the stopped node as input.
    ///
    /// As long as the account_id in the node_state matches the account_id of the stopped node,
    /// this function automatically takes care of properly redirecting all network messages to the new node.
    ///
    /// Additionally, we set the NetworkInfo for this node which is required for state sync to work.
    ///
    /// Returns the index of the new node in `node_datas`. The stopped node keeps its own entry.
    pub fn restart_node(&mut self, new_identifier: &str, node_state: NodeSetupState) -> usize {
        // setup_client handles adding the account_id and peer_id details to network_shared_state
        let node_data =
            setup_client(new_identifier, &mut self.test_loop, node_state, &self.shared_state);
        self.node_datas.push(node_data);
        self.node_datas.len() - 1
    }

    /// Function to add a new node in test loop environment. This function takes in the identifier
    /// and node_state of the new node as input.
    ///
    /// We set the NetworkInfo for this node which is required for state sync to work.
    ///
    /// Returns the index of the new node in `node_datas`.
    pub fn add_node(&mut self, identifier: &str, node_state: NodeSetupState) -> usize {
        // Logically this function is the same as restart_node
        self.restart_node(identifier, node_state)
    }

    pub fn get_node_data_by_account_id(&self, account_id: &AccountId) -> &NodeExecutionData {
        let idx = self.account_data_idx(account_id);
        &self.node_datas[idx]
    }

    pub fn validator(&self) -> TestLoopNode<'_> {
        self.node(0)
    }

    pub fn validator_mut(&mut self) -> TestLoopNodeMut<'_> {
        self.node_mut(0)
    }

    pub fn validator_runner(&mut self) -> NodeRunner<'_> {
        self.node_runner(0)
    }

    pub fn rpc_node(&self) -> TestLoopNode<'_> {
        let idx = self.rpc_data_idx();
        self.node(idx)
    }

    pub fn rpc_node_mut(&mut self) -> TestLoopNodeMut<'_> {
        let idx = self.rpc_data_idx();
        self.node_mut(idx)
    }

    pub fn node(&self, idx: usize) -> TestLoopNode<'_> {
        TestLoopNode { data: &self.test_loop.data, node_data: &self.node_datas[idx] }
    }

    pub fn node_mut(&mut self, idx: usize) -> TestLoopNodeMut<'_> {
        TestLoopNodeMut { data: &mut self.test_loop.data, node_data: &self.node_datas[idx] }
    }

    pub fn node_for_account(&self, account_id: &AccountId) -> TestLoopNode<'_> {
        let idx = self.account_data_idx(account_id);
        self.node(idx)
    }

    #[allow(dead_code)]
    pub fn node_for_account_mut(&mut self, account_id: &AccountId) -> TestLoopNodeMut<'_> {
        let idx = self.account_data_idx(account_id);
        self.node_mut(idx)
    }

    pub fn rpc_runner(&mut self) -> NodeRunner<'_> {
        let idx = self.rpc_data_idx();
        self.node_runner(idx)
    }

    pub fn node_runner(&mut self, idx: usize) -> NodeRunner<'_> {
        NodeRunner { test_loop: &mut self.test_loop, node_data: &self.node_datas[idx] }
    }

    pub fn runner_for_account(&mut self, account_id: &AccountId) -> NodeRunner<'_> {
        let idx = self.account_data_idx(account_id);
        self.node_runner(idx)
    }

    pub fn account_data_idx(&self, account_id: &AccountId) -> usize {
        self.node_datas
            .iter()
            .rposition(|d| &d.account_id == account_id)
            .unwrap_or_else(|| panic!("node with account id {account_id} not found"))
    }

    pub fn rpc_data_idx(&self) -> usize {
        self.account_data_idx(&rpc_account_id())
    }

    #[allow(dead_code)]
    pub fn archival_node(&self) -> TestLoopNode<'_> {
        let idx = self.archival_data_idx();
        self.node(idx)
    }

    pub fn archival_node_mut(&mut self) -> TestLoopNodeMut<'_> {
        let idx = self.archival_data_idx();
        self.node_mut(idx)
    }

    pub fn archival_runner(&mut self) -> NodeRunner<'_> {
        let idx = self.archival_data_idx();
        self.node_runner(idx)
    }

    pub fn archival_data_idx(&self) -> usize {
        self.account_data_idx(&archival_account_id())
    }

    pub fn node_state_builder(&self) -> NodeStateBuilder<'_> {
        let genesis = self.shared_state.genesis.clone();
        let tempdir_path = self.shared_state.tempdir.path().to_path_buf();
        NodeStateBuilder::new(genesis, tempdir_path)
            .bucket_config(self.shared_state.bucket_config.clone())
    }
}

impl Drop for TestLoopEnv {
    fn drop(&mut self) {
        // State sync dumper is not an Actor, handle stopping separately.
        for node_data in &self.node_datas {
            self.test_loop.data.get_mut(&node_data.state_sync_dumper_handle).stop();
        }
        self.test_loop.initiate_shutdown();
        self.test_loop.run_for(Duration::seconds(30));
    }
}
