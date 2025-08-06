use std::sync::Arc;
use std::task::Poll;

use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::types::Tip;
use near_client::Client;
use near_epoch_manager::shard_assignment::{account_id_to_shard_id, shard_id_to_uid};
use near_primitives::errors::InvalidTxError;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::views::{
    AccountView, FinalExecutionOutcomeView, FinalExecutionStatus, QueryRequest, QueryResponse,
    QueryResponseKind,
};

use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;
use crate::utils::transactions::TransactionRunner;

pub struct TestLoopClient<'a> {
    data: &'a NodeExecutionData,
    test_loop: &'a mut TestLoopV2,
}

impl<'a> TestLoopClient<'a> {
    pub fn from_env(env: &'a mut TestLoopEnv, client_account_id: AccountId) -> Self {
        let data = env
            .node_datas
            .iter_mut()
            .find(|data| data.account_id == client_account_id)
            .unwrap_or_else(|| panic!("client with account id {client_account_id} not found"));
        Self { data, test_loop: &mut env.test_loop }
    }

    pub fn node_client(&self) -> &Client {
        let client_handle = self.data.client_sender.actor_handle();
        &self.test_loop.data.get(&client_handle).client
    }

    pub fn head(&self) -> Arc<Tip> {
        self.node_client().chain.head().unwrap()
    }

    #[allow(unused)]
    pub fn run_until_node_head_height(&mut self, height: BlockHeight, maximum_duration: Duration) {
        let client_handle = self.data.client_sender.actor_handle();
        self.test_loop.run_until(
            |test_loop_data| {
                test_loop_data.get(&client_handle).client.chain.head().unwrap().height >= height
            },
            maximum_duration,
        );
    }

    pub fn run_for_number_of_blocks(&mut self, num_blocks: usize) {
        let max_block_production_delay = self.node_client().config.max_block_production_delay;
        let initial_head_height = self.head().height;
        let client_handle = self.data.client_sender.actor_handle();
        self.test_loop.run_until(
            |test_loop_data| {
                let current_height =
                    test_loop_data.get(&client_handle).client.chain.head().unwrap().height;
                current_height >= initial_head_height + num_blocks as u64
            },
            max_block_production_delay * (num_blocks as u32 + 1),
        );
    }

    pub fn run_tx(&mut self, tx: SignedTransaction, maximum_duration: Duration) -> Vec<u8> {
        let outcome = self.execute_tx(tx, maximum_duration).unwrap();
        match outcome.status {
            FinalExecutionStatus::SuccessValue(res) => res,
            status @ _ => panic!("Transaction failed with status {status:?}"),
        }
    }

    pub fn execute_tx(
        &mut self,
        tx: SignedTransaction,
        maximum_duration: Duration,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        let client_handle = self.data.client_sender.actor_handle();
        let tx_processor_sender = &self.data.rpc_handler_sender;
        let mut tx_runner = TransactionRunner::new(tx, false);
        let future_spawner = self.test_loop.future_spawner("TransactionRunner");

        let mut res = None;
        self.test_loop.run_until(
            |test_loop_data| {
                let client = &test_loop_data.get(&client_handle).client;
                match tx_runner.poll(&tx_processor_sender, client, &future_spawner) {
                    Poll::Pending => false,
                    Poll::Ready(tx_res) => {
                        res = Some(tx_res);
                        true
                    }
                }
            },
            maximum_duration,
        );

        res.unwrap()
    }

    pub fn runtime_query(&self, account_id: &AccountId, query: QueryRequest) -> QueryResponse {
        let client = self.node_client();
        let head = self.head();
        let last_block = client.chain.get_block(&head.last_block_hash).unwrap();
        let shard_id =
            account_id_to_shard_id(client.epoch_manager.as_ref(), &account_id, &head.epoch_id)
                .unwrap();
        let shard_uid =
            shard_id_to_uid(client.epoch_manager.as_ref(), shard_id, &head.epoch_id).unwrap();
        let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let last_chunk_header = &last_block.chunks()[shard_index];

        client
            .runtime_adapter
            .query(
                shard_uid,
                &last_chunk_header.prev_state_root(),
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().prev_hash(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &query,
            )
            .unwrap()
    }

    pub fn view_account_query(&self, account_id: &AccountId) -> AccountView {
        let response = self.runtime_query(
            &account_id,
            QueryRequest::ViewAccount { account_id: account_id.clone() },
        );
        let QueryResponseKind::ViewAccount(account_view) = response.kind else {
            panic!("Unexpected query response type")
        };
        account_view
    }
}

pub(crate) trait TestLoopEnvExt {
    fn client<'a>(&'a mut self, client_account_id: AccountId) -> TestLoopClient<'a>;
}

impl TestLoopEnvExt for TestLoopEnv {
    fn client<'a>(&'a mut self, client_account_id: AccountId) -> TestLoopClient<'a> {
        TestLoopClient::from_env(self, client_account_id)
    }
}
