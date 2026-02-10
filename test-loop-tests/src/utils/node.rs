use std::borrow::Cow;
use std::sync::Arc;
use std::task::Poll;

use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain::types::Tip;
use near_chain::{Block, BlockHeader};
use near_client::client_actor::ClientActor;
use near_client::{Client, ProcessTxRequest, ViewClientActor};
use near_epoch_manager::shard_assignment::{account_id_to_shard_id, shard_id_to_uid};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunk;
use near_primitives::transaction::{
    ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof, SignedTransaction,
};
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{
    AccountView, FinalExecutionOutcomeView, FinalExecutionStatus, QueryRequest, QueryResponse,
    QueryResponseKind,
};

use crate::setup::state::NodeExecutionData;
use crate::utils::account::rpc_account_id;
use crate::utils::transactions::TransactionRunner;

/// Represents single node in multinode test loop setup. It simplifies
/// access to Client and other actors by providing more user friendly API.
/// It serves as a main interface for test actions such as sending
/// transactions, waiting for blocks to be produces, querying state, etc.
pub struct TestLoopNode<'a> {
    data: Cow<'a, NodeExecutionData>,
}

impl<'a> From<&'a NodeExecutionData> for TestLoopNode<'a> {
    fn from(value: &'a NodeExecutionData) -> Self {
        Self { data: Cow::Borrowed(value) }
    }
}

impl From<NodeExecutionData> for TestLoopNode<'_> {
    fn from(value: NodeExecutionData) -> Self {
        Self { data: Cow::Owned(value) }
    }
}

impl<'a> TestLoopNode<'a> {
    pub fn for_account(node_datas: &'a [NodeExecutionData], account_id: &AccountId) -> Self {
        // cspell:ignore rfind
        // Uses `rfind` because `TestLoopEnv::restart_node()` appends a new copy to `node_datas`.
        let data = node_datas
            .iter()
            .rfind(|data| &data.account_id == account_id)
            .unwrap_or_else(|| panic!("client with account id {account_id} not found"));
        Self::from(data)
    }

    pub fn rpc(node_datas: &'a [NodeExecutionData]) -> Self {
        Self::for_account(node_datas, &rpc_account_id())
    }

    #[allow(unused)]
    pub fn all(node_datas: &'a [NodeExecutionData]) -> Vec<Self> {
        node_datas.iter().map(|data| Self::from(data)).collect()
    }

    pub fn data(&self) -> &NodeExecutionData {
        &self.data
    }

    pub fn client<'b>(&self, test_loop_data: &'b TestLoopData) -> &'b Client {
        let client_handle = self.data().client_sender.actor_handle();
        &test_loop_data.get(&client_handle).client
    }

    pub fn client_actor<'b>(&self, test_loop_data: &'b mut TestLoopData) -> &'b mut ClientActor {
        let client_handle = self.data().client_sender.actor_handle();
        test_loop_data.get_mut(&client_handle)
    }

    pub fn view_client_actor<'b>(
        &self,
        test_loop_data: &'b mut TestLoopData,
    ) -> &'b mut ViewClientActor {
        let handle = self.data().view_client_sender.actor_handle();
        test_loop_data.get_mut(&handle)
    }

    pub fn tail(&self, test_loop_data: &TestLoopData) -> BlockHeight {
        self.client(test_loop_data).chain.tail().unwrap()
    }

    pub fn head(&self, test_loop_data: &TestLoopData) -> Arc<Tip> {
        self.client(test_loop_data).chain.head().unwrap()
    }

    pub fn last_executed(&self, test_loop_data: &TestLoopData) -> Arc<Tip> {
        if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
            self.client(test_loop_data).chain.chain_store().spice_execution_head().unwrap()
        } else {
            self.client(test_loop_data).chain.head().unwrap()
        }
    }

    pub fn head_block(&self, test_loop_data: &TestLoopData) -> Arc<Block> {
        let block_hash = self.client(test_loop_data).chain.head().unwrap().last_block_hash;
        self.block(test_loop_data, block_hash)
    }

    pub fn last_executed_block(&self, test_loop_data: &TestLoopData) -> Arc<Block> {
        let block_hash = self.last_executed(test_loop_data).last_block_hash;
        self.block(test_loop_data, block_hash)
    }

    pub fn block(&self, test_loop_data: &TestLoopData, block_hash: CryptoHash) -> Arc<Block> {
        self.client(test_loop_data).chain.get_block(&block_hash).unwrap()
    }

    pub fn block_chunks(&self, test_loop_data: &TestLoopData, block: &Block) -> Vec<ShardChunk> {
        let chain = &self.client(test_loop_data).chain;
        block
            .chunks()
            .iter_raw()
            .map(|chunk_header| chain.get_chunk(chunk_header.chunk_hash()).unwrap())
            .collect()
    }

    pub fn execution_outcome(
        &self,
        test_loop_data: &TestLoopData,
        tx_hash_or_receipt_id: CryptoHash,
    ) -> ExecutionOutcomeWithId {
        self.client(test_loop_data)
            .chain
            .get_execution_outcome(&tx_hash_or_receipt_id)
            .unwrap_or_else(|err| {
                panic!("outcome with id {tx_hash_or_receipt_id} is not available: {err}")
            })
            .outcome_with_id
    }

    pub fn tx_receipt_id(&self, test_loop_data: &TestLoopData, tx_hash: CryptoHash) -> CryptoHash {
        let tx_execution_outcome = self.execution_outcome(test_loop_data, tx_hash);
        let [receipt_id] = tx_execution_outcome.outcome.receipt_ids[..] else {
            panic!("expected single receipt")
        };
        receipt_id
    }

    pub fn run_until_head_height(&self, test_loop: &mut TestLoopV2, height: BlockHeight) {
        let initial_height = self.head(&test_loop.data).height;
        let height_diff = height.saturating_sub(initial_height) as usize;
        self.run_until_head_height_with_timeout(
            test_loop,
            height,
            self.calculate_block_distance_timeout(&test_loop.data, height_diff),
        );
    }

    pub fn run_until_executed_height(&self, test_loop: &mut TestLoopV2, height: BlockHeight) {
        let initial_height = self.last_executed(&test_loop.data).height;
        let height_diff = height.saturating_sub(initial_height) as usize;
        test_loop.run_until(
            |test_loop_data| self.last_executed(test_loop_data).height >= height,
            self.calculate_block_distance_timeout(&test_loop.data, height_diff),
        );
    }

    pub fn run_until_head_height_with_timeout(
        &self,
        test_loop: &mut TestLoopV2,
        height: BlockHeight,
        maximum_duration: Duration,
    ) {
        test_loop.run_until(
            |test_loop_data| self.head(test_loop_data).height >= height,
            maximum_duration,
        );
    }

    pub fn run_for_number_of_blocks(&self, test_loop: &mut TestLoopV2, num_blocks: usize) {
        self.run_for_number_of_blocks_with_timeout(
            test_loop,
            num_blocks,
            self.calculate_block_distance_timeout(&test_loop.data, num_blocks),
        );
    }

    pub fn run_for_number_of_blocks_with_timeout(
        &self,
        test_loop: &mut TestLoopV2,
        num_blocks: usize,
        maximum_duration: Duration,
    ) {
        let initial_head_height = self.head(&test_loop.data).height;
        test_loop.run_until(
            |test_loop_data| {
                let current_height = self.head(&test_loop_data).height;
                current_height >= initial_head_height + num_blocks as u64
            },
            maximum_duration,
        );
    }

    pub fn run_until_new_epoch(&self, test_loop: &mut TestLoopV2) {
        let curr_epoch_id = self.head(&test_loop.data).epoch_id;
        let epoch_length = self.client(&test_loop.data).config.epoch_length as usize;
        test_loop.run_until(
            |test_loop_data| {
                let head = self.head(test_loop_data);
                head.epoch_id != curr_epoch_id
            },
            self.calculate_block_distance_timeout(&test_loop.data, epoch_length + 1),
        );
    }

    pub fn submit_tx(&self, tx: SignedTransaction) {
        let process_tx_request =
            ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
        self.data().rpc_handler_sender.send(process_tx_request);
    }

    pub fn run_until_outcome_available(
        &self,
        test_loop: &mut TestLoopV2,
        tx_hash_or_receipt_id: CryptoHash,
        maximum_duration: Duration,
    ) -> ExecutionOutcomeWithIdAndProof {
        let mut ret = None;
        test_loop.run_until(
            |test_loop_data| match self
                .client(test_loop_data)
                .chain
                .get_execution_outcome(&tx_hash_or_receipt_id)
            {
                Ok(outcome) => {
                    ret = Some(outcome);
                    true
                }
                Err(_) => false,
            },
            maximum_duration,
        );
        ret.unwrap()
    }

    /// With spice blocks are executed separately from production so this runs until block with passed in
    /// header is executed.
    /// Without spice returns immediately.
    pub fn run_until_block_executed(
        &self,
        test_loop: &mut TestLoopV2,
        block_header: &BlockHeader,
        maximum_duration: Duration,
    ) {
        let protocol_version = self
            .client(&test_loop.data)
            .epoch_manager
            .get_epoch_protocol_version(block_header.epoch_id())
            .unwrap();
        if ProtocolFeature::Spice.enabled(protocol_version) {
            test_loop.run_until(
                |test_loop_data| self.last_executed(test_loop_data).height >= block_header.height(),
                maximum_duration,
            );
        }
    }

    #[track_caller]
    pub fn run_tx(
        &self,
        test_loop: &mut TestLoopV2,
        tx: SignedTransaction,
        maximum_duration: Duration,
    ) -> Vec<u8> {
        let outcome = self.execute_tx(test_loop, tx, maximum_duration).unwrap();
        match outcome.status {
            FinalExecutionStatus::SuccessValue(res) => res,
            status @ _ => panic!("Transaction failed with status {status:?}"),
        }
    }

    pub fn execute_tx(
        &self,
        test_loop: &mut TestLoopV2,
        tx: SignedTransaction,
        maximum_duration: Duration,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        let tx_processor_sender = &self.data().rpc_handler_sender;
        let mut tx_runner = TransactionRunner::new(tx, false);
        let future_spawner = test_loop.future_spawner("TransactionRunner");

        let mut res = None;
        test_loop.run_until(
            |test_loop_data| {
                let client = self.client(test_loop_data);
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

    pub fn runtime_query(
        &self,
        test_loop_data: &TestLoopData,
        account_id: &AccountId,
        query: QueryRequest,
    ) -> QueryResponse {
        let client = self.client(test_loop_data);
        let head = self.head(test_loop_data);
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

    pub fn view_account_query(
        &self,
        test_loop_data: &TestLoopData,
        account_id: &AccountId,
    ) -> AccountView {
        let response = self.runtime_query(
            test_loop_data,
            &account_id,
            QueryRequest::ViewAccount { account_id: account_id.clone() },
        );
        let QueryResponseKind::ViewAccount(account_view) = response.kind else {
            panic!("Unexpected query response type")
        };
        account_view
    }

    #[cfg(feature = "test_features")]
    pub fn send_adversarial_message(
        &self,
        test_loop: &TestLoopV2,
        message: near_client::NetworkAdversarialMessage,
    ) {
        let client_sender = self.data().client_sender.clone();
        test_loop.send_adhoc_event(
            format!("send adversarial {:?} to {}", message, self.data().account_id),
            move |_| {
                client_sender.send(message);
            },
        );
    }

    fn calculate_block_distance_timeout(
        &self,
        test_loop_data: &TestLoopData,
        num_blocks: usize,
    ) -> Duration {
        let max_block_production_delay =
            self.client(test_loop_data).config.max_block_production_delay;
        max_block_production_delay * (num_blocks as u32 + 1)
    }

    /// Returns new TestLoopNode that takes ownership of internal NodeExecutionData.
    pub fn into_owned(self) -> TestLoopNode<'static> {
        TestLoopNode::from(self.data.into_owned())
    }
}
