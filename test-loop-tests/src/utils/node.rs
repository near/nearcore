use std::sync::Arc;
use std::task::Poll;

use near_async::messaging::CanSend;
use near_async::test_loop::TestLoopV2;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain::types::Tip;
use near_chain::{Block, BlockHeader};
use near_client::client_actor::ClientActor;
use near_client::{Client, ProcessTxRequest, Query, QueryError, ViewClientActor};
use near_crypto::{PublicKey, Signer};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunk;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{
    ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof, SignedTransaction,
};
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{
    AccessKeyView, AccountView, FinalExecutionOutcomeView, FinalExecutionStatus, QueryRequest,
    QueryResponse, QueryResponseKind,
};
use near_store::Store;
use near_store::adapter::StoreAdapter as _;

use crate::setup::state::NodeExecutionData;
use crate::utils::transactions::TransactionRunner;

pub struct TestLoopNode<'a> {
    pub(crate) data: &'a mut TestLoopData,
    pub(crate) node_data: &'a NodeExecutionData,
}

#[allow(dead_code)]
impl<'a> TestLoopNode<'a> {
    pub fn data(&self) -> &NodeExecutionData {
        self.node_data
    }

    pub fn client(&self) -> &Client {
        let handle = self.node_data.client_sender.actor_handle();
        &self.data.get(&handle).client
    }

    pub fn store(&self) -> Store {
        self.client().chain.chain_store.store().store()
    }

    pub fn client_actor(&mut self) -> &mut ClientActor {
        let handle = self.node_data.client_sender.actor_handle();
        self.data.get_mut(&handle)
    }

    pub fn view_client_actor(&mut self) -> &mut ViewClientActor {
        let handle = self.node_data.view_client_sender.actor_handle();
        self.data.get_mut(&handle)
    }

    pub fn tail(&self) -> BlockHeight {
        self.client().chain.tail()
    }

    pub fn head(&self) -> Arc<Tip> {
        self.client().chain.head().unwrap()
    }

    pub fn last_executed(&self) -> Arc<Tip> {
        if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
            self.client().chain.chain_store().spice_execution_head().unwrap()
        } else {
            self.client().chain.head().unwrap()
        }
    }

    pub fn head_block(&self) -> Arc<Block> {
        let block_hash = self.head().last_block_hash;
        self.block(block_hash)
    }

    pub fn last_executed_block(&self) -> Arc<Block> {
        let block_hash = self.last_executed().last_block_hash;
        self.block(block_hash)
    }

    pub fn block(&self, block_hash: CryptoHash) -> Arc<Block> {
        self.client().chain.get_block(&block_hash).unwrap()
    }

    pub fn block_chunks(&self, block: &Block) -> Vec<ShardChunk> {
        let chain = &self.client().chain;
        block
            .chunks()
            .iter_raw()
            .map(|chunk_header| chain.get_chunk(chunk_header.chunk_hash()).unwrap())
            .collect()
    }

    pub fn execution_outcome_with_proof(
        &self,
        tx_hash_or_receipt_id: CryptoHash,
    ) -> ExecutionOutcomeWithIdAndProof {
        self.client().chain.get_execution_outcome(&tx_hash_or_receipt_id).unwrap_or_else(|err| {
            panic!("outcome with id {tx_hash_or_receipt_id} is not available: {err}")
        })
    }

    pub fn execution_outcome(&self, tx_hash_or_receipt_id: CryptoHash) -> ExecutionOutcomeWithId {
        self.execution_outcome_with_proof(tx_hash_or_receipt_id).outcome_with_id
    }

    pub fn tx_receipt_id(&self, tx_hash: CryptoHash) -> CryptoHash {
        let tx_execution_outcome = self.execution_outcome(tx_hash);
        let [receipt_id] = tx_execution_outcome.outcome.receipt_ids[..] else {
            panic!("expected single receipt")
        };
        receipt_id
    }

    pub fn runtime_query(&self, query: QueryRequest) -> Result<QueryResponse, QueryError> {
        let handle = self.node_data.view_client_sender.actor_handle();
        let view_client: &ViewClientActor = self.data.get(&handle);
        view_client.handle_query(Query::new(
            near_primitives::types::BlockReference::Finality(
                near_primitives::types::Finality::None,
            ),
            query,
        ))
    }

    pub fn view_account_query(&self, account_id: &AccountId) -> Result<AccountView, QueryError> {
        let response =
            self.runtime_query(QueryRequest::ViewAccount { account_id: account_id.clone() })?;
        let QueryResponseKind::ViewAccount(account_view) = response.kind else {
            panic!("unexpected query response type")
        };
        Ok(account_view)
    }

    pub fn view_access_key_query(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKeyView, QueryError> {
        let response = self.runtime_query(QueryRequest::ViewAccessKey {
            account_id: account_id.clone(),
            public_key: public_key.clone(),
        })?;
        let QueryResponseKind::AccessKey(access_key_view) = response.kind else {
            panic!("unexpected query response type")
        };
        Ok(access_key_view)
    }

    pub fn get_next_nonce(&self, account_id: &AccountId) -> u64 {
        let signer: Signer = create_user_test_signer(account_id);
        let access_key = self.view_access_key_query(account_id, &signer.public_key()).unwrap();
        access_key.nonce + 1
    }

    pub fn submit_tx(&self, tx: SignedTransaction) {
        let process_tx_request =
            ProcessTxRequest { transaction: tx, is_forwarded: false, check_only: false };
        self.node_data.rpc_handler_sender.send(process_tx_request);
    }

    #[cfg(feature = "test_features")]
    pub fn validate_store(&mut self) {
        if cfg!(feature = "protocol_feature_spice") {
            return;
        }
        use near_async::messaging::Handler;
        use near_client::NetworkAdversarialMessage;
        let handle = self.node_data.client_sender.actor_handle();
        let client_actor = self.data.get_mut(&handle);
        let result = Handler::<NetworkAdversarialMessage, Option<u64>>::handle(
            client_actor,
            NetworkAdversarialMessage::AdvCheckStorageConsistency,
        );
        assert_ne!(result, Some(0), "store validation failed");
    }
}

pub struct NodeRunner<'a> {
    pub(crate) test_loop: &'a mut TestLoopV2,
    pub(crate) node_data: &'a NodeExecutionData,
}

#[allow(dead_code)]
impl<'a> NodeRunner<'a> {
    pub fn run_until(
        &mut self,
        mut condition: impl FnMut(&TestLoopNode<'_>) -> bool,
        maximum_duration: Duration,
    ) {
        let node_data = self.node_data;
        self.test_loop.run_until(
            |test_loop_data| {
                let node = TestLoopNode { data: test_loop_data, node_data };
                condition(&node)
            },
            maximum_duration,
        );
    }

    pub fn run_for(&mut self, duration: Duration) {
        self.test_loop.run_for(duration);
    }

    pub fn run_until_head_height(&mut self, height: BlockHeight) {
        let initial_height = self.head().height;
        let height_diff = height.saturating_sub(initial_height) as usize;
        let timeout = self.calculate_block_distance_timeout(height_diff);
        self.run_until(|node| node.head().height >= height, timeout);
    }

    pub fn run_until_executed_height(&mut self, height: BlockHeight) {
        let initial_height = self.last_executed().height;
        let height_diff = height.saturating_sub(initial_height) as usize;
        let extra = self.node_data.expected_execution_delay() as usize;
        let timeout = self.calculate_block_distance_timeout(height_diff + extra);
        self.run_until(|node| node.last_executed().height >= height, timeout);
    }

    pub fn run_until_head_height_with_timeout(
        &mut self,
        height: BlockHeight,
        maximum_duration: Duration,
    ) {
        self.run_until(|node| node.head().height >= height, maximum_duration);
    }

    pub fn run_for_number_of_blocks(&mut self, num_blocks: usize) {
        let timeout = self.calculate_block_distance_timeout(num_blocks);
        self.run_for_number_of_blocks_with_timeout(num_blocks, timeout);
    }

    pub fn run_for_number_of_blocks_with_timeout(
        &mut self,
        num_blocks: usize,
        maximum_duration: Duration,
    ) {
        let initial_head_height = self.head().height;
        self.run_until(
            |node| node.head().height >= initial_head_height + num_blocks as u64,
            maximum_duration,
        );
    }

    pub fn run_until_new_epoch(&mut self) {
        let curr_epoch_id = self.head().epoch_id;
        let epoch_length = self.client().config.epoch_length as usize;
        let timeout = self.calculate_block_distance_timeout(epoch_length + 1);
        self.run_until(|node| node.head().epoch_id != curr_epoch_id, timeout);
    }

    pub fn run_until_outcome_available(
        &mut self,
        tx_hash_or_receipt_id: CryptoHash,
        maximum_duration: Duration,
    ) -> ExecutionOutcomeWithIdAndProof {
        let mut ret = None;
        self.run_until(
            |node| match node.client().chain.get_execution_outcome(&tx_hash_or_receipt_id) {
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

    pub fn run_until_block_executed(
        &mut self,
        block_header: &BlockHeader,
        maximum_duration: Duration,
    ) {
        let protocol_version = self
            .client()
            .epoch_manager
            .get_epoch_protocol_version(block_header.epoch_id())
            .unwrap();
        if ProtocolFeature::Spice.enabled(protocol_version) {
            let block_height = block_header.height();
            self.run_until(
                |node| {
                    node.client().chain.chain_store().spice_execution_head().unwrap().height
                        >= block_height
                },
                maximum_duration,
            );
        }
    }

    #[track_caller]
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
        let tx_processor_sender = self.node_data.rpc_handler_sender.clone();
        let mut tx_runner = TransactionRunner::new(tx, false);
        let future_spawner = self.test_loop.future_spawner("TransactionRunner");
        let mut res = None;
        self.run_until(
            |node| match tx_runner.poll(&tx_processor_sender, node.client(), &future_spawner) {
                Poll::Pending => false,
                Poll::Ready(tx_res) => {
                    res = Some(tx_res);
                    true
                }
            },
            maximum_duration,
        );
        res.unwrap()
    }

    #[cfg(feature = "test_features")]
    pub fn send_adversarial_message(&self, message: near_client::NetworkAdversarialMessage) {
        let client_sender = self.node_data.client_sender.clone();
        let account_id = self.node_data.account_id.clone();
        self.test_loop.send_adhoc_event(
            format!("send adversarial {:?} to {}", message, account_id),
            move |_| {
                client_sender.send(message);
            },
        );
    }

    fn client(&self) -> &Client {
        let handle = self.node_data.client_sender.actor_handle();
        &self.test_loop.data.get(&handle).client
    }

    fn head(&self) -> Arc<Tip> {
        self.client().chain.head().unwrap()
    }

    fn last_executed(&self) -> Arc<Tip> {
        if ProtocolFeature::Spice.enabled(PROTOCOL_VERSION) {
            self.client().chain.chain_store().spice_execution_head().unwrap()
        } else {
            self.client().chain.head().unwrap()
        }
    }

    fn calculate_block_distance_timeout(&self, num_blocks: usize) -> Duration {
        self.client().config.max_block_production_delay * (num_blocks as u32 + 1)
    }
}
