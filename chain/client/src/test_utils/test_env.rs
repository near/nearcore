use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::adapter::ProcessTxResponse;
use crate::Client;
use near_async::messaging::CanSend;
use near_chain::test_utils::ValidatorSchedule;
use near_chain::{ChainGenesis, Provenance};
use near_chunks::client::ShardsManagerResponse;
use near_chunks::test_utils::MockClientAdapterForShardsManager;
use near_crypto::{InMemorySigner, KeyType, Signer};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::NetworkRequests;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg};
use near_o11y::testonly::TracingCapture;
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::block::Block;
use near_primitives::epoch_manager::RngSeed;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::sharding::PartialEncodedChunk;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochId, NumSeats};
use near_primitives::utils::MaybeValidated;
use near_primitives::version::ProtocolVersion;
use near_primitives::views::{
    AccountView, FinalExecutionOutcomeView, QueryRequest, QueryResponseKind, StateItem,
};
use once_cell::sync::OnceCell;

use super::setup::{setup_client_with_runtime, ShardsManagerAdapterForTest};
use super::test_env_builder::TestEnvBuilder;
use super::TEST_SEED;

/// An environment for writing integration tests with multiple clients.
/// This environment can simulate near nodes without network and it can be configured to use different runtimes.
pub struct TestEnv {
    pub chain_genesis: ChainGenesis,
    pub validators: Vec<AccountId>,
    pub network_adapters: Vec<Arc<MockPeerManagerAdapter>>,
    pub client_adapters: Vec<Arc<MockClientAdapterForShardsManager>>,
    pub shards_manager_adapters: Vec<ShardsManagerAdapterForTest>,
    pub clients: Vec<Client>,
    pub(crate) account_to_client_index: HashMap<AccountId, usize>,
    pub(crate) paused_blocks: Arc<Mutex<HashMap<CryptoHash, Arc<OnceCell<()>>>>>,
    // random seed to be inject in each client according to AccountId
    // if not set, a default constant TEST_SEED will be injected
    pub(crate) seeds: HashMap<AccountId, RngSeed>,
    pub(crate) archive: bool,
    pub(crate) save_trie_changes: bool,
}

impl TestEnv {
    pub fn builder(chain_genesis: ChainGenesis) -> TestEnvBuilder {
        TestEnvBuilder::new(chain_genesis)
    }

    /// Process a given block in the client with index `id`.
    /// Simulate the block processing logic in `Client`, i.e, it would run catchup and then process accepted blocks and possibly produce chunks.
    pub fn process_block(&mut self, id: usize, block: Block, provenance: Provenance) {
        self.clients[id].process_block_test(MaybeValidated::from(block), provenance).unwrap();
    }

    /// Produces block by given client, which may kick off chunk production.
    /// This means that transactions added before this call will be included in the next block produced by this validator.
    pub fn produce_block(&mut self, id: usize, height: BlockHeight) {
        let block = self.clients[id].produce_block(height).unwrap();
        self.process_block(id, block.unwrap(), Provenance::PRODUCED);
    }

    /// Pause processing of the given block, which means that the background
    /// thread which applies the chunks on the block will get blocked until
    /// `resume_block_processing` is called.
    ///
    /// Note that you must call `resume_block_processing` at some later point to
    /// unstuck the block.
    ///
    /// Implementation is rather crude and just hijacks our logging
    /// infrastructure. Hopefully this is good enough, but, if it isn't, we can
    /// add something more robust.
    pub fn pause_block_processing(&mut self, capture: &mut TracingCapture, block: &CryptoHash) {
        let paused_blocks = Arc::clone(&self.paused_blocks);
        paused_blocks.lock().unwrap().insert(*block, Arc::new(OnceCell::new()));
        capture.set_callback(move |msg| {
            if msg.starts_with("do_apply_chunks") {
                let cell = paused_blocks.lock().unwrap().iter().find_map(|(block_hash, cell)| {
                    if msg.contains(&format!("block_hash={block_hash}")) {
                        Some(Arc::clone(cell))
                    } else {
                        None
                    }
                });
                if let Some(cell) = cell {
                    cell.wait();
                }
            }
        });
    }

    /// See `pause_block_processing`.
    pub fn resume_block_processing(&mut self, block: &CryptoHash) {
        let mut paused_blocks = self.paused_blocks.lock().unwrap();
        let cell = paused_blocks.remove(block).unwrap();
        let _ = cell.set(());
    }

    pub fn client(&mut self, account_id: &AccountId) -> &mut Client {
        &mut self.clients[self.account_to_client_index[account_id]]
    }

    pub fn shards_manager(&self, account: &AccountId) -> &ShardsManagerAdapterForTest {
        &self.shards_manager_adapters[self.account_to_client_index[account]]
    }

    pub fn process_partial_encoded_chunks(&mut self) {
        let network_adapters = self.network_adapters.clone();

        let mut keep_going = true;
        while keep_going {
            // for network_adapter in network_adapters.iter() {
            for i in 0..network_adapters.len() {
                let network_adapter = network_adapters.get(i).unwrap();
                let _span =
                    tracing::debug_span!(target: "test", "process_partial_encoded_chunks", client=i).entered();

                keep_going = false;
                // process partial encoded chunks
                while let Some(request) = network_adapter.pop() {
                    // if there are any requests in any of the adapters reset
                    // keep going to true as processing of any message may
                    // trigger more messages to be processed in other clients
                    // it's a bit sad and it would be much nicer if all messages
                    // were forwarded to a single queue
                    // TODO would be nicer to first handle all PECs and then all PECFs
                    keep_going = true;
                    match request {
                        PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::PartialEncodedChunkMessage {
                                account_id,
                                partial_encoded_chunk,
                            },
                        ) => {
                            let partial_encoded_chunk =
                                PartialEncodedChunk::from(partial_encoded_chunk);
                            let message =
                                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                                    partial_encoded_chunk,
                                );
                            self.shards_manager(&account_id).send(message);
                        }
                        PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::PartialEncodedChunkForward { account_id, forward },
                        ) => {
                            let message =
                                ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                                    forward,
                                );
                            self.shards_manager(&account_id).send(message);
                        }
                        _ => {
                            tracing::debug!(target: "test", ?request, "skipping unsupported request type");
                        }
                    }
                }
            }
        }
    }

    /// Process all PartialEncodedChunkRequests in the network queue for a client
    /// `id`: id for the client
    pub fn process_partial_encoded_chunks_requests(&mut self, id: usize) {
        while let Some(request) = self.network_adapters[id].pop() {
            self.process_partial_encoded_chunk_request(id, request);
        }
    }

    /// Send the PartialEncodedChunkRequest to the target client, get response and process the response
    pub fn process_partial_encoded_chunk_request(
        &mut self,
        id: usize,
        request: PeerManagerMessageRequest,
    ) {
        if let PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::PartialEncodedChunkRequest { target, request, .. },
        ) = request
        {
            let target_id = self.account_to_client_index[&target.account_id.unwrap()];
            let response = self.get_partial_encoded_chunk_response(target_id, request);
            if let Some(response) = response {
                self.shards_manager_adapters[id].send(
                    ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                        partial_encoded_chunk_response: response,
                        received_time: Instant::now(),
                    },
                );
            }
        } else {
            panic!("The request is not a PartialEncodedChunk request {:?}", request);
        }
    }

    pub fn get_partial_encoded_chunk_response(
        &mut self,
        id: usize,
        request: PartialEncodedChunkRequestMsg,
    ) -> Option<PartialEncodedChunkResponseMsg> {
        self.shards_manager_adapters[id].send(
            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                partial_encoded_chunk_request: request.clone(),
                route_back: CryptoHash::default(),
            },
        );
        let response = self.network_adapters[id].pop_most_recent();
        match response {
            Some(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::PartialEncodedChunkResponse { route_back: _, response },
            )) => return Some(response),
            Some(response) => {
                self.network_adapters[id].put_back_most_recent(response);
            }
            None => {}
        }

        panic!(
            "Failed to process PartialEncodedChunkRequest from shards manager {}: {:?}",
            id, request
        );
    }

    pub fn process_shards_manager_responses(&mut self, id: usize) -> bool {
        let mut any_processed = false;
        while let Some(msg) = self.client_adapters[id].pop() {
            match msg {
                ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                    self.clients[id].on_chunk_completed(
                        partial_chunk,
                        shard_chunk,
                        Arc::new(|_| {}),
                    );
                }
                ShardsManagerResponse::InvalidChunk(encoded_chunk) => {
                    self.clients[id].on_invalid_chunk(encoded_chunk);
                }
                ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                    chunk_header,
                    chunk_producer,
                } => {
                    self.clients[id]
                        .on_chunk_header_ready_for_inclusion(chunk_header, chunk_producer);
                }
            }
            any_processed = true;
        }
        any_processed
    }

    pub fn process_shards_manager_responses_and_finish_processing_blocks(&mut self, idx: usize) {
        let _span =
            tracing::debug_span!(target: "test", "process_shards_manager", client=idx).entered();

        loop {
            self.process_shards_manager_responses(idx);
            if self.clients[idx].finish_blocks_in_processing().is_empty() {
                return;
            }
        }
    }

    pub fn send_money(&mut self, id: usize) -> ProcessTxResponse {
        let account_id = self.get_client_id(0);
        let signer =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
        let tx = SignedTransaction::send_money(
            1,
            account_id.clone(),
            account_id.clone(),
            &signer,
            100,
            self.clients[id].chain.head().unwrap().last_block_hash,
        );
        self.clients[id].process_tx(tx, false, false)
    }

    /// This function will actually bump to the latest protocol version instead of the provided one.
    /// See https://github.com/near/nearcore/issues/8590 for details.
    pub fn upgrade_protocol(&mut self, protocol_version: ProtocolVersion) {
        assert_eq!(self.clients.len(), 1, "at the moment, this support only a single client");

        let tip = self.clients[0].chain.head().unwrap();
        let epoch_id = self.clients[0]
            .epoch_manager
            .get_epoch_id_from_prev_block(&tip.last_block_hash)
            .unwrap();
        let block_producer =
            self.clients[0].epoch_manager.get_block_producer(&epoch_id, tip.height).unwrap();

        let mut block = self.clients[0].produce_block(tip.height + 1).unwrap().unwrap();
        eprintln!("Producing block with version {protocol_version}");
        block.mut_header().set_latest_protocol_version(protocol_version);
        block.mut_header().resign(&create_test_signer(block_producer.as_str()));

        let _ = self.clients[0]
            .process_block_test_no_produce_chunk(block.into(), Provenance::NONE)
            .unwrap();

        for i in 0..self.clients[0].chain.epoch_length * 2 {
            self.produce_block(0, tip.height + i + 2);
        }
    }

    pub fn query_account(&mut self, account_id: AccountId) -> AccountView {
        let client = &self.clients[0];
        let head = client.chain.head().unwrap();
        let last_block = client.chain.get_block(&head.last_block_hash).unwrap();
        let shard_id =
            client.epoch_manager.account_id_to_shard_id(&account_id, &head.epoch_id).unwrap();
        let shard_uid = client.epoch_manager.shard_id_to_uid(shard_id, &head.epoch_id).unwrap();
        let last_chunk_header = &last_block.chunks()[shard_id as usize];
        let response = client
            .runtime_adapter
            .query(
                shard_uid,
                &last_chunk_header.prev_state_root(),
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().prev_hash(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &QueryRequest::ViewAccount { account_id },
            )
            .unwrap();
        match response.kind {
            QueryResponseKind::ViewAccount(account_view) => account_view,
            _ => panic!("Wrong return value"),
        }
    }

    pub fn query_state(&mut self, account_id: AccountId) -> Vec<StateItem> {
        let client = &self.clients[0];
        let head = client.chain.head().unwrap();
        let last_block = client.chain.get_block(&head.last_block_hash).unwrap();
        let shard_id =
            client.epoch_manager.account_id_to_shard_id(&account_id, &head.epoch_id).unwrap();
        let shard_uid = client.epoch_manager.shard_id_to_uid(shard_id, &head.epoch_id).unwrap();
        let last_chunk_header = &last_block.chunks()[shard_id as usize];
        let response = client
            .runtime_adapter
            .query(
                shard_uid,
                &last_chunk_header.prev_state_root(),
                last_block.header().height(),
                last_block.header().raw_timestamp(),
                last_block.header().prev_hash(),
                last_block.header().hash(),
                last_block.header().epoch_id(),
                &QueryRequest::ViewState {
                    account_id,
                    prefix: vec![].into(),
                    include_proof: false,
                },
            )
            .unwrap();
        match response.kind {
            QueryResponseKind::ViewState(view_state_result) => view_state_result.values,
            _ => panic!("Wrong return value"),
        }
    }

    pub fn query_balance(&mut self, account_id: AccountId) -> Balance {
        self.query_account(account_id).amount
    }

    /// Restarts client at given index. Note that the new client reuses runtime
    /// adapter of old client.
    /// TODO (#8269): create new `KeyValueRuntime` for new client. Currently it
    /// doesn't work because `KeyValueRuntime` misses info about new epochs in
    /// memory caches.
    /// Though, it seems that it is not necessary for current use cases.
    pub fn restart(&mut self, idx: usize) {
        let account_id = self.get_client_id(idx).clone();
        let rng_seed = match self.seeds.get(&account_id) {
            Some(seed) => *seed,
            None => TEST_SEED,
        };
        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![self.validators.clone()]);
        let num_validator_seats = vs.all_block_producers().count() as NumSeats;
        self.clients[idx] = setup_client_with_runtime(
            num_validator_seats,
            Some(self.get_client_id(idx).clone()),
            false,
            self.network_adapters[idx].clone().into(),
            self.shards_manager_adapters[idx].clone(),
            self.chain_genesis.clone(),
            self.clients[idx].epoch_manager.clone(),
            self.clients[idx].shard_tracker.clone(),
            self.clients[idx].runtime_adapter.clone(),
            rng_seed,
            self.archive,
            self.save_trie_changes,
            None,
        )
    }

    /// Returns an [`AccountId`] used by a client at given index.  More
    /// specifically, returns validator id of the client’s validator signer.
    pub fn get_client_id(&self, idx: usize) -> &AccountId {
        self.clients[idx].validator_signer.as_ref().unwrap().validator_id()
    }

    pub fn get_runtime_config(&self, idx: usize, epoch_id: EpochId) -> RuntimeConfig {
        self.clients[idx].runtime_adapter.get_protocol_config(&epoch_id).unwrap().runtime_config
    }

    /// Create and sign transaction ready for execution.
    pub fn tx_from_actions(
        &mut self,
        actions: Vec<Action>,
        signer: &InMemorySigner,
        receiver: AccountId,
    ) -> SignedTransaction {
        let tip = self.clients[0].chain.head().unwrap();
        SignedTransaction::from_actions(
            tip.height + 1,
            signer.account_id.clone(),
            receiver,
            signer,
            actions,
            tip.last_block_hash,
        )
    }

    /// Wrap actions in a delegate action, put it in a transaction, sign.
    pub fn meta_tx_from_actions(
        &mut self,
        actions: Vec<Action>,
        sender: AccountId,
        relayer: AccountId,
        receiver_id: AccountId,
    ) -> SignedTransaction {
        let inner_signer =
            InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, sender.as_str());
        let relayer_signer =
            InMemorySigner::from_seed(relayer.clone(), KeyType::ED25519, relayer.as_str());
        let tip = self.clients[0].chain.head().unwrap();
        let user_nonce = tip.height + 1;
        let relayer_nonce = tip.height + 1;
        let delegate_action = DelegateAction {
            sender_id: inner_signer.account_id.clone(),
            receiver_id,
            actions: actions
                .into_iter()
                .map(|action| NonDelegateAction::try_from(action).unwrap())
                .collect(),
            nonce: user_nonce,
            max_block_height: tip.height + 100,
            public_key: inner_signer.public_key(),
        };
        let signature = inner_signer.sign(delegate_action.get_nep461_hash().as_bytes());
        let signed_delegate_action = SignedDelegateAction { delegate_action, signature };
        SignedTransaction::from_actions(
            relayer_nonce,
            relayer,
            sender,
            &relayer_signer,
            vec![Action::Delegate(Box::new(signed_delegate_action))],
            tip.last_block_hash,
        )
    }

    /// Process a tx and its receipts, then return the execution outcome.
    pub fn execute_tx(
        &mut self,
        tx: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        let tx_hash = tx.get_hash();
        let response = self.clients[0].process_tx(tx, false, false);
        // Check if the transaction got rejected
        match response {
            ProcessTxResponse::NoResponse
            | ProcessTxResponse::RequestRouted
            | ProcessTxResponse::ValidTx => (),
            ProcessTxResponse::InvalidTx(e) => return Err(e),
            ProcessTxResponse::DoesNotTrackShard => panic!("test setup is buggy"),
        }
        let max_iters = 100;
        let tip = self.clients[0].chain.head().unwrap();
        for i in 0..max_iters {
            let block = self.clients[0].produce_block(tip.height + i + 1).unwrap().unwrap();
            self.process_block(0, block.clone(), Provenance::PRODUCED);
            if let Ok(outcome) = self.clients[0].chain.get_final_transaction_result(&tx_hash) {
                return Ok(outcome);
            }
        }
        panic!("No transaction outcome found after {max_iters} blocks.")
    }

    /// Execute a function call transaction that calls main on the `TestEnv`.
    ///
    /// This function assumes that account has been deployed and that
    /// `InMemorySigner::from_seed` produces a valid signer that has it's key
    /// deployed already.
    pub fn call_main(&mut self, account: &AccountId) -> FinalExecutionOutcomeView {
        let signer = InMemorySigner::from_seed(account.clone(), KeyType::ED25519, account.as_str());
        let actions = vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "main".to_string(),
            args: vec![],
            gas: 3 * 10u64.pow(14),
            deposit: 0,
        }))];
        let tx = self.tx_from_actions(actions, &signer, signer.account_id.clone());
        self.execute_tx(tx).unwrap()
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let paused_blocks = self.paused_blocks.lock().unwrap();
        for cell in paused_blocks.values() {
            let _ = cell.set(());
        }
        if !paused_blocks.is_empty() && !std::thread::panicking() {
            panic!("some blocks are still paused, did you call `resume_block_processing`?")
        }
    }
}
