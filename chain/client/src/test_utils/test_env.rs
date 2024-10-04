use crate::{Client, DistributeStateWitnessRequest};
use near_async::messaging::{CanSend, IntoMultiSender};
use near_async::time::Clock;
use near_async::time::{Duration, Instant};
use near_chain::stateless_validation::processing_tracker::{
    ProcessingDoneTracker, ProcessingDoneWaiter,
};
use near_chain::test_utils::ValidatorSchedule;
use near_chain::types::Tip;
use near_chain::{ChainGenesis, ChainStoreAccess, Provenance};
use near_chain_configs::GenesisConfig;
use near_chain_primitives::error::QueryError;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::test_utils::{MockClientAdapterForShardsManager, SynchronousShardsManagerAdapter};
use near_crypto::{InMemorySigner, KeyType};
use near_network::client::ProcessTxResponse;
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::test_utils::MockPeerManagerAdapter;
use near_network::types::NetworkRequests;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg};
use near_o11y::testonly::TracingCapture;
use near_parameters::RuntimeConfig;
use near_primitives::action::delegate::{DelegateAction, NonDelegateAction, SignedDelegateAction};
use near_primitives::block::Block;
use near_primitives::epoch_info::RngSeed;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, PartialEncodedChunk};
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::state_witness::ChunkStateWitness;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};
use near_primitives::types::{AccountId, Balance, BlockHeight, EpochId, NumSeats, ShardId};
use near_primitives::utils::MaybeValidated;
use near_primitives::views::{
    AccountView, FinalExecutionOutcomeView, QueryRequest, QueryResponse, QueryResponseKind,
    StateItem,
};
use near_store::metadata::DbKind;
use near_store::ShardUId;
use near_vm_runner::logic::ProtocolVersion;
use once_cell::sync::OnceCell;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use time::ext::InstantExt as _;

use super::mock_partial_witness_adapter::MockPartialWitnessAdapter;
use super::setup::setup_client_with_runtime;
use super::test_env_builder::TestEnvBuilder;
use super::TEST_SEED;

/// Timeout used in tests that wait for a specific chunk endorsement to appear
const CHUNK_ENDORSEMENTS_TIMEOUT: Duration = Duration::seconds(10);

/// An environment for writing integration tests with multiple clients.
/// This environment can simulate near nodes without network and it can be configured to use different runtimes.
pub struct TestEnv {
    pub clock: Clock,
    pub chain_genesis: ChainGenesis,
    pub validators: Vec<AccountId>,
    pub network_adapters: Vec<Arc<MockPeerManagerAdapter>>,
    pub client_adapters: Vec<Arc<MockClientAdapterForShardsManager>>,
    pub partial_witness_adapters: Vec<MockPartialWitnessAdapter>,
    pub shards_manager_adapters: Vec<SynchronousShardsManagerAdapter>,
    pub clients: Vec<Client>,
    pub(crate) account_indices: AccountIndices,
    pub(crate) paused_blocks: Arc<Mutex<HashMap<CryptoHash, Arc<OnceCell<()>>>>>,
    // random seed to be inject in each client according to AccountId
    // if not set, a default constant TEST_SEED will be injected
    pub(crate) seeds: HashMap<AccountId, RngSeed>,
    pub(crate) archive: bool,
    pub(crate) save_trie_changes: bool,
}

pub struct StateWitnessPropagationOutput {
    /// Whether some propagated state witness includes two different post state
    /// roots.
    pub found_differing_post_state_root_due_to_state_transitions: bool,
}

impl TestEnv {
    pub fn default_builder() -> TestEnvBuilder {
        let clock = Clock::real();
        TestEnvBuilder::new(GenesisConfig::test(clock.clone())).clock(clock)
    }

    pub fn builder(genesis_config: &GenesisConfig) -> TestEnvBuilder {
        TestEnvBuilder::new(genesis_config.clone())
    }

    /// Process a given block in the client with index `id`.
    /// Simulate the block processing logic in `Client`, i.e, it would run catchup and then process accepted blocks and possibly produce chunks.
    /// Runs garbage collection manually
    pub fn process_block(&mut self, id: usize, block: Block, provenance: Provenance) {
        self.clients[id].process_block_test(MaybeValidated::from(block), provenance).unwrap();
        // runs gc
        let runtime_adapter = self.clients[id].chain.runtime_adapter.clone();
        let epoch_manager = self.clients[id].chain.epoch_manager.clone();
        let gc_config = self.clients[id].config.gc.clone();

        // A RPC node should do regular garbage collection.
        if !self.clients[id].config.archive {
            self.clients[id]
                .chain
                .mut_chain_store()
                .clear_data(&gc_config, runtime_adapter, epoch_manager)
                .unwrap();
        } else {
            // An archival node with split storage should perform garbage collection
            // on the hot storage. In order to determine if split storage is enabled
            // *and* that the migration to split storage is finished we can check
            // the store kind. It's only set to hot after the migration is finished.
            let store = self.clients[0].chain.chain_store().store();
            let kind = store.get_db_kind().unwrap();
            if kind == Some(DbKind::Hot) {
                self.clients[id]
                    .chain
                    .mut_chain_store()
                    .clear_data(&gc_config, runtime_adapter, epoch_manager)
                    .unwrap();
            } else {
                // An archival node with legacy storage or in the midst of migration to split
                // storage should do the legacy clear_archive_data.
                self.clients[id]
                    .chain
                    .mut_chain_store()
                    .clear_archive_data(gc_config.gc_blocks_limit, runtime_adapter)
                    .unwrap();
            }
        }
        self.propagate_chunk_state_witnesses_and_endorsements(false);
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
        self.account_indices.lookup_mut(&mut self.clients, account_id)
    }

    pub fn shards_manager(&self, account: &AccountId) -> &SynchronousShardsManagerAdapter {
        self.account_indices.lookup(&self.shards_manager_adapters, account)
    }

    pub fn process_partial_encoded_chunks(&mut self) {
        let network_adapters = self.network_adapters.clone();

        let mut keep_going = true;
        while keep_going {
            keep_going = false;
            // for network_adapter in network_adapters.iter() {
            for i in 0..network_adapters.len() {
                let network_adapter = network_adapters.get(i).unwrap();
                let _span =
                    tracing::debug_span!(target: "test", "process_partial_encoded_chunks", client=i).entered();

                keep_going |= network_adapter.handle_filtered(|request| match request {
                    PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::PartialEncodedChunkRequest { .. },
                    ) => {
                        self.process_partial_encoded_chunk_request(i, request);
                        None
                    }
                    PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::PartialEncodedChunkMessage {
                            account_id,
                            partial_encoded_chunk,
                        },
                    ) => {
                        let partial_encoded_chunk =
                            PartialEncodedChunk::from(partial_encoded_chunk);
                        let message = ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                            partial_encoded_chunk,
                        );
                        self.shards_manager(&account_id).send(message);
                        None
                    }
                    PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::PartialEncodedChunkForward { account_id, forward },
                    ) => {
                        let message =
                            ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                                forward,
                            );
                        self.shards_manager(&account_id).send(message);
                        None
                    }
                    _ => Some(request),
                });
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
            let target_id = self.account_indices.index(&target.account_id.unwrap());
            let response = self.get_partial_encoded_chunk_response(target_id, request);
            tracing::info!("Got response for PartialEncodedChunkRequest: {:?}", response);
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
                    let signer = self.clients[id].validator_signer.get();
                    self.clients[id].on_chunk_completed(partial_chunk, shard_chunk, None, &signer);
                }
                ShardsManagerResponse::InvalidChunk(encoded_chunk) => {
                    self.clients[id].on_invalid_chunk(encoded_chunk);
                }
                ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                    chunk_header,
                    chunk_producer,
                } => {
                    self.clients[id]
                        .mark_chunk_header_ready_for_inclusion(chunk_header, chunk_producer);
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

    fn found_differing_post_state_root_due_to_state_transitions(
        witness: &ChunkStateWitness,
    ) -> bool {
        let mut post_state_roots = HashSet::from([witness.main_state_transition.post_state_root]);
        post_state_roots.extend(witness.implicit_transitions.iter().map(|t| t.post_state_root));
        post_state_roots.len() >= 2
    }

    /// Processes all state witnesses sent over the network. The function waits for the processing to finish,
    /// so chunk endorsements are available immediately after this function returns.
    pub fn propagate_chunk_state_witnesses(
        &mut self,
        allow_errors: bool,
    ) -> StateWitnessPropagationOutput {
        let mut output = StateWitnessPropagationOutput {
            found_differing_post_state_root_due_to_state_transitions: false,
        };
        let mut witness_processing_done_waiters: Vec<ProcessingDoneWaiter> = Vec::new();

        // Here we are completely bypassing the partial_witness_actor and directly distributing the state witness to the
        // clients. Ideally the route should have been the following:
        // [client] ----(DistributeStateWitnessRequest)----> [partial_witness_actor]
        // [partial_witness_actor] ----(PartialEncodedStateWitness + Forward)----> [partial_witness_actor]
        // [partial_witness_actor] ----(ChunkStateWitnessMessage)----> [client]
        // But we go directly from processing DistributeStateWitnessRequest to sending it to all the chunk validators.
        // Validation of state witness is done in the partial_witness_actor which should be tested by test_loop.
        let partial_witness_adapters = self.partial_witness_adapters.clone();
        for (client_idx, partial_witness_adapter) in partial_witness_adapters.iter().enumerate() {
            while let Some(request) = partial_witness_adapter.pop_distribution_request() {
                let DistributeStateWitnessRequest { epoch_id, chunk_header, state_witness } =
                    request;

                let raw_witness_size = borsh::to_vec(&state_witness).unwrap().len();
                let chunk_validators = self.clients[client_idx]
                    .epoch_manager
                    .get_chunk_validator_assignments(
                        &epoch_id,
                        chunk_header.shard_id(),
                        chunk_header.height_created(),
                    )
                    .unwrap()
                    .ordered_chunk_validators();

                for account_id in chunk_validators {
                    let processing_done_tracker = ProcessingDoneTracker::new();
                    witness_processing_done_waiters.push(processing_done_tracker.make_waiter());

                    let client = self.client(&account_id);
                    let processing_result = client.process_chunk_state_witness(
                        state_witness.clone(),
                        raw_witness_size,
                        Some(processing_done_tracker),
                        client.validator_signer.get(),
                    );
                    if !allow_errors {
                        processing_result.unwrap();
                    }
                }

                // Update output.
                output.found_differing_post_state_root_due_to_state_transitions |=
                    Self::found_differing_post_state_root_due_to_state_transitions(&state_witness);
            }
        }

        // Wait for all state witnesses to be processed before returning.
        for processing_done_waiter in witness_processing_done_waiters {
            processing_done_waiter.wait();
        }

        output
    }

    pub fn propagate_chunk_endorsements(&mut self, allow_errors: bool) {
        // Clone the Vec to satisfy the borrow checker.
        let network_adapters = self.network_adapters.clone();
        for network_adapter in network_adapters {
            network_adapter.handle_filtered(|request| match request {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ChunkEndorsement(
                    account_id,
                    endorsement,
                )) => {
                    let processing_result =
                        self.client(&account_id).process_chunk_endorsement(endorsement);
                    if !allow_errors {
                        processing_result.unwrap();
                    }

                    None
                }
                _ => Some(request),
            });
        }
    }

    pub fn propagate_chunk_state_witnesses_and_endorsements(&mut self, allow_errors: bool) {
        self.propagate_chunk_state_witnesses(allow_errors);
        self.propagate_chunk_endorsements(allow_errors);
    }

    /// Wait until an endorsement for `chunk_hash` appears in the network messages send by
    /// the Client with index `client_idx`. Times out after CHUNK_ENDORSEMENTS_TIMEOUT.
    /// Doesn't process or consume the message, it just waits until the message appears on the network_adapter.
    pub fn wait_for_chunk_endorsement(
        &mut self,
        client_idx: usize,
        chunk_hash: &ChunkHash,
    ) -> Result<(), TimeoutError> {
        let start_time = Instant::now();
        let network_adapter = self.network_adapters[client_idx].clone();
        let mut endorsement_found = false;
        loop {
            network_adapter.handle_filtered(|request| {
                match &request {
                    PeerManagerMessageRequest::NetworkRequests(
                        NetworkRequests::ChunkEndorsement(_, endorsement),
                    ) => {
                        let endorsement_chunk_hash = match endorsement {
                            ChunkEndorsement::V1(endorsement) => endorsement.chunk_hash(),
                            ChunkEndorsement::V2(endorsement) => endorsement.chunk_hash(),
                        };
                        endorsement_found = endorsement_chunk_hash == chunk_hash;
                    }
                    _ => {}
                };
                Some(request)
            });

            if endorsement_found {
                return Ok(());
            }

            let elapsed_since_start = Instant::now().signed_duration_since(start_time);
            if elapsed_since_start > CHUNK_ENDORSEMENTS_TIMEOUT {
                return Err(TimeoutError(elapsed_since_start));
            }

            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    pub fn send_money(&mut self, id: usize) -> ProcessTxResponse {
        let account_id = self.get_client_id(0);
        let signer =
            InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, account_id.as_ref());
        let tx = SignedTransaction::send_money(
            1,
            account_id.clone(),
            account_id,
            &signer.into(),
            100,
            self.clients[id].chain.head().unwrap().last_block_hash,
        );
        self.clients[id].process_tx(tx, false, false)
    }

    /// This function used to be able to upgrade to a specific protocol version
    /// but due to <https://github.com/near/nearcore/issues/8590> that
    /// functionality does not work currently.  Hence it is renamed to upgrade
    /// to the latest version.
    pub fn upgrade_protocol_to_latest_version(&mut self) {
        assert_eq!(self.clients.len(), 1, "at the moment, this support only a single client");

        let tip = self.clients[0].chain.head().unwrap();
        let epoch_id = self.clients[0]
            .epoch_manager
            .get_epoch_id_from_prev_block(&tip.last_block_hash)
            .unwrap();
        let block_producer =
            self.clients[0].epoch_manager.get_block_producer(&epoch_id, tip.height).unwrap();

        let mut block = self.clients[0].produce_block(tip.height + 1).unwrap().unwrap();
        block.mut_header().resign(&create_test_signer(block_producer.as_str()));

        let _ = self.clients[0]
            .process_block_test_no_produce_chunk(block.into(), Provenance::NONE)
            .unwrap();

        for i in 0..self.clients[0].chain.epoch_length * 2 {
            self.produce_block(0, tip.height + i + 2);
        }
    }

    pub fn get_head_protocol_version(&self) -> ProtocolVersion {
        let tip = self.clients[0].chain.head().unwrap();
        self.clients[0].epoch_manager.get_epoch_protocol_version(&tip.epoch_id).unwrap()
    }

    pub fn query_account(&mut self, account_id: AccountId) -> AccountView {
        let client = &self.clients[0];
        let head = client.chain.head().unwrap();
        let last_block = client.chain.get_block(&head.last_block_hash).unwrap();
        let shard_id =
            client.epoch_manager.account_id_to_shard_id(&account_id, &head.epoch_id).unwrap();
        let shard_uid = client.epoch_manager.shard_id_to_uid(shard_id, &head.epoch_id).unwrap();
        let last_chunk_header = &last_block.chunks()[shard_id as usize];

        for i in 0..self.clients.len() {
            let tracks_shard = self.clients[i]
                .epoch_manager
                .cares_about_shard_from_prev_block(
                    &head.prev_block_hash,
                    &self.get_client_id(i),
                    shard_id,
                )
                .unwrap();
            if tracks_shard {
                let response = self.clients[i]
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
                    QueryResponseKind::ViewAccount(account_view) => return account_view,
                    _ => panic!("Wrong return value"),
                }
            }
        }
        panic!("No client tracks shard {}", shard_id);
    }

    /// Passes the given query to the runtime adapter using the current head and returns a result.
    pub fn query_view(&mut self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        let head = self.clients[0].chain.head().unwrap();
        let head_block = self.clients[0].chain.get_block(&head.last_block_hash).unwrap();
        self.clients[0].runtime_adapter.query(
            ShardUId::single_shard(),
            &head_block.chunks()[0].prev_state_root(),
            head.height,
            0,
            &head.prev_block_hash,
            &head.last_block_hash,
            head_block.header().epoch_id(),
            &request,
        )
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
        let account_id = self.get_client_id(idx);
        let rng_seed = match self.seeds.get(&account_id) {
            Some(seed) => *seed,
            None => TEST_SEED,
        };
        let vs = ValidatorSchedule::new().block_producers_per_epoch(vec![self.validators.clone()]);
        let num_validator_seats = vs.all_block_producers().count() as NumSeats;
        self.clients[idx] = setup_client_with_runtime(
            self.clock.clone(),
            num_validator_seats,
            false,
            self.network_adapters[idx].clone().as_multi_sender(),
            self.shards_manager_adapters[idx].clone(),
            self.chain_genesis.clone(),
            self.clients[idx].epoch_manager.clone(),
            self.clients[idx].shard_tracker.clone(),
            self.clients[idx].runtime_adapter.clone(),
            rng_seed,
            self.archive,
            self.save_trie_changes,
            None,
            self.clients[idx].partial_witness_adapter.clone(),
            self.clients[idx].validator_signer.get().unwrap(),
            self.clients[idx].resharding_sender.clone(),
        )
    }

    /// Returns an [`AccountId`] used by a client at given index.  More
    /// specifically, returns validator id of the client’s validator signer.
    pub fn get_client_id(&self, idx: usize) -> AccountId {
        let validator_signer = self.clients[idx].validator_signer.get();
        validator_signer.unwrap().validator_id().clone()
    }

    /// Returns the index of client with the given [`AccoountId`].
    pub fn get_client_index(&self, account_id: &AccountId) -> usize {
        self.account_indices.index(account_id)
    }

    /// Get block producer responsible for producing the block at height head.height + height_offset.
    /// Doesn't handle epoch boundaries with height_offset > 1. With offsets bigger than one,
    /// the function assumes that the epoch doesn't change after head.height + 1.
    pub fn get_block_producer_at_offset(&self, head: &Tip, height_offset: u64) -> AccountId {
        let client = &self.clients[0];
        let epoch_manager = &client.epoch_manager;
        let parent_hash = &head.last_block_hash;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
        let height = head.height + height_offset;

        epoch_manager.get_block_producer(&epoch_id, height).unwrap()
    }

    /// Get chunk producer responsible for producing the chunk at height head.height + height_offset.
    /// Doesn't handle epoch boundaries with height_offset > 1. With offsets bigger than one,
    /// the function assumes that the epoch doesn't change after head.height + 1.
    pub fn get_chunk_producer_at_offset(
        &self,
        head: &Tip,
        height_offset: u64,
        shard_id: ShardId,
    ) -> AccountId {
        let client = &self.clients[0];
        let epoch_manager = &client.epoch_manager;
        let parent_hash = &head.last_block_hash;
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(parent_hash).unwrap();
        let height = head.height + height_offset;

        epoch_manager.get_chunk_producer(&epoch_id, height, shard_id).unwrap()
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
            &signer.clone().into(),
            actions,
            tip.last_block_hash,
            0,
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
            &relayer_signer.into(),
            vec![Action::Delegate(Box::new(signed_delegate_action))],
            tip.last_block_hash,
            0,
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

    /// Print a short summary of all the blocks from genesis to head.
    pub fn print_summary(&self) {
        let client = &self.clients[0];

        let genesis_height = client.chain.genesis().height();
        let head_height = client.chain.head().unwrap().height;

        tracing::info!(target: "test", genesis_height, head_height, "printing summary");
        for height in genesis_height..head_height + 1 {
            self.print_block_summary(height);
        }
    }

    pub fn print_block_summary(&self, height: u64) {
        let client = &self.clients[0];
        let block = client.chain.get_block_by_height(height);
        let block = match block {
            Ok(block) => block,
            Err(err) => {
                tracing::info!(target: "test", ?err, "Block {}: missing", height);
                return;
            }
        };
        let prev_hash = block.header().prev_hash();
        let epoch_id = client.epoch_manager.get_epoch_id_from_prev_block(prev_hash).unwrap();
        let protocol_version = client.epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
        let latest_protocol_version = block.header().latest_protocol_version();

        let block_hash = block.hash();
        let chunk_mask = block.header().chunk_mask();

        tracing::info!(target: "test", height, ?block_hash, ?chunk_mask, protocol_version, latest_protocol_version, "block");
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

pub(crate) struct AccountIndices(pub(crate) HashMap<AccountId, usize>);

impl AccountIndices {
    pub fn index(&self, account_id: &AccountId) -> usize {
        self.0[account_id]
    }

    pub fn lookup<'a, T>(&self, container: &'a [T], account_id: &AccountId) -> &'a T {
        &container[self.0[account_id]]
    }

    pub fn lookup_mut<'a, T>(&self, container: &'a mut [T], account_id: &AccountId) -> &'a mut T {
        &mut container[self.0[account_id]]
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Timed out after {0:?}")]
pub struct TimeoutError(Duration);
