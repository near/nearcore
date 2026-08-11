//! The spice actors must be inert on a chain whose protocol version predates
//! spice activation, even though a spice build spawns them unconditionally.

use crate::spice::chunk_executor_actor::ChunkExecutorActor;
use crate::spice::chunk_validator_actor::{
    SpiceChunkStateWitnessMessage, SpiceChunkValidatorActor,
};
use crate::spice::data_distributor_actor::{
    SpiceDataDistributorActor, SpiceDataDistributorAdapter,
};
use assert_matches::assert_matches;
use itertools::Itertools as _;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{Actor, Handler, IntoAsyncSender as _, IntoSender as _, Sender, noop};
use near_async::test_utils::FakeDelayedActionRunner;
use near_async::time::Clock;
use near_chain::spice::chunk_application::ChunkPersistenceConfig;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::{ProcessedBlock, SpiceCoreWriterActor};
use near_chain::test_utils::{get_chain_with_genesis, process_block_sync};
use near_chain::{
    ApplyChunksSpawner, Block, BlockProcessingArtifact, Chain, ChainGenesis, ChainStoreAccess as _,
    Error, Provenance,
};
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{Genesis, MutableConfigValue, TrackedShardsConfig};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::spice::data_distribution::{
    SpiceChunkContractAccessesMessage, SpiceContractCodeRequestMessage,
    SpiceContractCodeResponseMessage, SpiceIncomingPartialData, SpicePartialDataRequest,
    SpicePartialDataRequestMessage,
};
use near_network::types::{
    NetworkRequestWithPermit, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt as _;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::partial_data::{
    SpiceDataCommitment, SpiceDataIdentifier, testonly_create_spice_partial_data,
};
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{
    SpiceChunkContractAccesses, SpiceContractCodeRequest, SpiceContractCodeResponse,
};
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, ChunkExecutionResult, SpiceChunkId};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use near_store::DBCol;
use near_store::adapter::StoreAdapter as _;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::str::FromStr as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// A protocol version that is supported by this binary but predates spice
fn pre_spice_protocol_version() -> ProtocolVersion {
    ProtocolFeature::Spice.protocol_version() - 1
}

/// The drop counters are process-global, so the tests that assert on their deltas
/// must not run concurrently with one another.
static DROP_COUNTERS: Mutex<()> = Mutex::new(());

/// Serializes access to the global drop counters. Poisoning is ignored: the test
/// that panicked has already failed, and failing every other test along with it
/// only hides which one broke.
fn lock_drop_counters() -> MutexGuard<'static, ()> {
    DROP_COUNTERS.lock().unwrap_or_else(|err| err.into_inner())
}

/// Spice columns that must stay untouched on a pre-spice chain.
///
/// `DBCol::all_next_block_hashes()` is deliberately absent: `ChainStoreUpdate` writes
/// it for every block gated on `cfg!(feature = "protocol_feature_spice")` rather than
/// on the block's own protocol version.
fn spice_columns() -> Vec<DBCol> {
    vec![
        DBCol::receipt_proofs(),
        DBCol::witnesses(),
        DBCol::endorsements(),
        DBCol::execution_results(),
        DBCol::uncertified_execution_results(),
        DBCol::uncertified_chunks(),
        DBCol::spice_endorsement_stats(),
        DBCol::contract_accesses(),
        DBCol::chunk_certifying_block(),
    ]
}

fn assert_spice_columns_empty(chain: &Chain) {
    let store = chain.chain_store.store();
    for col in spice_columns() {
        let count = store.iter(col).count();
        assert_eq!(count, 0, "spice column {col:?} was written on a pre-spice chain");
    }
}

fn dropped_count(kind: &str) -> u64 {
    near_chain::metrics::SPICE_PRE_ACTIVATION_MESSAGES_DROPPED.with_label_values(&[kind]).get()
}

const VALIDATOR: &str = "test-producer-0";

/// A pre-spice chain with a handful of non-spice blocks on top of genesis.
fn setup() -> (Genesis, Chain) {
    init_test_logger();

    let producers = (0..2).map(|i| format!("test-producer-{i}")).collect_vec();
    let validators_spec =
        ValidatorsSpec::desired_roles(&producers.iter().map(String::as_str).collect_vec(), &[]);

    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .protocol_version(pre_spice_protocol_version())
        .shard_layout(ShardLayout::multi_shard(2, 0))
        .validators_spec(validators_spec)
        .build();

    let mut chain = get_chain_with_genesis(Clock::real(), genesis.clone());
    for _ in 0..3 {
        let head = chain.chain_store.head().unwrap();
        let prev_block = chain.chain_store.get_block(&head.last_block_hash).unwrap();
        let block = build_pre_spice_block(chain.epoch_manager.as_ref(), &prev_block);
        process_block_sync(
            &mut chain,
            block.into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
    }
    (genesis, chain)
}

/// A pre-spice block: pre-spice header and body, and no new chunks. Chunks
/// carried over from the parent keep the block valid without having to forge
/// endorsement signatures, and missing chunks are legal pre-spice anyway.
fn build_pre_spice_block(
    epoch_manager: &dyn EpochManagerAdapter,
    prev_block: &Block,
) -> Arc<Block> {
    let block_producer = epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    let block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
        .protocol_version(pre_spice_protocol_version())
        .build();
    assert!(!block.is_spice_block(), "test must build pre-spice blocks");
    assert!(!block.header().is_spice(), "test must build pre-spice headers");
    block
}

fn latest_block(chain: &Chain) -> Arc<Block> {
    let head = chain.chain_store.head().unwrap();
    chain.chain_store.get_block(&head.last_block_hash).unwrap()
}

fn core_reader(chain: &Chain) -> SpiceCoreReader {
    SpiceCoreReader::new(
        chain.chain_store.store().chain_store(),
        chain.epoch_manager.clone(),
        Gas::from_teragas(100),
    )
}

/// A `PeerManagerAdapter` that counts every outbound request, so a test can
/// assert the actors stayed silent.
fn counting_network_adapter(sent: Arc<AtomicUsize>) -> PeerManagerAdapter {
    PeerManagerAdapter {
        async_request_sender: noop().into_async_sender(),
        set_chain_info_sender: noop().into_sender(),
        state_sync_event_sender: noop().into_sender(),
        request_sender: Sender::from_fn({
            let sent = sent.clone();
            move |_: PeerManagerMessageRequest| {
                sent.fetch_add(1, Ordering::Relaxed);
            }
        }),
        request_with_permit_sender: Sender::from_fn(move |_: NetworkRequestWithPermit| {
            sent.fetch_add(1, Ordering::Relaxed);
        }),
    }
}

/// A sender that counts messages instead of delivering them, so a test can
/// assert nothing was forwarded between actors.
fn counting_sender<M: 'static>(forwarded: Arc<AtomicUsize>) -> Sender<M> {
    Sender::from_fn(move |_: M| {
        forwarded.fetch_add(1, Ordering::Relaxed);
    })
}

/// A sender that parks messages so a test can hand them on to the receiving actor
/// itself. Used for the hops that production genuinely takes, where counting the
/// message as a failure would be wrong.
fn collecting_sender<M: 'static + Send>(collected: Arc<Mutex<Vec<M>>>) -> Sender<M> {
    Sender::from_fn(move |msg: M| {
        collected.lock().unwrap().push(msg);
    })
}

/// Counts scheduled chunk applies. A pre-spice chain must schedule none.
struct CountingSpawner {
    spawned: Arc<AtomicUsize>,
}

impl AsyncComputationSpawner for CountingSpawner {
    fn spawn_boxed(&self, _name: &str, _f: Box<dyn FnOnce() + Send>) {
        self.spawned.fetch_add(1, Ordering::Relaxed);
    }
}

/// The four spice actors, wired so that any outbound traffic is counted and any
/// inter-actor message is a test failure (nothing should be forwarded), except for
/// the distributor's two forwarding hops, which the test replays by hand.
struct Actors {
    executor: ChunkExecutorActor,
    validator: SpiceChunkValidatorActor,
    distributor: SpiceDataDistributorActor,
    core_writer: SpiceCoreWriterActor,
    sent: Arc<AtomicUsize>,
    forwarded: Arc<AtomicUsize>,
    spawned: Arc<AtomicUsize>,
    forwarded_accesses: Arc<Mutex<Vec<SpiceChunkContractAccessesMessage>>>,
    forwarded_code_responses: Arc<Mutex<Vec<SpiceContractCodeResponseMessage>>>,
}

impl Actors {
    fn new(genesis: &Genesis, chain: &Chain) -> Self {
        let sent = Arc::new(AtomicUsize::new(0));
        let forwarded = Arc::new(AtomicUsize::new(0));
        let spawned = Arc::new(AtomicUsize::new(0));
        let forwarded_accesses = Arc::new(Mutex::new(Vec::new()));
        let forwarded_code_responses = Arc::new(Mutex::new(Vec::new()));

        let validator_signer = MutableConfigValue::new(
            Some(Arc::new(create_test_signer(VALIDATOR))),
            "validator_signer",
        );
        let epoch_manager = chain.epoch_manager.clone();
        let chain_genesis = ChainGenesis::new(&genesis.config);
        let runtime = chain.runtime_adapter.clone();
        let shard_tracker = ShardTracker::new(
            TrackedShardsConfig::AllShards,
            epoch_manager.clone(),
            validator_signer.clone(),
        );
        let executor = ChunkExecutorActor::new(
            runtime.store().clone(),
            &chain_genesis,
            runtime.clone(),
            epoch_manager.clone(),
            shard_tracker.clone(),
            counting_network_adapter(sent.clone()),
            validator_signer.clone(),
            Arc::new(CountingSpawner { spawned: spawned.clone() }),
            counting_sender(forwarded.clone()),
            counting_sender(forwarded.clone()),
            SpiceDataDistributorAdapter {
                receipts: counting_sender(forwarded.clone()),
                witness: counting_sender(forwarded.clone()),
            },
            ChunkPersistenceConfig::default(),
        );
        let validator = SpiceChunkValidatorActor::new(
            runtime.store().clone(),
            &chain_genesis,
            runtime.clone(),
            epoch_manager.clone(),
            counting_network_adapter(sent.clone()),
            validator_signer.clone(),
            counting_sender(forwarded.clone()),
            ApplyChunksSpawner::default(),
        );
        let distributor = SpiceDataDistributorActor::new(
            epoch_manager.clone(),
            runtime.store().chain_store(),
            validator_signer.clone(),
            shard_tracker,
            core_reader(chain),
            counting_network_adapter(sent.clone()),
            counting_sender(forwarded.clone()),
            counting_sender(forwarded.clone()),
            collecting_sender(forwarded_accesses.clone()),
            collecting_sender(forwarded_code_responses.clone()),
        );
        let core_writer = SpiceCoreWriterActor::new(
            runtime.store().chain_store(),
            epoch_manager,
            validator_signer,
            core_reader(chain),
            counting_sender(forwarded.clone()),
            counting_sender(forwarded.clone()),
        );
        Actors {
            executor,
            validator,
            distributor,
            core_writer,
            sent,
            forwarded,
            spawned,
            forwarded_accesses,
            forwarded_code_responses,
        }
    }

    fn start(&mut self) {
        start_actor(&mut self.executor);
        start_actor(&mut self.validator);
        start_actor(&mut self.distributor);
        start_actor(&mut self.core_writer);
    }

    /// Fan `ProcessedBlock` to all four actors exactly as `ClientActor` does for
    /// every accepted block.
    fn process_block(&mut self, block_hash: CryptoHash) {
        self.executor.handle(ProcessedBlock { block_hash });
        self.validator.handle(ProcessedBlock { block_hash });
        self.distributor.handle(ProcessedBlock { block_hash });
        self.core_writer.handle(ProcessedBlock { block_hash });
    }

    /// Deliver everything the distributor forwarded to the validator, completing the
    /// production route for the kinds the network hands to the distributor.
    fn forward_to_validator(&mut self) {
        let accesses: Vec<_> = self.forwarded_accesses.lock().unwrap().drain(..).collect();
        for msg in accesses {
            self.validator.handle(msg);
        }
        let responses: Vec<_> = self.forwarded_code_responses.lock().unwrap().drain(..).collect();
        for msg in responses {
            self.validator.handle(msg);
        }
    }

    fn assert_silent(&self) {
        assert_eq!(self.sent.load(Ordering::Relaxed), 0, "spice actor sent a network request");
        assert_eq!(self.forwarded.load(Ordering::Relaxed), 0, "spice actor forwarded a message");
        assert_eq!(self.spawned.load(Ordering::Relaxed), 0, "spice actor scheduled a chunk apply");
        assert!(
            self.forwarded_accesses.lock().unwrap().is_empty(),
            "distributor forwarded contract accesses that the test never delivered",
        );
        assert!(
            self.forwarded_code_responses.lock().unwrap().is_empty(),
            "distributor forwarded a contract code response that the test never delivered",
        );
    }
}

fn start_actor<A: Actor>(actor: &mut A) {
    let mut runner = FakeDelayedActionRunner::default();
    actor.start_actor(&mut runner);
    runner.run_queued_actions(actor);
}

fn chunk_id(block: &Block) -> SpiceChunkId {
    SpiceChunkId { block_hash: *block.hash(), shard_id: block.chunks()[0].shard_id() }
}

/// A spice block must not enter a pre-spice epoch at all. `validate_header` rejects it
/// on the header's spice-ness, so it never reaches the actors — which is what lets the
/// actors gate on `is_spice()` rather than re-deriving spice-ness from the epoch.
///
/// This offers a wholly spice block (spice header *and* body). The narrower forgery the
/// header check exists for — a spice header on a non-spice body — is not constructible
/// from here, because `TestBlockBuilder` derives both from one protocol version and
/// `Block::new_block` is `pub(crate)` to `near-primitives`. Asserting on
/// `InvalidProtocolVersion` still pins the header check as the rejecting one: the
/// body-side check in `preprocess_block` returns `Error::Other`.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_block_is_rejected_in_pre_spice_epoch() {
    let (_genesis, mut chain) = setup();
    let prev_block = latest_block(&chain);
    let block_producer = chain
        .epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    let block = TestBlockBuilder::from_prev_block(Clock::real(), &prev_block, signer)
        .protocol_version(PROTOCOL_VERSION)
        .build();
    assert!(block.header().is_spice(), "test must offer a spice header");

    let err = process_block_sync(
        &mut chain,
        block.into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .expect_err("a spice block must not be accepted in a pre-spice epoch");
    assert_matches!(err, Error::InvalidProtocolVersion);
    assert_spice_columns_empty(&chain);
}

/// Actor startup must be inert on a pre-spice chain.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_actor_startup_is_inert_on_pre_spice_chain() {
    let (genesis, chain) = setup();
    let mut actors = Actors::new(&genesis, &chain);

    actors.start();

    actors.assert_silent();
    assert_spice_columns_empty(&chain);
}

/// `ProcessedBlock` for a pre-spice block must be a no-op in every actor.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_processed_block_is_inert_on_pre_spice_chain() {
    let (genesis, chain) = setup();
    let mut actors = Actors::new(&genesis, &chain);

    let mut block = latest_block(&chain);
    let mut hashes = vec![*block.hash()];
    while !block.header().is_genesis() {
        block = chain.chain_store.get_block(block.header().prev_hash()).unwrap();
        hashes.push(*block.hash());
    }
    for block_hash in hashes.into_iter().rev() {
        actors.process_block(block_hash);
    }

    actors.assert_silent();
    assert_spice_columns_empty(&chain);
}

/// Each spice message kind the network can route to us is dropped, counted, and
/// leaves no trace when spice is not active in the referencing block's epoch.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_network_messages_are_dropped_on_pre_spice_chain() {
    let _drop_counters = lock_drop_counters();
    let (genesis, chain) = setup();
    let mut actors = Actors::new(&genesis, &chain);
    let block = latest_block(&chain);
    let signer = create_test_signer(VALIDATOR);
    let requester = AccountId::from_str(VALIDATOR).unwrap();

    // Each entry: the metric label, and a closure that delivers that kind.
    let before: HashMap<&str, u64> = [
        "chunk_endorsement",
        "partial_data",
        "partial_data_request",
        "contract_accesses",
        "contract_code_request",
        "contract_code_response",
        "state_witness",
    ]
    .into_iter()
    .map(|kind| (kind, dropped_count(kind)))
    .collect();

    actors.core_writer.handle(SpiceChunkEndorsementMessage(
        SpiceChunkEndorsement::new(chunk_id(&block), new_test_execution_result(), &signer),
        RecvMessagePermit::none(),
    ));

    let data_id = SpiceDataIdentifier::Witness {
        block_hash: *block.hash(),
        shard_id: block.chunks()[0].shard_id(),
    };
    actors.distributor.handle(SpiceIncomingPartialData {
        data: testonly_create_spice_partial_data(
            data_id.clone(),
            SpiceDataCommitment {
                hash: CryptoHash::default(),
                root: CryptoHash::default(),
                encoded_length: 0,
            },
            vec![],
            Default::default(),
            requester.clone(),
        ),
        recv_permit: RecvMessagePermit::none(),
    });
    actors.distributor.handle(SpicePartialDataRequestMessage {
        request: SpicePartialDataRequest { data_id, requester },
        recv_permit: RecvMessagePermit::none(),
    });
    actors.distributor.handle(SpiceContractCodeRequestMessage(
        SpiceContractCodeRequest::new(chunk_id(&block), HashSet::new(), &signer),
        RecvMessagePermit::none(),
    ));

    actors.distributor.handle(SpiceChunkContractAccessesMessage(
        SpiceChunkContractAccesses::new(chunk_id(&block), HashSet::new(), &signer),
        RecvMessagePermit::none(),
    ));
    actors.distributor.handle(SpiceContractCodeResponseMessage(
        SpiceContractCodeResponse::encode(chunk_id(&block), &vec![]).unwrap(),
        RecvMessagePermit::none(),
    ));
    actors.forward_to_validator();

    actors.validator.handle(
        SpiceChunkStateWitnessMessage { witness: new_test_witness(&block), raw_witness_size: 0 }
            .span_wrap(),
    );

    for (kind, before) in before {
        assert_eq!(dropped_count(kind), before + 1, "expected exactly one dropped {kind} message",);
    }
    actors.assert_silent();
    assert_spice_columns_empty(&chain);
}

/// A spice message referencing a block we do not have falls back to the head, so a
/// pre-spice head still drops it. This is the branch a peer reaches by naming a
/// block hash we have never seen.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_message_for_unknown_block_is_dropped_on_pre_spice_chain() {
    let _drop_counters = lock_drop_counters();
    let (genesis, chain) = setup();
    let mut actors = Actors::new(&genesis, &chain);

    let shard_id = latest_block(&chain).chunks()[0].shard_id();
    let block_hash = CryptoHash::hash_bytes(b"a block this node has never seen");
    assert!(
        chain.chain_store.get_block_header(&block_hash).is_err(),
        "test must name a block the store does not have",
    );

    let before = dropped_count("chunk_endorsement");
    actors.core_writer.handle(SpiceChunkEndorsementMessage(
        SpiceChunkEndorsement::new(
            SpiceChunkId { block_hash, shard_id },
            new_test_execution_result(),
            &create_test_signer(VALIDATOR),
        ),
        RecvMessagePermit::none(),
    ));

    assert_eq!(
        dropped_count("chunk_endorsement"),
        before + 1,
        "expected the endorsement to be dropped via the head fallback",
    );
    actors.assert_silent();
    assert_spice_columns_empty(&chain);
}

fn new_test_execution_result() -> ChunkExecutionResult {
    ChunkExecutionResult {
        chunk_extra: ChunkExtra::new_with_only_state_root(&CryptoHash::default()),
        outgoing_receipts_root: CryptoHash::default(),
    }
}

fn new_test_witness(block: &Block) -> SpiceChunkStateWitness {
    SpiceChunkStateWitness::new(
        chunk_id(block),
        PartialState::TrieValues(vec![]),
        HashMap::new(),
        CryptoHash::default(),
        vec![],
        BTreeSet::new(),
        None,
    )
}
