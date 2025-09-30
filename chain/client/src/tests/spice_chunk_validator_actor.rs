use assert_matches::assert_matches;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{Handler, IntoSender as _, Message, Sender};
use near_async::time::Clock;
use near_chain::spice_core::{CoreStatementsProcessor, ExecutionResultEndorsed};
use near_chain::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use near_chain::types::{
    ApplyChunkResult, ApplyChunkShardContext, RuntimeStorageConfig, StorageDataSource,
};
use near_chain::{
    ApplyChunksSpawner, Block, BlockProcessingArtifact, Chain, ChainGenesis, Provenance,
};
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{Genesis, MutableConfigValue};
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt as _;
use near_o11y::testonly::init_test_logger;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::ReceiptProof;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::spice_state_witness::{
    SpiceChunkStateTransition, SpiceChunkStateWitness,
};
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{ChunkExecutionResult, ChunkExecutionResultHash, ShardId};
use near_primitives::validator_signer::ValidatorSigner;
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::get_genesis_state_roots;
use node_runtime::SignedValidPeriodTransactions;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::chunk_executor_actor::ProcessedBlock;
use crate::spice_chunk_validator_actor::{SpiceChunkStateWitnessMessage, SpiceChunkValidatorActor};
use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;

const TEST_RECEIPTS: Vec<Receipt> = Vec::new();

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_valid_witness_adds_endorsement_to_core_state() {
    let mut actor = setup();
    let head = actor.chain_store.head().unwrap();
    let block = actor.chain_store.get_block(&head.last_block_hash).unwrap();
    let prev_block = actor.chain_store.get_block(&head.prev_block_hash).unwrap();

    let starting_state_root = test_starting_state_root(&actor);
    record_execution_results(&actor, &prev_block, starting_state_root);

    let witness_message = valid_witness_message(&actor, &block, &prev_block, &starting_state_root);
    let post_state_root = witness_message.witness.main_state_transition().post_state_root;

    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_none());
    actor.handle(witness_message.span_wrap());

    let block_execution_results =
        actor.core_processor.get_block_execution_results(&block).unwrap().unwrap();
    assert_eq!(block_execution_results.0.len(), 1);
    let shard_id = block.chunks()[0].shard_id();
    assert_eq!(
        block_execution_results.0.get(&shard_id).unwrap().chunk_extra.state_root(),
        &post_state_root
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_valid_witness_sends_endorsements() {
    let mut actor = {
        let genesis = TestGenesisBuilder::new()
            .validators_spec(ValidatorsSpec::desired_roles(
                &["test-validator-1", "test-validator-2"],
                &[],
            ))
            .build();
        setup_with_genesis(genesis, Arc::new(create_test_signer("test-validator-1")))
    };
    let head = actor.chain_store.head().unwrap();
    let block = actor.chain_store.get_block(&head.last_block_hash).unwrap();
    let prev_block = actor.chain_store.get_block(&head.prev_block_hash).unwrap();

    let starting_state_root = test_starting_state_root(&actor);
    record_execution_results(&actor, &prev_block, starting_state_root);

    let witness_message = valid_witness_message(&actor, &block, &prev_block, &starting_state_root);
    let post_state_root = witness_message.witness.main_state_transition().post_state_root;

    assert_matches!(actor.network_rc.try_recv(), Err(TryRecvError::Empty));
    actor.handle(witness_message.span_wrap());
    let msg = actor.network_rc.try_recv().unwrap();
    // Since we shouldn't send endorsement to ourselves we should only send one endorsement.
    assert_matches!(actor.network_rc.try_recv(), Err(TryRecvError::Empty));
    let PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ChunkEndorsement(
        account,
        endorsement,
    )) = msg
    else {
        panic!();
    };
    assert_eq!(account.as_str(), "test-validator-2");
    let receipts_root = test_receipt_proofs_root(&actor, &block);
    assert_eq!(
        endorsement,
        test_chunk_endorsement("test-validator-1", &block, receipts_root, post_state_root,)
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_invalid_witness_does_not_send_endorsements() {
    let mut actor = {
        let genesis = TestGenesisBuilder::new()
            .validators_spec(ValidatorsSpec::desired_roles(
                &["test-validator-1", "test-validator-2"],
                &[],
            ))
            .build();
        setup_with_genesis(genesis, Arc::new(create_test_signer("test-validator-1")))
    };
    let head = actor.chain_store.head().unwrap();
    let block = actor.chain_store.get_block(&head.last_block_hash).unwrap();
    let prev_block = actor.chain_store.get_block(&head.prev_block_hash).unwrap();

    let starting_state_root = test_starting_state_root(&actor);
    record_execution_results(&actor, &prev_block, starting_state_root);

    let witness_message =
        invalid_witness_message(&actor, &block, &prev_block, &starting_state_root);

    actor.handle(witness_message.span_wrap());
    assert_matches!(actor.network_rc.try_recv(), Err(TryRecvError::Empty));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_invalid_witness_does_not_record_endorsement_in_core() {
    let mut actor = setup();
    let head = actor.chain_store.head().unwrap();
    let block = actor.chain_store.get_block(&head.last_block_hash).unwrap();
    let prev_block = actor.chain_store.get_block(&head.prev_block_hash).unwrap();

    let starting_state_root = test_starting_state_root(&actor);
    record_execution_results(&actor, &prev_block, starting_state_root);

    let witness_message =
        invalid_witness_message(&actor, &block, &prev_block, &starting_state_root);

    actor.handle(witness_message.span_wrap());
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_none());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_arriving_before_block() {
    let mut actor = setup();
    let head = actor.chain_store.head().unwrap();
    let prev_block = actor.chain_store.get_block(&head.last_block_hash).unwrap();

    let starting_state_root = test_starting_state_root(&actor);
    record_execution_results(&actor, &prev_block, starting_state_root);

    let block = build_block(&actor.chain, &prev_block);
    let witness_message = valid_witness_message(&actor, &block, &prev_block, &starting_state_root);

    actor.handle(witness_message.span_wrap());
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_none());

    process_block_sync(
        &mut actor.chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *block.hash() });
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_some());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_arriving_before_execution_results_for_parent() {
    let mut actor = setup();
    let head = actor.chain_store.head().unwrap();
    let block = actor.chain_store.get_block(&head.last_block_hash).unwrap();
    let prev_block = actor.chain_store.get_block(&head.prev_block_hash).unwrap();

    let starting_state_root = test_starting_state_root(&actor);

    let witness_message = valid_witness_message(&actor, &block, &prev_block, &starting_state_root);

    actor.handle(witness_message.span_wrap());
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_none());

    record_execution_results(&actor, &prev_block, starting_state_root);
    actor.handle(ExecutionResultEndorsed { block_hash: *prev_block.hash() });
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_some());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_arriving_before_block_and_execution_results() {
    let mut actor = setup();
    let head = actor.chain_store.head().unwrap();
    let prev_block = actor.chain_store.get_block(&head.last_block_hash).unwrap();

    let starting_state_root = test_starting_state_root(&actor);

    let block = build_block(&actor.chain, &prev_block);
    let witness_message = valid_witness_message(&actor, &block, &prev_block, &starting_state_root);

    actor.handle(witness_message.span_wrap());
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_none());

    process_block_sync(
        &mut actor.chain,
        block.clone().into(),
        Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
    )
    .unwrap();
    actor.handle(ProcessedBlock { block_hash: *block.hash() });
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_none());

    record_execution_results(&actor, &prev_block, starting_state_root);
    actor.handle(ExecutionResultEndorsed { block_hash: *prev_block.hash() });
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_some());
}

struct FakeSpawner {
    sc: UnboundedSender<Box<dyn FnOnce() + Send>>,
}

impl FakeSpawner {
    fn new() -> (FakeSpawner, UnboundedReceiver<Box<dyn FnOnce() + Send>>) {
        let (sc, rc) = unbounded_channel();
        (Self { sc }, rc)
    }
}

impl AsyncComputationSpawner for FakeSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        self.sc.send(f).unwrap();
    }
}

struct TestActor {
    actor: SpiceChunkValidatorActor,
    tasks_rc: UnboundedReceiver<Box<dyn FnOnce() + Send>>,
    network_rc: UnboundedReceiver<PeerManagerMessageRequest>,
    chain: Chain,

    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    core_processor: CoreStatementsProcessor,
}

impl<M> Handler<M> for TestActor
where
    M: Message,
    SpiceChunkValidatorActor: Handler<M>,
{
    fn handle(&mut self, msg: M) {
        self.actor.handle(msg);
        while let Ok(task) = self.tasks_rc.try_recv() {
            task();
        }
    }
}

fn build_block(chain: &Chain, prev_block: &Block) -> Arc<Block> {
    let block_producer = chain
        .epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    TestBlockBuilder::new(Clock::real(), prev_block, signer)
        .chunks(get_fake_next_block_chunk_headers(&prev_block, chain.epoch_manager.as_ref()))
        .spice_core_statements(vec![])
        .build()
}

fn setup_with_genesis(genesis: Genesis, signer: Arc<ValidatorSigner>) -> TestActor {
    init_test_logger();
    let chain = get_chain_with_genesis(Clock::real(), genesis.clone());
    let epoch_manager = chain.epoch_manager.clone();
    let runtime = chain.runtime_adapter.clone();

    let spice_core_processor = CoreStatementsProcessor::new_with_noop_senders(
        runtime.store().chain_store(),
        epoch_manager.clone(),
    );
    let chunk_endorsement_tracker = Arc::new(ChunkEndorsementTracker::new(
        epoch_manager.clone(),
        runtime.store().clone(),
        spice_core_processor.clone(),
    ));

    let (network_sc, network_rc) = unbounded_channel();
    let network_adapter = PeerManagerAdapter {
        async_request_sender: near_async::messaging::noop().into_sender(),
        set_chain_info_sender: near_async::messaging::noop().into_sender(),
        state_sync_event_sender: near_async::messaging::noop().into_sender(),
        request_sender: Sender::from_fn({
            move |event: PeerManagerMessageRequest| {
                network_sc.send(event).unwrap();
            }
        }),
    };

    let (spawner, tasks_rc) = FakeSpawner::new();

    let validator_signer = MutableConfigValue::new(Some(signer), "validator_signer");
    let mut actor = TestActor {
        actor: SpiceChunkValidatorActor::new(
            runtime.store().clone(),
            &ChainGenesis::new(&genesis.config),
            runtime.clone(),
            epoch_manager.clone(),
            network_adapter,
            validator_signer,
            spice_core_processor.clone(),
            chunk_endorsement_tracker,
            ApplyChunksSpawner::Custom(Arc::new(spawner)),
        ),
        tasks_rc,
        network_rc,
        chain_store: runtime.store().chain_store(),
        epoch_manager,
        core_processor: spice_core_processor,
        chain,
    };

    let mut prev_block = actor.chain.genesis_block();
    for _ in 0..3 {
        let block = build_block(&actor.chain, &prev_block);
        process_block_sync(
            &mut actor.chain,
            block.clone().into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
        prev_block = block;
    }

    actor
}

fn setup() -> TestActor {
    let validator = "test-validator";
    let genesis = TestGenesisBuilder::new()
        .validators_spec(ValidatorsSpec::desired_roles(&[validator], &[]))
        .build();
    setup_with_genesis(genesis, Arc::new(create_test_signer(&validator)))
}

fn test_chunk_endorsement(
    validator: &str,
    block: &Block,
    outgoing_receipts_root: CryptoHash,
    state_root: CryptoHash,
) -> ChunkEndorsement {
    assert_eq!(block.chunks().len(), 1);
    ChunkEndorsement::new_with_execution_result(
        *block.header().epoch_id(),
        ChunkExecutionResult {
            chunk_extra: ChunkExtra::new_with_only_state_root(&state_root),
            outgoing_receipts_root,
        },
        *block.hash(),
        block.chunks()[0].shard_id(),
        block.chunks()[0].height_created(),
        &create_test_signer(&validator),
    )
}

fn test_receipt_proofs_root(actor: &TestActor, block: &Block) -> CryptoHash {
    let epoch_id = block.header().epoch_id();
    let shard_layout = actor.epoch_manager.get_shard_layout(epoch_id).unwrap();
    let shard_ids: Vec<_> = shard_layout.shard_ids().collect();
    assert_eq!(shard_ids.len(), 1);
    let (root, _) = Chain::create_receipts_proofs_from_outgoing_receipts(
        &shard_layout,
        shard_ids[0],
        TEST_RECEIPTS,
    )
    .unwrap();
    root
}

fn test_receipt_proofs(actor: &TestActor, block: &Block) -> HashMap<ShardId, ReceiptProof> {
    let epoch_id = block.header().epoch_id();
    let chunks = block.chunks();
    assert_eq!(chunks.len(), 1);
    let chunk_header = &chunks[0];
    let shard_layout = actor.epoch_manager.get_shard_layout(epoch_id).unwrap();
    let (_, mut proofs) = Chain::create_receipts_proofs_from_outgoing_receipts(
        &shard_layout,
        chunk_header.shard_id(),
        TEST_RECEIPTS,
    )
    .unwrap();
    assert_eq!(proofs.len(), 1);
    HashMap::from([(chunk_header.shard_id(), proofs.remove(0))])
}

fn record_execution_results(actor: &TestActor, block: &Block, state_root: CryptoHash) {
    let epoch_id = block.header().epoch_id();
    let chunks = block.chunks();
    assert_eq!(chunks.len(), 1);
    let chunk_header = &chunks[0];
    let receipts_root = test_receipt_proofs_root(&actor, block);
    let chunk_validator_assignments = actor
        .epoch_manager
        .get_chunk_validator_assignments(
            epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )
        .unwrap();
    for (account, _) in chunk_validator_assignments.assignments() {
        let endorsement =
            test_chunk_endorsement(account.as_str(), &block, receipts_root, state_root);
        actor.core_processor.record_chunk_endorsement(endorsement).unwrap();
    }
    assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_some());
}

fn test_starting_state_root(actor: &TestActor) -> CryptoHash {
    get_genesis_state_roots(&actor.chain_store.store()).unwrap().unwrap()[0]
}

fn simulate_chunk_application(
    actor: &TestActor,
    block: &Block,
    prev_block: &Block,
    starting_state_root: &CryptoHash,
) -> (SpiceChunkStateTransition, ChunkExecutionResultHash) {
    let chunks = block.chunks();
    assert_eq!(chunks.len(), 1);
    let chunk_header = &chunks[0];
    let transactions = SignedValidPeriodTransactions::new(vec![], vec![]);
    let apply_result = actor
        .chain
        .runtime_adapter
        .apply_chunk(
            RuntimeStorageConfig {
                state_root: *starting_state_root,
                use_flat_storage: true,
                source: StorageDataSource::Db,
                state_patch: near_primitives::sandbox::state_patch::SandboxStatePatch::default(),
            },
            ApplyChunkReason::UpdateTrackedShard,
            ApplyChunkShardContext {
                shard_id: chunk_header.shard_id(),
                last_validator_proposals: chunk_header.prev_validator_proposals(),
                gas_limit: chunk_header.gas_limit(),
                is_new_chunk: true,
            },
            {
                let is_new_chunk = true;
                Chain::get_apply_chunk_block_context(&block, prev_block.header(), is_new_chunk)
                    .unwrap()
            },
            &TEST_RECEIPTS,
            transactions,
        )
        .unwrap();

    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    let chunk_extra = ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals.clone(),
        apply_result.total_gas_burnt,
        chunk_header.gas_limit(),
        apply_result.total_balance_burnt,
        apply_result.congestion_info,
        apply_result.bandwidth_requests.clone(),
    );
    let shard_layout = actor.epoch_manager.get_shard_layout(block.header().epoch_id()).unwrap();
    let (outgoing_receipts_root, _) = Chain::create_receipts_proofs_from_outgoing_receipts(
        &shard_layout,
        chunk_header.shard_id(),
        apply_result.outgoing_receipts,
    )
    .unwrap();
    let execution_result = ChunkExecutionResult { chunk_extra, outgoing_receipts_root };

    (
        SpiceChunkStateTransition {
            base_state: apply_result.proof.unwrap().nodes,
            post_state_root: apply_result.new_root,
        },
        execution_result.compute_hash(),
    )
}

fn test_witness_message(
    block: &Block,
    state_transition: SpiceChunkStateTransition,
    chunk_execution_result_hash: ChunkExecutionResultHash,
    receipt_proofs: HashMap<ShardId, ReceiptProof>,
    receipts_hash: CryptoHash,
) -> SpiceChunkStateWitnessMessage {
    let chunks = block.chunks();
    assert_eq!(chunks.len(), 1);
    let chunk_header = &chunks[0];
    let transactions = vec![];
    let witness = SpiceChunkStateWitness::new(
        near_primitives::types::SpiceChunkId {
            block_hash: *block.hash(),
            shard_id: chunk_header.shard_id(),
        },
        state_transition,
        receipt_proofs,
        receipts_hash,
        transactions,
        chunk_execution_result_hash,
    );
    let witness_size = borsh::object_length(&witness).unwrap();
    SpiceChunkStateWitnessMessage { witness, raw_witness_size: witness_size }
}

fn valid_witness_message(
    actor: &TestActor,
    block: &Block,
    prev_block: &Block,
    starting_state_root: &CryptoHash,
) -> SpiceChunkStateWitnessMessage {
    let (state_transition, execution_result_hash) =
        simulate_chunk_application(&actor, &block, &prev_block, &starting_state_root);
    let receipt_proofs = test_receipt_proofs(&actor, &prev_block);
    let receipts_hash = hash(&borsh::to_vec(&TEST_RECEIPTS).unwrap());
    test_witness_message(
        &block,
        state_transition,
        execution_result_hash,
        receipt_proofs,
        receipts_hash,
    )
}

fn invalid_witness_message(
    actor: &TestActor,
    block: &Block,
    prev_block: &Block,
    starting_state_root: &CryptoHash,
) -> SpiceChunkStateWitnessMessage {
    let (state_transition, execution_result_hash) = {
        let (mut transition, execution_result_hash) =
            simulate_chunk_application(&actor, &block, &prev_block, &starting_state_root);
        transition.post_state_root = CryptoHash::default();
        (transition, execution_result_hash)
    };
    let receipt_proofs = test_receipt_proofs(&actor, &prev_block);
    let receipts_hash = hash(&borsh::to_vec(&TEST_RECEIPTS).unwrap());
    test_witness_message(
        &block,
        state_transition,
        execution_result_hash,
        receipt_proofs,
        receipts_hash,
    )
}
