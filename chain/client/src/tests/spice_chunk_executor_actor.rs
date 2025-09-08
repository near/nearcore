use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use itertools::Itertools as _;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::Handler;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_async::time::Clock;
use near_chain::ChainStoreAccess;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain::spice_core::ExecutionResultEndorsed;
use near_chain::stateless_validation::chunk_validation::{
    MainStateTransitionCache, validate_chunk_state_witness,
};
use near_chain::stateless_validation::spice_chunk_validation::spice_pre_validate_chunk_state_witness;
use near_chain::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use near_chain::{Block, Chain, ChainGenesis};
use near_chain::{BlockProcessingArtifact, Provenance};
use near_chain_configs::MutableValidatorSigner;
use near_chain_configs::test_genesis::{ONE_NEAR, TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{Genesis, MutableConfigValue, TrackedShardsConfig};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptPriority};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ShardChunk;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
use near_primitives::types::AccountId;
use near_primitives::types::ShardId;
use near_primitives::types::{BlockExecutionResults, ChunkExecutionResult, NumShards};
use near_store::ShardUId;
use near_store::adapter::StoreAdapter as _;
use reed_solomon_erasure::ReedSolomon;
use std::collections::HashMap;
use std::str::FromStr as _;
use std::sync::Arc;

use crate::chunk_executor_actor::ChunkExecutorActor;
use crate::chunk_executor_actor::ExecutorApplyChunksDone;
use crate::chunk_executor_actor::ExecutorIncomingReceipts;
use crate::chunk_executor_actor::ProcessedBlock;
use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;

struct FakeSpawner {
    sc: UnboundedSender<Box<dyn FnOnce() + Send>>,
}

impl FakeSpawner {
    fn new() -> (FakeSpawner, UnboundedReceiver<Box<dyn FnOnce() + Send>>) {
        let (sc, rc) = unbounded();
        (Self { sc }, rc)
    }
}

impl AsyncComputationSpawner for FakeSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        self.sc.unbounded_send(f).unwrap();
    }
}

struct TestActor {
    actor: ChunkExecutorActor,
    actor_rc: UnboundedReceiver<ExecutorApplyChunksDone>,
    tasks_rc: UnboundedReceiver<Box<dyn FnOnce() + Send>>,
    chain: Chain,
}

impl<M> Handler<M> for TestActor
where
    M: actix::Message,
    ChunkExecutorActor: Handler<M>,
{
    fn handle(&mut self, msg: M) {
        self.actor.handle(msg);
    }
}

impl TestActor {
    fn new(
        genesis: Genesis,
        validator_signer: MutableValidatorSigner,
        shards: Vec<ShardUId>,
        network_sc: UnboundedSender<PeerManagerMessageRequest>,
    ) -> TestActor {
        let chain = get_chain_with_genesis(Clock::real(), genesis.clone());
        let epoch_manager = chain.epoch_manager.clone();

        let shard_tracker = ShardTracker::new(
            TrackedShardsConfig::Shards(shards),
            epoch_manager.clone(),
            validator_signer.clone(),
        );

        let chain_genesis = ChainGenesis::new(&genesis.config);
        let runtime = chain.runtime_adapter.clone();
        let genesis_hash = *chain.genesis().hash();

        let spice_core_processor = CoreStatementsProcessor::new_with_noop_senders(
            runtime.store().chain_store(),
            epoch_manager.clone(),
        );

        let chunk_endorsement_tracker = Arc::new(ChunkEndorsementTracker::new(
            epoch_manager.clone(),
            runtime.store().clone(),
            spice_core_processor.clone(),
        ));

        let (spawner, tasks_rc) = FakeSpawner::new();
        let save_latest_witnesses = false;
        let (actor_sc, actor_rc) = unbounded();
        let chunk_executor_adapter = Sender::from_fn(move |event: ExecutorApplyChunksDone| {
            actor_sc.unbounded_send(event).unwrap();
        });
        let network_adapter = PeerManagerAdapter {
            async_request_sender: near_async::messaging::noop().into_sender(),
            set_chain_info_sender: near_async::messaging::noop().into_sender(),
            state_sync_event_sender: near_async::messaging::noop().into_sender(),
            request_sender: Sender::from_fn({
                move |event: PeerManagerMessageRequest| {
                    network_sc.unbounded_send(event).unwrap();
                }
            }),
        };

        let actor = ChunkExecutorActor::new(
            runtime.store().clone(),
            &chain_genesis,
            genesis_hash,
            runtime.clone(),
            epoch_manager,
            shard_tracker,
            network_adapter,
            validator_signer,
            spice_core_processor,
            chunk_endorsement_tracker,
            Arc::new(spawner),
            chunk_executor_adapter,
            save_latest_witnesses,
        );
        TestActor { chain, actor, actor_rc, tasks_rc }
    }

    fn run_internal_events(&mut self) {
        loop {
            let mut events_processed = 0;
            while let Ok(Some(task)) = self.tasks_rc.try_next() {
                events_processed += 1;
                task();
            }
            while let Ok(Some(event)) = self.actor_rc.try_next() {
                events_processed += 1;
                self.actor.handle(event);
            }
            if events_processed == 0 {
                break;
            }
        }
    }

    fn handle_with_internal_events<M>(&mut self, msg: M)
    where
        M: actix::Message,
        ChunkExecutorActor: Handler<M>,
    {
        self.actor.handle(msg);
        self.run_internal_events();
    }
}

fn setup_with_shards(
    num_shards: usize,
    network_sc: UnboundedSender<PeerManagerMessageRequest>,
) -> Vec<TestActor> {
    init_test_logger();

    let signers: Vec<_> = (0..num_shards)
        .into_iter()
        .map(|i| Arc::new(create_test_signer(&format!("test{i}"))))
        .collect();

    let shard_layout = ShardLayout::multi_shard(num_shards as NumShards, 0);

    let accounts: Vec<_> = signers.iter().map(|signer| signer.validator_id().clone()).collect();
    let validators_spec =
        ValidatorsSpec::desired_roles(&accounts.iter().map(|a| a.as_str()).collect_vec(), &[]);

    let epoch_length = 10;
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, ONE_NEAR)
        .build();

    signers
        .into_iter()
        .zip(shard_layout.shard_uids())
        .map(|(signer, shard_uuid)| {
            let validator_signer = MutableConfigValue::new(Some(signer), "validator_signer");
            TestActor::new(genesis.clone(), validator_signer, vec![shard_uuid], network_sc.clone())
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| panic!())
}

/// Returns 2 TestActor instances first validators and second not.
fn setup_with_non_validator(
    network_sc: UnboundedSender<PeerManagerMessageRequest>,
) -> [TestActor; 2] {
    init_test_logger();
    let signer = Arc::new(create_test_signer("test1"));
    let shard_layout = ShardLayout::multi_shard(2, 0);
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .shard_layout(shard_layout.clone())
        .validators_spec(ValidatorsSpec::desired_roles(&["test1"], &[]))
        .add_user_account_simple(signer.validator_id().clone(), ONE_NEAR)
        .build();

    [
        TestActor::new(
            genesis.clone(),
            MutableConfigValue::new(Some(signer), "validator_signer"),
            shard_layout.shard_uids().collect(),
            network_sc.clone(),
        ),
        TestActor::new(
            genesis,
            MutableConfigValue::new(None, "validator_signer"),
            shard_layout.shard_uids().collect(),
            network_sc,
        ),
    ]
}

fn propagate_single_network_request(actors: &mut [TestActor], event: &PeerManagerMessageRequest) {
    let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
    match request {
        NetworkRequests::TestonlySpiceIncomingReceipts { block_hash, receipt_proofs } => {
            actors.iter_mut().for_each(|actor| {
                actor.handle_with_internal_events(ExecutorIncomingReceipts {
                    block_hash: *block_hash,
                    receipt_proofs: receipt_proofs.clone(),
                });
            });
        }
        NetworkRequests::TestonlySpiceStateWitness { .. } => {}
        NetworkRequests::ChunkEndorsement(..) => {}
        event => unreachable!("{event:?}"),
    }
}

fn propagate_network_requests(
    actors: &mut [TestActor],
    network_rc: &mut UnboundedReceiver<PeerManagerMessageRequest>,
) {
    while let Ok(Some(event)) = network_rc.try_next() {
        propagate_single_network_request(actors, &event);
    }
}

fn block_executed(actor: &TestActor, block: &Block) -> bool {
    let epoch_id = block.header().epoch_id();
    let shard_ids = actor.actor.epoch_manager.shard_ids(epoch_id).unwrap();
    for shard_id in shard_ids {
        if !actor.actor.shard_tracker.cares_about_shard(block.hash(), shard_id) {
            continue;
        }
        if !actor.actor.chunk_extra_exists(block.header().hash(), shard_id).unwrap() {
            return false;
        }
    }
    true
}

fn produce_block(actors: &mut [TestActor], prev_block: &Block) -> Arc<Block> {
    let chunks =
        get_fake_next_block_chunk_headers(&prev_block, actors[0].actor.epoch_manager.as_ref());
    for actor in actors.iter_mut() {
        let mut store_update = actor.actor.chain_store.store_update();
        for chunk_header in &chunks {
            store_update.save_chunk(ShardChunk::new(chunk_header.clone(), vec![], vec![]));
        }
        store_update.commit().unwrap();
    }
    let block_producer = actors[0]
        .actor
        .epoch_manager
        .get_block_producer_info(prev_block.header().epoch_id(), prev_block.header().height() + 1)
        .unwrap();
    let signer = Arc::new(create_test_signer(block_producer.account_id().as_str()));
    let block = TestBlockBuilder::new(Clock::real(), prev_block, signer).chunks(chunks).build();
    for actor in actors {
        process_block_sync(
            &mut actor.chain,
            block.clone().into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        )
        .unwrap();
    }
    block
}

fn produce_n_blocks(actors: &mut [TestActor], num_blocks: usize) -> Vec<Arc<Block>> {
    let mut prev_block = actors[0].chain.genesis_block();
    let mut blocks = Vec::new();
    for _ in 0..num_blocks {
        let block = produce_block(actors, &prev_block);
        blocks.push(block.clone());
        prev_block = block;
    }
    blocks
}

fn find_chunk_execution_result(
    actors: &mut [TestActor],
    block_hash: &CryptoHash,
    shard_layout: &ShardLayout,
    shard_id: ShardId,
) -> ChunkExecutionResult {
    for actor in actors {
        if let Some(chunk_extra) = actor.actor.get_chunk_extra(block_hash, shard_id).unwrap() {
            let outgoing_receipts =
                actor.actor.chain_store.get_outgoing_receipts(block_hash, shard_id).unwrap();
            let (outgoing_receipts_root, _receipt_proofs) =
                Chain::create_receipts_proofs_from_outgoing_receipts(
                    shard_layout,
                    shard_id,
                    Arc::unwrap_or_clone(outgoing_receipts),
                )
                .unwrap();
            return ChunkExecutionResult {
                chunk_extra: Arc::unwrap_or_clone(chunk_extra),
                outgoing_receipts_root,
            };
        }
    }
    panic!()
}

fn record_endorsements(actors: &mut [TestActor], block: &Block) {
    let epoch_id = block.header().epoch_id();
    let shard_layout = actors[0].actor.epoch_manager.get_shard_layout(epoch_id).unwrap();
    for chunk in block.chunks().iter_raw() {
        let shard_id = chunk.shard_id();
        let execution_result =
            find_chunk_execution_result(actors, block.hash(), &shard_layout, shard_id);

        for actor in actors.iter() {
            let Some(signer) = actor.actor.validator_signer.get() else {
                continue;
            };
            let endorsement = ChunkEndorsement::new_with_execution_result(
                *epoch_id,
                execution_result.clone(),
                *block.header().hash(),
                chunk,
                &signer,
            );
            for actor in actors.iter() {
                actor.actor.core_processor.record_chunk_endorsement(endorsement.clone()).unwrap();
            }
        }
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_executing_blocks() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_shards(3, network_sc);
    let blocks = produce_n_blocks(&mut actors, 5);
    for (i, block) in blocks.iter().enumerate() {
        for actor in &mut actors {
            assert!(!block_executed(&actor, &block), "block #{} is already executed", i + 1);
            actor
                .handle_with_internal_events(ProcessedBlock { block_hash: *block.header().hash() });
            assert!(block_executed(&actor, &block), "failed to execute block #{}", i + 1);
        }
        propagate_network_requests(&mut actors, &mut network_rc);
        record_endorsements(&mut actors, &block);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_scheduling_same_block_twice() {
    let (network_sc, _network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    actors[0].handle(ProcessedBlock { block_hash: *blocks[0].hash() });

    assert!(!block_executed(&actors[0], &blocks[0]));
    let mut tasks = Vec::new();
    while let Ok(Some(task)) = actors[0].tasks_rc.try_next() {
        tasks.push(task);
    }
    assert_ne!(tasks.len(), 0);

    actors[0].handle(ProcessedBlock { block_hash: *blocks[0].hash() });
    assert!(actors[0].tasks_rc.try_next().is_err(), "no new tasks should be scheduled");
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_executing_same_block_twice() {
    let (network_sc, _network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    assert!(!block_executed(&actors[0], &blocks[0]));
    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
    assert!(block_executed(&actors[0], &blocks[0]));

    actors[0].handle(ProcessedBlock { block_hash: *blocks[0].hash() });
    assert!(actors[0].tasks_rc.try_next().is_err(), "no new tasks should be scheduled");
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_execution_result_endorsement_trigger_next_blocks_execution() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);
    let fork_block = produce_block(&mut actors, &blocks[0]);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    propagate_network_requests(&mut actors, &mut network_rc);
    record_endorsements(&mut actors, &blocks[0]);

    assert!(!block_executed(&actors[0], &blocks[1]));
    assert!(!block_executed(&actors[0], &fork_block));
    actors[0]
        .handle_with_internal_events(ExecutionResultEndorsed { block_hash: *blocks[0].hash() });

    assert!(block_executed(&actors[0], &blocks[1]));
    assert!(block_executed(&actors[0], &fork_block));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_new_receipts_trigger_next_blocks_execution() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);
    let fork_block = produce_block(&mut actors, &blocks[0]);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    record_endorsements(&mut actors, &blocks[0]);

    assert!(!block_executed(&actors[0], &blocks[1]));
    assert!(!block_executed(&actors[0], &fork_block));
    propagate_network_requests(&mut actors, &mut network_rc);

    assert!(block_executed(&actors[0], &blocks[1]));
    assert!(block_executed(&actors[0], &fork_block));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_executing_without_execution_result() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }
    propagate_network_requests(&mut actors, &mut network_rc);

    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    assert!(!block_executed(&actors[0], &blocks[1]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_executing_without_receipts() {
    let (network_sc, _network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }
    record_endorsements(&mut actors, &blocks[0]);

    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    assert!(!block_executed(&actors[0], &blocks[1]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_executing_forks() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    propagate_network_requests(&mut actors, &mut network_rc);
    record_endorsements(&mut actors, &blocks[0]);

    let fork_block = produce_block(&mut actors, &blocks[0]);
    assert!(!block_executed(&actors[0], &blocks[1]));
    assert!(!block_executed(&actors[0], &fork_block));

    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    assert!(block_executed(&actors[0], &blocks[1]));
    assert!(!block_executed(&actors[0], &fork_block));

    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *fork_block.hash() });
    assert!(block_executed(&actors[0], &fork_block));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
#[should_panic]
fn test_not_executing_with_bad_receipts() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_shards(2, network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    record_endorsements(&mut actors, &blocks[0]);
    while let Ok(Some(event)) = network_rc.try_next() {
        let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
        let NetworkRequests::TestonlySpiceIncomingReceipts { block_hash, mut receipt_proofs } =
            request
        else {
            continue;
        };
        receipt_proofs[0].0.push(Receipt::new_balance_refund(
            &AccountId::from_str("test1").unwrap(),
            ONE_NEAR,
            ReceiptPriority::NoPriority,
        ));
        actors.iter_mut().for_each(|actor| {
            actor.handle_with_internal_events(ExecutorIncomingReceipts {
                block_hash,
                receipt_proofs: receipt_proofs.clone(),
            });
        });
    }

    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_tracking_several_shards() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_non_validator(network_sc);

    let blocks = produce_n_blocks(&mut actors, 3);
    for (i, block) in blocks.iter().enumerate() {
        actors[0]
            .handle_with_internal_events(ProcessedBlock { block_hash: *block.header().hash() });

        let epoch_id = block.header().epoch_id();
        let shard_ids = actors[0].actor.epoch_manager.shard_ids(epoch_id).unwrap();
        for shard_id in shard_ids {
            assert!(
                actors[0].actor.chunk_extra_exists(block.header().hash(), shard_id).unwrap(),
                "no execution results for block #{} shard_id={shard_id} block_hash {}",
                i + 1,
                block.hash(),
            );
        }
        propagate_network_requests(&mut actors, &mut network_rc);
        record_endorsements(&mut actors, &block);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_sending_witness_when_not_validator() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_non_validator(network_sc);
    let blocks = produce_n_blocks(&mut actors, 3);
    let actor = &mut actors[1];

    actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
    assert!(block_executed(&actor, &blocks[0]));

    let mut witnesses = Vec::new();
    while let Ok(Some(event)) = network_rc.try_next() {
        let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
        let NetworkRequests::TestonlySpiceStateWitness { state_witness } = request else {
            continue;
        };
        witnesses.push(state_witness);
    }
    assert_eq!(witnesses.len(), 0);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_executing_chain_of_ready_blocks() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_non_validator(network_sc);
    let blocks = produce_n_blocks(&mut actors, 5);

    for block in &blocks {
        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
        assert!(block_executed(&actors[0], block));
        propagate_network_requests(&mut actors, &mut network_rc);
        record_endorsements(&mut actors, &block);
    }

    for block in &blocks {
        assert!(!block_executed(&actors[1], block));
    }
    actors[1].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
    for block in &blocks {
        assert!(block_executed(&actors[1], block));
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_executing_out_of_order() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_non_validator(network_sc);
    let blocks = produce_n_blocks(&mut actors, 5);

    for block in &blocks {
        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
        assert!(block_executed(&actors[0], block));
        propagate_network_requests(&mut actors, &mut network_rc);
        record_endorsements(&mut actors, &block);
    }

    for block in &blocks {
        assert!(!block_executed(&actors[1], block));
    }
    actors[1].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    for block in &blocks {
        assert!(!block_executed(&actors[1], block));
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_is_valid() {
    let (network_sc, mut network_rc) = unbounded();
    let mut actors = setup_with_non_validator(network_sc);

    let prev_block = actors[0].chain.genesis_block();
    let block = produce_block(&mut actors, &prev_block);
    let actor = &mut actors[0];

    actor.handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
    assert!(block_executed(&actor, &block));

    let mut count_witnesses = 0;
    while let Ok(Some(event)) = network_rc.try_next() {
        let PeerManagerMessageRequest::NetworkRequests(request) = event else { unreachable!() };
        let NetworkRequests::TestonlySpiceStateWitness { state_witness } = request else {
            continue;
        };
        let pre_validation_result = spice_pre_validate_chunk_state_witness(
            &state_witness,
            &block,
            &prev_block,
            &BlockExecutionResults(HashMap::new()),
            actor.actor.epoch_manager.as_ref(),
            &actor.actor.chain_store,
        )
        .unwrap();

        let save_witness_if_invalid = false;
        assert!(
            validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                actor.actor.epoch_manager.as_ref(),
                actor.actor.runtime_adapter.as_ref(),
                &MainStateTransitionCache::default(),
                actor.actor.chain_store.store(),
                save_witness_if_invalid,
                Arc::new(ReedSolomon::new(1, 1).unwrap()),
            )
            .is_ok()
        );
        count_witnesses += 1;
    }
    assert!(count_witnesses > 0);
}
