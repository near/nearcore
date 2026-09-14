use crate::spice::chunk_executor_actor::ExecutorIncomingUnverifiedReceipts;
use crate::spice::chunk_executor_actor::{
    ChunkExecutorActor, is_descendant_of_final_execution_head,
};
use crate::spice::chunk_executor_actor::{
    ExecutorApplyChunksDone, get_witness, receipt_proof_exists,
};
use crate::spice::data_distributor_actor::DataVerification;
use crate::spice::data_distributor_actor::SpiceDataDistributorAdapter;
use crate::spice::data_distributor_actor::SpiceDistributorOutgoingReceipts;
use crate::spice::data_distributor_actor::SpiceDistributorStateWitness;
use crate::spice::data_manager::DataId;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use itertools::Itertools as _;
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::Actor;
use near_async::messaging::{Handler, IntoAsyncSender, IntoSender, Sender, noop};
use near_async::test_utils::FakeDelayedActionRunner;
use near_async::time::Clock;
use near_chain::ChainStoreAccess;
use near_chain::Error;
use near_chain::spice::boundary::{is_spice_activation_parent, seed_execution_heads_at_activation};
use near_chain::spice::chunk_application::ChunkPersistenceConfig;
use near_chain::spice::chunk_validation::spice_pre_validate_chunk_state_witness;
use near_chain::spice::chunk_validation::spice_validate_chunk_state_witness;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::ExecutionResultEndorsed;
use near_chain::spice::core_writer_actor::ProcessedBlock;
use near_chain::spice::core_writer_actor::SpiceCoreWriterActor;
use near_chain::test_utils::{
    get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
};
use near_chain::types::Tip;
use near_chain::{Block, Chain, ChainGenesis};
use near_chain::{BlockProcessingArtifact, Provenance};
use near_chain_configs::MutableValidatorSigner;
use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
use near_chain_configs::{Genesis, MutableConfigValue, TrackedShardsConfig};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::sharding::ReceiptProof;
use near_primitives::sharding::ShardChunk;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::test_utils::{
    TestBlockBuilder, create_test_signer, pre_spice_protocol_version,
};
use near_primitives::types::SpiceChunkId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    AccountId, Balance, ChunkExecutionResult, NumShards, ProtocolVersion, ShardId,
};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::ProtocolFeature;
use near_store::ShardUId;
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::StoreUpdateAdapter;
use parking_lot::RwLock;
use std::str::FromStr as _;
use std::sync::Arc;

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
    M: Send + 'static,
    ChunkExecutorActor: Handler<M>,
{
    fn handle(&mut self, msg: M) {
        self.actor.handle(msg);
    }
}

#[allow(clippy::large_enum_variant)]
enum OutgoingMessage {
    NetworkRequests(NetworkRequests),
    SpiceDistributorOutgoingReceipts(SpiceDistributorOutgoingReceipts),
    SpiceDistributorStateWitness(SpiceDistributorStateWitness),
    DataVerification(DataVerification),
}

// We don't derive clone because it's desirable to not have clone for spice distributor message to
// make sure that while distributing we aren't cloning unnecessarily.
impl Clone for OutgoingMessage {
    fn clone(&self) -> OutgoingMessage {
        match self {
            OutgoingMessage::NetworkRequests(requests) => {
                OutgoingMessage::NetworkRequests(requests.clone())
            }
            OutgoingMessage::SpiceDistributorOutgoingReceipts(
                SpiceDistributorOutgoingReceipts { block_hash, receipt_proofs },
            ) => OutgoingMessage::SpiceDistributorOutgoingReceipts(
                SpiceDistributorOutgoingReceipts {
                    block_hash: *block_hash,
                    receipt_proofs: receipt_proofs.clone(),
                },
            ),
            OutgoingMessage::SpiceDistributorStateWitness(SpiceDistributorStateWitness {
                state_witness,
                contract_accesses,
            }) => OutgoingMessage::SpiceDistributorStateWitness(SpiceDistributorStateWitness {
                state_witness: state_witness.clone(),
                contract_accesses: contract_accesses.clone(),
            }),
            OutgoingMessage::DataVerification(verification) => {
                OutgoingMessage::DataVerification(verification.clone())
            }
        }
    }
}

impl TestActor {
    fn new(
        genesis: Genesis,
        validator_signer: MutableValidatorSigner,
        tracking_shards: Vec<ShardUId>,
        outgoing_sc: UnboundedSender<OutgoingMessage>,
    ) -> TestActor {
        let chain = get_chain_with_genesis(Clock::real(), genesis.clone());
        let epoch_manager = chain.epoch_manager.clone();

        let shard_tracker = ShardTracker::new(
            TrackedShardsConfig::Shards(tracking_shards),
            epoch_manager.clone(),
            validator_signer.clone(),
        );

        let chain_genesis = ChainGenesis::new(&genesis.config);
        let runtime = chain.runtime_adapter.clone();

        let (spawner, tasks_rc) = FakeSpawner::new();
        let (actor_sc, actor_rc) = unbounded();
        let chunk_executor_adapter = Sender::from_fn(move |event: ExecutorApplyChunksDone| {
            actor_sc.unbounded_send(event).unwrap();
        });
        let network_adapter = PeerManagerAdapter {
            async_request_sender: noop().into_async_sender(),
            set_chain_info_sender: noop().into_sender(),
            state_sync_event_sender: noop().into_sender(),
            request_sender: Sender::from_fn({
                let outgoing_sc = outgoing_sc.clone();
                move |message: PeerManagerMessageRequest| {
                    let PeerManagerMessageRequest::NetworkRequests(request) = message else {
                        unreachable!()
                    };
                    outgoing_sc.unbounded_send(OutgoingMessage::NetworkRequests(request)).unwrap();
                }
            }),
            request_with_permit_sender: Sender::from_fn({
                let outgoing_sc = outgoing_sc.clone();
                move |message: near_network::types::NetworkRequestWithPermit| {
                    // ignore the permit in tests
                    outgoing_sc
                        .unbounded_send(OutgoingMessage::NetworkRequests(message.request))
                        .unwrap();
                }
            }),
        };
        let data_distributor_adapter = SpiceDataDistributorAdapter {
            receipts: Sender::from_fn({
                let outgoing_sc = outgoing_sc.clone();
                move |message| {
                    outgoing_sc
                        .unbounded_send(OutgoingMessage::SpiceDistributorOutgoingReceipts(message))
                        .unwrap();
                }
            }),
            witness: Sender::from_fn({
                let outgoing_sc = outgoing_sc.clone();
                move |message| {
                    outgoing_sc
                        .unbounded_send(OutgoingMessage::SpiceDistributorStateWitness(message))
                        .unwrap();
                }
            }),
            data_verification: Sender::from_fn({
                move |message| {
                    outgoing_sc.unbounded_send(OutgoingMessage::DataVerification(message)).unwrap();
                }
            }),
        };
        let core_writer_actor = Arc::new(RwLock::new(SpiceCoreWriterActor::new(
            runtime.store().chain_store(),
            epoch_manager.clone(),
            validator_signer.clone(),
            core_reader(&chain),
            noop().into_sender(),
            noop().into_sender(),
        )));
        let core_writer_sender =
            Sender::from_fn(move |message| core_writer_actor.write().handle(message));

        let actor = ChunkExecutorActor::new(
            runtime.store().clone(),
            &chain_genesis,
            runtime.clone(),
            epoch_manager,
            shard_tracker,
            network_adapter,
            validator_signer,
            Arc::new(spawner),
            chunk_executor_adapter,
            core_writer_sender,
            data_distributor_adapter,
            ChunkPersistenceConfig::default(),
        );
        TestActor { chain, actor, actor_rc, tasks_rc }
    }

    fn drain_tasks(&mut self) -> Vec<Box<dyn FnOnce() + Send>> {
        let mut tasks = Vec::new();
        while let Ok(Some(task)) = self.tasks_rc.try_next() {
            tasks.push(task)
        }
        tasks
    }

    fn drain_events(&mut self) -> Vec<ExecutorApplyChunksDone> {
        let mut events = Vec::new();
        while let Ok(Some(event)) = self.actor_rc.try_next() {
            events.push(event);
        }
        events
    }

    fn run_internal_events(&mut self) {
        loop {
            let mut events_processed = 0;
            for task in self.drain_tasks() {
                events_processed += 1;
                task();
            }
            for event in self.drain_events() {
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
        M: Send + 'static,
        ChunkExecutorActor: Handler<M>,
    {
        self.actor.handle(msg);
        self.run_internal_events();
    }
}

fn core_reader(chain: &Chain) -> SpiceCoreReader {
    SpiceCoreReader::new(
        chain.chain_store.store().chain_store(),
        chain.epoch_manager.clone(),
        Gas::from_teragas(100),
    )
}

fn setup_with_shards(
    num_shards: usize,
    outgoing_sc: UnboundedSender<OutgoingMessage>,
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
        .add_user_accounts_simple(&accounts, Balance::from_near(1))
        .build();

    signers
        .into_iter()
        .zip(shard_layout.shard_uids())
        .map(|(signer, shard_uuid)| {
            let validator_signer = MutableConfigValue::new(Some(signer), "validator_signer");
            TestActor::new(genesis.clone(), validator_signer, vec![shard_uuid], outgoing_sc.clone())
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap_or_else(|_| panic!())
}

/// Returns 2 TestActor instances first validators and second not.
fn setup_with_non_validator(outgoing_sc: UnboundedSender<OutgoingMessage>) -> [TestActor; 2] {
    init_test_logger();
    let signer = Arc::new(create_test_signer("test1"));
    let shard_layout = ShardLayout::multi_shard(2, 0);
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .shard_layout(shard_layout.clone())
        .validators_spec(ValidatorsSpec::desired_roles(&["test1"], &[]))
        .add_user_account_simple(signer.validator_id().clone(), Balance::from_near(1))
        .build();

    [
        TestActor::new(
            genesis.clone(),
            MutableConfigValue::new(Some(signer), "validator_signer"),
            shard_layout.shard_uids().collect(),
            outgoing_sc.clone(),
        ),
        TestActor::new(
            genesis,
            MutableConfigValue::new(None, "validator_signer"),
            shard_layout.shard_uids().collect(),
            outgoing_sc,
        ),
    ]
}

fn simulate_single_outgoing_message(actors: &mut [TestActor], message: &OutgoingMessage) {
    match message {
        OutgoingMessage::NetworkRequests(requests) => match requests {
            NetworkRequests::SpiceChunkEndorsement(..) => {}
            request => unreachable!("{request:?}"),
        },
        OutgoingMessage::SpiceDistributorOutgoingReceipts(SpiceDistributorOutgoingReceipts {
            block_hash,
            receipt_proofs,
        }) => {
            for receipt_proof in receipt_proofs {
                actors.iter_mut().for_each(|actor| {
                    if actor.actor.validator_signer.get().is_some() {
                        let data_id = DataId::receipt_proof(
                            *block_hash,
                            receipt_proof.1.from_shard_id,
                            receipt_proof.1.to_shard_id,
                        );
                        actor.handle_with_internal_events(ExecutorIncomingUnverifiedReceipts {
                            data_id,
                            receipt_proof: receipt_proof.clone(),
                        });
                    }
                });
            }
        }
        OutgoingMessage::SpiceDistributorStateWitness(_) => {}
        OutgoingMessage::DataVerification(_) => {}
    }
}

fn simulate_outgoing_messages(
    actors: &mut [TestActor],
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) {
    while let Ok(Some(message)) = outgoing_rc.try_next() {
        simulate_single_outgoing_message(actors, &message);
    }
}

fn block_executed(actor: &TestActor, block: &Block) -> bool {
    let epoch_id = block.header().epoch_id();
    for shard_uid in actor.actor.epoch_manager.shard_uids(epoch_id).unwrap() {
        if !actor.actor.shard_tracker.cares_about_shard(block.hash(), shard_uid.shard_id()) {
            continue;
        }
        match actor
            .actor
            .chain_store
            .chunk_store()
            .get_chunk_extra(block.header().hash(), &shard_uid)
        {
            Ok(_) => {}
            Err(Error::DBNotFoundErr(_)) => return false,
            Err(err) => panic!("unexpected error reading chunk extra: {err:?}"),
        }
    }
    true
}

fn produce_block(actors: &mut [TestActor], prev_block: &Block) -> Arc<Block> {
    let chunks =
        get_fake_next_block_chunk_headers(&prev_block, actors[0].actor.epoch_manager.as_ref());
    for actor in actors.iter_mut() {
        let mut store_update = actor.chain.chain_store.store_update();
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
    let block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer)
        .chunks(chunks)
        .spice_core_statements(vec![])
        .build();
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
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, shard_layout);
    for actor in actors {
        if let Ok(chunk_extra) =
            actor.actor.chain_store.chunk_store().get_chunk_extra(block_hash, &shard_uid)
        {
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
            let endorsement = SpiceChunkEndorsement::new(
                SpiceChunkId { block_hash: *block.hash(), shard_id },
                execution_result.clone(),
                &signer,
            );
            for actor in actors.iter() {
                actor.actor.core_writer_sender.send(SpiceChunkEndorsementMessage(
                    endorsement.clone(),
                    RecvMessagePermit::none(),
                ));
            }
        }
    }
}

fn execute_blocks_until_final_execution_head_moves(
    actors: &mut [TestActor],
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) {
    let genesis = actors[0].chain.genesis_block();
    let genesis_height = genesis.header().height();
    let mut prev_block = genesis;

    // We set some limit to make sure we don't run infinite loop if something is wrong.
    let block_limit = 10;
    for _ in 0..block_limit {
        let block = produce_block(actors, &prev_block);
        for actor in actors.iter_mut() {
            actor.handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
            assert!(
                block_executed(actor, &block),
                "{:?} did not execute block",
                actor.actor.validator_signer,
            );
        }
        simulate_outgoing_messages(actors, outgoing_rc);
        record_endorsements(actors, &block);

        prev_block = block;

        let Ok(final_execution_head) = actors[0].chain.chain_store.spice_final_execution_head()
        else {
            continue;
        };
        if final_execution_head.height > genesis_height {
            return;
        }
    }
    panic!("final execution head did not move within {block_limit} blocks");
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_executing_blocks() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(3, outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 5);
    for (i, block) in blocks.iter().enumerate() {
        for actor in &mut actors {
            assert!(!block_executed(&actor, &block), "block #{} is already executed", i + 1);
            actor
                .handle_with_internal_events(ProcessedBlock { block_hash: *block.header().hash() });
            assert!(block_executed(&actor, &block), "failed to execute block #{}", i + 1);
        }
        simulate_outgoing_messages(&mut actors, &mut outgoing_rc);
        record_endorsements(&mut actors, &block);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_non_validator_executing_blocks() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_non_validator(outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 5);
    for (i, block) in blocks.iter().enumerate() {
        for actor in &mut actors {
            actor
                .handle_with_internal_events(ProcessedBlock { block_hash: *block.header().hash() });
            assert!(block_executed(&actor, &block), "failed to execute block #{}", i + 1);
        }
        simulate_outgoing_messages(&mut actors, &mut outgoing_rc);
        record_endorsements(&mut actors, &block);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_scheduling_same_block_twice() {
    let (outgoing_sc, _outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
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
    let (outgoing_sc, _outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
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
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 3);
    let fork_block = produce_block(&mut actors, &blocks[0]);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    // Announce the descendants: each parks because blocks[0]'s execution result
    // is not yet available. (The real client sends a ProcessedBlock per block.)
    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *fork_block.hash() });

    simulate_outgoing_messages(&mut actors, &mut outgoing_rc);
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
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 3);
    let fork_block = produce_block(&mut actors, &blocks[0]);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    // Announce the descendants: each parks until blocks[0]'s receipts arrive.
    // (The real client sends a ProcessedBlock per block.)
    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *fork_block.hash() });

    record_endorsements(&mut actors, &blocks[0]);

    assert!(!block_executed(&actors[0], &blocks[1]));
    assert!(!block_executed(&actors[0], &fork_block));
    simulate_outgoing_messages(&mut actors, &mut outgoing_rc);

    assert!(block_executed(&actors[0], &blocks[1]));
    assert!(block_executed(&actors[0], &fork_block));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_executing_without_execution_result() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }
    simulate_outgoing_messages(&mut actors, &mut outgoing_rc);

    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    assert!(!block_executed(&actors[0], &blocks[1]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_executing_without_receipts() {
    let (outgoing_sc, _outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
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
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    simulate_outgoing_messages(&mut actors, &mut outgoing_rc);
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
fn test_not_executing_forks_past_final_execution_head() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(1, outgoing_sc);
    let genesis = actors[0].chain.genesis_block();
    let fork_block = produce_block(&mut actors, &genesis);

    execute_blocks_until_final_execution_head_moves(&mut actors, &mut outgoing_rc);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *fork_block.hash() });
    }
    assert!(!block_executed(&actors[0], &fork_block));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_applying_forks_past_final_execution_head() {
    let (outgoing_sc, _outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(1, outgoing_sc);
    assert_eq!(actors.len(), 1);
    let genesis = actors[0].chain.genesis_block();

    let fork_block = produce_block(&mut actors, &genesis);
    actors[0].actor.handle(ProcessedBlock { block_hash: *fork_block.hash() });
    // Delaying internal tasks and events simulates a race of fork block processing starting and
    // final execution head moving while it's ongoing.
    let fork_tasks = actors[0].drain_tasks();
    let fork_events = actors[0].drain_events();

    let mut blocks = Vec::new();
    #[allow(clippy::redundant_clone)]
    let mut prev_block = genesis.clone();
    loop {
        let block = produce_block(&mut actors, &prev_block);
        actors[0].actor.handle(ProcessedBlock { block_hash: *block.hash() });
        blocks.push(block.clone());
        let last_final_block = block.header().last_final_block();
        if last_final_block != &CryptoHash::default() && last_final_block != genesis.hash() {
            break;
        }
        prev_block = block;
    }

    actors[0].run_internal_events();
    for block in blocks {
        assert!(block_executed(&actors[0], &block));
    }

    let final_execution_head = actors[0].chain.chain_store.spice_final_execution_head().unwrap();
    assert!(final_execution_head.height > genesis.header().height());

    for task in fork_tasks {
        task();
    }
    for event in fork_events {
        actors[0].handle(event);
    }
    actors[0].run_internal_events();

    assert!(!block_executed(&actors[0], &fork_block));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_final_execution_head_is_updated_when_tracking_no_shards() {
    init_test_logger();

    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let producer_signer = Arc::new(create_test_signer("producer"));
    let validator_signer = Arc::new(create_test_signer("validator"));
    let shard_layout = ShardLayout::single_shard();
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .shard_layout(shard_layout.clone())
        .validators_spec(ValidatorsSpec::desired_roles(&["producer"], &["validator"]))
        .build();

    let mut actors = [
        TestActor::new(
            genesis.clone(),
            MutableConfigValue::new(Some(producer_signer), "validator_signer"),
            shard_layout.shard_uids().collect(),
            outgoing_sc.clone(),
        ),
        TestActor::new(
            genesis,
            MutableConfigValue::new(Some(validator_signer), "validator_signer"),
            vec![],
            outgoing_sc,
        ),
    ];

    // Precondition: actor[1] genuinely tracks zero shards.
    let genesis_hash = *actors[1].chain.genesis_block().hash();
    assert!(
        actors[1].actor.shard_tracker.tracked_shard_uids(&genesis_hash).unwrap().is_empty(),
        "actor[1] must track zero shards for this test to be meaningful",
    );

    execute_blocks_until_final_execution_head_moves(&mut actors, &mut outgoing_rc);
    // Having final execution head updated even when we are tracking no shards is very useful for
    // distribution since it allows having a consistent checkpoint from which we can figure which
    // data we need even when we would only soon start tracking particular shards and tracking
    // no shards at the moment or consistently running witness validation only and tracking no
    // shards.
    assert_eq!(
        actors[0].chain.chain_store.spice_final_execution_head().unwrap(),
        actors[1].chain.chain_store.spice_final_execution_head().unwrap()
    );
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_executing_with_bad_receipts() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 3);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
        assert!(block_executed(&actor, &blocks[0]));
    }

    record_endorsements(&mut actors, &blocks[0]);
    while let Ok(Some(mut message)) = outgoing_rc.try_next() {
        let OutgoingMessage::SpiceDistributorOutgoingReceipts(SpiceDistributorOutgoingReceipts {
            receipt_proofs,
            ..
        }) = &mut message
        else {
            simulate_single_outgoing_message(&mut actors, &message);
            continue;
        };
        receipt_proofs[0].0.push(Receipt::new_balance_refund(
            &AccountId::from_str("test1").unwrap(),
            Balance::from_near(1),
        ));
        simulate_single_outgoing_message(&mut actors, &message);
    }

    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *blocks[1].hash() });
    assert!(!block_executed(&actors[0], &blocks[1]));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_extra_pending_bad_receipt_proof_does_not_prevent_execution() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let genesis = actors[0].chain.genesis_block();
    let first_block = produce_block(&mut actors, &genesis);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *first_block.hash() });
        assert!(block_executed(&actor, &first_block));
    }

    while let Ok(Some(mut message)) = outgoing_rc.try_next() {
        let OutgoingMessage::SpiceDistributorOutgoingReceipts(SpiceDistributorOutgoingReceipts {
            receipt_proofs,
            ..
        }) = &mut message
        else {
            simulate_single_outgoing_message(&mut actors, &message);
            continue;
        };
        let mut extra_proof = receipt_proofs[0].clone();
        extra_proof.0.push(Receipt::new_balance_refund(
            &AccountId::from_str("test1").unwrap(),
            Balance::from_near(1),
        ));
        receipt_proofs.push(extra_proof);
        simulate_single_outgoing_message(&mut actors, &message);
    }
    record_endorsements(&mut actors, &first_block);

    let second_block = produce_block(&mut actors, &first_block);
    actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *second_block.hash() });
    assert!(block_executed(&actors[0], &second_block));
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_verifying_a_network_receipt_reports_verified() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let genesis_block = actors[0].chain.genesis_block();
    let block = produce_block(&mut actors, &genesis_block);
    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
        assert!(block_executed(&actor, &block));
    }
    record_endorsements(&mut actors, &block);
    let receipt_proof = outgoing_receipt_proof(&mut outgoing_rc);
    let data_id = DataId::receipt_proof(
        *block.hash(),
        receipt_proof.1.from_shard_id,
        receipt_proof.1.to_shard_id,
    );

    actors[0].handle_with_internal_events(ExecutorIncomingUnverifiedReceipts {
        data_id: data_id.clone(),
        receipt_proof,
    });

    assert_eq!(drain_verifications(&mut outgoing_rc), vec![DataVerification::Ok(data_id)]);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_verifying_an_invalid_network_receipt_reports_failed() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let genesis_block = actors[0].chain.genesis_block();
    let block = produce_block(&mut actors, &genesis_block);
    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
        assert!(block_executed(&actor, &block));
    }
    record_endorsements(&mut actors, &block);
    let mut receipt_proof = outgoing_receipt_proof(&mut outgoing_rc);
    receipt_proof.0.push(Receipt::new_balance_refund(
        &AccountId::from_str("test1").unwrap(),
        Balance::from_near(1),
    ));
    let data_id = DataId::receipt_proof(
        *block.hash(),
        receipt_proof.1.from_shard_id,
        receipt_proof.1.to_shard_id,
    );

    actors[0].handle_with_internal_events(ExecutorIncomingUnverifiedReceipts {
        data_id: data_id.clone(),
        receipt_proof,
    });

    assert_eq!(drain_verifications(&mut outgoing_rc), vec![DataVerification::Failed(data_id)]);
}

/// First receipt proof the actors sent out; drops every other queued message.
fn outgoing_receipt_proof(outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>) -> ReceiptProof {
    let mut proof = None;
    while let Ok(Some(message)) = outgoing_rc.try_next() {
        if proof.is_none() {
            if let OutgoingMessage::SpiceDistributorOutgoingReceipts(
                SpiceDistributorOutgoingReceipts { receipt_proofs, .. },
            ) = &message
            {
                proof = Some(receipt_proofs[0].clone());
            }
        }
    }
    proof.expect("executing a block should send receipt proofs")
}

fn drain_verifications(
    outgoing_rc: &mut UnboundedReceiver<OutgoingMessage>,
) -> Vec<DataVerification> {
    let mut verifications = Vec::new();
    while let Ok(Some(message)) = outgoing_rc.try_next() {
        if let OutgoingMessage::DataVerification(verification) = message {
            verifications.push(verification);
        }
    }
    verifications
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_receipts_arriving_after_execution_scheduled_are_not_pending() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(2, outgoing_sc);
    let genesis = actors[0].chain.genesis_block();
    let block_producing_receipts = produce_block(&mut actors, &genesis);

    for actor in &mut actors {
        actor.handle_with_internal_events(ProcessedBlock {
            block_hash: *block_producing_receipts.hash(),
        });
        assert!(block_executed(&actor, &block_producing_receipts));
    }

    let mut extra_receipts = Vec::new();
    while let Ok(Some(message)) = outgoing_rc.try_next() {
        if matches!(
            message,
            OutgoingMessage::SpiceDistributorOutgoingReceipts(
                SpiceDistributorOutgoingReceipts { .. }
            )
        ) {
            extra_receipts.push(message.clone());
        }
        simulate_single_outgoing_message(&mut actors, &message);
    }
    record_endorsements(&mut actors, &block_producing_receipts);
    let block_receiving_receipts = produce_block(&mut actors, &block_producing_receipts);
    // We don't use handle_with_internal_events so that block execution wouldn't be finished.
    actors[0].handle(ProcessedBlock { block_hash: *block_receiving_receipts.hash() });
    // We have to drain tasks to make sure they aren't run on new receipts internal events
    // handling.
    let tasks = actors[0].drain_tasks();
    assert!(!tasks.is_empty());

    assert!(!extra_receipts.is_empty());
    for message in extra_receipts {
        simulate_single_outgoing_message(&mut actors, &message);
    }
    assert!(!block_executed(&actors[0], &block_receiving_receipts));
    assert_eq!(actors[0].actor.pending_receipts_count(), 0, "pending receipts are saved")
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_tracking_several_shards() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_non_validator(outgoing_sc);

    let blocks = produce_n_blocks(&mut actors, 3);
    for (i, block) in blocks.iter().enumerate() {
        actors[0]
            .handle_with_internal_events(ProcessedBlock { block_hash: *block.header().hash() });

        let epoch_id = block.header().epoch_id();
        let shard_layout = actors[0].actor.epoch_manager.get_shard_layout(epoch_id).unwrap();
        for shard_uid in shard_layout.shard_uids() {
            assert!(
                actors[0]
                    .actor
                    .chain_store
                    .chunk_store()
                    .get_chunk_extra(block.header().hash(), &shard_uid)
                    .is_ok(),
                "no execution results for block #{} shard_uid={shard_uid:?} block_hash {}",
                i + 1,
                block.hash(),
            );
        }
        simulate_outgoing_messages(&mut actors, &mut outgoing_rc);
        record_endorsements(&mut actors, &block);
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_sending_witness_when_not_validator() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_non_validator(outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 3);
    let actor = &mut actors[1];

    actor.handle_with_internal_events(ProcessedBlock { block_hash: *blocks[0].hash() });
    assert!(block_executed(&actor, &blocks[0]));

    let mut witnesses = Vec::new();
    while let Ok(Some(event)) = outgoing_rc.try_next() {
        let OutgoingMessage::SpiceDistributorStateWitness(SpiceDistributorStateWitness {
            state_witness,
            ..
        }) = event
        else {
            continue;
        };
        witnesses.push(state_witness);
    }
    assert_eq!(witnesses.len(), 0);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_executing_chain_of_ready_blocks() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_non_validator(outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 5);

    for block in &blocks {
        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
        assert!(block_executed(&actors[0], block));
        simulate_outgoing_messages(&mut actors, &mut outgoing_rc);
        record_endorsements(&mut actors, &block);
    }

    for block in &blocks {
        assert!(!block_executed(&actors[1], block));
    }
    // Every input is on disk, so announcing the blocks (one ProcessedBlock each,
    // as the client does) executes the whole chain.
    for block in &blocks {
        actors[1].handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
    }
    for block in &blocks {
        assert!(block_executed(&actors[1], block));
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_not_executing_out_of_order() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_non_validator(outgoing_sc);
    let blocks = produce_n_blocks(&mut actors, 5);

    for block in &blocks {
        actors[0].handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
        assert!(block_executed(&actors[0], block));
        simulate_outgoing_messages(&mut actors, &mut outgoing_rc);
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
fn test_witness_is_saved() {
    let (outgoing_sc, _outgoing_rc) = unbounded();
    let mut actors = setup_with_non_validator(outgoing_sc);

    let prev_block = actors[0].chain.genesis_block();
    let block = produce_block(&mut actors, &prev_block);
    let actor = &mut actors[0];
    let shard_id = block.chunks().get(0).unwrap().shard_id();

    actor.handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
    assert!(block_executed(&actor, &block));

    let witness = get_witness(actor.chain.chain_store().store_ref(), block.hash(), shard_id);
    assert!(witness.is_some());
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_witness_is_valid() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let mut actors = setup_with_non_validator(outgoing_sc);

    let prev_block = actors[0].chain.genesis_block();
    let block = produce_block(&mut actors, &prev_block);
    let actor = &mut actors[0];

    actor.handle_with_internal_events(ProcessedBlock { block_hash: *block.hash() });
    assert!(block_executed(&actor, &block));

    let mut count_witnesses = 0;
    while let Ok(Some(event)) = outgoing_rc.try_next() {
        let OutgoingMessage::SpiceDistributorStateWitness(SpiceDistributorStateWitness {
            state_witness,
            ..
        }) = event
        else {
            continue;
        };
        let prev_block_execution_results = actor
            .actor
            .core_reader
            .get_block_execution_results(prev_block.header())
            .unwrap()
            .unwrap();
        let shard_id = state_witness.chunk_id().shard_id;
        let prev_validator_proposals =
            actor.actor.core_reader.prev_validator_proposals(prev_block.hash(), shard_id).unwrap();
        let pre_validation_result = spice_pre_validate_chunk_state_witness(
            &state_witness,
            &block,
            &prev_block_execution_results,
            actor.actor.epoch_manager.as_ref(),
            &actor.chain.chain_store,
            prev_validator_proposals,
        )
        .unwrap();

        assert!(
            spice_validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                actor.actor.epoch_manager.as_ref(),
                actor.actor.runtime_adapter.as_ref(),
            )
            .is_ok()
        );
        count_witnesses += 1;
    }
    assert!(count_witnesses > 0);
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_actor_catches_up_on_start_from_genesis() {
    let (outgoing_sc, mut _outgoing_rc) = unbounded();
    let mut actors = setup_with_shards(1, outgoing_sc);
    assert_eq!(actors.len(), 1);
    let blocks = produce_n_blocks(&mut actors, 3);

    let actor = &mut actors[0];
    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.actor.start_actor(&mut fake_runner);
    actor.run_internal_events();
    for block in blocks {
        assert!(block_executed(&actor, &block));
    }
}

#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_actor_catches_up_on_start_from_final_execution_head() {
    let (outgoing_sc, mut outgoing_rc) = unbounded();
    let signer =
        MutableConfigValue::new(Some(Arc::new(create_test_signer("test1"))), "validator_signer");
    let shard_layout = ShardLayout::single_shard();
    let genesis = TestGenesisBuilder::new()
        .shard_layout(shard_layout.clone())
        .validators_spec(ValidatorsSpec::desired_roles(&["test1"], &[]))
        .build();
    let mut actors =
        [TestActor::new(genesis, signer, shard_layout.shard_uids().collect(), outgoing_sc)];

    let genesis_block = actors[0].chain.genesis_block();

    execute_blocks_until_final_execution_head_moves(&mut actors, &mut outgoing_rc);

    let final_execution_head = actors[0].chain.chain_store.spice_final_execution_head().unwrap();
    assert!(final_execution_head.height > genesis_block.header().height());

    let head_block = actors[0].chain.get_head_block().unwrap();
    let final_execution_head_block =
        actors[0].chain.get_block(&final_execution_head.last_block_hash).unwrap();
    assert!(final_execution_head_block.header().height() < head_block.header().height());

    let first_fork = produce_block(&mut actors, &final_execution_head_block);
    let second_fork = produce_block(&mut actors, &final_execution_head_block);
    let new_block = produce_block(&mut actors, &head_block);

    let actor = &mut actors[0];
    assert!(!block_executed(&actor, &first_fork));
    assert!(!block_executed(&actor, &second_fork));
    assert!(!block_executed(&actor, &new_block));

    let mut fake_runner = FakeDelayedActionRunner::default();
    actor.actor.start_actor(&mut fake_runner);
    actor.run_internal_events();

    assert!(block_executed(&actor, &first_fork));
    assert!(block_executed(&actor, &second_fork));
    assert!(block_executed(&actor, &new_block));
}

#[test]
fn test_is_descendant_of_final_execution_head_with_long_forks() {
    let signer = Arc::new(create_test_signer("test1"));
    let mut chain = {
        let genesis = TestGenesisBuilder::new()
            .validators_spec(ValidatorsSpec::desired_roles(&[signer.validator_id().as_str()], &[]))
            .build();
        get_chain_with_genesis(Clock::real(), genesis)
    };
    let genesis = chain.genesis_block();

    let mut block_height = genesis.header().height();
    let mut new_block = |chain: &mut Chain, prev_block: &Block| {
        block_height += 1;
        let block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer.clone())
            .height(block_height)
            .build();
        let mut store_update = chain.chain_store.store_update();
        store_update.save_block(block.clone());
        store_update.save_block_header(block.header().clone()).unwrap();
        store_update.commit().unwrap();
        block
    };

    let mut last_block = new_block(&mut chain, &genesis);

    let store = chain.chain_store.store();
    let mut store_update = store.store_update();
    store_update
        .chain_store_update()
        .set_spice_final_execution_head(&Tip::from_header(last_block.header()));
    store_update.commit();

    let mut last_fork_block = new_block(&mut chain, &genesis);
    for _ in 0..2 {
        last_block = new_block(&mut chain, &last_block);
        last_fork_block = new_block(&mut chain, &last_fork_block);
    }

    assert_eq!(
        is_descendant_of_final_execution_head(&chain.chain_store, last_block.header()),
        true
    );
    assert_eq!(
        is_descendant_of_final_execution_head(&chain.chain_store, last_fork_block.header()),
        false
    );
}

#[test]
fn test_is_descendant_of_final_execution_head_returns_false_for_final_execution_head() {
    let signer = Arc::new(create_test_signer("test1"));
    let mut chain = {
        let genesis = TestGenesisBuilder::new()
            .validators_spec(ValidatorsSpec::desired_roles(&[signer.validator_id().as_str()], &[]))
            .build();
        get_chain_with_genesis(Clock::real(), genesis)
    };
    let genesis = chain.genesis_block();

    let block = TestBlockBuilder::from_prev_block(Clock::real(), &genesis, signer).build();
    let mut store_update = chain.chain_store.store_update();
    store_update.save_block(block.clone());
    store_update.save_block_header(block.header().clone()).unwrap();
    store_update.commit().unwrap();
    let store = chain.chain_store.store();
    let mut spice_head_update = store.store_update();
    spice_head_update
        .chain_store_update()
        .set_spice_final_execution_head(&Tip::from_header(block.header()));
    spice_head_update.commit();

    assert_eq!(is_descendant_of_final_execution_head(&chain.chain_store, block.header()), false);
}

fn build_saved_block(
    chain: &mut Chain,
    prev_block: &Block,
    height: u64,
    protocol_version: ProtocolVersion,
    signer: &Arc<ValidatorSigner>,
) -> Arc<Block> {
    let block = TestBlockBuilder::from_prev_block(Clock::real(), prev_block, signer.clone())
        .height(height)
        .protocol_version(protocol_version)
        .build();
    let mut store_update = chain.chain_store.store_update();
    store_update.save_block(block.clone());
    store_update.save_block_header(block.header().clone()).unwrap();
    store_update.commit().unwrap();
    block
}

fn seed_execution_heads(chain: &Chain, parent: &Block, block: &Block) {
    let mut store_update = chain.chain_store.store().store_update();
    seed_execution_heads_at_activation(&mut store_update, block, parent.header()).unwrap();
    store_update.commit();
}

/// A pre-spice chain genesis..height 3 at consecutive heights, so `Block::produce`
/// resolves real last-final blocks: a first spice block built on the block at
/// height 3 finalizes the block at height 2.
fn pre_spice_boundary_chain() -> (Chain, Arc<ValidatorSigner>, Vec<Arc<Block>>) {
    let signer = Arc::new(create_test_signer("test1"));
    let mut chain = {
        let genesis = TestGenesisBuilder::new()
            .protocol_version(pre_spice_protocol_version())
            .validators_spec(ValidatorsSpec::desired_roles(&[signer.validator_id().as_str()], &[]))
            .build();
        get_chain_with_genesis(Clock::real(), genesis)
    };
    let mut blocks = vec![chain.genesis_block()];
    for height in 1..=3u64 {
        let block = build_saved_block(
            &mut chain,
            blocks[height as usize - 1].clone().as_ref(),
            height,
            pre_spice_protocol_version(),
            &signer,
        );
        blocks.push(block);
    }
    (chain, signer, blocks)
}

/// Re-seeding the execution heads from sibling boundary forks changes nothing:
/// both setters are forward-only, and every first spice block on a same-height
/// fork resolves the same last final block.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_activation_head_seeding_is_idempotent_across_sibling_boundary_forks() {
    let (mut chain, signer, blocks) = pre_spice_boundary_chain();
    let spice_protocol_version = ProtocolFeature::Spice.protocol_version();
    let parent = blocks[3].clone();
    let first_spice =
        build_saved_block(&mut chain, parent.as_ref(), 4, spice_protocol_version, &signer);
    assert_eq!(first_spice.header().last_final_block(), blocks[2].hash());

    seed_execution_heads(&chain, parent.as_ref(), first_spice.as_ref());
    let chain_store = chain.chain_store.store().chain_store();
    assert_eq!(&chain_store.spice_execution_head().unwrap().last_block_hash, parent.hash());
    assert_eq!(
        &chain_store.spice_final_execution_head().unwrap().last_block_hash,
        blocks[2].hash()
    );

    // A sibling first spice block on the same parent re-seeds to the same heads.
    let sibling_first_spice =
        build_saved_block(&mut chain, parent.as_ref(), 4, spice_protocol_version, &signer);
    seed_execution_heads(&chain, parent.as_ref(), sibling_first_spice.as_ref());
    assert_eq!(&chain_store.spice_execution_head().unwrap().last_block_hash, parent.hash());
    assert_eq!(
        &chain_store.spice_final_execution_head().unwrap().last_block_hash,
        blocks[2].hash()
    );

    // A same-height sibling activation parent on a fork re-seeds without moving
    // the heads either: the forward-only execution head setter skips equal heights.
    let sibling_parent = build_saved_block(
        &mut chain,
        blocks[2].clone().as_ref(),
        3,
        pre_spice_protocol_version(),
        &signer,
    );
    let fork_first_spice =
        build_saved_block(&mut chain, sibling_parent.as_ref(), 4, spice_protocol_version, &signer);
    seed_execution_heads(&chain, sibling_parent.as_ref(), fork_first_spice.as_ref());
    assert_eq!(&chain_store.spice_execution_head().unwrap().last_block_hash, parent.hash());
    assert_eq!(
        &chain_store.spice_final_execution_head().unwrap().last_block_hash,
        blocks[2].hash()
    );
}

/// At the boundary the final execution head is seeded to the first spice block's
/// last final block. `is_descendant_of_final_execution_head` gates execution on it
/// by walking a block's ancestry down to the head's height and comparing heights
/// only, never hashes. This pins both consequences for boundary forks:
/// - a fork whose blocks sit at the same heights as the canonical chain passes,
///   even though its ancestor at the head's height is a different block;
/// - a fork that branched below the seeded head and skipped its height has no
///   ancestor at that height, fails the check, and so never executes — only a
///   losing fork can have that shape, since the seeded head is a final block.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_activation_seeded_head_rejects_height_skipping_boundary_fork() {
    let (mut chain, signer, blocks) = pre_spice_boundary_chain();
    let spice_protocol_version = ProtocolFeature::Spice.protocol_version();
    let parent = blocks[3].clone();
    let first_spice =
        build_saved_block(&mut chain, parent.as_ref(), 4, spice_protocol_version, &signer);
    seed_execution_heads(&chain, parent.as_ref(), first_spice.as_ref());

    // The canonical first spice block descends through the seeded head's height.
    assert!(is_descendant_of_final_execution_head(&chain.chain_store, first_spice.header()));

    // A same-height sibling boundary fork passes either way.
    let sibling_parent = build_saved_block(
        &mut chain,
        blocks[2].clone().as_ref(),
        3,
        pre_spice_protocol_version(),
        &signer,
    );
    let sibling_first_spice =
        build_saved_block(&mut chain, sibling_parent.as_ref(), 4, spice_protocol_version, &signer);
    assert!(is_descendant_of_final_execution_head(
        &chain.chain_store,
        sibling_first_spice.header()
    ));

    // A boundary fork branching below the seeded head and skipping its height
    // has no ancestor at that height and is rejected.
    let skipping_parent = build_saved_block(
        &mut chain,
        blocks[1].clone().as_ref(),
        3,
        pre_spice_protocol_version(),
        &signer,
    );
    let skipping_first_spice =
        build_saved_block(&mut chain, skipping_parent.as_ref(), 4, spice_protocol_version, &signer);
    assert!(!is_descendant_of_final_execution_head(
        &chain.chain_store,
        skipping_first_spice.header()
    ));
}

/// Saves `block` and records it in the epoch manager the way block postprocessing
/// does, without running block processing: the epoch manager has to know the block
/// to answer activation-boundary questions about it, and its chunks have to be on
/// disk for the executor to read them.
fn save_and_record_pre_spice_block(chain: &mut Chain, block: &Arc<Block>) {
    let protocol_version =
        chain.epoch_manager.get_epoch_protocol_version(block.header().epoch_id()).unwrap();
    let mut store_update = chain.chain_store.store_update();
    store_update.save_block(block.clone());
    store_update.save_block_header(block.header().clone()).unwrap();
    for chunk_header in block.chunks().iter_raw() {
        store_update.save_chunk(ShardChunk::new(chunk_header.clone(), vec![], vec![]));
    }
    let block_info = BlockInfo::from_header(
        block.header(),
        block.header().height().saturating_sub(2),
        protocol_version,
    );
    let epoch_manager_update = chain
        .epoch_manager
        .add_validator_proposals(block_info, *block.header().random_value())
        .unwrap();
    store_update.merge(epoch_manager_update.into());
    store_update.commit().unwrap();
}

/// Extends `chain` with fabricated pre-spice blocks that vote for spice until the
/// tip is a spice activation parent, and returns it. Each header stays pinned to
/// the pre-spice version while its vote is the spice one, which is what makes the
/// epoch after the returned block the first spice epoch.
fn build_to_activation_parent(chain: &mut Chain, signer: &Arc<ValidatorSigner>) -> Arc<Block> {
    let mut block = chain.genesis_block();
    for _ in 0..MAX_BLOCKS_TO_ACTIVATION {
        let epoch_manager = chain.epoch_manager.clone();
        let chunks = get_fake_next_block_chunk_headers(&block, epoch_manager.as_ref());
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(block.hash()).unwrap();
        let next_epoch_id = epoch_manager.get_next_epoch_id_from_prev_block(block.hash()).unwrap();
        let height = block.header().height() + 1;
        // The epoch info aggregator asserts one bitmap slot per assigned chunk
        // validator, so the endorsement vectors have to be sized from the epoch.
        let chunk_endorsements = epoch_manager
            .get_shard_layout(&epoch_id)
            .unwrap()
            .shard_ids()
            .map(|shard_id| {
                let assignments = epoch_manager
                    .get_chunk_validator_assignments(&epoch_id, shard_id, height)
                    .unwrap();
                vec![Some(Box::new(signer.sign_bytes(&[]))); assignments.assignments().len()]
            })
            .collect();
        let mut next = TestBlockBuilder::from_prev_block(Clock::real(), &block, signer.clone())
            .chunks(chunks)
            .chunk_endorsements(chunk_endorsements)
            .epoch_id(epoch_id)
            .next_epoch_id(next_epoch_id)
            .protocol_version(pre_spice_protocol_version())
            .build_owned();
        next.mut_header().set_latest_protocol_version(ProtocolFeature::Spice.protocol_version());
        next.mut_header().resign(signer.as_ref());
        let next = Arc::new(next);
        save_and_record_pre_spice_block(chain, &next);
        block = next;
        if is_spice_activation_parent(chain.epoch_manager.as_ref(), block.hash()).unwrap() {
            return block;
        }
    }
    panic!("chain never reached a spice activation parent")
}

const MAX_BLOCKS_TO_ACTIVATION: usize = 30;
const BOUNDARY_NUM_SHARDS: NumShards = 3;
/// The one shard whose chunk extra is withheld below.
const BROKEN_SHARD_INDEX: usize = 1;

/// The boundary bootstrap has to survive a shard it cannot synthesize.
///
/// The coordinator creates a per-shard executor for every shard tracked in the
/// activation parent's epoch *or* the first spice epoch, so a shard this node
/// rotates into under spice gets an executor even though the node never applied
/// the parent's chunk for it and holds no `ChunkExtra` to synthesize from. That
/// shard's bootstrap must fail on its own without taking the other shards' work
/// with it: their endorsements, receipt proofs and witnesses are the only ones
/// they will ever produce for the parent, since nothing retries the bootstrap
/// outside `start_actor` recovery.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_boundary_bootstrap_isolates_a_shard_it_cannot_synthesize() {
    init_test_logger();
    let (outgoing_sc, _outgoing_rc) = unbounded();
    let signer = Arc::new(create_test_signer("test0"));
    let shard_layout = ShardLayout::multi_shard(BOUNDARY_NUM_SHARDS, 0);
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&Clock::real())
        .epoch_length(5)
        .transaction_validity_period(10)
        .protocol_version(pre_spice_protocol_version())
        .shard_layout(shard_layout.clone())
        .validators_spec(ValidatorsSpec::desired_roles(&["test0"], &[]))
        .add_user_account_simple(signer.validator_id().clone(), Balance::from_near(1))
        .build();
    let mut test_actor = TestActor::new(
        genesis,
        MutableConfigValue::new(Some(signer.clone()), "validator_signer"),
        shard_layout.shard_uids().collect(),
        outgoing_sc,
    );

    let activation_parent = build_to_activation_parent(&mut test_actor.chain, &signer);

    // Every shard but one looks like a shard this node applied pre-spice: the
    // withheld chunk extra is what makes the remaining shard unsynthesizable.
    let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
    let broken_shard_uid = shard_uids[BROKEN_SHARD_INDEX];
    let mut store_update = test_actor.chain.chain_store.store_update();
    for shard_uid in &shard_uids {
        if *shard_uid == broken_shard_uid {
            continue;
        }
        store_update.save_outgoing_receipt(
            activation_parent.hash(),
            shard_uid.shard_id(),
            vec![Receipt::new_balance_refund(
                &signer.validator_id().clone(),
                Balance::from_near(1),
            )],
        );
        store_update.save_chunk_extra(
            activation_parent.hash(),
            shard_uid,
            ChunkExtra::new_with_only_state_root(&CryptoHash::hash_bytes(
                shard_uid.shard_id().to_string().as_bytes(),
            ))
            .into(),
        );
    }
    store_update.commit().unwrap();

    let result = test_actor.actor.handle_processed_block(activation_parent.hash());
    assert!(
        result.is_ok(),
        "one unsynthesizable shard must not fail the whole boundary bootstrap: {:?}",
        result.unwrap_err(),
    );

    // Each synthesizable shard still produced and persisted its receipt proofs,
    // whichever order the coordinator visited the executors in.
    let store = test_actor.actor.chain_store.store();
    for shard_uid in &shard_uids {
        let from_shard_id = shard_uid.shard_id();
        let expected = *shard_uid != broken_shard_uid;
        for to_shard_id in shard_layout.shard_ids() {
            assert_eq!(
                receipt_proof_exists(&store, activation_parent.hash(), to_shard_id, from_shard_id),
                expected,
                "receipt proof {from_shard_id} -> {to_shard_id} at the activation parent",
            );
        }
    }
}
