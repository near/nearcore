use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt as _};
use near_async::messaging::{Handler, IntoSender as _, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::spice_core::{CoreStatementsProcessor, ExecutionResultEndorsed};
use near_chain::stateless_validation::chunk_validation::{
    MainStateTransitionCache, validate_chunk_state_witness,
};
use near_chain::stateless_validation::spice_chunk_validation::spice_pre_validate_chunk_state_witness;
use near_chain::types::RuntimeAdapter;
use near_chain::{ApplyChunksSpawner, Block, ChainGenesis, ChainStore, Error};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessSize,
};
use near_primitives::types::BlockExecutionResults;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Store;
use near_store::adapter::StoreAdapter as _;
use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::chunk_executor_actor::ProcessedBlock;
use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;

pub struct SpiceChunkValidatorActor {
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_adapter: PeerManagerAdapter,
    save_latest_witnesses: bool,
    save_invalid_witnesses: bool,

    validator_signer: MutableValidatorSigner,
    core_processor: CoreStatementsProcessor,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,

    /// Map holding witnesses we cannot process yet keyed by the block hash witness is for.
    pending_witnesses: HashMap<CryptoHash, Vec<ChunkStateWitness>>,
    main_state_transition_result_cache: MainStateTransitionCache,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,

    rs: Arc<ReedSolomon>,
}

impl near_async::messaging::Actor for SpiceChunkValidatorActor {}

impl SpiceChunkValidatorActor {
    pub fn new(
        store: Store,
        genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_adapter: PeerManagerAdapter,
        validator_signer: MutableValidatorSigner,
        core_processor: CoreStatementsProcessor,
        chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
        validation_spawner: ApplyChunksSpawner,
        save_latest_witnesses: bool,
        save_invalid_witnesses: bool,
    ) -> Self {
        // TODO(spice): Assess if this limit still makes sense for spice.
        // See ChunkValidator::new in c/c/s/s/chunk_validator/mod.rs for rationale used currently.
        let validation_thread_limit =
            runtime_adapter.get_shard_layout(PROTOCOL_VERSION).num_shards() as usize;
        let data_parts = epoch_manager.num_data_parts();
        let parity_parts = epoch_manager.num_total_parts() - data_parts;
        let rs = Arc::new(ReedSolomon::new(data_parts, parity_parts).unwrap());
        Self {
            pending_witnesses: HashMap::new(),
            save_latest_witnesses,
            save_invalid_witnesses,
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            network_adapter,
            validator_signer,
            core_processor,
            chunk_endorsement_tracker,
            validation_spawner: validation_spawner.into_spawner(validation_thread_limit),
            main_state_transition_result_cache: MainStateTransitionCache::default(),
            rs,
        }
    }
}

impl Handler<ProcessedBlock> for SpiceChunkValidatorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        let block = match self.chain_store.get_block(&block_hash) {
            Ok(block) => block,
            Err(err) => {
                tracing::error!(target: "spice_chunk_validator", %block_hash, ?err, "failed to get block");
                return;
            }
        };

        if let Some(signer) = self.validator_signer.get() {
            if let Err(err) = self.process_ready_pending_state_witnesses(block, signer) {
                tracing::error!(target: "spice_chunk_validator", %block_hash, ?err, "failed to process ready pending state witnesses");
            }
        }
    }
}

impl Handler<ExecutionResultEndorsed> for SpiceChunkValidatorActor {
    fn handle(&mut self, ExecutionResultEndorsed { block_hash }: ExecutionResultEndorsed) {
        if let Some(signer) = self.validator_signer.get() {
            let next_block_hashes =
                self.chain_store.get_all_next_block_hashes(&block_hash).unwrap();
            for next_block_hash in next_block_hashes {
                let next_block = self.chain_store.get_block(&next_block_hash).expect(
                    "block added to next blocks only after it's processed so it should be in store",
                );
                if let Err(err) =
                    self.process_ready_pending_state_witnesses(next_block, signer.clone())
                {
                    tracing::error!(target: "spice_chunk_validator", %next_block_hash, %block_hash, ?err, "failed to process ready pending state witnesses");
                }
            }
        }
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceChunkValidatorWitnessSender {
    pub chunk_state_witness: Sender<SpanWrapped<ChunkStateWitnessMessage>>,
}

impl Handler<SpanWrapped<ChunkStateWitnessMessage>> for SpiceChunkValidatorActor {
    #[perf]
    fn handle(&mut self, msg: SpanWrapped<ChunkStateWitnessMessage>) {
        let msg = msg.span_unwrap();
        let ChunkStateWitnessMessage { witness, raw_witness_size, .. } = msg;
        let Some(signer) = self.validator_signer.get() else {
            tracing::error!(target: "spice_chunk_validator", ?witness, "Received a chunk state witness but this is not a validator node.");
            return;
        };
        if let Err(err) = self.process_chunk_state_witness(witness, raw_witness_size, signer) {
            tracing::error!(target: "spice_chunk_validator", ?err, "Error processing chunk state witness");
        }
    }
}

impl SpiceChunkValidatorActor {
    fn process_chunk_state_witness(
        &mut self,
        witness: ChunkStateWitness,
        raw_witness_size: ChunkStateWitnessSize,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        tracing::debug!(
            target: "spice_chunk_validator",
            chunk_hash=?witness.chunk_header().chunk_hash(),
            shard_id=?witness.chunk_header().shard_id(),
            "process_chunk_state_witness",
        );

        if self.save_latest_witnesses {
            self.chain_store.save_latest_chunk_state_witness(&witness)?;
        }

        match self.witness_processing_readiness(&witness)? {
            WitnessProcessingReadiness::NotReady => {
                self.handle_not_ready_state_witness(witness, raw_witness_size);
                Ok(())
            }
            WitnessProcessingReadiness::Ready(witness_validation_context) => self
                .validate_state_witness_and_send_endorsements(
                    &witness_validation_context,
                    witness.clone(),
                    signer,
                ),
        }
    }

    fn witness_processing_readiness(
        &self,
        witness: &ChunkStateWitness,
    ) -> Result<WitnessProcessingReadiness, Error> {
        let block_hash = witness.main_state_transition().block_hash;
        let chunk_hash = witness.chunk_header().chunk_hash();
        let block = match self.chain_store.get_block(&block_hash) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(err)) => {
                tracing::debug!(
                    target: "spice_chunk_validator",
                    ?chunk_hash,
                    ?block_hash,
                    ?err,
                    "witness for block isn't ready for processing; block is missing");
                return Ok(WitnessProcessingReadiness::NotReady);
            }
            Err(err) => return Err(err),
        };
        let prev_block = self.chain_store.get_block(block.header().prev_hash())?;

        let Some(prev_block_execution_results) =
            self.core_processor.get_block_execution_results(&prev_block)?
        else {
            tracing::debug!(
                target: "spice_chunk_validator",
                ?chunk_hash,
                ?block_hash,
                prev_block_hash=?prev_block.header().hash(),
                "witness for block isn't ready for processing; missing previous execution results required for chunk validation");
            return Ok(WitnessProcessingReadiness::NotReady);
        };

        Ok(WitnessProcessingReadiness::Ready(WitnessValidationContext {
            block,
            prev_block,
            prev_block_execution_results,
        }))
    }

    fn process_ready_pending_state_witnesses(
        &mut self,
        block: Arc<Block>,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let prev_hash = *block.header().prev_hash();
        let prev_block = self.chain_store.get_block(&prev_hash)?;
        let Some(prev_block_execution_results) =
            self.core_processor.get_block_execution_results(&prev_block)?
        else {
            tracing::debug!(
                target: "spice_chunk_validator",
                ?prev_hash,
                "process_ready_pending_state_witnesses: new block is available, but some of the prev block execution results are still missing");
            return Ok(());
        };

        let ready_witnesses = self.pending_witnesses.remove(block.header().hash());

        let witness_validation_context =
            WitnessValidationContext { block, prev_block, prev_block_execution_results };
        for witness in ready_witnesses.into_iter().flatten() {
            tracing::debug!(
                target: "spice_chunk_validator",
                ?prev_hash,
                witness_chunk=?witness.chunk_header().chunk_hash(),
                "processing ready pending state witnesses");
            self.validate_state_witness_and_send_endorsements(
                &witness_validation_context,
                witness,
                signer.clone(),
            )?;
        }
        Ok(())
    }

    fn handle_not_ready_state_witness(&mut self, witness: ChunkStateWitness, _witness_size: usize) {
        // TODO(spice): Implement additional checks before adding witness to pending witnesses, see Client's orphan_witness_handling.rs.
        let block_hash = witness.main_state_transition().block_hash;
        self.pending_witnesses.entry(block_hash).or_default().push(witness);
    }

    fn validate_state_witness_and_send_endorsements(
        &self,
        WitnessValidationContext { block, prev_block, prev_block_execution_results }: &WitnessValidationContext,
        witness: ChunkStateWitness,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let block_hash = witness.main_state_transition().block_hash;
        assert_eq!(&block_hash, block.header().hash());

        let pre_validation_result = spice_pre_validate_chunk_state_witness(
            &witness,
            &block,
            &prev_block,
            &prev_block_execution_results,
            self.epoch_manager.as_ref(),
            &self.chain_store,
        )?;

        let chunk_production_key = witness.chunk_production_key();
        let chunk_producer_name =
            self.epoch_manager.get_chunk_producer_info(&chunk_production_key)?.take_account_id();

        let save_witness_if_invalid = self.save_invalid_witnesses;
        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        let main_state_transition_cache = self.main_state_transition_result_cache.clone();
        let store = self.chain_store.store();
        let network_sender = self.network_adapter.clone().into_sender();
        let chunk_endorsement_tracker = self.chunk_endorsement_tracker.clone();
        let rs = self.rs.clone();
        self.validation_spawner.spawn("spice_stateless_validation", move || {
            let chunk_header = witness.chunk_header().clone();
            let shard_id = witness.chunk_header().shard_id();
            let chunk_execution_result = match validate_chunk_state_witness(
                witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
                &main_state_transition_cache,
                store,
                save_witness_if_invalid,
                rs,
            ) {
                Ok(execution_result) => execution_result,
                Err(err) => {
                    near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                        .with_label_values(&[&shard_id.to_string(), err.prometheus_label_value()])
                        .inc();
                    tracing::error!(
                        target: "spice_chunk_validator",
                        ?err,
                        ?chunk_producer_name,
                        ?chunk_production_key,
                        "Failed to validate chunk"
                    );
                    return;
                }
            };

            let endorsement = ChunkEndorsement::new_with_execution_result(
                epoch_id,
                chunk_execution_result,
                block_hash,
                &chunk_header,
                &signer,
            );
            send_spice_chunk_endorsement(
                endorsement.clone(),
                epoch_manager.as_ref(),
                &network_sender,
                &signer,
            );
            chunk_endorsement_tracker
                .process_chunk_endorsement(endorsement)
                .expect("Node should always be able to record it's own endorsement");
        });
        Ok(())
    }
}

pub fn send_spice_chunk_endorsement(
    endorsement: ChunkEndorsement,
    epoch_manager: &dyn EpochManagerAdapter,
    network_sender: &Sender<PeerManagerMessageRequest>,
    signer: &ValidatorSigner,
) {
    let block_hash = endorsement.block_hash().unwrap();
    let epoch_id = epoch_manager.get_epoch_id(block_hash).unwrap();
    let next_epoch_id = epoch_manager.get_next_epoch_id(block_hash).unwrap();

    // Everyone should be aware of all core statements to make sure that execution can proceed
    // without waiting on endorsements appearing in consensus.
    let validators = epoch_manager
        .get_epoch_info(&epoch_id)
        .unwrap()
        .validators_iter()
        // A potential optimization here is to send only to the first few block producers
        // of the next epoch (instead of to everyone). This may reduce amount of data sent if validator
        // set changes drastically, but may cause execution delays on epoch boundaries.
        .chain(epoch_manager.get_epoch_info(&next_epoch_id).unwrap().validators_iter())
        .map(|stake| stake.take_account_id())
        .collect::<HashSet<_>>();

    for account in validators {
        if &account == signer.validator_id() {
            continue;
        }
        network_sender.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkEndorsement(account, endorsement.clone()),
        ));
    }
}

enum WitnessProcessingReadiness {
    NotReady,
    Ready(WitnessValidationContext),
}

struct WitnessValidationContext {
    block: Arc<Block>,
    prev_block: Arc<Block>,
    prev_block_execution_results: BlockExecutionResults,
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use near_async::time::Clock;
    use near_chain::test_utils::{
        get_chain_with_genesis, get_fake_next_block_chunk_headers, process_block_sync,
    };
    use near_chain::types::{ApplyChunkShardContext, RuntimeStorageConfig, StorageDataSource};
    use near_chain::{BlockProcessingArtifact, Chain, Provenance};
    use near_chain_configs::test_genesis::{TestGenesisBuilder, ValidatorsSpec};
    use near_chain_configs::{Genesis, MutableConfigValue};
    use near_o11y::span_wrapped_msg::SpanWrappedMessageExt as _;
    use near_o11y::testonly::init_test_logger;
    use near_primitives::apply::ApplyChunkReason;
    use near_primitives::hash::hash;
    use near_primitives::receipt::Receipt;
    use near_primitives::sharding::{ChunkHash, ReceiptProof};
    use near_primitives::stateless_validation::state_witness::ChunkStateTransition;
    use near_primitives::test_utils::{TestBlockBuilder, create_test_signer};
    use near_primitives::types::chunk_extra::ChunkExtra;
    use near_primitives::types::{AccountId, ChunkExecutionResult};
    use near_store::adapter::chain_store::ChainStoreAdapter;
    use near_store::get_genesis_state_roots;
    use node_runtime::SignedValidPeriodTransactions;
    use std::str::FromStr as _;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

    use super::*;

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

        let witness_message =
            valid_witness_message(&actor, &block, &prev_block, &starting_state_root);
        let post_state_root = witness_message.witness.main_state_transition().post_state_root;

        assert!(actor.core_processor.get_block_execution_results(&block).unwrap().is_none());
        actor.handle(witness_message.span_wrap());

        let block_execution_results =
            actor.core_processor.get_block_execution_results(&block).unwrap().unwrap();
        assert_eq!(block_execution_results.0.len(), 1);
        let chunk_hash = block.chunks()[0].chunk_hash().clone();
        assert_eq!(
            block_execution_results.0.get(&chunk_hash).unwrap().chunk_extra.state_root(),
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

        let witness_message =
            valid_witness_message(&actor, &block, &prev_block, &starting_state_root);
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
        let witness_message =
            valid_witness_message(&actor, &block, &prev_block, &starting_state_root);

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

        let witness_message =
            valid_witness_message(&actor, &block, &prev_block, &starting_state_root);

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
        let witness_message =
            valid_witness_message(&actor, &block, &prev_block, &starting_state_root);

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
        M: actix::Message,
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
            .get_block_producer_info(
                prev_block.header().epoch_id(),
                prev_block.header().height() + 1,
            )
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
        let save_latest_witnesses = false;
        let save_invalid_witnesses = false;
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
                save_latest_witnesses,
                save_invalid_witnesses,
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
            &block.chunks()[0],
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

    fn test_receipt_proofs(actor: &TestActor, block: &Block) -> HashMap<ChunkHash, ReceiptProof> {
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
        HashMap::from([(chunk_header.chunk_hash().clone(), proofs.remove(0))])
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

    fn test_chunk_state_transition(
        actor: &TestActor,
        block: &Block,
        prev_block: &Block,
        starting_state_root: &CryptoHash,
    ) -> ChunkStateTransition {
        let chunks = block.chunks();
        assert_eq!(chunks.len(), 1);
        let chunk_header = &chunks[0];
        let transactions = SignedValidPeriodTransactions::new(vec![], vec![]);
        let apply_result = actor
            .actor
            .runtime_adapter
            .apply_chunk(
                RuntimeStorageConfig {
                    state_root: *starting_state_root,
                    use_flat_storage: true,
                    source: StorageDataSource::Db,
                    state_patch: near_primitives::sandbox::state_patch::SandboxStatePatch::default(
                    ),
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
        ChunkStateTransition {
            block_hash: *block.hash(),
            base_state: apply_result.proof.unwrap().nodes,
            post_state_root: apply_result.new_root,
        }
    }

    fn test_witness_message(
        block: &Block,
        state_transition: ChunkStateTransition,
        receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
        receipts_hash: CryptoHash,
    ) -> ChunkStateWitnessMessage {
        let epoch_id = block.header().epoch_id();
        let chunks = block.chunks();
        assert_eq!(chunks.len(), 1);
        let chunk_header = &chunks[0];
        let transactions = vec![];
        let implicit_transitions = vec![];
        let new_transactions = vec![];
        let witness = ChunkStateWitness::new(
            AccountId::from_str("unused").unwrap(),
            *epoch_id,
            chunk_header.clone(),
            state_transition,
            receipt_proofs,
            receipts_hash,
            transactions,
            implicit_transitions,
            new_transactions,
            PROTOCOL_VERSION,
        );
        let witness_size = borsh::object_length(&witness).unwrap();
        ChunkStateWitnessMessage {
            witness,
            raw_witness_size: witness_size,
            processing_done_tracker: None,
        }
    }

    fn valid_witness_message(
        actor: &TestActor,
        block: &Block,
        prev_block: &Block,
        starting_state_root: &CryptoHash,
    ) -> ChunkStateWitnessMessage {
        let state_transition =
            test_chunk_state_transition(&actor, &block, &prev_block, &starting_state_root);
        let receipt_proofs = test_receipt_proofs(&actor, &prev_block);
        let receipts_hash = hash(&borsh::to_vec(&TEST_RECEIPTS).unwrap());
        test_witness_message(&block, state_transition, receipt_proofs, receipts_hash)
    }

    fn invalid_witness_message(
        actor: &TestActor,
        block: &Block,
        prev_block: &Block,
        starting_state_root: &CryptoHash,
    ) -> ChunkStateWitnessMessage {
        let state_transition = {
            let mut transition =
                test_chunk_state_transition(&actor, &block, &prev_block, &starting_state_root);
            transition.post_state_root = CryptoHash::default();
            transition
        };
        let receipt_proofs = test_receipt_proofs(&actor, &prev_block);
        let receipts_hash = hash(&borsh::to_vec(&TEST_RECEIPTS).unwrap());
        test_witness_message(&block, state_transition, receipt_proofs, receipts_hash)
    }
}
