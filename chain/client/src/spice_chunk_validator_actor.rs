use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt as _};
use near_async::messaging::{Handler, IntoSender as _, Sender};
use near_async::{Message, MultiSend, MultiSenderFrom};
use near_chain::spice_core::{CoreStatementsProcessor, ExecutionResultEndorsed};
use near_chain::stateless_validation::spice_chunk_validation::{
    spice_pre_validate_chunk_state_witness, spice_validate_chunk_state_witness,
};
use near_chain::types::RuntimeAdapter;
use near_chain::{ApplyChunksSpawner, Block, ChainGenesis, ChainStore, Error};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_performance_metrics_macros::perf;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessSize;
use near_primitives::types::BlockExecutionResults;
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Store;

use crate::chunk_executor_actor::ProcessedBlock;
use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;

pub struct SpiceChunkValidatorActor {
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_adapter: PeerManagerAdapter,

    validator_signer: MutableValidatorSigner,
    core_processor: CoreStatementsProcessor,
    chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,

    /// Map holding witnesses we cannot process yet keyed by the block hash witness is for.
    pending_witnesses: HashMap<CryptoHash, Vec<SpiceChunkStateWitness>>,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,
}

#[derive(Message, Debug)]
pub struct SpiceChunkStateWitnessMessage {
    pub witness: SpiceChunkStateWitness,
    pub raw_witness_size: ChunkStateWitnessSize,
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
    ) -> Self {
        // TODO(spice): Assess if this limit still makes sense for spice.
        // See ChunkValidator::new in c/c/s/s/chunk_validator/mod.rs for rationale used currently.
        let validation_thread_limit =
            runtime_adapter.get_shard_layout(PROTOCOL_VERSION).num_shards() as usize;
        Self {
            pending_witnesses: HashMap::new(),
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            network_adapter,
            validator_signer,
            core_processor,
            chunk_endorsement_tracker,
            validation_spawner: validation_spawner.into_spawner(validation_thread_limit),
        }
    }
}

// TODO(spice): Since data distributor makes sure block is available before witness is sent to the
// chunk validator actor we don't need to handle possibility of missing blocks in this actor.
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
    pub chunk_state_witness: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,
}

impl Handler<SpanWrapped<SpiceChunkStateWitnessMessage>> for SpiceChunkValidatorActor {
    #[perf]
    fn handle(&mut self, msg: SpanWrapped<SpiceChunkStateWitnessMessage>) {
        let msg = msg.span_unwrap();
        let SpiceChunkStateWitnessMessage { witness, raw_witness_size, .. } = msg;
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
        witness: SpiceChunkStateWitness,
        raw_witness_size: ChunkStateWitnessSize,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        tracing::debug!(
            target: "spice_chunk_validator",
            chunk_id=?witness.chunk_id(),
            "process_chunk_state_witness",
        );

        // TODO(spice): Implementing saving latest witness based on configuration.

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
        witness: &SpiceChunkStateWitness,
    ) -> Result<WitnessProcessingReadiness, Error> {
        let chunk_id = witness.chunk_id();
        let block_hash = chunk_id.block_hash;
        let block = match self.chain_store.get_block(&block_hash) {
            Ok(block) => block,
            Err(Error::DBNotFoundErr(err)) => {
                tracing::debug!(
                    target: "spice_chunk_validator",
                    ?chunk_id,
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
                ?chunk_id,
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
                chunk_id=?witness.chunk_id(),
                "processing ready pending state witnesses");
            self.validate_state_witness_and_send_endorsements(
                &witness_validation_context,
                witness,
                signer.clone(),
            )?;
        }
        Ok(())
    }

    fn handle_not_ready_state_witness(
        &mut self,
        witness: SpiceChunkStateWitness,
        _witness_size: usize,
    ) {
        // TODO(spice): Implement additional checks before adding witness to pending witnesses, see Client's orphan_witness_handling.rs.
        let block_hash = witness.chunk_id().block_hash;
        self.pending_witnesses.entry(block_hash).or_default().push(witness);
    }

    fn validate_state_witness_and_send_endorsements(
        &self,
        WitnessValidationContext { block, prev_block, prev_block_execution_results }: &WitnessValidationContext,
        witness: SpiceChunkStateWitness,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let chunk_id = witness.chunk_id().clone();
        let block_hash = chunk_id.block_hash;
        assert_eq!(&block_hash, block.header().hash());

        let pre_validation_result = spice_pre_validate_chunk_state_witness(
            &witness,
            &block,
            &prev_block,
            &prev_block_execution_results,
            self.epoch_manager.as_ref(),
            &self.chain_store,
        )?;

        // TODO(spice): Adjust logic to work when chunk may be missing.
        let chunk_height_created = block
            .chunks()
            .iter_raw()
            .find(|chunk| chunk.shard_id() == chunk_id.shard_id)
            .map(ShardChunkHeader::height_created)
            .unwrap();

        let epoch_id = self.epoch_manager.get_epoch_id(&block_hash)?;
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        let network_sender = self.network_adapter.clone().into_sender();
        let chunk_endorsement_tracker = self.chunk_endorsement_tracker.clone();
        self.validation_spawner.spawn("spice_stateless_validation", move || {
            // TODO(spice): Implement saving of invalid witnesses.
            let chunk_execution_result = match spice_validate_chunk_state_witness(
                witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
            ) {
                Ok(execution_result) => execution_result,
                Err(err) => {
                    near_chain::stateless_validation::metrics::CHUNK_WITNESS_VALIDATION_FAILED_TOTAL
                        .with_label_values(&[&chunk_id.shard_id.to_string(), err.prometheus_label_value()])
                        .inc();
                    tracing::error!(
                        target: "spice_chunk_validator",
                        ?err,
                        ?chunk_id,
                        "Failed to validate chunk"
                    );
                    return;
                }
            };

            let endorsement = ChunkEndorsement::new_with_execution_result(
                epoch_id,
                chunk_execution_result,
                block_hash,
                chunk_id.shard_id,
                chunk_height_created,
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
