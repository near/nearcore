use std::collections::{HashMap, HashSet};
use std::iter::repeat_n;
use std::num::NonZeroUsize;
use std::sync::Arc;

use rand::Rng as _;

use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt as _};
use near_async::messaging::{CanSend as _, Handler, IntoSender as _, Sender};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::spice_core::SpiceCoreReader;
use near_chain::spice_core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::stateless_validation::spice_chunk_validation::{
    spice_pre_validate_chunk_state_witness, spice_validate_chunk_state_witness,
};
use near_chain::types::RuntimeAdapter;
use near_chain::{ApplyChunksSpawner, Block, ChainGenesis, ChainStore, Error};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::spice_data_distribution::{
    SpiceChunkContractAccessesMessage, SpiceContractCodeResponseMessage,
};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_primitives::hash::CryptoHash;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{CodeBytes, CodeHash};
use near_primitives::stateless_validation::spice_chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::stateless_validation::spice_state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::state_witness::ChunkStateWitnessSize;
use near_primitives::types::{BlockExecutionResults, SpiceChunkId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Store;
use near_store::adapter::StoreAdapter as _;

// Each pendin chunk is up to ~80MB in the worst case (17MB witness + 64MB contracts). This results
// in a max of ~2GB of memory used for pending chunks.
// On the other hand if the validator follows 8 shards this means 3 pending chunks per shard on
// average, so we do not want to drop below that.
const MAX_PENDING_CHUNKS: usize = 24;

pub struct SpiceChunkValidatorActor {
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_adapter: PeerManagerAdapter,

    validator_signer: MutableValidatorSigner,
    core_reader: SpiceCoreReader,
    core_writer_sender: Sender<SpiceChunkEndorsementMessage>,

    /// Map holding witnesses we cannot process yet keyed by the block hash witness is for.
    pending_witnesses: HashMap<CryptoHash, Vec<SpiceChunkStateWitness>>,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,

    /// Per-chunk state for witnesses waiting on contract bytes.
    pending_chunks: lru::LruCache<SpiceChunkId, PendingChunkParts>,
}

/// Tracks the state of a chunk that is waiting on contract bytes and/or its witness.
struct PendingChunkParts {
    /// None = haven't received contract accesses message yet.
    /// Some(empty) = all contracts available (either cached or received).
    /// Some(non-empty) = still waiting for these contracts.
    missing: Option<HashSet<CodeHash>>,
    /// Contract bytes collected so far (from code responses).
    /// TODO(spice),TODO(pipelining): This is not strictly needed, as we could send the contracts for compilation/caching as they arrive without waiting for all of them + witness to be present.
    contracts: Vec<CodeBytes>,
    /// The witness, if it has arrived.
    witness: Option<SpiceChunkStateWitness>,
}

impl PendingChunkParts {
    fn new() -> Self {
        Self { missing: None, contracts: Vec::new(), witness: None }
    }
}

#[derive(Debug, PartialEq)]
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
        core_writer_sender: Sender<SpiceChunkEndorsementMessage>,
        validation_spawner: ApplyChunksSpawner,
    ) -> Self {
        let core_reader =
            SpiceCoreReader::new(store.chain_store(), epoch_manager.clone(), genesis.gas_limit);
        // TODO(spice): Assess if this limit still makes sense for spice.
        // See ChunkValidator::new in c/c/s/s/chunk_validator/mod.rs for rationale used currently.
        let validation_thread_limit =
            runtime_adapter.get_shard_limit(PROTOCOL_VERSION) as usize * 3;
        Self {
            pending_witnesses: HashMap::new(),
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            network_adapter,
            validator_signer,
            core_reader,
            core_writer_sender,
            validation_spawner: validation_spawner.into_spawner(validation_thread_limit),
            pending_chunks: lru::LruCache::new(NonZeroUsize::new(MAX_PENDING_CHUNKS).unwrap()),
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
            let next_block_hashes = self.chain_store.get_all_next_block_hashes(&block_hash);
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

impl Handler<SpiceChunkContractAccessesMessage> for SpiceChunkValidatorActor {
    fn handle(
        &mut self,
        SpiceChunkContractAccessesMessage(accesses): SpiceChunkContractAccessesMessage,
    ) {
        if let Err(err) = self.handle_spice_contract_accesses(accesses) {
            tracing::error!(target: "spice_chunk_validator", ?err, "error handling contract accesses");
        }
    }
}

impl Handler<SpiceContractCodeResponseMessage> for SpiceChunkValidatorActor {
    fn handle(
        &mut self,
        SpiceContractCodeResponseMessage(response): SpiceContractCodeResponseMessage,
    ) {
        if let Err(err) = self.handle_spice_contract_code_response(response) {
            tracing::error!(target: "spice_chunk_validator", ?err, "error handling contract code response");
        }
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceChunkValidatorWitnessSender {
    pub chunk_state_witness: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,
}

impl Handler<SpanWrapped<SpiceChunkStateWitnessMessage>> for SpiceChunkValidatorActor {
    fn handle(&mut self, msg: SpanWrapped<SpiceChunkStateWitnessMessage>) {
        let msg = msg.span_unwrap();
        let SpiceChunkStateWitnessMessage { witness, raw_witness_size, .. } = msg;
        let Some(signer) = self.validator_signer.get() else {
            tracing::error!(target: "spice_chunk_validator", ?witness, "received a chunk state witness but this is not a validator node");
            return;
        };
        if let Err(err) = self.process_chunk_state_witness(witness, raw_witness_size, signer) {
            tracing::error!(target: "spice_chunk_validator", ?err, "error processing chunk state witness");
        }
    }
}

impl SpiceChunkValidatorActor {
    fn process_chunk_state_witness(
        &mut self,
        witness: SpiceChunkStateWitness,
        _raw_witness_size: ChunkStateWitnessSize,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let chunk_id = witness.chunk_id().clone();
        tracing::debug!(
            target: "spice_chunk_validator",
            ?chunk_id,
            "process_chunk_state_witness",
        );

        // TODO(spice): Implementing saving latest witness based on configuration.

        match self.witness_processing_readiness(&witness)? {
            WitnessProcessingReadiness::NotReady => {
                // Block not ready: store in pending_witnesses for block arrival notification.
                self.pending_witnesses.entry(chunk_id.block_hash).or_default().push(witness);
                Ok(())
            }
            WitnessProcessingReadiness::Ready(_) => {
                // Block ready: store witness in pending_chunks and try to finalize.
                self.pending_chunks
                    .get_or_insert_mut(chunk_id.clone(), PendingChunkParts::new)
                    .witness = Some(witness);
                self.try_finalize_chunk(&chunk_id, signer)
            }
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
            self.core_reader.get_block_execution_results(prev_block.header())?
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
        let Some(_prev_block_execution_results) =
            self.core_reader.get_block_execution_results(prev_block.header())?
        else {
            tracing::debug!(
                target: "spice_chunk_validator",
                ?prev_hash,
                "process_ready_pending_state_witnesses: new block is available, but some of the prev block execution results are still missing");
            return Ok(());
        };

        let ready_witnesses = self.pending_witnesses.remove(block.header().hash());
        for witness in ready_witnesses.into_iter().flatten() {
            let chunk_id = witness.chunk_id().clone();
            tracing::debug!(
                target: "spice_chunk_validator",
                ?prev_hash,
                ?chunk_id,
                "processing ready pending state witness");
            self.pending_chunks
                .get_or_insert_mut(chunk_id.clone(), PendingChunkParts::new)
                .witness = Some(witness);
            self.try_finalize_chunk(&chunk_id, signer.clone())?;
        }
        Ok(())
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

        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        let network_sender = self.network_adapter.clone().into_sender();
        let block_height = block.header().height();
        let core_writer_sender = self.core_writer_sender.clone();
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
                        ?block_height,
                        "failed to validate chunk"
                    );
                    return;
                }
            };

            let endorsement = SpiceChunkEndorsement::new(
                chunk_id,
                chunk_execution_result,
                &signer,
            );
            send_spice_chunk_endorsement(
                endorsement.clone(),
                epoch_manager.as_ref(),
                &network_sender,
                &signer,
            );
            core_writer_sender.send(SpiceChunkEndorsementMessage(endorsement));
        });
        Ok(())
    }

    /// Handles contract accesses message from a chunk producer.
    /// Checks the compiled contract cache for each hash, then requests any missing
    /// contracts from a random chunk producer.
    fn handle_spice_contract_accesses(
        &mut self,
        accesses: near_primitives::stateless_validation::contract_distribution::SpiceChunkContractAccesses,
    ) -> Result<(), Error> {
        let chunk_id = accesses.chunk_id().clone();

        // Verify signature of accesses message against any chunk producer for the shard
        // in the epoch (any node tracking the shard may send contract accesses).
        let epoch_id = self.epoch_manager.get_epoch_id(&chunk_id.block_hash)?;
        let producers =
            self.epoch_manager.get_epoch_chunk_producers_for_shard(&epoch_id, chunk_id.shard_id)?;
        let is_valid_signature = producers.iter().any(|account_id| {
            let Ok(validator) =
                self.epoch_manager.get_validator_by_account_id(&epoch_id, account_id)
            else {
                return false;
            };
            accesses.verify_signature(validator.public_key())
        });
        if !is_valid_signature {
            return Err(Error::Other("invalid spice contract accesses signature".to_owned()));
        }

        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        let runtime_config = self.runtime_adapter.get_runtime_config(protocol_version);
        let cache = self.runtime_adapter.compiled_contract_cache();

        let mut missing = HashSet::new();
        for code_hash in accesses.contracts() {
            if crate::stateless_validation::contracts_cache_contains_contract(
                cache,
                code_hash,
                runtime_config,
            ) {
                continue;
            }
            missing.insert(code_hash.clone());
        }

        if !missing.is_empty() {
            let mut producers = self
                .epoch_manager
                .get_epoch_chunk_producers_for_shard(&epoch_id, chunk_id.shard_id)?;
            let target = producers.swap_remove(rand::thread_rng().gen_range(0..producers.len()));
            let signer = self
                .validator_signer
                .get()
                .ok_or_else(|| Error::NotAValidator("no signer".to_owned()))?;
            let request = near_primitives::stateless_validation::contract_distribution::SpiceContractCodeRequest::new(
                chunk_id.clone(),
                missing.clone(),
                &signer,
            );
            // TODO(spice): retry with different producers if request fails or times out.
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpiceContractCodeRequest(target, request),
            ));
        }

        self.pending_chunks.get_or_insert_mut(chunk_id.clone(), PendingChunkParts::new).missing =
            Some(missing);

        let signer = self
            .validator_signer
            .get()
            .ok_or_else(|| Error::NotAValidator("no signer".to_owned()))?;
        self.try_finalize_chunk(&chunk_id, signer)
    }

    /// Handles a contract code response from a chunk producer.
    /// Resolves received contracts across all pending chunks (not just the chunk_id
    /// in the response), since multiple chunks may be waiting on the same contract.
    fn handle_spice_contract_code_response(
        &mut self,
        response: near_primitives::stateless_validation::contract_distribution::SpiceContractCodeResponse,
    ) -> Result<(), Error> {
        let contracts = response.decompress_contracts()?;

        // Map code_hash -> bytes for the received contracts.
        let mut received: HashMap<CodeHash, CodeBytes> = HashMap::new();
        for contract in contracts {
            let hash = CodeHash(near_primitives::hash::hash(&contract.0));
            received.insert(hash, contract);
        }

        // Resolve across all pending chunks that are waiting on any of these contracts.
        let mut maybe_ready: Vec<SpiceChunkId> = Vec::new();
        for (chunk_id, entry) in &mut self.pending_chunks {
            if let Some(missing) = &mut entry.missing {
                let mut resolved_any = false;
                for (hash, bytes) in &received {
                    if missing.remove(hash) {
                        entry.contracts.push(bytes.clone());
                        resolved_any = true;
                    }
                }
                if resolved_any && missing.is_empty() {
                    maybe_ready.push(chunk_id.clone());
                }
            }
        }

        let signer = self
            .validator_signer
            .get()
            .ok_or_else(|| Error::NotAValidator("no signer".to_owned()))?;
        for chunk_id in maybe_ready {
            self.try_finalize_chunk(&chunk_id, signer.clone())?;
        }
        Ok(())
    }

    /// Attempts to finalize a chunk that is pending on contracts and/or witness.
    /// Succeeds when: witness is present, contract accesses have been received,
    /// all missing contracts have been fulfilled, and the block is ready.
    /// On success, merges contract bytes into the witness base_state and validates.
    fn try_finalize_chunk(
        &mut self,
        chunk_id: &SpiceChunkId,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let can_finalize = match self.pending_chunks.peek(chunk_id) {
            None => false,
            Some(entry) => {
                entry.witness.is_some()
                    && matches!(&entry.missing, Some(missing) if missing.is_empty())
            }
        };

        if !can_finalize {
            return Ok(());
        }

        // Remove entry so we own the data and avoid borrow conflicts.
        let PendingChunkParts { missing: _, contracts, witness } =
            self.pending_chunks.pop(chunk_id).unwrap();
        let mut witness = witness.unwrap();

        // Check block readiness.
        match self.witness_processing_readiness(&witness)? {
            WitnessProcessingReadiness::NotReady => {
                // Put it back — block isn't ready yet.
                self.pending_chunks.put(
                    chunk_id.clone(),
                    PendingChunkParts {
                        missing: Some(HashSet::new()),
                        contracts,
                        witness: Some(witness),
                    },
                );
                Ok(())
            }
            WitnessProcessingReadiness::Ready(ctx) => {
                // Merge contract bytes into witness base_state.
                if !contracts.is_empty() {
                    let PartialState::TrieValues(values) =
                        &mut witness.mut_main_state_transition().base_state;
                    values.extend(contracts.into_iter().map(|code| code.0));
                }

                self.validate_state_witness_and_send_endorsements(&ctx, witness, signer)
            }
        }
    }
}

pub fn send_spice_chunk_endorsement(
    endorsement: SpiceChunkEndorsement,
    epoch_manager: &dyn EpochManagerAdapter,
    network_sender: &Sender<PeerManagerMessageRequest>,
    signer: &ValidatorSigner,
) {
    let block_hash = endorsement.block_hash();
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

    let endorsements = repeat_n(endorsement, validators.len());
    for (account, endorsement) in validators.into_iter().zip(endorsements) {
        if &account == signer.validator_id() {
            continue;
        }
        network_sender.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpiceChunkEndorsement(account, endorsement),
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
