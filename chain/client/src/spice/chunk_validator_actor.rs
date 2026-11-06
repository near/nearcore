use crate::stateless_validation::contracts_cache_contains_contract;
use lru::LruCache;
use near_async::futures::{AsyncComputationSpawner, AsyncComputationSpawnerExt as _};
use near_async::messaging::{CanSend as _, Handler, IntoSender as _, Sender};
use near_chain::spice::activation::{
    SpiceMessageGate, SpiceMessageKind, is_spice_or_last_pre_spice_block,
};
use near_chain::spice::chunk_validation::{
    spice_pre_validate_chunk_state_witness, spice_validate_chunk_state_witness,
};
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::{ExecutionResultEndorsed, ProcessedBlock};
use near_chain::types::RuntimeAdapter;
use near_chain::{ApplyChunksSpawner, Block, ChainGenesis, ChainStore, ChainStoreAccess, Error};
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::recv_permit::RecvMessagePermit;
use near_network::spice::data_distribution::{
    SpiceChunkContractAccessesMessage, SpiceContractCodeResponseMessage,
};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_primitives::errors::EpochError;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::contract_distribution::{
    CodeBytes, CodeHash, MAX_CONTRACTS_PER_REQUEST,
    MAX_UNCOMPRESSED_CONTRACT_CODE_PER_REQUEST_SIZE, SpiceChunkContractAccesses,
    SpiceContractCodeRequest, SpiceContractCodeResponse, total_code_size,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::types::{BlockExecutionResults, SpiceChunkId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Store;
use near_store::adapter::StoreAdapter as _;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::iter::repeat_n;
use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;

mod boundary;

// Each pending chunk stores the uncompressed witness plus uncompressed contracts.
// In the worst case the witness is bounded by MAX_UNCOMPRESSED_STATE_WITNESS_SIZE (64 MiB)
// and the contracts by MAX_UNCOMPRESSED_CONTRACT_CODE_PER_REQUEST_SIZE (256 MiB), giving
// ~320 MiB per chunk.  MAX_PENDING_CHUNKS * 320 MiB ≈ 7.5 GiB worst-case memory budget.
// On the other hand if the validator follows 8 shards this means 3 pending chunks per shard on
// average, so we do not want to drop below that.
// TODO(spice): add test covering the relationship between constants.
// TODO(spice-data-distribution): a pending chunk can hold several witnesses, so this bounds
// chunks, not memory.
pub(crate) const MAX_PENDING_CHUNKS: usize = 24;

/// How many blocks we have not received yet we hold contract accesses for. Honest accesses
/// only run ahead of their block by network races, so a few blocks suffice; the cap bounds
/// what a chunk producer can make us hold by naming blocks that never arrive.
pub(crate) const MAX_BLOCKS_WITH_PENDING_CONTRACT_ACCESSES: usize = 10;

pub struct SpiceChunkValidatorActor {
    chain_store: ChainStore,
    runtime_adapter: Arc<dyn RuntimeAdapter>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_adapter: PeerManagerAdapter,

    validator_signer: MutableValidatorSigner,
    core_reader: SpiceCoreReader,
    core_writer_sender: Sender<SpiceChunkEndorsementMessage>,

    /// Witnesses we cannot process yet because the referenced block, or the execution
    /// results it depends on, are not in the store.
    waiting_for_block: HashMap<CryptoHash, Vec<SpiceChunkStateWitnessMessage>>,
    /// Contract accesses whose block is not in the store yet, keyed by block and then by
    /// shard and sender, keeping the first message of each sender.
    pending_contract_accesses:
        LruCache<CryptoHash, HashMap<(ShardId, AccountId), SpiceChunkContractAccesses>>,
    validation_spawner: Arc<dyn AsyncComputationSpawner>,

    /// Per-chunk witness candidates and contract code, until our endorsement exists.
    partial_chunk_data: LruCache<SpiceChunkId, PartialChunkData>,

    spice_gate: SpiceMessageGate,
}

/// A chunk's witness candidates and the contract code requested for them.
struct PartialChunkData {
    /// Whether a contract accesses message arrived; only the first one starts a code request.
    accesses_received: bool,
    /// Code hashes requested from a producer.
    // TODO(spice-data-distribution): code is requested once and never again, so a request
    // that is never answered stalls every candidate needing that code until the chunk is evicted.
    requested_code: HashSet<CodeHash>,
    /// Requested code received so far.
    /// TODO(spice),TODO(pipelining): This is not strictly needed - we could send the contracts for compilation/caching as they arrive without waiting for all of them + witness to be present.
    code: HashMap<CodeHash, CodeBytes>,
    /// Candidates whose block is ready, waiting for contract code.
    candidates: Vec<WitnessCandidate>,
    /// Hashes of the candidates submitted for validation.
    submitted: HashSet<CryptoHash>,
}

/// A witness waiting for the contract code its own accesses list names.
struct WitnessCandidate {
    witness: SpiceChunkStateWitness,
    witness_hash: CryptoHash,
    /// Listed code neither cached nor received yet.
    missing: HashSet<CodeHash>,
}

impl PartialChunkData {
    fn new() -> Self {
        Self {
            accesses_received: false,
            requested_code: HashSet::new(),
            code: HashMap::new(),
            candidates: Vec::new(),
            submitted: HashSet::new(),
        }
    }

    /// Marks the code in `contracts` not requested yet as requested, and returns it.
    fn take_unrequested(&mut self, contracts: HashSet<CodeHash>) -> HashSet<CodeHash> {
        let unrequested: HashSet<CodeHash> =
            contracts.into_iter().filter(|hash| !self.requested_code.contains(hash)).collect();
        self.requested_code.extend(unrequested.iter().cloned());
        unrequested
    }

    /// Whether a witness with this hash is held or was submitted.
    fn has_witness(&self, witness_hash: &CryptoHash) -> bool {
        self.submitted.contains(witness_hash)
            || self.candidates.iter().any(|candidate| &candidate.witness_hash == witness_hash)
    }
}

#[derive(Debug, PartialEq)]
pub struct SpiceChunkStateWitnessMessage {
    pub witness: SpiceChunkStateWitness,
    /// Hash of the witness's committed encoding; equal witnesses have equal hashes.
    pub witness_hash: CryptoHash,
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
            waiting_for_block: HashMap::new(),
            pending_contract_accesses: LruCache::new(
                NonZeroUsize::new(MAX_BLOCKS_WITH_PENDING_CONTRACT_ACCESSES).unwrap(),
            ),
            chain_store: ChainStore::new(store, true, genesis.transaction_validity_period),
            runtime_adapter,
            epoch_manager,
            network_adapter,
            validator_signer,
            core_reader,
            core_writer_sender,
            validation_spawner: validation_spawner.into_spawner(validation_thread_limit),
            partial_chunk_data: LruCache::new(NonZeroUsize::new(MAX_PENDING_CHUNKS).unwrap()),
            spice_gate: SpiceMessageGate::default(),
        }
    }

    /// How many spice messages of `kind` this actor dropped because spice is not active.
    #[cfg(feature = "test_features")]
    pub fn spice_dropped_count(&self, kind: SpiceMessageKind) -> u64 {
        self.spice_gate.dropped_count(kind)
    }

    /// How many contract accesses messages are buffered for blocks not in the store yet.
    #[cfg(test)]
    pub(crate) fn num_pending_contract_accesses(&self) -> usize {
        self.pending_contract_accesses.iter().map(|(_, by_sender)| by_sender.len()).sum()
    }

    /// Whether anything is held for the chunk: candidates, submitted hashes or code.
    #[cfg(test)]
    pub(crate) fn is_chunk_pending(&self, chunk_id: &SpiceChunkId) -> bool {
        self.partial_chunk_data.contains(chunk_id)
    }
}

// TODO(spice): Since data distributor makes sure block is available before witness is sent to the
// chunk validator actor we don't need to handle possibility of missing blocks in this actor.
impl Handler<ProcessedBlock> for SpiceChunkValidatorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        // Pre-spice chunks are validated as part of block processing; no witness
        // can be waiting on a pre-spice block — except a last pre-spice block, whose
        // boundary witness can arrive before the block itself.
        match is_spice_or_last_pre_spice_block(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &block_hash,
        ) {
            Ok(true) => {}
            Ok(false) => return,
            Err(err) => {
                tracing::error!(target: "spice_chunk_validator", %block_hash, ?err, "failed to get block header");
                return;
            }
        }
        let block = match self.chain_store.get_block(&block_hash) {
            Ok(block) => block,
            Err(err) => {
                tracing::error!(target: "spice_chunk_validator", %block_hash, ?err, "failed to get block");
                return;
            }
        };

        if let Some(signer) = self.validator_signer.get() {
            if let Err(err) = self.process_waiting_for_block(block, signer) {
                tracing::error!(target: "spice_chunk_validator", %block_hash, ?err, "failed to process data waiting for block");
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
                if let Err(err) = self.process_waiting_for_block(next_block, signer.clone()) {
                    tracing::error!(target: "spice_chunk_validator", %next_block_hash, %block_hash, ?err, "failed to process data waiting for block");
                }
            }
        }
    }
}

impl Handler<SpiceChunkContractAccessesMessage> for SpiceChunkValidatorActor {
    fn handle(
        &mut self,
        SpiceChunkContractAccessesMessage(accesses, _recv_permit): SpiceChunkContractAccessesMessage,
    ) {
        if !self.spice_gate.should_process(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            SpiceMessageKind::ContractAccesses,
            &accesses.chunk_id().block_hash,
        ) {
            return;
        }
        if let Err(err) = self.handle_spice_contract_accesses(accesses) {
            tracing::error!(target: "spice_chunk_validator", ?err, "error handling contract accesses");
        }
    }
}

impl Handler<SpiceContractCodeResponseMessage> for SpiceChunkValidatorActor {
    fn handle(
        &mut self,
        SpiceContractCodeResponseMessage(response, _recv_permit): SpiceContractCodeResponseMessage,
    ) {
        if !self.spice_gate.should_process(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            SpiceMessageKind::ContractCodeResponse,
            &response.chunk_id().block_hash,
        ) {
            return;
        }
        if let Err(err) = self.handle_spice_contract_code_response(response) {
            tracing::error!(target: "spice_chunk_validator", ?err, "error handling contract code response");
        }
    }
}

impl Handler<SpanWrapped<SpiceChunkStateWitnessMessage>> for SpiceChunkValidatorActor {
    fn handle(&mut self, msg: SpanWrapped<SpiceChunkStateWitnessMessage>) {
        let msg = msg.span_unwrap();
        if !self.spice_gate.should_process(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            SpiceMessageKind::StateWitness,
            &msg.witness.chunk_id().block_hash,
        ) {
            return;
        }
        let Some(signer) = self.validator_signer.get() else {
            tracing::error!(target: "spice_chunk_validator", witness = ?msg.witness, "received a chunk state witness but this is not a validator node");
            return;
        };
        if let Err(err) = self.process_chunk_state_witness(msg, signer) {
            tracing::error!(target: "spice_chunk_validator", ?err, "error processing chunk state witness");
        }
    }
}

impl SpiceChunkValidatorActor {
    fn process_chunk_state_witness(
        &mut self,
        msg: SpiceChunkStateWitnessMessage,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let chunk_id = msg.witness.chunk_id().clone();
        tracing::debug!(
            target: "spice_chunk_validator",
            ?chunk_id,
            "process_chunk_state_witness",
        );
        // TODO(spice): Implementing saving latest witness based on configuration.

        match self.witness_processing_readiness(&msg.witness)? {
            WitnessProcessingReadiness::NotReady => {
                // Block not ready: store for block arrival notification.
                // TODO(spice): Implement additional checks (size limit, distance to head) before adding witness to `waiting_for_block`. See non-spice handle_orphan_witness().
                self.waiting_for_block.entry(chunk_id.block_hash).or_default().push(msg);
                Ok(())
            }
            WitnessProcessingReadiness::Ready(_) => self.add_candidate(msg, signer),
        }
    }

    fn witness_processing_readiness(
        &self,
        witness: &SpiceChunkStateWitness,
    ) -> Result<WitnessProcessingReadiness, Error> {
        let chunk_id = witness.chunk_id();
        let block_hash = chunk_id.block_hash;
        let shard_id = chunk_id.shard_id;
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
        if !block.is_spice_block() {
            let context = self.boundary_witness_validation_context(block)?;
            return Ok(WitnessProcessingReadiness::Ready(context));
        }
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

        let prev_validator_proposals = match self
            .core_reader
            .prev_validator_proposals(prev_block.hash(), shard_id)
        {
            Ok(proposals) => proposals,
            Err(err) => {
                tracing::debug!(
                        target: "spice_chunk_validator",
                        ?chunk_id,
                        prev_block_hash = ?prev_block.hash(),
                        ?err,
                        "witness for block isn't ready for processing; missing execution results for validator proposals");
                return Ok(WitnessProcessingReadiness::NotReady);
            }
        };

        Ok(WitnessProcessingReadiness::Ready(WitnessValidationContext {
            block,
            prev_block,
            prev_block_execution_results,
            prev_validator_proposals,
        }))
    }

    fn process_waiting_for_block(
        &mut self,
        block: Arc<Block>,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let block_hash = *block.header().hash();

        // Process deferred contract accesses first so that partial_chunk_data entries
        // have their `missing` set before witnesses try to finalize.
        let contract_accesses = self.pending_contract_accesses.pop(&block_hash);
        for accesses in contract_accesses.into_iter().flat_map(HashMap::into_values) {
            if let Err(err) = self.handle_spice_contract_accesses(accesses) {
                tracing::error!(target: "spice_chunk_validator", %block_hash, ?err, "error handling deferred contract accesses");
            }
        }

        let Some(witnesses) = self.waiting_for_block.remove(&block_hash) else {
            return Ok(());
        };

        let prev_hash = *block.header().prev_hash();
        let prev_block = self.chain_store.get_block(&prev_hash)?;
        if block.is_spice_block()
            && self.core_reader.get_block_execution_results(prev_block.header())?.is_none()
        {
            tracing::debug!(
                target: "spice_chunk_validator",
                ?prev_hash,
                "process_waiting_for_block: new block is available, but some of the prev block execution results are still missing");
            // Put witnesses back; contract accesses already went through
            // handle_spice_contract_accesses and are now in partial_chunk_data.
            self.waiting_for_block.insert(block_hash, witnesses);
            return Ok(());
        };

        let mut unready_witnesses = Vec::new();
        for msg in witnesses {
            let shard_id = msg.witness.chunk_id().shard_id;
            if block.is_spice_block() {
                match self.core_reader.prev_validator_proposals(prev_block.hash(), shard_id) {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::debug!(
                            target: "spice_chunk_validator",
                            ?prev_hash,
                            chunk_id=?msg.witness.chunk_id(),
                            ?err,
                            "witness not ready; missing execution results for validator proposals");
                        unready_witnesses.push(msg);
                        continue;
                    }
                };
            }
            tracing::debug!(
                target: "spice_chunk_validator",
                ?prev_hash,
                chunk_id = ?msg.witness.chunk_id(),
                "processing ready pending state witness");
            if let Err(err) = self.add_candidate(msg, signer.clone()) {
                self.requeue_waiting_witnesses(&block_hash, unready_witnesses);
                return Err(err);
            }
        }

        self.requeue_waiting_witnesses(&block_hash, unready_witnesses);
        Ok(())
    }

    fn requeue_waiting_witnesses(
        &mut self,
        block_hash: &CryptoHash,
        witnesses: Vec<SpiceChunkStateWitnessMessage>,
    ) {
        if !witnesses.is_empty() {
            self.waiting_for_block.entry(*block_hash).or_default().extend(witnesses);
        }
    }

    fn validate_state_witness_and_send_endorsements(
        &self,
        WitnessValidationContext {
            block,
            prev_block,
            prev_block_execution_results,
            prev_validator_proposals,
        }: &WitnessValidationContext,
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
            prev_validator_proposals.clone(),
        )?;

        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        let network_sender = self.network_adapter.clone().into_sender();
        let block_height = block.header().height();
        let core_writer_sender = self.core_writer_sender.clone();
        // TODO(spice-data-distribution): candidates of all shards share this pool; bound each
        // shard's share, so a shard's fake witnesses cannot delay validation of other shards.
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
                        .with_label_values(&[chunk_id.shard_id.to_string().as_str(), err.prometheus_label_value()])
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
            core_writer_sender.send(SpiceChunkEndorsementMessage(endorsement, RecvMessagePermit::none()));
        });
        Ok(())
    }

    /// Handles a contract accesses message from a chunk producer. The first one for a chunk
    /// requests the listed code not cached yet from its sender, before any witness arrives.
    fn handle_spice_contract_accesses(
        &mut self,
        accesses: SpiceChunkContractAccesses,
    ) -> Result<(), Error> {
        let chunk_id = accesses.chunk_id().clone();

        if accesses.contracts().len() > MAX_CONTRACTS_PER_REQUEST {
            tracing::debug!(
                target: "spice_chunk_validator",
                ?chunk_id,
                num_contracts = accesses.contracts().len(),
                "contract accesses message exceeds maximum number of contracts"
            );
            return Ok(());
        }

        // Verify signature of accesses message against any chunk producer for the shard
        // in the epoch (any node tracking the shard may send contract accesses).
        let epoch_id = match self.epoch_manager.get_epoch_id(&chunk_id.block_hash) {
            Ok(epoch_id) => epoch_id,
            Err(EpochError::EpochOutOfBounds(_)) | Err(EpochError::MissingBlock(_)) => {
                // Block not in store yet — defer until it arrives.
                tracing::debug!(
                    target: "spice_chunk_validator",
                    ?chunk_id,
                    "contract accesses for block not yet available; deferring",
                );
                return self.add_pending_contract_accesses(accesses);
            }
            Err(err) => return Err(err.into()),
        };
        let next_epoch_id = self.epoch_manager.get_next_epoch_id(&chunk_id.block_hash)?;
        let Some(sender) =
            self.find_contract_accesses_sender(&epoch_id, &next_epoch_id, &accesses)?
        else {
            return Err(Error::Other("invalid spice contract accesses signature".to_owned()));
        };

        if self.partial_chunk_data.peek(&chunk_id).is_some_and(|entry| entry.accesses_received) {
            return Ok(());
        }
        let signer = self
            .validator_signer
            .get()
            .ok_or_else(|| Error::NotAValidator("no signer".to_owned()))?;
        let uncached = self.uncached_contracts(&epoch_id, accesses.contracts())?;
        let entry =
            self.partial_chunk_data.get_or_insert_mut(chunk_id.clone(), PartialChunkData::new);
        entry.accesses_received = true;
        let to_request = entry.take_unrequested(uncached);
        if to_request.is_empty() {
            return Ok(());
        }
        // The sender executed the chunk, so it has the code.
        // TODO(spice),TODO(spice-contract-distribution): retry with another sender if request fails or times out.
        self.request_code(&chunk_id, sender, to_request, &signer);
        Ok(())
    }

    /// Buffers contract accesses for a block not in the store yet.
    ///
    /// Such a block is past the final head, so the sender must be a chunk producer for the
    /// shard in the final head's epoch or the next one. The exact epoch is checked once the
    /// block arrives.
    fn add_pending_contract_accesses(
        &mut self,
        accesses: SpiceChunkContractAccesses,
    ) -> Result<(), Error> {
        let chunk_id = accesses.chunk_id().clone();
        let final_head = self.chain_store.final_head()?;
        let mut sender = None;
        // The epoch after the next one is not known yet, so a block of the next epoch is
        // checked against that epoch's keys only.
        for (epoch_id, next_epoch_id) in [
            (final_head.epoch_id, final_head.next_epoch_id),
            (final_head.next_epoch_id, final_head.next_epoch_id),
        ] {
            let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
            if shard_layout.get_shard_index(chunk_id.shard_id).is_err() {
                continue;
            }
            sender = self.find_contract_accesses_sender(&epoch_id, &next_epoch_id, &accesses)?;
            if sender.is_some() {
                break;
            }
        }
        let Some(sender) = sender else {
            return Err(Error::Other(
                "invalid signature on spice contract accesses for unknown block".to_owned(),
            ));
        };
        self.pending_contract_accesses
            .get_or_insert_mut(chunk_id.block_hash, HashMap::new)
            .entry((chunk_id.shard_id, sender))
            .or_insert(accesses);
        Ok(())
    }

    /// The chunk producer for the shard in `epoch_id` that signed `accesses` with its key in
    /// `epoch_id` or `next_epoch_id`, if any.
    fn find_contract_accesses_sender(
        &self,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
        accesses: &SpiceChunkContractAccesses,
    ) -> Result<Option<AccountId>, Error> {
        let producers = self
            .epoch_manager
            .get_epoch_chunk_producers_for_shard(epoch_id, accesses.chunk_id().shard_id)?;
        // TODO(spice),TODO(spice-perf): We could get the expected public key from the message (or
        // by using sender if possible), check the signature, and then check the public id is in an expected hash set (or just iterate them), to avoid checking many signatures.
        Ok(producers.into_iter().find(|account_id| {
            let Ok(public_keys) =
                self.epoch_manager.get_validator_signing_keys(epoch_id, next_epoch_id, account_id)
            else {
                return false;
            };
            public_keys.iter().any(|public_key| accesses.verify_signature(public_key))
        }))
    }

    /// Handles a contract code response from a chunk producer. Code is kept for every pending
    /// chunk that requested it, not just the one in the response.
    fn handle_spice_contract_code_response(
        &mut self,
        response: SpiceContractCodeResponse,
    ) -> Result<(), Error> {
        let received: HashMap<CodeHash, CodeBytes> = response
            .decompress_contracts()?
            .into_iter()
            .map(|contract| (CodeHash(hash(&contract.0)), contract))
            .collect();
        let mut maybe_ready: Vec<SpiceChunkId> = Vec::new();
        for (chunk_id, entry) in &mut self.partial_chunk_data {
            let PartialChunkData { requested_code, code, candidates, .. } = entry;
            let mut added = false;
            for (hash, bytes) in &received {
                if requested_code.contains(hash) && !code.contains_key(hash) {
                    code.insert(hash.clone(), bytes.clone());
                    added = true;
                }
            }
            if !added {
                continue;
            }
            for candidate in candidates {
                candidate.missing.retain(|hash| !code.contains_key(hash));
            }
            maybe_ready.push(chunk_id.clone());
        }
        let signer = self
            .validator_signer
            .get()
            .ok_or_else(|| Error::NotAValidator("no signer".to_owned()))?;
        for chunk_id in maybe_ready {
            self.validate_ready_candidates(&chunk_id, signer.clone())?;
        }
        Ok(())
    }

    /// Adds a witness whose block is ready as a candidate of its chunk, unless our endorsement
    /// exists or the same witness is held or was submitted. Requests the listed code nobody was
    /// asked for yet, then submits the chunk's ready candidates.
    fn add_candidate(
        &mut self,
        msg: SpiceChunkStateWitnessMessage,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        let SpiceChunkStateWitnessMessage { witness, witness_hash } = msg;
        let chunk_id = witness.chunk_id().clone();
        if self.forget_if_endorsed(&chunk_id, signer.validator_id()) {
            return Ok(());
        }
        // A chunk cannot access that many contracts, so the witness is not one to validate.
        if witness.contract_accesses().len() > MAX_CONTRACTS_PER_REQUEST {
            tracing::debug!(
                target: "spice_chunk_validator",
                ?chunk_id,
                num_contracts = witness.contract_accesses().len(),
                "witness lists more contracts than a chunk can access"
            );
            return Ok(());
        }
        let epoch_id = self.epoch_manager.get_epoch_id(&chunk_id.block_hash)?;
        let uncached = self.uncached_contracts(&epoch_id, witness.contract_accesses())?;
        // TODO(spice-data-distribution): this always asks the first producer; spread requesters
        // over the producers and retry with another one if it does not answer. A producer
        // answers one code request per chunk and requester, so code asked here from a producer
        // already asked for this chunk never arrives, and the candidate waits until evicted.
        let producer = self
            .epoch_manager
            .get_epoch_chunk_producers_for_shard(&epoch_id, chunk_id.shard_id)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Other("chunk has no producers".to_owned()))?;
        let entry =
            self.partial_chunk_data.get_or_insert_mut(chunk_id.clone(), PartialChunkData::new);
        if entry.has_witness(&witness_hash) {
            return Ok(());
        }
        let missing: HashSet<CodeHash> =
            uncached.into_iter().filter(|hash| !entry.code.contains_key(hash)).collect();
        let to_request = entry.take_unrequested(missing.clone());
        entry.candidates.push(WitnessCandidate { witness, witness_hash, missing });
        if !to_request.is_empty() {
            self.request_code(&chunk_id, producer, to_request, &signer);
        }
        self.validate_ready_candidates(&chunk_id, signer)
    }

    /// Submits for validation each candidate of the chunk whose code is all here. Once our
    /// endorsement exists, drops the chunk instead.
    fn validate_ready_candidates(
        &mut self,
        chunk_id: &SpiceChunkId,
        signer: Arc<ValidatorSigner>,
    ) -> Result<(), Error> {
        if self.forget_if_endorsed(chunk_id, signer.validator_id()) {
            return Ok(());
        }
        let Some(entry) = self.partial_chunk_data.get_mut(chunk_id) else {
            return Ok(());
        };
        let (ready, waiting): (Vec<_>, Vec<_>) =
            mem::take(&mut entry.candidates).into_iter().partition(|c| c.missing.is_empty());
        entry.candidates = waiting;
        for WitnessCandidate { mut witness, witness_hash, .. } in ready {
            // A candidate whose block is not ready, or whose readiness cannot be read, waits for
            // the block again; it is not submitted and its code is not merged.
            let ctx = match self.witness_processing_readiness(&witness) {
                Ok(WitnessProcessingReadiness::Ready(ctx)) => ctx,
                readiness => {
                    if let Err(err) = readiness {
                        tracing::error!(target: "spice_chunk_validator", ?err, ?chunk_id, ?witness_hash, "failed to check witness readiness");
                    }
                    let msg = SpiceChunkStateWitnessMessage { witness, witness_hash };
                    self.waiting_for_block.entry(chunk_id.block_hash).or_default().push(msg);
                    continue;
                }
            };
            let entry = self
                .partial_chunk_data
                .get_mut(chunk_id)
                .expect("nothing removes the chunk while its candidates are submitted");
            let code: Vec<CodeBytes> = witness
                .contract_accesses()
                .iter()
                .filter_map(|hash| entry.code.get(hash).cloned())
                .collect();
            if total_code_size(&code) > MAX_UNCOMPRESSED_CONTRACT_CODE_PER_REQUEST_SIZE {
                tracing::debug!(
                    target: "spice_chunk_validator",
                    ?chunk_id,
                    ?witness_hash,
                    "witness needs more contract code than one request covers; dropping it"
                );
                continue;
            }
            entry.submitted.insert(witness_hash);
            witness.merge_contracts(code);
            // A failed candidate must not keep the others from validation.
            if let Err(err) =
                self.validate_state_witness_and_send_endorsements(&ctx, witness, signer.clone())
            {
                tracing::debug!(target: "spice_chunk_validator", ?err, ?chunk_id, ?witness_hash, "witness candidate failed pre-validation");
            }
        }
        Ok(())
    }

    /// Whether our endorsement of the chunk exists. If it does, drops every witness held for
    /// the chunk.
    fn forget_if_endorsed(&mut self, chunk_id: &SpiceChunkId, me: &AccountId) -> bool {
        if !self.core_reader.endorsement_exists(&chunk_id.block_hash, chunk_id.shard_id, me) {
            return false;
        }
        self.partial_chunk_data.pop(chunk_id);
        if let Entry::Occupied(mut waiting) = self.waiting_for_block.entry(chunk_id.block_hash) {
            waiting.get_mut().retain(|msg| msg.witness.chunk_id() != chunk_id);
            if waiting.get().is_empty() {
                waiting.remove();
            }
        }
        true
    }

    /// The contracts in `contracts` not in the compiled contract cache of `epoch_id`.
    fn uncached_contracts<'a>(
        &self,
        epoch_id: &EpochId,
        contracts: impl IntoIterator<Item = &'a CodeHash>,
    ) -> Result<HashSet<CodeHash>, Error> {
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        let runtime_config = self.runtime_adapter.get_runtime_config(protocol_version);
        let cache = self.runtime_adapter.compiled_contract_cache();
        Ok(contracts
            .into_iter()
            .filter(|hash| !contracts_cache_contains_contract(cache, hash, runtime_config))
            .cloned()
            .collect())
    }

    fn request_code(
        &self,
        chunk_id: &SpiceChunkId,
        producer: AccountId,
        contracts: HashSet<CodeHash>,
        signer: &ValidatorSigner,
    ) {
        let request = SpiceContractCodeRequest::new(chunk_id.clone(), contracts, signer);
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpiceContractCodeRequest(producer, request),
        ));
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
    prev_validator_proposals: Vec<ValidatorStake>,
}
