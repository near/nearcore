use crate::metrics;
use crate::spice::chunk_executor_actor::ExecutorIncomingUnverifiedReceipts;
use crate::spice::chunk_executor_actor::get_contract_accesses;
use crate::spice::chunk_executor_actor::get_witness;
use crate::spice::chunk_validator_actor::{
    SpiceChunkStateWitnessMessage, send_spice_chunk_endorsement,
};
use crate::spice::data_manager::{
    CheckedDataRequest, DATA_PARTS_RATIO, Decoded, EncodedData, PartsOutcome, ProcessedBlockOutput,
    PullConfig, PullRequest, Push, RequestedItem, SenderFault, ServeError, ServeOutcome,
    ServePolicies, SpiceData, SpiceDataManager, SpiceDataServer, encode,
};
pub use crate::spice::data_manager::{DataId, MalformedDataRequest};
use lru::LruCache;
use near_async::MultiSend;
use near_async::MultiSenderFrom;
use near_async::futures::DelayedActionRunner;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_async::time::Clock;
use near_chain::Block;
use near_chain::spice::activation::{
    SpiceMessageGate, SpiceMessageKind, is_spice_or_last_pre_spice_block,
    spice_enabled_at_head_on_startup,
};
use near_chain::spice::all_stake_fallback::{
    fallback_eligible, fallback_endorsers, is_fallback_only_chunk,
};
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::ProcessedBlock;
use near_chain::stateless_validation::metrics::PROCESS_CONTRACT_CODE_REQUEST_TIME;
use near_chain::types::Tip;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::spice::data_distribution::SpiceChunkContractAccessesMessage;
use near_network::spice::data_distribution::SpiceContractCodeRequestMessage;
use near_network::spice::data_distribution::SpiceContractCodeResponseMessage;
use near_network::spice::data_distribution::SpiceIncomingPartialData;
use near_network::spice::data_distribution::{SpiceDataRequest, SpiceDataRequestMessage};
use near_network::types::{NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt as _;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::partial_data::SpiceDataIdentifier;
use near_primitives::spice::partial_data::SpicePartialData;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::contract_distribution::{
    BoundedContractCodes, CodeBytes, CodeHash, MAX_CONTRACTS_PER_REQUEST,
    SpiceChunkContractAccesses, SpiceContractCodeRequest, SpiceContractCodeResponse,
    has_repeated_contracts, split_contracts_for_response,
};
use near_primitives::types::AccountId;
use near_primitives::types::ShardId;
use near_primitives::types::SpiceChunkId;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::StorageError::MissingTrieValue;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::{TrieDBStorage, TrieStorage};
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Near chain error: {0}")]
    NearChainError(#[from] near_chain::Error),
    #[error("data sender is not a validator")]
    SenderIsNotValidator,
    #[error("data is irrelevant")]
    DataIsIrrelevant(SpiceDataIdentifier),
    #[error("store io error")]
    StoreIoError(std::io::Error),
    #[error("malformed data request: {0}")]
    MalformedRequest(MalformedDataRequest),
    #[error("sender fault: {0}")]
    SenderFault(#[from] SenderFault),
    #[error("other error: {0}")]
    Other(&'static str),
}

impl From<EpochError> for Error {
    fn from(value: EpochError) -> Self {
        match value {
            EpochError::NotAValidator(..) => Error::SenderIsNotValidator,
            _ => Error::NearChainError(near_chain::Error::from(value)),
        }
    }
}

/// Bundles channels for all SPICE-related messages that the network layer dispatches to.
/// Acts as a demux: handles messages it owns (partial data, etc) directly, and forwards the other
/// message types (contract-{accesses,response}) to validator via injected senders.
pub struct SpiceDataDistributorActor {
    clock: Clock,
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) core_reader: SpiceCoreReader,
    rs_encoders: ReedSolomonEncoderCache,
    validator_signer: MutableValidatorSigner,

    network_adapter: PeerManagerAdapter,
    executor_sender: Sender<ExecutorIncomingUnverifiedReceipts>,
    witness_validator_sender: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,
    /// Forwarding senders for messages that are routed through the distributor
    /// (via SpiceDataDistributorSenderForNetwork) but ultimately handled by the validator actor.
    contract_accesses_validator_sender: Sender<SpiceChunkContractAccessesMessage>,
    contract_code_response_validator_sender: Sender<SpiceContractCodeResponseMessage>,

    /// Deduplication cache for contract code requests. Keyed by (chunk, requester)
    /// to avoid redundant storage lookups and network responses for repeated requests.
    processed_contract_code_requests: LruCache<(SpiceChunkId, AccountId), ()>,

    spice_gate: SpiceMessageGate,

    /// Chunks whose witness we already pushed for the all-stake fallback. A chunk stays eligible
    /// for many blocks, so we push each once and leave a lost push to the recipients' request.
    /// Only the oldest uncertified block's chunks are ever eligible, and entries go away once the
    /// chunk certifies, so this holds at most one block's worth of chunks.
    pushed_fallback_witnesses: HashSet<SpiceChunkId>,

    /// Malformed data requests seen, by reason. Only under `test_features`, so a test can assert
    /// a request was rejected for the reason it targets rather than merely producing no output.
    /// Production observability is [`metrics::SPICE_MALFORMED_DATA_REQUESTS`].
    #[cfg(feature = "test_features")]
    malformed_data_requests: HashMap<MalformedDataRequest, u64>,

    /// Fetches the data this node needs.
    data_manager: SpiceDataManager,
    /// Pushes and serves the data this node produces.
    data_server: SpiceDataServer,
}

impl near_async::messaging::Actor for SpiceDataDistributorActor {
    fn start_actor(&mut self, _ctx: &mut dyn DelayedActionRunner<Self>) {
        if !cfg!(feature = "protocol_feature_spice") {
            return;
        }
        // The cut-off reads the spice final execution head, which only exists once spice is
        // active, so a pre-spice head walks from the last final block instead.
        if spice_enabled_at_head_on_startup(&self.chain_store) {
            self.track_from_cutoff_on_startup()
                .expect("we should be able to figure out missing data on startup");
        } else if let Err(err) = self.track_from_last_final_block_on_startup() {
            tracing::error!(target: "spice_data_distribution", ?err, "failed to track boundary data on startup");
        }
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorAdapter {
    pub receipts: Sender<SpiceDistributorOutgoingReceipts>,
    pub witness: Sender<SpiceDistributorStateWitness>,
}

#[derive(Debug)]
pub struct SpiceDistributorOutgoingReceipts {
    pub block_hash: CryptoHash,
    pub receipt_proofs: Vec<ReceiptProof>,
}

#[derive(Debug)]
pub struct SpiceDistributorStateWitness {
    pub state_witness: SpiceChunkStateWitness,
    pub contract_accesses: HashSet<CodeHash>,
}

impl Handler<SpiceDistributorOutgoingReceipts> for SpiceDataDistributorActor {
    fn handle(
        &mut self,
        SpiceDistributorOutgoingReceipts {
            block_hash,
            receipt_proofs,
        }: SpiceDistributorOutgoingReceipts,
    ) {
        for proof in receipt_proofs {
            let data_id =
                DataId::receipt_proof(block_hash, proof.1.from_shard_id, proof.1.to_shard_id);
            if let Err(err) = self.push_data(&data_id, &SpiceData::ReceiptProof(proof)) {
                tracing::error!(target: "spice_data_distribution", ?err, ?data_id, "failed to distribute receipt proof");
            }
        }
    }
}

impl Handler<SpiceDistributorStateWitness> for SpiceDataDistributorActor {
    fn handle(
        &mut self,
        SpiceDistributorStateWitness { state_witness, contract_accesses }: SpiceDistributorStateWitness,
    ) {
        let chunk_id = state_witness.chunk_id().clone();

        // Send contract accesses to chunk validators before distributing the witness.
        // Even when empty, this signals to validators that no contracts need to be fetched,
        // unblocking witness validation. Sending before the witness allows validators to
        // check their compiled contract cache and request missing contracts in parallel
        // with witness reassembly.
        if let Err(err) = self.send_contract_accesses(&chunk_id, contract_accesses) {
            tracing::error!(target: "spice_data_distribution", ?err, ?chunk_id, "failed to send contract accesses");
        }

        // TODO(spice): compress witness before distributing.
        let data_id = DataId::Witness(chunk_id);
        let data = SpiceData::StateWitness(Box::new(state_witness));
        if let Err(err) = self.push_data(&data_id, &data) {
            tracing::error!(target: "spice_data_distribution", ?err, ?data_id, "failed to distribute state witness");
        }
    }
}

impl Handler<SpiceIncomingPartialData> for SpiceDataDistributorActor {
    fn handle(
        &mut self,
        SpiceIncomingPartialData { data, recv_permit: _recv_permit }: SpiceIncomingPartialData,
    ) {
        let block_hash = *data.block_hash();
        if !self.spice_gate.should_process(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            SpiceMessageKind::PartialData,
            &block_hash,
        ) {
            return;
        }
        let sender = data.sender().clone();
        let Err(inner) = self.receive_data(data) else {
            return;
        };
        match inner {
            Error::DataIsIrrelevant(data_id) => {
                tracing::debug!(target: "spice_data_distribution", ?data_id, ?sender, "received irrelevant data");
            }
            Error::NearChainError(_) | Error::StoreIoError(_) | Error::Other(_) => {
                tracing::warn!(target: "spice_data_distribution", err = ?inner, ?block_hash, ?sender, "failed to handle receiving partial data");
            }
            // An honest sender can hit this when its view of the epoch differs from ours.
            Error::SenderIsNotValidator => {
                tracing::debug!(target: "spice_data_distribution", err = ?inner, ?block_hash, ?sender, "partial data does not match this node's epoch view");
            }
            Error::MalformedRequest(_) | Error::SenderFault(_) => {
                // TODO(spice): Implement banning or de-prioritization of nodes from which we
                // receive invalid data.
                tracing::debug!(target: "spice_data_distribution", err = ?inner, ?block_hash, ?sender, "rejected invalid partial data");
            }
        }
    }
}

impl Handler<SpiceDataRequestMessage> for SpiceDataDistributorActor {
    fn handle(&mut self, msg: SpiceDataRequestMessage) -> () {
        if let Err(err) = self.handle_data_request(msg.request) {
            self.record_malformed_data_request(&err);
            tracing::debug!(target: "spice_data_distribution", ?err, "not handling data request");
        }
    }
}

impl Handler<SpiceContractCodeRequestMessage> for SpiceDataDistributorActor {
    fn handle(
        &mut self,
        SpiceContractCodeRequestMessage(request, _recv_permit): SpiceContractCodeRequestMessage,
    ) {
        if !self.spice_gate.should_process(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            SpiceMessageKind::ContractCodeRequest,
            &request.chunk_id().block_hash,
        ) {
            return;
        }
        if let Err(err) = self.handle_spice_contract_code_request(request) {
            tracing::error!(target: "spice_data_distribution", ?err, "failure when handling contract code request");
        }
    }
}

// These messages are routed through the distributor (via SpiceDataDistributorSenderForNetwork)
// but are ultimately handled by the SpiceChunkValidatorActor. Forward them.
impl Handler<SpiceChunkContractAccessesMessage> for SpiceDataDistributorActor {
    fn handle(&mut self, msg: SpiceChunkContractAccessesMessage) {
        self.contract_accesses_validator_sender.send(msg);
    }
}

impl Handler<SpiceContractCodeResponseMessage> for SpiceDataDistributorActor {
    fn handle(&mut self, msg: SpiceContractCodeResponseMessage) {
        self.contract_code_response_validator_sender.send(msg);
    }
}

impl Handler<ProcessedBlock> for SpiceDataDistributorActor {
    fn handle(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        match is_spice_or_last_pre_spice_block(
            &self.chain_store,
            self.epoch_manager.as_ref(),
            &block_hash,
        ) {
            Ok(true) => {}
            Ok(false) => return,
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, %block_hash, "failed to get block header");
                return;
            }
        }
        if let Err(err) = self.push_fallback_witnesses(&block_hash) {
            tracing::error!(target: "spice_data_distribution", ?err, "failed pushing fallback witnesses");
        }
        if let Err(err) = self.broadcast_fallback_endorsements(&block_hash) {
            tracing::error!(target: "spice_data_distribution", ?err, "failed broadcasting fallback endorsements");
        }
        self.process_block_in_data_manager(&block_hash);
    }
}

impl SpiceDataDistributorActor {
    pub fn new(
        clock: Clock,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        chain_store: ChainStoreAdapter,
        validator_signer: MutableValidatorSigner,
        shard_tracker: ShardTracker,
        core_reader: SpiceCoreReader,
        network_adapter: PeerManagerAdapter,
        executor_sender: Sender<ExecutorIncomingUnverifiedReceipts>,
        witness_validator_sender: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,
        contract_accesses_validator_sender: Sender<SpiceChunkContractAccessesMessage>,
        contract_code_response_validator_sender: Sender<SpiceContractCodeResponseMessage>,
    ) -> Self {
        const PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE: NonZeroUsize =
            NonZeroUsize::new(30).unwrap();
        let data_manager = SpiceDataManager::new(
            PullConfig::default(),
            chain_store.clone(),
            epoch_manager.clone(),
            shard_tracker,
        );
        let data_server = SpiceDataServer::new(
            chain_store.clone(),
            epoch_manager.clone(),
            ServePolicies::new(chain_store.clone(), epoch_manager.clone()),
        );
        Self {
            clock,
            data_manager,
            data_server,
            // TODO(spice): Evaluate whether the same data parts ratio makes sense for all data
            // distributed.
            rs_encoders: ReedSolomonEncoderCache::new(DATA_PARTS_RATIO),
            epoch_manager,
            chain_store,
            core_reader,
            validator_signer,
            network_adapter,
            executor_sender,
            witness_validator_sender,
            contract_accesses_validator_sender,
            contract_code_response_validator_sender,
            processed_contract_code_requests: LruCache::new(
                PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE,
            ),
            spice_gate: SpiceMessageGate::default(),
            pushed_fallback_witnesses: HashSet::new(),
            #[cfg(feature = "test_features")]
            malformed_data_requests: HashMap::new(),
        }
    }

    /// How many spice messages of `kind` this actor dropped because spice is not active.
    #[cfg(feature = "test_features")]
    pub fn spice_dropped_count(&self, kind: SpiceMessageKind) -> u64 {
        self.spice_gate.dropped_count(kind)
    }

    /// Whether the data manager tracks an item for `id`, in any state.
    #[cfg(test)]
    pub(crate) fn is_tracking(&self, id: &DataId) -> bool {
        self.data_manager.is_tracking(id)
    }

    /// How many data requests this actor rejected for `reason`.
    #[cfg(feature = "test_features")]
    pub fn malformed_data_request_count(&self, reason: MalformedDataRequest) -> u64 {
        self.malformed_data_requests.get(&reason).copied().unwrap_or(0)
    }

    /// Tallies a rejected request, ignoring every other error. Errors that are not the sender's
    /// fault (missing block, data we do not have) must not land here.
    fn record_malformed_data_request(&mut self, err: &Error) {
        let Error::MalformedRequest(reason) = err else { return };
        metrics::SPICE_MALFORMED_DATA_REQUESTS.with_label_values(&[reason.as_str()]).inc();
        #[cfg(feature = "test_features")]
        {
            *self.malformed_data_requests.entry(*reason).or_default() += 1;
        }
    }

    fn send_pull_requests(&self, requester: &AccountId, requests: Vec<PullRequest>) {
        for PullRequest { producer, wants } in requests {
            let wants =
                wants.into_iter().map(|(id, ordinals)| (SpiceDataIdentifier::from(&id), ordinals));
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpiceDataRequest {
                    request: SpiceDataRequest::new(wants.collect(), requester.clone()),
                    producer,
                },
            ));
        }
    }

    // TODO(spice): before distributing persist data keyed by id to allow it being re-requested.
    fn push_data(&mut self, data_id: &DataId, data: &SpiceData) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            debug_assert!(false);
            return Err(Error::Other("trying to distribute data without validator_signer"));
        };
        let Push { recipients, commitment, part } =
            self.data_server.build_push(signer.validator_id(), data_id, data)?;
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpicePartialData {
                partial_data: SpicePartialData::new(
                    SpiceDataIdentifier::from(data_id),
                    commitment,
                    vec![part],
                    &signer,
                ),
                recipients,
            },
        ));
        Ok(())
    }

    // TODO(spice): Implement dynamically changing the recipients for witness if relevant chunk
    // isn't endorsed for too long.
    // TODO(spice): Cache the results since likely they would be used often.
    fn witness_recipients_and_producers(
        &self,
        block: &Block,
        shard_id: ShardId,
    ) -> Result<(HashSet<AccountId>, Vec<AccountId>), Error> {
        let epoch_id = block.header().epoch_id();
        let producers =
            self.epoch_manager.get_epoch_chunk_producers_for_shard(epoch_id, shard_id)?;
        let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            epoch_id,
            shard_id,
            block.header().height(),
        )?;
        // Since producers would produce the data anyway they shouldn't be in the recipients set.
        let recipients = validator_assignments
            .ordered_chunk_validators()
            .into_iter()
            .filter(|account| !producers.contains(account))
            .collect();
        Ok((recipients, producers))
    }

    /// Hands parts to the data manager and delivers the data it decodes.
    pub(crate) fn receive_data(&mut self, data: SpicePartialData) -> Result<(), Error> {
        let id = data.id().clone();
        match self.data_manager.on_parts_received(data) {
            Ok(PartsOutcome::Decoded(decoded)) => {
                self.deliver_decoded(decoded);
                Ok(())
            }
            Ok(PartsOutcome::Collecting | PartsOutcome::AlreadySettled) => Ok(()),
            Ok(PartsOutcome::NotWanted) => Err(Error::DataIsIrrelevant(id)),
            Err(err) => Err(err.into()),
        }
    }

    /// Routes data the manager decoded to its consumer.
    fn deliver_decoded(&self, decoded: Decoded) {
        match decoded {
            Decoded::ReceiptProof(id, receipt_proof) => {
                tracing::trace!(target: "spice_data_distribution", ?id, "delivering decoded receipt proof");
                self.executor_sender.send(ExecutorIncomingUnverifiedReceipts { id, receipt_proof });
            }
            Decoded::Witness { chunk_id, witness } => {
                tracing::trace!(target: "spice_data_distribution", ?chunk_id, "delivering decoded witness");
                self.witness_validator_sender
                    .send(SpiceChunkStateWitnessMessage { witness: *witness }.span_wrap());
            }
        }
    }

    /// As a non-designated epoch validator, broadcasts our recorded endorsement of each overdue
    /// chunk not yet on chain, so it can certify via the all-stake fallback. Re-evaluated per
    /// block.
    fn broadcast_fallback_endorsements(&self, block_hash: &CryptoHash) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Ok(());
        };
        let me = signer.validator_id();
        let block = self.chain_store.get_block(block_hash)?;
        let carrying_height = block.header().height() + 1;

        for chunk_info in self.core_reader.get_uncertified_chunks(block_hash)? {
            let chunk_id = &chunk_info.chunk_id;
            let chunk_block = self.chain_store.get_block(&chunk_id.block_hash)?;
            if !fallback_eligible(
                self.epoch_manager.as_ref(),
                chunk_block.header(),
                &chunk_info,
                carrying_height,
            )? {
                continue;
            }
            let epoch_id = chunk_block.header().epoch_id();
            if self.epoch_manager.get_validator_by_account_id(epoch_id, me).is_err() {
                continue;
            }
            let assignments = self.epoch_manager.get_chunk_validator_assignments(
                epoch_id,
                chunk_id.shard_id,
                chunk_block.header().height(),
            )?;
            if assignments.contains(me) {
                continue;
            }
            if self.core_reader.endorsement_exists(&chunk_id.block_hash, chunk_id.shard_id, me) {
                // Recorded at apply time by a tracker, or on witness validation by a non-tracker.
                // Broadcast once per block until it is on chain: a first broadcast can reach
                // producers before they see the fallback open for the chunk, and is dropped there
                // as irrelevant.
                let on_chain =
                    chunk_info.all_present_endorsements().any(|(account_id, _)| account_id == me);
                if !on_chain {
                    self.broadcast_own_fallback_endorsement(chunk_id, &signer);
                }
            }
        }
        Ok(())
    }

    /// As a chunk producer, push an overdue chunk's witness to the epoch validators that did not
    /// receive it in the initial distribution, so they can endorse it via the all-stake fallback.
    /// Every producer of the shard holds the witness and sees the same eligibility, so each sends
    /// its own part to the wider set exactly as it did in the initial distribution.
    fn push_fallback_witnesses(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Ok(());
        };
        let me = signer.validator_id();
        let block = self.chain_store.get_block(block_hash)?;
        let carrying_height = block.header().height() + 1;

        let uncertified_chunks = self.core_reader.get_uncertified_chunks(block_hash)?;
        let still_uncertified: HashSet<&SpiceChunkId> =
            uncertified_chunks.iter().map(|chunk_info| &chunk_info.chunk_id).collect();
        self.pushed_fallback_witnesses.retain(|chunk_id| still_uncertified.contains(chunk_id));

        for chunk_info in &uncertified_chunks {
            let chunk_id = &chunk_info.chunk_id;
            if self.pushed_fallback_witnesses.contains(chunk_id) {
                continue;
            }
            let chunk_block = self.chain_store.get_block(&chunk_id.block_hash)?;
            if !fallback_eligible(
                self.epoch_manager.as_ref(),
                chunk_block.header(),
                chunk_info,
                carrying_height,
            )? {
                continue;
            }
            let data_id = SpiceDataIdentifier::Witness {
                block_hash: chunk_id.block_hash,
                shard_id: chunk_id.shard_id,
            };
            // The designated recipients of the initial distribution are not needed here:
            // fallback_endorsers already excludes every designated validator.
            let (_, producers) =
                self.witness_recipients_and_producers(&chunk_block, chunk_id.shard_id)?;
            let Some(my_producer_index) = producers.iter().position(|producer| producer == me)
            else {
                continue;
            };
            let recipients: HashSet<AccountId> = fallback_endorsers(
                self.epoch_manager.as_ref(),
                chunk_block.header().epoch_id(),
                chunk_id.shard_id,
                chunk_block.header().height(),
            )?
            .into_iter()
            .filter(|account_id| !producers.contains(account_id))
            .collect();
            debug_assert!(!recipients.contains(me));
            if recipients.is_empty() {
                continue;
            }
            let Some(mut distribution_data) = self.stored_witness_data(&chunk_id, producers.len())
            else {
                if is_fallback_only_chunk(
                    self.epoch_manager.as_ref(),
                    chunk_block.header(),
                    chunk_id.shard_id,
                )? {
                    // Eligible from its own block, before we applied the chunk that produces the
                    // witness. Later blocks retry.
                    tracing::debug!(target: "spice_data_distribution", ?data_id, "witness for the fallback-only chunk not yet produced - chunk not applied");
                } else {
                    tracing::warn!(target: "spice_data_distribution", ?data_id, "no witness to push for the all-stake fallback");
                }
                continue;
            };
            let my_part = distribution_data.parts.swap_remove(my_producer_index);

            // Sent before the witness for the same reason as in the initial distribution: the
            // recipient can check its compiled contract cache while the parts arrive.
            let accesses = get_contract_accesses(
                self.chain_store.store_ref(),
                &chunk_id.block_hash,
                chunk_id.shard_id,
            )
            .expect("contract accesses should have been written atomically with witness");
            let accesses_msg = SpiceChunkContractAccesses::new(chunk_id.clone(), accesses, &signer);
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpiceChunkContractAccesses(
                    recipients.iter().cloned().collect(),
                    accesses_msg,
                ),
            ));
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpicePartialData {
                    partial_data: SpicePartialData::new(
                        data_id,
                        distribution_data.commitment,
                        vec![my_part],
                        &signer,
                    ),
                    recipients,
                },
            ));
            self.pushed_fallback_witnesses.insert(chunk_id.clone());
        }
        Ok(())
    }

    /// Rebuild the wire endorsement from our recorded result and broadcast it, so producers can
    /// include it in the all-stake fallback tally. The result was persisted when we recorded the
    /// endorsement at apply time.
    fn broadcast_own_fallback_endorsement(
        &self,
        chunk_id: &SpiceChunkId,
        signer: &ValidatorSigner,
    ) {
        let Some(stored) = self.core_reader.get_endorsement(
            &chunk_id.block_hash,
            chunk_id.shard_id,
            signer.validator_id(),
        ) else {
            // The caller just checked that it exists.
            debug_assert!(false, "no recorded endorsement to broadcast for {chunk_id:?}");
            return;
        };
        let Some(execution_result) =
            self.core_reader.get_uncertified_execution_result(&stored.execution_result_hash)
        else {
            tracing::debug!(target: "spice_data_distribution", ?chunk_id, result_hash = ?stored.execution_result_hash, "no execution result for the recorded endorsement");
            return;
        };
        let endorsement = SpiceChunkEndorsement::new(
            chunk_id.clone(),
            Arc::unwrap_or_clone(execution_result),
            signer,
        );
        send_spice_chunk_endorsement(
            endorsement,
            self.epoch_manager.as_ref(),
            &self.network_adapter.clone().into_sender(),
            signer,
        );
    }

    fn stored_witness_data(
        &mut self,
        chunk_id: &SpiceChunkId,
        producers_count: usize,
    ) -> Option<EncodedData> {
        let witness =
            get_witness(self.chain_store.store_ref(), &chunk_id.block_hash, chunk_id.shard_id)?;
        let data = SpiceData::StateWitness(Box::new(witness));
        Some(encode(&self.rs_encoders.entry(producers_count), &data))
    }

    fn handle_data_request(&mut self, request: SpiceDataRequest) -> Result<(), Error> {
        let (wants, requester) = request.into_parts();
        let request = CheckedDataRequest::new(wants).map_err(Error::MalformedRequest)?;
        for item in request.into_items() {
            if !self.spice_gate.should_process_entry(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                SpiceMessageKind::DataRequest,
                item.id().block_hash(),
            ) {
                continue;
            }
            if let Err(err) = self.serve_requested_item(&item, &requester) {
                self.record_malformed_data_request(&err);
                tracing::debug!(target: "spice_data_distribution", ?err, data_id = ?item.id(), ?requester, "not serving data request");
            }
        }
        Ok(())
    }

    fn serve_requested_item(
        &mut self,
        item: &RequestedItem,
        requester: &AccountId,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("without validator signer we cannot handle data requests"));
        };
        let id = item.id();
        let (commitment, parts) = match self
            .data_server
            .requested_parts(signer.validator_id(), item)
        {
            Ok(ServeOutcome::Parts { commitment, parts }) => (commitment, parts),
            Ok(ServeOutcome::NotStored) => {
                // TODO(spice): Make sure we send requests for data only after we know it may be
                // available and make this into error.
                tracing::debug!(target: "spice_data_distribution", ?id, ?requester, "received request for unknown data");
                return Ok(());
            }
            Err(ServeError::Requester(fault)) => return Err(Error::MalformedRequest(fault)),
            Err(ServeError::Chain(err)) => return Err(err.into()),
        };
        // TODO(spice): Check that requester is one of the recipients and implement a
        // lower-priority way for other nodes that aren't validators (e.g. rpc nodes) to get
        // data they require.
        // TODO(spice-data-distribution): sign requests, or answer unsigned ones by route-back
        // only; the requester is unauthenticated, so a peer can make this node send parts to any
        // account.
        debug_assert_eq!(parts.len(), item.ordinals().len());
        if let DataId::Witness(chunk_id) = id {
            // The requester then asks this node for missing contract code rather than the shard's
            // first producer, which it asks when only the witness's own accesses are known.
            // TODO(spice-data-distribution): drop this (once a validator spreads code requests over
            // the producers or with striped pulls).
            let accesses = get_contract_accesses(
                self.chain_store.store_ref(),
                &chunk_id.block_hash,
                chunk_id.shard_id,
            )
            .expect("contract accesses should have been written atomically with witness");
            let accesses_msg =
                SpiceChunkContractAccesses::new(chunk_id.clone(), accesses, signer.as_ref());
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpiceChunkContractAccesses(vec![requester.clone()], accesses_msg),
            ));
        }
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpicePartialData {
                partial_data: SpicePartialData::new(
                    SpiceDataIdentifier::from(id),
                    commitment,
                    parts,
                    signer.as_ref(),
                ),
                recipients: HashSet::from([requester.clone()]),
            },
        ));
        Ok(())
    }

    /// Sends contract accesses (code hashes) to chunk validators so they can check their
    /// compiled contract cache and request any missing contracts.
    fn send_contract_accesses(
        &self,
        chunk_id: &SpiceChunkId,
        contract_accesses: HashSet<CodeHash>,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("trying to send contract accesses without validator_signer"));
        };

        let block = self.chain_store.get_block(&chunk_id.block_hash)?;
        let epoch_id = block.header().epoch_id();
        let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            epoch_id,
            chunk_id.shard_id,
            block.header().height(),
        )?;
        let targets: Vec<AccountId> = validator_assignments
            .ordered_chunk_validators()
            .into_iter()
            .filter(|v| v != signer.validator_id())
            .collect();

        let accesses_msg =
            SpiceChunkContractAccesses::new(chunk_id.clone(), contract_accesses, &signer);

        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpiceChunkContractAccesses(targets, accesses_msg),
        ));
        Ok(())
    }

    /// Handles a request from a chunk validator for missing contract code.
    /// Validates that the requested contracts were actually accessed in the chunk,
    /// retrieves contract bytes from trie storage, and sends the response.
    /// Returns Ok(()) both on success and when the request is silently dropped
    /// (e.g. unknown chunk, invalid contract hash). Returns Err only on
    /// infrastructure failures (missing signer, storage errors).
    fn handle_spice_contract_code_request(
        &mut self,
        request: SpiceContractCodeRequest,
    ) -> Result<(), Error> {
        let chunk_id = request.chunk_id().clone();
        let requester = request.requester().clone();

        if request.contracts().len() > MAX_CONTRACTS_PER_REQUEST {
            tracing::debug!(
                target: "spice_data_distribution",
                ?chunk_id,
                ?requester,
                num_contracts = request.contracts().len(),
                "contract code request exceeds maximum number of contracts"
            );
            return Ok(());
        }
        if has_repeated_contracts(request.contracts()) {
            tracing::warn!(
                target: "spice_data_distribution",
                ?chunk_id,
                ?requester,
                num_contracts = request.contracts().len(),
                "contract code request repeats a hash, not serving"
            );
            return Ok(());
        }

        // Fetch block header early — needed for both validation and storage lookup below.
        let block_header = self.chain_store.get_block_header(&chunk_id.block_hash)?;
        let epoch_id = block_header.epoch_id();

        let _timer = PROCESS_CONTRACT_CODE_REQUEST_TIME
            .with_label_values(&[&metrics::shard_id_metric_label(
                self.epoch_manager.as_ref(),
                epoch_id,
                chunk_id.shard_id,
            )])
            .start_timer();

        // Verify the request signature before any check that touches the cache.
        let public_keys = self
            .epoch_manager
            .get_validator_signing_keys_for_block(&chunk_id.block_hash, &requester)?;
        if !public_keys.iter().any(|public_key| request.verify_signature(public_key)) {
            tracing::warn!(
                target: "spice_data_distribution",
                ?chunk_id,
                ?requester,
                "invalid contract code request signature"
            );
            return Ok(());
        }

        // Verify requester is a chunk validator for this chunk.
        let assignments = self.epoch_manager.get_chunk_validator_assignments(
            epoch_id,
            chunk_id.shard_id,
            block_header.height(),
        )?;
        // Designated validators may always request; a non-designated epoch validator may once the
        // chunk is past the fallback window, since it then needs the code to endorse via the
        // all-stake fallback.
        if !assignments.contains(&requester) {
            let head = self.chain_store.head()?;
            let eligible = self.core_reader.fallback_eligible_in_carrying_block(
                head.height + 1,
                &head.last_block_hash,
                &chunk_id,
            )?;
            if !eligible {
                tracing::warn!(
                    target: "spice_data_distribution",
                    ?chunk_id,
                    ?requester,
                    "contract code request from non-chunk-validator"
                );
                return Ok(());
            }
        }

        // Deduplicate repeated requests from the same requester for the same chunk.
        // TODO(spice): This mirrors the current approach in non-spice data flow. There may be
        // valid reasons to re-request the contract codes.
        let dedup_key = (chunk_id.clone(), requester.clone());
        if self.processed_contract_code_requests.contains(&dedup_key) {
            tracing::debug!(
                target: "spice_data_distribution",
                ?chunk_id,
                ?requester,
                "contract code request already processed"
            );
            return Ok(());
        }
        let Some(valid_accesses) = get_contract_accesses(
            self.chain_store.store_ref(),
            &chunk_id.block_hash,
            chunk_id.shard_id,
        ) else {
            tracing::warn!(
                target: "spice_data_distribution",
                ?chunk_id,
                ?requester,
                "received contract code request for unknown chunk"
            );
            return Ok(());
        };

        for contract_hash in request.contracts() {
            if !valid_accesses.contains(contract_hash) {
                tracing::warn!(
                    target: "spice_data_distribution",
                    ?chunk_id,
                    ?contract_hash,
                    "requested contract was not accessed in this chunk"
                );
                return Ok(());
            }
        }

        // Mark as processed only after validating the request, to prevent a
        // malicious request with invalid hashes from poisoning the dedup cache.
        self.processed_contract_code_requests.push(dedup_key, ());

        let shard_uid = shard_id_to_uid(self.epoch_manager.as_ref(), chunk_id.shard_id, epoch_id)?;
        let storage =
            TrieDBStorage::new(TrieStoreAdapter::new(self.chain_store.store()), shard_uid);

        let mut contracts = BoundedContractCodes::default();
        for contract_hash in request.contracts() {
            match storage.retrieve_raw_bytes(&contract_hash.0) {
                Ok(bytes) => {
                    // Bounds what one request can make us send; the requester gives up on a set
                    // past this anyway.
                    if !contracts.push(CodeBytes(bytes)) {
                        tracing::warn!(
                            target: "spice_data_distribution",
                            ?chunk_id,
                            ?requester,
                            total_size = contracts.total_size(),
                            "requested contract code exceeds the per-request cap, not serving"
                        );
                        return Ok(());
                    }
                }
                Err(MissingTrieValue(_)) => {
                    tracing::warn!(
                        target: "spice_data_distribution",
                        ?contract_hash,
                        ?chunk_id,
                        "requested contract hash is not present in storage"
                    );
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!(
                        target: "spice_data_distribution",
                        ?err,
                        ?contract_hash,
                        ?chunk_id,
                        "storage error retrieving contract bytes"
                    );
                    return Err(Error::Other("storage error retrieving contract bytes"));
                }
            }
        }

        // The set may not fit one response. `handle_spice_contract_code_response`
        // resolves contracts incrementally, so the groups reassemble on the requester.
        for group in split_contracts_for_response(contracts.into_codes()) {
            let response = SpiceContractCodeResponse::encode(chunk_id.clone(), &group)
                .map_err(Error::StoreIoError)?;
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpiceContractCodeResponse(requester.clone(), response),
            ));
        }
        Ok(())
    }

    /// Startup counterpart of [`Self::track_from_cutoff_on_startup`] for a pre-spice head,
    /// walking up from the last final block rather than the cut-off. A fork past the last final
    /// block may already hold a last pre-spice block, or even a spice block.
    fn track_from_last_final_block_on_startup(&mut self) -> Result<(), Error> {
        let start = self.chain_store.last_final_block_tip()?;
        self.track_descendants(&start)
    }

    fn track_from_cutoff_on_startup(&mut self) -> Result<(), Error> {
        let cutoff = self.data_manager.cutoff()?;
        self.track_descendants(&cutoff)
    }

    /// Hands every processed spice or last pre-spice block descending from `start` to the data
    /// manager, parents first.
    fn track_descendants(&mut self, start: &Tip) -> Result<(), Error> {
        for block_hash in self.chain_store.get_processed_descendants(start)? {
            match is_spice_or_last_pre_spice_block(
                &self.chain_store,
                self.epoch_manager.as_ref(),
                &block_hash,
            ) {
                Ok(true) => self.process_block_in_data_manager(&block_hash),
                Ok(false) => {}
                Err(err) => {
                    tracing::error!(target: "spice_data_distribution", ?err, %block_hash, "failed to read a block during startup recovery; skipping");
                }
            }
        }
        Ok(())
    }

    /// Hands a processed block to the data manager, sends the pulls it asks for and delivers the
    /// data it decoded.
    fn process_block_in_data_manager(&mut self, block_hash: &CryptoHash) {
        // TODO(spice): Allow requesting data without signer using route back. Until then the
        // manager records the requests below as outstanding on a node that cannot send them.
        let signer = self.validator_signer.get();
        let me = signer.as_ref().map(|signer| signer.validator_id());
        let ProcessedBlockOutput { pull_requests, decoded } =
            self.data_manager.on_block_processed(block_hash, self.clock.now(), me);
        if let Some(requester) = me {
            self.send_pull_requests(requester, pull_requests);
        }
        for data in decoded {
            self.deliver_decoded(data);
        }
    }
}
