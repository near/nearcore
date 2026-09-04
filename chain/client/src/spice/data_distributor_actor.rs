use crate::metrics;
use crate::spice::chunk_executor_actor::ExecutorIncomingUnverifiedReceipts;
use crate::spice::chunk_executor_actor::get_contract_accesses;
use crate::spice::chunk_executor_actor::get_receipt_proof;
use crate::spice::chunk_executor_actor::get_witness;
use crate::spice::chunk_validator_actor::{
    SpiceChunkStateWitnessMessage, send_spice_chunk_endorsement,
};
pub use crate::spice::data_manager::DataId;
use crate::spice::data_manager::{
    DataManagerError, Policies, ReceivedParts, SpiceData, SpiceDataManager, VerifiedCodedPart,
};
use itertools::Itertools as _;
use lru::LruCache;
use near_async::MultiSend;
use near_async::MultiSenderFrom;
use near_async::futures::DelayedActionRunner;
use near_async::futures::DelayedActionRunnerExt as _;
use near_async::messaging::CanSend;
use near_async::messaging::Handler;
use near_async::messaging::IntoSender;
use near_async::messaging::Sender;
use near_async::time::{Clock, Duration};
use near_chain::Block;
use near_chain::spice::activation::{
    SpiceMessageGate, SpiceMessageKind, spice_enabled_at_head_on_startup, spice_enabled_for_block,
};
use near_chain::spice::all_stake_fallback::{
    fallback_eligible, fallback_endorsers, is_fallback_only_chunk,
};
use near_chain::spice::core::{SpiceCoreReader, get_last_certified_block_header};
use near_chain::spice::core_writer_actor::ProcessedBlock;
use near_chain::stateless_validation::metrics::PROCESS_CONTRACT_CODE_REQUEST_TIME;
use near_chain_configs::MutableValidatorSigner;
use near_chain_primitives::ApplyChunksMode;
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
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::merklize;
use near_primitives::reed_solomon;
use near_primitives::reed_solomon::ReedSolomonEncoderCache;
use near_primitives::reed_solomon::ReedSolomonPartsTracker;
use near_primitives::sharding::ReceiptProof;
use near_primitives::spice::chunk_endorsement::SpiceChunkEndorsement;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::spice::partial_data::SpiceDataIdentifier;
use near_primitives::spice::partial_data::SpiceDataPart;
use near_primitives::spice::partial_data::SpicePartialData;
use near_primitives::spice::partial_data::SpiceVerifiedPartialData;
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::contract_distribution::{
    CodeBytes, CodeHash, MAX_CONTRACTS_PER_REQUEST, SpiceChunkContractAccesses,
    SpiceContractCodeRequest, SpiceContractCodeResponse,
};
use near_primitives::types::AccountId;
use near_primitives::types::BlockHeight;
use near_primitives::types::EpochId;
use near_primitives::types::ShardId;
use near_primitives::types::SpiceChunkId;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::validator_signer::ValidatorSigner;
use near_store::StorageError::MissingTrieValue;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::trie_store::TrieStoreAdapter;
use near_store::{TrieDBStorage, TrieStorage};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash as _, Hasher as _};
use std::num::NonZeroUsize;
use std::sync::Arc;
use strum::IntoStaticStr;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("Near chain error: {0}")]
    NearChainError(#[from] near_chain::Error),
    #[error("sender is not in the set of producers")]
    SenderIsNotProducer,
    #[error("node is not in the set of recipients")]
    NodeIsNotRecipient,
    #[error("witness id shard_id in invalid")]
    InvalidWitnessShardId,
    #[error("decoded witness shard_id in invalid")]
    InvalidDecodedWitnessShardId,
    #[error("decoded witness block hash in invalid")]
    InvalidDecodedWitnessBlockHash,
    #[error("part doesn't match commitment root")]
    InvalidCommitment,
    #[error("decoded data doesn't match commitment hash")]
    InvalidCommitmentHash,
    #[error("receipt proof id to_shard_id is invalid")]
    InvalidReceiptToShardId,
    #[error("receipt proof id from_shard_id is invalid")]
    InvalidReceiptFromShardId,
    #[error("parts is empty")]
    PartsIsEmpty,
    #[error("decoded data doesn't match id")]
    IdAndDataMismatch,
    #[error("data sender is not a validator")]
    SenderIsNotValidator,
    #[error("partial data signature is invalid")]
    InvalidPartialDataSignature,
    #[error("data is irrelevant")]
    DataIsIrrelevant(SpiceDataIdentifier),
    #[error("error decoding the data: {0}")]
    DecodeError(std::io::Error),
    #[error("store io error")]
    StoreIoError(std::io::Error),
    #[error("malformed data request: {0}")]
    MalformedRequest(MalformedDataRequest),
    #[error("data manager error: {0}")]
    DataManager(#[from] DataManagerError),
    #[error("other error: {0}")]
    Other(&'static str),
}

/// Why an inbound data request was rejected as malformed. A peer cannot produce any of these
/// without violating the request grammar, so each is attributable to the sender.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, IntoStaticStr, thiserror::Error)]
#[strum(serialize_all = "snake_case")]
pub enum MalformedDataRequest {
    #[error("no entries")]
    NoEntries,
    #[error("too many entries")]
    TooManyEntries,
    #[error("entry without ordinals")]
    EntryWithoutOrdinals,
    #[error("too many ordinals")]
    TooManyOrdinals,
    #[error("ordinal outside the producer set")]
    OrdinalOutsideProducerSet,
    #[error("shard id outside the shard layout")]
    UnknownShard,
}

impl MalformedDataRequest {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

impl From<EpochError> for Error {
    fn from(value: EpochError) -> Self {
        match value {
            EpochError::NotAValidator(..) => Error::SenderIsNotValidator,
            _ => Error::NearChainError(near_chain::Error::from(value)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiveDataError {
    #[error("failed receiving data with relevant block available")]
    ReceivingDataWithBlock(Error),
    #[error("failed receiving data with no block available")]
    ReceivingDataWithoutBlock(Error),
    #[error("Near chain error: {0}")]
    NearChainError(#[from] near_chain::Error),
}

impl ReceiveDataError {
    fn inner(&self) -> Option<&Error> {
        match self {
            ReceiveDataError::ReceivingDataWithBlock(error)
            | ReceiveDataError::ReceivingDataWithoutBlock(error) => Some(error),
            ReceiveDataError::NearChainError(_) => return None,
        }
    }
}

/// Blocks between the all-stake fallback opening for a chunk and a non-designated validator
/// starting to request its witness. The producers push the witness when the fallback opens, so the
/// request only covers a push that did not arrive.
pub(crate) const FALLBACK_WITNESS_PULL_GRACE: BlockHeight = 2;

/// A producer pushes the moment it sees the fallback open for a chunk. A receiver whose head is a
/// few blocks behind does not see it open yet, so it accepts a push for a chunk that becomes
/// fallback eligible within this many blocks. Only decides whether to buffer the parts.
pub(crate) const FALLBACK_WITNESS_PUSH_LOOKAHEAD: BlockHeight = 2;

/// How often the distributor re-requests the data it is still waiting on.
pub const DATA_REQUEST_INTERVAL: Duration = Duration::milliseconds(1000);

/// Share of the parts an item is encoded into that suffice to decode it.
pub const DATA_PARTS_RATIO: f64 = 0.6;

/// Max number of entries `(data_id, ordinals)` a single batched request may carry.
/// This caps the encodes (CPU).
pub(crate) const MAX_REQUESTED_DATA_IDS: usize = 32;

/// Max total number of parts a single batched request may carry.
/// This caps the number of parts sent back, not their size. Must stay above the largest
/// per-shard chunk-producer set.
// TODO(spice): Bound the outbound bytes per (data_id, requester) too.
pub(crate) const MAX_REQUESTED_PARTS: usize = 256;

/// Bundles channels for all SPICE-related messages that the network layer dispatches to.
/// Acts as a demux: handles messages it owns (partial data, etc) directly, and forwards the other
/// message types (contract-{accesses,response}) to validator via injected senders.
pub struct SpiceDataDistributorActor {
    chain_store: ChainStoreAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub(crate) core_reader: SpiceCoreReader,
    rs_encoders: ReedSolomonEncoderCache,
    validator_signer: MutableValidatorSigner,
    shard_tracker: ShardTracker,

    network_adapter: PeerManagerAdapter,
    executor_sender: Sender<ExecutorIncomingUnverifiedReceipts>,
    witness_validator_sender: Sender<SpanWrapped<SpiceChunkStateWitnessMessage>>,
    /// Forwarding senders for messages that are routed through the distributor
    /// (via SpiceDataDistributorSenderForNetwork) but ultimately handled by the validator actor.
    contract_accesses_validator_sender: Sender<SpiceChunkContractAccessesMessage>,
    contract_code_response_validator_sender: Sender<SpiceContractCodeResponseMessage>,

    /// Spice Partial Data which we cannot decode or validate yet because of missing corresponding block.
    /// Key is block hash, value is data with sender
    pending_partial_data: LruCache<CryptoHash, Vec<SpiceVerifiedPartialData>>,

    // TODO(spice): Populate data we are waiting on during actor start.
    waiting_on_data: HashMap<SpiceDataIdentifier, WaitingOnDataEntry>,
    // Purpose of this cache is to help make sure we don't decode the same data over and over.
    // TODO(spice): Once we remove data from waiting_on_data when it's saved (either relevant
    // endorsement or receipts are validated and saved), we should get rid of this cache and rely
    // only on store to make sure we don't wait on data we already have.
    recently_decoded_data: LruCache<SpiceDataIdentifier, ()>,

    /// Deduplication cache for contract code requests. Keyed by (chunk, requester)
    /// to avoid redundant storage lookups and network responses for repeated requests.
    processed_contract_code_requests: LruCache<(SpiceChunkId, AccountId), ()>,

    spice_gate: SpiceMessageGate,

    /// Chunks whose witness we already pushed for the all-stake fallback. A chunk stays eligible
    /// for many blocks, so we push each once and leave a lost push to the recipients' request.
    /// Only the oldest uncertified block's chunks are ever eligible, and entries go away once the
    /// chunk certifies, so this holds at most one block's worth of chunks.
    pushed_fallback_witnesses: HashSet<SpiceChunkId>,

    /// Rounds of [`Self::request_waiting_on_data`], so each retry moves to another producer.
    request_round: u64,

    /// Malformed data requests seen, by reason. Only under `test_features`, so a test can assert
    /// a request was rejected for the reason it targets rather than merely producing no output.
    /// Production observability is [`metrics::SPICE_MALFORMED_DATA_REQUESTS`].
    #[cfg(feature = "test_features")]
    malformed_data_requests: HashMap<MalformedDataRequest, u64>,

    /// Fetch engine for receipt proofs; witnesses stay on `waiting_on_data` until they
    /// switch over too.
    data_manager: SpiceDataManager,
}

struct DistributionData {
    parts: Vec<SpiceDataPart>,
    commitment: SpiceDataCommitment,
}

impl near_async::messaging::Actor for SpiceDataDistributorActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if !cfg!(feature = "protocol_feature_spice") {
            return;
        }
        // `start_waiting_on_missing_data` reads the spice final execution head,
        // which only exists once spice is active, so it is skipped while the head
        // is still pre-spice
        if spice_enabled_at_head_on_startup(&self.chain_store) {
            self.start_waiting_on_missing_data()
                .expect("we should be able to figure out missing data on startup");
        }
        self.schedule_data_fetching(ctx);
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SpiceDataDistributorAdapter {
    pub receipts: Sender<SpiceDistributorOutgoingReceipts>,
    pub witness: Sender<SpiceDistributorStateWitness>,
    pub data_verification: Sender<DataVerification>,
}

struct DataPartsEntry {
    tracker: ReedSolomonPartsTracker<SpiceData>,
}

/// Data we still miss: the parts received so far, and when we may start requesting it.
struct WaitingOnDataEntry {
    parts_by_commitment: HashMap<SpiceDataCommitment, DataPartsEntry>,
    /// Head height from which we send requests for this data. Designated recipients are allowed to
    /// request right away; fallback recipients hold back so the producers' push can arrive first.
    request_from_height: BlockHeight,
}

impl WaitingOnDataEntry {
    fn request_immediately() -> Self {
        Self { parts_by_commitment: HashMap::new(), request_from_height: 0 }
    }

    fn request_from_height(request_from_height: BlockHeight) -> Self {
        Self { parts_by_commitment: HashMap::new(), request_from_height }
    }
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

/// Consumer's verification result on data the engine delivered.
#[derive(Debug, Clone, PartialEq)]
pub enum DataVerification {
    /// Consumer verified and persisted the delivered data.
    Ok(DataId),
    /// Consumer verified the delivered data and found it invalid. Reporting it bans
    /// the decoded commitment, so it must never mean the check could not run.
    Failed(DataId),
}

impl Handler<DataVerification> for SpiceDataDistributorActor {
    fn handle(&mut self, verification: DataVerification) {
        let (data_id, result) = match verification {
            DataVerification::Ok(data_id) => {
                let result = self.data_manager.on_verified(&data_id);
                (data_id, result)
            }
            DataVerification::Failed(data_id) => {
                let result = self.data_manager.on_failed(&data_id);
                (data_id, result)
            }
        };
        if let Err(err) = result {
            // A verification result can race item expiry, so failing to apply one is not an error.
            tracing::debug!(target: "spice_data_distribution", ?err, ?data_id, "ignoring expired data verification result");
        }
    }
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
            let data_id = SpiceDataIdentifier::ReceiptProof {
                block_hash,
                from_shard_id: proof.1.from_shard_id,
                to_shard_id: proof.1.to_shard_id,
            };
            if let Err(err) = self.distribute_data(data_id.clone(), &SpiceData::ReceiptProof(proof))
            {
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

        let data_id = SpiceDataIdentifier::Witness {
            block_hash: chunk_id.block_hash,
            shard_id: chunk_id.shard_id,
        };
        // TODO(spice): compress witness before distributing.
        if let Err(err) =
            self.distribute_data(data_id.clone(), &SpiceData::StateWitness(Box::new(state_witness)))
        {
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
            SpiceMessageKind::PartialData,
            &block_hash,
        ) {
            return;
        }
        let sender = data.sender().clone();
        if let Err(err) = self.receive_data(data) {
            if let Some(Error::DataIsIrrelevant(data_id)) = err.inner() {
                self.waiting_on_data.remove(&data_id);
                tracing::debug!(target: "spice_data_distribution", ?err, ?data_id, ?sender, "received irrelevant data");
                return;
            }
            // TODO(spice): Implement banning or de-prioritization of nodes from which we receive
            // invalid data.
            tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, ?sender, "failed to handle receiving partial data");
            return;
        };
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
        // A pre-spice block distributes no receipts or witnesses and produces no
        // endorsements, so there is nothing to wait on or contribute for it.
        match spice_enabled_for_block(&self.chain_store, &block_hash) {
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
        if let Err(err) = self.contribute_fallback_endorsements(&block_hash) {
            tracing::error!(target: "spice_data_distribution", ?err, "failed contributing fallback endorsements");
        }
        if let Err(err) = self.start_waiting_on_data(&block_hash) {
            tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failure when starting waiting on data");
        }
        if let Err(err) = self.process_pending_partial_data(&block_hash) {
            tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failure when processing pending partial data");
        }
        match self.chain_store.spice_final_execution_head() {
            Ok(head) => self.data_manager.on_final_execution_head(head.height),
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, ?block_hash, "failure when reading the final execution head");
            }
        }
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
        const RECENTLY_DECODED_DATA_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(100).unwrap();
        const PENDING_PARTIAL_DATA_CAP: NonZeroUsize = NonZeroUsize::new(10).unwrap();
        const PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE: NonZeroUsize =
            NonZeroUsize::new(30).unwrap();
        let data_manager = SpiceDataManager::new(
            clock,
            DATA_PARTS_RATIO,
            Policies::new(chain_store.clone(), epoch_manager.clone(), shard_tracker.clone()),
        );
        Self {
            data_manager,
            // TODO(spice): Evaluate whether the same data parts ratio makes sense for all data
            // distributed.
            rs_encoders: ReedSolomonEncoderCache::new(DATA_PARTS_RATIO),
            epoch_manager,
            chain_store,
            core_reader,
            validator_signer,
            shard_tracker,
            network_adapter,
            executor_sender,
            witness_validator_sender,
            contract_accesses_validator_sender,
            contract_code_response_validator_sender,
            pending_partial_data: LruCache::new(PENDING_PARTIAL_DATA_CAP),
            waiting_on_data: HashMap::new(),
            recently_decoded_data: LruCache::new(RECENTLY_DECODED_DATA_CACHE_SIZE),
            processed_contract_code_requests: LruCache::new(
                PROCESSED_CONTRACT_CODE_REQUESTS_CACHE_SIZE,
            ),
            spice_gate: SpiceMessageGate::default(),
            pushed_fallback_witnesses: HashSet::new(),
            request_round: 0,
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

    // TODO(spice): before distributing persist data keyed by id to allow it being re-requested.
    fn distribute_data(
        &mut self,
        data_id: SpiceDataIdentifier,
        data: &SpiceData,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            debug_assert!(false);
            return Err(Error::Other("trying to distribute data without validator_signer"));
        };
        let me = signer.validator_id();
        let block = self.chain_store.get_block(data_id.block_hash())?;
        let (recipients, producers) = self.recipients_and_producers(&data_id, &block)?;
        debug_assert!(producers.contains(me));
        debug_assert!(!recipients.contains(me));
        let me_ord = producers.iter().position(|p| p == me).unwrap();

        let mut distribution_data = self.encode_distribution_data(data, producers.len());

        let my_part = distribution_data.parts.swap_remove(me_ord);

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
        Ok(())
    }

    fn encode_distribution_data(
        &mut self,
        data: &SpiceData,
        total_parts: usize,
    ) -> DistributionData {
        let encoder = self.rs_encoders.entry(total_parts);
        let (boxed_parts, encoded_length) = encoder.encode(data);
        debug_assert_eq!(boxed_parts.len(), total_parts);

        let parts: Vec<&[u8]> =
            boxed_parts.iter().map(|x| x.as_deref().unwrap()).collect::<Vec<_>>();
        let (merkle_root, merkle_proofs) = merklize(&parts);
        // TODO(spice): As an optimization we should be able to avoid serializing data both in
        // encode and to compute hash.
        let data_hash = hash(&borsh::to_vec(&data).unwrap());
        let commitment = SpiceDataCommitment {
            hash: data_hash,
            root: merkle_root,
            encoded_length: encoded_length as u64,
        };

        debug_assert_eq!(boxed_parts.len(), merkle_proofs.len());
        let parts = boxed_parts
            .into_iter()
            .zip(merkle_proofs)
            .enumerate()
            .map(|(part_ord, (boxed_part, merkle_proof))| SpiceDataPart {
                part_ord: part_ord as u64,
                part: boxed_part.unwrap(),
                merkle_proof,
            })
            .collect_vec();
        DistributionData { commitment, parts }
    }

    // TODO(spice): Implement dynamically changing the recipients for witness if relevant chunk
    // isn't endorsed for too long.
    // TODO(spice): Cache the results since likely they would be used often.
    fn recipients_and_producers(
        &self,
        data_id: &SpiceDataIdentifier,
        block: &Block,
    ) -> Result<(HashSet<AccountId>, Vec<AccountId>), Error> {
        let (recipients, producers) = match data_id {
            SpiceDataIdentifier::ReceiptProof { from_shard_id, to_shard_id, block_hash } => {
                debug_assert_eq!(block.hash(), block_hash);
                let epoch_id = block.header().epoch_id();
                let next_block_epoch_id =
                    self.epoch_manager.get_epoch_id_from_prev_block(block_hash)?;
                // TODO(spice-resharding): validate whether from_shard_id and to_shard_id would be
                // correct when resharding.
                let producers = self
                    .epoch_manager
                    .get_epoch_chunk_producers_for_shard(&epoch_id, *from_shard_id)?;
                let recipients = self
                    .epoch_manager
                    .get_epoch_chunk_producers_for_shard(&next_block_epoch_id, *to_shard_id)?;
                (recipients, producers)
            }
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                debug_assert_eq!(block.hash(), block_hash);
                let epoch_id = block.header().epoch_id();
                let producers =
                    self.epoch_manager.get_epoch_chunk_producers_for_shard(epoch_id, *shard_id)?;
                let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                    epoch_id,
                    *shard_id,
                    block.header().height(),
                )?;
                let recipients = validator_assignments.ordered_chunk_validators();
                (recipients, producers)
            }
        };
        // Since producers would produce the data anyway they shouldn't be in the recipients set.
        let mut recipients_set: HashSet<_> = HashSet::from_iter(recipients.into_iter());
        for account in &producers {
            recipients_set.remove(account);
        }
        Ok((recipients_set, producers))
    }

    pub(crate) fn receive_data(&mut self, data: SpicePartialData) -> Result<(), ReceiveDataError> {
        let block_hash = data.block_hash();
        let block = match self.chain_store.get_block(block_hash) {
            Ok(block) => block,
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                return self
                    .add_pending_partial_data(data)
                    .map_err(ReceiveDataError::ReceivingDataWithoutBlock);
            }
            Err(err) => return Err(err.into()),
        };
        self.receive_data_with_block(data, &block).map_err(ReceiveDataError::ReceivingDataWithBlock)
    }

    fn add_pending_partial_data(&mut self, data: SpicePartialData) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("cannot receive data without validator_signer"));
        };
        let me = signer.validator_id();

        let possible_epoch_ids = self.possible_epoch_ids(data.block_hash())?;
        let validator =
            self.get_sender_validator_from_possible_epoch_ids(&possible_epoch_ids, data.sender())?;

        let data =
            data.into_verified(validator.public_key()).ok_or(Error::InvalidPartialDataSignature)?;

        let id = &data.id;
        let sender = &data.sender;
        if !self.possible_producers(id, &possible_epoch_ids)?.contains(sender) {
            return Err(Error::SenderIsNotProducer);
        }
        if !self.is_pending_data_needed(me, id, &possible_epoch_ids)? {
            return Err(Error::NodeIsNotRecipient);
        }
        if data.parts.is_empty() {
            return Err(Error::PartsIsEmpty);
        }
        // TODO(spice): Verify that size of partial data isn't too large.
        self.pending_partial_data.get_or_insert_mut(*id.block_hash(), Vec::new).push(data);
        Ok(())
    }

    fn receive_data_with_block(
        &mut self,
        partial_data: SpicePartialData,
        block: &Block,
    ) -> Result<(), Error> {
        let sender_validator = self
            .epoch_manager
            .get_validator_by_account_id(block.header().epoch_id(), partial_data.sender())?;
        let partial_data = partial_data
            .into_verified(sender_validator.public_key())
            .ok_or(Error::InvalidPartialDataSignature)?;

        self.receive_verified_data_with_block(partial_data, block)
    }

    fn receive_verified_data_with_block(
        &mut self,
        SpiceVerifiedPartialData { id, commitment, parts, sender }: SpiceVerifiedPartialData,
        block: &Block,
    ) -> Result<(), Error> {
        self.verify_data_id(&id, block)?;
        let (_recipients, producers) = self.recipients_and_producers(&id, block)?;
        if !producers.contains(&sender) {
            return Err(Error::SenderIsNotProducer);
        }

        // Items may not be tracked yet if we received data after the block
        // became available but before we processed it.
        self.start_waiting_on_data(block.hash())?;

        match &id {
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                let data_id = DataId::receipt_proof(*block_hash, *from_shard_id, *to_shard_id);
                match self.data_manager.on_parts_received(
                    &sender,
                    &data_id,
                    &commitment,
                    parts,
                    producers.len(),
                ) {
                    Ok(ReceivedParts::Complete(SpiceData::ReceiptProof(receipt_proof))) => {
                        self.executor_sender
                            .send(ExecutorIncomingUnverifiedReceipts { data_id, receipt_proof });
                        Ok(())
                    }
                    Ok(ReceivedParts::Complete(SpiceData::StateWitness(_))) => {
                        unreachable!("decode checked the data against its receipt-proof id")
                    }
                    Ok(ReceivedParts::Collecting) => Ok(()),
                    Ok(ReceivedParts::NotWanted) => Err(Error::DataIsIrrelevant(id)),
                    Err(err) => Err(err.into()),
                }
            }
            SpiceDataIdentifier::Witness { .. } => {
                self.receive_witness_data_with_block(id, commitment, parts, block, &producers)
            }
        }
    }

    fn receive_witness_data_with_block(
        &mut self,
        id: SpiceDataIdentifier,
        commitment: SpiceDataCommitment,
        parts: Vec<SpiceDataPart>,
        block: &Block,
        producers: &[AccountId],
    ) -> Result<(), Error> {
        if !self.waiting_on_data.contains_key(&id) {
            self.start_waiting_on_pushed_fallback_witness(&id, block)?;
        }

        let Some(waiting) = self.waiting_on_data.get_mut(&id) else {
            return Err(Error::DataIsIrrelevant(id));
        };

        // TODO(spice): Check that encoded_length isn't too large.
        // TODO(spice-data-distribution): verify every part before inserting any, keep the ones that
        // verified, and report the sender for the rest. Today the first bad part aborts the loop
        // without undoing the inserts before it, so what a message contributes depends on where the
        // bad part sits; and the tracker below is allocated for an unverified commitment, sized by a
        // length the sender chose, before a single proof is checked.
        let encoded_length = commitment.encoded_length;
        let total_parts = producers.len();
        let entry = waiting.parts_by_commitment.entry(commitment.clone()).or_insert_with(|| {
            let encoder = self.rs_encoders.entry(total_parts);
            DataPartsEntry {
                tracker: ReedSolomonPartsTracker::new(encoder, encoded_length as usize),
            }
        });
        let mut decoded = false;
        for SpiceDataPart { part_ord, part, merkle_proof } in parts {
            if decoded {
                break;
            }
            // TODO(spice-data-distribution): witness ingress moves onto the engine's
            // insert_part; the unwrap below goes with the old tracker (#16275).
            let verified =
                VerifiedCodedPart::verify(&commitment, total_parts, part_ord, part, &merkle_proof)
                    .map_err(|_| Error::InvalidCommitment)?;
            // TODO(spice): Verify that size of partial data isn't too large.
            let create_decode_span = None;
            let ordinal = verified.ordinal();
            match entry.tracker.insert_part(ordinal, verified.into_part(), create_decode_span) {
                reed_solomon::InsertPartResult::Accepted => {}
                reed_solomon::InsertPartResult::PartAlreadyAvailable => {}
                reed_solomon::InsertPartResult::InvalidPartOrd => {
                    debug_assert!(
                        false,
                        "verification with merkle_proof should make sure part_ord is correct"
                    );
                    return Err(Error::Other(
                        "verification with merkle_proof passed, but part_ord is still invalid",
                    ));
                }
                reed_solomon::InsertPartResult::Decoded(Ok(data)) => {
                    decoded = true;
                    let data_hash = hash(&borsh::to_vec(&data).unwrap());
                    if data_hash != commitment.hash {
                        return Err(Error::InvalidCommitmentHash);
                    }
                    let SpiceData::StateWitness(witness) = data else {
                        return Err(Error::IdAndDataMismatch);
                    };
                    let SpiceDataIdentifier::Witness { block_hash, shard_id } = &id else {
                        unreachable!("only witness ids take this path");
                    };
                    let chunk_id = witness.chunk_id();
                    if &chunk_id.shard_id != shard_id {
                        return Err(Error::InvalidDecodedWitnessShardId);
                    }
                    if &chunk_id.block_hash != block_hash {
                        return Err(Error::InvalidDecodedWitnessBlockHash);
                    }
                    self.witness_validator_sender.send(
                        SpiceChunkStateWitnessMessage {
                            witness: *witness,
                            raw_witness_size: encoded_length as usize,
                        }
                        .span_wrap(),
                    );
                }
                reed_solomon::InsertPartResult::Decoded(Err(err)) => {
                    return Err(Error::DecodeError(err));
                }
            }
        }
        if decoded {
            // TODO(spice): Handle the possibility of receiving invalid data in
            // which case we would need to keep requesting it.
            tracing::debug!(target: "spice_data_distribution", ?id, ?commitment, "decoded data; stop waiting");
            self.waiting_on_data.remove(&id);
            self.recently_decoded_data.push(id, ());
        }
        Ok(())
    }

    /// Whether we already hold the artifact the witness exists to produce: our endorsement.
    // TODO(spice-data-distribution): responsibility creep — remove when witnesses move onto the engine.
    fn is_witness_known(&self, me: &AccountId, block_hash: &CryptoHash, shard_id: ShardId) -> bool {
        self.core_reader.endorsement_exists(block_hash, shard_id, me)
    }

    fn verify_data_id(&self, id: &SpiceDataIdentifier, block: &Block) -> Result<(), Error> {
        match id {
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                debug_assert_eq!(block_hash, block.hash());
                let shard_layout =
                    self.epoch_manager.get_shard_layout(block.header().epoch_id())?;
                let shard_ids: HashSet<_> = shard_layout.shard_ids().collect();
                if !shard_ids.contains(from_shard_id) {
                    return Err(Error::InvalidReceiptFromShardId);
                }
                // TODO(spice-resharding): If to_shard_id may be from the next_epoch this check
                // needs to be adjusted.
                if !shard_ids.contains(to_shard_id) {
                    return Err(Error::InvalidReceiptToShardId);
                }
            }
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                debug_assert_eq!(block_hash, block.hash());
                let shard_layout =
                    self.epoch_manager.get_shard_layout(block.header().epoch_id())?;
                let shard_ids: HashSet<_> = shard_layout.shard_ids().collect();
                if !shard_ids.contains(shard_id) {
                    return Err(Error::InvalidWitnessShardId);
                }
            }
        }
        Ok(())
    }

    fn get_sender_validator_from_possible_epoch_ids(
        &self,
        possible_epoch_ids: &[EpochId],
        sender: &AccountId,
    ) -> Result<ValidatorStake, Error> {
        for epoch_id in possible_epoch_ids {
            if let Ok(validator) = self.epoch_manager.get_validator_by_account_id(&epoch_id, sender)
            {
                return Ok(validator);
            }
        }
        Err(Error::SenderIsNotValidator)
    }

    fn possible_epoch_ids(&self, block_hash: &CryptoHash) -> Result<Vec<EpochId>, Error> {
        let possible_epoch_ids = if self.chain_store.block_exists(block_hash) {
            let epoch_id = self.epoch_manager.get_epoch_id(block_hash)?;
            vec![epoch_id]
        } else {
            let final_head = self.chain_store.final_head()?;
            // Since block doesn't exist it has to be after the final head.
            // Here we assume we aren't catching up.
            // TODO(spice): consider if this needs to be adjusted when implementing various syncs.
            vec![final_head.epoch_id, final_head.next_epoch_id]
        };
        Ok(possible_epoch_ids)
    }

    fn possible_producers(
        &self,
        id: &SpiceDataIdentifier,
        possible_epoch_ids: &[EpochId],
    ) -> Result<HashSet<AccountId>, Error> {
        let mut possible_producers = HashSet::new();
        for epoch_id in possible_epoch_ids {
            match id {
                SpiceDataIdentifier::Witness { shard_id, .. } => {
                    possible_producers.extend(
                        self.epoch_manager
                            .get_epoch_chunk_producers_for_shard(&epoch_id, *shard_id)?
                            .into_iter(),
                    );
                }
                SpiceDataIdentifier::ReceiptProof { from_shard_id, .. } => {
                    possible_producers.extend(
                        self.epoch_manager
                            .get_epoch_chunk_producers_for_shard(&epoch_id, *from_shard_id)?
                            .into_iter(),
                    );
                }
            }
        }
        Ok(possible_producers)
    }

    fn is_pending_data_needed(
        &self,
        me: &AccountId,
        id: &SpiceDataIdentifier,
        possible_epoch_ids: &[EpochId],
    ) -> Result<bool, Error> {
        for epoch_id in possible_epoch_ids {
            match id {
                SpiceDataIdentifier::Witness { .. } => {
                    let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
                    if epoch_info
                        .validators_iter()
                        .map(|stake| stake.take_account_id())
                        .contains(me)
                    {
                        return Ok(true);
                    }
                }
                SpiceDataIdentifier::ReceiptProof { to_shard_id, .. } => {
                    // TODO(spice): Use information in shard_tracker and epoch manager to assess if we
                    // need this data.
                    let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
                    if shard_layout.shard_ids().contains(to_shard_id) {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    fn process_pending_partial_data(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let ready_data = self.pending_partial_data.pop(&block_hash).unwrap_or_default();
        if ready_data.is_empty() {
            return Ok(());
        }
        let block = self.chain_store.get_block(&block_hash)?;
        for data in ready_data {
            let data_id = data.id.clone();
            let commitment = data.commitment.clone();
            if let Err(err) = self.receive_verified_data_with_block(data, &block) {
                if let Error::DataIsIrrelevant(_) = err {
                    self.waiting_on_data.remove(&data_id);
                    tracing::debug!(target: "spice_data_distribution", ?err, ?data_id, ?commitment, "processing irrelevant data");
                } else {
                    tracing::error!(target: "spice_data_distribution", ?err, ?data_id, ?commitment, "failed to process partial data");
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_partial_data_size(&self) -> usize {
        self.pending_partial_data.len()
    }

    #[cfg(any(test, feature = "test_features"))]
    pub fn waiting_on_data_ids(&self) -> Vec<SpiceDataIdentifier> {
        self.waiting_on_data.keys().cloned().collect()
    }

    /// Data of a block on a dead fork (below the final head, off the canonical chain) is never
    /// applied. A witness of a chunk certified as of the final head is never endorsed, and the
    /// producers collect it (see `clear_witnesses_data`). Neither may ever arrive.
    fn stop_waiting_on_data_for_dead_forks_and_final_certified_blocks(&mut self) {
        let Ok(final_head) = self.chain_store.final_head() else {
            return;
        };
        let last_certified_height = match get_last_certified_block_header(
            &self.chain_store,
            &final_head.last_block_hash,
        ) {
            Ok(header) => header.height(),
            Err(err) => {
                tracing::debug!(target: "spice_data_distribution", ?err, "no last certified block to stop waiting on witnesses at");
                return;
            }
        };
        let mut unneeded = Vec::new();
        for id in self.waiting_on_data.keys() {
            let block_hash = id.block_hash();
            let height = match self.chain_store.get_block_height(block_hash) {
                Ok(height) => height,
                Err(err) => {
                    // The rules below should have dropped the entry before its block was
                    // collected.
                    tracing::error!(target: "spice_data_distribution", ?err, ?id, "block for which we wait on data is gone; stop waiting on it");
                    unneeded.push((id.clone(), false));
                    continue;
                }
            };
            if height > final_head.height {
                continue;
            }
            let on_dead_fork =
                self.chain_store.get_block_hash_by_height(height).ok().as_ref() != Some(block_hash);
            let certified = matches!(id, SpiceDataIdentifier::Witness { .. })
                && height <= last_certified_height;
            if on_dead_fork || certified {
                unneeded.push((id.clone(), on_dead_fork));
            }
        }
        for (id, on_dead_fork) in unneeded {
            tracing::debug!(target: "spice_data_distribution", ?id, on_dead_fork, last_certified_height, "data is no longer needed; stop waiting on it");
            self.waiting_on_data.remove(&id);
        }
    }

    // TODO(spice): Implement a state machine to track all the data we produce or may need. This
    // would help make sure that we cannot have and request data at the same time.
    /// As a non-designated epoch validator, certify overdue chunks via the all-stake fallback:
    /// broadcast our recorded endorsement, or pull the witness to produce one. Re-evaluated per block.
    fn contribute_fallback_endorsements(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
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
                continue;
            }
            let tracks_shard = self.shard_tracker.should_apply_chunk(
                ApplyChunksMode::IsCaughtUp,
                chunk_block.header().prev_hash(),
                chunk_id.shard_id,
            );
            // A tracker that hasn't applied the chunk yet has no result to endorse; it records and
            // broadcasts after it applies. A non-tracker pulls the witness so it can produce one.
            if !tracks_shard {
                self.start_waiting_on_fallback_witness(
                    chunk_id,
                    &chunk_block,
                    me,
                    block.header().height() + FALLBACK_WITNESS_PULL_GRACE,
                )?;
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
            let (_, producers) = self.recipients_and_producers(&data_id, &chunk_block)?;
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
            let Some(mut distribution_data) = self.get_distribution_data(&data_id, producers.len())
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

    /// Pull a chunk's witness (not received by non-designated validators in the initial
    /// distribution) so we can apply the chunk and endorse it for the fallback.
    fn start_waiting_on_fallback_witness(
        &mut self,
        chunk_id: &SpiceChunkId,
        chunk_block: &Block,
        me: &AccountId,
        request_from_height: BlockHeight,
    ) -> Result<(), Error> {
        let id = SpiceDataIdentifier::Witness {
            block_hash: chunk_id.block_hash,
            shard_id: chunk_id.shard_id,
        };
        let (_recipients, producers) = self.recipients_and_producers(&id, chunk_block)?;
        if producers.contains(me)
            || self.waiting_on_data.contains_key(&id)
            || self.recently_decoded_data.contains(&id)
            || self.is_witness_known(me, &chunk_id.block_hash, chunk_id.shard_id)
        {
            return Ok(());
        }
        self.waiting_on_data
            .insert(id, WaitingOnDataEntry::request_from_height(request_from_height));
        Ok(())
    }

    /// A producer pushes an overdue chunk's witness as soon as the fallback opens, which can be
    /// before we processed the block that opened it. Start waiting on the witness now, so the
    /// pushed parts have somewhere to go, if we are a validator the fallback expects to endorse it.
    fn start_waiting_on_pushed_fallback_witness(
        &mut self,
        id: &SpiceDataIdentifier,
        chunk_block: &Block,
    ) -> Result<(), Error> {
        let SpiceDataIdentifier::Witness { block_hash, shard_id } = id else {
            return Ok(());
        };
        let Some(signer) = self.validator_signer.get() else {
            return Ok(());
        };
        let me = signer.validator_id();
        let epoch_id = chunk_block.header().epoch_id();
        if self.epoch_manager.get_validator_by_account_id(epoch_id, me).is_err() {
            return Ok(());
        }
        let assignments = self.epoch_manager.get_chunk_validator_assignments(
            epoch_id,
            *shard_id,
            chunk_block.header().height(),
        )?;
        if assignments.contains(me) {
            return Ok(());
        }
        if self.shard_tracker.should_apply_chunk(
            ApplyChunksMode::IsCaughtUp,
            chunk_block.header().prev_hash(),
            *shard_id,
        ) {
            return Ok(());
        }
        let chunk_id = SpiceChunkId { block_hash: *block_hash, shard_id: *shard_id };
        let head = self.chain_store.head()?;
        if !self.core_reader.fallback_eligible_in_carrying_block(
            head.height + 1 + FALLBACK_WITNESS_PUSH_LOOKAHEAD,
            &head.last_block_hash,
            &chunk_id,
        )? {
            return Ok(());
        }
        self.start_waiting_on_fallback_witness(
            &chunk_id,
            chunk_block,
            me,
            head.height + FALLBACK_WITNESS_PULL_GRACE,
        )
    }

    fn start_waiting_on_data(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let signer = self.validator_signer.get();
        let me = signer.as_ref().map(|signer| signer.validator_id());
        // TODO(spice): Allow requesting data without signer using route back.
        let Some(me) = me else {
            tracing::debug!(target: "spice_data_distribution", "not starting data waiting since we have no signer");
            return Ok(());
        };

        let block = self.chain_store.get_block(block_hash)?;
        let shard_layout = self.epoch_manager.get_shard_layout(&block.header().epoch_id())?;

        let shards_we_apply: HashSet<ShardId> = shard_layout
            .shard_ids()
            .filter(|shard_id| {
                let prev_hash = block.header().prev_hash();
                self.shard_tracker.should_apply_chunk(
                    ApplyChunksMode::IsCaughtUp,
                    prev_hash,
                    *shard_id,
                )
            })
            .collect();

        for shard_id in shard_layout.shard_ids() {
            // If we will apply chunk we will also produce endorsement so no need to request
            // witness from elsewhere.
            if shards_we_apply.contains(&shard_id) {
                continue;
            }

            let validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
                block.header().epoch_id(),
                shard_id,
                block.header().height(),
            )?;
            if !validator_assignments.contains(me) {
                continue;
            }

            let id = SpiceDataIdentifier::Witness { block_hash: *block_hash, shard_id };
            let (_recipients, producers) = self.recipients_and_producers(&id, &block)?;
            assert!(!producers.contains(me));

            if self.waiting_on_data.contains_key(&id) {
                continue;
            }
            if self.recently_decoded_data.contains(&id) {
                continue;
            }
            if self.is_witness_known(me, block_hash, shard_id) {
                tracing::debug!(target: "spice_data_distribution", ?id, "data is known; will not start waiting on it");
                continue;
            }
            self.waiting_on_data.insert(id, WaitingOnDataEntry::request_immediately());
        }

        self.data_manager.on_block(block.header())?;
        Ok(())
    }

    fn schedule_data_fetching(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.stop_waiting_on_data_for_dead_forks_and_final_certified_blocks();
        self.request_waiting_on_data();
        self.request_round = self.request_round.wrapping_add(1);

        ctx.run_later(
            "SpiceDataDistributorActor request waiting on data",
            // TODO(spice): Make duration configurable.
            DATA_REQUEST_INTERVAL,
            move |act, ctx| {
                act.schedule_data_fetching(ctx);
            },
        );
    }

    fn request_waiting_on_data(&self) {
        // TODO(spice): Allow requesting data without signer using route back.
        let Some(signer) = self.validator_signer.get() else {
            tracing::debug!(target: "spice_data_distribution", "no validator signer to request waiting on data");
            return;
        };
        let me = signer.validator_id();
        let head_height = match self.chain_store.head() {
            Ok(head) => head.height,
            Err(err) => {
                tracing::error!(target: "spice_data_distribution", ?err, "no head to request data at");
                return;
            }
        };
        // TODO(spice): Stop waiting on witnesses past final certification head.

        for (id, waiting) in &self.waiting_on_data {
            if head_height < waiting.request_from_height {
                continue;
            }
            let block = self
                .chain_store
                .get_block(id.block_hash())
                .expect("block for which we wait on data should always be available");
            let (_recipients, mut producers) = self.recipients_and_producers(&id, &block).expect(
                "producers and recipients that we wait on data for should always be available",
            );
            assert!(!producers.contains(me));
            assert!(!producers.is_empty());

            // TODO(spice): Implement requesting only the parts we are still missing from random
            // producers.
            // TODO(spice): Request data only we know may be available. (For example based on
            // execution and certification heads.)
            let total_parts = producers.len();
            let producer_index =
                producer_index_to_request_from(total_parts, id, me, self.request_round);
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpiceDataRequest {
                    // TODO(spice): Batch the ids that resolve to the same producer.
                    request: SpiceDataRequest::new(
                        BTreeMap::from([(id.clone(), (0..total_parts as u64).collect())]),
                        me.clone(),
                    ),
                    producer: producers.swap_remove(producer_index),
                },
            ));
        }
    }

    fn get_distribution_data(
        &mut self,
        data_id: &SpiceDataIdentifier,
        producers_count: usize,
    ) -> Option<DistributionData> {
        let data = match data_id {
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                get_receipt_proof(
                    self.chain_store.store_ref(),
                    block_hash,
                    *to_shard_id,
                    *from_shard_id,
                )
                .map(SpiceData::ReceiptProof)
            }
            SpiceDataIdentifier::Witness { block_hash, shard_id } => {
                get_witness(self.chain_store.store_ref(), block_hash, *shard_id)
                    .map(Box::new)
                    .map(SpiceData::StateWitness)
            }
        };

        data.map(|data| self.encode_distribution_data(&data, producers_count))
    }

    fn handle_data_request(&mut self, request: SpiceDataRequest) -> Result<(), Error> {
        let (wants, requester) = request.into_parts();
        validate_wants(&wants)?;
        for (data_id, ordinals) in wants {
            if !self.spice_gate.should_process_entry(
                &self.chain_store,
                SpiceMessageKind::DataRequest,
                data_id.block_hash(),
            ) {
                continue;
            }
            if let Err(err) = self.serve_data_request(&data_id, &ordinals, &requester) {
                self.record_malformed_data_request(&err);
                tracing::debug!(target: "spice_data_distribution", ?err, ?data_id, ?requester, "not serving data request");
            }
        }
        Ok(())
    }

    /// Rejects shard ids outside the layout they are read against. Without this they would fail
    /// deeper in the epoch manager, where the error is indistinguishable from data we do not have.
    fn validate_requested_shards(
        &self,
        data_id: &SpiceDataIdentifier,
        block: &Block,
    ) -> Result<(), Error> {
        let epoch_id = block.header().epoch_id();
        let in_layout = |epoch_id: &EpochId, shard_id: ShardId| -> Result<bool, Error> {
            Ok(self.epoch_manager.get_shard_layout(epoch_id)?.shard_ids().contains(&shard_id))
        };
        let known = match data_id {
            SpiceDataIdentifier::Witness { shard_id, .. } => in_layout(epoch_id, *shard_id)?,
            SpiceDataIdentifier::ReceiptProof { block_hash, from_shard_id, to_shard_id } => {
                let next_block_epoch_id =
                    self.epoch_manager.get_epoch_id_from_prev_block(block_hash)?;
                in_layout(epoch_id, *from_shard_id)?
                    && in_layout(&next_block_epoch_id, *to_shard_id)?
            }
        };
        if !known {
            return Err(Error::MalformedRequest(MalformedDataRequest::UnknownShard));
        }
        Ok(())
    }

    fn serve_data_request(
        &mut self,
        data_id: &SpiceDataIdentifier,
        ordinals: &BTreeSet<u64>,
        requester: &AccountId,
    ) -> Result<(), Error> {
        let Some(signer) = self.validator_signer.get() else {
            return Err(Error::Other("without validator signer we cannot handle data requests"));
        };

        let block = self.chain_store.get_block(data_id.block_hash())?;
        self.validate_requested_shards(data_id, &block)?;
        let (_recipients, producers) = self.recipients_and_producers(data_id, &block)?;
        if !producers.contains(signer.validator_id()) {
            return Err(Error::Other("we do not produce requested data"));
        }
        let total_parts = producers.len();
        if ordinals.last().is_some_and(|highest| *highest >= total_parts as u64) {
            return Err(Error::MalformedRequest(MalformedDataRequest::OrdinalOutsideProducerSet));
        }

        let Some(data) = self.get_distribution_data(data_id, total_parts) else {
            // TODO(spice): Make sure we send requests for data only after we know it may be
            // available and make this into error.
            tracing::debug!(target:"spice_data_distribution", ?data_id, ?requester, "received request for unknown data");
            return Ok(());
        };
        // TODO(spice): Check that requester is one of the recipients and implement a
        // lower-priority way for other nodes that aren't validators (e.g. rpc nodes) to get
        // data they require.

        // For witness requests, also send contract accesses so that the requester
        // (e.g. a chunk validator catching up after restart) can request them if not available in their local cache.
        if let SpiceDataIdentifier::Witness { block_hash, shard_id } = data_id {
            let chunk_id = SpiceChunkId { block_hash: *block_hash, shard_id: *shard_id };
            let accesses =
                get_contract_accesses(self.chain_store.store_ref(), block_hash, *shard_id)
                    .expect("contract accesses should have been written atomically with witness");
            let accesses_msg = SpiceChunkContractAccesses::new(chunk_id, accesses, &signer);
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::SpiceChunkContractAccesses(vec![requester.clone()], accesses_msg),
            ));
        }

        let parts: Vec<_> =
            data.parts.into_iter().filter(|part| ordinals.contains(&part.part_ord)).collect();
        debug_assert_eq!(parts.len(), ordinals.len());

        let recipients = HashSet::from([requester.clone()]);
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpicePartialData {
                partial_data: SpicePartialData::new(
                    data_id.clone(),
                    data.commitment,
                    parts,
                    &signer,
                ),
                recipients,
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
        let _timer = PROCESS_CONTRACT_CODE_REQUEST_TIME
            .with_label_values(&[&chunk_id.shard_id.to_string()])
            .start_timer();
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

        // Fetch block header early — needed for both validation and storage lookup below.
        let block_header = self.chain_store.get_block_header(&chunk_id.block_hash)?;
        let epoch_id = block_header.epoch_id();

        // Verify request signature before any other checks to prevent cache pollution.
        let validator = self.epoch_manager.get_validator_by_account_id(epoch_id, &requester)?;
        if !request.verify_signature(validator.public_key()) {
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

        let mut contracts = Vec::new();
        for contract_hash in request.contracts() {
            match storage.retrieve_raw_bytes(&contract_hash.0) {
                Ok(bytes) => contracts.push(CodeBytes(bytes)),
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

        let response =
            SpiceContractCodeResponse::encode(chunk_id, &contracts).map_err(Error::StoreIoError)?;
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::SpiceContractCodeResponse(requester, response),
        ));
        Ok(())
    }

    fn start_waiting_on_missing_data(&mut self) -> Result<(), Error> {
        let start_block = self.chain_store.spice_final_execution_head()?.last_block_hash;

        let mut next_block_hashes: VecDeque<_> =
            self.chain_store.get_all_next_block_hashes(&start_block).into();
        while let Some(block_hash) = next_block_hashes.pop_front() {
            self.start_waiting_on_data(&block_hash)?;
            next_block_hashes.extend(&self.chain_store.get_all_next_block_hashes(&block_hash));
        }
        Ok(())
    }
}

/// Checks a request against the caps before any of it is served, so a request that asks for too
/// much costs nothing beyond this pass. Ordinals are checked against the producer count when
/// serving the entry, where that count is known.
fn validate_wants(wants: &BTreeMap<SpiceDataIdentifier, BTreeSet<u64>>) -> Result<(), Error> {
    if wants.is_empty() {
        return Err(Error::MalformedRequest(MalformedDataRequest::NoEntries));
    }
    if wants.len() > MAX_REQUESTED_DATA_IDS {
        return Err(Error::MalformedRequest(MalformedDataRequest::TooManyEntries));
    }
    if wants.values().any(|ordinals| ordinals.is_empty()) {
        return Err(Error::MalformedRequest(MalformedDataRequest::EntryWithoutOrdinals));
    }
    if wants.values().map(|ordinals| ordinals.len()).sum::<usize>() > MAX_REQUESTED_PARTS {
        return Err(Error::MalformedRequest(MalformedDataRequest::TooManyOrdinals));
    }
    Ok(())
}

/// The starting producer index is derived from a hash of (data_id, requester), so requests for the
/// same data are spread across producers instead of all landing on one. Adding `round` advances the
/// index each tick, so retries move along rather than repeatedly targeting an unresponsive producer.
fn producer_index_to_request_from(
    num_producers: usize,
    data_id: &SpiceDataIdentifier,
    requester: &AccountId,
    round: u64,
) -> usize {
    let mut hasher = DefaultHasher::new();
    data_id.hash(&mut hasher);
    requester.hash(&mut hasher);
    (hasher.finish().wrapping_add(round) % num_producers as u64) as usize
}
