use super::encoding::WITNESS_RATIO_DATA_PARTS;
use crate::metrics;
use crate::stateless_validation::chunk_validation_actor::ChunkValidationSenderForPartialWitness;
use itertools::Itertools;
use lru::LruCache;
use near_async::messaging::CanSend;
use near_async::time::{Clock, Duration, Instant};
use near_cache::SyncLruCache;
use near_chain::Error;
use near_chain::chain::ChunkStateWitnessMessage;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::hash::CryptoHash;
use near_primitives::reed_solomon::{
    InsertPartResult, ReedSolomonEncoder, ReedSolomonEncoderCache, ReedSolomonPartsTracker,
};
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::contract_distribution::{CodeBytes, CodeHash};
use near_primitives::stateless_validation::partial_witness::VersionedPartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessSize, EncodedChunkStateWitness,
};
use near_primitives::types::ShardId;
use near_primitives::utils::compression::CompressedData;
use near_primitives::utils::index_to_bytes;
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, Store};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use time::ext::InstantExt as _;

/// Max number of chunks to keep in the witness tracker cache. We reach here only after validation
/// of the partial_witness so the LRU cache size need not be too large.
/// This effectively limits memory usage to the size of the cache multiplied by
/// MAX_COMPRESSED_STATE_WITNESS_SIZE times the number of shards.
const WITNESS_PARTS_CACHE_SIZE: usize = 5;

/// Number of entries to keep in LRU cache of the processed state witnesses
/// We only store small amount of data (ChunkProductionKey) per entry there,
/// so we don't have to worry much about memory usage here.
const PROCESSED_WITNESSES_CACHE_SIZE: usize = 50;

/// How long to wait for requested contract codes before validating the witness without them.
/// Roughly two block times: past that the response is too late to be worth holding the witness
/// for, and proceeding best-effort beats not validating at all.
pub(super) const ACCESSED_CONTRACTS_REQUEST_TIMEOUT: Duration = Duration::seconds(2);

type DecodePartialWitnessResult = std::io::Result<EncodedChunkStateWitness>;

/// Key under which witness parts and processed witnesses are tracked.
///
/// A [`ChunkProductionKey`] on its own does not identify a single authorized producer. Once the
/// producer is resolved from the branch anchor carried by the message, two blocks at the anchor
/// height can name two different producers for the same (shard, epoch, height), and both of their
/// messages carry a valid signature.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WitnessCacheKey {
    chunk: ChunkProductionKey,
    /// Branch anchor the message was signed against, or `None` for messages that carry no anchor,
    /// where the epoch sampler already pins exactly one producer per chunk key.
    anchor: Option<CryptoHash>,
}

impl WitnessCacheKey {
    fn new(chunk: ChunkProductionKey, anchor: Option<&CryptoHash>) -> Self {
        Self { chunk, anchor: anchor.copied() }
    }
}

/// What an incoming message contributes, and which cache entries it applies to.
enum TrackerUpdate {
    /// Applies to the one entry named by the key, creating it if it does not exist yet.
    Keyed(WitnessCacheKey, CacheUpdate),
    /// Applies to every entry under `chunk` that is waiting for exactly `hashes`.
    Contracts { chunk: ChunkProductionKey, hashes: HashSet<CodeHash>, codes: Vec<CodeBytes> },
}

impl TrackerUpdate {
    fn shard_id(&self) -> ShardId {
        match self {
            Self::Keyed(key, _) => key.chunk.shard_id,
            Self::Contracts { chunk, .. } => chunk.shard_id,
        }
    }
}

enum AccessedContractsState {
    /// Haven't received `ChunkContractAccesses` message yet.
    Unknown,
    /// Received `ChunkContractAccesses` and sent `ContractCodeRequest`,
    /// waiting for response from the chunk producer.
    Requested { contract_hashes: HashSet<CodeHash>, requested_at: Instant },
    /// Received a valid `ContractCodeResponse`.
    Received(Vec<CodeBytes>),
}

impl AccessedContractsState {
    fn metrics_label(&self) -> &str {
        match &self {
            AccessedContractsState::Unknown => "unknown",
            AccessedContractsState::Requested { .. } => "requested",
            AccessedContractsState::Received(_) => "received",
        }
    }
}

enum WitnessPartsState {
    /// Haven't received any parts yet.
    Empty,
    /// Received at least one part, but not enough to decode the witness.
    WaitingParts(ReedSolomonPartsTracker<EncodedChunkStateWitness>),
    /// Received enough parts and tried decoding the witness.
    Decoded { decode_result: DecodePartialWitnessResult, decoded_at: Instant },
}

struct CacheEntry {
    clock: Clock,
    created_at: Instant,
    shard_id: ShardId,
    witness_parts: WitnessPartsState,
    accessed_contracts: AccessedContractsState,
}

enum CacheUpdate {
    WitnessPart(VersionedPartialEncodedStateWitness, Arc<ReedSolomonEncoder>),
    AccessedContractHashes(HashSet<CodeHash>),
    /// Received codes together with their hashes, computed once by the caller since they are
    /// needed both to find the entry and to check it.
    AccessedContractCodes {
        codes: Vec<CodeBytes>,
        hashes: HashSet<CodeHash>,
    },
}

impl CacheEntry {
    pub fn new(clock: Clock, shard_id: ShardId) -> Self {
        Self {
            created_at: clock.now(),
            clock,
            shard_id,
            witness_parts: WitnessPartsState::Empty,
            accessed_contracts: AccessedContractsState::Unknown,
        }
    }

    pub fn data_parts_required(&self) -> Option<usize> {
        match &self.witness_parts {
            WitnessPartsState::WaitingParts(data) => Some(data.data_parts_required()),
            WitnessPartsState::Empty | WitnessPartsState::Decoded { .. } => None,
        }
    }

    pub fn data_parts_present(&self) -> Option<usize> {
        match &self.witness_parts {
            WitnessPartsState::WaitingParts(parts) => Some(parts.data_parts_present()),
            WitnessPartsState::Empty | WitnessPartsState::Decoded { .. } => None,
        }
    }

    pub fn total_size(&self) -> usize {
        let parts_size = match &self.witness_parts {
            WitnessPartsState::Empty => 0,
            WitnessPartsState::WaitingParts(parts) => parts.total_parts_size(),
            WitnessPartsState::Decoded { decode_result, .. } => {
                decode_result.as_ref().map_or(0, |witness| witness.size_bytes())
            }
        };
        let contracts_size = match &self.accessed_contracts {
            AccessedContractsState::Unknown | AccessedContractsState::Requested { .. } => 0,
            AccessedContractsState::Received(contracts) => {
                contracts.iter().map(|code| code.0.len()).sum()
            }
        };
        parts_size + contracts_size
    }

    pub fn update(
        &mut self,
        update: CacheUpdate,
    ) -> Option<(DecodePartialWitnessResult, Vec<CodeBytes>)> {
        match update {
            CacheUpdate::WitnessPart(partial_witness, encoder) => {
                self.process_witness_part(partial_witness, encoder);
            }
            CacheUpdate::AccessedContractHashes(code_hashes) => {
                self.set_requested_contracts(code_hashes);
            }
            CacheUpdate::AccessedContractCodes { codes, hashes } => {
                self.set_received_contracts(codes, hashes);
            }
        }
        self.try_finalize()
    }

    fn process_witness_part(
        &mut self,
        partial_witness: VersionedPartialEncodedStateWitness,
        encoder: Arc<ReedSolomonEncoder>,
    ) {
        let _span = tracing::debug_span!(
            target: "client",
            "process_witness_part",
            height = partial_witness.chunk_production_key().height_created,
            shard_id = %partial_witness.chunk_production_key().shard_id,
            part_ord = partial_witness.part_ord(),
            part_size = partial_witness.part_size(),
            part_encoded_length = partial_witness.encoded_length(),
            tag_witness_distribution = true,
        )
        .entered();
        if matches!(self.witness_parts, WitnessPartsState::Empty) {
            let parts = ReedSolomonPartsTracker::new(encoder, partial_witness.encoded_length());
            self.witness_parts = WitnessPartsState::WaitingParts(parts);
        }
        let parts = match &mut self.witness_parts {
            WitnessPartsState::Empty => unreachable!(),
            WitnessPartsState::WaitingParts(parts) => parts,
            WitnessPartsState::Decoded { .. } => return,
        };
        let key = partial_witness.chunk_production_key();
        if parts.encoded_length() != partial_witness.encoded_length() {
            tracing::warn!(
                target: "client",
                ?key,
                expected = parts.encoded_length(),
                actual = partial_witness.encoded_length(),
                "partial encoded witness encoded length field does not match",
            );
            return;
        }
        let part_ord = partial_witness.part_ord();
        let part = partial_witness.into_part();
        let create_decode_span = move || {
            tracing::debug_span!(
                target: "client",
                "decode_witness_parts",
                height = key.height_created,
                shard_id = %key.shard_id,
                tag_witness_distribution = true,
            )
            .entered()
        };
        match parts.insert_part(part_ord, part, Some(Box::new(create_decode_span))) {
            InsertPartResult::Accepted => {}
            InsertPartResult::PartAlreadyAvailable => {
                tracing::warn!(
                    target: "client",
                    ?key,
                    part_ord,
                    "received duplicate or redundant state witness part"
                );
            }
            InsertPartResult::InvalidPartOrd => {
                tracing::warn!(
                    target: "client",
                    ?key,
                    part_ord,
                    "received invalid partial witness part ord"
                );
            }
            InsertPartResult::Decoded(decode_result) => {
                self.witness_parts =
                    WitnessPartsState::Decoded { decode_result, decoded_at: self.clock.now() };
                metrics::DECODE_PARTIAL_WITNESS_ACCESSED_CONTRACTS_STATE_COUNT
                    .with_label_values(&[
                        self.shard_id.to_string().as_str(),
                        self.accessed_contracts.metrics_label(),
                    ])
                    .inc();
            }
        }
    }

    fn set_requested_contracts(&mut self, contract_hashes: HashSet<CodeHash>) {
        match &self.accessed_contracts {
            AccessedContractsState::Unknown => {
                self.accessed_contracts = AccessedContractsState::Requested {
                    contract_hashes,
                    requested_at: self.clock.now(),
                };
            }
            AccessedContractsState::Requested { .. } | AccessedContractsState::Received(_) => {
                tracing::warn!(target: "client", "already received accessed contract hashes");
            }
        }
    }

    fn set_received_contracts(
        &mut self,
        contract_codes: Vec<CodeBytes>,
        actual: HashSet<CodeHash>,
    ) {
        match &self.accessed_contracts {
            AccessedContractsState::Requested { contract_hashes, requested_at } => {
                let expected = contract_hashes;
                if actual != *expected {
                    tracing::warn!(
                        target: "client",
                        ?actual,
                        ?expected,
                        "received contracts hashes do not match the requested ones"
                    );
                    return;
                }
                let now = self.clock.now();
                let shard_id_label = self.shard_id.to_string();
                metrics::RECEIVE_WITNESS_ACCESSED_CONTRACT_CODES_TIME
                    .with_label_values(&[&shard_id_label])
                    .observe(now.signed_duration_since(*requested_at).as_seconds_f64());
                if let WitnessPartsState::Decoded { decoded_at, .. } = &self.witness_parts {
                    metrics::WITNESS_ACCESSED_CONTRACT_CODES_DELAY
                        .with_label_values(&[&shard_id_label])
                        .observe(now.signed_duration_since(*decoded_at).as_seconds_f64());
                }
                self.accessed_contracts = AccessedContractsState::Received(contract_codes);
            }
            AccessedContractsState::Unknown => {
                tracing::warn!(target: "client", "received accessed contracts without sending a request");
            }
            AccessedContractsState::Received(_) => {
                tracing::warn!(target: "client", "already received accessed contract codes");
            }
        }
    }

    fn is_awaiting_contracts(&self, hashes: &HashSet<CodeHash>) -> bool {
        matches!(
            &self.accessed_contracts,
            AccessedContractsState::Requested { contract_hashes, .. } if contract_hashes == hashes
        )
    }

    fn contract_request_expired(&self) -> bool {
        matches!(&self.witness_parts, WitnessPartsState::Decoded { .. })
            && matches!(
                &self.accessed_contracts,
                AccessedContractsState::Requested { requested_at, .. }
                    if self.clock.now().signed_duration_since(*requested_at)
                        >= ACCESSED_CONTRACTS_REQUEST_TIMEOUT
            )
    }

    fn expire_contract_request(&mut self) -> Option<(DecodePartialWitnessResult, Vec<CodeBytes>)> {
        if !self.contract_request_expired() {
            return None;
        }
        self.accessed_contracts = AccessedContractsState::Unknown;
        self.try_finalize()
    }

    fn try_finalize(&mut self) -> Option<(DecodePartialWitnessResult, Vec<CodeBytes>)> {
        let parts_ready = matches!(self.witness_parts, WitnessPartsState::Decoded { .. });
        let contracts_ready = matches!(
            self.accessed_contracts,
            // We consider `Unknown` to be ready state for the following reasons:
            // - Chunk might not have any accessed contracts and in that case we
            //   do not send `ChunkContractAccesses` message.
            // - `ChunkContractAccesses` message might have been lost or delayed by
            //   the network. In this case it is better to proceed with a best-effort
            //   attempt to validate witness since in most cases we have all contracts
            //   available in the compiled contracts cache.
            AccessedContractsState::Unknown | AccessedContractsState::Received(_)
        );
        if !(parts_ready && contracts_ready) {
            return None;
        }
        let decode_result = match &mut self.witness_parts {
            WitnessPartsState::Empty | WitnessPartsState::WaitingParts(_) => unreachable!(),
            WitnessPartsState::Decoded { .. } => {
                // We want to avoid copying decoded witness, so we move it out of the state
                // and reset it to Empty.
                let WitnessPartsState::Decoded { decode_result, .. } =
                    std::mem::replace(&mut self.witness_parts, WitnessPartsState::Empty)
                else {
                    unreachable!()
                };
                decode_result
            }
        };
        let contracts: Vec<CodeBytes> = match &mut self.accessed_contracts {
            AccessedContractsState::Unknown => vec![],
            AccessedContractsState::Requested { .. } => unreachable!(),
            AccessedContractsState::Received(_) => {
                // We want to avoid copying contracts, so we move them out of the state
                // and reset it to Unknown.
                let AccessedContractsState::Received(contracts) = std::mem::replace(
                    &mut self.accessed_contracts,
                    AccessedContractsState::Unknown,
                ) else {
                    unreachable!()
                };
                contracts
            }
        };
        Some((decode_result, contracts))
    }
}

/// Per-shard state tracking for partial witness processing.
struct ShardWitnessTracker {
    /// Cache of witness parts being assembled for this shard.
    parts_cache: LruCache<WitnessCacheKey, CacheEntry>,
    /// Track processed witnesses to avoid duplicate processing for this shard.
    processed_witnesses: SyncLruCache<WitnessCacheKey, ()>,
}

impl ShardWitnessTracker {
    fn new() -> Self {
        Self {
            parts_cache: LruCache::new(NonZeroUsize::new(WITNESS_PARTS_CACHE_SIZE).unwrap()),
            processed_witnesses: SyncLruCache::new(PROCESSED_WITNESSES_CACHE_SIZE),
        }
    }

    fn total_size(&self) -> usize {
        self.parts_cache.iter().map(|(_, entry)| entry.total_size()).sum()
    }

    /// Finds every entry under `chunk` that is waiting for exactly `hashes`.
    fn find_pending_contracts(
        &self,
        chunk: &ChunkProductionKey,
        hashes: &HashSet<CodeHash>,
    ) -> Vec<WitnessCacheKey> {
        self.parts_cache
            .iter()
            .filter(|(key, entry)| &key.chunk == chunk && entry.is_awaiting_contracts(hashes))
            .map(|(key, _)| key.clone())
            .collect_vec()
    }

    /// Gives up on contract code requests that went unanswered, returning whatever that lets us
    /// finalize.
    fn expire_stale_contract_requests(&mut self) -> Vec<FinalizedWitness> {
        let stale = self
            .parts_cache
            .iter()
            .filter(|(_, entry)| entry.contract_request_expired())
            .map(|(key, _)| key.clone())
            .collect_vec();
        let mut finalized = Vec::new();
        for key in stale {
            let Some(entry) = self.parts_cache.peek_mut(&key) else { continue };
            let created_at = entry.created_at;
            let Some((decode_result, contracts)) = entry.expire_contract_request() else {
                continue;
            };
            tracing::debug!(target: "client", ?key, "gave up waiting for accessed contract codes");
            if decode_result.is_ok() {
                self.processed_witnesses.push(key.clone(), ());
            }
            self.parts_cache.pop(&key);
            finalized.push(FinalizedWitness { key, decode_result, contracts, created_at });
        }
        finalized
    }
}

/// A cache entry that has everything it is going to get and is ready to be handed on.
struct FinalizedWitness {
    key: WitnessCacheKey,
    decode_result: DecodePartialWitnessResult,
    contracts: Vec<CodeBytes>,
    created_at: Instant,
}

/// Track the Reed Solomon erasure encoded parts of the `EncodedChunkStateWitness`. These are created
/// by the chunk producer and distributed to validators. Note that we do not need all the parts of to
/// recreate the full state witness.
pub struct PartialEncodedStateWitnessTracker {
    clock: Clock,
    /// Sender to send the encoded state witness to the chunk validation actor.
    chunk_validation_sender: ChunkValidationSenderForPartialWitness,
    /// Epoch manager to get the set of chunk validators
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Used to tell whether an anchor is on our chain when choosing what to evict.
    store: Store,
    /// Per-shard tracking of witness parts and processed witnesses.
    /// Each shard is tracked independently.
    shard_trackers: Mutex<HashMap<ShardId, Arc<Mutex<ShardWitnessTracker>>>>,
    /// Reed Solomon encoder for decoding state witness parts.
    encoders: Mutex<ReedSolomonEncoderCache>,
}

impl PartialEncodedStateWitnessTracker {
    pub fn new(
        clock: Clock,
        chunk_validation_sender: ChunkValidationSenderForPartialWitness,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        store: Store,
    ) -> Self {
        Self {
            clock,
            chunk_validation_sender,
            epoch_manager,
            store,
            shard_trackers: Mutex::new(HashMap::new()),
            encoders: Mutex::new(ReedSolomonEncoderCache::new(WITNESS_RATIO_DATA_PARTS)),
        }
    }

    pub fn store_partial_encoded_state_witness(
        &self,
        partial_witness: VersionedPartialEncodedStateWitness,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_witness, "store_partial_encoded_state_witness");
        let chunk_key = partial_witness.chunk_production_key();
        let encoder = self.get_encoder(&chunk_key)?;
        let key = WitnessCacheKey::new(chunk_key, partial_witness.prev_block_hash());
        let update = CacheUpdate::WitnessPart(partial_witness, encoder);
        self.process_update(TrackerUpdate::Keyed(key, update))
    }

    pub fn store_accessed_contract_hashes(
        &self,
        key: ChunkProductionKey,
        anchor: Option<&CryptoHash>,
        hashes: HashSet<CodeHash>,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?key, ?anchor, ?hashes, "store_accessed_contract_hashes");
        let update = CacheUpdate::AccessedContractHashes(hashes);
        self.process_update(TrackerUpdate::Keyed(WitnessCacheKey::new(key, anchor), update))
    }

    pub fn store_accessed_contract_codes(
        &self,
        key: ChunkProductionKey,
        codes: Vec<CodeBytes>,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?key, codes_len = codes.len(), "store_accessed_contract_codes");
        let hashes: HashSet<CodeHash> = codes.iter().map(CodeBytes::hash).collect();
        self.process_update(TrackerUpdate::Contracts { chunk: key, hashes, codes })
    }

    fn process_update(&self, update: TrackerUpdate) -> Result<(), Error> {
        let shard_id = update.shard_id();
        let shard_tracker_mutex = self.shard_tracker(shard_id);
        let mut shard_tracker = shard_tracker_mutex.lock();

        let mut finalized = Vec::new();
        match update {
            TrackerUpdate::Keyed(key, update) => {
                if !Self::is_processed(&shard_tracker, &key) {
                    self.maybe_insert_new_entry_in_parts_cache(
                        &mut shard_tracker.parts_cache,
                        &key,
                    );
                    finalized.extend(Self::apply_to_entry(&mut shard_tracker, key, update));
                }
            }
            TrackerUpdate::Contracts { chunk, hashes, codes } => {
                let keys = shard_tracker.find_pending_contracts(&chunk, &hashes);
                if keys.is_empty() {
                    tracing::debug!(
                        target: "client",
                        ?chunk,
                        "received contract codes nothing is waiting for"
                    );
                }
                // `CodeBytes` is an `Arc<[u8]>`, so handing the codes to several entries copies
                // the vec spine and bumps refcounts rather than the contract bytes.
                for key in keys {
                    let update = CacheUpdate::AccessedContractCodes {
                        codes: codes.clone(),
                        hashes: hashes.clone(),
                    };
                    finalized.extend(Self::apply_to_entry(&mut shard_tracker, key, update));
                }
            }
        }
        finalized.extend(shard_tracker.expire_stale_contract_requests());
        let total_size = shard_tracker.total_size();
        drop(shard_tracker);

        metrics::PARTIAL_WITNESS_CACHE_SIZE
            .with_label_values(&[shard_id.to_string().as_str()])
            .set(total_size as f64);

        self.deliver_witnesses(finalized)
    }

    /// Whether the witness at `key` has already been decoded and handed on.
    fn is_processed(shard_tracker: &ShardWitnessTracker, key: &WitnessCacheKey) -> bool {
        if shard_tracker.processed_witnesses.contains(key) {
            tracing::debug!(
                target: "client",
                ?key,
                "received data for already processed witness"
            );
            return true;
        }
        false
    }

    /// Applies `update` to the entry at `key`, returning the witness if that completed it.
    fn apply_to_entry(
        shard_tracker: &mut ShardWitnessTracker,
        key: WitnessCacheKey,
        update: CacheUpdate,
    ) -> Option<FinalizedWitness> {
        if Self::is_processed(shard_tracker, &key) {
            return None;
        }
        let entry = shard_tracker.parts_cache.get_mut(&key)?;
        let created_at = entry.created_at;
        let (decode_result, contracts) = entry.update(update)?;
        if decode_result.is_ok() {
            shard_tracker.processed_witnesses.push(key.clone(), ());
        }
        shard_tracker.parts_cache.pop(&key);
        Some(FinalizedWitness { key, decode_result, contracts, created_at })
    }

    fn shard_tracker(&self, shard_id: ShardId) -> Arc<Mutex<ShardWitnessTracker>> {
        let mut map = self.shard_trackers.lock();
        Arc::clone(
            map.entry(shard_id).or_insert_with(|| Arc::new(Mutex::new(ShardWitnessTracker::new()))),
        )
    }

    /// Hands every finalized entry on, returning the first delivery error if there was one.
    ///
    /// One entry failing must not sink the others, so the remaining errors are only logged.
    fn deliver_witnesses(&self, finalized: Vec<FinalizedWitness>) -> Result<(), Error> {
        let mut first_err = None;
        for witness in finalized {
            if let Err(err) = self.deliver_witness(witness) {
                match &first_err {
                    None => first_err = Some(err),
                    Some(_) => {
                        tracing::warn!(target: "client", ?err, "failed to deliver witness")
                    }
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Decodes a fully assembled witness and hands it to the chunk validation actor.
    fn deliver_witness(&self, finalized: FinalizedWitness) -> Result<(), Error> {
        let FinalizedWitness { key, decode_result, contracts, created_at } = finalized;
        {
            // Record the time taken from receiving first part to decoding partial witness.
            let time_to_last_part = self.clock.now().signed_duration_since(created_at);
            metrics::PARTIAL_WITNESS_TIME_TO_LAST_PART
                .with_label_values(&[key.chunk.shard_id.to_string().as_str()])
                .observe(time_to_last_part.as_seconds_f64());

            let encoded_witness = match decode_result {
                Ok(encoded_chunk_state_witness) => encoded_chunk_state_witness,
                Err(err) => {
                    // We ideally never expect the decoding to fail. In case it does, we received a bad part
                    // from the chunk producer.
                    tracing::error!(
                        target: "client",
                        ?err,
                        shard_id = %key.chunk.shard_id,
                        height_created = key.chunk.height_created,
                        "failed to reed solomon decode witness parts, maybe malicious or corrupt data"
                    );
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "Failed to reed solomon decode witness parts: {err}",
                    )));
                }
            };

            let (mut witness, raw_witness_size) = {
                let _span = tracing::debug_span!(
                    target: "client",
                    "decode_state_witness",
                    height = key.chunk.height_created,
                    shard_id = %key.chunk.shard_id,
                    tag_witness_distribution = true)
                .entered();
                match self.decode_state_witness(&encoded_witness) {
                    Ok(decoded) => decoded,
                    Err(err) => {
                        tracing::error!(target: "client", ?err, ?key, "failed to decode witness");
                        return Err(err);
                    }
                }
            };
            if witness.chunk_production_key() != key.chunk {
                return Err(Error::InvalidPartialChunkStateWitness(format!(
                    "Decoded witness key {:?} doesn't match partial witness {:?}",
                    witness.chunk_production_key(),
                    key.chunk,
                )));
            }

            // Merge accessed contracts into the main transition's partial state.
            let PartialState::TrieValues(values) =
                &mut witness.mut_main_state_transition().base_state;
            values.extend(contracts.into_iter().map(|code| code.0.into()));

            tracing::debug!(target: "client", ?key, "sending encoded witness to chunk validation actor");
            let _span = tracing::debug_span!(
                target: "client",
                "send_witness_to_chunk_validation_actor",
                chunk_hash = ?witness.chunk_header().chunk_hash(),
                height = key.chunk.height_created,
                shard_id = %key.chunk.shard_id,
                raw_witness_size = raw_witness_size,
                encoded_witness_size = encoded_witness.size_bytes(),
                tag_witness_distribution = true,
            )
            .entered();
            self.chunk_validation_sender.send(ChunkStateWitnessMessage {
                witness,
                raw_witness_size,
                processing_done_tracker: None,
            });
        }

        Ok(())
    }

    fn get_encoder(&self, key: &ChunkProductionKey) -> Result<Arc<ReedSolomonEncoder>, Error> {
        // The expected number of parts for the Reed Solomon encoding is the number of chunk validators.
        let num_parts = self
            .epoch_manager
            .get_chunk_validator_assignments(&key.epoch_id, key.shard_id, key.height_created)?
            .len();
        let mut encoders = self.encoders.lock();
        Ok(encoders.entry(num_parts))
    }

    /// Whether `anchor` is the block our own chain has at that anchor's height.
    ///
    /// This only picks an eviction
    /// victim, so an answer we cannot determine (we are behind the anchor's height, or neither
    /// the block nor our head is around) counts as on-chain and the cache falls back to plain
    /// LRU.
    fn is_anchor_on_our_chain(&self, anchor: &CryptoHash) -> bool {
        let Ok(anchor_info) = self.epoch_manager.get_block_info(anchor) else {
            return true;
        };
        let anchor_height = anchor_info.height();
        match self.store.get_ser::<CryptoHash>(DBCol::BlockHeight, &index_to_bytes(anchor_height)) {
            Some(ours) => &ours == anchor,
            // No row: off our chain if we have already passed that height, undeterminable if we
            // have not got there yet or cannot read our own head.
            None => match self.store.chain_store().header_head() {
                Ok(head) => anchor_height > head.height,
                Err(_) => true,
            },
        }
    }

    // Function to insert a new entry into the cache for the chunk hash if it does not already exist
    // We additionally check if an evicted entry has been fully decoded and processed.
    fn maybe_insert_new_entry_in_parts_cache(
        &self,
        parts_cache: &mut LruCache<WitnessCacheKey, CacheEntry>,
        key: &WitnessCacheKey,
    ) {
        // Entries can legitimately share a chunk key, one per anchor, but at most one of those
        // anchors is on our chain. Drop the least recently used entry anchored elsewhere before
        // letting the LRU choose, so a flood of anchors cannot push out the one we need.
        if !parts_cache.contains(key) && parts_cache.len() == parts_cache.cap().get() {
            let off_chain = parts_cache
                .iter()
                .filter(|(k, _)| k.anchor.is_some_and(|a| !self.is_anchor_on_our_chain(&a)))
                .map(|(k, _)| k.clone())
                .last();
            if let Some(victim) = off_chain {
                tracing::debug!(
                    target: "client",
                    ?victim,
                    "evicting witness entry anchored off our chain"
                );
                parts_cache.pop(&victim);
            }
        }
        if !parts_cache.contains(key) {
            if let Some((evicted_key, evicted_entry)) = parts_cache
                .push(key.clone(), CacheEntry::new(self.clock.clone(), key.chunk.shard_id))
            {
                tracing::debug!(
                    target: "client",
                    ?evicted_key,
                    data_parts_present = ?evicted_entry.data_parts_present(),
                    data_parts_required = ?evicted_entry.data_parts_required(),
                    "evicted unprocessed partial state witness"
                );
            }
        }
    }

    fn decode_state_witness(
        &self,
        encoded_witness: &EncodedChunkStateWitness,
    ) -> Result<(ChunkStateWitness, ChunkStateWitnessSize), Error> {
        let decode_start = std::time::Instant::now();

        let (witness, raw_witness_size) = encoded_witness.decode()?;
        let decode_elapsed_seconds = decode_start.elapsed().as_secs_f64();
        let witness_shard = witness.chunk_header().shard_id();

        // Record metrics after validating the witness
        near_chain::stateless_validation::metrics::CHUNK_STATE_WITNESS_DECODE_TIME
            .with_label_values(&[&witness_shard.to_string()])
            .observe(decode_elapsed_seconds);

        Ok((witness, raw_witness_size))
    }
}
