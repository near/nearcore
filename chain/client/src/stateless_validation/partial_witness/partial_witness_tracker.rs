use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use near_async::messaging::CanSend;
use near_async::time::Instant;
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
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessSize, EncodedChunkStateWitness,
};
use near_primitives::types::ShardId;
use near_primitives::version::ProtocolFeature;
use near_vm_runner::logic::ProtocolVersion;
use parking_lot::Mutex;
use time::ext::InstantExt as _;

use crate::client_actor::ClientSenderForPartialWitness;
use crate::metrics;

use near_primitives::utils::compression::CompressedData;

use super::encoding::WITNESS_RATIO_DATA_PARTS;

/// Max number of chunks to keep in the witness tracker cache. We reach here only after validation
/// of the partial_witness so the LRU cache size need not be too large.
/// This effectively limits memory usage to the size of the cache multiplied by
/// MAX_COMPRESSED_STATE_WITNESS_SIZE, currently 40 * 48MiB = 1920MiB.
const WITNESS_PARTS_CACHE_SIZE: usize = 40;

/// Number of entries to keep in LRU cache of the processed state witnesses
/// We only store small amount of data (ChunkProductionKey) per entry there,
/// so we don't have to worry much about memory usage here.
const PROCESSED_WITNESSES_CACHE_SIZE: usize = 200;

type DecodePartialWitnessResult = std::io::Result<EncodedChunkStateWitness>;

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
    created_at: Instant,
    shard_id: ShardId,
    witness_parts: WitnessPartsState,
    accessed_contracts: AccessedContractsState,
}

enum CacheUpdate {
    WitnessPart(PartialEncodedStateWitness, Arc<ReedSolomonEncoder>),
    AccessedContractHashes(HashSet<CodeHash>),
    AccessedContractCodes(Vec<CodeBytes>),
}

impl CacheEntry {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            created_at: Instant::now(),
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
            CacheUpdate::AccessedContractCodes(contract_codes) => {
                self.set_received_contracts(contract_codes);
            }
        }
        self.try_finalize()
    }

    fn process_witness_part(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
        encoder: Arc<ReedSolomonEncoder>,
    ) {
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
                "Partial encoded witness encoded length field does not match",
            );
            return;
        }
        let part_ord = partial_witness.part_ord();
        let part = partial_witness.into_part();
        match parts.insert_part(part_ord, part) {
            InsertPartResult::Accepted => {}
            InsertPartResult::PartAlreadyAvailable => {
                tracing::warn!(
                    target: "client",
                    ?key,
                    part_ord,
                    "Received duplicate or redundant state witness part"
                );
            }
            InsertPartResult::InvalidPartOrd => {
                tracing::warn!(
                    target: "client",
                    ?key,
                    part_ord,
                    "Received invalid partial witness part ord"
                );
            }
            InsertPartResult::Decoded(decode_result) => {
                self.witness_parts =
                    WitnessPartsState::Decoded { decode_result, decoded_at: Instant::now() };
                metrics::DECODE_PARTIAL_WITNESS_ACCESSED_CONTRACTS_STATE_COUNT
                    .with_label_values(&[
                        &self.shard_id.to_string(),
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
                    requested_at: Instant::now(),
                };
            }
            AccessedContractsState::Requested { .. } | AccessedContractsState::Received(_) => {
                tracing::warn!(target: "client", "Already received accessed contract hashes");
            }
        }
    }

    fn set_received_contracts(&mut self, contract_codes: Vec<CodeBytes>) {
        match &self.accessed_contracts {
            AccessedContractsState::Requested { contract_hashes, requested_at } => {
                let actual = HashSet::from_iter(
                    contract_codes.iter().map(|code| CodeHash(CryptoHash::hash_bytes(&code.0))),
                );
                let expected = contract_hashes;
                if actual != *expected {
                    tracing::warn!(
                        target: "client",
                        ?actual,
                        ?expected,
                        "Received contracts hashes do not match the requested ones"
                    );
                    return;
                }
                let shard_id_label = self.shard_id.to_string();
                metrics::RECEIVE_WITNESS_ACCESSED_CONTRACT_CODES_TIME
                    .with_label_values(&[&shard_id_label])
                    .observe(requested_at.elapsed().as_secs_f64());
                if let WitnessPartsState::Decoded { decoded_at, .. } = &self.witness_parts {
                    metrics::WITNESS_ACCESSED_CONTRACT_CODES_DELAY
                        .with_label_values(&[&shard_id_label])
                        .observe(decoded_at.elapsed().as_secs_f64());
                }
                self.accessed_contracts = AccessedContractsState::Received(contract_codes);
            }
            AccessedContractsState::Unknown => {
                tracing::warn!(target: "client", "Received accessed contracts without sending a request");
            }
            AccessedContractsState::Received(_) => {
                tracing::warn!(target: "client", "Already received accessed contract codes");
            }
        }
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

/// Track the Reed Solomon erasure encoded parts of the `EncodedChunkStateWitness`. These are created
/// by the chunk producer and distributed to validators. Note that we do not need all the parts of to
/// recreate the full state witness.
pub struct PartialEncodedStateWitnessTracker {
    /// Sender to send the encoded state witness to the client actor.
    client_sender: ClientSenderForPartialWitness,
    /// Epoch manager to get the set of chunk validators
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    /// Keeps track of state witness parts received from chunk producers.
    parts_cache: Mutex<LruCache<ChunkProductionKey, CacheEntry>>,
    /// Keeps track of the already decoded witnesses. This is needed
    /// to protect chunk validator from processing the same witness multiple
    /// times.
    processed_witnesses: SyncLruCache<ChunkProductionKey, ()>,
    /// Reed Solomon encoder for decoding state witness parts.
    encoders: Mutex<ReedSolomonEncoderCache>,
}

impl PartialEncodedStateWitnessTracker {
    pub fn new(
        client_sender: ClientSenderForPartialWitness,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self {
            client_sender,
            epoch_manager,
            parts_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(WITNESS_PARTS_CACHE_SIZE).unwrap(),
            )),
            processed_witnesses: SyncLruCache::new(PROCESSED_WITNESSES_CACHE_SIZE),
            encoders: Mutex::new(ReedSolomonEncoderCache::new(WITNESS_RATIO_DATA_PARTS)),
        }
    }

    pub fn store_partial_encoded_state_witness(
        &self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_witness, "store_partial_encoded_state_witness");
        let key = partial_witness.chunk_production_key();
        let encoder = self.get_encoder(&key)?;
        let update = CacheUpdate::WitnessPart(partial_witness, encoder);
        self.process_update(key, true, update)
    }

    pub fn store_accessed_contract_hashes(
        &self,
        key: ChunkProductionKey,
        hashes: HashSet<CodeHash>,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?key, ?hashes, "store_accessed_contract_hashes");
        let update = CacheUpdate::AccessedContractHashes(hashes);
        self.process_update(key, true, update)
    }

    pub fn store_accessed_contract_codes(
        &self,
        key: ChunkProductionKey,
        codes: Vec<CodeBytes>,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?key, codes_len = codes.len(), "store_accessed_contract_codes");
        let update = CacheUpdate::AccessedContractCodes(codes);
        self.process_update(key, false, update)
    }

    fn process_update(
        &self,
        key: ChunkProductionKey,
        create_if_not_exists: bool,
        update: CacheUpdate,
    ) -> Result<(), Error> {
        if self.processed_witnesses.contains(&key) {
            tracing::debug!(
                target: "client",
                ?key,
                "Received data for the already processed witness"
            );
            return Ok(());
        }

        let mut parts_cache = self.parts_cache.lock();
        if create_if_not_exists {
            Self::maybe_insert_new_entry_in_parts_cache(&mut parts_cache, &key);
        }
        let Some(entry) = parts_cache.get_mut(&key) else {
            return Ok(());
        };
        let total_size: usize = if let Some((decode_result, accessed_contracts)) =
            entry.update(update)
        {
            // Record the time taken from receiving first part to decoding partial witness.
            let time_to_last_part = Instant::now().signed_duration_since(entry.created_at);
            metrics::PARTIAL_WITNESS_TIME_TO_LAST_PART
                .with_label_values(&[key.shard_id.to_string().as_str()])
                .observe(time_to_last_part.as_seconds_f64());

            parts_cache.pop(&key);
            let total_size = parts_cache.iter().map(|(_, entry)| entry.total_size()).sum();
            drop(parts_cache);

            self.processed_witnesses.push(key.clone(), ());

            let encoded_witness = match decode_result {
                Ok(encoded_chunk_state_witness) => encoded_chunk_state_witness,
                Err(err) => {
                    // We ideally never expect the decoding to fail. In case it does, we received a bad part
                    // from the chunk producer.
                    tracing::error!(
                        target: "client",
                        ?err,
                        shard_id = ?key.shard_id,
                        height_created = key.height_created,
                        "Failed to reed solomon decode witness parts. Maybe malicious or corrupt data."
                    );
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "Failed to reed solomon decode witness parts: {err}",
                    )));
                }
            };

            let protocol_version = self.epoch_manager.get_epoch_protocol_version(&key.epoch_id)?;
            let (mut witness, raw_witness_size) =
                self.decode_state_witness(&encoded_witness, protocol_version)?;
            if witness.chunk_production_key() != key {
                return Err(Error::InvalidPartialChunkStateWitness(format!(
                    "Decoded witness key {:?} doesn't match partial witness {:?}",
                    witness.chunk_production_key(),
                    key,
                )));
            }

            // Merge accessed contracts into the main transition's partial state.
            let PartialState::TrieValues(values) =
                &mut witness.mut_main_state_transition().base_state;
            values.extend(accessed_contracts.into_iter().map(|code| code.0.into()));

            tracing::debug!(target: "client", ?key, "Sending encoded witness to client.");
            self.client_sender.send(ChunkStateWitnessMessage { witness, raw_witness_size });

            total_size
        } else {
            parts_cache.iter().map(|(_, entry)| entry.total_size()).sum()
        };
        metrics::PARTIAL_WITNESS_CACHE_SIZE.set(total_size as f64);

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

    // Function to insert a new entry into the cache for the chunk hash if it does not already exist
    // We additionally check if an evicted entry has been fully decoded and processed.
    fn maybe_insert_new_entry_in_parts_cache(
        parts_cache: &mut LruCache<ChunkProductionKey, CacheEntry>,
        key: &ChunkProductionKey,
    ) {
        if !parts_cache.contains(key) {
            if let Some((evicted_key, evicted_entry)) =
                parts_cache.push(key.clone(), CacheEntry::new(key.shard_id))
            {
                tracing::debug!(
                    target: "client",
                    ?evicted_key,
                    data_parts_present = ?evicted_entry.data_parts_present(),
                    data_parts_required = ?evicted_entry.data_parts_required(),
                    "Evicted unprocessed partial state witness."
                );
            }
        }
    }

    fn decode_state_witness(
        &self,
        encoded_witness: &EncodedChunkStateWitness,
        protocol_version: ProtocolVersion,
    ) -> Result<(ChunkStateWitness, ChunkStateWitnessSize), Error> {
        let decode_start = std::time::Instant::now();

        let (witness, raw_witness_size) =
            if ProtocolFeature::VersionedStateWitness.enabled(protocol_version) {
                // If VersionedStateWitness is enabled, we expect the type of encoded_witness to be ChunkStateWitness
                encoded_witness.decode()?
            } else {
                // If VersionedStateWitness is not enabled,
                // we expect the type of encoded_witness to be ChunkStateWitnessV1 to maintain backward compatibility
                // We then decode and wrap it in ChunkStateWitness::V1
                let (witness, raw_witness_size) = encoded_witness.decode()?;
                (ChunkStateWitness::V1(witness), raw_witness_size)
            };
        let decode_elapsed_seconds = decode_start.elapsed().as_secs_f64();
        let witness_shard = witness.chunk_header().shard_id();

        // Record metrics after validating the witness
        near_chain::stateless_validation::metrics::CHUNK_STATE_WITNESS_DECODE_TIME
            .with_label_values(&[&witness_shard.to_string()])
            .observe(decode_elapsed_seconds);

        Ok((witness, raw_witness_size))
    }
}
