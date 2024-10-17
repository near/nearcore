use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use near_async::messaging::CanSend;
use near_async::time::Instant;
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::challenge::PartialState;
use near_primitives::hash::CryptoHash;
use near_primitives::stateless_validation::contract_distribution::{CodeBytes, CodeHash};
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessSize, EncodedChunkStateWitness,
};
use near_primitives::stateless_validation::ChunkProductionKey;
use time::ext::InstantExt as _;

use crate::client_actor::ClientSenderForPartialWitness;
use crate::metrics;

use super::encoding::{WitnessEncoder, WitnessEncoderCache, WitnessPart};
use near_primitives::utils::compression::CompressedData;

/// Max number of chunks to keep in the witness tracker cache. We reach here only after validation
/// of the partial_witness so the LRU cache size need not be too large.
/// This effectively limits memory usage to the size of the cache multiplied by
/// MAX_COMPRESSED_STATE_WITNESS_SIZE, currently 40 * 48MiB = 1920MiB.
const WITNESS_PARTS_CACHE_SIZE: usize = 40;

/// Number of entries to keep in LRU cache of the processed state witnesses
/// We only store small amount of data (ChunkProductionKey) per entry there,
/// so we don't have to worry much about memory usage here.
const PROCESSED_WITNESSES_CACHE_SIZE: usize = 200;

pub type DecodePartialWitnessResult = std::io::Result<(EncodedChunkStateWitness, Vec<CodeBytes>)>;

enum AccessedContractsState {
    Unknown,
    Pending(Vec<CodeHash>),
    Received(Vec<CodeBytes>),
}

struct CacheEntry {
    pub created_at: Instant,
    pub data_parts_present: usize,
    pub parts: Vec<WitnessPart>,
    pub encoder: Arc<WitnessEncoder>,
    pub encoded_length: Option<usize>,
    pub accessed_contracts: AccessedContractsState,
    pub total_parts_size: usize,
}

impl CacheEntry {
    pub fn new(encoder: Arc<WitnessEncoder>) -> Self {
        Self {
            created_at: Instant::now(),
            data_parts_present: 0,
            parts: vec![None; encoder.total_parts()],
            total_parts_size: 0,
            encoded_length: None,
            accessed_contracts: AccessedContractsState::Unknown,
            encoder,
        }
    }

    fn data_parts_required(&self) -> usize {
        self.encoder.data_parts()
    }

    // Function to insert a part into the cache entry for the chunk hash. Additionally, it tries to
    // decode and return the state witness if all parts are present.
    pub fn insert_in_cache_entry(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Option<DecodePartialWitnessResult> {
        let ChunkProductionKey { shard_id, height_created, .. } =
            partial_witness.chunk_production_key();
        let (part_ord, part, encoded_length) = partial_witness.decompose();

        // Check if the part is already present.
        if self.parts[part_ord].is_some() {
            tracing::warn!("Received duplicate or redundant partial state witness part. shard_id={shard_id:?}, height_created={height_created:?}, part_ord={part_ord:?}");
            return None;
        }

        match self.encoded_length {
            Some(current_encoded_length) => {
                if current_encoded_length != encoded_length {
                    tracing::warn!("Partial encoded witness length field doesn't match, expected {current_encoded_length}, got {encoded_length}");
                    return None;
                }
            }
            None => {
                self.encoded_length = Some(encoded_length);
            }
        }

        // Increment the count of data parts present even if the part has been decoded before.
        // We use this in metrics to track the number of parts received. Insert the part into the cache entry.
        self.data_parts_present += 1;
        self.total_parts_size += part.len();
        self.parts[part_ord] = Some(part);

        self.decode_if_ready()
    }

    pub fn set_pending_contracts(&mut self, contract_hashes: Vec<CodeHash>) {
        match &self.accessed_contracts {
            AccessedContractsState::Unknown => {
                self.accessed_contracts = AccessedContractsState::Pending(contract_hashes);
            }
            AccessedContractsState::Pending(_) | AccessedContractsState::Received(_) => {
                todo!("warn")
            }
        }
    }

    pub fn set_received_contracts(
        &mut self,
        contract_code: Vec<CodeBytes>,
    ) -> Option<DecodePartialWitnessResult> {
        match &self.accessed_contracts {
            AccessedContractsState::Pending(hashes) => {
                let actual = HashSet::<CryptoHash>::from_iter(
                    contract_code.iter().map(|code| CryptoHash::hash_bytes(&code.0)),
                );
                let expected = HashSet::from_iter(hashes.iter().map(|hash| hash.0));
                if actual == expected {
                    self.accessed_contracts = AccessedContractsState::Received(contract_code);
                    self.decode_if_ready()
                } else {
                    todo!("warn")
                }
            }
            AccessedContractsState::Unknown | AccessedContractsState::Received(_) => todo!("warn"),
        }
    }

    fn decode_if_ready(&mut self) -> Option<DecodePartialWitnessResult> {
        if self.data_parts_present < self.data_parts_required() {
            return None;
        }
        let encoded_length = self
            .encoded_length
            .expect("Expect encoded length to be set here since we proccessed at least one part");
        let contracts: Vec<CodeBytes> = match &mut self.accessed_contracts {
            AccessedContractsState::Unknown => vec![],
            AccessedContractsState::Pending(_) => {
                return None;
            }
            AccessedContractsState::Received(contracts) => {
                // TODO: this is ugly
                std::mem::take(contracts)
            }
        };

        let decode_result = self.encoder.decode(&mut self.parts, encoded_length);
        Some(decode_result.map(|decode_result| (decode_result, contracts)))
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
    parts_cache: LruCache<ChunkProductionKey, CacheEntry>,
    /// Keeps track of the already decoded witnesses. This is needed
    /// to protect chunk validator from processing the same witness multiple
    /// times.
    processed_witnesses: LruCache<ChunkProductionKey, ()>,
    /// Reed Solomon encoder for decoding state witness parts.
    encoders: WitnessEncoderCache,
}

impl PartialEncodedStateWitnessTracker {
    pub fn new(
        client_sender: ClientSenderForPartialWitness,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        Self {
            client_sender,
            epoch_manager,
            parts_cache: LruCache::new(NonZeroUsize::new(WITNESS_PARTS_CACHE_SIZE).unwrap()),
            processed_witnesses: LruCache::new(
                NonZeroUsize::new(PROCESSED_WITNESSES_CACHE_SIZE).unwrap(),
            ),
            encoders: WitnessEncoderCache::new(),
        }
    }

    pub fn store_partial_encoded_state_witness(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        tracing::debug!(target: "client", ?partial_witness, "store_partial_encoded_state_witness");

        let key = partial_witness.chunk_production_key();
        if self.processed_witnesses.contains(&key) {
            tracing::debug!(
                target: "client",
                ?partial_witness,
                "Received redundant part for already processed witness"
            );
            return Ok(());
        }

        self.maybe_insert_new_entry_in_parts_cache(&partial_witness)?;
        let entry = self.parts_cache.get_mut(&key).unwrap();

        if let Some(decode_result) = entry.insert_in_cache_entry(partial_witness) {
            // Record the time taken from receiving first part to decoding partial witness.
            let time_to_last_part = Instant::now().signed_duration_since(entry.created_at);
            metrics::PARTIAL_WITNESS_TIME_TO_LAST_PART
                .with_label_values(&[key.shard_id.to_string().as_str()])
                .observe(time_to_last_part.as_seconds_f64());

            self.parts_cache.pop(&key);
            self.processed_witnesses.push(key.clone(), ());

            let (encoded_witness, accessed_contracts) = match decode_result {
                Ok(data) => data,
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

            let (mut witness, raw_witness_size) = self.decode_state_witness(&encoded_witness)?;
            if witness.chunk_production_key() != key {
                return Err(Error::InvalidPartialChunkStateWitness(format!(
                    "Decoded witness key {:?} doesn't match partial witness {:?}",
                    witness.chunk_production_key(),
                    key,
                )));
            }

            let PartialState::TrieValues(values) = &mut witness.main_state_transition.base_state;
            values.extend(accessed_contracts.into_iter().map(|code| code.0.into()));

            tracing::debug!(target: "client", ?key, "Sending encoded witness to client.");
            self.client_sender.send(ChunkStateWitnessMessage { witness, raw_witness_size });
        }
        self.record_total_parts_cache_size_metric();
        Ok(())
    }

    fn get_num_parts(&self, partial_witness: &PartialEncodedStateWitness) -> Result<usize, Error> {
        // The expected number of parts for the Reed Solomon encoding is the number of chunk validators.
        let ChunkProductionKey { shard_id, epoch_id, height_created } =
            partial_witness.chunk_production_key();
        Ok(self
            .epoch_manager
            .get_chunk_validator_assignments(&epoch_id, shard_id, height_created)?
            .len())
    }

    // Function to insert a new entry into the cache for the chunk hash if it does not already exist
    // We additionally check if an evicted entry has been fully decoded and processed.
    fn maybe_insert_new_entry_in_parts_cache(
        &mut self,
        partial_witness: &PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        // Insert a new entry into the cache for the chunk hash.
        let key = partial_witness.chunk_production_key();
        if self.parts_cache.contains(&key) {
            return Ok(());
        }
        let num_parts = self.get_num_parts(&partial_witness)?;
        let new_entry = CacheEntry::new(self.encoders.entry(num_parts));
        if let Some((evicted_key, evicted_entry)) = self.parts_cache.push(key, new_entry) {
            tracing::warn!(
                target: "client",
                ?evicted_key,
                data_parts_present = ?evicted_entry.data_parts_present,
                data_parts_required = ?evicted_entry.data_parts_required(),
                "Evicted unprocessed partial state witness."
            );
        }
        Ok(())
    }

    fn record_total_parts_cache_size_metric(&self) {
        let total_size: usize =
            self.parts_cache.iter().map(|(_, entry)| entry.total_parts_size).sum();
        metrics::PARTIAL_WITNESS_CACHE_SIZE.set(total_size as f64);
    }

    fn decode_state_witness(
        &self,
        encoded_witness: &EncodedChunkStateWitness,
    ) -> Result<(ChunkStateWitness, ChunkStateWitnessSize), Error> {
        let decode_start = std::time::Instant::now();
        let (witness, raw_witness_size) = encoded_witness.decode()?;
        let decode_elapsed_seconds = decode_start.elapsed().as_secs_f64();
        let witness_shard = witness.chunk_header.shard_id();

        // Record metrics after validating the witness
        near_chain::stateless_validation::metrics::CHUNK_STATE_WITNESS_DECODE_TIME
            .with_label_values(&[&witness_shard.to_string()])
            .observe(decode_elapsed_seconds);

        Ok((witness, raw_witness_size))
    }
}
