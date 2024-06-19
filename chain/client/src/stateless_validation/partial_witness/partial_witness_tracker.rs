use std::num::NonZeroUsize;
use std::sync::Arc;

use lru::LruCache;
use near_async::messaging::CanSend;
use near_async::time::Instant;
use near_chain::chain::ChunkStateWitnessMessage;
use near_chain::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_o11y::log_assert_fail;
use near_primitives::stateless_validation::{
    ChunkProductionKey, ChunkStateWitness, ChunkStateWitnessSize, EncodedChunkStateWitness,
    PartialEncodedStateWitness,
};
use time::ext::InstantExt as _;

use crate::client_actor::ClientSenderForPartialWitness;
use crate::metrics;

use super::encoding::{WitnessEncoder, WitnessEncoderCache, WitnessPart};

/// Max number of chunks to keep in the witness tracker cache. We reach here only after validation
/// of the partial_witness so the LRU cache size need not be too large.
/// This effectively limits memory usage to the size of the cache multiplied by
/// MAX_COMPRESSED_STATE_WITNESS_SIZE, currently 40 * 32MiB = 1280MiB
const WITNESS_PARTS_CACHE_SIZE: usize = 40;

/// Number of entries to keep in LRU cache of the processed state witnesses
/// We only store small amount of data (ChunkProductionKey) per entry there,
/// so we don't have to worry much about memory usage here.
const PROCESSED_WITNESSES_CACHE_SIZE: usize = 200;

struct CacheEntry {
    pub created_at: Instant,
    pub data_parts_present: usize,
    pub parts: Vec<WitnessPart>,
    pub encoder: Arc<WitnessEncoder>,
    pub total_parts_size: usize,
}

impl CacheEntry {
    pub fn new(encoder: Arc<WitnessEncoder>) -> Self {
        Self {
            created_at: Instant::now(),
            data_parts_present: 0,
            parts: vec![None; encoder.total_parts()],
            total_parts_size: 0,
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
    ) -> Option<std::io::Result<EncodedChunkStateWitness>> {
        let shard_id = partial_witness.shard_id();
        let height_created = partial_witness.height_created();
        let (part_ord, part, encoded_length) = partial_witness.decompose();

        // Check if the part is already present.
        if self.parts[part_ord].is_some() {
            log_assert_fail!("Received duplicate or redundant partial state witness part. shard_id={shard_id:?}, height_created={height_created:?}, part_ord={part_ord:?}");
            return None;
        }

        // Increment the count of data parts present even if the part has been decoded before.
        // We use this in metrics to track the number of parts received. Insert the part into the cache entry.
        self.data_parts_present += 1;
        self.total_parts_size += part.len();
        self.parts[part_ord] = Some(part);

        // If we have enough parts, try to decode the state witness.
        if self.data_parts_present < self.data_parts_required() {
            return None;
        }

        let decode_result = self.encoder.decode(&mut self.parts, encoded_length);
        Some(decode_result)
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

            let encoded_witness = match decode_result {
                Ok(encoded_chunk_state_witness) => encoded_chunk_state_witness,
                Err(err) => {
                    // We ideally never expect the decoding to fail. In case it does, we received a bad part
                    // from the chunk producer.
                    tracing::error!(
                        target: "client",
                        ?err,
                        shard_id = key.shard_id,
                        height_created = key.height_created,
                        "Failed to reed solomon decode witness parts. Maybe malicious or corrupt data."
                    );
                    return Err(Error::InvalidPartialChunkStateWitness(format!(
                        "Failed to reed solomon decode witness parts: {err}",
                    )));
                }
            };

            let (witness, raw_witness_size) = self.decode_state_witness(&encoded_witness)?;
            if witness.chunk_production_key() != key {
                return Err(Error::InvalidPartialChunkStateWitness(format!(
                    "Decoded witness key {:?} doesn't match partial witness {:?}",
                    witness.chunk_production_key(),
                    key,
                )));
            }

            tracing::debug!(target: "client", ?key, "Sending encoded witness to client.");
            self.client_sender.send(ChunkStateWitnessMessage { witness, raw_witness_size });
        }
        self.record_total_parts_cache_size_metric();
        Ok(())
    }

    fn get_num_parts(&self, partial_witness: &PartialEncodedStateWitness) -> Result<usize, Error> {
        // The expected number of parts for the Reed Solomon encoding is the number of chunk validators.
        Ok(self
            .epoch_manager
            .get_chunk_validator_assignments(
                partial_witness.epoch_id(),
                partial_witness.shard_id(),
                partial_witness.height_created(),
            )?
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
        let protocol_version =
            self.epoch_manager.get_epoch_protocol_version(partial_witness.epoch_id())?;
        let new_entry = CacheEntry::new(self.encoders.entry(num_parts, protocol_version));
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
