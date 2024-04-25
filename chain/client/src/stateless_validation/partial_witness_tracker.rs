use std::collections::HashMap;

use lru::LruCache;
use near_async::messaging::CanSend;
use near_chain::chain::ReceiveChunkStateWitnessMessage;
use near_chain::Error;
use near_primitives::reed_solomon::rs_decode;
use near_primitives::sharding::ChunkHash;
use near_primitives::stateless_validation::{EncodedChunkStateWitness, PartialEncodedStateWitness};
use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::client_actions::ClientSenderForStateWitness;

/// Max number of chunks to keep in the witness tracker cache. We reach here only after validation
/// of the partial_witness so the LRU cache size need not be too large.
const NUM_CHUNKS_IN_WITNESS_TRACKER_CACHE: usize = 200;

/// Ratio of the number of data parts to total parts in the Reed Solomon encoding.
/// The tradeoff here is having a higher ratio is better for handling missing parts and network errors
/// but increases the size of the encoded state witness and the total network bandwidth requirements.
const RATIO_DATA_PARTS: f32 = 0.8;

/// Reed Solomon encoder for encoding state witness parts.
/// We keep one wrapper for each length of chunk_validators to avoid re-creating the encoder.
/// This is used by `PartialEncodedStateWitnessTracker`
pub struct RsMap {
    rs_map: HashMap<usize, ReedSolomon>,
}

impl RsMap {
    pub fn new() -> Self {
        Self { rs_map: HashMap::new() }
    }

    pub fn entry(&mut self, total_parts: usize) -> &ReedSolomon {
        self.rs_map.entry(total_parts).or_insert_with(|| {
            let data_parts = std::cmp::max((total_parts as f32 * RATIO_DATA_PARTS) as usize, 1);
            ReedSolomon::new(data_parts, total_parts - data_parts).unwrap()
        })
    }
}

struct CacheEntry {
    pub is_decoded: bool,
    pub data_parts_present: usize,
    pub data_parts_required: usize,
    pub parts: Vec<Option<Box<[u8]>>>,
}

impl CacheEntry {
    pub fn new(total_parts: usize, data_parts: usize) -> Self {
        Self {
            is_decoded: false,
            data_parts_present: 0,
            data_parts_required: data_parts,
            parts: vec![None; total_parts],
        }
    }

    // Function to insert a part into the cache entry for the chunk hash. Additionally, it tries to
    // decode and return the state witness if all parts are present.
    pub fn insert_in_cache_entry(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
        rs: &ReedSolomon,
    ) -> Option<EncodedChunkStateWitness> {
        let chunk_hash = partial_witness.chunk_header().chunk_hash();
        let (part_ord, part, encoded_length) = partial_witness.decompose();

        // Check if the part is already present.
        if self.parts[part_ord].is_some() {
            tracing::warn!(
                target: "stateless_validation",
                ?chunk_hash,
                part_ord = ?part_ord,
                "Received duplicate or redundant partial state witness part."
            );
            return None;
        }

        // Check if we have already decoded the state witness.
        if self.is_decoded {
            return None;
        }

        // Insert the part into the cache entry.
        self.parts[part_ord] = Some(part);
        self.data_parts_present += 1;

        // If we have enough parts, try to decode the state witness.
        if self.data_parts_present < self.data_parts_required {
            return None;
        }
        self.is_decoded = true;
        match rs_decode(&rs, &mut self.parts, encoded_length) {
            Ok(encoded_chunk_state_witness) => Some(encoded_chunk_state_witness),
            Err(err) => {
                // We ideally never expect the decoding to fail. In case it does, we received a bad part
                // from the chunk producer.
                tracing::error!(target: "stateless_validation", ?err, ?chunk_hash, "Failed to decode witness part.");
                None
            }
        }
    }
}

/// Track the Reed Solomon erasure encoded parts of the `EncodedChunkStateWitness`. These are created
/// by the chunk producer and distributed to validators. Note that we do not need all the parts of to
/// recreate the full state witness.
pub struct PartialEncodedStateWitnessTracker {
    /// Sender to send the encoded state witness to the client actor.
    client_sender: ClientSenderForStateWitness,
    /// Keeps track of state witness parts received from chunk producers.
    parts_cache: LruCache<ChunkHash, CacheEntry>,
    /// Reed Solomon encoder for decoding state witness parts.
    rs_map: RsMap,
}

impl PartialEncodedStateWitnessTracker {
    pub fn new(client_sender: ClientSenderForStateWitness) -> Self {
        Self {
            client_sender,
            parts_cache: LruCache::new(NUM_CHUNKS_IN_WITNESS_TRACKER_CACHE),
            rs_map: RsMap::new(),
        }
    }

    pub fn store_partial_encoded_state_witness(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        let rs = self.rs_map.entry(partial_witness.num_parts());

        PartialEncodedStateWitnessTracker::maybe_insert_new_entry_in_parts_cache(
            &partial_witness,
            &mut self.parts_cache,
            &rs,
        );

        let chunk_hash = partial_witness.chunk_header().chunk_hash();
        let entry = self.parts_cache.get_mut(&chunk_hash).unwrap();

        if let Some(encoded_witness) = entry.insert_in_cache_entry(partial_witness, rs) {
            self.client_sender.send(ReceiveChunkStateWitnessMessage(encoded_witness));
        }
        Ok(())
    }

    // Function to insert a new entry into the cache for the chunk hash if it does not already exist
    // We additionally check if an evicted entry has been fully decoded and processed.
    fn maybe_insert_new_entry_in_parts_cache(
        partial_witness: &PartialEncodedStateWitness,
        parts_cache: &mut LruCache<ChunkHash, CacheEntry>,
        rs: &ReedSolomon,
    ) {
        // Insert a new entry into the cache for the chunk hash.
        let chunk_hash = partial_witness.chunk_header().chunk_hash();
        if parts_cache.contains(&chunk_hash) {
            return;
        }
        let new_entry = CacheEntry::new(rs.total_shard_count(), rs.data_shard_count());
        if let Some((evicted_chunk_hash, evicted_entry)) = parts_cache.push(chunk_hash, new_entry) {
            // Check if the evicted entry has been fully decoded and processed.
            if !evicted_entry.is_decoded {
                tracing::warn!(
                    target: "stateless_validation",
                    ?evicted_chunk_hash,
                    data_parts_present = ?evicted_entry.data_parts_present,
                    data_parts_required = ?evicted_entry.data_parts_required,
                    "Evicted unprocessed partial state witness."
                );
            }
        }
    }
}
