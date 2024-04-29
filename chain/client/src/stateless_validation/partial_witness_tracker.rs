use std::collections::HashMap;
use std::sync::Arc;

use lru::LruCache;
use near_chain::Error;
use near_primitives::reed_solomon::reed_solomon_decode;
use near_primitives::sharding::ChunkHash;
use near_primitives::stateless_validation::{EncodedChunkStateWitness, PartialEncodedStateWitness};
use reed_solomon_erasure::galois_8::ReedSolomon;

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
    rs_map: HashMap<usize, Arc<ReedSolomon>>,
}

impl RsMap {
    pub fn new() -> Self {
        Self { rs_map: HashMap::new() }
    }

    pub fn entry(&mut self, total_parts: usize) -> Arc<ReedSolomon> {
        self.rs_map
            .entry(total_parts)
            .or_insert_with(|| {
                let data_parts = std::cmp::max((total_parts as f32 * RATIO_DATA_PARTS) as usize, 1);
                Arc::new(ReedSolomon::new(data_parts, total_parts - data_parts).unwrap())
            })
            .clone()
    }
}

struct CacheEntry {
    pub is_decoded: bool,
    pub data_parts_present: usize,
    pub data_parts_required: usize,
    pub parts: Vec<Option<Box<[u8]>>>,
    pub rs: Arc<ReedSolomon>,
}

impl CacheEntry {
    pub fn new(rs: Arc<ReedSolomon>) -> Self {
        Self {
            is_decoded: false,
            data_parts_present: 0,
            data_parts_required: rs.data_shard_count(),
            parts: vec![None; rs.total_shard_count()],
            rs,
        }
    }

    // Function to insert a part into the cache entry for the chunk hash. Additionally, it tries to
    // decode and return the state witness if all parts are present.
    pub fn insert_in_cache_entry(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
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
        match reed_solomon_decode(&self.rs, &mut self.parts, encoded_length) {
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
    /// Keeps track of state witness parts received from chunk producers.
    parts_cache: LruCache<ChunkHash, CacheEntry>,
    /// Reed Solomon encoder for decoding state witness parts.
    rs_map: RsMap,
}

impl PartialEncodedStateWitnessTracker {
    pub fn new() -> Self {
        Self {
            parts_cache: LruCache::new(NUM_CHUNKS_IN_WITNESS_TRACKER_CACHE),
            rs_map: RsMap::new(),
        }
    }

    pub fn store_partial_encoded_state_witness(
        &mut self,
        partial_witness: PartialEncodedStateWitness,
    ) -> Result<(), Error> {
        self.maybe_insert_new_entry_in_parts_cache(&partial_witness);

        let chunk_hash = partial_witness.chunk_header().chunk_hash();
        let entry = self.parts_cache.get_mut(&chunk_hash).unwrap();

        if let Some(_witness) = entry.insert_in_cache_entry(partial_witness) {
            // TODO(stateless_validation): Send encoded state witness to client here.
        }
        Ok(())
    }

    // Function to insert a new entry into the cache for the chunk hash if it does not already exist
    // We additionally check if an evicted entry has been fully decoded and processed.
    fn maybe_insert_new_entry_in_parts_cache(
        &mut self,
        partial_witness: &PartialEncodedStateWitness,
    ) {
        // Insert a new entry into the cache for the chunk hash.
        let chunk_hash = partial_witness.chunk_header().chunk_hash();
        if self.parts_cache.contains(&chunk_hash) {
            return;
        }
        let rs = self.rs_map.entry(partial_witness.num_parts());
        let new_entry = CacheEntry::new(rs);
        if let Some((evicted_chunk_hash, evicted_entry)) =
            self.parts_cache.push(chunk_hash, new_entry)
        {
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
