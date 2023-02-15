use std::collections::{HashMap, HashSet};

use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunkPart, PartialEncodedChunkV2, ReceiptProof, ShardChunkHeader,
};
use near_primitives::types::{BlockHeight, BlockHeightDelta, ShardId};
use std::collections::hash_map::Entry::Occupied;
use tracing::warn;

// This file implements EncodedChunksCache, which provides three main functionalities:
// 1) It stores a map from a chunk hash to all the parts and receipts received so far for the chunk.
//    This map is used to aggregate chunk parts and receipts before the full chunk can be reconstructed
//    or the necessary parts and receipts are received.
//    When a PartialEncodedChunk is received, the parts and receipts it contains are merged to the
//    corresponding chunk entry in the map.
//    Entries in the map are removed if the chunk is found to be invalid or the chunk goes out of
//    horizon [chain_head_height - HEIGHT_HORIZON, chain_head_height + MAX_HEIGHTS_AHEAD]
// 2) It stores the set of incomplete chunks, indexed by the block hash of the previous block.
//    A chunk always starts incomplete. It can be marked as complete through
//    `mark_entry_complete`. A complete entry means the chunk has all parts and receipts needed.
// 3) It stores a map from block hash to chunk headers that are ready to be included in a block.
//    This functionality is meant for block producers. When producing a block, the block producer
//    will only include chunks in the block for which it has received the part it owns.
//    Users of the data structure are responsible for adding chunk to this map at the right time.

/// A chunk is out of horizon if its height + HEIGHT_HORIZON < largest_seen_height
const HEIGHT_HORIZON: BlockHeightDelta = 1024;
/// A chunk is out of horizon if its height > HEIGHT_HORIZON + largest_seen_height
const MAX_HEIGHTS_AHEAD: BlockHeightDelta = 5;

/// EncodedChunksCacheEntry stores the consolidated parts and receipts received for a chunk
/// When a PartialEncodedChunk is received, it can be merged to the existing EncodedChunksCacheEntry
/// for the chunk
pub struct EncodedChunksCacheEntry {
    pub header: ShardChunkHeader,
    pub parts: HashMap<u64, PartialEncodedChunkPart>,
    pub receipts: HashMap<ShardId, ReceiptProof>,
    /// whether this entry has all parts and receipts
    pub complete: bool,
    /// whether this chunk is ready for inclusion for producing a block
    pub ready_for_inclusion: bool,
    /// Whether the header has been **fully** validated.
    /// Every entry added to the cache already has their header "partially" validated
    /// by validate_chunk_header. When the previous block is accepted, they must be
    /// validated again to make sure they are fully validated.
    /// See comments in `validate_chunk_header` for more context on partial vs full validation
    pub header_fully_validated: bool,
}

pub struct EncodedChunksCache {
    /// Largest seen height from the head of the chain
    largest_seen_height: BlockHeight,

    /// A map from a chunk hash to the corresponding EncodedChunksCacheEntry of the chunk
    /// Entries in this map have height in
    /// [chain_head_height - HEIGHT_HORIZON, chain_head_height + MAX_HEIGHTS_AHEAD]
    encoded_chunks: HashMap<ChunkHash, EncodedChunksCacheEntry>,
    /// A map from a block height to chunk hashes at this height for all chunk stored in the cache
    /// This is used to gc chunks that are out of horizon
    height_map: HashMap<BlockHeight, HashSet<ChunkHash>>,
    /// A map from block height to shard ID to the chunk hash we've received, so we only process
    /// one chunk per shard per height.
    height_to_shard_to_chunk: HashMap<BlockHeight, HashMap<ShardId, ChunkHash>>,
    /// A map from a block hash to a set of incomplete chunks (does not have all parts and receipts yet)
    /// whose previous block is the block hash.
    incomplete_chunks: HashMap<CryptoHash, HashSet<ChunkHash>>,
}

impl EncodedChunksCacheEntry {
    pub fn from_chunk_header(header: ShardChunkHeader) -> Self {
        EncodedChunksCacheEntry {
            header,
            parts: HashMap::new(),
            receipts: HashMap::new(),
            complete: false,
            ready_for_inclusion: false,
            header_fully_validated: false,
        }
    }

    /// Inserts previously unknown chunks and receipts, returning the part ords that were
    /// previously unknown.
    pub fn merge_in_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: &PartialEncodedChunkV2,
    ) -> HashSet<u64> {
        let mut previously_missing_part_ords = HashSet::new();
        for part_info in partial_encoded_chunk.parts.iter() {
            let part_ord = part_info.part_ord;
            self.parts.entry(part_ord).or_insert_with(|| {
                previously_missing_part_ords.insert(part_ord);
                part_info.clone()
            });
        }

        for receipt in partial_encoded_chunk.receipts.iter() {
            let shard_id = receipt.1.to_shard_id;
            self.receipts.entry(shard_id).or_insert_with(|| receipt.clone());
        }
        previously_missing_part_ords
    }
}

impl EncodedChunksCache {
    pub fn new() -> Self {
        EncodedChunksCache {
            largest_seen_height: 0,
            encoded_chunks: HashMap::new(),
            height_map: HashMap::new(),
            height_to_shard_to_chunk: HashMap::new(),
            incomplete_chunks: HashMap::new(),
        }
    }

    pub fn get(&self, chunk_hash: &ChunkHash) -> Option<&EncodedChunksCacheEntry> {
        self.encoded_chunks.get(chunk_hash)
    }

    /// Mark an entry as complete, which means it has all parts and receipts needed
    pub fn mark_entry_complete(&mut self, chunk_hash: &ChunkHash) {
        if let Some(entry) = self.encoded_chunks.get_mut(chunk_hash) {
            entry.complete = true;
            let previous_block_hash = &entry.header.prev_block_hash().clone();
            self.remove_chunk_from_incomplete_chunks(previous_block_hash, chunk_hash);
        } else {
            warn!(target:"chunks", "cannot mark non-existent entry as complete {:?}", chunk_hash);
        }
    }

    pub fn mark_entry_validated(&mut self, chunk_hash: &ChunkHash) {
        if let Some(entry) = self.encoded_chunks.get_mut(chunk_hash) {
            entry.header_fully_validated = true;
        } else {
            warn!("no entry exist {:?}", chunk_hash);
        }
    }

    /// Get a list of incomplete chunks whose previous block hash is `prev_block_hash`
    pub fn get_incomplete_chunks(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Option<&HashSet<ChunkHash>> {
        self.incomplete_chunks.get(prev_block_hash)
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) -> Option<EncodedChunksCacheEntry> {
        if let Some(entry) = self.encoded_chunks.remove(chunk_hash) {
            self.remove_chunk_from_incomplete_chunks(entry.header.prev_block_hash(), chunk_hash);
            Some(entry)
        } else {
            None
        }
    }

    // Remove the chunk from the `incomplete_chunks` map. This is an internal function.
    // Use `mark_entry_complete` instead for outside calls
    fn remove_chunk_from_incomplete_chunks(
        &mut self,
        prev_block_hash: &CryptoHash,
        chunk_hash: &ChunkHash,
    ) {
        if let Occupied(mut entry) = self.incomplete_chunks.entry(*prev_block_hash) {
            entry.get_mut().remove(chunk_hash);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    // Create an empty entry from the header and insert it if there is no entry for the chunk already
    // Return a mutable reference to the entry
    pub fn get_or_insert_from_header(
        &mut self,
        chunk_header: &ShardChunkHeader,
    ) -> &mut EncodedChunksCacheEntry {
        let chunk_hash = chunk_header.chunk_hash();
        self.encoded_chunks.entry(chunk_hash).or_insert_with_key(|chunk_hash| {
            self.height_map
                .entry(chunk_header.height_created())
                .or_default()
                .insert(chunk_hash.clone());
            self.height_to_shard_to_chunk
                .entry(chunk_header.height_created())
                .or_default()
                .insert(chunk_header.shard_id(), chunk_hash.clone());
            self.incomplete_chunks
                .entry(*chunk_header.prev_block_hash())
                .or_default()
                .insert(chunk_hash.clone());
            EncodedChunksCacheEntry::from_chunk_header(chunk_header.clone())
        })
    }

    pub fn height_within_front_horizon(&self, height: BlockHeight) -> bool {
        height >= self.largest_seen_height && height <= self.largest_seen_height + MAX_HEIGHTS_AHEAD
    }

    pub fn height_within_rear_horizon(&self, height: BlockHeight) -> bool {
        height + HEIGHT_HORIZON >= self.largest_seen_height && height <= self.largest_seen_height
    }

    pub fn height_within_horizon(&self, height: BlockHeight) -> bool {
        self.height_within_front_horizon(height) || self.height_within_rear_horizon(height)
    }

    pub fn get_chunk_hash_by_height_and_shard(
        &self,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Option<&ChunkHash> {
        self.height_to_shard_to_chunk.get(&height)?.get(&shard_id)
    }

    /// Add parts and receipts stored in a partial encoded chunk to the corresponding chunk entry,
    /// returning the set of part ords that were previously unknown.
    pub fn merge_in_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: &PartialEncodedChunkV2,
    ) -> HashSet<u64> {
        let entry = self.get_or_insert_from_header(&partial_encoded_chunk.header);
        entry.merge_in_partial_encoded_chunk(partial_encoded_chunk)
    }

    /// Remove a chunk from the cache if it is outside of horizon
    pub fn remove_from_cache_if_outside_horizon(&mut self, chunk_hash: &ChunkHash) {
        if let Some(entry) = self.encoded_chunks.get(chunk_hash) {
            let height = entry.header.height_created();
            if !self.height_within_horizon(height) {
                self.remove(&chunk_hash);
            }
        }
    }

    /// Update largest seen height and removes chunks from the cache that are outside of horizon
    pub fn update_largest_seen_height<T>(
        &mut self,
        new_height: BlockHeight,
        requested_chunks: &HashMap<ChunkHash, T>,
    ) {
        let old_largest_seen_height = self.largest_seen_height;
        self.largest_seen_height = new_height;
        for height in old_largest_seen_height.saturating_sub(HEIGHT_HORIZON)
            ..self.largest_seen_height.saturating_sub(HEIGHT_HORIZON)
        {
            if let Some(chunks_to_remove) = self.height_map.remove(&height) {
                for chunk_hash in chunks_to_remove {
                    if !requested_chunks.contains_key(&chunk_hash) {
                        self.remove(&chunk_hash);
                    }
                }
            }
            self.height_to_shard_to_chunk.remove(&height);
        }
    }

    /// Marks the chunk for inclusion in a block; returns true if we haven't already
    /// called for this chunk. Requires that the chunk is already in the cache.
    pub fn mark_chunk_for_inclusion(&mut self, chunk_hash: &ChunkHash) -> bool {
        let entry = self.encoded_chunks.get_mut(chunk_hash).unwrap();
        if entry.ready_for_inclusion {
            false
        } else {
            entry.ready_for_inclusion = true;
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use near_crypto::KeyType;
    use near_primitives::hash::CryptoHash;
    use near_primitives::sharding::{PartialEncodedChunkV2, ShardChunkHeader, ShardChunkHeaderV2};
    use near_primitives::validator_signer::InMemoryValidatorSigner;

    use crate::chunk_cache::EncodedChunksCache;
    use crate::ChunkRequestInfo;

    fn create_chunk_header(height: u64, shard_id: u64) -> ShardChunkHeader {
        let signer =
            InMemoryValidatorSigner::from_random("test".parse().unwrap(), KeyType::ED25519);
        ShardChunkHeader::V2(ShardChunkHeaderV2::new(
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            1,
            height,
            shard_id,
            0,
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
            &signer,
        ))
    }

    #[test]
    fn test_incomplete_chunks() {
        let mut cache = EncodedChunksCache::new();
        let header0 = create_chunk_header(1, 0);
        let header1 = create_chunk_header(1, 1);
        cache.get_or_insert_from_header(&header0);
        cache.merge_in_partial_encoded_chunk(&PartialEncodedChunkV2 {
            header: header1.clone(),
            parts: vec![],
            receipts: vec![],
        });
        assert_eq!(
            cache.get_incomplete_chunks(&CryptoHash::default()).unwrap(),
            &HashSet::from([header0.chunk_hash(), header1.chunk_hash()])
        );
        cache.mark_entry_complete(&header0.chunk_hash());
        assert_eq!(
            cache.get_incomplete_chunks(&CryptoHash::default()).unwrap(),
            &[header1.chunk_hash()].into_iter().collect::<HashSet<_>>()
        );
        cache.mark_entry_complete(&header1.chunk_hash());
        assert_eq!(cache.get_incomplete_chunks(&CryptoHash::default()), None);
    }

    #[test]
    fn test_cache_removal() {
        let mut cache = EncodedChunksCache::new();
        let header = create_chunk_header(1, 0);
        let partial_encoded_chunk =
            PartialEncodedChunkV2 { header: header, parts: vec![], receipts: vec![] };
        cache.merge_in_partial_encoded_chunk(&partial_encoded_chunk);
        assert!(!cache.height_map.is_empty());

        cache.update_largest_seen_height::<ChunkRequestInfo>(2000, &HashMap::default());
        assert!(cache.encoded_chunks.is_empty());
        assert!(cache.height_map.is_empty());
    }
}
