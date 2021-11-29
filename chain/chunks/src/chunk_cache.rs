use std::collections::{HashMap, HashSet};

use cached::{Cached, SizedCache};

use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunkPart, PartialEncodedChunkV2, ReceiptProof, ShardChunkHeader,
};
use near_primitives::types::{BlockHeight, BlockHeightDelta, ShardId};

const HEIGHT_HORIZON: BlockHeightDelta = 1024;
const MAX_HEIGHTS_AHEAD: BlockHeightDelta = 5;
const CHUNK_HEADER_HEIGHT_HORIZON: BlockHeightDelta = 10;
const NUM_BLOCK_HASH_TO_CHUNK_HEADER: usize = 30;

pub struct EncodedChunksCacheEntry {
    pub header: ShardChunkHeader,
    pub parts: HashMap<u64, PartialEncodedChunkPart>,
    pub receipts: HashMap<ShardId, ReceiptProof>,
}

pub struct EncodedChunksCache {
    /// Largest seen height from the head of the chain
    largest_seen_height: BlockHeight,

    encoded_chunks: HashMap<ChunkHash, EncodedChunksCacheEntry>,
    height_map: HashMap<BlockHeight, HashSet<ChunkHash>>,
    block_hash_to_chunk_headers: SizedCache<CryptoHash, HashMap<ShardId, ShardChunkHeader>>,
}

impl EncodedChunksCacheEntry {
    pub fn from_chunk_header(header: ShardChunkHeader) -> Self {
        EncodedChunksCacheEntry { header, parts: HashMap::new(), receipts: HashMap::new() }
    }

    pub fn merge_in_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: &PartialEncodedChunkV2,
    ) {
        for part_info in partial_encoded_chunk.parts.iter() {
            let part_ord = part_info.part_ord;
            self.parts.entry(part_ord).or_insert_with(|| part_info.clone());
        }

        for receipt in partial_encoded_chunk.receipts.iter() {
            let shard_id = receipt.1.to_shard_id;
            self.receipts.entry(shard_id).or_insert_with(|| receipt.clone());
        }
    }
}

impl EncodedChunksCache {
    pub fn new() -> Self {
        EncodedChunksCache {
            largest_seen_height: 0,
            encoded_chunks: HashMap::new(),
            height_map: HashMap::new(),
            block_hash_to_chunk_headers: SizedCache::with_size(NUM_BLOCK_HASH_TO_CHUNK_HEADER),
        }
    }

    pub fn get(&self, chunk_hash: &ChunkHash) -> Option<&EncodedChunksCacheEntry> {
        self.encoded_chunks.get(&chunk_hash)
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) -> Option<EncodedChunksCacheEntry> {
        self.encoded_chunks.remove(&chunk_hash)
    }

    pub fn insert(&mut self, chunk_hash: ChunkHash, entry: EncodedChunksCacheEntry) {
        self.height_map
            .entry(entry.header.height_created())
            .or_insert_with(|| HashSet::default())
            .insert(chunk_hash.clone());
        self.encoded_chunks.insert(chunk_hash, entry);
    }

    // `chunk_header` must be `Some` if the entry is absent, caller must ensure that
    pub fn get_or_insert_from_header(
        &mut self,
        chunk_hash: ChunkHash,
        chunk_header: &ShardChunkHeader,
    ) -> &mut EncodedChunksCacheEntry {
        self.encoded_chunks
            .entry(chunk_hash)
            .or_insert_with(|| EncodedChunksCacheEntry::from_chunk_header(chunk_header.clone()))
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

    pub fn merge_in_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: &PartialEncodedChunkV2,
    ) {
        let chunk_hash = partial_encoded_chunk.header.chunk_hash();
        let entry =
            self.get_or_insert_from_header(chunk_hash.clone(), &partial_encoded_chunk.header);
        let height = entry.header.height_created();
        entry.merge_in_partial_encoded_chunk(&partial_encoded_chunk);
        self.height_map.entry(height).or_insert_with(|| HashSet::default()).insert(chunk_hash);
    }

    pub fn remove_from_cache_if_outside_horizon(&mut self, chunk_hash: &ChunkHash) {
        if let Some(entry) = self.encoded_chunks.get(chunk_hash) {
            let height = entry.header.height_created();
            if !self.height_within_horizon(height) {
                self.encoded_chunks.remove(chunk_hash);
            }
        }
    }

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
                        self.encoded_chunks.remove(&chunk_hash);
                    }
                }
            }
        }
    }

    pub fn insert_chunk_header(&mut self, shard_id: ShardId, header: ShardChunkHeader) {
        let height = header.height_created();
        if height >= self.largest_seen_height.saturating_sub(CHUNK_HEADER_HEIGHT_HORIZON)
            && height <= self.largest_seen_height + MAX_HEIGHTS_AHEAD
        {
            let prev_block_hash = header.prev_block_hash();
            let mut block_hash_to_chunk_headers = self
                .block_hash_to_chunk_headers
                .cache_remove(&prev_block_hash)
                .unwrap_or_else(|| HashMap::new());
            block_hash_to_chunk_headers.insert(shard_id, header);
            self.block_hash_to_chunk_headers
                .cache_set(prev_block_hash, block_hash_to_chunk_headers);
        }
    }

    pub fn get_chunk_headers_for_block(
        &mut self,
        prev_block_hash: &CryptoHash,
    ) -> HashMap<ShardId, ShardChunkHeader> {
        self.block_hash_to_chunk_headers
            .cache_remove(prev_block_hash)
            .unwrap_or_else(|| HashMap::new())
    }

    pub fn num_chunks_for_block(&mut self, prev_block_hash: &CryptoHash) -> ShardId {
        self.block_hash_to_chunk_headers
            .cache_get(prev_block_hash)
            .map(|x| x.len() as ShardId)
            .unwrap_or_else(|| 0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use near_crypto::KeyType;
    use near_primitives::hash::CryptoHash;
    use near_primitives::sharding::{PartialEncodedChunkV2, ShardChunkHeader, ShardChunkHeaderV2};
    use near_primitives::validator_signer::InMemoryValidatorSigner;

    use crate::chunk_cache::EncodedChunksCache;
    use crate::ChunkRequestInfo;

    #[test]
    fn test_cache_removal() {
        let mut cache = EncodedChunksCache::new();
        let signer =
            InMemoryValidatorSigner::from_random("test".parse().unwrap(), KeyType::ED25519);
        let partial_encoded_chunk = PartialEncodedChunkV2 {
            header: ShardChunkHeader::V2(ShardChunkHeaderV2::new(
                CryptoHash::default(),
                CryptoHash::default(),
                CryptoHash::default(),
                CryptoHash::default(),
                1,
                1,
                0,
                0,
                0,
                0,
                CryptoHash::default(),
                CryptoHash::default(),
                vec![],
                &signer,
            )),
            parts: vec![],
            receipts: vec![],
        };
        cache.merge_in_partial_encoded_chunk(&partial_encoded_chunk);
        cache.update_largest_seen_height::<ChunkRequestInfo>(2000, &HashMap::default());
        assert!(cache.encoded_chunks.is_empty());
        assert!(cache.height_map.is_empty());
    }
}
