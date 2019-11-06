use std::collections::HashMap;

use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunk, PartialEncodedChunkPart, ReceiptProof, ShardChunkHeader,
};
use near_primitives::types::{BlockIndex, ShardId};

const HEIGHT_HORIZON: u64 = 1024;
const MAX_HEIGHTS_AHEAD: u64 = 5;

pub struct EncodedChunksCacheEntry {
    pub header: ShardChunkHeader,
    pub parts: HashMap<u64, PartialEncodedChunkPart>,
    pub receipts: HashMap<ShardId, ReceiptProof>,
}

pub struct EncodedChunksCache {
    largest_seen_height: BlockIndex,

    encoded_chunks: HashMap<ChunkHash, EncodedChunksCacheEntry>,
    height_map: HashMap<BlockIndex, Vec<ChunkHash>>,
}

impl EncodedChunksCacheEntry {
    pub fn from_chunk_header(header: ShardChunkHeader) -> Self {
        EncodedChunksCacheEntry { header, parts: HashMap::new(), receipts: HashMap::new() }
    }

    pub fn merge_in_partial_encoded_chunk(&mut self, partial_encoded_chunk: &PartialEncodedChunk) {
        for part_info in partial_encoded_chunk.parts.iter() {
            let part_ord = part_info.part_ord;
            if !self.parts.contains_key(&part_ord) {
                self.parts.insert(part_ord, part_info.clone());
            }
        }

        for receipt in partial_encoded_chunk.receipts.iter() {
            let shard_id = receipt.1.to_shard_id;
            if !self.receipts.contains_key(&shard_id) {
                self.receipts.insert(shard_id, receipt.clone());
            }
        }
    }
}

impl EncodedChunksCache {
    pub fn new() -> Self {
        EncodedChunksCache {
            largest_seen_height: 0,
            encoded_chunks: HashMap::new(),
            height_map: HashMap::new(),
        }
    }

    pub fn get(&self, chunk_hash: &ChunkHash) -> Option<&EncodedChunksCacheEntry> {
        self.encoded_chunks.get(&chunk_hash)
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) -> Option<EncodedChunksCacheEntry> {
        self.encoded_chunks.remove(&chunk_hash)
    }

    pub fn insert(&mut self, chunk_hash: ChunkHash, entry: EncodedChunksCacheEntry) {
        self.encoded_chunks.insert(chunk_hash, entry);
    }

    // `chunk_header` must be `Some` if the entry is absent, caller must ensure that
    pub fn get_or_insert_from_header(
        &mut self,
        chunk_hash: ChunkHash,
        chunk_header: Option<&ShardChunkHeader>,
    ) -> &mut EncodedChunksCacheEntry {
        self.encoded_chunks.entry(chunk_hash).or_insert_with(|| {
            EncodedChunksCacheEntry::from_chunk_header(chunk_header.unwrap().clone())
        })
    }

    pub fn merge_in_partial_encoded_chunk(
        &mut self,
        partial_encoded_chunk: &PartialEncodedChunk,
    ) -> bool {
        let chunk_hash = partial_encoded_chunk.chunk_hash.clone();
        if self.encoded_chunks.contains_key(&chunk_hash) || partial_encoded_chunk.header.is_some() {
            if let Some(header) = &partial_encoded_chunk.header {
                let height = header.inner.height_created;

                if height + HEIGHT_HORIZON < self.largest_seen_height {
                    return false;
                }

                if height > self.largest_seen_height + MAX_HEIGHTS_AHEAD {
                    return false;
                }
            }

            self.get_or_insert_from_header(chunk_hash, partial_encoded_chunk.header.as_ref())
                .merge_in_partial_encoded_chunk(&partial_encoded_chunk);
            return true;
        } else {
            return false;
        }
    }

    pub fn update_largest_seen_height(&mut self, new_height: BlockIndex) {
        let old_largest_seen_height = self.largest_seen_height;
        self.largest_seen_height = new_height;
        for height in old_largest_seen_height.saturating_sub(HEIGHT_HORIZON)
            ..self.largest_seen_height.saturating_sub(HEIGHT_HORIZON)
        {
            if let Some(chunks_to_remove) = self.height_map.remove(&height) {
                for chunk_hash in chunks_to_remove {
                    self.encoded_chunks.remove(&chunk_hash);
                }
            }
        }
    }
}
