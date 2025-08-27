use std::sync::Arc;

use near_primitives::errors::ChunkAccessError;
use near_primitives::sharding::{ChunkHash, PartialEncodedChunk, ShardChunk};
use near_primitives::types::BlockHeight;
use near_primitives::utils::height_hash_to_bytes;

use crate::{DBCol, Store};

use super::StoreAdapter;

#[derive(Clone)]
pub struct ChunkStoreAdapter {
    store: Store,
}

impl StoreAdapter for ChunkStoreAdapter {
    fn store_ref(&self) -> &Store {
        &self.store
    }
}

impl ChunkStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn get_partial_chunk(
        &self,
        chunk_height: BlockHeight,
        chunk_hash: &ChunkHash,
    ) -> Result<Arc<PartialEncodedChunk>, ChunkAccessError> {
        let key = height_hash_to_bytes(chunk_height, &chunk_hash.0);
        self.store
            .caching_get_ser(DBCol::PartialChunks, &key)
            .expect("Borsh should not have failed here")
            .ok_or_else(|| ChunkAccessError::ChunkMissing(chunk_hash.clone()))
    }

    pub fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<ShardChunk, ChunkAccessError> {
        self.store
            .get_ser(DBCol::Chunks, chunk_hash.as_ref())
            .expect("Borsh should not have failed here")
            .ok_or_else(|| ChunkAccessError::ChunkMissing(chunk_hash.clone()))
    }
}
