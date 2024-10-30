use std::sync::Arc;

use near_primitives::errors::ChunkAccessError;
use near_primitives::sharding::{ChunkHash, PartialEncodedChunk, ShardChunk};

use crate::{DBCol, Store};

use super::StoreAdapter;

#[derive(Clone)]
pub struct ChunkStoreAdapter {
    store: Store,
}

impl StoreAdapter for ChunkStoreAdapter {
    fn store(&self) -> Store {
        self.store.clone()
    }
}

impl ChunkStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn get_partial_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Arc<PartialEncodedChunk>, ChunkAccessError> {
        self.store
            .get_ser(DBCol::PartialChunks, chunk_hash.as_ref())
            .expect("Borsh should not have failed here")
            .ok_or_else(|| ChunkAccessError::ChunkMissing(chunk_hash.clone()))
    }

    pub fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<ShardChunk>, ChunkAccessError> {
        self.store
            .get_ser(DBCol::Chunks, chunk_hash.as_ref())
            .expect("Borsh should not have failed here")
            .ok_or_else(|| ChunkAccessError::ChunkMissing(chunk_hash.clone()))
    }
}
