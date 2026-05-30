use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};
use crate::{DBCol, Store, StoreUpdate};
use near_chain_primitives::Error;
use near_primitives::chunk_apply_stats::ChunkApplyStats;
use near_primitives::errors::ChunkAccessError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardUId, get_block_shard_uid};
use near_primitives::sharding::{ChunkHash, EncodedShardChunk, PartialEncodedChunk, ShardChunk};
use near_primitives::spice::state_witness::SpiceChunkStateWitness;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::utils::{
    get_block_shard_id, get_contract_accesses_key, get_witnesses_key, index_to_bytes,
};
use std::collections::HashSet;
use std::io;
use std::sync::Arc;

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

    pub fn store_update(&self) -> ChunkStoreUpdateAdapter<'static> {
        ChunkStoreUpdateAdapter {
            store_update: StoreUpdateHolder::Owned(self.store.store_update()),
        }
    }

    pub fn get_partial_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Arc<PartialEncodedChunk>, ChunkAccessError> {
        self.store
            .caching_get_ser(DBCol::PartialChunks, chunk_hash.as_ref())
            .ok_or_else(|| ChunkAccessError::ChunkMissing(chunk_hash.clone()))
    }

    pub fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<ShardChunk, ChunkAccessError> {
        self.store
            .get_ser(DBCol::Chunks, chunk_hash.as_ref())
            .ok_or_else(|| ChunkAccessError::ChunkMissing(chunk_hash.clone()))
    }

    /// Does this partial chunk exist?
    pub fn partial_chunk_exists(&self, h: &ChunkHash) -> bool {
        self.store.exists(DBCol::PartialChunks, h.as_ref())
    }

    /// Does this chunk exist?
    pub fn chunk_exists(&self, h: &ChunkHash) -> bool {
        self.store.exists(DBCol::Chunks, h.as_ref())
    }

    /// Returns a HashSet of Chunk Hashes for current Height
    pub fn get_all_chunk_hashes_by_height(&self, height: BlockHeight) -> HashSet<ChunkHash> {
        self.store.get_ser(DBCol::ChunkHashesByHeight, &index_to_bytes(height)).unwrap_or_default()
    }

    /// Returns encoded chunk if it's invalid otherwise None.
    pub fn is_invalid_chunk(&self, chunk_hash: &ChunkHash) -> Option<Arc<EncodedShardChunk>> {
        self.store.get_ser(DBCol::InvalidChunks, chunk_hash.as_ref())
    }

    /// Information from applying chunk.
    pub fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        option_to_not_found(
            self.store
                .caching_get_ser(DBCol::ChunkExtra, &get_block_shard_uid(block_hash, shard_uid)),
            format_args!("CHUNK EXTRA: {}:{:?}", block_hash, shard_uid),
        )
    }

    pub fn get_chunk_apply_stats(
        &self,
        block_hash: &CryptoHash,
        shard_id: &ShardId,
    ) -> Option<ChunkApplyStats> {
        self.store.get_ser(DBCol::ChunkApplyStats, &get_block_shard_id(block_hash, *shard_id))
    }

    pub fn get_witness(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Option<SpiceChunkStateWitness> {
        let key = get_witnesses_key(block_hash, shard_id);
        self.store.get_ser(DBCol::witnesses(), &key)
    }

    pub fn get_contract_accesses(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Option<HashSet<CodeHash>> {
        let key = get_contract_accesses_key(block_hash, shard_id);
        let accesses: Arc<Vec<CodeHash>> =
            self.store.caching_get_ser(DBCol::contract_accesses(), &key)?;
        Some(accesses.iter().cloned().collect())
    }
}

pub struct ChunkStoreUpdateAdapter<'a> {
    store_update: StoreUpdateHolder<'a>,
}

impl Into<StoreUpdate> for ChunkStoreUpdateAdapter<'static> {
    fn into(self) -> StoreUpdate {
        self.store_update.into()
    }
}

impl ChunkStoreUpdateAdapter<'static> {
    pub fn commit(self) -> io::Result<()> {
        let store_update: StoreUpdate = self.into();
        store_update.commit();
        Ok(())
    }
}

impl<'a> StoreUpdateAdapter for ChunkStoreUpdateAdapter<'a> {
    fn store_update(&mut self) -> &mut StoreUpdate {
        &mut self.store_update
    }
}

impl<'a> ChunkStoreUpdateAdapter<'a> {
    pub fn new(store_update: &'a mut StoreUpdate) -> Self {
        Self { store_update: StoreUpdateHolder::Reference(store_update) }
    }

    pub fn set_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
        chunk_extra: &ChunkExtra,
    ) {
        self.store_update.set_ser(
            DBCol::ChunkExtra,
            &get_block_shard_uid(block_hash, shard_uid),
            chunk_extra,
        );
    }

    pub fn set_chunk_apply_stats(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        stats: &ChunkApplyStats,
    ) {
        self.store_update.set_ser(
            DBCol::ChunkApplyStats,
            &get_block_shard_id(block_hash, shard_id),
            stats,
        );
    }

    pub fn set_witness(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        witness: &SpiceChunkStateWitness,
    ) {
        let key = get_witnesses_key(block_hash, shard_id);
        self.store_update.set_ser(DBCol::witnesses(), &key, witness);
    }

    pub fn set_contract_accesses(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        contract_accesses: &HashSet<CodeHash>,
    ) {
        let key = get_contract_accesses_key(block_hash, shard_id);
        let value: Vec<CodeHash> = contract_accesses.iter().cloned().collect();
        self.store_update.set_ser(DBCol::contract_accesses(), &key, &value);
    }
}

fn option_to_not_found<T, F>(res: Option<T>, field_name: F) -> Result<T, Error>
where
    F: std::string::ToString,
{
    res.ok_or_else(|| Error::DBNotFoundErr(field_name.to_string()))
}
