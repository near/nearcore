use super::StoreAdapter;
use crate::{DBCol, Store, get_genesis_height};
use near_chain_primitives::Error;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::utils::index_to_bytes;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

#[derive(Clone)]
pub struct BlockStoreAdapter {
    store: Store,
}

impl StoreAdapter for BlockStoreAdapter {
    fn store_ref(&self) -> &Store {
        &self.store
    }
}

impl BlockStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Get full block.
    pub fn get_block(&self, block_hash: &CryptoHash) -> Result<Arc<Block>, Error> {
        option_to_not_found(
            self.store.caching_get_ser(DBCol::Block, block_hash.as_ref()),
            format_args!("BLOCK: {}", block_hash),
        )
    }

    /// Returns a number of references for Block with `block_hash`
    pub fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::BlockRefCount, block_hash.as_ref()),
            format_args!("BLOCK REFCOUNT: {}", block_hash),
        )
    }

    /// Does this full block exist?
    pub fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(DBCol::Block, h.as_ref()).map_err(|e| e.into())
    }

    /// Get block header.
    pub fn get_block_header(&self, h: &CryptoHash) -> Result<Arc<BlockHeader>, Error> {
        option_to_not_found(
            self.store.caching_get_ser(DBCol::BlockHeader, h.as_ref()),
            format_args!("BLOCK HEADER: {}", h),
        )
    }

    /// Get block height.
    pub fn get_block_height(&self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            let genesis_height = get_genesis_height(&self.store)
                .expect("Store failed on fetching genesis height")
                .expect("Genesis height not found in storage");
            Ok(genesis_height)
        } else {
            Ok(self.get_block_header(hash)?.height())
        }
    }

    /// Get previous header.
    pub fn get_previous_header(&self, header: &BlockHeader) -> Result<Arc<BlockHeader>, Error> {
        self.get_block_header(header.prev_hash())
    }

    /// Returns hash of the block on the main chain for given height.
    pub fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.caching_get_ser(DBCol::BlockHeight, &index_to_bytes(height)),
            format_args!("BLOCK HEIGHT: {}", height),
        )
        .map(|v| *v)
    }

    /// Returns a hashmap of epoch id -> set of all blocks got for current (height, epoch_id)
    pub fn get_all_block_hashes_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<Arc<HashMap<EpochId, HashSet<CryptoHash>>>, Error> {
        Ok(self.store.get_ser(DBCol::BlockPerHeight, &index_to_bytes(height))?.unwrap_or_default())
    }

    /// Returns a HashSet of Header Hashes for current Height
    pub fn get_all_header_hashes_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<HashSet<CryptoHash>, Error> {
        Ok(self
            .store
            .get_ser(DBCol::HeaderHashesByHeight, &index_to_bytes(height))?
            .unwrap_or_default())
    }

    /// Returns block header from the current chain for given height if present.
    pub fn get_block_header_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<Arc<BlockHeader>, Error> {
        let hash = self.get_block_hash_by_height(height)?;
        self.get_block_header(&hash)
    }

    pub fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::NextBlockHashes, hash.as_ref()),
            format_args!("NEXT BLOCK HASH: {}", hash),
        )
    }
}

fn option_to_not_found<T, F>(res: io::Result<Option<T>>, field_name: F) -> Result<T, Error>
where
    F: std::string::ToString,
{
    match res {
        Ok(Some(o)) => Ok(o),
        Ok(None) => Err(Error::DBNotFoundErr(field_name.to_string())),
        Err(e) => Err(e.into()),
    }
}
