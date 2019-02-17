use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use log::info;
use primitives::hash::CryptoHash;
use primitives::types::BlockId;
use primitives::utils::index_to_bytes;
use serde_derive::{Deserialize, Serialize};
use storage::GenericStorage;

use primitives::block_traits::{SignedBlock, SignedHeader};

/// General BlockChain container.
pub struct BlockChain<H, B, S> {
    /// Storage backend.
    storage: RwLock<Arc<S>>,
    // TODO: Add fork choice rule, tracking of finalized, pending, etc blocks.
}

impl<H, B, S> BlockChain<H, B, S>
where
    H: SignedHeader,
    B: SignedBlock<SignedHeader = H>,
    S: GenericStorage<H, B>,
{
    pub fn new(genesis: B, storage: Arc<S>) -> Self {
        let bc = Self { storage: RwLock::new(storage) };
        bc.storage
            .write()
            .blockchain_storage_mut()
            .set_genesis(genesis)
            .expect("Failed to initialize storage from genesis");
        bc
    }

    pub fn best_block(&self) -> B {
        let mut guard = self.storage.write();
        let hash = guard.blockchain_storage_mut().best_block_hash().unwrap().unwrap();
        guard.blockchain_storage_mut().block(hash).unwrap().unwrap()
    }

    #[inline]
    pub fn best_index(&self) -> u64 {
        self.best_block().header().index()
    }

    #[inline]
    pub fn best_hash(&self) -> CryptoHash {
        self.storage.write().blockchain_storage_mut().best_block_hash().unwrap().unwrap().clone()
    }

    /// Check if block already is known.
    pub fn is_known(&self, hash: &CryptoHash) -> bool {
        match self.storage.write().blockchain_storage_mut().header(hash) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    fn update_best_block(&self, block: B) {
        let hash = block.block_hash();
        let mut guard = self.storage.write();
        guard.blockchain_storage_mut().set_best_block_hash(hash.clone());
        guard.blockchain_storage_mut().add_block(block).unwrap();
    }

    /// Inserts a verified block.
    /// Returns true if block is disconnected.
    pub fn insert_block(&self, block: B) -> bool {
        let block_hash = block.block_hash();
        if self.is_known(&block_hash) {
            // TODO: known header but not known block.
            return false;
        }

        let maybe_parent = self
            .storage
            .write()
            .blockchain_storage_mut()
            .block(&block.header().parent_hash())
            .unwrap();
        if maybe_parent.is_none() {
            return false;
        }
        self.update_best_block(block);
        true
    }

    fn get_block_hash_by_index(&self, index: u64) -> Option<CryptoHash> {
        read_with_cache(
            &self.storage,
            storage::COL_BLOCK_INDEX,
            &self.index_to_hash,
            &index_to_bytes(index),
        )
    }

    pub fn get_block(&self, id: &BlockId) -> Option<B> {
        let hash = match id {
            BlockId::Number(idx) => self.get_block_hash_by_index(*idx)?,
            BlockId::Hash(h) => h,
        };
        self.storage.write().blockchain_storage_mut().block(&hash).unwrap()
    }

    fn get_block_header_by_hash(&self, block_hash: &CryptoHash) -> Option<H> {
        read_with_cache(&self.storage, storage::COL_HEADERS, &self.headers, block_hash.as_ref())
    }

    pub fn get_header(&self, id: &BlockId) -> Option<H> {
        let hash = match id {
            BlockId::Number(idx) => self.get_block_hash_by_index(*idx)?,
            BlockId::Hash(h) => h,
        };
        self.storage.write().blockchain_storage_mut().header(&hash).unwrap()
    }

    pub fn get_blocks_by_index(&self, start: u64, limit: u64) -> Result<Vec<B>, String> {
        let mut blocks = vec![];
        for i in start..=(start + limit) {
            if let Some(block) = self.get_block(&BlockId::Number(i)) {
                blocks.push(block)
            }
        }
        Ok(blocks)
    }
}
