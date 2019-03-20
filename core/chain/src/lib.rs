use std::sync::{Arc, RwLock};

use primitives::hash::CryptoHash;
use primitives::types::BlockId;
use primitives::block_traits::{SignedBlock, SignedHeader};
use storage::GenericStorage;
use std::marker::PhantomData;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub mod test_utils;

/// General BlockChain container.
pub struct BlockChain<H, B, S> {
    /// Storage backend.
    storage: Arc<RwLock<S>>,
    // TODO: Add fork choice rule, tracking of finalized, pending, etc blocks.
    phantom_header: PhantomData<H>,
    phantom_block: PhantomData<B>,
}

impl<H, B, S> BlockChain<H, B, S>
where
    H: SignedHeader,
    B: SignedBlock<SignedHeader = H>,
    S: GenericStorage<H, B>,
{
    pub fn new(genesis: B, storage: Arc<RwLock<S>>) -> Self {
        let bc =
            Self { storage, phantom_header: Default::default(), phantom_block: Default::default() };
        bc.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .set_genesis(genesis)
            .expect("Failed to initialize storage from genesis");
        bc
    }

    pub fn genesis_hash(&self) -> CryptoHash {
        *self.storage.write().expect(POISONED_LOCK_ERR).blockchain_storage_mut().genesis_hash()
    }

    /// Returns best block, might be not available if only header was imported.
    pub fn best_block(&self) -> Option<B> {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        let hash = *guard.blockchain_storage_mut().best_block_hash().unwrap().unwrap();
        guard.blockchain_storage_mut().block(&hash).unwrap().cloned()
    }

    pub fn best_header(&self) -> B::SignedHeader {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        let hash = *guard.blockchain_storage_mut().best_block_hash().unwrap().unwrap();
        guard.blockchain_storage_mut().header(&hash).unwrap().unwrap().clone()
    }

    #[inline]
    pub fn best_index(&self) -> u64 {
        self.best_header().index()
    }

    #[inline]
    pub fn best_hash(&self) -> CryptoHash {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        *guard.blockchain_storage_mut().best_block_hash().unwrap().unwrap()
    }

    /// Check if block header already is known.
    pub fn is_known_header(&self, hash: &CryptoHash) -> bool {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        match guard.blockchain_storage_mut().header(hash) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    /// Check if block already is known.
    pub fn is_known_block(&self, hash: &CryptoHash) -> bool {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        match guard.blockchain_storage_mut().block(hash) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    fn update_best_block_header(&self, header: B::SignedHeader) {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        guard.blockchain_storage_mut().add_header(header).unwrap();
    }

    fn update_best_block(&self, block: B) {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        guard.blockchain_storage_mut().add_block(block).unwrap();
    }

    fn update_existing_header(&self, block: B) {
        let hash = block.block_hash();
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        guard.blockchain_storage_mut().set_block(&hash, block).unwrap();
    }

    /// Inserts a verified block.
    pub fn insert_block(&self, block: B) {
        let block_hash = block.block_hash();
        if self.is_known_header(&block_hash) {
            if ! self.is_known_block(&block_hash) {
                self.update_existing_header(block);
            }
            return;
        }

        if self
            .storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .block(&block.header().parent_hash())
            .unwrap()
            .is_none()
        {
            return;
        }
        self.update_best_block(block);
    }

    /// Inserts a verified header.
    pub fn insert_header(&self, header: B::SignedHeader) {
        let block_hash = header.block_hash();
        if self.is_known_header(&block_hash) {
            return;
        }

        if self
            .storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .header(&header.parent_hash())
            .unwrap()
            .is_none()
        {
            return;
        }
        self.update_best_block_header(header);
    }

    pub fn get_block(&self, id: &BlockId) -> Option<B> {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        let hash = *match id {
            BlockId::Number(idx) => guard.blockchain_storage_mut().hash_by_index(*idx).unwrap()?,
            BlockId::Hash(h) => h,
        };
        guard.blockchain_storage_mut().block(&hash).unwrap().cloned()
    }

    pub fn get_header(&self, id: &BlockId) -> Option<H> {
        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        let hash = *match id {
            BlockId::Number(idx) => guard.blockchain_storage_mut().hash_by_index(*idx).unwrap()?,
            BlockId::Hash(h) => h,
        };
        guard.blockchain_storage_mut().header(&hash).unwrap().cloned()
    }

    pub fn get_blocks_by_indices(&self, start: u64, limit: u64) -> Vec<B> {
        let mut blocks = vec![];
        for i in start..=(start + limit) {
            if let Some(block) = self.get_block(&BlockId::Number(i)) {
                blocks.push(block)
            }
        }
        blocks
    }

    pub fn get_headers_by_indices(&self, start: u64, limit: u64) -> Vec<B::SignedHeader> {
        let mut headers = vec![];
        for i in start..=(start + limit) {
            if let Some(header) = self.get_header(&BlockId::Number(i)) {
                headers.push(header)
            }
        }
        headers
    }
}
