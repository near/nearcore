use std::io;
use std::sync::Arc;

use near_store::{Store, StoreUpdate, COL_BLOCK, COL_BLOCK_HEADER, COL_BLOCK_MISC, COL_STATE_REF};
use primitives::hash::CryptoHash;

use crate::error::{Error, ErrorKind};
use crate::types::{Block, BlockHeader, Tip};
use primitives::types::MerkleHash;

const HEAD_KEY: &[u8; 4] = b"HEAD";
const TAIL_KEY: &[u8; 4] = b"TAIL";
const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";

/// All chain-related database operations.
pub struct ChainStore {
    store: Arc<Store>,
}

pub fn option_to_not_found<T>(res: io::Result<Option<T>>, field_name: &str) -> Result<T, Error> {
    match res {
        Ok(Some(o)) => Ok(o),
        Ok(None) => Err(ErrorKind::DBNotFoundErr(field_name.to_owned()).into()),
        Err(e) => Err(e.into()),
    }
}

impl ChainStore {
    pub fn new(store: Arc<Store>) -> ChainStore {
        ChainStore { store }
    }

    pub fn store_update(&self) -> ChainStoreUpdate {
        ChainStoreUpdate::new(self.store.store_update())
    }

    /// The head.
    pub fn head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, HEAD_KEY), "HEAD")
    }

    /// The tail.
    pub fn tail(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, TAIL_KEY), "TAIL")
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    pub fn head_header(&self) -> Result<BlockHeader, Error> {
        self.get_block_header(&self.head()?.last_block_hash)
    }

    /// Head of the header chain (not the same thing as head_header).
    pub fn header_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, HEADER_HEAD_KEY), "HEADER_HEAD")
    }

    /// Get full block.
    pub fn get_block(&self, h: &CryptoHash) -> Result<Block, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK, h.as_ref()), &format!("BLOCK: {}", h))
    }

    /// Does this full block exist?
    pub fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(COL_BLOCK, h.as_ref()).map_err(|e| e.into())
    }

    /// Get previous header.
    pub fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
    }

    /// Get state root hash after applying header with given hash.
    pub fn get_post_state_root(&self, h: &CryptoHash) -> Result<MerkleHash, Error> {
        option_to_not_found(self.store.get_ser(COL_STATE_REF, h.as_ref()), &format!("STATE ROOT: {}", h))
    }

    /// Get block header.
    pub fn get_block_header(&self, h: &CryptoHash) -> Result<BlockHeader, Error> {
        option_to_not_found(
            self.store.get_ser(COL_BLOCK_HEADER, h.as_ref()),
            &format!("BLOCK HEADER: {}", h),
        )
    }
}

pub struct ChainStoreUpdate {
    store_update: StoreUpdate,
}

impl ChainStoreUpdate {
    pub fn new(store_update: StoreUpdate) -> Self {
        ChainStoreUpdate { store_update }
    }

    /// Update both header and block body head.
    pub fn save_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.save_body_head(t)?;
        self.save_header_head(t)
    }

    /// Update block body head.
    pub fn save_body_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK_MISC, HEAD_KEY, t).map_err(|e| e.into())
    }

    /// Update block body tail.
    pub fn save_body_tail(&mut self, t: &Tip) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK_MISC, TAIL_KEY, t).map_err(|e| e.into())
    }

    /// Update header head.
    pub fn save_header_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK_MISC, HEADER_HEAD_KEY, t).map_err(|e| e.into())
    }

    /// Save block.
    pub fn save_block(&mut self, block: &Block) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK, block.hash().as_ref(), block).map_err(|e| e.into())
    }

    /// Save post applying block state root.
    pub fn save_post_state_root(&mut self, hash: &CryptoHash, state_root: &CryptoHash) -> Result<(), Error> {
        self.store_update.set_ser(COL_STATE_REF, hash.as_ref(), state_root).map_err(|e| e.into())
    }

    pub fn delete_block(&mut self, hash: &CryptoHash) -> Result<(), Error> {
        self.store_update.delete(COL_BLOCK, hash.as_ref());
        Ok(())
    }

    pub fn save_block_header(&mut self, header: &BlockHeader) -> Result<(), Error> {
        self.store_update
            .set_ser(COL_BLOCK_HEADER, header.hash().as_ref(), header)
            .map_err(|e| e.into())
    }

    /// Merge another StoreUpdate into this one
    pub fn merge(&mut self, store_update: StoreUpdate) {
        self.store_update.merge(store_update);
    }

    pub fn finalize(self) -> StoreUpdate {
        self.store_update
    }
}
