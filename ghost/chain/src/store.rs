use kvdb::{DBTransaction, KeyValueDB};
use primitives::hash::CryptoHash;
use primitives::serialize::{Decode, Encode};
use serde::de::DeserializeOwned;
use std::io;
use std::sync::Arc;

use crate::chain::Chain;
use crate::error::{Error, ErrorKind};
use crate::types::{Block, BlockHeader, Tip};

const COL_BLOCK_MISC: Option<u32> = Some(0);
const COL_BLOCK: Option<u32> = Some(1);
const COL_BLOCK_HEADER: Option<u32> = Some(2);

const HEAD_KEY: &[u8; 4] = b"HEAD";
const TAIL_KEY: &[u8; 4] = b"TAIL";
const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";

pub struct Store {
    storage: Arc<KeyValueDB>,
}

impl Store {
    pub fn new(storage: Arc<KeyValueDB>) -> Store {
        Store { storage }
    }

    pub fn get_ser<T>(&self, column: Option<u32>, key: &[u8]) -> Result<Option<T>, io::Error> {
        match self.storage.get(column, key) {
            Ok(Some(bytes)) => {
                unimplemented!();
                // Decode::decode(&bytes.to_vec())
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn exists(&self, column: Option<u32>, key: &[u8]) -> Result<bool, Error> {
        match self.storage.get(column, key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate::new(self.storage.clone())
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    storage: Arc<KeyValueDB>,
    transaction: DBTransaction,
}

impl StoreUpdate {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction }
    }

    pub fn set(&mut self, column: Option<u32>, key: &[u8], value: &[u8]) {
        self.transaction.put(column, key, value)
    }

    pub fn set_ser<T>(&mut self, column: Option<u32>, key: &[u8], value: &T) -> Result<(), Error> {
        // TODO serialize
        self.set(column, key, &vec![]);
        Ok(())
    }

    pub fn delete(&mut self, column: Option<u32>, key: &[u8]) {
        self.transaction.delete(column, key);
    }

    pub fn commit(mut self) -> Result<(), Error> {
        self.storage.write(self.transaction).map_err(|e| e.into())
    }
}

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
        self.store.exists(COL_BLOCK, h.as_ref())
    }

    /// Get previous header.
    pub fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
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

    pub fn save_body_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK_MISC, HEAD_KEY, t)
    }

    pub fn save_body_tail(&mut self, t: &Tip) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK_MISC, TAIL_KEY, t)
    }

    pub fn save_header_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK_MISC, HEADER_HEAD_KEY, t)
    }

    pub fn save_block(&mut self, block: &Block) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK, block.hash().as_ref(), block)
    }

    pub fn delete_block(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        self.store_update.delete(COL_BLOCK, block_hash.as_ref());
        Ok(())
    }

    pub fn save_block_header(&mut self, header: &BlockHeader) -> Result<(), Error> {
        self.store_update.set_ser(COL_BLOCK_HEADER, header.hash().as_ref(), header)
    }

    pub fn finalize(mut self) -> StoreUpdate {
        self.store_update
    }
}
