use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use cached::SizedCache;

use near_store::{
    read_with_cache, Store, StoreUpdate, COL_BLOCK, COL_BLOCK_HEADER, COL_BLOCK_MISC, COL_STATE_REF,
};
use primitives::hash::CryptoHash;
use primitives::types::MerkleHash;

use crate::error::{Error, ErrorKind};
use crate::types::{Block, BlockHeader, Tip};

const HEAD_KEY: &[u8; 4] = b"HEAD";
const TAIL_KEY: &[u8; 4] = b"TAIL";
const SYNC_HEAD_KEY: &[u8; 9] = b"SYNC_HEAD";
const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";

/// lru cache size
const CACHE_SIZE: usize = 20;

/// All chain-related database operations.
pub struct ChainStore {
    store: Arc<Store>,
    /// Cache with headers.
    headers: SizedCache<Vec<u8>, BlockHeader>,
    /// Cache with blocks.
    blocks: SizedCache<Vec<u8>, Block>,
    /// Cache with state roots.
    post_state_roots: SizedCache<Vec<u8>, MerkleHash>,
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
        ChainStore {
            store,
            blocks: SizedCache::with_size(CACHE_SIZE),
            headers: SizedCache::with_size(CACHE_SIZE),
            post_state_roots: SizedCache::with_size(CACHE_SIZE),
        }
    }

    pub fn store_update(&mut self) -> ChainStoreUpdate {
        ChainStoreUpdate::new(self, self.store.store_update())
    }

    /// The chain head.
    pub fn head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, HEAD_KEY), "HEAD")
    }

    /// The chain tail (as far as chain goes).
    pub fn tail(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, TAIL_KEY), "TAIL")
    }

    /// The "sync" head: last header we received from syncing.
    pub fn sync_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, SYNC_HEAD_KEY), "SYNC_HEAD")
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    pub fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.get_block_header(&self.head()?.last_block_hash)
    }

    /// Head of the header chain (not the same thing as head_header).
    pub fn header_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, HEADER_HEAD_KEY), "HEADER_HEAD")
    }

    /// Get full block.
    pub fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, COL_BLOCK, &mut self.blocks, h.as_ref()),
            &format!("BLOCK: {}", h),
        )
    }

    /// Does this full block exist?
    pub fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(COL_BLOCK, h.as_ref()).map_err(|e| e.into())
    }

    /// Get previous header.
    pub fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
    }

    /// Get state root hash after applying header with given hash.
    pub fn get_post_state_root(&mut self, h: &CryptoHash) -> Result<&MerkleHash, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, COL_STATE_REF, &mut self.post_state_roots, h.as_ref()),
            &format!("STATE ROOT: {}", h),
        )
    }

    /// Get block header.
    pub fn get_block_header(&mut self, h: &CryptoHash) -> Result<&BlockHeader, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, COL_BLOCK_HEADER, &mut self.headers, h.as_ref()),
            &format!("BLOCK HEADER: {}", h),
        )
    }
}

/// Provides layer to update chain without touching underlaying database.
/// This serves few purposes, main one is that even if executable exists/fails during update the database is in consistent state.
pub struct ChainStoreUpdate<'a> {
    chain_store: &'a mut ChainStore,
    store_update: StoreUpdate,
    /// Blocks added during this update. Takes ownership (unclear how to not do it because of failure exists).
    blocks: HashMap<CryptoHash, Block>,
    headers: HashMap<CryptoHash, BlockHeader>,
    post_state_roots: HashMap<CryptoHash, MerkleHash>,
    head: Option<Tip>,
    tail: Option<Tip>,
    header_head: Option<Tip>,
    sync_head: Option<Tip>,
}

impl<'a> ChainStoreUpdate<'a> {
    pub fn new(chain_store: &'a mut ChainStore, store_update: StoreUpdate) -> Self {
        ChainStoreUpdate {
            chain_store,
            store_update,
            blocks: HashMap::default(),
            headers: HashMap::default(),
            post_state_roots: HashMap::default(),
            head: None,
            tail: None,
            header_head: None,
            sync_head: None,
        }
    }

    /// The chain head.
    pub fn head(&self) -> Result<Tip, Error> {
        if let Some(head) = &self.head {
            Ok(head.clone())
        } else {
            self.chain_store.head()
        }
    }

    /// The chain tail (as far as chain goes).
    pub fn tail(&mut self) -> Result<Tip, Error> {
        if let Some(tail) = &self.tail {
            Ok(tail.clone())
        } else {
            self.chain_store.tail()
        }
    }

    /// The "sync" head: last header we received from syncing.
    pub fn sync_head(&mut self) -> Result<Tip, Error> {
        if let Some(sync_head) = &self.sync_head {
            Ok(sync_head.clone())
        } else {
            self.chain_store.sync_head()
        }
    }

    /// Head of the header chain (not the same thing as head_header).
    pub fn header_head(&mut self) -> Result<Tip, Error> {
        if let Some(header_head) = &self.header_head {
            Ok(header_head.clone())
        } else {
            self.chain_store.header_head()
        }
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    pub fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.get_block_header(&(self.head()?.last_block_hash))
    }

    /// Get full block.
    pub fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error> {
        if let Some(block) = self.blocks.get(h) {
            Ok(block)
        } else {
            self.chain_store.get_block(h)
        }
    }

    /// Does this full block exist?
    pub fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        Ok(self.blocks.contains_key(h) || self.chain_store.block_exists(h)?)
    }

    /// Get previous header.
    pub fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
    }

    /// Get state root hash after applying header with given hash.
    pub fn get_post_state_root(&mut self, h: &CryptoHash) -> Result<&MerkleHash, Error> {
        if let Some(post_state_root) = self.post_state_roots.get(h) {
            Ok(post_state_root)
        } else {
            self.chain_store.get_post_state_root(h)
        }
    }

    /// Get block header.
    pub fn get_block_header(&mut self, h: &CryptoHash) -> Result<&BlockHeader, Error> {
        if let Some(header) = self.headers.get(h) {
            Ok(header)
        } else {
            self.chain_store.get_block_header(h)
        }
    }

    /// Update both header and block body head.
    pub fn save_head(&mut self, t: &Tip) {
        self.save_body_head(t);
        self.save_header_head(t);
    }

    /// Update block body head.
    pub fn save_body_head(&mut self, t: &Tip) {
        self.head = Some(t.clone());
    }

    /// Update block body tail.
    pub fn save_body_tail(&mut self, t: &Tip) {
        self.tail = Some(t.clone());
    }

    /// Update header head.
    pub fn save_header_head(&mut self, t: &Tip) {
        self.header_head = Some(t.clone());
    }

    /// Save "sync" head.
    pub fn save_sync_head(&mut self, t: &Tip) {
        self.sync_head = Some(t.clone());
    }

    /// Save block.
    pub fn save_block(&mut self, block: Block) {
        self.blocks.insert(block.hash(), block);
    }

    /// Save post applying block state root.
    pub fn save_post_state_root(&mut self, hash: &CryptoHash, state_root: &CryptoHash) {
        self.post_state_roots.insert(*hash, *state_root);
    }

    pub fn delete_block(&mut self, hash: &CryptoHash) {
        // TODO: figure out if we need to cache this?
        self.store_update.delete(COL_BLOCK, hash.as_ref());
    }

    pub fn save_block_header(&mut self, header: BlockHeader) {
        self.headers.insert(header.hash(), header);
    }

    /// Merge another StoreUpdate into this one
    pub fn merge(&mut self, store_update: StoreUpdate) {
        self.store_update.merge(store_update);
    }

    pub fn finalize(mut self) -> Result<StoreUpdate, Error> {
        if let Some(t) = self.head {
            self.store_update
                .set_ser(COL_BLOCK_MISC, HEAD_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.tail {
            self.store_update
                .set_ser(COL_BLOCK_MISC, TAIL_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.header_head {
            self.store_update
                .set_ser(COL_BLOCK_MISC, HEADER_HEAD_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.sync_head {
            self.store_update
                .set_ser(COL_BLOCK_MISC, SYNC_HEAD_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, block) in self.blocks.drain() {
            self.store_update
                .set_ser(COL_BLOCK, hash.as_ref(), &block)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, header) in self.headers.drain() {
            self.store_update
                .set_ser(COL_BLOCK_HEADER, hash.as_ref(), &header)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, state_root) in self.post_state_roots.drain() {
            self.store_update
                .set_ser(COL_STATE_REF, hash.as_ref(), &state_root)
                .map_err::<Error, _>(|e| e.into())?;
        }
        Ok(self.store_update)
    }
}
