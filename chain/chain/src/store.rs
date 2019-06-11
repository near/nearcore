use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

use cached::SizedCache;
use log::debug;

use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{ReceiptTransaction, TransactionResult};
use near_primitives::types::{BlockIndex, MerkleHash};
use near_primitives::utils::index_to_bytes;
use near_store::{
    read_with_cache, Store, StoreUpdate, WrappedTrieChanges, COL_BLOCK, COL_BLOCK_HEADER,
    COL_BLOCK_INDEX, COL_BLOCK_MISC, COL_RECEIPTS, COL_STATE_REF, COL_TRANSACTION_RESULT,
};

use crate::error::{Error, ErrorKind};
use crate::types::{Block, BlockHeader, Tip};

const HEAD_KEY: &[u8; 4] = b"HEAD";
const TAIL_KEY: &[u8; 4] = b"TAIL";
const SYNC_HEAD_KEY: &[u8; 9] = b"SYNC_HEAD";
const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";

/// lru cache size
const CACHE_SIZE: usize = 20;

/// Accesses the chain store. Used to create atomic editable views that can be reverted.
pub trait ChainStoreAccess {
    /// Returns underlaying store.
    fn store(&self) -> &Store;
    /// The chain head.
    fn head(&self) -> Result<Tip, Error>;
    /// The chain tail (as far as chain goes).
    fn tail(&self) -> Result<Tip, Error>;
    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error>;
    /// The "sync" head: last header we received from syncing.
    fn sync_head(&self) -> Result<Tip, Error>;
    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error>;
    /// Get full block.
    fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error>;
    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error>;
    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error>;
    /// Get state root hash after applying header with given hash.
    fn get_post_state_root(&mut self, h: &CryptoHash) -> Result<&MerkleHash, Error>;
    /// Get block header.
    fn get_block_header(&mut self, h: &CryptoHash) -> Result<&BlockHeader, Error>;
    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&mut self, height: BlockIndex) -> Result<CryptoHash, Error>;
    /// Returns resulting receipt for given block.
    fn get_receipts(&mut self, hash: &CryptoHash) -> Result<&Vec<ReceiptTransaction>, Error>;
    /// Returns transaction result for given tx hash.
    fn get_transaction_result(&mut self, hash: &CryptoHash) -> Result<&TransactionResult, Error>;
}

/// All chain-related database operations.
pub struct ChainStore {
    store: Arc<Store>,
    /// Cache with headers.
    headers: SizedCache<Vec<u8>, BlockHeader>,
    /// Cache with blocks.
    blocks: SizedCache<Vec<u8>, Block>,
    /// Cache with state roots.
    post_state_roots: SizedCache<Vec<u8>, MerkleHash>,
    // Cache with index to hash on the main chain.
    // block_index: SizedCache<Vec<u8>, CryptoHash>,
    /// Cache with receipts.
    receipts: SizedCache<Vec<u8>, Vec<ReceiptTransaction>>,
    /// Cache transaction statuses.
    transaction_results: SizedCache<Vec<u8>, TransactionResult>,
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
            // block_index: SizedCache::with_size(CACHE_SIZE),
            receipts: SizedCache::with_size(CACHE_SIZE),
            transaction_results: SizedCache::with_size(CACHE_SIZE),
        }
    }

    pub fn store_update(&mut self) -> ChainStoreUpdate<Self> {
        ChainStoreUpdate::new(self)
    }
}

impl ChainStoreAccess for ChainStore {
    fn store(&self) -> &Store {
        &*self.store
    }
    /// The chain head.
    fn head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, HEAD_KEY), "HEAD")
    }

    /// The chain tail (as far as chain goes).
    fn tail(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, TAIL_KEY), "TAIL")
    }

    /// The "sync" head: last header we received from syncing.
    fn sync_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, SYNC_HEAD_KEY), "SYNC_HEAD")
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.get_block_header(&self.head()?.last_block_hash)
    }

    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(COL_BLOCK_MISC, HEADER_HEAD_KEY), "HEADER_HEAD")
    }

    /// Get full block.
    fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, COL_BLOCK, &mut self.blocks, h.as_ref()),
            &format!("BLOCK: {}", h),
        )
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(COL_BLOCK, h.as_ref()).map_err(|e| e.into())
    }

    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
    }

    /// Get state root hash after applying header with given hash.
    fn get_post_state_root(&mut self, h: &CryptoHash) -> Result<&MerkleHash, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, COL_STATE_REF, &mut self.post_state_roots, h.as_ref()),
            &format!("STATE ROOT: {}", h),
        )
    }

    /// Get block header.
    fn get_block_header(&mut self, h: &CryptoHash) -> Result<&BlockHeader, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, COL_BLOCK_HEADER, &mut self.headers, h.as_ref()),
            &format!("BLOCK HEADER: {}", h),
        )
    }

    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&mut self, height: BlockIndex) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.get_ser(COL_BLOCK_INDEX, &index_to_bytes(height)),
            &format!("BLOCK INDEX: {}", height),
        )
        // TODO: cache needs to be deleted when things get updated.
        //        option_to_not_found(
        //            read_with_cache(
        //                &*self.store,
        //                COL_BLOCK_INDEX,
        //                &mut self.block_index,
        //                &index_to_bytes(height),
        //            ),
        //            &format!("BLOCK INDEX: {}", height),
        //        )
    }

    fn get_receipts(&mut self, hash: &CryptoHash) -> Result<&Vec<ReceiptTransaction>, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, COL_RECEIPTS, &mut self.receipts, hash.as_ref()),
            &format!("RECEIPT: {}", hash),
        )
    }

    fn get_transaction_result(&mut self, hash: &CryptoHash) -> Result<&TransactionResult, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                COL_TRANSACTION_RESULT,
                &mut self.transaction_results,
                hash.as_ref(),
            ),
            &format!("TRANSACTION: {}", hash),
        )
    }
}

/// Provides layer to update chain without touching underlaying database.
/// This serves few purposes, main one is that even if executable exists/fails during update the database is in consistent state.
pub struct ChainStoreUpdate<'a, T> {
    chain_store: &'a mut T,
    store_updates: Vec<StoreUpdate>,
    /// Blocks added during this update. Takes ownership (unclear how to not do it because of failure exists).
    blocks: HashMap<CryptoHash, Block>,
    deleted_blocks: HashSet<CryptoHash>,
    headers: HashMap<CryptoHash, BlockHeader>,
    post_state_roots: HashMap<CryptoHash, MerkleHash>,
    block_index: HashMap<BlockIndex, Option<CryptoHash>>,
    receipts: HashMap<CryptoHash, Vec<ReceiptTransaction>>,
    transaction_results: HashMap<CryptoHash, TransactionResult>,
    head: Option<Tip>,
    tail: Option<Tip>,
    header_head: Option<Tip>,
    sync_head: Option<Tip>,
    trie_changes: Option<WrappedTrieChanges>,
}

impl<'a, T: ChainStoreAccess> ChainStoreUpdate<'a, T> {
    pub fn new(chain_store: &'a mut T) -> Self {
        ChainStoreUpdate {
            chain_store,
            store_updates: vec![],
            blocks: HashMap::default(),
            deleted_blocks: HashSet::default(),
            headers: HashMap::default(),
            block_index: HashMap::default(),
            post_state_roots: HashMap::default(),
            receipts: HashMap::default(),
            transaction_results: HashMap::default(),
            head: None,
            tail: None,
            header_head: None,
            sync_head: None,
            trie_changes: None,
        }
    }
}

impl<'a, T: ChainStoreAccess> ChainStoreAccess for ChainStoreUpdate<'a, T> {
    fn store(&self) -> &Store {
        self.chain_store.store()
    }
    /// The chain head.
    fn head(&self) -> Result<Tip, Error> {
        if let Some(head) = &self.head {
            Ok(head.clone())
        } else {
            self.chain_store.head()
        }
    }

    /// The chain tail (as far as chain goes).
    fn tail(&self) -> Result<Tip, Error> {
        if let Some(tail) = &self.tail {
            Ok(tail.clone())
        } else {
            self.chain_store.tail()
        }
    }

    /// The "sync" head: last header we received from syncing.
    fn sync_head(&self) -> Result<Tip, Error> {
        if let Some(sync_head) = &self.sync_head {
            Ok(sync_head.clone())
        } else {
            self.chain_store.sync_head()
        }
    }

    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error> {
        if let Some(header_head) = &self.header_head {
            Ok(header_head.clone())
        } else {
            self.chain_store.header_head()
        }
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.get_block_header(&(self.head()?.last_block_hash))
    }

    /// Get full block.
    fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error> {
        if let Some(block) = self.blocks.get(h) {
            Ok(block)
        } else {
            self.chain_store.get_block(h)
        }
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        Ok(self.blocks.contains_key(h) || self.chain_store.block_exists(h)?)
    }

    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
    }

    /// Get state root hash after applying header with given hash.
    fn get_post_state_root(&mut self, hash: &CryptoHash) -> Result<&MerkleHash, Error> {
        if let Some(post_state_root) = self.post_state_roots.get(hash) {
            Ok(post_state_root)
        } else {
            self.chain_store.get_post_state_root(hash)
        }
    }

    /// Get block header.
    fn get_block_header(&mut self, hash: &CryptoHash) -> Result<&BlockHeader, Error> {
        if let Some(header) = self.headers.get(hash) {
            Ok(header)
        } else {
            self.chain_store.get_block_header(hash)
        }
    }

    /// Get block header from the current chain by height.
    fn get_block_hash_by_height(&mut self, height: BlockIndex) -> Result<CryptoHash, Error> {
        self.chain_store.get_block_hash_by_height(height)
    }

    /// Get receipts produced for block with givien hash.
    fn get_receipts(&mut self, hash: &CryptoHash) -> Result<&Vec<ReceiptTransaction>, Error> {
        if let Some(receipts) = self.receipts.get(hash) {
            Ok(receipts)
        } else {
            self.chain_store.get_receipts(hash)
        }
    }

    fn get_transaction_result(&mut self, hash: &CryptoHash) -> Result<&TransactionResult, Error> {
        self.chain_store.get_transaction_result(hash)
    }
}

impl<'a, T: ChainStoreAccess> ChainStoreUpdate<'a, T> {
    /// Update both header and block body head.
    pub fn save_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.save_body_head(t);
        self.save_header_head(t)
    }

    /// Update block body head.
    pub fn save_body_head(&mut self, t: &Tip) {
        self.head = Some(t.clone());
    }

    /// Update block body tail.
    pub fn save_body_tail(&mut self, t: &Tip) {
        self.tail = Some(t.clone());
    }

    fn update_block_index(&mut self, height: BlockIndex, hash: CryptoHash) -> Result<(), Error> {
        let mut prev_hash = hash;
        let mut prev_height = height;
        loop {
            let header = self.get_block_header(&prev_hash)?;
            let (header_height, header_hash, header_prev_hash) =
                (header.height, header.hash(), header.prev_hash);
            // Clean up block indicies between blocks.
            for height in (header_height + 1)..prev_height {
                self.block_index.insert(height, None);
            }
            match self.get_block_hash_by_height(header_height).map(|h| h.clone()) {
                Ok(cur_hash) if cur_hash == header_hash => {
                    // Found common ancestor.
                    return Ok(());
                }
                _ => {
                    self.block_index.insert(header_height, Some(header_hash));
                    prev_hash = header_prev_hash;
                    prev_height = header_height;
                }
            };
        }
    }

    /// Update header head and height to hash index for this branch.
    pub fn save_header_head(&mut self, t: &Tip) -> Result<(), Error> {
        if t.height > 0 {
            self.update_block_index(t.height, t.prev_block_hash)?;
        }
        self.block_index.insert(t.height, Some(t.last_block_hash));
        self.header_head = Some(t.clone());
        Ok(())
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
        self.deleted_blocks.insert(*hash);
    }

    pub fn save_block_header(&mut self, header: BlockHeader) {
        self.headers.insert(header.hash(), header);
    }

    pub fn save_receipt(&mut self, hash: &CryptoHash, receipt: Vec<ReceiptTransaction>) {
        self.receipts.insert(*hash, receipt);
    }

    pub fn save_transaction_result(&mut self, hash: &CryptoHash, result: TransactionResult) {
        self.transaction_results.insert(*hash, result);
    }

    /// Starts a sub-ChainUpdate with atomic commit/rollback of all operations done
    /// within this scope.
    /// If the closure returns and error, all changes are canceled.
    #[allow(dead_code)]
    pub fn extending<F>(&mut self, f: F) -> Result<bool, Error>
    where
        F: FnOnce(&mut ChainStoreUpdate<'_, ChainStoreUpdate<'a, T>>) -> Result<bool, Error>,
    {
        let mut child_store_update = ChainStoreUpdate::new(self);
        let res = f(&mut child_store_update);
        match res {
            // Committing changes.
            Ok(true) => {
                let store_update = child_store_update.finalize()?;
                self.store_updates.push(store_update);
                Ok(true)
            }
            // Rolling back changes.
            Ok(false) => Ok(false),
            Err(err) => {
                debug!(target: "chain", "Error returned, discarding extension");
                Err(err)
            }
        }
    }

    pub fn save_trie_changes(&mut self, trie_changes: WrappedTrieChanges) {
        self.trie_changes = Some(trie_changes);
    }

    /// Merge another StoreUpdate into this one
    pub fn merge(&mut self, store_update: StoreUpdate) {
        self.store_updates.push(store_update);
    }

    pub fn finalize(mut self) -> Result<StoreUpdate, Error> {
        let mut store_update = self.store().store_update();
        if let Some(t) = self.head {
            store_update.set_ser(COL_BLOCK_MISC, HEAD_KEY, &t).map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.tail {
            store_update.set_ser(COL_BLOCK_MISC, TAIL_KEY, &t).map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.header_head {
            store_update
                .set_ser(COL_BLOCK_MISC, HEADER_HEAD_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.sync_head {
            store_update
                .set_ser(COL_BLOCK_MISC, SYNC_HEAD_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, block) in self.blocks.drain() {
            store_update
                .set_ser(COL_BLOCK, hash.as_ref(), &block)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for hash in self.deleted_blocks.drain() {
            store_update.delete(COL_BLOCK, hash.as_ref());
        }
        for (hash, header) in self.headers.drain() {
            store_update
                .set_ser(COL_BLOCK_HEADER, hash.as_ref(), &header)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, state_root) in self.post_state_roots.drain() {
            store_update
                .set_ser(COL_STATE_REF, hash.as_ref(), &state_root)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (height, hash) in self.block_index.drain() {
            if let Some(hash) = hash {
                store_update
                    .set_ser(COL_BLOCK_INDEX, &index_to_bytes(height), &hash)
                    .map_err::<Error, _>(|e| e.into())?;
            } else {
                store_update.delete(COL_BLOCK_INDEX, &index_to_bytes(height));
            }
        }
        for (hash, receipt) in self.receipts.drain() {
            store_update.set_ser(COL_RECEIPTS, hash.as_ref(), &receipt)?;
        }
        for (hash, tx_result) in self.transaction_results.drain() {
            store_update.set_ser(COL_TRANSACTION_RESULT, hash.as_ref(), &tx_result)?;
        }
        if let Some(trie_changes) = self.trie_changes {
            trie_changes
                .insertions_into(&mut store_update)
                .map_err(|err| ErrorKind::Other(err.to_string()))?;
            // TODO: save deletions separately for garbage collection.
        }
        for other in self.store_updates {
            store_update.merge(other);
        }
        Ok(store_update)
    }

    pub fn commit(self) -> Result<(), Error> {
        let store_update = self.finalize()?;
        store_update.commit().map_err(|e| e.into())
    }
}
