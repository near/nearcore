use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use cached::SizedCache;
use log::debug;

use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{ChunkHash, ChunkOnePart, ShardChunk, ShardChunkHeader};
use near_primitives::transaction::TransactionResult;
use near_primitives::types::{AccountId, BlockIndex, ChunkExtra, ShardId};
use near_primitives::utils::{index_to_bytes, to_timestamp};
use near_store::{
    read_with_cache, Store, StoreUpdate, WrappedTrieChanges, COL_BLOCK, COL_BLOCKS_TO_CATCHUP,
    COL_BLOCK_HEADER, COL_BLOCK_INDEX, COL_BLOCK_MISC, COL_CHUNKS, COL_CHUNK_EXTRA,
    COL_CHUNK_ONE_PARTS, COL_INCOMING_RECEIPTS, COL_OUTGOING_RECEIPTS, COL_STATE_DL_INFOS,
    COL_TRANSACTION_RESULT,
};

use crate::error::{Error, ErrorKind};
use crate::types::{
    Block, BlockHeader, LatestKnown, ReceiptResponse, ShardFullChunkOrOnePart, Tip,
};
use crate::RuntimeAdapter;
use chrono::Utc;

const HEAD_KEY: &[u8; 4] = b"HEAD";
const TAIL_KEY: &[u8; 4] = b"TAIL";
const SYNC_HEAD_KEY: &[u8; 9] = b"SYNC_HEAD";
const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";

/// lru cache size
const CACHE_SIZE: usize = 20;

#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct ShardInfo(pub ShardId, pub ChunkHash);

fn get_block_shard_id(block_hash: &CryptoHash, shard_id: ShardId) -> Vec<u8> {
    let mut res = Vec::with_capacity(40);
    res.extend_from_slice(block_hash.as_ref());
    res.extend_from_slice(&shard_id.to_le_bytes());
    res
}

/// Contains the information that is used to sync state for shards as epochs switch
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize)]
pub struct StateSyncInfo {
    /// The first block of the epoch for which syncing is happening
    pub epoch_tail_hash: CryptoHash,
    /// Shards to fetch state
    pub shards: Vec<ShardInfo>,
}

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
    /// Get full chunk.
    fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error>;
    /// Get full chunk from header, with possible error that contains the header for further retreival.
    fn get_chunk_from_header(&mut self, header: &ShardChunkHeader) -> Result<&ShardChunk, Error> {
        self.get_chunk(&header.chunk_hash())
            .map_err(|_| ErrorKind::ChunksMissing(vec![header.clone()]).into())
    }
    /// Get chunk one part.
    fn get_chunk_one_part(&mut self, header: &ShardChunkHeader) -> Result<&ChunkOnePart, Error>;
    /// Get a collection of chunks and chunk_one_parts for a given height
    fn get_chunks_or_one_parts(
        &mut self,
        me: &Option<AccountId>,
        parent_hash: CryptoHash,
        height: u64,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        headers: &Vec<ShardChunkHeader>,
    ) -> Result<Vec<ShardFullChunkOrOnePart>, Error>;
    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error>;
    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error>;
    /// Get chunk extra info for given chunk hash.
    fn get_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&ChunkExtra, Error>;
    /// Get block header.
    fn get_block_header(&mut self, h: &CryptoHash) -> Result<&BlockHeader, Error>;
    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&mut self, height: BlockIndex) -> Result<CryptoHash, Error>;
    /// Returns resulting receipt for given block.
    fn get_outgoing_receipts(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error>;
    fn get_incoming_receipts(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error>;
    /// Returns transaction result for given tx hash.
    fn get_transaction_result(&mut self, hash: &CryptoHash) -> Result<&TransactionResult, Error>;

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error>;

    /// Returns latest known height and time it was seen.
    fn get_latest_known(&mut self) -> Result<LatestKnown, Error>;

    /// Save the latest known.
    fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error>;
}

/// All chain-related database operations.
pub struct ChainStore {
    store: Arc<Store>,
    /// Latest known.
    latest_known: Option<LatestKnown>,
    /// Cache with headers.
    headers: SizedCache<Vec<u8>, BlockHeader>,
    /// Cache with blocks.
    blocks: SizedCache<Vec<u8>, Block>,
    /// Cache with chunks
    chunks: HashMap<ChunkHash, ShardChunk>,
    /// Cache with chunk one parts
    chunk_one_parts: HashMap<ChunkHash, ChunkOnePart>,
    /// Cache with chunk extra.
    chunk_extras: SizedCache<Vec<u8>, ChunkExtra>,
    // Cache with index to hash on the main chain.
    // block_index: SizedCache<Vec<u8>, CryptoHash>,
    /// Cache with outgoing receipts.
    outgoing_receipts: SizedCache<Vec<u8>, Vec<Receipt>>,
    /// Cache with incoming receipts.
    incoming_receipts: SizedCache<Vec<u8>, Vec<Receipt>>,
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
            latest_known: None,
            blocks: SizedCache::with_size(CACHE_SIZE),
            headers: SizedCache::with_size(CACHE_SIZE),
            chunks: HashMap::new(),
            chunk_one_parts: HashMap::new(),
            chunk_extras: SizedCache::with_size(CACHE_SIZE),
            // block_index: SizedCache::with_size(CACHE_SIZE),
            outgoing_receipts: SizedCache::with_size(CACHE_SIZE),
            incoming_receipts: SizedCache::with_size(CACHE_SIZE),
            transaction_results: SizedCache::with_size(CACHE_SIZE),
        }
    }

    pub fn store(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn store_update(&mut self) -> ChainStoreUpdate<Self> {
        ChainStoreUpdate::new(self)
    }

    pub fn iterate_state_sync_infos(&self) -> Vec<(CryptoHash, StateSyncInfo)> {
        self.store
            .iter(COL_STATE_DL_INFOS)
            .map(|(k, v)| {
                (
                    CryptoHash::try_from(k.as_ref()).unwrap(),
                    StateSyncInfo::try_from_slice(v.as_ref()).unwrap(),
                )
            })
            .collect()
    }

    pub fn get_outgoing_receipts_for_shard(
        &mut self,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_included_height: BlockIndex,
    ) -> Result<ReceiptResponse, Error> {
        let mut receipts_block_hash = prev_block_hash;
        loop {
            let block_header = self.get_block_header(&receipts_block_hash)?;

            if block_header.inner.height == last_included_height {
                let receipts = if let Ok(cur_receipts) =
                    self.get_outgoing_receipts(&receipts_block_hash, shard_id)
                {
                    cur_receipts.clone()
                } else {
                    vec![]
                };
                return Ok(ReceiptResponse(receipts_block_hash, receipts));
            } else {
                receipts_block_hash = block_header.inner.prev_hash;
            }
        }
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

    /// Get full chunk.
    fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error> {
        let entry = self.chunks.entry(chunk_hash.clone());
        match entry {
            Entry::Occupied(s) => Ok(s.into_mut()),
            Entry::Vacant(s) => {
                if let Ok(Some(chunk)) = self.store.get_ser(COL_CHUNKS, chunk_hash.as_ref()) {
                    Ok(s.insert(chunk))
                } else {
                    Err(ErrorKind::ChunkMissing(chunk_hash.clone()).into())
                }
            }
        }
    }

    /// Get Chunk one part.
    fn get_chunk_one_part(&mut self, header: &ShardChunkHeader) -> Result<&ChunkOnePart, Error> {
        let chunk_hash = header.chunk_hash();
        let entry = self.chunk_one_parts.entry(chunk_hash.clone());
        match entry {
            Entry::Occupied(s) => Ok(s.into_mut()),
            Entry::Vacant(s) => {
                if let Ok(Some(chunk_one_part)) =
                    self.store.get_ser(COL_CHUNK_ONE_PARTS, chunk_hash.as_ref())
                {
                    Ok(s.insert(chunk_one_part))
                } else {
                    Err(ErrorKind::ChunksMissing(vec![header.clone()]).into())
                }
            }
        }
    }

    fn get_chunks_or_one_parts(
        &mut self,
        me: &Option<AccountId>,
        parent_hash: CryptoHash,
        height: u64,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        headers: &Vec<ShardChunkHeader>,
    ) -> Result<Vec<ShardFullChunkOrOnePart>, Error> {
        let mut ret = vec![];
        let mut missing = vec![];

        // First find missing ShardChunk's and ChunkOnePart's in the cache and fetch them
        for (shard_id, chunk_header) in headers.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if chunk_header.height_included == height {
                let chunk_hash = chunk_header.chunk_hash();
                if runtime_adapter.cares_about_shard(me.as_ref(), &parent_hash, shard_id, true)
                    || runtime_adapter.will_care_about_shard(
                        me.as_ref(),
                        &parent_hash,
                        shard_id,
                        true,
                    )
                {
                    let entry = self.chunks.entry(chunk_hash.clone());
                    match entry {
                        Entry::Occupied(_) => (),
                        Entry::Vacant(s) => {
                            if let Ok(Some(chunk)) =
                                self.store.get_ser(COL_CHUNKS, chunk_hash.as_ref())
                            {
                                s.insert(chunk);
                            } else {
                                missing.push(chunk_header.clone());
                            }
                        }
                    };
                } else {
                    let entry = self.chunk_one_parts.entry(chunk_hash.clone());
                    match entry {
                        Entry::Occupied(_) => (),
                        Entry::Vacant(s) => {
                            if let Ok(Some(chunk_one_part)) =
                                self.store.get_ser(COL_CHUNK_ONE_PARTS, chunk_hash.as_ref())
                            {
                                s.insert(chunk_one_part);
                            } else {
                                missing.push(chunk_header.clone());
                            }
                        }
                    };
                }
            }
        }

        if !missing.is_empty() {
            return Err(ErrorKind::ChunksMissing(missing).into());
        }

        // Then get all the data from the cache
        for (shard_id, chunk_header) in headers.iter().enumerate() {
            let shard_id = shard_id as ShardId;
            if chunk_header.height_included != height {
                ret.push(ShardFullChunkOrOnePart::NoChunk);
            } else if runtime_adapter.cares_about_shard(me.as_ref(), &parent_hash, shard_id, true)
                || runtime_adapter.will_care_about_shard(me.as_ref(), &parent_hash, shard_id, true)
            {
                ret.push(ShardFullChunkOrOnePart::FullChunk(
                    self.chunks.get(&chunk_header.chunk_hash()).unwrap(),
                ));
            } else {
                ret.push(ShardFullChunkOrOnePart::OnePart(
                    self.chunk_one_parts.get(&chunk_header.chunk_hash()).unwrap(),
                ));
            }
        }

        Ok(ret)
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(COL_BLOCK, h.as_ref()).map_err(|e| e.into())
    }

    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.get_block_header(&header.inner.prev_hash)
    }

    /// Get state root hash after applying header with given hash.
    fn get_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&ChunkExtra, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                COL_CHUNK_EXTRA,
                &mut self.chunk_extras,
                &get_block_shard_id(block_hash, shard_id),
            ),
            &format!("CHUNK EXTRA: {}:{}", block_hash, shard_id),
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

    fn get_outgoing_receipts(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                COL_OUTGOING_RECEIPTS,
                &mut self.outgoing_receipts,
                &get_block_shard_id(block_hash, shard_id),
            ),
            &format!("OUTGOING RECEIPT: {}", block_hash),
        )
    }

    fn get_incoming_receipts(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                COL_INCOMING_RECEIPTS,
                &mut self.incoming_receipts,
                &get_block_shard_id(block_hash, shard_id),
            ),
            &format!("INCOMING RECEIPT: {}", block_hash),
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

    fn get_blocks_to_catchup(&self, hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        Ok(self.store.get_ser(COL_BLOCKS_TO_CATCHUP, hash.as_ref())?.unwrap_or_else(|| vec![]))
    }

    fn get_latest_known(&mut self) -> Result<LatestKnown, Error> {
        if self.latest_known.is_none() {
            self.latest_known = Some(option_to_not_found(
                self.store.get_ser(COL_BLOCK_MISC, LATEST_KNOWN_KEY),
                "LATEST_KNOWN_KEY",
            )?);
        }
        Ok(self.latest_known.as_ref().unwrap().clone())
    }

    fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error> {
        let mut store_update = self.store.store_update();
        store_update.set_ser(COL_BLOCK_MISC, LATEST_KNOWN_KEY, &latest_known)?;
        self.latest_known = Some(latest_known);
        store_update.commit().map_err(|err| err.into())
    }
}

/// Provides layer to update chain without touching the underlying database.
/// This serves few purposes, main one is that even if executable exists/fails during update the database is in consistent state.
pub struct ChainStoreUpdate<'a, T> {
    chain_store: &'a mut T,
    store_updates: Vec<StoreUpdate>,
    /// Blocks added during this update. Takes ownership (unclear how to not do it because of failure exists).
    blocks: HashMap<CryptoHash, Block>,
    deleted_blocks: HashSet<CryptoHash>,
    headers: HashMap<CryptoHash, BlockHeader>,
    chunk_extras: HashMap<(CryptoHash, ShardId), ChunkExtra>,
    block_index: HashMap<BlockIndex, Option<CryptoHash>>,
    outgoing_receipts: HashMap<(CryptoHash, ShardId), Vec<Receipt>>,
    incoming_receipts: HashMap<(CryptoHash, ShardId), Vec<Receipt>>,
    transaction_results: HashMap<CryptoHash, TransactionResult>,
    head: Option<Tip>,
    tail: Option<Tip>,
    header_head: Option<Tip>,
    sync_head: Option<Tip>,
    trie_changes: Vec<WrappedTrieChanges>,
    add_blocks_to_catchup: Vec<(CryptoHash, CryptoHash)>,
    remove_blocks_to_catchup: Vec<CryptoHash>,
    add_state_dl_infos: Vec<StateSyncInfo>,
    remove_state_dl_infos: Vec<CryptoHash>,
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
            chunk_extras: HashMap::default(),
            outgoing_receipts: HashMap::default(),
            incoming_receipts: HashMap::default(),
            transaction_results: HashMap::default(),
            head: None,
            tail: None,
            header_head: None,
            sync_head: None,
            trie_changes: vec![],
            add_blocks_to_catchup: vec![],
            remove_blocks_to_catchup: vec![],
            add_state_dl_infos: vec![],
            remove_state_dl_infos: vec![],
        }
    }

    pub fn get_incoming_receipts_for_shard(
        &mut self,
        shard_id: ShardId,
        mut block_hash: CryptoHash,
        last_chunk_header: &ShardChunkHeader,
    ) -> Result<Vec<ReceiptResponse>, Error> {
        let mut ret = vec![];

        if last_chunk_header.inner.prev_block_hash == CryptoHash::default() {
            return Ok(ret);
        }

        loop {
            let header = self.get_block_header(&block_hash)?;

            if header.inner.height == last_chunk_header.height_included {
                break;
            }

            let prev_hash = header.inner.prev_hash;

            if let Ok(receipts) = self.get_incoming_receipts(&block_hash, shard_id) {
                ret.push(ReceiptResponse(block_hash, receipts.clone()));
            }

            block_hash = prev_hash;
        }

        Ok(ret)
    }

    /// WARNING
    ///
    /// Usually ChainStoreUpdate has some uncommitted changes
    /// and chain_store don't have access to them until they become committed.
    /// Make sure you're doing it right.
    pub fn get_chain_store(&mut self) -> &mut T {
        self.chain_store
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
        self.get_block_header(&header.inner.prev_hash)
    }

    /// Get state root hash after applying header with given hash.
    fn get_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&ChunkExtra, Error> {
        if let Some(chunk_extra) = self.chunk_extras.get(&(*block_hash, shard_id)) {
            Ok(chunk_extra)
        } else {
            self.chain_store.get_chunk_extra(block_hash, shard_id)
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

    /// Get receipts produced for block with given hash.
    fn get_outgoing_receipts(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error> {
        if let Some(receipts) = self.outgoing_receipts.get(&(*hash, shard_id)) {
            Ok(receipts)
        } else {
            self.chain_store.get_outgoing_receipts(hash, shard_id)
        }
    }

    /// Get receipts produced for block with givien hash.
    fn get_incoming_receipts(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error> {
        if let Some(receipts) = self.incoming_receipts.get(&(*hash, shard_id)) {
            Ok(receipts)
        } else {
            self.chain_store.get_incoming_receipts(hash, shard_id)
        }
    }

    fn get_transaction_result(&mut self, hash: &CryptoHash) -> Result<&TransactionResult, Error> {
        self.chain_store.get_transaction_result(hash)
    }

    fn get_chunks_or_one_parts(
        &mut self,
        me: &Option<String>,
        parent_hash: CryptoHash,
        height: u64,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        headers: &Vec<ShardChunkHeader>,
    ) -> Result<Vec<ShardFullChunkOrOnePart>, Error> {
        self.chain_store.get_chunks_or_one_parts(me, parent_hash, height, runtime_adapter, headers)
    }

    fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error> {
        self.chain_store.get_chunk(chunk_hash)
    }

    fn get_chunk_from_header(&mut self, header: &ShardChunkHeader) -> Result<&ShardChunk, Error> {
        self.chain_store.get_chunk_from_header(header)
    }

    fn get_chunk_one_part(&mut self, header: &ShardChunkHeader) -> Result<&ChunkOnePart, Error> {
        self.chain_store.get_chunk_one_part(header)
    }

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        // Make sure we never request a block to catchup after altering the data structure
        assert_eq!(self.add_blocks_to_catchup.len(), 0);
        assert_eq!(self.remove_blocks_to_catchup.len(), 0);

        self.chain_store.get_blocks_to_catchup(prev_hash)
    }

    fn get_latest_known(&mut self) -> Result<LatestKnown, Error> {
        self.chain_store.get_latest_known()
    }

    fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error> {
        self.chain_store.save_latest_known(latest_known)
    }
}

impl<'a, T: ChainStoreAccess> ChainStoreUpdate<'a, T> {
    /// Update both header and block body head.
    pub fn save_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.save_body_head(t)?;
        self.save_header_head(t)
    }

    /// Update block body head and latest known height.
    pub fn save_body_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.try_save_latest_known(t.height)?;
        self.head = Some(t.clone());
        Ok(())
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
                (header.inner.height, header.hash(), header.inner.prev_hash);
            // Clean up block indicies between blocks.
            for height in (header_height + 1)..prev_height {
                self.block_index.insert(height, None);
            }
            match self.get_block_hash_by_height(header_height) {
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
        self.try_save_latest_known(t.height)?;
        self.block_index.insert(t.height, Some(t.last_block_hash));
        self.header_head = Some(t.clone());
        Ok(())
    }

    /// Save "sync" head.
    pub fn save_sync_head(&mut self, t: &Tip) {
        self.sync_head = Some(t.clone());
    }

    /// Save new height if it's above currently latest known.
    pub fn try_save_latest_known(&mut self, height: BlockIndex) -> Result<(), Error> {
        let latest_known = self.get_latest_known().ok();
        if latest_known.is_none() || height > latest_known.unwrap().height {
            self.save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        }
        Ok(())
    }

    /// Save block.
    pub fn save_block(&mut self, block: Block) {
        self.blocks.insert(block.hash(), block);
    }

    /// Save post applying block state root.
    pub fn save_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        chunk_extra: ChunkExtra,
    ) {
        self.chunk_extras.insert((*block_hash, shard_id), chunk_extra);
    }

    pub fn delete_block(&mut self, hash: &CryptoHash) {
        self.deleted_blocks.insert(*hash);
    }

    pub fn save_block_header(&mut self, header: BlockHeader) {
        self.headers.insert(header.hash(), header);
    }

    pub fn save_outgoing_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        receipt: Vec<Receipt>,
    ) {
        self.outgoing_receipts.insert((*hash, shard_id), receipt);
    }

    pub fn save_incoming_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        receipt: Vec<Receipt>,
    ) {
        self.incoming_receipts.insert((*hash, shard_id), receipt);
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
        self.trie_changes.push(trie_changes);
    }

    pub fn add_block_to_catchup(&mut self, prev_hash: CryptoHash, block_hash: CryptoHash) {
        self.add_blocks_to_catchup.push((prev_hash, block_hash));
    }

    pub fn remove_block_to_catchup(&mut self, prev_hash: CryptoHash) {
        self.remove_blocks_to_catchup.push(prev_hash);
    }

    pub fn add_state_dl_info(&mut self, info: StateSyncInfo) {
        self.add_state_dl_infos.push(info);
    }

    pub fn remove_state_dl_info(&mut self, hash: CryptoHash) {
        self.remove_state_dl_infos.push(hash);
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
        for ((block_hash, shard_id), chunk_extra) in self.chunk_extras.drain() {
            store_update
                .set_ser(COL_CHUNK_EXTRA, &get_block_shard_id(&block_hash, shard_id), &chunk_extra)
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
        for ((block_hash, shard_id), receipt) in self.outgoing_receipts.drain() {
            store_update.set_ser(
                COL_OUTGOING_RECEIPTS,
                &get_block_shard_id(&block_hash, shard_id),
                &receipt,
            )?;
        }
        for ((block_hash, shard_id), receipt) in self.incoming_receipts.drain() {
            store_update.set_ser(
                COL_INCOMING_RECEIPTS,
                &get_block_shard_id(&block_hash, shard_id),
                &receipt,
            )?;
        }
        for (hash, tx_result) in self.transaction_results.drain() {
            store_update.set_ser(COL_TRANSACTION_RESULT, hash.as_ref(), &tx_result)?;
        }
        for trie_changes in self.trie_changes {
            trie_changes
                .insertions_into(&mut store_update)
                .map_err(|err| ErrorKind::Other(err.to_string()))?;
            // TODO: save deletions separately for garbage collection.
        }
        let mut affected_catchup_blocks = HashSet::new();
        for hash in self.remove_blocks_to_catchup {
            assert!(!affected_catchup_blocks.contains(&hash));
            if affected_catchup_blocks.contains(&hash) {
                return Err(ErrorKind::Other(
                    "Multiple changes to the store affect the same catchup block".to_string(),
                )
                .into());
            }
            affected_catchup_blocks.insert(hash);

            store_update.delete(COL_BLOCKS_TO_CATCHUP, hash.as_ref());
        }
        for (prev_hash, new_hash) in self.add_blocks_to_catchup {
            assert!(!affected_catchup_blocks.contains(&prev_hash));
            if affected_catchup_blocks.contains(&prev_hash) {
                return Err(ErrorKind::Other(
                    "Multiple changes to the store affect the same catchup block".to_string(),
                )
                .into());
            }
            affected_catchup_blocks.insert(prev_hash);

            let mut prev_table =
                self.chain_store.get_blocks_to_catchup(&prev_hash).unwrap_or_else(|_| vec![]);
            prev_table.push(new_hash);
            store_update.set_ser(COL_BLOCKS_TO_CATCHUP, prev_hash.as_ref(), &prev_table)?;
        }
        for state_dl_info in self.add_state_dl_infos {
            store_update.set_ser(
                COL_STATE_DL_INFOS,
                state_dl_info.epoch_tail_hash.as_ref(),
                &state_dl_info,
            )?;
        }
        for hash in self.remove_state_dl_infos {
            store_update.delete(COL_STATE_DL_INFOS, hash.as_ref());
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
