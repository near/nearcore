use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::io;
use std::sync::Arc;

use serde::Serialize;

use borsh::{BorshDeserialize, BorshSerialize};
use cached::{Cached, SizedCache};
use chrono::Utc;

use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, ReceiptProof, ShardChunk, ShardChunkHeader,
};
use near_primitives::transaction::{
    ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof, SignedTransaction,
};
use near_primitives::types::{
    AccountId, BlockExtra, BlockHeight, ChunkExtra, EpochId, NumBlocks, ShardId,
};
use near_primitives::utils::{index_to_bytes, to_timestamp};
use near_store::{
    read_with_cache, ColBlock, ColBlockExtra, ColBlockHeader, ColBlockHeight, ColBlockMisc,
    ColBlockPerHeight, ColBlocksToCatchup, ColChallengedBlocks, ColChunkExtra, ColChunks,
    ColEpochLightClientBlocks, ColIncomingReceipts, ColInvalidChunks, ColLastApprovalPerAccount,
    ColLastBlockWithNewChunk, ColMyLastApprovalsPerChain, ColNextBlockHashes,
    ColNextBlockWithNewChunk, ColOutgoingReceipts, ColPartialChunks, ColReceiptIdToShardId,
    ColStateDlInfos, ColTransactionResult, ColTransactions, Store, StoreUpdate, WrappedTrieChanges,
};

use crate::byzantine_assert;
use crate::error::{Error, ErrorKind};
use crate::types::{Block, BlockHeader, LatestKnown, ReceiptProofResponse, ReceiptResponse, Tip};
use near_primitives::block::{Approval, Weight};
use near_primitives::errors::InvalidTxError;
use near_primitives::merkle::MerklePath;
use near_primitives::views::LightClientBlockView;

const HEAD_KEY: &[u8; 4] = b"HEAD";
const TAIL_KEY: &[u8; 4] = b"TAIL";
const SYNC_HEAD_KEY: &[u8; 9] = b"SYNC_HEAD";
const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
const LARGEST_APPROVED_WEIGHT_KEY: &[u8; 23] = b"LARGEST_APPROVED_WEIGHT";
const LARGEST_APPROVED_SCORE_KEY: &[u8; 22] = b"LARGEST_APPROVED_SCORE";

/// lru cache size
const CACHE_SIZE: usize = 100;
const CHUNK_CACHE_SIZE: usize = 1024;

#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardInfo(pub ShardId, pub ChunkHash);

fn get_block_shard_id(block_hash: &CryptoHash, shard_id: ShardId) -> Vec<u8> {
    let mut res = Vec::with_capacity(40);
    res.extend_from_slice(block_hash.as_ref());
    res.extend_from_slice(&shard_id.to_le_bytes());
    res
}

/// Contains the information that is used to sync state for shards as epochs switch
#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct StateSyncInfo {
    /// The first block of the epoch for which syncing is happening
    pub epoch_tail_hash: CryptoHash,
    /// Shards to fetch state
    pub shards: Vec<ShardInfo>,
}

/// Header cache used for transaction history validation.
/// The headers stored here should be all on the same fork.
pub struct HeaderList {
    queue: VecDeque<CryptoHash>,
    headers: HashMap<CryptoHash, BlockHeader>,
}

impl HeaderList {
    pub fn new() -> Self {
        HeaderList { queue: VecDeque::default(), headers: HashMap::default() }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn contains(&self, hash: &CryptoHash) -> bool {
        self.headers.contains_key(hash)
    }

    pub fn push_back(&mut self, block_header: BlockHeader) {
        self.queue.push_back(block_header.hash);
        self.headers.insert(block_header.hash, block_header);
    }

    pub fn push_front(&mut self, block_header: BlockHeader) {
        let block_hash = block_header.hash;
        self.queue.push_front(block_hash);
        self.headers.insert(block_hash, block_header);
    }

    pub fn pop_front(&mut self) -> Option<BlockHeader> {
        let front = if let Some(hash) = self.queue.pop_front() {
            hash
        } else {
            return None;
        };
        let header = self.headers.remove(&front).unwrap();
        Some(header)
    }

    pub fn pop_back(&mut self) -> Option<BlockHeader> {
        let back = if let Some(hash) = self.queue.pop_back() {
            hash
        } else {
            return None;
        };
        let header = self.headers.remove(&back).unwrap();
        Some(header)
    }

    pub fn from_headers(headers: Vec<BlockHeader>) -> Self {
        let mut res = Self::new();
        for header in headers {
            res.push_back(header);
        }
        res
    }

    /// Tries to update the cache. if `hash` is in the cache, remove everything before `hash`
    /// and replace them with `new_list`. `new_list` must contain contiguous block headers, ordered
    /// from higher height to lower height.
    /// Returns true if `hash` is in the cache and false otherwise.
    fn update(&mut self, hash: &CryptoHash, new_list: &[BlockHeader]) -> bool {
        if !self.headers.contains_key(hash) {
            return false;
        }
        loop {
            let front = if let Some(elem) = self.queue.front() {
                elem.clone()
            } else {
                break;
            };
            if &front == hash {
                break;
            } else {
                self.queue.pop_front();
                self.headers.remove(&front);
            }
        }
        for header in new_list.into_iter().rev() {
            self.push_front(header.clone());
        }
        true
    }
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
    /// Largest weight and score for which the approval was ever created
    fn largest_approved_weight(&self) -> Result<Weight, Error>;
    fn largest_approved_score(&self) -> Result<Weight, Error>;
    /// Get full block.
    fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error>;
    /// Get full chunk.
    fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error>;
    /// Get partial chunk.
    fn get_partial_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&PartialEncodedChunk, Error>;
    /// Get full chunk from header, with possible error that contains the header for further retrieval.
    fn get_chunk_clone_from_header(
        &mut self,
        header: &ShardChunkHeader,
    ) -> Result<ShardChunk, Error> {
        let shard_chunk_result = self.get_chunk(&header.chunk_hash());
        match shard_chunk_result {
            Err(_) => {
                return Err(ErrorKind::ChunksMissing(vec![header.clone()]).into());
            }
            Ok(shard_chunk) => {
                byzantine_assert!(header.height_included > 0 || header.inner.height_created == 0);
                if header.height_included == 0 && header.inner.height_created > 0 {
                    return Err(ErrorKind::Other(format!(
                        "Invalid header: {:?} for chunk {:?}",
                        header, shard_chunk
                    ))
                    .into());
                }
                let mut shard_chunk_clone = shard_chunk.clone();
                shard_chunk_clone.header.height_included = header.height_included;
                Ok(shard_chunk_clone)
            }
        }
    }
    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error>;
    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error>;
    /// GEt block extra for given block.
    fn get_block_extra(&mut self, block_hash: &CryptoHash) -> Result<&BlockExtra, Error>;
    /// Get chunk extra info for given block hash + shard id.
    fn get_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&ChunkExtra, Error>;
    /// Get block header.
    fn get_block_header(&mut self, h: &CryptoHash) -> Result<&BlockHeader, Error>;
    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&mut self, height: BlockHeight) -> Result<CryptoHash, Error>;
    /// Returns block header from the current chain for given height if present.
    fn get_header_by_height(&mut self, height: BlockHeight) -> Result<&BlockHeader, Error> {
        let hash = self.get_block_hash_by_height(height)?;
        self.get_block_header(&hash)
    }
    fn get_next_block_hash(&mut self, hash: &CryptoHash) -> Result<&CryptoHash, Error>;
    fn get_epoch_light_client_block(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<&LightClientBlockView, Error>;
    /// Check if we have block header at given height across any chain.
    /// Returns a hashmap of epoch id -> block hash that we can use to determine whether the block is double signed
    /// For each epoch id we need to store just one block hash because for the same epoch id the signer of a given
    /// height must be the same.
    fn get_any_block_hash_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<&HashMap<EpochId, CryptoHash>, Error>;
    /// Returns block header from the current chain defined by `sync_hash` for given height if present.
    fn get_header_on_chain_by_height(
        &mut self,
        sync_hash: &CryptoHash,
        height: BlockHeight,
    ) -> Result<&BlockHeader, Error> {
        let mut header = self.get_block_header(sync_hash)?;
        let mut hash = sync_hash.clone();
        while header.inner_lite.height > height {
            hash = header.prev_hash;
            header = self.get_block_header(&hash)?;
        }
        if header.inner_lite.height < height {
            return Err(ErrorKind::InvalidBlockHeight.into());
        }
        self.get_block_header(&hash)
    }
    /// Returns resulting receipt for given block.
    fn get_my_last_approval(&mut self, block_hash: &CryptoHash) -> Result<&Approval, Error>;
    /// Returns resulting receipt for given block.
    fn get_last_approval_for_account(&mut self, account_id: &AccountId)
        -> Result<&Approval, Error>;
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
    ) -> Result<&Vec<ReceiptProof>, Error>;
    /// Returns transaction and receipt outcome for given hash.
    fn get_execution_outcome(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<&ExecutionOutcomeWithIdAndProof, Error>;
    /// Returns whether the block with the given hash was challenged
    fn is_block_challenged(&mut self, hash: &CryptoHash) -> Result<bool, Error>;

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error>;

    /// Returns latest known height and time it was seen.
    fn get_latest_known(&mut self) -> Result<LatestKnown, Error>;

    /// Save the latest known.
    fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error>;

    /// Returns encoded chunk if it's invalid otherwise None.
    fn is_invalid_chunk(
        &mut self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<&EncodedShardChunk>, Error>;

    /// Get destination shard id for receipt id.
    fn get_shard_id_for_receipt_id(&mut self, receipt_id: &CryptoHash) -> Result<&ShardId, Error>;

    /// For a given block and a given shard, get the next block hash where a new chunk for the shard is included.
    fn get_next_block_hash_with_new_chunk(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Option<&CryptoHash>, Error>;

    fn get_last_block_with_new_chunk(
        &mut self,
        shard_id: ShardId,
    ) -> Result<Option<&CryptoHash>, Error>;

    fn get_transaction(
        &mut self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<&SignedTransaction>, Error>;
}

/// All chain-related database operations.
pub struct ChainStore {
    store: Arc<Store>,
    /// Latest known.
    latest_known: Option<LatestKnown>,
    /// Cache with headers.
    headers: SizedCache<Vec<u8>, BlockHeader>,
    /// Cache with headers for transaction validation.
    header_history: HeaderList,
    /// Cache with blocks.
    blocks: SizedCache<Vec<u8>, Block>,
    /// Cache with chunks
    chunks: SizedCache<Vec<u8>, ShardChunk>,
    /// Cache with partial chunks
    partial_chunks: SizedCache<Vec<u8>, PartialEncodedChunk>,
    /// Cache with block extra.
    block_extras: SizedCache<Vec<u8>, BlockExtra>,
    /// Cache with chunk extra.
    chunk_extras: SizedCache<Vec<u8>, ChunkExtra>,
    /// Cache with index to hash on the main chain.
    height: SizedCache<Vec<u8>, CryptoHash>,
    /// Cache with index to hash on any chain.
    block_hash_per_height: SizedCache<Vec<u8>, HashMap<EpochId, CryptoHash>>,
    /// Next block hashes for each block on the canonical chain
    next_block_hashes: SizedCache<Vec<u8>, CryptoHash>,
    /// Light client blocks corresponding to the last finalized block of each epoch
    epoch_light_client_blocks: SizedCache<Vec<u8>, LightClientBlockView>,
    /// Cache of my last approvals
    my_last_approvals: SizedCache<Vec<u8>, Approval>,
    /// Cache of last approvals for each account
    last_approvals_per_account: SizedCache<Vec<u8>, Approval>,
    /// Cache with outgoing receipts.
    outgoing_receipts: SizedCache<Vec<u8>, Vec<Receipt>>,
    /// Cache with incoming receipts.
    incoming_receipts: SizedCache<Vec<u8>, Vec<ReceiptProof>>,
    /// Cache transaction statuses.
    outcomes: SizedCache<Vec<u8>, ExecutionOutcomeWithIdAndProof>,
    /// Invalid chunks.
    invalid_chunks: SizedCache<Vec<u8>, EncodedShardChunk>,
    /// Mapping from receipt id to destination shard id
    receipt_id_to_shard_id: SizedCache<Vec<u8>, ShardId>,
    /// Mapping from block to a map of shard id to the next block hash where a new chunk for the
    /// shard is included.
    next_block_with_new_chunk: SizedCache<Vec<u8>, CryptoHash>,
    /// Shard id to last block that contains a new chunk for this shard.
    last_block_with_new_chunk: SizedCache<Vec<u8>, CryptoHash>,
    /// Transactions
    transactions: SizedCache<Vec<u8>, SignedTransaction>,
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
            header_history: HeaderList::new(),
            chunks: SizedCache::with_size(CHUNK_CACHE_SIZE),
            partial_chunks: SizedCache::with_size(CHUNK_CACHE_SIZE),
            block_extras: SizedCache::with_size(CACHE_SIZE),
            chunk_extras: SizedCache::with_size(CACHE_SIZE),
            height: SizedCache::with_size(CACHE_SIZE),
            block_hash_per_height: SizedCache::with_size(CACHE_SIZE),
            next_block_hashes: SizedCache::with_size(CACHE_SIZE),
            epoch_light_client_blocks: SizedCache::with_size(CACHE_SIZE),
            my_last_approvals: SizedCache::with_size(CACHE_SIZE),
            last_approvals_per_account: SizedCache::with_size(CACHE_SIZE),
            outgoing_receipts: SizedCache::with_size(CACHE_SIZE),
            incoming_receipts: SizedCache::with_size(CACHE_SIZE),
            outcomes: SizedCache::with_size(CACHE_SIZE),
            invalid_chunks: SizedCache::with_size(CACHE_SIZE),
            receipt_id_to_shard_id: SizedCache::with_size(CHUNK_CACHE_SIZE),
            next_block_with_new_chunk: SizedCache::with_size(CHUNK_CACHE_SIZE),
            last_block_with_new_chunk: SizedCache::with_size(CHUNK_CACHE_SIZE),
            transactions: SizedCache::with_size(CHUNK_CACHE_SIZE),
        }
    }

    pub fn owned_store(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn store_update(&mut self) -> ChainStoreUpdate {
        ChainStoreUpdate::new(self)
    }

    pub fn iterate_state_sync_infos(&self) -> Vec<(CryptoHash, StateSyncInfo)> {
        self.store
            .iter(ColStateDlInfos)
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
        last_included_height: BlockHeight,
    ) -> Result<ReceiptResponse, Error> {
        let mut receipts_block_hash = prev_block_hash;
        loop {
            let block_header = self.get_block_header(&receipts_block_hash)?;

            if block_header.inner_lite.height == last_included_height {
                let receipts = if let Ok(cur_receipts) =
                    self.get_outgoing_receipts(&receipts_block_hash, shard_id)
                {
                    cur_receipts.clone()
                } else {
                    vec![]
                };
                return Ok(ReceiptResponse(receipts_block_hash, receipts));
            } else {
                receipts_block_hash = block_header.prev_hash;
            }
        }
    }

    pub fn check_blocks_on_same_chain(
        &mut self,
        cur_header: &BlockHeader,
        base_block_hash: &CryptoHash,
        max_difference_in_blocks: NumBlocks,
    ) -> Result<(), InvalidTxError> {
        // first step: update cache head
        if self.header_history.is_empty() {
            self.header_history.push_back(cur_header.clone());
        }
        let mut prev_block_hash = cur_header.prev_hash;

        let contains_hash = self.header_history.update(&cur_header.hash, &[]);
        if !contains_hash {
            let mut header_list = vec![cur_header.clone()];
            let mut found_ancestor = false;
            while !self.header_history.is_empty() {
                let prev_block_header = if let Ok(header) = self.get_block_header(&prev_block_hash)
                {
                    header.clone()
                } else {
                    return Err(InvalidTxError::InvalidChain);
                };
                self.header_history.pop_front();
                if self.header_history.update(&prev_block_header.hash, &header_list) {
                    found_ancestor = true;
                    break;
                }
                prev_block_hash = prev_block_header.prev_hash;
                header_list.push(prev_block_header);
            }
            if !found_ancestor {
                self.header_history = HeaderList::from_headers(header_list);
            }
            // It is possible that cur_len is max_difference_in_blocks + 1 after the above update.
            let cur_len = self.header_history.len() as NumBlocks;
            if cur_len > max_difference_in_blocks {
                for _ in 0..cur_len - max_difference_in_blocks {
                    self.header_history.pop_back();
                }
            }
        }

        // second step: check if `base_block_hash` exists
        assert!(max_difference_in_blocks >= self.header_history.len() as NumBlocks);
        if self.header_history.contains(base_block_hash) {
            return Ok(());
        }
        let num_to_fetch = max_difference_in_blocks - self.header_history.len() as NumBlocks;
        // here the queue cannot be empty so it is safe to unwrap
        let last_hash = self.header_history.queue.back().unwrap();
        prev_block_hash = self.header_history.headers.get(last_hash).unwrap().prev_hash;
        for _ in 0..num_to_fetch {
            let cur_block_header = if let Ok(header) = self.get_block_header(&prev_block_hash) {
                header.clone()
            } else {
                return Err(InvalidTxError::InvalidChain);
            };
            prev_block_hash = cur_block_header.prev_hash;
            let cur_block_hash = cur_block_header.hash;
            self.header_history.push_back(cur_block_header);
            if &cur_block_hash == base_block_hash {
                return Ok(());
            }
        }
        Err(InvalidTxError::Expired)
    }

    pub fn get_block_height(&mut self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            Ok(0)
        } else {
            Ok(self.get_block_header(hash)?.inner_lite.height)
        }
    }
}

impl ChainStoreAccess for ChainStore {
    fn store(&self) -> &Store {
        &*self.store
    }
    /// The chain head.
    fn head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(ColBlockMisc, HEAD_KEY), "HEAD")
    }

    /// The chain tail (as far as chain goes).
    fn tail(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(ColBlockMisc, TAIL_KEY), "TAIL")
    }

    /// The "sync" head: last header we received from syncing.
    fn sync_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(ColBlockMisc, SYNC_HEAD_KEY), "SYNC_HEAD")
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.get_block_header(&self.head()?.last_block_hash)
    }

    /// Largest weight for which the approval was ever created
    fn largest_approved_weight(&self) -> Result<Weight, Error> {
        option_to_not_found(
            self.store.get_ser(ColBlockMisc, LARGEST_APPROVED_WEIGHT_KEY),
            "LARGEST_APPROVED_WEIGHT_KEY",
        )
    }

    /// Largest score for which the approval was ever created
    fn largest_approved_score(&self) -> Result<Weight, Error> {
        option_to_not_found(
            self.store.get_ser(ColBlockMisc, LARGEST_APPROVED_SCORE_KEY),
            "LARGEST_APPROVED_SCORE_KEY",
        )
    }

    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(ColBlockMisc, HEADER_HEAD_KEY), "HEADER_HEAD")
    }

    /// Get full block.
    fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, ColBlock, &mut self.blocks, h.as_ref()),
            &format!("BLOCK: {}", h),
        )
    }

    /// Get full chunk.
    fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error> {
        match read_with_cache(&*self.store, ColChunks, &mut self.chunks, chunk_hash.as_ref()) {
            Ok(Some(shard_chunk)) => Ok(shard_chunk),
            _ => Err(ErrorKind::ChunkMissing(chunk_hash.clone()).into()),
        }
    }

    /// Get partial chunk.
    fn get_partial_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&PartialEncodedChunk, Error> {
        match read_with_cache(
            &*self.store,
            ColPartialChunks,
            &mut self.partial_chunks,
            chunk_hash.as_ref(),
        ) {
            Ok(Some(shard_chunk)) => Ok(shard_chunk),
            _ => Err(ErrorKind::ChunkMissing(chunk_hash.clone()).into()),
        }
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(ColBlock, h.as_ref()).map_err(|e| e.into())
    }

    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
    }

    /// Information from applying block.
    fn get_block_extra(&mut self, block_hash: &CryptoHash) -> Result<&BlockExtra, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColBlockExtra,
                &mut self.block_extras,
                block_hash.as_ref(),
            ),
            &format!("BLOCK EXTRA: {}", block_hash),
        )
    }

    /// Information from applying chunk.
    fn get_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&ChunkExtra, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColChunkExtra,
                &mut self.chunk_extras,
                &get_block_shard_id(block_hash, shard_id),
            ),
            &format!("CHUNK EXTRA: {}:{}", block_hash, shard_id),
        )
    }

    /// Get block header.
    fn get_block_header(&mut self, h: &CryptoHash) -> Result<&BlockHeader, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, ColBlockHeader, &mut self.headers, h.as_ref()),
            &format!("BLOCK HEADER: {}", h),
        )
    }

    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&mut self, height: BlockHeight) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.get_ser(ColBlockHeight, &index_to_bytes(height)),
            &format!("BLOCK HEIGHT: {}", height),
        )
        // TODO: cache needs to be deleted when things get updated.
        //        option_to_not_found(
        //            read_with_cache(
        //                &*self.store,
        //                ColBlockHeight,
        //                &mut self.height,
        //                &index_to_bytes(height),
        //            ),
        //            &format!("BLOCK HEIGHT: {}", height),
        //        )
    }

    fn get_next_block_hash(&mut self, hash: &CryptoHash) -> Result<&CryptoHash, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColNextBlockHashes,
                &mut self.next_block_hashes,
                hash.as_ref(),
            ),
            &format!("NEXT BLOCK HASH: {}", hash),
        )
    }

    fn get_epoch_light_client_block(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<&LightClientBlockView, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColEpochLightClientBlocks,
                &mut self.epoch_light_client_blocks,
                hash.as_ref(),
            ),
            &format!("EPOCH LIGHT CLIENT BLOCK: {}", hash),
        )
    }

    fn get_any_block_hash_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<&HashMap<EpochId, CryptoHash>, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColBlockPerHeight,
                &mut self.block_hash_per_height,
                &index_to_bytes(height),
            ),
            &format!("BLOCK PER HEIGHT: {}", height),
        )
    }

    fn get_my_last_approval(&mut self, block_hash: &CryptoHash) -> Result<&Approval, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColMyLastApprovalsPerChain,
                &mut self.my_last_approvals,
                block_hash.as_ref(),
            ),
            &format!("MY LAST APPROVAL: {}", block_hash),
        )
    }

    fn get_last_approval_for_account(
        &mut self,
        account_id: &AccountId,
    ) -> Result<&Approval, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColLastApprovalPerAccount,
                &mut self.last_approvals_per_account,
                account_id.as_ref(),
            ),
            &format!("LAST APPROVAL FOR ACCOUNT: {}", account_id),
        )
    }

    fn get_outgoing_receipts(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColOutgoingReceipts,
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
    ) -> Result<&Vec<ReceiptProof>, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColIncomingReceipts,
                &mut self.incoming_receipts,
                &get_block_shard_id(block_hash, shard_id),
            ),
            &format!("INCOMING RECEIPT: {}", block_hash),
        )
    }

    fn get_execution_outcome(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<&ExecutionOutcomeWithIdAndProof, Error> {
        option_to_not_found(
            read_with_cache(&*self.store, ColTransactionResult, &mut self.outcomes, hash.as_ref()),
            &format!("TRANSACTION: {}", hash),
        )
    }

    fn get_blocks_to_catchup(&self, hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        Ok(self.store.get_ser(ColBlocksToCatchup, hash.as_ref())?.unwrap_or_else(|| vec![]))
    }

    fn get_latest_known(&mut self) -> Result<LatestKnown, Error> {
        if self.latest_known.is_none() {
            self.latest_known = Some(option_to_not_found(
                self.store.get_ser(ColBlockMisc, LATEST_KNOWN_KEY),
                "LATEST_KNOWN_KEY",
            )?);
        }
        Ok(self.latest_known.as_ref().unwrap().clone())
    }

    fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error> {
        let mut store_update = self.store.store_update();
        store_update.set_ser(ColBlockMisc, LATEST_KNOWN_KEY, &latest_known)?;
        self.latest_known = Some(latest_known);
        store_update.commit().map_err(|err| err.into())
    }

    fn is_block_challenged(&mut self, hash: &CryptoHash) -> Result<bool, Error> {
        return Ok(self
            .store
            .get_ser(ColChallengedBlocks, hash.as_ref())?
            .unwrap_or_else(|| false));
    }

    fn is_invalid_chunk(
        &mut self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<&EncodedShardChunk>, Error> {
        read_with_cache(
            &*self.store,
            ColInvalidChunks,
            &mut self.invalid_chunks,
            chunk_hash.as_ref(),
        )
        .map_err(|err| err.into())
    }

    fn get_shard_id_for_receipt_id(&mut self, receipt_id: &CryptoHash) -> Result<&ShardId, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColReceiptIdToShardId,
                &mut self.receipt_id_to_shard_id,
                receipt_id.as_ref(),
            ),
            &format!("RECEIPT ID: {}", receipt_id),
        )
    }

    fn get_next_block_hash_with_new_chunk(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: u64,
    ) -> Result<Option<&CryptoHash>, Error> {
        read_with_cache(
            &*self.store,
            ColNextBlockWithNewChunk,
            &mut self.next_block_with_new_chunk,
            &get_block_shard_id(block_hash, shard_id),
        )
        .map_err(|e| e.into())
    }

    fn get_last_block_with_new_chunk(
        &mut self,
        shard_id: u64,
    ) -> Result<Option<&CryptoHash>, Error> {
        read_with_cache(
            &*self.store,
            ColLastBlockWithNewChunk,
            &mut self.last_block_with_new_chunk,
            &index_to_bytes(shard_id),
        )
        .map_err(|e| e.into())
    }

    fn get_transaction(
        &mut self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<&SignedTransaction>, Error> {
        read_with_cache(&*self.store, ColTransactions, &mut self.transactions, tx_hash.as_ref())
            .map_err(|e| e.into())
    }
}

/// Cache update for ChainStore
struct ChainStoreCacheUpdate {
    blocks: HashMap<CryptoHash, Block>,
    deleted_blocks: HashSet<CryptoHash>,
    headers: HashMap<CryptoHash, BlockHeader>,
    block_extras: HashMap<CryptoHash, BlockExtra>,
    chunk_extras: HashMap<(CryptoHash, ShardId), ChunkExtra>,
    chunks: HashMap<ChunkHash, ShardChunk>,
    partial_chunks: HashMap<ChunkHash, PartialEncodedChunk>,
    block_hash_per_height: HashMap<BlockHeight, HashMap<EpochId, CryptoHash>>,
    height_to_hashes: HashMap<BlockHeight, Option<CryptoHash>>,
    next_block_hashes: HashMap<CryptoHash, CryptoHash>,
    epoch_light_client_blocks: HashMap<CryptoHash, LightClientBlockView>,
    my_last_approvals: HashMap<CryptoHash, Approval>,
    last_approvals_per_account: HashMap<AccountId, Approval>,
    outgoing_receipts: HashMap<(CryptoHash, ShardId), Vec<Receipt>>,
    incoming_receipts: HashMap<(CryptoHash, ShardId), Vec<ReceiptProof>>,
    outcomes: HashMap<CryptoHash, ExecutionOutcomeWithIdAndProof>,
    invalid_chunks: HashMap<ChunkHash, EncodedShardChunk>,
    receipt_id_to_shard_id: HashMap<CryptoHash, ShardId>,
    next_block_with_new_chunk: HashMap<(CryptoHash, ShardId), CryptoHash>,
    last_block_with_new_chunk: HashMap<ShardId, CryptoHash>,
    transactions: HashSet<SignedTransaction>,
}

impl ChainStoreCacheUpdate {
    pub fn new() -> Self {
        Self {
            blocks: Default::default(),
            deleted_blocks: Default::default(),
            headers: Default::default(),
            block_extras: Default::default(),
            chunk_extras: HashMap::default(),
            chunks: Default::default(),
            partial_chunks: Default::default(),
            block_hash_per_height: HashMap::default(),
            height_to_hashes: Default::default(),
            next_block_hashes: HashMap::default(),
            epoch_light_client_blocks: HashMap::default(),
            my_last_approvals: HashMap::default(),
            last_approvals_per_account: HashMap::default(),
            outgoing_receipts: HashMap::default(),
            incoming_receipts: HashMap::default(),
            outcomes: Default::default(),
            invalid_chunks: Default::default(),
            receipt_id_to_shard_id: Default::default(),
            next_block_with_new_chunk: Default::default(),
            last_block_with_new_chunk: Default::default(),
            transactions: Default::default(),
        }
    }
}

/// Provides layer to update chain without touching the underlying database.
/// This serves few purposes, main one is that even if executable exists/fails during update the database is in consistent state.
pub struct ChainStoreUpdate<'a> {
    chain_store: &'a mut ChainStore,
    store_updates: Vec<StoreUpdate>,
    /// Blocks added during this update. Takes ownership (unclear how to not do it because of failure exists).
    chain_store_cache_update: ChainStoreCacheUpdate,
    head: Option<Tip>,
    tail: Option<Tip>,
    header_head: Option<Tip>,
    sync_head: Option<Tip>,
    largest_approved_weight: Option<Weight>,
    largest_approved_score: Option<Weight>,
    trie_changes: Vec<WrappedTrieChanges>,
    add_blocks_to_catchup: Vec<(CryptoHash, CryptoHash)>,
    // A pair (prev_hash, hash) to be removed from blocks to catchup
    remove_blocks_to_catchup: Vec<(CryptoHash, CryptoHash)>,
    // A prev_hash to be removed with all the hashes associated with it
    remove_prev_blocks_to_catchup: Vec<CryptoHash>,
    add_state_dl_infos: Vec<StateSyncInfo>,
    remove_state_dl_infos: Vec<CryptoHash>,
    challenged_blocks: HashSet<CryptoHash>,
}

impl<'a> ChainStoreUpdate<'a> {
    pub fn new(chain_store: &'a mut ChainStore) -> Self {
        ChainStoreUpdate {
            chain_store,
            store_updates: vec![],
            chain_store_cache_update: ChainStoreCacheUpdate::new(),
            head: None,
            tail: None,
            header_head: None,
            sync_head: None,
            largest_approved_weight: None,
            largest_approved_score: None,
            trie_changes: vec![],
            add_blocks_to_catchup: vec![],
            remove_blocks_to_catchup: vec![],
            remove_prev_blocks_to_catchup: vec![],
            add_state_dl_infos: vec![],
            remove_state_dl_infos: vec![],
            challenged_blocks: HashSet::default(),
        }
    }

    pub fn get_incoming_receipts_for_shard(
        &mut self,
        shard_id: ShardId,
        mut block_hash: CryptoHash,
        last_chunk_height_included: BlockHeight,
    ) -> Result<Vec<ReceiptProofResponse>, Error> {
        let mut ret = vec![];

        loop {
            let header = self.get_block_header(&block_hash)?;

            if header.inner_lite.height < last_chunk_height_included {
                panic!("get_incoming_receipts_for_shard failed");
            }

            if header.inner_lite.height == last_chunk_height_included {
                break;
            }

            let prev_hash = header.prev_hash;

            if let Ok(receipt_proofs) = self.get_incoming_receipts(&block_hash, shard_id) {
                ret.push(ReceiptProofResponse(block_hash, receipt_proofs.clone()));
            } else {
                ret.push(ReceiptProofResponse(block_hash, vec![]));
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
    pub fn get_chain_store(&mut self) -> &mut ChainStore {
        self.chain_store
    }
}

impl<'a> ChainStoreAccess for ChainStoreUpdate<'a> {
    fn store(&self) -> &Store {
        &*self.chain_store.store
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

    fn largest_approved_weight(&self) -> Result<Weight, Error> {
        if let Some(largest_approved_weight) = &self.largest_approved_weight {
            Ok(largest_approved_weight.clone())
        } else {
            self.chain_store.largest_approved_weight()
        }
    }

    fn largest_approved_score(&self) -> Result<Weight, Error> {
        if let Some(largest_approved_score) = &self.largest_approved_score {
            Ok(largest_approved_score.clone())
        } else {
            self.chain_store.largest_approved_score()
        }
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.get_block_header(&(self.head()?.last_block_hash))
    }

    /// Get full block.
    fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error> {
        if let Some(block) = self.chain_store_cache_update.blocks.get(h) {
            Ok(block)
        } else {
            self.chain_store.get_block(h)
        }
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        Ok(self.chain_store_cache_update.blocks.contains_key(h)
            || self.chain_store.block_exists(h)?)
    }

    /// Get previous header.
    fn get_previous_header(&mut self, header: &BlockHeader) -> Result<&BlockHeader, Error> {
        self.get_block_header(&header.prev_hash)
    }

    fn get_block_extra(&mut self, block_hash: &CryptoHash) -> Result<&BlockExtra, Error> {
        if let Some(block_extra) = self.chain_store_cache_update.block_extras.get(block_hash) {
            Ok(block_extra)
        } else {
            self.chain_store.get_block_extra(block_hash)
        }
    }

    /// Get state root hash after applying header with given hash.
    fn get_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&ChunkExtra, Error> {
        if let Some(chunk_extra) =
            self.chain_store_cache_update.chunk_extras.get(&(*block_hash, shard_id))
        {
            Ok(chunk_extra)
        } else {
            self.chain_store.get_chunk_extra(block_hash, shard_id)
        }
    }

    /// Get block header.
    fn get_block_header(&mut self, hash: &CryptoHash) -> Result<&BlockHeader, Error> {
        if let Some(header) = self.chain_store_cache_update.headers.get(hash) {
            Ok(header)
        } else {
            self.chain_store.get_block_header(hash)
        }
    }

    /// Get block header from the current chain by height.
    fn get_block_hash_by_height(&mut self, height: BlockHeight) -> Result<CryptoHash, Error> {
        self.chain_store.get_block_hash_by_height(height)
    }

    fn get_any_block_hash_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<&HashMap<EpochId, CryptoHash>, Error> {
        self.chain_store.get_any_block_hash_by_height(height)
    }

    fn get_next_block_hash(&mut self, hash: &CryptoHash) -> Result<&CryptoHash, Error> {
        if let Some(next_hash) = self.chain_store_cache_update.next_block_hashes.get(hash) {
            Ok(next_hash)
        } else {
            self.chain_store.get_next_block_hash(hash)
        }
    }

    fn get_epoch_light_client_block(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<&LightClientBlockView, Error> {
        if let Some(light_client_block) =
            self.chain_store_cache_update.epoch_light_client_blocks.get(hash)
        {
            Ok(light_client_block)
        } else {
            self.chain_store.get_epoch_light_client_block(hash)
        }
    }

    fn get_my_last_approval(&mut self, block_hash: &CryptoHash) -> Result<&Approval, Error> {
        if let Some(approval) = self.chain_store_cache_update.my_last_approvals.get(block_hash) {
            Ok(approval)
        } else {
            self.chain_store.get_my_last_approval(block_hash)
        }
    }

    fn get_last_approval_for_account(
        &mut self,
        account_id: &AccountId,
    ) -> Result<&Approval, Error> {
        if let Some(approval) =
            self.chain_store_cache_update.last_approvals_per_account.get(account_id)
        {
            Ok(approval)
        } else {
            self.chain_store.get_last_approval_for_account(account_id)
        }
    }

    /// Get receipts produced for block with given hash.
    fn get_outgoing_receipts(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<Receipt>, Error> {
        if let Some(receipts) =
            self.chain_store_cache_update.outgoing_receipts.get(&(*hash, shard_id))
        {
            Ok(receipts)
        } else {
            self.chain_store.get_outgoing_receipts(hash, shard_id)
        }
    }

    /// Get receipts produced for block with given hash.
    fn get_incoming_receipts(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<&Vec<ReceiptProof>, Error> {
        if let Some(receipt_proofs) =
            self.chain_store_cache_update.incoming_receipts.get(&(*hash, shard_id))
        {
            Ok(receipt_proofs)
        } else {
            self.chain_store.get_incoming_receipts(hash, shard_id)
        }
    }

    fn get_execution_outcome(
        &mut self,
        hash: &CryptoHash,
    ) -> Result<&ExecutionOutcomeWithIdAndProof, Error> {
        self.chain_store.get_execution_outcome(hash)
    }

    fn get_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&ShardChunk, Error> {
        if let Some(chunk) = self.chain_store_cache_update.chunks.get(chunk_hash) {
            Ok(chunk)
        } else {
            self.chain_store.get_chunk(chunk_hash)
        }
    }

    fn get_partial_chunk(&mut self, chunk_hash: &ChunkHash) -> Result<&PartialEncodedChunk, Error> {
        if let Some(partial_chunk) = self.chain_store_cache_update.partial_chunks.get(chunk_hash) {
            Ok(partial_chunk)
        } else {
            self.chain_store.get_partial_chunk(chunk_hash)
        }
    }

    fn get_chunk_clone_from_header(
        &mut self,
        header: &ShardChunkHeader,
    ) -> Result<ShardChunk, Error> {
        if let Some(chunk) = self.chain_store_cache_update.chunks.get(&header.hash) {
            Ok(chunk.clone())
        } else {
            self.chain_store.get_chunk_clone_from_header(header)
        }
    }

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        // Make sure we never request a block to catchup after altering the data structure
        assert_eq!(self.add_blocks_to_catchup.len(), 0);
        assert_eq!(self.remove_blocks_to_catchup.len(), 0);
        assert_eq!(self.remove_prev_blocks_to_catchup.len(), 0);

        self.chain_store.get_blocks_to_catchup(prev_hash)
    }

    fn get_latest_known(&mut self) -> Result<LatestKnown, Error> {
        self.chain_store.get_latest_known()
    }

    fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error> {
        self.chain_store.save_latest_known(latest_known)
    }

    fn is_block_challenged(&mut self, hash: &CryptoHash) -> Result<bool, Error> {
        if self.challenged_blocks.contains(&hash) {
            return Ok(true);
        }
        self.chain_store.is_block_challenged(hash)
    }

    fn is_invalid_chunk(
        &mut self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<&EncodedShardChunk>, Error> {
        if let Some(chunk) = self.chain_store_cache_update.invalid_chunks.get(&chunk_hash) {
            Ok(Some(chunk))
        } else {
            self.chain_store.is_invalid_chunk(chunk_hash)
        }
    }

    fn get_shard_id_for_receipt_id(&mut self, receipt_id: &CryptoHash) -> Result<&u64, Error> {
        if let Some(shard_id) = self.chain_store_cache_update.receipt_id_to_shard_id.get(receipt_id)
        {
            Ok(shard_id)
        } else {
            self.chain_store.get_shard_id_for_receipt_id(receipt_id)
        }
    }

    fn get_next_block_hash_with_new_chunk(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: u64,
    ) -> Result<Option<&CryptoHash>, Error> {
        if let Some(hash) =
            self.chain_store_cache_update.next_block_with_new_chunk.get(&(*block_hash, shard_id))
        {
            Ok(Some(hash))
        } else {
            self.chain_store.get_next_block_hash_with_new_chunk(block_hash, shard_id)
        }
    }

    fn get_last_block_with_new_chunk(
        &mut self,
        shard_id: u64,
    ) -> Result<Option<&CryptoHash>, Error> {
        if let Some(hash) = self.chain_store_cache_update.last_block_with_new_chunk.get(&shard_id) {
            Ok(Some(hash))
        } else {
            self.chain_store.get_last_block_with_new_chunk(shard_id)
        }
    }

    fn get_transaction(
        &mut self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<&SignedTransaction>, Error> {
        if let Some(tx) = self.chain_store_cache_update.transactions.get(tx_hash) {
            Ok(Some(tx))
        } else {
            self.chain_store.get_transaction(tx_hash)
        }
    }
}

impl<'a> ChainStoreUpdate<'a> {
    /// Update both header and block body head.
    pub fn save_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.save_body_head(t)?;
        self.save_header_head_if_not_challenged(t)
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

    fn update_height_if_not_challenged(
        &mut self,
        height: BlockHeight,
        hash: CryptoHash,
    ) -> Result<(), Error> {
        let mut prev_hash = hash;
        let mut prev_height = height;
        loop {
            let header = self.get_block_header(&prev_hash)?;
            let (header_height, header_hash, header_prev_hash) =
                (header.inner_lite.height, header.hash(), header.prev_hash);
            // Clean up block indicies between blocks.
            for height in (header_height + 1)..prev_height {
                self.chain_store_cache_update.height_to_hashes.insert(height, None);
            }
            match self.get_block_hash_by_height(header_height) {
                Ok(cur_hash) if cur_hash == header_hash => {
                    // Found common ancestor.
                    return Ok(());
                }
                _ => {
                    if self.is_block_challenged(&header_hash)? {
                        return Err(ErrorKind::ChallengedBlockOnChain.into());
                    }
                    self.chain_store_cache_update
                        .height_to_hashes
                        .insert(header_height, Some(header_hash));
                    self.chain_store_cache_update
                        .next_block_hashes
                        .insert(header_prev_hash, header_hash);
                    prev_hash = header_prev_hash;
                    prev_height = header_height;
                }
            };
        }
    }

    /// Update header head and height to hash index for this branch.
    pub fn save_header_head_if_not_challenged(&mut self, t: &Tip) -> Result<(), Error> {
        if t.height > 0 {
            self.update_height_if_not_challenged(t.height, t.prev_block_hash)?;
        }
        self.try_save_latest_known(t.height)?;

        match &self.header_head() {
            Ok(prev_tip) => {
                if prev_tip.height > t.height {
                    for height in (t.height + 1)..=prev_tip.height {
                        self.chain_store_cache_update.height_to_hashes.insert(height, None);
                    }
                }
            }
            Err(err) => match err.kind() {
                ErrorKind::DBNotFoundErr(_) => {}
                e => return Err(e.into()),
            },
        }

        self.chain_store_cache_update.height_to_hashes.insert(t.height, Some(t.last_block_hash));
        self.chain_store_cache_update
            .next_block_hashes
            .insert(t.prev_block_hash, t.last_block_hash);
        self.header_head = Some(t.clone());
        Ok(())
    }

    /// Save "sync" head.
    pub fn save_sync_head(&mut self, t: &Tip) {
        self.sync_head = Some(t.clone());
    }

    pub fn save_largest_approved_weight(&mut self, weight: &Weight) {
        self.largest_approved_weight = Some(weight.clone());
    }

    pub fn save_largest_approved_score(&mut self, score: &Weight) {
        self.largest_approved_score = Some(score.clone());
    }

    /// Save new height if it's above currently latest known.
    pub fn try_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let latest_known = self.get_latest_known().ok();
        if latest_known.is_none() || height > latest_known.unwrap().height {
            self.save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        }
        Ok(())
    }

    /// Save block.
    pub fn save_block(&mut self, block: Block) {
        self.chain_store_cache_update.blocks.insert(block.hash(), block);
    }

    /// Save post applying block extra info.
    pub fn save_block_extra(&mut self, block_hash: &CryptoHash, block_extra: BlockExtra) {
        self.chain_store_cache_update.block_extras.insert(*block_hash, block_extra);
    }

    /// Save post applying chunk extra info.
    pub fn save_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        chunk_extra: ChunkExtra,
    ) {
        self.chain_store_cache_update.chunk_extras.insert((*block_hash, shard_id), chunk_extra);
    }

    pub fn save_chunk(&mut self, chunk_hash: &ChunkHash, chunk: ShardChunk) {
        self.chain_store_cache_update.chunks.insert(chunk_hash.clone(), chunk);
    }

    pub fn save_partial_chunk(
        &mut self,
        chunk_hash: &ChunkHash,
        partial_chunk: PartialEncodedChunk,
    ) {
        self.chain_store_cache_update.partial_chunks.insert(chunk_hash.clone(), partial_chunk);
    }

    pub fn delete_block(&mut self, hash: &CryptoHash) {
        self.chain_store_cache_update.deleted_blocks.insert(*hash);
    }

    pub fn save_block_header(&mut self, header: BlockHeader) {
        self.chain_store_cache_update.headers.insert(header.hash(), header);
    }

    pub fn save_next_block_hash(&mut self, hash: &CryptoHash, next_hash: CryptoHash) {
        self.chain_store_cache_update.next_block_hashes.insert(hash.clone(), next_hash);
    }

    pub fn save_epoch_light_client_block(
        &mut self,
        epoch_hash: &CryptoHash,
        light_client_block: LightClientBlockView,
    ) {
        self.chain_store_cache_update
            .epoch_light_client_blocks
            .insert(epoch_hash.clone(), light_client_block);
    }

    pub fn save_my_last_approval(&mut self, block_hash: &CryptoHash, approval: Approval) {
        self.chain_store_cache_update.my_last_approvals.insert(block_hash.clone(), approval);
    }

    pub fn save_last_approval_for_account(&mut self, account_id: &AccountId, approval: Approval) {
        self.chain_store_cache_update
            .last_approvals_per_account
            .insert(account_id.clone(), approval);
    }

    pub fn save_outgoing_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        receipt: Vec<Receipt>,
    ) {
        self.chain_store_cache_update.outgoing_receipts.insert((*hash, shard_id), receipt);
    }

    pub fn save_incoming_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        receipt_proof: Vec<ReceiptProof>,
    ) {
        self.chain_store_cache_update.incoming_receipts.insert((*hash, shard_id), receipt_proof);
    }

    pub fn save_outcomes_with_proofs(
        &mut self,
        block_hash: &CryptoHash,
        outcomes: Vec<ExecutionOutcomeWithId>,
        proofs: Vec<MerklePath>,
    ) {
        for (outcome_with_id, proof) in outcomes.into_iter().zip(proofs.into_iter()) {
            self.chain_store_cache_update.outcomes.insert(
                outcome_with_id.id,
                ExecutionOutcomeWithIdAndProof { outcome_with_id, proof, block_hash: *block_hash },
            );
        }
    }

    pub fn save_transactions(&mut self, transactions: Vec<SignedTransaction>) {
        for transaction in transactions {
            self.chain_store_cache_update.transactions.insert(transaction);
        }
    }

    pub fn save_outcome_with_proof(
        &mut self,
        id: CryptoHash,
        outcome_with_proof: ExecutionOutcomeWithIdAndProof,
    ) {
        self.chain_store_cache_update.outcomes.insert(id, outcome_with_proof);
    }

    pub fn save_trie_changes(&mut self, trie_changes: WrappedTrieChanges) {
        self.trie_changes.push(trie_changes);
    }

    pub fn add_block_to_catchup(&mut self, prev_hash: CryptoHash, block_hash: CryptoHash) {
        self.add_blocks_to_catchup.push((prev_hash, block_hash));
    }

    pub fn remove_block_to_catchup(&mut self, prev_hash: CryptoHash, hash: CryptoHash) {
        self.remove_blocks_to_catchup.push((prev_hash, hash));
    }

    pub fn remove_prev_block_to_catchup(&mut self, hash: CryptoHash) {
        self.remove_prev_blocks_to_catchup.push(hash);
    }

    pub fn add_state_dl_info(&mut self, info: StateSyncInfo) {
        self.add_state_dl_infos.push(info);
    }

    pub fn remove_state_dl_info(&mut self, hash: CryptoHash) {
        self.remove_state_dl_infos.push(hash);
    }

    pub fn save_challenged_block(&mut self, hash: CryptoHash) {
        self.challenged_blocks.insert(hash);
    }

    pub fn save_invalid_chunk(&mut self, chunk: EncodedShardChunk) {
        self.chain_store_cache_update.invalid_chunks.insert(chunk.chunk_hash(), chunk);
    }

    pub fn save_receipt_shard_id(&mut self, receipt_id: CryptoHash, shard_id: ShardId) {
        self.chain_store_cache_update.receipt_id_to_shard_id.insert(receipt_id, shard_id);
    }

    pub fn save_block_hash_with_new_chunk(&mut self, block_hash: CryptoHash, shard_id: ShardId) {
        if let Ok(Some(&last_block_hash)) = self.get_last_block_with_new_chunk(shard_id) {
            self.chain_store_cache_update
                .next_block_with_new_chunk
                .insert((last_block_hash, shard_id), block_hash);
        }
        self.chain_store_cache_update.last_block_with_new_chunk.insert(shard_id, block_hash);
    }

    /// Merge another StoreUpdate into this one
    pub fn merge(&mut self, store_update: StoreUpdate) {
        self.store_updates.push(store_update);
    }

    fn finalize(&mut self) -> Result<StoreUpdate, Error> {
        let mut store_update = self.store().store_update();
        if let Some(t) = self.head.take() {
            store_update.set_ser(ColBlockMisc, HEAD_KEY, &t).map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.tail.take() {
            store_update.set_ser(ColBlockMisc, TAIL_KEY, &t).map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.header_head.take() {
            store_update
                .set_ser(ColBlockMisc, HEADER_HEAD_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.sync_head.take() {
            store_update
                .set_ser(ColBlockMisc, SYNC_HEAD_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.largest_approved_weight {
            store_update
                .set_ser(ColBlockMisc, LARGEST_APPROVED_WEIGHT_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        if let Some(t) = self.largest_approved_score {
            store_update
                .set_ser(ColBlockMisc, LARGEST_APPROVED_SCORE_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, block) in self.chain_store_cache_update.blocks.iter() {
            store_update
                .set_ser(ColBlock, hash.as_ref(), block)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for hash in self.chain_store_cache_update.deleted_blocks.iter() {
            store_update.delete(ColBlock, hash.as_ref());
        }
        for (hash, header) in self.chain_store_cache_update.headers.iter() {
            let map = match self.chain_store.get_any_block_hash_by_height(header.inner_lite.height)
            {
                Ok(m) => {
                    if !m.contains_key(&header.inner_lite.epoch_id) {
                        Some(m.clone())
                    } else {
                        None
                    }
                }
                Err(_) => Some(HashMap::new()),
            };
            if let Some(mut new_map) = map {
                new_map.insert(header.inner_lite.epoch_id.clone(), *hash);
                store_update
                    .set_ser(ColBlockPerHeight, &index_to_bytes(header.inner_lite.height), &new_map)
                    .map_err::<Error, _>(|e| e.into())?;
                self.chain_store_cache_update
                    .block_hash_per_height
                    .insert(header.inner_lite.height, new_map);
            }
            store_update
                .set_ser(ColBlockHeader, hash.as_ref(), header)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for ((block_hash, shard_id), chunk_extra) in
            self.chain_store_cache_update.chunk_extras.iter()
        {
            store_update
                .set_ser(ColChunkExtra, &get_block_shard_id(block_hash, *shard_id), chunk_extra)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (block_hash, block_extra) in self.chain_store_cache_update.block_extras.iter() {
            store_update
                .set_ser(ColBlockExtra, block_hash.as_ref(), block_extra)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (chunk_hash, chunk) in self.chain_store_cache_update.chunks.iter() {
            store_update
                .set_ser(ColChunks, chunk_hash.as_ref(), chunk)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (chunk_hash, partial_chunk) in self.chain_store_cache_update.partial_chunks.iter() {
            store_update
                .set_ser(ColPartialChunks, chunk_hash.as_ref(), partial_chunk)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (height, hash) in self.chain_store_cache_update.height_to_hashes.iter() {
            if let Some(hash) = hash {
                store_update
                    .set_ser(ColBlockHeight, &index_to_bytes(*height), hash)
                    .map_err::<Error, _>(|e| e.into())?;
            } else {
                store_update.delete(ColBlockHeight, &index_to_bytes(*height));
            }
        }
        for (block_hash, next_hash) in self.chain_store_cache_update.next_block_hashes.iter() {
            store_update.set_ser(ColNextBlockHashes, block_hash.as_ref(), next_hash)?;
        }
        for (epoch_hash, light_client_block) in
            self.chain_store_cache_update.epoch_light_client_blocks.iter()
        {
            store_update.set_ser(
                ColEpochLightClientBlocks,
                epoch_hash.as_ref(),
                light_client_block,
            )?;
        }
        for (block_hash, approval) in self.chain_store_cache_update.my_last_approvals.iter() {
            store_update.set_ser(ColMyLastApprovalsPerChain, block_hash.as_ref(), approval)?;
        }
        for (account_id, approval) in
            self.chain_store_cache_update.last_approvals_per_account.iter()
        {
            store_update.set_ser(ColLastApprovalPerAccount, account_id.as_ref(), approval)?;
        }
        for ((block_hash, shard_id), receipt) in
            self.chain_store_cache_update.outgoing_receipts.iter()
        {
            store_update.set_ser(
                ColOutgoingReceipts,
                &get_block_shard_id(block_hash, *shard_id),
                receipt,
            )?;
        }
        for ((block_hash, shard_id), receipt) in
            self.chain_store_cache_update.incoming_receipts.iter()
        {
            store_update.set_ser(
                ColIncomingReceipts,
                &get_block_shard_id(block_hash, *shard_id),
                receipt,
            )?;
        }
        for (hash, outcome) in self.chain_store_cache_update.outcomes.iter() {
            store_update.set_ser(ColTransactionResult, hash.as_ref(), outcome)?;
        }
        for (receipt_id, shard_id) in self.chain_store_cache_update.receipt_id_to_shard_id.iter() {
            store_update.set_ser(ColReceiptIdToShardId, receipt_id.as_ref(), shard_id)?;
        }
        for ((block_hash, shard_id), next_block_hash) in
            self.chain_store_cache_update.next_block_with_new_chunk.iter()
        {
            store_update.set_ser(
                ColNextBlockWithNewChunk,
                &get_block_shard_id(block_hash, *shard_id),
                next_block_hash,
            )?;
        }
        for (shard_id, block_hash) in self.chain_store_cache_update.last_block_with_new_chunk.iter()
        {
            store_update.set_ser(
                ColLastBlockWithNewChunk,
                &index_to_bytes(*shard_id),
                block_hash,
            )?;
        }
        for transaction in self.chain_store_cache_update.transactions.iter() {
            store_update.set_ser(ColTransactions, transaction.get_hash().as_ref(), transaction)?;
        }
        for trie_changes in self.trie_changes.drain(..) {
            trie_changes
                .insertions_into(&mut store_update)
                .map_err(|err| ErrorKind::Other(err.to_string()))?;
            // TODO: save deletions separately for garbage collection.
        }
        let mut affected_catchup_blocks = HashSet::new();
        for (prev_hash, hash) in self.remove_blocks_to_catchup.drain(..) {
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

            let mut remove_idx = prev_table.len();
            for (i, val) in prev_table.iter().enumerate() {
                if *val == hash {
                    remove_idx = i;
                }
            }

            assert_ne!(remove_idx, prev_table.len());
            prev_table.swap_remove(remove_idx);

            if prev_table.len() > 0 {
                store_update.set_ser(ColBlocksToCatchup, prev_hash.as_ref(), &prev_table)?;
            } else {
                store_update.delete(ColBlocksToCatchup, prev_hash.as_ref());
            }
        }
        for prev_hash in self.remove_prev_blocks_to_catchup.drain(..) {
            assert!(!affected_catchup_blocks.contains(&prev_hash));
            if affected_catchup_blocks.contains(&prev_hash) {
                return Err(ErrorKind::Other(
                    "Multiple changes to the store affect the same catchup block".to_string(),
                )
                .into());
            }
            affected_catchup_blocks.insert(prev_hash);

            store_update.delete(ColBlocksToCatchup, prev_hash.as_ref());
        }
        for (prev_hash, new_hash) in self.add_blocks_to_catchup.drain(..) {
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
            store_update.set_ser(ColBlocksToCatchup, prev_hash.as_ref(), &prev_table)?;
        }
        for state_dl_info in self.add_state_dl_infos.drain(..) {
            store_update.set_ser(
                ColStateDlInfos,
                state_dl_info.epoch_tail_hash.as_ref(),
                &state_dl_info,
            )?;
        }
        for hash in self.remove_state_dl_infos.drain(..) {
            store_update.delete(ColStateDlInfos, hash.as_ref());
        }
        for hash in self.challenged_blocks.drain() {
            store_update.set_ser(ColChallengedBlocks, hash.as_ref(), &true)?;
        }
        for (chunk_hash, chunk) in self.chain_store_cache_update.invalid_chunks.iter() {
            store_update.set_ser(ColInvalidChunks, chunk_hash.as_ref(), chunk)?;
        }
        for other in self.store_updates.drain(..) {
            store_update.merge(other);
        }
        Ok(store_update)
    }

    pub fn commit(mut self) -> Result<(), Error> {
        let store_update = self.finalize()?;
        store_update.commit()?;
        let ChainStoreCacheUpdate {
            blocks,
            deleted_blocks,
            headers,
            block_extras,
            chunk_extras,
            chunks,
            partial_chunks,
            block_hash_per_height,
            height_to_hashes,
            next_block_hashes,
            epoch_light_client_blocks,
            last_approvals_per_account,
            my_last_approvals,
            outgoing_receipts,
            incoming_receipts,
            outcomes,
            invalid_chunks,
            receipt_id_to_shard_id,
            next_block_with_new_chunk,
            last_block_with_new_chunk,
            transactions,
        } = self.chain_store_cache_update;
        for (hash, block) in blocks {
            self.chain_store.blocks.cache_set(hash.into(), block);
        }
        for hash in deleted_blocks {
            self.chain_store.blocks.cache_remove(&hash.into());
        }
        for (hash, header) in headers {
            self.chain_store.headers.cache_set(hash.into(), header);
        }
        for (hash, block_extra) in block_extras {
            self.chain_store.block_extras.cache_set(hash.into(), block_extra);
        }
        for ((block_hash, shard_id), chunk_extra) in chunk_extras {
            let key = get_block_shard_id(&block_hash, shard_id);
            self.chain_store.chunk_extras.cache_set(key, chunk_extra);
        }
        for (hash, chunk) in chunks {
            self.chain_store.chunks.cache_set(hash.into(), chunk);
        }
        for (hash, partial_chunk) in partial_chunks {
            self.chain_store.partial_chunks.cache_set(hash.into(), partial_chunk);
        }
        for (height, epoch_id_to_hash) in block_hash_per_height {
            self.chain_store
                .block_hash_per_height
                .cache_set(index_to_bytes(height), epoch_id_to_hash);
        }
        for (height, block_hash) in height_to_hashes {
            let bytes = index_to_bytes(height);
            if let Some(hash) = block_hash {
                self.chain_store.height.cache_set(bytes, hash);
            } else {
                self.chain_store.height.cache_remove(&bytes);
            }
        }
        for (account_id, approval) in last_approvals_per_account {
            self.chain_store.last_approvals_per_account.cache_set(account_id.into(), approval);
        }
        for (block_hash, next_hash) in next_block_hashes {
            self.chain_store.next_block_hashes.cache_set(block_hash.into(), next_hash);
        }
        for (epoch_hash, light_client_block) in epoch_light_client_blocks {
            self.chain_store
                .epoch_light_client_blocks
                .cache_set(epoch_hash.into(), light_client_block);
        }
        for (block_hash, approval) in my_last_approvals {
            self.chain_store.my_last_approvals.cache_set(block_hash.into(), approval);
        }
        for ((block_hash, shard_id), shard_outgoing_receipts) in outgoing_receipts {
            let key = get_block_shard_id(&block_hash, shard_id);
            self.chain_store.outgoing_receipts.cache_set(key, shard_outgoing_receipts);
        }
        for ((block_hash, shard_id), shard_incoming_receipts) in incoming_receipts {
            let key = get_block_shard_id(&block_hash, shard_id);
            self.chain_store.incoming_receipts.cache_set(key, shard_incoming_receipts);
        }
        for (hash, outcome) in outcomes {
            self.chain_store.outcomes.cache_set(hash.into(), outcome);
        }
        for (hash, invalid_chunk) in invalid_chunks {
            self.chain_store.invalid_chunks.cache_set(hash.into(), invalid_chunk);
        }
        for (receipt_id, shard_id) in receipt_id_to_shard_id {
            self.chain_store.receipt_id_to_shard_id.cache_set(receipt_id.into(), shard_id);
        }
        for ((block_hash, shard_id), next_block_hash) in next_block_with_new_chunk {
            self.chain_store
                .next_block_with_new_chunk
                .cache_set(get_block_shard_id(&block_hash, shard_id), next_block_hash);
        }
        for (shard_id, block_hash) in last_block_with_new_chunk {
            self.chain_store
                .last_block_with_new_chunk
                .cache_set(index_to_bytes(shard_id), block_hash);
        }
        for transaction in transactions {
            self.chain_store.transactions.cache_set(transaction.get_hash().into(), transaction);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::KeyValueRuntime;
    use crate::{Chain, ChainGenesis};
    use borsh::ser::BorshSerialize;
    use cached::Cached;
    use near_crypto::{InMemorySigner, KeyType, Signer};
    use near_primitives::block::Block;
    use near_primitives::errors::InvalidTxError;
    use near_primitives::hash::hash;
    use near_primitives::types::EpochId;
    use near_primitives::utils::index_to_bytes;
    use near_store::test_utils::create_test_store;
    use std::sync::Arc;

    fn get_chain() -> Chain {
        let store = create_test_store();
        let chain_genesis = ChainGenesis::test();
        let validators = vec![vec!["test1"]];
        let runtime_adapter = Arc::new(KeyValueRuntime::new_with_validators(
            store.clone(),
            validators
                .into_iter()
                .map(|inner| inner.into_iter().map(Into::into).collect())
                .collect(),
            1,
            1,
            10,
        ));
        Chain::new(store.clone(), runtime_adapter, &chain_genesis).unwrap()
    }

    #[test]
    fn test_header_cache_long_fork() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer = Arc::new(InMemorySigner::from_seed("test1", KeyType::ED25519, "test1"));
        let short_fork = vec![Block::empty_with_height(&genesis, 1, &*signer.clone())];
        let mut store_update = chain.mut_store().store_update();
        store_update.save_block_header(short_fork[0].header.clone());
        store_update.commit().unwrap();

        let short_fork_head = short_fork[0].clone().header;
        assert!(chain
            .mut_store()
            .check_blocks_on_same_chain(
                &short_fork_head,
                &genesis.hash(),
                transaction_validity_period
            )
            .is_ok());
        let mut long_fork = vec![];
        let mut prev_block = genesis.clone();
        let mut store_update = chain.mut_store().store_update();
        for i in 1..(transaction_validity_period + 2) {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header.clone());
            long_fork.push(block);
        }
        store_update.commit().unwrap();
        let valid_base_hash = long_fork[1].hash();
        let cur_header = &long_fork.last().unwrap().header;
        assert!(chain
            .mut_store()
            .check_blocks_on_same_chain(cur_header, &valid_base_hash, transaction_validity_period)
            .is_ok());
        let invalid_base_hash = long_fork[0].hash();
        assert_eq!(
            chain.mut_store().check_blocks_on_same_chain(
                cur_header,
                &invalid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
        assert_eq!(
            chain.store().header_history.queue.clone().into_iter().collect::<Vec<_>>(),
            long_fork
                .iter()
                .rev()
                .take(transaction_validity_period as usize)
                .map(|h| h.hash())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_header_cache_normal_case() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer = Arc::new(InMemorySigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut blocks = vec![];
        let mut prev_block = genesis.clone();
        let mut store_update = chain.mut_store().store_update();
        for i in 1..(transaction_validity_period + 2) {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header.clone());
            blocks.push(block);
        }
        store_update.commit().unwrap();
        let valid_base_hash = blocks[1].hash();
        let cur_header = &blocks.last().unwrap().header;
        assert!(chain
            .mut_store()
            .check_blocks_on_same_chain(cur_header, &valid_base_hash, transaction_validity_period)
            .is_ok());
        assert_eq!(chain.store().header_history.len(), transaction_validity_period as usize);
        let new_block = Block::empty_with_height(
            &blocks.last().unwrap(),
            transaction_validity_period + 2,
            &*signer.clone(),
        );
        let mut store_update = chain.mut_store().store_update();
        store_update.save_block_header(new_block.header.clone());
        store_update.commit().unwrap();
        assert_eq!(
            chain.mut_store().check_blocks_on_same_chain(
                &new_block.header,
                &valid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_header_cache_off_by_one() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer = Arc::new(InMemorySigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut short_fork = vec![];
        let mut prev_block = genesis.clone();
        let mut store_update = chain.mut_store().store_update();
        for i in 1..(transaction_validity_period + 1) {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header.clone());
            short_fork.push(block);
        }
        store_update.commit().unwrap();

        let short_fork_head = short_fork.last().unwrap().clone().header;
        assert_eq!(
            chain.mut_store().check_blocks_on_same_chain(
                &short_fork_head,
                &genesis.hash(),
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
        let mut long_fork = vec![];
        let mut prev_block = genesis.clone();
        let mut store_update = chain.mut_store().store_update();
        for i in 1..(transaction_validity_period * 5) {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header.clone());
            long_fork.push(block);
        }
        store_update.commit().unwrap();
        let long_fork_head = &long_fork.last().unwrap().header;
        assert_eq!(
            chain.mut_store().check_blocks_on_same_chain(
                long_fork_head,
                &genesis.hash(),
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_cache_invalidation() {
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer = Arc::new(InMemorySigner::from_seed("test1", KeyType::ED25519, "test1"));
        let block1 = Block::empty_with_height(&genesis, 1, &*signer.clone());
        let mut block2 = block1.clone();
        block2.header.inner_lite.epoch_id = EpochId(hash(&[1, 2, 3]));
        let bytes = block2.header.try_to_vec().unwrap();
        block2.header.hash = hash(&bytes);
        block2.header.signature = signer.sign(block2.header.hash.as_ref());

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[1])));
        store_update.chain_store_cache_update.headers.insert(block1.hash(), block1.header);
        store_update.commit().unwrap();

        let block_hash = chain.mut_store().height.cache_get(&index_to_bytes(1)).cloned();
        let epoch_id_to_hash =
            chain.mut_store().block_hash_per_height.cache_get(&index_to_bytes(1)).cloned();

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[2])));
        store_update.chain_store_cache_update.headers.insert(block2.header.hash, block2.header);
        store_update.commit().unwrap();

        let block_hash1 = chain.mut_store().height.cache_get(&index_to_bytes(1)).cloned();
        let epoch_id_to_hash1 =
            chain.mut_store().block_hash_per_height.cache_get(&index_to_bytes(1)).cloned();

        assert_ne!(block_hash, block_hash1);
        assert_ne!(epoch_id_to_hash, epoch_id_to_hash1);
    }
}
