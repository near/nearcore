use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use cached::{Cached, SizedCache};
use chrono::Utc;

use near_chain_primitives::error::{Error, ErrorKind};
use near_primitives::block::{Approval, Tip};
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, ReceiptProof, ShardChunk, ShardChunkHeader,
    StateSyncInfo,
};
use near_primitives::syncing::{
    get_num_state_parts, ReceiptProofResponse, ReceiptResponse, ShardStateSyncResponseHeader,
    StateHeaderKey, StatePartKey,
};
use near_primitives::transaction::{
    ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof, SignedTransaction,
};
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    AccountId, BlockExtra, BlockHeight, EpochId, GCCount, NumBlocks, ShardId, StateChanges,
    StateChangesExt, StateChangesKinds, StateChangesKindsExt, StateChangesRequest,
};
use near_primitives::utils::{get_block_shard_id, index_to_bytes, to_timestamp};
use near_primitives::views::LightClientBlockView;
use near_store::{
    read_with_cache, ColBlock, ColBlockExtra, ColBlockHeader, ColBlockHeight, ColBlockInfo,
    ColBlockMerkleTree, ColBlockMisc, ColBlockOrdinal, ColBlockPerHeight, ColBlockRefCount,
    ColBlocksToCatchup, ColChallengedBlocks, ColChunkExtra, ColChunkHashesByHeight,
    ColChunkPerHeightShard, ColChunks, ColEpochLightClientBlocks, ColGCCount,
    ColHeaderHashesByHeight, ColIncomingReceipts, ColInvalidChunks, ColLastBlockWithNewChunk,
    ColNextBlockHashes, ColNextBlockWithNewChunk, ColOutcomeIds, ColOutgoingReceipts,
    ColPartialChunks, ColProcessedBlockHeights, ColReceiptIdToShardId, ColReceipts, ColState,
    ColStateChanges, ColStateDlInfos, ColStateHeaders, ColStateParts, ColTransactionResult,
    ColTransactions, ColTrieChanges, DBCol, KeyForStateChanges, ShardTries, Store, StoreUpdate,
    TrieChanges, WrappedTrieChanges, CHUNK_TAIL_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY,
    HEADER_HEAD_KEY, HEAD_KEY, LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, SHOULD_COL_GC,
    TAIL_KEY,
};

use crate::types::{Block, BlockHeader, LatestKnown};
use crate::{byzantine_assert, ReceiptResult};

/// lru cache size
#[cfg(not(feature = "no_cache"))]
const CACHE_SIZE: usize = 100;
#[cfg(not(feature = "no_cache"))]
const CHUNK_CACHE_SIZE: usize = 1024;

#[cfg(feature = "no_cache")]
const CACHE_SIZE: usize = 1;
#[cfg(feature = "no_cache")]
const CHUNK_CACHE_SIZE: usize = 1;

#[derive(Clone)]
pub enum GCMode {
    Fork(ShardTries),
    Canonical(ShardTries),
    StateSync { clear_block_info: bool },
}

fn get_height_shard_id(height: BlockHeight, shard_id: ShardId) -> Vec<u8> {
    let mut res = Vec::with_capacity(40);
    res.extend_from_slice(&height.to_le_bytes());
    res.extend_from_slice(&shard_id.to_le_bytes());
    res
}

/// Accesses the chain store. Used to create atomic editable views that can be reverted.
pub trait ChainStoreAccess {
    /// Returns underlaying store.
    fn store(&self) -> &Store;
    /// The chain head.
    fn head(&self) -> Result<Tip, Error>;
    /// The chain Blocks Tail height.
    fn tail(&self) -> Result<BlockHeight, Error>;
    /// The chain Chunks Tail height.
    fn chunk_tail(&self) -> Result<BlockHeight, Error>;
    /// Tail height of the fork cleaning process.
    fn fork_tail(&self) -> Result<BlockHeight, Error>;
    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error>;
    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error>;
    /// The chain final head. It is guaranteed to be monotonically increasing.
    fn final_head(&self) -> Result<Tip, Error>;
    /// Larget approval target height sent by us
    fn largest_target_height(&self) -> Result<BlockHeight, Error>;
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
                byzantine_assert!(header.height_included() > 0 || header.height_created() == 0);
                if header.height_included() == 0 && header.height_created() > 0 {
                    return Err(ErrorKind::Other(format!(
                        "Invalid header: {:?} for chunk {:?}",
                        header, shard_chunk
                    ))
                    .into());
                }
                let mut shard_chunk_clone = shard_chunk.clone();
                shard_chunk_clone.set_height_included(header.height_included());
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
    /// Returns hash of the first available block after genesis.
    fn get_earliest_block_hash(&mut self) -> Result<Option<CryptoHash>, Error> {
        // To find the earliest available block we use the `tail` marker primarily
        // used by garbage collection system.
        // NOTE: `tail` is the block height at which we can say that there is
        // at most 1 block available in the range from the genesis height to
        // the tail. Thus, the strategy is to find the first block AFTER the tail
        // height, and use the `prev_hash` to get the reference to the earliest
        // block.
        let head_header_height = self.head_header()?.height();
        let tail = self.tail()?;

        // There is a corner case when there are no blocks after the tail, and
        // the tail is in fact the earliest block available on the chain.
        if let Ok(block_hash) = self.get_block_hash_by_height(tail) {
            return Ok(Some(block_hash.clone()));
        }
        for height in tail + 1..=head_header_height {
            if let Ok(block_hash) = self.get_block_hash_by_height(height) {
                let earliest_block_hash = self.get_block_header(&block_hash)?.prev_hash().clone();
                debug_assert!(matches!(self.block_exists(&earliest_block_hash), Ok(true)));
                return Ok(Some(earliest_block_hash));
            }
        }
        Ok(None)
    }
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
    /// Returns a number of references for Block with `block_hash`
    fn get_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<&u64, Error>;
    /// Check if we saw chunk hash at given height and shard id.
    fn get_any_chunk_hash_by_height_shard(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<&ChunkHash, Error>;
    /// Returns block header from the current chain defined by `sync_hash` for given height if present.
    fn get_header_on_chain_by_height(
        &mut self,
        sync_hash: &CryptoHash,
        height: BlockHeight,
    ) -> Result<&BlockHeader, Error> {
        let mut header = self.get_block_header(sync_hash)?;
        let mut hash = sync_hash.clone();
        while header.height() > height {
            hash = *header.prev_hash();
            header = self.get_block_header(&hash)?;
        }
        let header_height = header.height();
        if header_height < height {
            return Err(ErrorKind::InvalidBlockHeight(header_height).into());
        }
        self.get_block_header(&hash)
    }
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
    /// Returns whether the block with the given hash was challenged
    fn is_block_challenged(&mut self, hash: &CryptoHash) -> Result<bool, Error>;

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error>;

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

    fn get_receipt(&mut self, receipt_id: &CryptoHash) -> Result<Option<&Receipt>, Error>;

    fn get_genesis_height(&self) -> BlockHeight;

    fn get_block_merkle_tree(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&PartialMerkleTree, Error>;

    fn get_block_hash_from_ordinal(
        &mut self,
        block_ordinal: NumBlocks,
    ) -> Result<&CryptoHash, Error>;

    fn get_block_merkle_tree_from_ordinal(
        &mut self,
        block_ordinal: NumBlocks,
    ) -> Result<&PartialMerkleTree, Error> {
        let block_hash = *self.get_block_hash_from_ordinal(block_ordinal)?;
        self.get_block_merkle_tree(&block_hash)
    }

    fn is_height_processed(&mut self, height: BlockHeight) -> Result<bool, Error>;

    fn get_block_height(&mut self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            Ok(self.get_genesis_height())
        } else {
            Ok(self.get_block_header(hash)?.height())
        }
    }
}

/// All chain-related database operations.
pub struct ChainStore {
    store: Arc<Store>,
    /// Genesis block height.
    genesis_height: BlockHeight,
    /// Latest known.
    latest_known: Option<LatestKnown>,
    /// Current head of the chain
    head: Option<Tip>,
    /// Tail height of the chain,
    tail: Option<BlockHeight>,
    /// Cache with headers.
    headers: SizedCache<Vec<u8>, BlockHeader>,
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
    /// Cache with height to hash on the main chain.
    height: SizedCache<Vec<u8>, CryptoHash>,
    /// Cache with height to block hash on any chain.
    block_hash_per_height: SizedCache<Vec<u8>, HashMap<EpochId, HashSet<CryptoHash>>>,
    /// Cache with height and shard_id to any chunk hash.
    chunk_hash_per_height_shard: SizedCache<Vec<u8>, ChunkHash>,
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
    /// Receipts
    receipts: SizedCache<Vec<u8>, Receipt>,
    /// Cache with Block Refcounts
    block_refcounts: SizedCache<Vec<u8>, u64>,
    /// Cache of block hash -> block merkle tree at the current block
    block_merkle_tree: SizedCache<Vec<u8>, PartialMerkleTree>,
    /// Cache of block ordinal to block hash.
    block_ordinal_to_hash: SizedCache<Vec<u8>, CryptoHash>,
    /// Processed block heights.
    processed_block_heights: SizedCache<Vec<u8>, ()>,
}

pub fn option_to_not_found<T>(res: io::Result<Option<T>>, field_name: &str) -> Result<T, Error> {
    match res {
        Ok(Some(o)) => Ok(o),
        Ok(None) => Err(ErrorKind::DBNotFoundErr(field_name.to_owned()).into()),
        Err(e) => Err(e.into()),
    }
}

impl ChainStore {
    pub fn new(store: Arc<Store>, genesis_height: BlockHeight) -> ChainStore {
        ChainStore {
            store,
            genesis_height,
            latest_known: None,
            head: None,
            tail: None,
            blocks: SizedCache::with_size(CACHE_SIZE),
            headers: SizedCache::with_size(CACHE_SIZE),
            chunks: SizedCache::with_size(CHUNK_CACHE_SIZE),
            partial_chunks: SizedCache::with_size(CHUNK_CACHE_SIZE),
            block_extras: SizedCache::with_size(CACHE_SIZE),
            chunk_extras: SizedCache::with_size(CACHE_SIZE),
            height: SizedCache::with_size(CACHE_SIZE),
            block_hash_per_height: SizedCache::with_size(CACHE_SIZE),
            block_refcounts: SizedCache::with_size(CACHE_SIZE),
            chunk_hash_per_height_shard: SizedCache::with_size(CACHE_SIZE),
            next_block_hashes: SizedCache::with_size(CACHE_SIZE),
            epoch_light_client_blocks: SizedCache::with_size(CACHE_SIZE),
            my_last_approvals: SizedCache::with_size(CACHE_SIZE),
            last_approvals_per_account: SizedCache::with_size(CACHE_SIZE),
            outgoing_receipts: SizedCache::with_size(CACHE_SIZE),
            incoming_receipts: SizedCache::with_size(CACHE_SIZE),
            invalid_chunks: SizedCache::with_size(CACHE_SIZE),
            receipt_id_to_shard_id: SizedCache::with_size(CHUNK_CACHE_SIZE),
            next_block_with_new_chunk: SizedCache::with_size(CHUNK_CACHE_SIZE),
            last_block_with_new_chunk: SizedCache::with_size(CHUNK_CACHE_SIZE),
            transactions: SizedCache::with_size(CHUNK_CACHE_SIZE),
            receipts: SizedCache::with_size(CHUNK_CACHE_SIZE),
            block_merkle_tree: SizedCache::with_size(CACHE_SIZE),
            block_ordinal_to_hash: SizedCache::with_size(CACHE_SIZE),
            processed_block_heights: SizedCache::with_size(CACHE_SIZE),
        }
    }

    pub fn owned_store(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn store_update(&mut self) -> ChainStoreUpdate<'_> {
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

            if block_header.height() == last_included_height {
                let receipts = if let Ok(cur_receipts) =
                    self.get_outgoing_receipts(&receipts_block_hash, shard_id)
                {
                    cur_receipts.clone()
                } else {
                    vec![]
                };
                return Ok(ReceiptResponse(receipts_block_hash, receipts));
            } else {
                receipts_block_hash = *block_header.prev_hash();
            }
        }
    }

    /// For a given transaction, it expires if the block that the chunk points to is more than `validity_period`
    /// ahead of the block that has `base_block_hash`.
    pub fn check_transaction_validity_period(
        &mut self,
        prev_block_header: &BlockHeader,
        base_block_hash: &CryptoHash,
        validity_period: BlockHeight,
    ) -> Result<(), InvalidTxError> {
        // if both are on the canonical chain, comparing height is sufficient
        // we special case this because it is expected that this scenario will happen in most cases.
        let base_height =
            self.get_block_header(base_block_hash).map_err(|_| InvalidTxError::Expired)?.height();
        let prev_height = prev_block_header.height();
        if let Ok(base_block_hash_by_height) = self.get_block_hash_by_height(base_height) {
            if &base_block_hash_by_height == base_block_hash {
                if let Ok(prev_hash) = self.get_block_hash_by_height(prev_height) {
                    if &prev_hash == prev_block_header.hash() {
                        if prev_height <= base_height + validity_period {
                            return Ok(());
                        } else {
                            return Err(InvalidTxError::Expired);
                        }
                    }
                }
            }
        }

        // if the base block height is smaller than `last_final_height` we only need to check
        // whether the base block is the same as the one with that height on the canonical fork.
        // Otherwise we walk back the chain to check whether base block is on the same chain.
        let last_final_height = self
            .get_block_height(&prev_block_header.last_final_block())
            .map_err(|_| InvalidTxError::InvalidChain)?;

        if prev_height > base_height + validity_period {
            Err(InvalidTxError::Expired)
        } else if last_final_height >= base_height {
            let base_block_hash_by_height = self
                .get_block_hash_by_height(base_height)
                .map_err(|_| InvalidTxError::InvalidChain)?;
            if &base_block_hash_by_height == base_block_hash {
                if prev_height <= base_height + validity_period {
                    Ok(())
                } else {
                    Err(InvalidTxError::Expired)
                }
            } else {
                Err(InvalidTxError::InvalidChain)
            }
        } else {
            let header = self
                .get_header_on_chain_by_height(prev_block_header.hash(), base_height)
                .map_err(|_| InvalidTxError::InvalidChain)?;
            if header.hash() == base_block_hash {
                Ok(())
            } else {
                Err(InvalidTxError::InvalidChain)
            }
        }
    }
}

impl ChainStore {
    /// Returns all outcomes generated by applying transaction or receipt with the given id.
    pub fn get_outcomes_by_id(
        &self,
        id: &CryptoHash,
    ) -> Result<Vec<ExecutionOutcomeWithIdAndProof>, Error> {
        Ok(self.store.get_ser(ColTransactionResult, id.as_ref())?.unwrap_or_else(|| vec![]))
    }

    /// Returns a vector of Outcome ids for given block and shard id
    pub fn get_outcomes_by_block_hash_and_shard_id(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Vec<CryptoHash>, Error> {
        Ok(self
            .store
            .get_ser(ColOutcomeIds, &get_block_shard_id(block_hash, shard_id))?
            .unwrap_or_default())
    }

    /// Returns a hashmap of epoch id -> set of all blocks got for current (height, epoch_id)
    pub fn get_all_block_hashes_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<&HashMap<EpochId, HashSet<CryptoHash>>, Error> {
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

    /// Returns a HashSet of Chunk Hashes for current Height
    pub fn get_all_chunk_hashes_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<HashSet<ChunkHash>, Error> {
        Ok(self.store.get_ser(ColChunkHashesByHeight, &index_to_bytes(height))?.unwrap_or_default())
    }

    /// Returns a HashSet of Header Hashes for current Height
    pub fn get_all_header_hashes_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<HashSet<CryptoHash>, Error> {
        Ok(self
            .store
            .get_ser(ColHeaderHashesByHeight, &index_to_bytes(height))?
            .unwrap_or_default())
    }

    pub fn get_state_header(
        &mut self,
        shard_id: ShardId,
        block_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        let key = StateHeaderKey(shard_id, block_hash).try_to_vec()?;
        match self.store.get_ser(ColStateHeaders, &key) {
            Ok(Some(header)) => Ok(header),
            _ => Err(ErrorKind::Other("Cannot get shard_state_header".into()).into()),
        }
    }

    /// Returns latest known height and time it was seen.
    pub fn get_latest_known(&mut self) -> Result<LatestKnown, Error> {
        if self.latest_known.is_none() {
            self.latest_known = Some(option_to_not_found(
                self.store.get_ser(ColBlockMisc, LATEST_KNOWN_KEY),
                "LATEST_KNOWN_KEY",
            )?);
        }
        Ok(self.latest_known.as_ref().unwrap().clone())
    }

    /// Save the latest known.
    pub fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error> {
        let mut store_update = self.store.store_update();
        store_update.set_ser(ColBlockMisc, LATEST_KNOWN_KEY, &latest_known)?;
        self.latest_known = Some(latest_known);
        store_update.commit().map_err(|err| err.into())
    }

    /// Retrieve the kinds of state changes occurred in a given block.
    ///
    /// We store different types of data, so we prefer to only expose minimal information about the
    /// changes (i.e. a kind of the change and an account id).
    pub fn get_state_changes_in_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<StateChangesKinds, Error> {
        // We store the trie changes under a compound key: `block_hash + trie_key`, so when we
        // query the changes, we reverse the process by splitting the key using simple slicing of an
        // array of bytes, essentially, extracting `trie_key`.
        //
        // Example: data changes are stored under a key:
        //
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR + user_specified_key)
        //
        // Thus, to query the list of touched accounts we do the following:
        // 1. Query RocksDB for `block_hash` prefix.
        // 2. Extract the original Trie key out of the keys returned by RocksDB
        // 3. Try extracting `account_id` from the key using KeyFor* implementations

        let storage_key = KeyForStateChanges::get_prefix(&block_hash);

        let mut block_changes = storage_key.find_iter(&self.store);

        Ok(StateChangesKinds::from_changes(&mut block_changes)?)
    }

    pub fn get_state_changes_with_cause_in_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<StateChanges, Error> {
        let storage_key = KeyForStateChanges::get_prefix(&block_hash);

        let mut block_changes = storage_key.find_iter(&self.store);

        Ok(StateChanges::from_changes(&mut block_changes)?)
    }

    /// Retrieve the key-value changes from the store and decode them appropriately.
    ///
    /// We store different types of data, so we need to take care of all the types. That is, the
    /// account data and the access keys are internally-serialized and we have to deserialize those
    /// values appropriately. Code and data changes are simple blobs of data, so we return them as
    /// base64-encoded blobs.
    pub fn get_state_changes(
        &self,
        block_hash: &CryptoHash,
        state_changes_request: &StateChangesRequest,
    ) -> Result<StateChanges, Error> {
        // We store the trie changes under a compound key: `block_hash + trie_key`, so when we
        // query the changes, we reverse the process by splitting the key using simple slicing of an
        // array of bytes, essentially, extracting `trie_key`.
        //
        // Example: data changes are stored under a key:
        //
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR + user_specified_key)
        //
        // Thus, to query all the changes by a user-specified key prefix, we do the following:
        // 1. Query RocksDB for
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR + user_specified_key_prefix)
        //
        // 2. In the simplest case, to extract the full key we need to slice the RocksDB key by a length of
        //     block_hash + (col::ACCOUNT + account_id + ACCOUNT_DATA_SEPARATOR)
        //
        //    In this implementation, however, we decoupled this process into two steps:
        //
        //    2.1. Split off the `block_hash` (internally in `KeyForStateChanges`), thus we are
        //         left working with a key that was used in the trie.
        //    2.2. Parse the trie key with a relevant KeyFor* implementation to ensure consistency

        Ok(match state_changes_request {
            StateChangesRequest::AccountChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = TrieKey::Account { account_id: account_id.clone() }.to_vec();
                    let storage_key = KeyForStateChanges::new(&block_hash, data_key.as_ref());
                    let changes_per_key = storage_key.find_exact_iter(&self.store);
                    changes.extend(StateChanges::from_account_changes(changes_per_key)?);
                }
                changes
            }
            StateChangesRequest::SingleAccessKeyChanges { keys } => {
                let mut changes = StateChanges::new();
                for key in keys {
                    let data_key = TrieKey::AccessKey {
                        account_id: key.account_id.clone(),
                        public_key: key.public_key.clone(),
                    }
                    .to_vec();
                    let storage_key = KeyForStateChanges::new(&block_hash, data_key.as_ref());
                    let changes_per_key = storage_key.find_exact_iter(&self.store);
                    changes.extend(StateChanges::from_access_key_changes(changes_per_key)?);
                }
                changes
            }
            StateChangesRequest::AllAccessKeyChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = trie_key_parsers::get_raw_prefix_for_access_keys(account_id);
                    let storage_key = KeyForStateChanges::new(&block_hash, data_key.as_ref());
                    let changes_per_key_prefix = storage_key.find_iter(&self.store);
                    changes.extend(StateChanges::from_access_key_changes(changes_per_key_prefix)?);
                }
                changes
            }
            StateChangesRequest::ContractCodeChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key =
                        TrieKey::ContractCode { account_id: account_id.clone() }.to_vec();
                    let storage_key = KeyForStateChanges::new(&block_hash, data_key.as_ref());
                    let changes_per_key = storage_key.find_exact_iter(&self.store);
                    changes.extend(StateChanges::from_contract_code_changes(changes_per_key)?);
                }
                changes
            }
            StateChangesRequest::DataChanges { account_ids, key_prefix } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = trie_key_parsers::get_raw_prefix_for_contract_data(
                        account_id,
                        key_prefix.as_ref(),
                    );
                    let storage_key = KeyForStateChanges::new(&block_hash, data_key.as_ref());
                    let changes_per_key_prefix = storage_key.find_iter(&self.store);
                    changes.extend(StateChanges::from_data_changes(changes_per_key_prefix)?);
                }
                changes
            }
        })
    }
}

impl ChainStoreAccess for ChainStore {
    fn store(&self) -> &Store {
        &*self.store
    }
    /// The chain head.
    fn head(&self) -> Result<Tip, Error> {
        if let Some(ref tip) = self.head {
            Ok(tip.clone())
        } else {
            option_to_not_found(self.store.get_ser(ColBlockMisc, HEAD_KEY), "HEAD")
        }
    }

    /// The chain Blocks Tail height, used by GC.
    fn tail(&self) -> Result<BlockHeight, Error> {
        if let Some(tail) = self.tail.as_ref() {
            Ok(*tail)
        } else {
            self.store
                .get_ser(ColBlockMisc, TAIL_KEY)
                .map(|option| option.unwrap_or_else(|| self.genesis_height))
                .map_err(|e| e.into())
        }
    }

    /// The chain Chunks Tail height, used by GC.
    fn chunk_tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(ColBlockMisc, CHUNK_TAIL_KEY)
            .map(|option| option.unwrap_or_else(|| self.genesis_height))
            .map_err(|e| e.into())
    }

    fn fork_tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(ColBlockMisc, FORK_TAIL_KEY)
            .map(|option| option.unwrap_or_else(|| self.genesis_height))
            .map_err(|e| e.into())
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error> {
        self.get_block_header(&self.head()?.last_block_hash)
    }

    /// Largest height for which we created a doomslug endorsement
    fn largest_target_height(&self) -> Result<BlockHeight, Error> {
        match self.store.get_ser(ColBlockMisc, LARGEST_TARGET_HEIGHT_KEY) {
            Ok(Some(o)) => Ok(o),
            Ok(None) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(ColBlockMisc, HEADER_HEAD_KEY), "HEADER_HEAD")
    }

    /// Final head of the chain.
    fn final_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(ColBlockMisc, FINAL_HEAD_KEY), "FINAL HEAD")
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
        self.get_block_header(header.prev_hash())
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

    fn get_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<&u64, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColBlockRefCount,
                &mut self.block_refcounts,
                block_hash.as_ref(),
            ),
            &format!("BLOCK REFCOUNT: {}", block_hash),
        )
    }

    fn get_any_chunk_hash_by_height_shard(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<&ChunkHash, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColChunkPerHeightShard,
                &mut self.chunk_hash_per_height_shard,
                &get_height_shard_id(height, shard_id),
            ),
            &format!("CHUNK PER HEIGHT AND SHARD ID: {} {}", height, shard_id),
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

    fn get_blocks_to_catchup(&self, hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        Ok(self.store.get_ser(ColBlocksToCatchup, hash.as_ref())?.unwrap_or_else(|| vec![]))
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

    fn get_receipt(&mut self, receipt_id: &CryptoHash) -> Result<Option<&Receipt>, Error> {
        read_with_cache(&*self.store, ColReceipts, &mut self.receipts, receipt_id.as_ref())
            .map_err(|e| e.into())
    }

    fn get_genesis_height(&self) -> BlockHeight {
        self.genesis_height
    }

    fn get_block_merkle_tree(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&PartialMerkleTree, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColBlockMerkleTree,
                &mut self.block_merkle_tree,
                block_hash.as_ref(),
            ),
            &format!("BLOCK MERKLE TREE: {}", block_hash),
        )
    }

    fn get_block_hash_from_ordinal(
        &mut self,
        block_ordinal: NumBlocks,
    ) -> Result<&CryptoHash, Error> {
        option_to_not_found(
            read_with_cache(
                &*self.store,
                ColBlockOrdinal,
                &mut self.block_ordinal_to_hash,
                &index_to_bytes(block_ordinal),
            ),
            &format!("BLOCK ORDINAL: {}", block_ordinal),
        )
    }

    fn is_height_processed(&mut self, height: BlockHeight) -> Result<bool, Error> {
        read_with_cache(
            &*self.store,
            ColProcessedBlockHeights,
            &mut self.processed_block_heights,
            &index_to_bytes(height),
        )
        .map(|r| r.is_some())
        .map_err(|e| e.into())
    }
}

/// Cache update for ChainStore
#[derive(Default)]
struct ChainStoreCacheUpdate {
    blocks: HashMap<CryptoHash, Block>,
    headers: HashMap<CryptoHash, BlockHeader>,
    block_extras: HashMap<CryptoHash, BlockExtra>,
    chunk_extras: HashMap<(CryptoHash, ShardId), ChunkExtra>,
    chunks: HashMap<ChunkHash, ShardChunk>,
    partial_chunks: HashMap<ChunkHash, PartialEncodedChunk>,
    block_hash_per_height: HashMap<BlockHeight, HashMap<EpochId, HashSet<CryptoHash>>>,
    chunk_hash_per_height_shard: HashMap<(BlockHeight, ShardId), ChunkHash>,
    height_to_hashes: HashMap<BlockHeight, Option<CryptoHash>>,
    next_block_hashes: HashMap<CryptoHash, CryptoHash>,
    epoch_light_client_blocks: HashMap<CryptoHash, LightClientBlockView>,
    my_last_approvals: HashMap<CryptoHash, Approval>,
    last_approvals_per_account: HashMap<AccountId, Approval>,
    outgoing_receipts: HashMap<(CryptoHash, ShardId), Vec<Receipt>>,
    incoming_receipts: HashMap<(CryptoHash, ShardId), Vec<ReceiptProof>>,
    outcomes: HashMap<CryptoHash, Vec<ExecutionOutcomeWithIdAndProof>>,
    outcome_ids: HashMap<(CryptoHash, ShardId), Vec<CryptoHash>>,
    invalid_chunks: HashMap<ChunkHash, EncodedShardChunk>,
    receipt_id_to_shard_id: HashMap<CryptoHash, ShardId>,
    next_block_with_new_chunk: HashMap<(CryptoHash, ShardId), CryptoHash>,
    last_block_with_new_chunk: HashMap<ShardId, CryptoHash>,
    transactions: HashSet<SignedTransaction>,
    receipts: HashMap<CryptoHash, Receipt>,
    block_refcounts: HashMap<CryptoHash, u64>,
    block_merkle_tree: HashMap<CryptoHash, PartialMerkleTree>,
    block_ordinal_to_hash: HashMap<NumBlocks, CryptoHash>,
    gc_count: HashMap<DBCol, GCCount>,
    processed_block_heights: HashSet<BlockHeight>,
}

/// Provides layer to update chain without touching the underlying database.
/// This serves few purposes, main one is that even if executable exists/fails during update the database is in consistent state.
pub struct ChainStoreUpdate<'a> {
    chain_store: &'a mut ChainStore,
    store_updates: Vec<StoreUpdate>,
    /// Blocks added during this update. Takes ownership (unclear how to not do it because of failure exists).
    chain_store_cache_update: ChainStoreCacheUpdate,
    head: Option<Tip>,
    tail: Option<BlockHeight>,
    chunk_tail: Option<BlockHeight>,
    fork_tail: Option<BlockHeight>,
    header_head: Option<Tip>,
    final_head: Option<Tip>,
    largest_target_height: Option<BlockHeight>,
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
            chain_store_cache_update: ChainStoreCacheUpdate::default(),
            head: None,
            tail: None,
            chunk_tail: None,
            fork_tail: None,
            header_head: None,
            final_head: None,
            largest_target_height: None,
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

            if header.height() < last_chunk_height_included {
                panic!("get_incoming_receipts_for_shard failed");
            }

            if header.height() == last_chunk_height_included {
                break;
            }

            let prev_hash = *header.prev_hash();

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

    /// The chain Block Tail height, used by GC.
    fn tail(&self) -> Result<BlockHeight, Error> {
        if let Some(tail) = &self.tail {
            Ok(tail.clone())
        } else {
            self.chain_store.tail()
        }
    }

    /// The chain Chunks Tail height, used by GC.
    fn chunk_tail(&self) -> Result<BlockHeight, Error> {
        if let Some(chunk_tail) = &self.chunk_tail {
            Ok(chunk_tail.clone())
        } else {
            self.chain_store.chunk_tail()
        }
    }

    /// Fork tail used by GC
    fn fork_tail(&self) -> Result<BlockHeight, Error> {
        if let Some(fork_tail) = &self.fork_tail {
            Ok(fork_tail.clone())
        } else {
            self.chain_store.fork_tail()
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

    fn final_head(&self) -> Result<Tip, Error> {
        if let Some(final_head) = self.final_head.as_ref() {
            Ok(final_head.clone())
        } else {
            self.chain_store.final_head()
        }
    }

    fn largest_target_height(&self) -> Result<BlockHeight, Error> {
        if let Some(largest_target_height) = &self.largest_target_height {
            Ok(largest_target_height.clone())
        } else {
            self.chain_store.largest_target_height()
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
        self.get_block_header(header.prev_hash())
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
        match self.chain_store_cache_update.height_to_hashes.get(&height) {
            Some(Some(hash)) => Ok(*hash),
            Some(None) => Err(ErrorKind::DBNotFoundErr(format!("BLOCK HEIGHT: {}", height)).into()),
            None => self.chain_store.get_block_hash_by_height(height),
        }
    }

    fn get_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<&u64, Error> {
        if let Some(refcount) = self.chain_store_cache_update.block_refcounts.get(block_hash) {
            Ok(refcount)
        } else {
            let refcount = match self.chain_store.get_block_refcount(block_hash) {
                Ok(refcount) => refcount,
                Err(e) => match e.kind() {
                    ErrorKind::DBNotFoundErr(_) => &0,
                    _ => return Err(e),
                },
            };
            Ok(refcount)
        }
    }

    fn get_any_chunk_hash_by_height_shard(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<&ChunkHash, Error> {
        if let Some(chunk_hash) =
            self.chain_store_cache_update.chunk_hash_per_height_shard.get(&(height, shard_id))
        {
            Ok(chunk_hash)
        } else {
            self.chain_store.get_any_chunk_hash_by_height_shard(height, shard_id)
        }
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
        if let Some(chunk) = self.chain_store_cache_update.chunks.get(&header.chunk_hash()) {
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

    fn get_receipt(&mut self, receipt_id: &CryptoHash) -> Result<Option<&Receipt>, Error> {
        if let Some(receipt) = self.chain_store_cache_update.receipts.get(receipt_id) {
            Ok(Some(receipt))
        } else {
            self.chain_store.get_receipt(&receipt_id)
        }
    }

    fn get_genesis_height(&self) -> BlockHeight {
        self.chain_store.genesis_height
    }

    fn get_block_merkle_tree(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&PartialMerkleTree, Error> {
        if let Some(merkle_tree) = self.chain_store_cache_update.block_merkle_tree.get(block_hash) {
            Ok(merkle_tree)
        } else {
            self.chain_store.get_block_merkle_tree(block_hash)
        }
    }

    fn get_block_hash_from_ordinal(
        &mut self,
        block_ordinal: NumBlocks,
    ) -> Result<&CryptoHash, Error> {
        if let Some(block_hash) =
            self.chain_store_cache_update.block_ordinal_to_hash.get(&block_ordinal)
        {
            Ok(block_hash)
        } else {
            self.chain_store.get_block_hash_from_ordinal(block_ordinal)
        }
    }

    fn is_height_processed(&mut self, height: BlockHeight) -> Result<bool, Error> {
        if self.chain_store_cache_update.processed_block_heights.contains(&height) {
            Ok(true)
        } else {
            self.chain_store.is_height_processed(height)
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

    pub fn save_final_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.final_head = Some(t.clone());
        Ok(())
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
                (header.height(), *header.hash(), *header.prev_hash());
            // Clean up block indices between blocks.
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

    /// Save header head in Epoch Sync
    /// Checking validity of header head is delegated to Epoch Sync methods
    pub fn force_save_header_head(&mut self, t: &Tip) -> Result<(), Error> {
        self.try_save_latest_known(t.height)?;

        // TODO #3488
        // Bowen: It seems that height_to_hashes is used to update ColBlockHeight, which stores blocks,
        // not block headers, by height. Therefore I wonder whether this line here breaks some invariant
        // since now we potentially don't have the corresponding block in storage.

        //self.chain_tore_cache_update.height_to_hashes.insert(t.height, Some(t.last_block_hash));
        //self.chain_store_cache_update.next_block_hashes.insert(t.prev_block_hash, t.last_block_hash);
        self.header_head = Some(t.clone());
        Ok(())
    }

    /// Update header head and height to hash index for this branch.
    pub fn save_header_head_if_not_challenged(&mut self, t: &Tip) -> Result<(), Error> {
        if t.height > self.chain_store.genesis_height {
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

    pub fn save_largest_target_height(&mut self, height: BlockHeight) {
        self.largest_target_height = Some(height);
    }

    /// Save new height if it's above currently latest known.
    pub fn try_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let latest_known = self.chain_store.get_latest_known().ok();
        if latest_known.is_none() || height > latest_known.unwrap().height {
            self.chain_store
                .save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        }
        Ok(())
    }

    #[cfg(feature = "adversarial")]
    pub fn adv_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let header = self.get_header_by_height(height)?;
        let tip = Tip::from_header(&header);
        self.chain_store
            .save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        self.save_head(&tip)?;
        Ok(())
    }

    /// Save block.
    pub fn save_block(&mut self, block: Block) {
        self.chain_store_cache_update.blocks.insert(*block.hash(), block);
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

    pub fn save_chunk(&mut self, chunk: ShardChunk) {
        for transaction in chunk.transactions() {
            self.chain_store_cache_update.transactions.insert(transaction.clone());
        }
        for receipt in chunk.receipts() {
            self.chain_store_cache_update.receipts.insert(receipt.receipt_id, receipt.clone());
        }
        self.chain_store_cache_update.chunks.insert(chunk.chunk_hash(), chunk);
    }

    pub fn save_partial_chunk(&mut self, partial_chunk: PartialEncodedChunk) {
        self.chain_store_cache_update
            .partial_chunks
            .insert(partial_chunk.chunk_hash(), partial_chunk);
    }

    pub fn save_block_merkle_tree(
        &mut self,
        block_hash: CryptoHash,
        block_merkle_tree: PartialMerkleTree,
    ) {
        self.chain_store_cache_update
            .block_ordinal_to_hash
            .insert(block_merkle_tree.size(), block_hash);
        self.chain_store_cache_update.block_merkle_tree.insert(block_hash, block_merkle_tree);
    }

    fn update_and_save_block_merkle_tree(&mut self, header: &BlockHeader) -> Result<(), Error> {
        let prev_hash = *header.prev_hash();
        if prev_hash == CryptoHash::default() {
            self.save_block_merkle_tree(*header.hash(), PartialMerkleTree::default());
        } else {
            let mut block_merkle_tree = self.get_block_merkle_tree(&prev_hash)?.clone();
            block_merkle_tree.insert(prev_hash);
            self.save_block_merkle_tree(*header.hash(), block_merkle_tree);
        }
        Ok(())
    }

    /// Used only in Epoch Sync finalization
    /// Validity of Header is checked by Epoch Sync methods
    pub fn save_block_header_no_update_tree(&mut self, header: BlockHeader) -> Result<(), Error> {
        self.chain_store_cache_update.headers.insert(*header.hash(), header);
        Ok(())
    }

    pub fn save_block_header(&mut self, header: BlockHeader) -> Result<(), Error> {
        self.update_and_save_block_merkle_tree(&header)?;
        self.chain_store_cache_update.headers.insert(*header.hash(), header);
        Ok(())
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

    pub fn save_outgoing_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        receipt_result: ReceiptResult,
    ) {
        let mut outgoing_receipts = Vec::new();
        for (receipt_shard_id, receipts) in receipt_result {
            for receipt in receipts {
                self.chain_store_cache_update
                    .receipt_id_to_shard_id
                    .insert(receipt.receipt_id, receipt_shard_id);
                outgoing_receipts.push(receipt);
            }
        }
        self.chain_store_cache_update
            .outgoing_receipts
            .insert((*hash, shard_id), outgoing_receipts);
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
        shard_id: ShardId,
        outcomes: Vec<ExecutionOutcomeWithId>,
        proofs: Vec<MerklePath>,
    ) {
        let mut outcome_ids = Vec::with_capacity(outcomes.len());
        for (outcome_with_id, proof) in outcomes.into_iter().zip(proofs.into_iter()) {
            outcome_ids.push(outcome_with_id.id);
            self.chain_store_cache_update
                .outcomes
                .entry(outcome_with_id.id)
                .or_insert_with(Vec::new)
                .push(ExecutionOutcomeWithIdAndProof {
                    outcome_with_id,
                    proof,
                    block_hash: *block_hash,
                })
        }
        self.chain_store_cache_update.outcome_ids.insert((*block_hash, shard_id), outcome_ids);
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

    pub fn save_block_hash_with_new_chunk(&mut self, block_hash: CryptoHash, shard_id: ShardId) {
        if let Ok(Some(&last_block_hash)) = self.get_last_block_with_new_chunk(shard_id) {
            self.chain_store_cache_update
                .next_block_with_new_chunk
                .insert((last_block_hash, shard_id), block_hash);
        }
        self.chain_store_cache_update.last_block_with_new_chunk.insert(shard_id, block_hash);
    }

    pub fn save_chunk_hash(
        &mut self,
        height: BlockHeight,
        shard_id: ShardId,
        chunk_hash: ChunkHash,
    ) {
        self.chain_store_cache_update
            .chunk_hash_per_height_shard
            .insert((height, shard_id), chunk_hash);
    }

    pub fn save_block_height_processed(&mut self, height: BlockHeight) {
        self.chain_store_cache_update.processed_block_heights.insert(height);
    }

    pub fn inc_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let refcount = match self.get_block_refcount(block_hash) {
            Ok(refcount) => refcount.clone(),
            Err(e) => match e.kind() {
                ErrorKind::DBNotFoundErr(_) => 0,
                _ => return Err(e),
            },
        };
        self.chain_store_cache_update.block_refcounts.insert(*block_hash, refcount + 1);
        Ok(())
    }

    pub fn dec_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let refcount = self.get_block_refcount(block_hash)?.clone();
        if refcount > 0 {
            self.chain_store_cache_update.block_refcounts.insert(*block_hash, refcount - 1);
            Ok(())
        } else {
            debug_assert!(false, "refcount can not be negative");
            Err(ErrorKind::Other(format!("cannot decrease refcount for {:?}", block_hash)).into())
        }
    }

    pub fn reset_tail(&mut self) {
        self.tail = None;
        self.chunk_tail = None;
        self.fork_tail = None;
    }

    pub fn update_tail(&mut self, height: BlockHeight) {
        self.tail = Some(height);
        let genesis_height = self.get_genesis_height();
        // When fork tail is behind tail, it doesn't hurt to set it to tail for consistency.
        if self.fork_tail.unwrap_or(genesis_height) < height {
            self.fork_tail = Some(height);
        }

        let chunk_tail = self.chunk_tail().unwrap_or(genesis_height);
        if chunk_tail == genesis_height {
            // For consistency, Chunk Tail should be set if Tail is set
            self.chunk_tail = Some(self.get_genesis_height());
        }
    }

    pub fn update_fork_tail(&mut self, height: BlockHeight) {
        self.fork_tail = Some(height);
    }

    pub fn update_chunk_tail(&mut self, height: BlockHeight) {
        self.chunk_tail = Some(height);
    }

    pub fn clear_chunk_data_and_headers(
        &mut self,
        min_chunk_height: BlockHeight,
    ) -> Result<(), Error> {
        let chunk_tail = self.chunk_tail()?;
        for height in chunk_tail..min_chunk_height {
            let chunk_hashes = self.chain_store.get_all_chunk_hashes_by_height(height)?;
            for chunk_hash in chunk_hashes {
                // 1. Delete chunk-related data
                let chunk = self.get_chunk(&chunk_hash)?.clone();
                debug_assert_eq!(chunk.cloned_header().height_created(), height);
                for transaction in chunk.transactions() {
                    self.gc_col(ColTransactions, &transaction.get_hash().into());
                }
                for receipt in chunk.receipts() {
                    self.gc_col(ColReceipts, &receipt.get_hash().into());
                }

                // 2. Delete chunk_hash-indexed data
                let chunk_header_hash = chunk_hash.clone().into();
                self.gc_col(ColChunks, &chunk_header_hash);
                self.gc_col(ColPartialChunks, &chunk_header_hash);
                self.gc_col(ColInvalidChunks, &chunk_header_hash);
            }

            let header_hashes = self.chain_store.get_all_header_hashes_by_height(height)?;
            for _header_hash in header_hashes {
                // 3. Delete header_hash-indexed data
                // TODO #3488: enable
                //self.gc_col(ColBlockHeader, &header_hash.into());
            }

            // 4. Delete chunks_tail-related data
            self.gc_col(ColChunkHashesByHeight, &index_to_bytes(height));
            self.gc_col(ColHeaderHashesByHeight, &index_to_bytes(height));
        }
        self.update_chunk_tail(min_chunk_height);
        Ok(())
    }

    // Clearing block data of `block_hash`, if on a fork.
    // Clearing block data of `block_hash.prev`, if on the Canonical Chain.
    pub fn clear_block_data(
        &mut self,
        mut block_hash: CryptoHash,
        gc_mode: GCMode,
    ) -> Result<(), Error> {
        let mut store_update = self.store().store_update();
        let header = self.get_block_header(&block_hash).expect("block header must exist").clone();

        // 1. Apply revert insertions or deletions from ColTrieChanges for Trie
        match gc_mode.clone() {
            GCMode::Fork(tries) => {
                // If the block is on a fork, we delete the state that's the result of applying this block
                for shard_id in 0..header.chunk_mask().len() as ShardId {
                    self.store()
                        .get_ser(ColTrieChanges, &get_block_shard_id(&block_hash, shard_id))?
                        .map(|trie_changes: TrieChanges| {
                            tries
                                .revert_insertions(&trie_changes, shard_id, &mut store_update)
                                .map(|_| {
                                    self.gc_col(
                                        ColTrieChanges,
                                        &get_block_shard_id(&block_hash, shard_id),
                                    );
                                    self.inc_gc_col_state();
                                })
                                .map_err(|err| ErrorKind::Other(err.to_string()))
                        })
                        .unwrap_or(Ok(()))?;
                }
            }
            GCMode::Canonical(tries) => {
                // If the block is on canonical chain, we delete the state that's before applying this block
                for shard_id in 0..header.chunk_mask().len() as ShardId {
                    self.store()
                        .get_ser(ColTrieChanges, &get_block_shard_id(&block_hash, shard_id))?
                        .map(|trie_changes: TrieChanges| {
                            tries
                                .apply_deletions(&trie_changes, shard_id, &mut store_update)
                                .map(|_| {
                                    self.gc_col(
                                        ColTrieChanges,
                                        &get_block_shard_id(&block_hash, shard_id),
                                    );
                                    self.inc_gc_col_state();
                                })
                                .map_err(|err| ErrorKind::Other(err.to_string()))
                        })
                        .unwrap_or(Ok(()))?;
                }
                // Set `block_hash` on previous one
                block_hash = *self.get_block_header(&block_hash)?.prev_hash();
            }
            GCMode::StateSync { .. } => {
                // Not apply the data from ColTrieChanges
                for shard_id in 0..header.chunk_mask().len() as ShardId {
                    self.gc_col(ColTrieChanges, &get_block_shard_id(&block_hash, shard_id));
                }
            }
        }

        let block = self
            .get_block(&block_hash)
            .expect("block data is not expected to be already cleaned")
            .clone();
        let height = block.header().height();

        // 2. Delete shard_id-indexed data (Receipts, State Headers and Parts, etc.)
        for shard_id in 0..block.header().chunk_mask().len() as ShardId {
            let block_shard_id = get_block_shard_id(&block_hash, shard_id);
            self.gc_outgoing_receipts(&block_hash, shard_id);
            self.gc_col(ColIncomingReceipts, &block_shard_id);
            self.gc_col(ColChunkPerHeightShard, &block_shard_id);
            self.gc_col(ColNextBlockWithNewChunk, &block_shard_id);
            self.gc_col(ColChunkExtra, &block_shard_id);

            // For incoming State Parts it's done in chain.clear_downloaded_parts()
            // The following code is mostly for outgoing State Parts.
            // However, if node crashes while State Syncing, it may never clear
            // downloaded State parts in `clear_downloaded_parts`.
            // We need to make sure all State Parts are removed.
            if let Ok(shard_state_header) = self.chain_store.get_state_header(shard_id, block_hash)
            {
                let state_num_parts =
                    get_num_state_parts(shard_state_header.state_root_node().memory_usage);
                self.gc_col_state_parts(block_hash, shard_id, state_num_parts)?;
                let key = StateHeaderKey(shard_id, block_hash).try_to_vec()?;
                self.gc_col(ColStateHeaders, &key);
            }

            if self.get_last_block_with_new_chunk(shard_id)? == Some(&block_hash) {
                self.gc_col(ColLastBlockWithNewChunk, &index_to_bytes(shard_id));
            }
        }

        // 3. Delete block_hash-indexed data
        let block_hash_vec: Vec<u8> = block_hash.as_ref().into();
        self.gc_col(ColBlock, &block_hash_vec);
        self.gc_col(ColBlockExtra, &block_hash_vec);
        self.gc_col(ColNextBlockHashes, &block_hash_vec);
        self.gc_col(ColChallengedBlocks, &block_hash_vec);
        self.gc_col(ColBlocksToCatchup, &block_hash_vec);
        let storage_key = KeyForStateChanges::get_prefix(&block_hash);
        let stored_state_changes: Vec<Vec<u8>> = self
            .chain_store
            .store()
            .iter_prefix(ColStateChanges, storage_key.as_ref())
            .map(|key| key.0.into())
            .collect();
        for key in stored_state_changes {
            self.gc_col(ColStateChanges, &key);
        }
        self.gc_col(ColBlockRefCount, &block_hash_vec);
        self.gc_outcomes(&block)?;
        match gc_mode {
            GCMode::StateSync { clear_block_info: false } => {}
            _ => self.gc_col(ColBlockInfo, &block_hash_vec),
        }
        self.gc_col(ColStateDlInfos, &block_hash_vec);

        // 4. Update or delete block_hash_per_height
        self.gc_col_block_per_height(&block_hash, height, &block.header().epoch_id())?;

        match gc_mode {
            GCMode::Fork(_) => {
                // 5. Forks only clearing
                self.dec_block_refcount(block.header().prev_hash())?;
            }
            GCMode::Canonical(_) => {
                // 6. Canonical Chain only clearing
                // Delete chunks, chunk-indexed data and block headers
                let mut min_chunk_height = self.tail()?;
                for chunk_header in block.chunks().iter() {
                    if min_chunk_height > chunk_header.height_created() {
                        min_chunk_height = chunk_header.height_created();
                    }
                }
                self.clear_chunk_data_and_headers(min_chunk_height)?;
            }
            GCMode::StateSync { .. } => {
                // 7. State Sync clearing
                // Chunks deleted separately
            }
        };
        self.merge(store_update);
        Ok(())
    }

    pub fn inc_gc_col_state(&mut self) {
        self.inc_gc(ColState);
    }

    fn inc_gc(&mut self, col: DBCol) {
        self.chain_store_cache_update.gc_count.entry(col).and_modify(|x| *x += 1).or_insert(1);
    }

    pub fn gc_col_block_per_height(
        &mut self,
        block_hash: &CryptoHash,
        height: BlockHeight,
        epoch_id: &EpochId,
    ) -> Result<(), Error> {
        let mut store_update = self.store().store_update();
        let epoch_to_hashes_ref = self.chain_store.get_all_block_hashes_by_height(height)?;
        let mut epoch_to_hashes = epoch_to_hashes_ref.clone();
        let hashes =
            epoch_to_hashes.get_mut(epoch_id).ok_or("current epoch id should exist".to_string())?;
        hashes.remove(&block_hash);
        if hashes.is_empty() {
            epoch_to_hashes.remove(epoch_id);
        }
        let key = index_to_bytes(height);
        if epoch_to_hashes.is_empty() {
            store_update.delete(ColBlockPerHeight, &key);
            self.chain_store.block_hash_per_height.cache_remove(&key);
        } else {
            store_update.set_ser(ColBlockPerHeight, &key, &epoch_to_hashes)?;
            self.chain_store
                .block_hash_per_height
                .cache_set(index_to_bytes(height), epoch_to_hashes);
        }
        self.inc_gc(ColBlockPerHeight);
        if self.is_height_processed(height)? {
            self.gc_col(ColProcessedBlockHeights, &key);
        }
        self.merge(store_update);
        Ok(())
    }

    pub fn gc_col_state_parts(
        &mut self,
        sync_hash: CryptoHash,
        shard_id: ShardId,
        num_parts: u64,
    ) -> Result<(), Error> {
        for part_id in 0..num_parts {
            let key = StatePartKey(sync_hash, shard_id, part_id).try_to_vec()?;
            self.gc_col(ColStateParts, &key);
        }
        Ok(())
    }

    pub fn gc_outgoing_receipts(&mut self, block_hash: &CryptoHash, shard_id: ShardId) {
        let mut store_update = self.store().store_update();
        match self.get_outgoing_receipts(block_hash, shard_id).map(|receipts| {
            receipts.iter().map(|receipt| receipt.receipt_id.clone()).collect::<Vec<_>>()
        }) {
            Ok(receipt_ids) => {
                for receipt_id in receipt_ids {
                    let key: Vec<u8> = receipt_id.into();
                    store_update.update_refcount(ColReceiptIdToShardId, &key, &[], -1);
                    self.chain_store.receipt_id_to_shard_id.cache_remove(&key);
                    self.inc_gc(ColReceiptIdToShardId);
                }
            }
            Err(error) => {
                match error.kind() {
                    ErrorKind::DBNotFoundErr(_) => {
                        // Sometimes we don't save outgoing receipts. See the usages of save_outgoing_receipt.
                        // The invariant is that ColOutgoingReceipts has same receipts as ColReceiptIdToShardId.
                    }
                    _ => {
                        tracing::error!(target: "chain", "Error getting outgoing receipts for block {}, shard {}: {:?}", block_hash, shard_id, error);
                    }
                }
            }
        }

        let key = get_block_shard_id(block_hash, shard_id);
        store_update.delete(ColOutgoingReceipts, &key);
        self.chain_store.outgoing_receipts.cache_remove(&key);
        self.inc_gc(ColOutgoingReceipts);
        self.merge(store_update);
    }

    pub fn gc_outcomes(&mut self, block: &Block) -> Result<(), Error> {
        let block_hash = block.hash();
        let mut store_update = self.store().store_update();
        for chunk_header in
            block.chunks().iter().filter(|h| h.height_included() == block.header().height())
        {
            let shard_id = chunk_header.shard_id();
            let outcome_ids =
                self.chain_store.get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id)?;
            for outcome_id in outcome_ids {
                let mut outcomes_with_id = self.chain_store.get_outcomes_by_id(&outcome_id)?;
                outcomes_with_id.retain(|outcome| &outcome.block_hash != block_hash);
                if outcomes_with_id.is_empty() {
                    self.gc_col(ColTransactionResult, &outcome_id.as_ref().into());
                } else {
                    store_update.set_ser(
                        ColTransactionResult,
                        outcome_id.as_ref(),
                        &outcomes_with_id,
                    )?;
                }
            }
            self.gc_col(ColOutcomeIds, &get_block_shard_id(block_hash, shard_id));
        }
        self.merge(store_update);
        Ok(())
    }

    fn gc_col(&mut self, col: DBCol, key: &Vec<u8>) {
        assert!(SHOULD_COL_GC[col as usize]);
        let mut store_update = self.store().store_update();
        match col {
            DBCol::ColOutgoingReceipts => {
                panic!("Must use gc_outgoing_receipts");
            }
            DBCol::ColIncomingReceipts => {
                store_update.delete(col, key);
                self.chain_store.incoming_receipts.cache_remove(key);
            }
            DBCol::ColChunkPerHeightShard => {
                store_update.delete(col, key);
                self.chain_store.chunk_hash_per_height_shard.cache_remove(key);
            }
            DBCol::ColNextBlockWithNewChunk => {
                store_update.delete(col, key);
                self.chain_store.next_block_with_new_chunk.cache_remove(key);
            }
            DBCol::ColStateHeaders => {
                store_update.delete(col, key);
            }
            DBCol::ColBlockHeader => {
                // TODO #3488
                store_update.delete(col, key);
                self.chain_store.headers.cache_remove(key);
                unreachable!();
            }
            DBCol::ColBlock => {
                store_update.delete(col, key);
                self.chain_store.blocks.cache_remove(key);
            }
            DBCol::ColBlockExtra => {
                store_update.delete(col, key);
                self.chain_store.block_extras.cache_remove(key);
            }
            DBCol::ColNextBlockHashes => {
                store_update.delete(col, key);
                self.chain_store.next_block_hashes.cache_remove(key);
            }
            DBCol::ColChallengedBlocks => {
                store_update.delete(col, key);
            }
            DBCol::ColBlocksToCatchup => {
                store_update.delete(col, key);
            }
            DBCol::ColStateChanges => {
                store_update.delete(col, key);
            }
            DBCol::ColBlockRefCount => {
                store_update.delete(col, key);
                self.chain_store.block_refcounts.cache_remove(key);
            }
            DBCol::ColReceiptIdToShardId => {
                panic!("Must use gc_outgoing_receipts");
            }
            DBCol::ColTransactions => {
                store_update.update_refcount(col, key, &[], -1);
                self.chain_store.transactions.cache_remove(key);
            }
            DBCol::ColReceipts => {
                store_update.update_refcount(col, key, &[], -1);
                self.chain_store.receipts.cache_remove(key);
            }
            DBCol::ColChunks => {
                store_update.delete(col, key);
                self.chain_store.chunks.cache_remove(key);
            }
            DBCol::ColChunkExtra => {
                store_update.delete(col, key);
                self.chain_store.chunk_extras.cache_remove(key);
            }
            DBCol::ColPartialChunks => {
                store_update.delete(col, key);
                self.chain_store.partial_chunks.cache_remove(key);
            }
            DBCol::ColInvalidChunks => {
                store_update.delete(col, key);
                self.chain_store.invalid_chunks.cache_remove(key);
            }
            DBCol::ColChunkHashesByHeight => {
                store_update.delete(col, key);
            }
            DBCol::ColStateParts => {
                store_update.delete(col, key);
            }
            DBCol::ColState => {
                panic!("Actual gc happens elsewhere, call inc_gc_col_state to increase gc count");
            }
            DBCol::ColTrieChanges => {
                store_update.delete(col, key);
            }
            DBCol::ColBlockPerHeight => {
                panic!("Must use gc_col_glock_per_height method to gc ColBlockPerHeight");
            }
            DBCol::ColTransactionResult => {
                store_update.delete(col, key);
            }
            DBCol::ColOutcomeIds => {
                store_update.delete(col, key);
            }
            DBCol::ColStateDlInfos => {
                store_update.delete(col, key);
            }
            DBCol::ColBlockInfo => {
                store_update.delete(col, key);
            }
            DBCol::ColLastBlockWithNewChunk => {
                store_update.delete(col, key);
                self.chain_store.last_block_with_new_chunk.cache_remove(key);
            }
            DBCol::ColProcessedBlockHeights => {
                store_update.delete(col, key);
                self.chain_store.processed_block_heights.cache_remove(key);
            }
            DBCol::ColHeaderHashesByHeight => {
                store_update.delete(col, key);
            }
            DBCol::ColDbVersion
            | DBCol::ColBlockMisc
            | DBCol::ColGCCount
            | DBCol::ColBlockHeight
            | DBCol::ColPeers
            | DBCol::ColBlockMerkleTree
            | DBCol::ColAccountAnnouncements
            | DBCol::ColEpochLightClientBlocks
            | DBCol::ColPeerComponent
            | DBCol::ColLastComponentNonce
            | DBCol::ColComponentEdges
            | DBCol::ColEpochInfo
            | DBCol::ColEpochStart
            | DBCol::ColEpochValidatorInfo
            | DBCol::ColBlockOrdinal
            | DBCol::_ColTransactionRefCount
            | DBCol::ColCachedContractCode => {
                unreachable!();
            }
        }
        self.inc_gc(col);
        self.merge(store_update);
    }

    /// Merge another StoreUpdate into this one
    pub fn merge(&mut self, store_update: StoreUpdate) {
        self.store_updates.push(store_update);
    }

    fn write_col_misc<T: BorshSerialize>(
        store_update: &mut StoreUpdate,
        key: &[u8],
        value: &mut Option<T>,
    ) -> Result<(), Error> {
        if let Some(t) = value.take() {
            store_update.set_ser(ColBlockMisc, key, &t)?;
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<StoreUpdate, Error> {
        let mut store_update = self.store().store_update();
        Self::write_col_misc(&mut store_update, HEAD_KEY, &mut self.head)?;
        Self::write_col_misc(&mut store_update, TAIL_KEY, &mut self.tail)?;
        Self::write_col_misc(&mut store_update, CHUNK_TAIL_KEY, &mut self.chunk_tail)?;
        Self::write_col_misc(&mut store_update, FORK_TAIL_KEY, &mut self.fork_tail)?;
        Self::write_col_misc(&mut store_update, HEADER_HEAD_KEY, &mut self.header_head)?;
        Self::write_col_misc(&mut store_update, FINAL_HEAD_KEY, &mut self.final_head)?;
        Self::write_col_misc(
            &mut store_update,
            LARGEST_TARGET_HEIGHT_KEY,
            &mut self.largest_target_height,
        )?;
        debug_assert!(self.chain_store_cache_update.blocks.len() <= 1);
        for (hash, block) in self.chain_store_cache_update.blocks.iter() {
            let mut map =
                match self.chain_store.get_all_block_hashes_by_height(block.header().height()) {
                    Ok(m) => m.clone(),
                    Err(_) => HashMap::new(),
                };
            map.entry(block.header().epoch_id().clone())
                .or_insert_with(|| HashSet::new())
                .insert(*hash);
            store_update.set_ser(
                ColBlockPerHeight,
                &index_to_bytes(block.header().height()),
                &map,
            )?;
            self.chain_store_cache_update
                .block_hash_per_height
                .insert(block.header().height(), map);
            store_update.set_ser(ColBlock, hash.as_ref(), block)?;
        }
        let mut header_hashes_by_height: HashMap<BlockHeight, HashSet<CryptoHash>> = HashMap::new();
        debug_assert!(self.chain_store_cache_update.headers.len() <= 1);
        for (hash, header) in self.chain_store_cache_update.headers.iter() {
            if self.chain_store.get_block_header(hash).is_ok() {
                // No need to add same Header once again
                continue;
            }

            match header_hashes_by_height.entry(header.height()) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().insert(hash.clone());
                }
                Entry::Vacant(entry) => {
                    let mut hash_set =
                        match self.chain_store.get_all_header_hashes_by_height(header.height()) {
                            Ok(hash_set) => hash_set.clone(),
                            Err(_) => HashSet::new(),
                        };
                    hash_set.insert(hash.clone());
                    entry.insert(hash_set);
                }
            };
            store_update.set_ser(ColBlockHeader, hash.as_ref(), header)?;
        }
        for (height, hash_set) in header_hashes_by_height {
            store_update.set_ser(ColHeaderHashesByHeight, &index_to_bytes(height), &hash_set)?;
        }
        for ((block_hash, shard_id), chunk_extra) in
            self.chain_store_cache_update.chunk_extras.iter()
        {
            store_update.set_ser(
                ColChunkExtra,
                &get_block_shard_id(block_hash, *shard_id),
                chunk_extra,
            )?;
        }
        for (block_hash, block_extra) in self.chain_store_cache_update.block_extras.iter() {
            store_update.set_ser(ColBlockExtra, block_hash.as_ref(), block_extra)?;
        }
        for ((height, shard_id), chunk_hash) in
            self.chain_store_cache_update.chunk_hash_per_height_shard.iter()
        {
            let key = get_height_shard_id(*height, *shard_id);
            store_update.set_ser(ColChunkPerHeightShard, &key, chunk_hash)?;
        }
        let mut chunk_hashes_by_height: HashMap<BlockHeight, HashSet<ChunkHash>> = HashMap::new();
        for (chunk_hash, chunk) in self.chain_store_cache_update.chunks.iter() {
            if self.chain_store.get_chunk(chunk_hash).is_ok() {
                // No need to add same Chunk once again
                continue;
            }

            let height_created = chunk.height_created();
            match chunk_hashes_by_height.entry(height_created) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().insert(chunk_hash.clone());
                }
                Entry::Vacant(entry) => {
                    let mut hash_set =
                        match self.chain_store.get_all_chunk_hashes_by_height(height_created) {
                            Ok(hash_set) => hash_set.clone(),
                            Err(_) => HashSet::new(),
                        };
                    hash_set.insert(chunk_hash.clone());
                    entry.insert(hash_set);
                }
            };

            // Increase transaction refcounts for all included txs
            for tx in chunk.transactions().iter() {
                let bytes = tx.try_to_vec().expect("Borsh cannot fail");
                store_update.update_refcount(ColTransactions, tx.get_hash().as_ref(), &bytes, 1);
            }

            // Increase receipt refcounts for all included receipts
            for receipt in chunk.receipts().iter() {
                let bytes = receipt.try_to_vec().expect("Borsh cannot fail");
                store_update.update_refcount(ColReceipts, receipt.get_hash().as_ref(), &bytes, 1);
            }

            store_update.set_ser(ColChunks, chunk_hash.as_ref(), chunk)?;
        }
        for (height, hash_set) in chunk_hashes_by_height {
            store_update.set_ser(ColChunkHashesByHeight, &index_to_bytes(height), &hash_set)?;
        }
        for (chunk_hash, partial_chunk) in self.chain_store_cache_update.partial_chunks.iter() {
            store_update.set_ser(ColPartialChunks, chunk_hash.as_ref(), partial_chunk)?;
        }
        for (height, hash) in self.chain_store_cache_update.height_to_hashes.iter() {
            if let Some(hash) = hash {
                store_update.set_ser(ColBlockHeight, &index_to_bytes(*height), hash)?;
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
        for (hash, outcomes) in self.chain_store_cache_update.outcomes.iter() {
            let mut existing_outcomes = self.chain_store.get_outcomes_by_id(hash)?;
            existing_outcomes.extend_from_slice(outcomes);
            store_update.set_ser(ColTransactionResult, hash.as_ref(), &existing_outcomes)?;
        }
        for ((block_hash, shard_id), ids) in self.chain_store_cache_update.outcome_ids.iter() {
            store_update.set_ser(
                ColOutcomeIds,
                &get_block_shard_id(block_hash, *shard_id),
                &ids,
            )?;
        }
        for (receipt_id, shard_id) in self.chain_store_cache_update.receipt_id_to_shard_id.iter() {
            let data = shard_id.try_to_vec()?;
            store_update.update_refcount(ColReceiptIdToShardId, receipt_id.as_ref(), &data, 1);
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
        for (block_hash, refcount) in self.chain_store_cache_update.block_refcounts.iter() {
            store_update.set_ser(ColBlockRefCount, block_hash.as_ref(), refcount)?;
        }
        for (block_hash, block_merkle_tree) in
            self.chain_store_cache_update.block_merkle_tree.iter()
        {
            store_update.set_ser(ColBlockMerkleTree, block_hash.as_ref(), block_merkle_tree)?;
        }
        for (block_ordinal, block_hash) in
            self.chain_store_cache_update.block_ordinal_to_hash.iter()
        {
            store_update.set_ser(ColBlockOrdinal, &index_to_bytes(*block_ordinal), block_hash)?;
        }
        for mut wrapped_trie_changes in self.trie_changes.drain(..) {
            wrapped_trie_changes
                .wrapped_into(&mut store_update)
                .map_err(|err| ErrorKind::Other(err.to_string()))?;
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
        for block_height in self.chain_store_cache_update.processed_block_heights.iter() {
            store_update.set_ser(ColProcessedBlockHeights, &index_to_bytes(*block_height), &())?;
        }
        for (col, mut gc_count) in self.chain_store_cache_update.gc_count.clone().drain() {
            if let Ok(Some(value)) = self.store().get_ser::<GCCount>(
                ColGCCount,
                &col.try_to_vec().expect("Failed to serialize DBCol"),
            ) {
                gc_count += value;
            }
            store_update.set_ser(
                ColGCCount,
                &col.try_to_vec().expect("Failed to serialize DBCol"),
                &gc_count,
            )?;
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
            headers,
            block_extras,
            chunk_extras,
            chunks,
            partial_chunks,
            block_hash_per_height,
            chunk_hash_per_height_shard,
            height_to_hashes,
            next_block_hashes,
            epoch_light_client_blocks,
            last_approvals_per_account,
            my_last_approvals,
            outgoing_receipts,
            incoming_receipts,
            invalid_chunks,
            receipt_id_to_shard_id,
            next_block_with_new_chunk,
            last_block_with_new_chunk,
            transactions,
            receipts,
            block_refcounts,
            block_merkle_tree,
            block_ordinal_to_hash,
            processed_block_heights,
            ..
        } = self.chain_store_cache_update;
        for (hash, block) in blocks {
            self.chain_store.blocks.cache_set(hash.into(), block);
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
        for ((height, shard_id), chunk_hash) in chunk_hash_per_height_shard {
            let key = get_height_shard_id(height, shard_id);
            self.chain_store.chunk_hash_per_height_shard.cache_set(key, chunk_hash);
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
        for (receipt_id, receipt) in receipts {
            self.chain_store.receipts.cache_set(receipt_id.into(), receipt);
        }
        for (block_hash, refcount) in block_refcounts {
            self.chain_store.block_refcounts.cache_set(block_hash.into(), refcount);
        }
        for (block_hash, merkle_tree) in block_merkle_tree {
            self.chain_store.block_merkle_tree.cache_set(block_hash.into(), merkle_tree);
        }
        for (block_ordinal, block_hash) in block_ordinal_to_hash {
            self.chain_store
                .block_ordinal_to_hash
                .cache_set(index_to_bytes(block_ordinal), block_hash);
        }
        for block_height in processed_block_heights {
            self.chain_store.processed_block_heights.cache_set(index_to_bytes(block_height), ());
        }
        self.chain_store.head = self.head;
        self.chain_store.tail = self.tail;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use borsh::BorshSerialize;
    use cached::Cached;
    use strum::IntoEnumIterator;

    use near_crypto::KeyType;
    use near_primitives::block::{Block, Tip};
    #[cfg(feature = "expensive_tests")]
    use near_primitives::epoch_manager::block_info::BlockInfo;
    use near_primitives::errors::InvalidTxError;
    use near_primitives::hash::hash;
    use near_primitives::types::{BlockHeight, EpochId, GCCount, NumBlocks};
    use near_primitives::utils::index_to_bytes;
    use near_primitives::validator_signer::InMemoryValidatorSigner;
    use near_store::test_utils::create_test_store;
    use near_store::DBCol;
    #[cfg(feature = "expensive_tests")]
    use {crate::store_validator::StoreValidator, near_chain_configs::GenesisConfig};

    use crate::store::{ChainStoreAccess, GCMode};
    use crate::test_utils::KeyValueRuntime;
    use crate::{Chain, ChainGenesis, DoomslugThresholdMode};

    fn get_chain() -> Chain {
        get_chain_with_epoch_length(10)
    }

    fn get_chain_with_epoch_length(epoch_length: NumBlocks) -> Chain {
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
            epoch_length,
        ));
        Chain::new(runtime_adapter, &chain_genesis, DoomslugThresholdMode::NoApprovals).unwrap()
    }

    #[test]
    fn test_tx_validity_long_fork() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let short_fork = vec![Block::empty_with_height(&genesis, 1, &*signer.clone())];
        let mut store_update = chain.mut_store().store_update();
        store_update.save_block_header(short_fork[0].header().clone()).unwrap();
        store_update.commit().unwrap();

        let short_fork_head = short_fork[0].header().clone();
        assert!(chain
            .mut_store()
            .check_transaction_validity_period(
                &short_fork_head,
                &genesis.hash(),
                transaction_validity_period
            )
            .is_ok());
        let mut long_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period + 3) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .update_height_if_not_challenged(block.header().height(), *block.hash())
                .unwrap();
            long_fork.push(block);
            store_update.commit().unwrap();
        }
        let valid_base_hash = long_fork[1].hash();
        let cur_header = &long_fork.last().unwrap().header();
        assert!(chain
            .mut_store()
            .check_transaction_validity_period(
                cur_header,
                &valid_base_hash,
                transaction_validity_period
            )
            .is_ok());
        let invalid_base_hash = long_fork[0].hash();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                cur_header,
                &invalid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_normal_case() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut blocks = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period + 2) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .update_height_if_not_challenged(block.header().height(), *block.hash())
                .unwrap();
            blocks.push(block);
            store_update.commit().unwrap();
        }
        let valid_base_hash = blocks[1].hash();
        let cur_header = &blocks.last().unwrap().header();
        assert!(chain
            .mut_store()
            .check_transaction_validity_period(
                cur_header,
                &valid_base_hash,
                transaction_validity_period
            )
            .is_ok());
        let new_block = Block::empty_with_height(
            &blocks.last().unwrap(),
            transaction_validity_period + 3,
            &*signer.clone(),
        );
        let mut store_update = chain.mut_store().store_update();
        store_update.save_block_header(new_block.header().clone()).unwrap();
        store_update
            .update_height_if_not_challenged(new_block.header().height(), *new_block.hash())
            .unwrap();
        store_update.commit().unwrap();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                &new_block.header(),
                &valid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_off_by_one() {
        let transaction_validity_period = 5;
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut short_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period + 2) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            short_fork.push(block);
            store_update.commit().unwrap();
        }

        let short_fork_head = short_fork.last().unwrap().header().clone();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                &short_fork_head,
                &genesis.hash(),
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
        let mut long_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period * 5) {
            let mut store_update = chain.mut_store().store_update();
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            long_fork.push(block);
            store_update.commit().unwrap();
        }
        let long_fork_head = &long_fork.last().unwrap().header();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
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
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let block1 = Block::empty_with_height(&genesis, 1, &*signer.clone());
        let mut block2 = block1.clone();
        block2.mut_header().get_mut().inner_lite.epoch_id = EpochId(hash(&[1, 2, 3]));
        block2.mut_header().resign(&*signer);

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[1])));
        store_update
            .chain_store_cache_update
            .blocks
            .insert(*block1.header().hash(), block1.clone());
        store_update.commit().unwrap();

        let block_hash = chain.mut_store().height.cache_get(&index_to_bytes(1)).cloned();
        let epoch_id_to_hash =
            chain.mut_store().block_hash_per_height.cache_get(&index_to_bytes(1)).cloned();

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[2])));
        store_update
            .chain_store_cache_update
            .blocks
            .insert(*block2.header().hash(), block2.clone());
        store_update.commit().unwrap();

        let block_hash1 = chain.mut_store().height.cache_get(&index_to_bytes(1)).cloned();
        let epoch_id_to_hash1 =
            chain.mut_store().block_hash_per_height.cache_get(&index_to_bytes(1)).cloned();

        assert_ne!(block_hash, block_hash1);
        assert_ne!(epoch_id_to_hash, epoch_id_to_hash1);
    }

    /// Test that garbage collection works properly. The blocks behind gc head should be garbage
    /// collected while the blocks that are ahead of it should not.
    #[test]
    fn test_clear_old_data() {
        let mut chain = get_chain_with_epoch_length(1);
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut prev_block = genesis.clone();
        let mut blocks = vec![prev_block.clone()];
        for i in 1..15 {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            blocks.push(block.clone());
            let mut store_update = chain.mut_store().store_update();
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
            store_update.save_head(&Tip::from_header(block.header())).unwrap();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(*block.header().hash()));
            store_update.save_next_block_hash(&prev_block.hash(), *block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        chain.epoch_length = 1;
        let trie = chain.runtime_adapter.get_tries();
        assert!(chain.clear_data(trie, 100).is_ok());

        // epoch didn't change so no data is garbage collected.
        for i in 0..15 {
            println!("height = {} hash = {}", i, blocks[i].hash());
            if i < 8 {
                assert!(chain.get_block(&blocks[i].hash()).is_err());
                assert!(chain
                    .mut_store()
                    .get_all_block_hashes_by_height(i as BlockHeight)
                    .is_err());
            } else {
                assert!(chain.get_block(&blocks[i].hash()).is_ok());
                assert!(chain.mut_store().get_all_block_hashes_by_height(i as BlockHeight).is_ok());
            }
        }

        let gced_cols = [
            DBCol::ColBlock,
            DBCol::ColOutgoingReceipts,
            DBCol::ColIncomingReceipts,
            DBCol::ColBlockInfo,
            DBCol::ColBlocksToCatchup,
            DBCol::ColChallengedBlocks,
            DBCol::ColStateDlInfos,
            DBCol::ColBlockExtra,
            DBCol::ColBlockPerHeight,
            DBCol::ColNextBlockHashes,
            DBCol::ColNextBlockWithNewChunk,
            DBCol::ColChunkPerHeightShard,
            DBCol::ColBlockRefCount,
            DBCol::ColOutcomeIds,
            DBCol::ColChunkExtra,
        ];
        for col in DBCol::iter() {
            println!("current column is {:?}", col);
            if gced_cols.contains(&col) {
                // only genesis block includes new chunk.
                let count = if col == DBCol::ColOutcomeIds { Some(1) } else { Some(8) };
                assert_eq!(
                    chain
                        .store()
                        .store
                        .get_ser::<GCCount>(
                            DBCol::ColGCCount,
                            &col.try_to_vec().expect("Failed to serialize DBCol")
                        )
                        .unwrap(),
                    count
                );
            } else {
                assert_eq!(
                    chain
                        .store()
                        .store
                        .get_ser::<GCCount>(
                            DBCol::ColGCCount,
                            &col.try_to_vec().expect("Failed to serialize DBCol")
                        )
                        .unwrap(),
                    None
                );
            }
        }
    }

    #[test]
    fn test_clear_old_data_fixed_height() {
        let mut chain = get_chain();
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut prev_block = genesis.clone();
        let mut blocks = vec![prev_block.clone()];
        for i in 1..10 {
            let mut store_update = chain.mut_store().store_update();

            let block = Block::empty_with_height(&prev_block, i, &*signer);
            blocks.push(block.clone());
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
            store_update.save_head(&Tip::from_header(&block.header())).unwrap();
            store_update.save_block_header(block.header().clone()).unwrap();
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(*block.header().hash()));
            store_update.save_next_block_hash(&prev_block.hash(), *block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        assert!(chain.get_block(&blocks[4].hash()).is_ok());
        assert!(chain.get_block(&blocks[5].hash()).is_ok());
        assert!(chain.get_block(&blocks[6].hash()).is_ok());
        assert!(chain.get_block_header(&blocks[5].hash()).is_ok());
        assert_eq!(
            chain
                .mut_store()
                .get_all_block_hashes_by_height(5)
                .unwrap()
                .values()
                .flatten()
                .collect::<Vec<_>>(),
            vec![blocks[5].hash()]
        );
        assert!(chain.mut_store().get_next_block_hash(&blocks[5].hash()).is_ok());

        let trie = chain.runtime_adapter.get_tries();
        let mut store_update = chain.mut_store().store_update();
        assert!(store_update.clear_block_data(*blocks[5].hash(), GCMode::Canonical(trie)).is_ok());
        store_update.commit().unwrap();

        assert!(chain.get_block(blocks[4].hash()).is_err());
        assert!(chain.get_block(blocks[5].hash()).is_ok());
        assert!(chain.get_block(blocks[6].hash()).is_ok());
        // block header should be available
        assert!(chain.get_block_header(blocks[4].hash()).is_ok());
        assert!(chain.get_block_header(blocks[5].hash()).is_ok());
        assert!(chain.get_block_header(blocks[6].hash()).is_ok());
        assert!(chain.mut_store().get_all_block_hashes_by_height(4).is_err());
        assert!(chain.mut_store().get_all_block_hashes_by_height(5).is_ok());
        assert!(chain.mut_store().get_all_block_hashes_by_height(6).is_ok());
        assert!(chain.mut_store().get_next_block_hash(blocks[4].hash()).is_err());
        assert!(chain.mut_store().get_next_block_hash(blocks[5].hash()).is_ok());
        assert!(chain.mut_store().get_next_block_hash(blocks[6].hash()).is_ok());
    }

    /// Test that `gc_blocks_limit` works properly
    #[cfg(feature = "expensive_tests")]
    #[test]
    fn test_clear_old_data_too_many_heights() {
        for i in 1..5 {
            println!("gc_blocks_limit == {:?}", i);
            test_clear_old_data_too_many_heights_common(i);
        }
        test_clear_old_data_too_many_heights_common(25);
        test_clear_old_data_too_many_heights_common(50);
        test_clear_old_data_too_many_heights_common(87);
    }

    #[cfg(feature = "expensive_tests")]
    fn test_clear_old_data_too_many_heights_common(gc_blocks_limit: NumBlocks) {
        let mut chain = get_chain_with_epoch_length(1);
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut prev_block = genesis.clone();
        let mut blocks = vec![prev_block.clone()];
        {
            let mut store_update = chain.mut_store().store_update().store().store_update();
            let block_info = BlockInfo::default();
            store_update
                .set_ser(DBCol::ColBlockInfo, genesis.hash().as_ref(), &block_info)
                .unwrap();
            store_update.commit().unwrap();
        }
        for i in 1..1000 {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            blocks.push(block.clone());

            let mut store_update = chain.mut_store().store_update();
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(block.header().prev_hash()).unwrap();
            store_update.save_head(&Tip::from_header(&block.header())).unwrap();
            store_update.save_block_header(block.header().clone()).unwrap();
            {
                let mut store_update = store_update.store().store_update();
                let block_info = BlockInfo::default();
                store_update
                    .set_ser(DBCol::ColBlockInfo, block.hash().as_ref(), &block_info)
                    .unwrap();
                store_update.commit().unwrap();
            }
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(*block.header().hash()));
            store_update.save_next_block_hash(&prev_block.hash(), *block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        let trie = chain.runtime_adapter.get_tries();

        for iter in 0..10 {
            println!("ITERATION #{:?}", iter);
            assert!(chain.clear_data(trie.clone(), gc_blocks_limit).is_ok());

            // epoch didn't change so no data is garbage collected.
            for i in 0..1000 {
                if i < (iter + 1) * gc_blocks_limit as usize {
                    assert!(chain.get_block(&blocks[i].hash()).is_err());
                    assert!(chain
                        .mut_store()
                        .get_all_block_hashes_by_height(i as BlockHeight)
                        .is_err());
                } else {
                    assert!(chain.get_block(&blocks[i].hash()).is_ok());
                    assert!(chain
                        .mut_store()
                        .get_all_block_hashes_by_height(i as BlockHeight)
                        .is_ok());
                }
            }
            let mut genesis = GenesisConfig::default();
            genesis.genesis_height = 0;
            let mut store_validator = StoreValidator::new(
                None,
                genesis.clone(),
                chain.runtime_adapter.clone(),
                chain.store().owned_store(),
            );
            store_validator.validate();
            println!("errors = {:?}", store_validator.errors);
            assert!(!store_validator.is_failed());
        }
    }
}
