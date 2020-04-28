use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::io;
use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use cached::{Cached, SizedCache};
use chrono::Utc;
use serde::Serialize;
use tracing::debug;

use near_primitives::block::Approval;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::MerklePath;
use near_primitives::receipt::Receipt;
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, ReceiptProof, ShardChunk, ShardChunkHeader,
};
use near_primitives::transaction::{
    ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof, SignedTransaction,
};
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::{
    AccountId, BlockExtra, BlockHeight, ChunkExtra, EpochId, ShardId, StateChanges,
    StateChangesExt, StateChangesKinds, StateChangesKindsExt, StateChangesRequest, StateHeaderKey,
};
use near_primitives::utils::{index_to_bytes, to_timestamp};
use near_primitives::views::LightClientBlockView;
use near_store::{
    read_with_cache, ColBlock, ColBlockExtra, ColBlockHeader, ColBlockHeight, ColBlockMisc,
    ColBlockPerHeight, ColBlockRefCount, ColBlocksToCatchup, ColChallengedBlocks, ColChunkExtra,
    ColChunkPerHeightShard, ColChunks, ColEpochLightClientBlocks, ColIncomingReceipts,
    ColInvalidChunks, ColLastBlockWithNewChunk, ColNextBlockHashes, ColNextBlockWithNewChunk,
    ColOutgoingReceipts, ColPartialChunks, ColReceiptIdToShardId, ColState, ColStateChanges,
    ColStateDlInfos, ColStateHeaders, ColTransactionResult, ColTransactions, ColTrieChanges,
    KeyForStateChanges, Store, StoreUpdate, Trie, TrieChanges, WrappedTrieChanges,
};

use crate::byzantine_assert;
use crate::error::{Error, ErrorKind};
use crate::types::{Block, BlockHeader, LatestKnown, ReceiptProofResponse, ReceiptResponse, Tip};

const HEAD_KEY: &[u8; 4] = b"HEAD";
const TAIL_KEY: &[u8; 4] = b"TAIL";
const SYNC_HEAD_KEY: &[u8; 9] = b"SYNC_HEAD";
const HEADER_HEAD_KEY: &[u8; 11] = b"HEADER_HEAD";
const LATEST_KNOWN_KEY: &[u8; 12] = b"LATEST_KNOWN";
const LARGEST_TARGET_HEIGHT_KEY: &[u8; 21] = b"LARGEST_TARGET_HEIGHT";

/// lru cache size
const CACHE_SIZE: usize = 100;
const CHUNK_CACHE_SIZE: usize = 1024;

#[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Serialize)]
pub struct ShardInfo(pub ShardId, pub ChunkHash);

#[derive(Clone)]
pub enum GCMode {
    Fork(Arc<Trie>),
    Canonical(Arc<Trie>),
    StateSync,
}

fn get_block_shard_id(block_hash: &CryptoHash, shard_id: ShardId) -> Vec<u8> {
    let mut res = Vec::with_capacity(40);
    res.extend_from_slice(block_hash.as_ref());
    res.extend_from_slice(&shard_id.to_le_bytes());
    res
}

fn get_height_shard_id(height: BlockHeight, shard_id: ShardId) -> Vec<u8> {
    let mut res = Vec::with_capacity(40);
    res.extend_from_slice(&height.to_le_bytes());
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

/// Accesses the chain store. Used to create atomic editable views that can be reverted.
pub trait ChainStoreAccess {
    /// Returns underlaying store.
    fn store(&self) -> &Store;
    /// The chain head.
    fn head(&self) -> Result<Tip, Error>;
    /// The chain tail height.
    fn tail(&self) -> Result<BlockHeight, Error>;
    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error>;
    /// The "sync" head: last header we received from syncing.
    fn sync_head(&self) -> Result<Tip, Error>;
    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&mut self) -> Result<&BlockHeader, Error>;
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
    /// Returns a hashmap of epoch id -> set of all blocks got for current (height, epoch_id)
    fn get_all_block_hashes_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<&HashMap<EpochId, HashSet<CryptoHash>>, Error>;
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

    fn get_state_changes_in_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<StateChangesKinds, Error>;

    fn get_state_changes(
        &self,
        block_hash: &CryptoHash,
        state_changes_request: &StateChangesRequest,
    ) -> Result<StateChanges, Error>;

    fn get_genesis_height(&self) -> BlockHeight;
}

/// All chain-related database operations.
pub struct ChainStore {
    store: Arc<Store>,
    /// Genesis block height.
    genesis_height: BlockHeight,
    /// Latest known.
    latest_known: Option<LatestKnown>,
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
    /// Cache with height to hash on any chain.
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
    /// Cache with height to hash on any chain.
    block_refcounts: SizedCache<Vec<u8>, u64>,
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
        let base_height = self
            .get_block_header(base_block_hash)
            .map_err(|_| InvalidTxError::Expired)?
            .inner_lite
            .height;
        let prev_height = prev_block_header.inner_lite.height;
        if let Ok(base_block_hash_by_height) = self.get_block_hash_by_height(base_height) {
            if &base_block_hash_by_height == base_block_hash {
                if let Ok(prev_hash) = self.get_block_hash_by_height(prev_height) {
                    if prev_hash == prev_block_header.hash {
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
            .get_block_height(&prev_block_header.inner_rest.last_final_block)
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
                .get_header_on_chain_by_height(&prev_block_header.hash, base_height)
                .map_err(|_| InvalidTxError::InvalidChain)?;
            if &header.hash == base_block_hash {
                Ok(())
            } else {
                Err(InvalidTxError::InvalidChain)
            }
        }
    }

    pub fn get_block_height(&mut self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            Ok(self.genesis_height)
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

    /// The chain tail height, used by GC.
    fn tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(ColBlockMisc, TAIL_KEY)
            .map(|option| option.unwrap_or_else(|| self.genesis_height))
            .map_err(|e| e.into())
    }

    /// The "sync" head: last header we received from syncing.
    fn sync_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(ColBlockMisc, SYNC_HEAD_KEY), "SYNC_HEAD")
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

    /// Get full block.
    fn get_block(&mut self, h: &CryptoHash) -> Result<&Block, Error> {
        let block_result = option_to_not_found(
            read_with_cache(&*self.store, ColBlock, &mut self.blocks, h.as_ref()),
            &format!("BLOCK: {}", h),
        );
        match block_result {
            Ok(block) => Ok(block),
            Err(e) => match e.kind() {
                ErrorKind::DBNotFoundErr(_) => {
                    let block_header_result = read_with_cache(
                        &*self.store,
                        ColBlockHeader,
                        &mut self.headers,
                        h.as_ref(),
                    );
                    match block_header_result {
                        Ok(Some(_)) => Err(ErrorKind::BlockMissing(h.clone()).into()),
                        Ok(None) => Err(e),
                        Err(header_error) => {
                            debug_assert!(
                                false,
                                "If the block was not found, the block header may either \
                                exist or not found as well, instead the error was returned {:?}",
                                header_error
                            );
                            debug!(
                                target: "store",
                                "If the block was not found, the block header may either \
                                exist or not found as well, instead the error was returned {:?}. \
                                This is not expected to happen, but it is not a fatal error.",
                                header_error
                            );
                            Err(e)
                        }
                    }
                }
                _ => Err(e),
            },
        }
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

    fn get_all_block_hashes_by_height(
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

    /// Retrieve the kinds of state changes occurred in a given block.
    ///
    /// We store different types of data, so we prefer to only expose minimal information about the
    /// changes (i.e. a kind of the change and an account id).
    fn get_state_changes_in_block(
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

    /// Retrieve the key-value changes from the store and decode them appropriately.
    ///
    /// We store different types of data, so we need to take care of all the types. That is, the
    /// account data and the access keys are internally-serialized and we have to deserialize those
    /// values appropriately. Code and data changes are simple blobs of data, so we return them as
    /// base64-encoded blobs.
    fn get_state_changes(
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

    fn get_genesis_height(&self) -> BlockHeight {
        self.genesis_height
    }
}

/// Cache update for ChainStore
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
    outcomes: HashMap<CryptoHash, ExecutionOutcomeWithIdAndProof>,
    invalid_chunks: HashMap<ChunkHash, EncodedShardChunk>,
    receipt_id_to_shard_id: HashMap<CryptoHash, ShardId>,
    next_block_with_new_chunk: HashMap<(CryptoHash, ShardId), CryptoHash>,
    last_block_with_new_chunk: HashMap<ShardId, CryptoHash>,
    transactions: HashSet<SignedTransaction>,
    block_refcounts: HashMap<CryptoHash, u64>,
}

impl ChainStoreCacheUpdate {
    pub fn new() -> Self {
        Self {
            blocks: Default::default(),
            headers: Default::default(),
            block_extras: Default::default(),
            chunk_extras: HashMap::default(),
            chunks: Default::default(),
            partial_chunks: Default::default(),
            block_hash_per_height: HashMap::default(),
            chunk_hash_per_height_shard: HashMap::default(),
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
            block_refcounts: HashMap::default(),
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
    tail: Option<BlockHeight>,
    header_head: Option<Tip>,
    sync_head: Option<Tip>,
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
            chain_store_cache_update: ChainStoreCacheUpdate::new(),
            head: None,
            tail: None,
            header_head: None,
            sync_head: None,
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

    /// The chain tail height, used by GC.
    fn tail(&self) -> Result<BlockHeight, Error> {
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

    fn get_all_block_hashes_by_height(
        &mut self,
        height: BlockHeight,
    ) -> Result<&HashMap<EpochId, HashSet<CryptoHash>>, Error> {
        self.chain_store.get_all_block_hashes_by_height(height)
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

    fn get_state_changes_in_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<StateChangesKinds, Error> {
        self.chain_store.get_state_changes_in_block(block_hash)
    }

    fn get_state_changes(
        &self,
        block_hash: &CryptoHash,
        state_changes_request: &StateChangesRequest,
    ) -> Result<StateChanges, Error> {
        self.chain_store.get_state_changes(block_hash, state_changes_request)
    }

    fn get_genesis_height(&self) -> BlockHeight {
        self.chain_store.genesis_height
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

    /// Save "sync" head.
    pub fn save_sync_head(&mut self, t: &Tip) {
        self.sync_head = Some(t.clone());
    }

    pub fn save_largest_target_height(&mut self, height: &BlockHeight) {
        self.largest_target_height = Some(height.clone());
    }

    /// Save new height if it's above currently latest known.
    pub fn try_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let latest_known = self.get_latest_known().ok();
        if latest_known.is_none() || height > latest_known.unwrap().height {
            self.save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        }
        Ok(())
    }

    #[cfg(feature = "adversarial")]
    pub fn adv_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let header = self.get_header_by_height(height)?;
        let tip = Tip::from_header(&header);
        self.save_latest_known(LatestKnown { height, seen: to_timestamp(Utc::now()) })?;
        self.save_head(&tip)?;
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

    pub fn update_tail(&mut self, height: BlockHeight) {
        self.tail = Some(height);
    }

    pub fn clear_state_data(&mut self) {
        let mut store_update = self.store().store_update();

        let stored_state = self.chain_store.store().iter_prefix(ColState, &[]);
        for (key, _) in stored_state {
            store_update.delete(ColState, key.as_ref());
        }
        self.merge(store_update);
    }

    // Clearing block data of `block_hash`, if on a fork.
    // Clearing block data of `block_hash.prev`, if on the Canonical Chain.
    pub fn clear_block_data(
        &mut self,
        mut block_hash: CryptoHash,
        gc_mode: GCMode,
    ) -> Result<(), Error> {
        let header = self
            .get_block_header(&block_hash)
            .expect("block data is not expected to be already cleaned")
            .clone();
        if header.inner_lite.height == self.get_genesis_height() {
            // Don't clean Genesis
            return Ok(());
        }

        let mut store_update = self.store().store_update();

        // 1. Apply revert insertions or deletions from ColTrieChanges for Trie
        match gc_mode.clone() {
            GCMode::Fork(trie) => {
                // If the block is on a fork, we delete the state that's the result of applying this block
                self.store()
                    .get_ser(ColTrieChanges, block_hash.as_ref())?
                    .map(|trie_changes: TrieChanges| {
                        trie_changes
                            .revert_insertions_into(trie.clone(), &mut store_update)
                            .map_err(|err| ErrorKind::Other(err.to_string()))
                    })
                    .unwrap_or(Ok(()))?;
            }
            GCMode::Canonical(trie) => {
                // If the block is on canonical chain, we delete the state that's before applying this block
                self.store()
                    .get_ser(ColTrieChanges, block_hash.as_ref())?
                    .map(|trie_changes: TrieChanges| {
                        trie_changes
                            .deletions_into(trie.clone(), &mut store_update)
                            .map_err(|err| ErrorKind::Other(err.to_string()))
                    })
                    .unwrap_or(Ok(()))?;
                // Set `block_hash` on previous one
                block_hash = self.get_block_header(&block_hash)?.prev_hash;
            }
            GCMode::StateSync => {
                // Do nothing here
            }
        }

        let block = self
            .get_block(&block_hash)
            .expect("block data is not expected to be already cleaned")
            .clone();
        let height = block.header.inner_lite.height;
        if height == self.get_genesis_height() {
            if let GCMode::Fork(_) = gc_mode {
                // Broken GC prerequisites found
                assert!(false);
            }
            return Ok(());
        }

        // 2. Delete shard_id-indexed data (shards, receipts, transactions)
        for shard_id in 0..block.header.inner_rest.chunk_mask.len() as ShardId {
            // 2a. Delete outgoing receipts (ColOutgoingReceipts)
            store_update.delete(ColOutgoingReceipts, &get_block_shard_id(&block_hash, shard_id));
            self.chain_store
                .outgoing_receipts
                .cache_remove(&get_block_shard_id(&block_hash, shard_id));
            // 2b. Delete incoming receipts (ColIncomingReceipts)
            store_update.delete(ColIncomingReceipts, &get_block_shard_id(&block_hash, shard_id));
            self.chain_store
                .incoming_receipts
                .cache_remove(&get_block_shard_id(&block_hash, shard_id));
            // 2c. Delete from chunk_hash_per_height_shard (ColChunkPerHeightShard)
            store_update.delete(ColChunkPerHeightShard, &get_height_shard_id(height, shard_id));
            self.chain_store
                .chunk_hash_per_height_shard
                .cache_remove(&get_height_shard_id(height, shard_id));
            // 2d. Delete from next_block_with_new_chunk (ColNextBlockWithNewChunk)
            store_update
                .delete(ColNextBlockWithNewChunk, &get_block_shard_id(&block_hash, shard_id));
            self.chain_store
                .next_block_with_new_chunk
                .cache_remove(&get_block_shard_id(&block_hash, shard_id));
            // 2e. Delete from ColStateHeaders
            let key = StateHeaderKey(shard_id, block_hash).try_to_vec()?;
            store_update.delete(ColStateHeaders, &key);
            // 2f. Delete from ColStateParts
            // Already done, check chain.clear_downloaded_parts()
        }
        for chunk_header in block.chunks {
            if let Ok(chunk) = self.get_chunk_clone_from_header(&chunk_header) {
                // 2g. Delete from receipt_id_to_shard_id (ColReceiptIdToShardId)
                for receipt in chunk.receipts {
                    store_update.delete(ColReceiptIdToShardId, receipt.receipt_id.as_ref());
                    self.chain_store
                        .receipt_id_to_shard_id
                        .cache_remove(&receipt.receipt_id.into());
                }
                // 2h. Delete from ColTransactions
                for transaction in chunk.transactions {
                    store_update.delete(ColTransactions, transaction.get_hash().as_ref());
                    self.chain_store.transactions.cache_remove(&transaction.get_hash().into());
                }
            }

            // 3. Delete chunk_hash-indexed data
            let chunk_header_hash = chunk_header.hash.clone().into();
            let chunk_header_hash_ref = chunk_header.hash.as_ref();
            // 3a. Delete chunks (ColChunks)
            store_update.delete(ColChunks, chunk_header_hash_ref);
            self.chain_store.chunks.cache_remove(&chunk_header_hash);
            // 3b. Delete chunk extras (ColChunkExtra)
            store_update.delete(ColChunkExtra, chunk_header_hash_ref);
            self.chain_store.chunk_extras.cache_remove(&chunk_header_hash);
            // 3c. Delete partial_chunks (ColPartialChunks)
            store_update.delete(ColPartialChunks, chunk_header_hash_ref);
            self.chain_store.partial_chunks.cache_remove(&chunk_header_hash);
            // 3d. Delete invalid chunks (ColInvalidChunks)
            store_update.delete(ColInvalidChunks, chunk_header_hash_ref);
            self.chain_store.invalid_chunks.cache_remove(&chunk_header_hash);
        }

        // 4. Delete block_hash-indexed data
        //let chunk_header_hash = chunk_header.hash.clone().into();
        let block_hash_ref = block_hash.as_ref();
        // 4a. Delete block (ColBlock)
        store_update.delete(ColBlock, block_hash_ref);
        self.chain_store.blocks.cache_remove(&block_hash.into());
        // 4b. Delete block header (ColBlockHeader) - don't do because header sync needs headers
        // 4c. Delete block extras (ColBlockExtra)
        store_update.delete(ColBlockExtra, block_hash_ref);
        self.chain_store.block_extras.cache_remove(&block_hash.into());
        // 4d. Delete from next_block_hashes (ColNextBlockHashes)
        store_update.delete(ColNextBlockHashes, block_hash_ref);
        self.chain_store.next_block_hashes.cache_remove(&block_hash.into());
        // 4e. Delete from ColChallengedBlocks
        store_update.delete(ColChallengedBlocks, block_hash_ref);
        // 4f. Delete from ColBlocksToCatchup
        store_update.delete(ColBlocksToCatchup, block_hash_ref);
        // 4g. Delete from KV state changes
        let storage_key = KeyForStateChanges::get_prefix(&block_hash);
        // 4g1. We should collect all the keys which key prefix equals to `block_hash`
        let stored_state_changes =
            self.chain_store.store().iter_prefix(ColStateChanges, storage_key.as_ref());
        // 4g2. Remove from ColStateChanges all found State Changes
        for (key, _) in stored_state_changes {
            store_update.delete(ColStateChanges, key.as_ref());
        }
        // 4h. Delete from ColBlockRefCount
        store_update.delete(ColBlockRefCount, block_hash_ref);
        self.chain_store.block_refcounts.cache_remove(&block_hash.into());

        match gc_mode {
            GCMode::Fork(_) => {
                // 5. Forks only clearing
                // 5a. Update block_hash_per_height
                let epoch_to_hashes_ref =
                    self.get_all_block_hashes_by_height(height).expect("current height exists");
                let mut epoch_to_hashes = epoch_to_hashes_ref.clone();
                let hashes = epoch_to_hashes
                    .get_mut(&block.header.inner_lite.epoch_id)
                    .expect("current epoch id should exist");
                hashes.remove(&block_hash);
                store_update.set_ser(
                    ColBlockPerHeight,
                    &index_to_bytes(height),
                    &epoch_to_hashes,
                )?;
                self.chain_store
                    .block_hash_per_height
                    .cache_set(index_to_bytes(height), epoch_to_hashes);
                // 5b. Decreasing block refcount
                self.dec_block_refcount(&block.header.prev_hash)?;
            }
            GCMode::Canonical(_) => {
                // 6. Canonical Chain only clearing
                // 6a. Delete blocks with current height (ColBlockPerHeight)
                store_update.delete(ColBlockPerHeight, &index_to_bytes(height));
                self.chain_store.block_hash_per_height.cache_remove(&index_to_bytes(height));
                // 6b. Delete from ColBlockHeight - don't do because: block sync needs it + genesis should be accessible
            }
            GCMode::StateSync => {
                // 7. Post State Sync clearing
                // 7a. Delete blocks with current height (ColBlockPerHeight) - that's fine to do it multiple times
                store_update.delete(ColBlockPerHeight, &index_to_bytes(height));
                self.chain_store.block_hash_per_height.cache_remove(&index_to_bytes(height));
            }
        };
        self.merge(store_update);
        Ok(())
    }

    pub fn clear_forks_data(&mut self, trie: Arc<Trie>, height: BlockHeight) -> Result<(), Error> {
        if let Ok(blocks_current_height) = self.get_all_block_hashes_by_height(height) {
            let blocks_current_height =
                blocks_current_height.values().flatten().cloned().collect::<Vec<_>>();
            for block_hash in blocks_current_height.iter() {
                let mut current_hash = *block_hash;
                loop {
                    // Block `block_hash` is not on the Canonical Chain
                    // because shorter chain cannot be Canonical one
                    // and it may be safely deleted
                    // and all its ancestors while there are no other sibling blocks rely on it.
                    if *self.get_block_refcount(&current_hash)? == 0 {
                        let prev_hash = self.get_block_header(&current_hash)?.prev_hash;

                        // It's safe to call `clear_block_data` for prev data because it clears fork only here
                        self.clear_block_data(current_hash, GCMode::Fork(trie.clone()))?;

                        current_hash = prev_hash;
                    } else {
                        // Block of `current_hash` is an ancestor for some other blocks, stopping
                        break;
                    }
                }
            }
        }

        Ok(())
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
            store_update.set_ser(ColBlockMisc, TAIL_KEY, &t)?
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
        if let Some(t) = self.largest_target_height {
            store_update
                .set_ser(ColBlockMisc, LARGEST_TARGET_HEIGHT_KEY, &t)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, block) in self.chain_store_cache_update.blocks.iter() {
            let mut map = match self
                .chain_store
                .get_all_block_hashes_by_height(block.header.inner_lite.height)
            {
                Ok(m) => m.clone(),
                Err(_) => HashMap::new(),
            };
            map.entry(block.header.inner_lite.epoch_id.clone())
                .or_insert_with(|| HashSet::new())
                .insert(*hash);
            store_update
                .set_ser(ColBlockPerHeight, &index_to_bytes(block.header.inner_lite.height), &map)
                .map_err::<Error, _>(|e| e.into())?;
            self.chain_store_cache_update
                .block_hash_per_height
                .insert(block.header.inner_lite.height, map);
            store_update
                .set_ser(ColBlock, hash.as_ref(), block)
                .map_err::<Error, _>(|e| e.into())?;
        }
        for (hash, header) in self.chain_store_cache_update.headers.iter() {
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
        for ((height, shard_id), chunk_hash) in
            self.chain_store_cache_update.chunk_hash_per_height_shard.iter()
        {
            let key = get_height_shard_id(*height, *shard_id);
            store_update
                .set_ser(ColChunkPerHeightShard, &key, chunk_hash)
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
        for (block_hash, refcount) in self.chain_store_cache_update.block_refcounts.iter() {
            store_update.set_ser(ColBlockRefCount, block_hash.as_ref(), refcount)?;
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
            outcomes,
            invalid_chunks,
            receipt_id_to_shard_id,
            next_block_with_new_chunk,
            last_block_with_new_chunk,
            transactions,
            block_refcounts,
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
        for (block_hash, refcount) in block_refcounts {
            self.chain_store.block_refcounts.cache_set(block_hash.into(), refcount);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cached::Cached;

    use near_crypto::KeyType;
    use near_primitives::block::Block;
    use near_primitives::errors::InvalidTxError;
    use near_primitives::hash::hash;
    use near_primitives::types::{BlockHeight, EpochId, NumBlocks};
    use near_primitives::utils::index_to_bytes;
    use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
    use near_store::test_utils::create_test_store;

    use crate::chain::{check_refcount_map, MAX_HEIGHTS_TO_CLEAR};
    use crate::store::{ChainStoreAccess, GCMode};
    use crate::test_utils::KeyValueRuntime;
    use crate::{Chain, ChainGenesis, DoomslugThresholdMode, Tip};

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
        store_update.save_block_header(short_fork[0].header.clone());
        store_update.commit().unwrap();

        let short_fork_head = short_fork[0].clone().header;
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
        let mut store_update = chain.mut_store().store_update();
        for i in 1..(transaction_validity_period + 3) {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header.clone());
            store_update
                .update_height_if_not_challenged(block.header.inner_lite.height, block.hash())
                .unwrap();
            long_fork.push(block);
        }
        store_update.commit().unwrap();
        let valid_base_hash = long_fork[1].hash();
        let cur_header = &long_fork.last().unwrap().header;
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
        let mut store_update = chain.mut_store().store_update();
        for i in 1..(transaction_validity_period + 2) {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header.clone());
            store_update
                .update_height_if_not_challenged(block.header.inner_lite.height, block.hash())
                .unwrap();
            blocks.push(block);
        }
        store_update.commit().unwrap();
        let valid_base_hash = blocks[1].hash();
        let cur_header = &blocks.last().unwrap().header;
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
        store_update.save_block_header(new_block.header.clone());
        store_update
            .update_height_if_not_challenged(new_block.header.inner_lite.height, new_block.hash())
            .unwrap();
        store_update.commit().unwrap();
        assert_eq!(
            chain.mut_store().check_transaction_validity_period(
                &new_block.header,
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
        let mut store_update = chain.mut_store().store_update();
        for i in 1..(transaction_validity_period + 2) {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            prev_block = block.clone();
            store_update.save_block_header(block.header.clone());
            short_fork.push(block);
        }
        store_update.commit().unwrap();

        let short_fork_head = short_fork.last().unwrap().clone().header;
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
        block2.header.inner_lite.epoch_id = EpochId(hash(&[1, 2, 3]));
        let (block_hash, signature) = signer.sign_block_header_parts(
            block2.header.prev_hash,
            &block2.header.inner_lite,
            &block2.header.inner_rest,
        );
        block2.header.hash = block_hash;
        block2.header.signature = signature;

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[1])));
        store_update.chain_store_cache_update.blocks.insert(block1.header.hash, block1.clone());
        store_update.commit().unwrap();

        let block_hash = chain.mut_store().height.cache_get(&index_to_bytes(1)).cloned();
        let epoch_id_to_hash =
            chain.mut_store().block_hash_per_height.cache_get(&index_to_bytes(1)).cloned();

        let mut store_update = chain.mut_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[2])));
        store_update.chain_store_cache_update.blocks.insert(block2.header.hash, block2.clone());
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
            store_update.inc_block_refcount(&block.header.prev_hash).unwrap();
            store_update.save_head(&Tip::from_header(&block.header)).unwrap();
            store_update.save_block_header(block.header.clone());
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(block.header.hash));
            store_update.save_next_block_hash(&prev_block.hash(), block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        assert!(check_refcount_map(&mut chain).is_ok());
        chain.epoch_length = 1;
        let trie = chain.runtime_adapter.get_trie();
        assert!(chain.clear_data(trie).is_ok());

        assert!(chain.get_block(&blocks[0].hash()).is_ok());

        // epoch didn't change so no data is garbage collected.
        for i in 1..15 {
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
        assert!(check_refcount_map(&mut chain).is_ok());
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
            store_update.inc_block_refcount(&block.header.prev_hash).unwrap();
            store_update.save_head(&Tip::from_header(&block.header)).unwrap();
            store_update.save_block_header(block.header.clone());
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(block.header.hash));
            store_update.save_next_block_hash(&prev_block.hash(), block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }
        assert!(check_refcount_map(&mut chain).is_ok());

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
            vec![&blocks[5].hash()]
        );
        assert!(chain.mut_store().get_next_block_hash(&blocks[5].hash()).is_ok());

        let trie = chain.runtime_adapter.get_trie();
        let mut store_update = chain.mut_store().store_update();
        assert!(store_update.clear_block_data(blocks[5].hash(), GCMode::Canonical(trie)).is_ok());
        store_update.commit().unwrap();

        assert!(chain.get_block(&blocks[4].hash()).is_err());
        assert!(chain.get_block(&blocks[5].hash()).is_ok());
        assert!(chain.get_block(&blocks[6].hash()).is_ok());
        // block header should be available
        assert!(chain.get_block_header(&blocks[4].hash()).is_ok());
        assert!(chain.get_block_header(&blocks[5].hash()).is_ok());
        assert!(chain.get_block_header(&blocks[6].hash()).is_ok());
        assert!(chain.mut_store().get_all_block_hashes_by_height(4).is_err());
        assert!(chain.mut_store().get_all_block_hashes_by_height(5).is_ok());
        assert!(chain.mut_store().get_all_block_hashes_by_height(6).is_ok());
        assert!(chain.mut_store().get_next_block_hash(&blocks[4].hash()).is_err());
        assert!(chain.mut_store().get_next_block_hash(&blocks[5].hash()).is_ok());
        assert!(chain.mut_store().get_next_block_hash(&blocks[6].hash()).is_ok());
    }

    /// Test that MAX_HEIGHTS_TO_CLEAR works properly
    #[test]
    fn test_clear_old_data_too_many_heights() {
        let mut chain = get_chain_with_epoch_length(1);
        let genesis = chain.get_block_by_height(0).unwrap().clone();
        let signer =
            Arc::new(InMemoryValidatorSigner::from_seed("test1", KeyType::ED25519, "test1"));
        let mut prev_block = genesis.clone();
        let mut blocks = vec![prev_block.clone()];
        for i in 1..1000 {
            let block = Block::empty_with_height(&prev_block, i, &*signer.clone());
            blocks.push(block.clone());

            let mut store_update = chain.mut_store().store_update();
            store_update.save_block(block.clone());
            store_update.inc_block_refcount(&block.header.prev_hash).unwrap();
            store_update.save_head(&Tip::from_header(&block.header)).unwrap();
            store_update.save_block_header(block.header.clone());
            store_update
                .chain_store_cache_update
                .height_to_hashes
                .insert(i, Some(block.header.hash));
            store_update.save_next_block_hash(&prev_block.hash(), block.hash());
            store_update.commit().unwrap();

            prev_block = block.clone();
        }

        assert!(check_refcount_map(&mut chain).is_ok());
        let trie = chain.runtime_adapter.get_trie();

        for iter in 0..10 {
            println!("ITERATION #{:?}", iter);
            assert!(chain.clear_data(trie.clone()).is_ok());

            assert!(chain.get_block(&blocks[0].hash()).is_ok());

            // epoch didn't change so no data is garbage collected.
            for i in 1..1000 {
                if i < (iter + 1) * (MAX_HEIGHTS_TO_CLEAR - 1) as usize {
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
            assert!(check_refcount_map(&mut chain).is_ok());
        }
    }
}
