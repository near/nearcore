use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::io;

use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use near_cache::CellLruCache;

use near_chain_primitives::error::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block::Tip;
use near_primitives::checked_feature;
use near_primitives::errors::InvalidTxError;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{MerklePath, PartialMerkleTree};
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::account_id_to_shard_id;
use near_primitives::shard_layout::{get_block_shard_uid, ShardLayout, ShardUId};
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, ReceiptProof, ShardChunk, ShardChunkHeader,
    StateSyncInfo,
};
use near_primitives::state_sync::{
    ReceiptProofResponse, ShardStateSyncResponseHeader, StateHeaderKey, StateSyncDumpProgress,
};
use near_primitives::stateless_validation::stored_chunk_state_transition_data::StoredChunkStateTransitionData;
use near_primitives::transaction::{
    ExecutionOutcomeWithId, ExecutionOutcomeWithIdAndProof, ExecutionOutcomeWithProof,
    SignedTransaction,
};
use near_primitives::trie_key::{trie_key_parsers, TrieKey};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{
    BlockExtra, BlockHeight, EpochId, NumBlocks, ShardId, StateChanges, StateChangesExt,
    StateChangesKinds, StateChangesKindsExt, StateChangesRequest,
};
use near_primitives::utils::{
    get_block_shard_id, get_outcome_id_block_hash, get_outcome_id_block_hash_rev, index_to_bytes,
    to_timestamp,
};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::LightClientBlockView;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{
    DBCol, KeyForStateChanges, PartialStorage, Store, StoreUpdate, WrappedTrieChanges,
    CHUNK_TAIL_KEY, FINAL_HEAD_KEY, FORK_TAIL_KEY, HEADER_HEAD_KEY, HEAD_KEY,
    LARGEST_TARGET_HEIGHT_KEY, LATEST_KNOWN_KEY, TAIL_KEY,
};

use crate::byzantine_assert;
use crate::types::{Block, BlockHeader, LatestKnown};
use near_store::db::{StoreStatistics, STATE_SYNC_DUMP_KEY};
use std::sync::Arc;

mod latest_witnesses;
pub use latest_witnesses::LatestWitnessesInfo;

/// lru cache size
#[cfg(not(feature = "no_cache"))]
const CACHE_SIZE: usize = 100;
#[cfg(not(feature = "no_cache"))]
const CHUNK_CACHE_SIZE: usize = 1024;

#[cfg(feature = "no_cache")]
const CACHE_SIZE: usize = 1;
#[cfg(feature = "no_cache")]
const CHUNK_CACHE_SIZE: usize = 1;

/// Accesses the chain store. Used to create atomic editable views that can be reverted.
pub trait ChainStoreAccess {
    /// Returns underlying chain store
    fn chain_store(&self) -> &ChainStore;
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
    fn head_header(&self) -> Result<BlockHeader, Error>;
    /// The chain final head. It is guaranteed to be monotonically increasing.
    fn final_head(&self) -> Result<Tip, Error>;
    /// Largest approval target height sent by us
    fn largest_target_height(&self) -> Result<BlockHeight, Error>;
    /// Get full block.
    fn get_block(&self, h: &CryptoHash) -> Result<Block, Error>;
    /// Get full chunk.
    fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<ShardChunk>, Error>;
    /// Get partial chunk.
    fn get_partial_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<PartialEncodedChunk>, Error>;
    /// Get full chunk from header, with possible error that contains the header for further retrieval.
    fn get_chunk_clone_from_header(&self, header: &ShardChunkHeader) -> Result<ShardChunk, Error> {
        let shard_chunk_result = self.get_chunk(&header.chunk_hash());
        match shard_chunk_result {
            Err(_) => {
                return Err(Error::ChunksMissing(vec![header.clone()]));
            }
            Ok(shard_chunk) => {
                byzantine_assert!(header.height_included() > 0 || header.height_created() == 0);
                if header.height_included() == 0 && header.height_created() > 0 {
                    return Err(Error::Other(format!(
                        "Invalid header: {:?} for chunk {:?}",
                        header, shard_chunk
                    )));
                }
                let mut shard_chunk_clone = ShardChunk::clone(&shard_chunk);
                shard_chunk_clone.set_height_included(header.height_included());
                Ok(shard_chunk_clone)
            }
        }
    }
    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error>;
    /// Does this chunk exist?
    fn chunk_exists(&self, h: &ChunkHash) -> Result<bool, Error>;
    /// Get previous header.
    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error>;
    /// GEt block extra for given block.
    fn get_block_extra(&self, block_hash: &CryptoHash) -> Result<Arc<BlockExtra>, Error>;
    /// Get chunk extra info for given block hash + shard id.
    fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error>;
    /// Get block header.
    fn get_block_header(&self, h: &CryptoHash) -> Result<BlockHeader, Error>;
    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error>;
    /// Returns hash of the first available block after genesis.
    fn get_earliest_block_hash(&self) -> Result<Option<CryptoHash>, Error> {
        // To find the earliest available block we use the `tail` marker primarily
        // used by garbage collection system.
        // NOTE: `tail` is the block height at which we can say that there is
        // at most 1 block available in the range from the genesis height to
        // the tail. Thus, the strategy is to find the first block AFTER the tail
        // height, and use the `prev_hash` to get the reference to the earliest
        // block.
        // The earliest block can be the genesis block.
        let head_header_height = self.head_header()?.height();
        let tail = self.tail()?;

        // There is a corner case when there are no blocks after the tail, and
        // the tail is in fact the earliest block available on the chain.
        if let Ok(block_hash) = self.get_block_hash_by_height(tail) {
            return Ok(Some(block_hash));
        }
        for height in tail + 1..=head_header_height {
            if let Ok(block_hash) = self.get_block_hash_by_height(height) {
                let earliest_block_hash = *self.get_block_header(&block_hash)?.prev_hash();
                debug_assert!(matches!(self.block_exists(&earliest_block_hash), Ok(true)));
                return Ok(Some(earliest_block_hash));
            }
        }
        Ok(None)
    }
    /// Returns block header from the current chain for given height if present.
    fn get_block_header_by_height(&self, height: BlockHeight) -> Result<BlockHeader, Error> {
        let hash = self.get_block_hash_by_height(height)?;
        self.get_block_header(&hash)
    }
    fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error>;
    fn get_epoch_light_client_block(
        &self,
        hash: &CryptoHash,
    ) -> Result<Arc<LightClientBlockView>, Error>;
    /// Returns a number of references for Block with `block_hash`
    fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error>;
    /// Returns block header from the current chain defined by `sync_hash` for given height if present.
    fn get_block_header_on_chain_by_height(
        &self,
        sync_hash: &CryptoHash,
        height: BlockHeight,
    ) -> Result<BlockHeader, Error> {
        let mut header = self.get_block_header(sync_hash)?;
        let mut hash = *sync_hash;
        while header.height() > height {
            hash = *header.prev_hash();
            header = self.get_block_header(&hash)?;
        }
        let header_height = header.height();
        if header_height < height {
            return Err(Error::InvalidBlockHeight(header_height));
        }
        self.get_block_header(&hash)
    }
    /// Returns resulting receipt for given block.
    fn get_outgoing_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<Receipt>>, Error>;

    fn get_incoming_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error>;

    /// Collect incoming receipts for shard `shard_id` from
    /// the block at height `last_chunk_height_included` (non-inclusive) to the block `block_hash` (inclusive)
    /// This is because the chunks for the shard are empty for the blocks in between,
    /// so the receipts from these blocks are propagated
    fn get_incoming_receipts_for_shard(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
        mut shard_id: ShardId,
        mut block_hash: CryptoHash,
        last_chunk_height_included: BlockHeight,
    ) -> Result<Vec<ReceiptProofResponse>, Error> {
        println!(
            "get_incoming_receipts_for_shard {} {:?} {}",
            shard_id, block_hash, last_chunk_height_included
        );
        let _span =
            tracing::debug_span!(target: "chain", "get_incoming_receipts_for_shard", ?shard_id, ?block_hash, last_chunk_height_included).entered();

        let mut ret = vec![];

        let target_shard_id = shard_id;
        let target_shard_layout = epoch_manager.get_shard_layout_from_prev_block(&block_hash)?;

        loop {
            let header = self.get_block_header(&block_hash)?;

            if header.height() < last_chunk_height_included {
                panic!("get_incoming_receipts_for_shard failed");
            }

            if header.height() == last_chunk_height_included {
                break;
            }

            // let shard_layout = epoch_manager.get_shard_layout_from_prev_block(&block_hash)?;
            // let prev_shard_layout = epoch_manager.get_shard_layout_from_prev_block(prev_hash)?;

            // if shard_layout != prev_shard_layout {
            //     let parent_shard_id = shard_layout.get_parent_shard_id(shard_id)?;
            //     tracing::debug!(
            //         target: "chain",
            //         version = shard_layout.version(),
            //         prev_version = prev_shard_layout.version(),
            //         ?shard_id,
            //         ?parent_shard_id,
            //         "crossing epoch boundary with shard layout change, updating shard id"
            //     );
            //     shard_id = parent_shard_id;
            // }

            let receipts_proofs = self.get_incoming_receipts(&block_hash, shard_id);
            match receipts_proofs {
                Ok(receipt_proofs) => {
                    tracing::debug!(
                        target: "chain",
                        "found receipts from block with missing chunks",
                    );

                    // If the shard layout changed we need to filter receipts to
                    // make sure we only include receipts where receiver belongs
                    // to the target shard id in the target shard layout.
                    let filtered_receipt_proofs = filter_incoming_receipts_for_shard(
                        &target_shard_layout,
                        target_shard_id,
                        receipt_proofs,
                    );

                    ret.push(ReceiptProofResponse(block_hash, filtered_receipt_proofs.into()));
                }
                Err(err) => {
                    tracing::debug!(
                        target: "chain",
                        ?err,
                        "could not find receipts from block with missing chunks"
                    );

                    // This can happen when all chunks are missing in a block
                    // and then we can safely assume that there aren't any
                    // incoming receipts. It would be nicer to explicitly check
                    // that condition rather than relying on errors when reading
                    // from the db.
                    ret.push(ReceiptProofResponse(block_hash, Arc::new(vec![])));
                }
            }

            let prev_hash = header.prev_hash();
            shard_id = epoch_manager.get_prev_shard_id(&block_hash, shard_id)?.0;
            block_hash = *prev_hash;
        }

        Ok(ret)
    }

    /// Returns whether the block with the given hash was challenged
    fn is_block_challenged(&self, hash: &CryptoHash) -> Result<bool, Error>;

    fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error>;

    /// Returns encoded chunk if it's invalid otherwise None.
    fn is_invalid_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<Arc<EncodedShardChunk>>, Error>;

    fn get_transaction(
        &self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<Arc<SignedTransaction>>, Error>;

    /// Fetch a receipt by id, if it is stored in the store.
    ///
    /// Note that not _all_ receipts are persisted. Some receipts are ephemeral,
    /// get processed immediately after creation and don't even get to the
    /// database.
    fn get_receipt(&self, receipt_id: &CryptoHash) -> Result<Option<Arc<Receipt>>, Error>;

    fn get_genesis_height(&self) -> BlockHeight;

    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error>;

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error>;

    fn get_block_merkle_tree_from_ordinal(
        &self,
        block_ordinal: NumBlocks,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        let block_hash = self.get_block_hash_from_ordinal(block_ordinal)?;
        self.get_block_merkle_tree(&block_hash)
    }

    fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error>;

    fn get_block_height(&self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            Ok(self.get_genesis_height())
        } else {
            Ok(self.get_block_header(hash)?.height())
        }
    }

    /// Get epoch id of the last block with existing chunk for the given shard id.
    fn get_epoch_id_of_last_block_with_chunk(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<EpochId, Error> {
        let mut candidate_hash = *hash;
        let block_header = self.get_block_header(&candidate_hash)?;
        let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id())?;
        let mut shard_id = shard_id;
        let mut shard_index = shard_layout.get_shard_index(shard_id);
        loop {
            let block_header = self.get_block_header(&candidate_hash)?;
            if *block_header
                .chunk_mask()
                .get(shard_index)
                .ok_or_else(|| Error::InvalidShardId(shard_id))?
            {
                break Ok(*block_header.epoch_id());
            }
            candidate_hash = *block_header.prev_hash();
            (shard_id, shard_index) =
                epoch_manager.get_prev_shard_ids(&candidate_hash, vec![shard_id])?[0];
        }
    }
}

/// Given a vector of receipts return only the receipts that should be assigned
/// to the target shard id in the target shard layout. Used when collecting the
/// incoming receipts and the shard layout changed.
fn filter_incoming_receipts_for_shard(
    target_shard_layout: &ShardLayout,
    target_shard_id: ShardId,
    receipt_proofs: Arc<Vec<ReceiptProof>>,
) -> Vec<ReceiptProof> {
    let mut filtered_receipt_proofs = vec![];
    for receipt_proof in receipt_proofs.iter() {
        let mut filtered_receipts = vec![];
        let ReceiptProof(receipts, shard_proof) = receipt_proof.clone();
        for receipt in receipts {
            let receiver_shard_id =
                account_id_to_shard_id(receipt.receiver_id(), target_shard_layout);
            if receiver_shard_id == target_shard_id {
                tracing::trace!(target: "chain", receipt_id=?receipt.receipt_id(), "including receipt");
                filtered_receipts.push(receipt);
            } else {
                tracing::trace!(target: "chain", receipt_id=?receipt.receipt_id(), "excluding receipt");
            }
        }
        // TODO(resharding) adjust the shard proof accordingly
        // currently this only matters for state sync
        let receipt_proof = ReceiptProof(filtered_receipts, shard_proof);
        filtered_receipt_proofs.push(receipt_proof);
    }
    filtered_receipt_proofs
}

/// All chain-related database operations.
pub struct ChainStore {
    store: Store,
    /// Genesis block height.
    genesis_height: BlockHeight,
    /// Latest known.
    latest_known: once_cell::unsync::OnceCell<LatestKnown>,
    /// Current head of the chain
    head: Option<Tip>,
    /// Tail height of the chain,
    tail: Option<BlockHeight>,
    /// Cache with headers.
    pub(crate) headers: CellLruCache<Vec<u8>, BlockHeader>,
    /// Cache with blocks.
    pub(crate) blocks: CellLruCache<Vec<u8>, Block>,
    /// Cache with chunks
    pub(crate) chunks: CellLruCache<Vec<u8>, Arc<ShardChunk>>,
    /// Cache with partial chunks
    pub(crate) partial_chunks: CellLruCache<Vec<u8>, Arc<PartialEncodedChunk>>,
    /// Cache with block extra.
    pub(crate) block_extras: CellLruCache<Vec<u8>, Arc<BlockExtra>>,
    /// Cache with chunk extra.
    pub(crate) chunk_extras: CellLruCache<Vec<u8>, Arc<ChunkExtra>>,
    /// Cache with height to hash on the main chain.
    height: CellLruCache<Vec<u8>, CryptoHash>,
    /// Cache with height to block hash on any chain.
    pub(crate) block_hash_per_height:
        CellLruCache<Vec<u8>, Arc<HashMap<EpochId, HashSet<CryptoHash>>>>,
    /// Next block hashes for each block on the canonical chain
    pub(crate) next_block_hashes: CellLruCache<Vec<u8>, CryptoHash>,
    /// Light client blocks corresponding to the last finalized block of each epoch
    epoch_light_client_blocks: CellLruCache<Vec<u8>, Arc<LightClientBlockView>>,
    /// Cache with outgoing receipts.
    pub(crate) outgoing_receipts: CellLruCache<Vec<u8>, Arc<Vec<Receipt>>>,
    /// Cache with incoming receipts.
    pub(crate) incoming_receipts: CellLruCache<Vec<u8>, Arc<Vec<ReceiptProof>>>,
    /// Invalid chunks.
    pub(crate) invalid_chunks: CellLruCache<Vec<u8>, Arc<EncodedShardChunk>>,
    /// Transactions
    pub(crate) transactions: CellLruCache<Vec<u8>, Arc<SignedTransaction>>,
    /// Receipts
    pub(crate) receipts: CellLruCache<Vec<u8>, Arc<Receipt>>,
    /// Cache with Block Refcounts
    pub(crate) block_refcounts: CellLruCache<Vec<u8>, u64>,
    /// Cache of block hash -> block merkle tree at the current block
    block_merkle_tree: CellLruCache<Vec<u8>, Arc<PartialMerkleTree>>,
    /// Cache of block ordinal to block hash.
    block_ordinal_to_hash: CellLruCache<Vec<u8>, CryptoHash>,
    /// Processed block heights.
    pub(crate) processed_block_heights: CellLruCache<Vec<u8>, ()>,
    /// save_trie_changes should be set to true iff
    /// - archive is false - non-archival nodes need trie changes to perform garbage collection
    /// - archive is true, cold_store is configured and migration to split_storage is finished - node
    /// working in split storage mode needs trie changes in order to do garbage collection on hot.
    save_trie_changes: bool,
}

fn option_to_not_found<T, F>(res: io::Result<Option<T>>, field_name: F) -> Result<T, Error>
where
    F: std::string::ToString,
{
    match res {
        Ok(Some(o)) => Ok(o),
        Ok(None) => Err(Error::DBNotFoundErr(field_name.to_string())),
        Err(e) => Err(e.into()),
    }
}

impl ChainStore {
    pub fn new(store: Store, genesis_height: BlockHeight, save_trie_changes: bool) -> ChainStore {
        ChainStore {
            store,
            genesis_height,
            latest_known: once_cell::unsync::OnceCell::new(),
            head: None,
            tail: None,
            blocks: CellLruCache::new(CACHE_SIZE),
            headers: CellLruCache::new(CACHE_SIZE),
            chunks: CellLruCache::new(CHUNK_CACHE_SIZE),
            partial_chunks: CellLruCache::new(CHUNK_CACHE_SIZE),
            block_extras: CellLruCache::new(CACHE_SIZE),
            chunk_extras: CellLruCache::new(CACHE_SIZE),
            height: CellLruCache::new(CACHE_SIZE),
            block_hash_per_height: CellLruCache::new(CACHE_SIZE),
            block_refcounts: CellLruCache::new(CACHE_SIZE),
            next_block_hashes: CellLruCache::new(CACHE_SIZE),
            epoch_light_client_blocks: CellLruCache::new(CACHE_SIZE),
            outgoing_receipts: CellLruCache::new(CACHE_SIZE),
            incoming_receipts: CellLruCache::new(CACHE_SIZE),
            invalid_chunks: CellLruCache::new(CACHE_SIZE),
            transactions: CellLruCache::new(CHUNK_CACHE_SIZE),
            receipts: CellLruCache::new(CHUNK_CACHE_SIZE),
            block_merkle_tree: CellLruCache::new(CACHE_SIZE),
            block_ordinal_to_hash: CellLruCache::new(CACHE_SIZE),
            processed_block_heights: CellLruCache::new(CACHE_SIZE),
            save_trie_changes,
        }
    }

    pub fn store_update(&mut self) -> ChainStoreUpdate<'_> {
        ChainStoreUpdate::new(self)
    }

    pub fn iterate_state_sync_infos(&self) -> Result<Vec<(CryptoHash, StateSyncInfo)>, Error> {
        self.store
            .iter(DBCol::StateDlInfos)
            .map(|item| match item {
                Ok((k, v)) => Ok((
                    CryptoHash::try_from(k.as_ref()).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("wrong key length: {k:?}"),
                        )
                    })?,
                    StateSyncInfo::try_from_slice(v.as_ref())?,
                )),
                Err(err) => Err(err.into()),
            })
            .collect()
    }

    /// Get outgoing receipts that will be *sent* from shard `shard_id` from block whose prev block
    /// is `prev_block_hash`
    /// Note that the meaning of outgoing receipts here are slightly different from
    /// `save_outgoing_receipts` or `get_outgoing_receipts`.
    /// There, outgoing receipts for a shard refers to receipts that are generated
    /// from the shard from block `prev_block_hash`.
    /// Here, outgoing receipts for a shard refers to receipts that will be sent from this shard
    /// to other shards in the block after `prev_block_hash`
    /// The difference of one block is important because shard layout may change between the previous
    /// block and the current block and the meaning of `shard_id` will change.
    ///
    /// Note, the current way of implementation assumes that at least one chunk is generated before
    /// shard layout are changed twice. This is not a problem right now because we are changing shard
    /// layout for the first time for simple nightshade and generally not a problem if shard layout
    /// changes very rarely.
    /// But we need to implement a more theoretically correct algorithm if shard layouts will change
    /// more often in the future
    /// <https://github.com/near/nearcore/issues/4877>
    pub fn get_outgoing_receipts_for_shard(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
        prev_block_hash: CryptoHash,
        shard_id: ShardId,
        last_included_height: BlockHeight,
    ) -> Result<Vec<Receipt>, Error> {
        let shard_layout = epoch_manager.get_shard_layout_from_prev_block(&prev_block_hash)?;
        let mut receipts_block_hash = prev_block_hash;
        loop {
            let block_header = self.get_block_header(&receipts_block_hash)?;

            if block_header.height() != last_included_height {
                receipts_block_hash = *block_header.prev_hash();
                continue;
            }
            let receipts_shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id())?;

            // get the shard from which the outgoing receipt were generated
            let receipts_shard_id = if shard_layout != receipts_shard_layout {
                shard_layout.get_parent_shard_id(shard_id)?
            } else {
                shard_id
            };

            let mut receipts = self
                .get_outgoing_receipts(&receipts_block_hash, receipts_shard_id)
                .map(|v| v.to_vec())
                .unwrap_or_default();

            if shard_layout != receipts_shard_layout {
                // the shard layout has changed so we need to reassign the outgoing receipts
                let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash)?;
                let protocol_version = epoch_manager.get_epoch_protocol_version(&epoch_id)?;
                Self::reassign_outgoing_receipts_for_resharding(
                    &mut receipts,
                    protocol_version,
                    &shard_layout,
                    shard_id,
                    receipts_shard_id,
                )?;
            }

            return Ok(receipts);
        }
    }

    /// TODO validate if this logic works for Resharding V3.
    fn reassign_outgoing_receipts_for_resharding(
        receipts: &mut Vec<Receipt>,
        protocol_version: ProtocolVersion,
        shard_layout: &ShardLayout,
        shard_id: ShardId,
        receipts_shard_id: ShardId,
    ) -> Result<(), Error> {
        tracing::trace!(target: "resharding", ?protocol_version, ?shard_id, ?receipts_shard_id, "reassign_outgoing_receipts_for_resharding");
        // If simple nightshade v2 is enabled and stable use that.
        // Same reassignment of outgoing receipts works for simple nightshade v3
        if checked_feature!("stable", SimpleNightshadeV2, protocol_version) {
            Self::reassign_outgoing_receipts_for_resharding_v2(
                receipts,
                shard_layout,
                shard_id,
                receipts_shard_id,
            )?;
            return Ok(());
        }
        Ok(())
    }

    /// Reassign the outgoing receipts from the parent shard to the children
    /// shards.
    ///
    /// This method does it based on the "lowest child index" approach where it
    /// assigns all the receipts from parent to the child shard with the lowest
    /// index. It's meant to be used for the resharding from simple nightshade
    /// with 4 shards to simple nightshade v2 with 5 shards and subsequent
    /// reshardings.
    ///
    /// e.g. in the following resharding
    /// 0->0', 1->1', 2->2', 3->3',4'
    /// 0' will get all outgoing receipts from its parent 0
    /// 1' will get all outgoing receipts from its parent 1
    /// 2' will get all outgoing receipts from its parent 2
    /// 3' will get all outgoing receipts from its parent 3
    /// 4' will get no outgoing receipts from its parent 3
    /// All receipts are distributed to children, each exactly once.
    fn reassign_outgoing_receipts_for_resharding_v2(
        receipts: &mut Vec<Receipt>,
        shard_layout: &ShardLayout,
        shard_id: ShardId,
        receipts_shard_id: ShardId,
    ) -> Result<(), Error> {
        let split_shard_ids = shard_layout.get_children_shards_ids(receipts_shard_id);
        let split_shard_ids =
            split_shard_ids.ok_or(Error::InvalidSplitShardsIds(shard_id, receipts_shard_id))?;

        // The target shard id is the split shard with the lowest shard id.
        let target_shard_id = split_shard_ids.iter().min();
        let target_shard_id =
            *target_shard_id.ok_or(Error::InvalidSplitShardsIds(shard_id, receipts_shard_id))?;

        if shard_id == target_shard_id {
            // This shard_id is the lowest index child, it gets all the receipts.
            Ok(())
        } else {
            // This shard_id is not the lowest index child, it gets no receipts.
            receipts.clear();
            Ok(())
        }
    }

    /// For a given transaction, it expires if the block that the chunk points to is more than `validity_period`
    /// ahead of the block that has `base_block_hash`.
    pub fn check_transaction_validity_period(
        &self,
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
            .get_block_height(prev_block_header.last_final_block())
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
                .get_block_header_on_chain_by_height(prev_block_header.hash(), base_height)
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
    /// Returns outcomes on all forks generated by applying transaction or
    /// receipt with the given id.
    pub fn get_outcomes_by_id(
        &self,
        id: &CryptoHash,
    ) -> Result<Vec<ExecutionOutcomeWithIdAndProof>, Error> {
        self.store
            .iter_prefix_ser::<ExecutionOutcomeWithProof>(
                DBCol::TransactionResultForBlock,
                id.as_ref(),
            )
            .map(|item| {
                let (key, outcome_with_proof) = item?;
                let (_, block_hash) = get_outcome_id_block_hash_rev(key.as_ref())?;
                Ok(ExecutionOutcomeWithIdAndProof {
                    proof: outcome_with_proof.proof,
                    block_hash,
                    outcome_with_id: ExecutionOutcomeWithId {
                        id: *id,
                        outcome: outcome_with_proof.outcome,
                    },
                })
            })
            .collect()
    }

    pub fn get_outcome_by_id_and_block_hash(
        &self,
        id: &CryptoHash,
        block_hash: &CryptoHash,
    ) -> Result<Option<ExecutionOutcomeWithProof>, Error> {
        Ok(self.store.get_ser(
            DBCol::TransactionResultForBlock,
            &get_outcome_id_block_hash(id, block_hash),
        )?)
    }

    /// Returns a vector of Outcome ids for given block and shard id
    pub fn get_outcomes_by_block_hash_and_shard_id(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Vec<CryptoHash>, Error> {
        Ok(self
            .store
            .get_ser(DBCol::OutcomeIds, &get_block_shard_id(block_hash, shard_id))?
            .unwrap_or_default())
    }

    /// Get all execution outcomes generated when the chunk are applied
    pub fn get_block_execution_outcomes(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<HashMap<ShardId, Vec<ExecutionOutcomeWithIdAndProof>>, Error> {
        let block = self.get_block(block_hash)?;
        let chunk_headers = block.chunks().iter().cloned().collect::<Vec<_>>();

        let mut res = HashMap::new();
        for chunk_header in chunk_headers {
            let shard_id = chunk_header.shard_id();
            let outcomes = self
                .get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id)?
                .into_iter()
                .filter_map(|id| {
                    let outcome_with_proof =
                        self.get_outcome_by_id_and_block_hash(&id, block_hash).ok()??;
                    Some(ExecutionOutcomeWithIdAndProof {
                        proof: outcome_with_proof.proof,
                        block_hash: *block_hash,
                        outcome_with_id: ExecutionOutcomeWithId {
                            id,
                            outcome: outcome_with_proof.outcome,
                        },
                    })
                })
                .collect::<Vec<_>>();
            res.insert(shard_id, outcomes);
        }
        Ok(res)
    }

    /// Returns a hashmap of epoch id -> set of all blocks got for current (height, epoch_id)
    pub fn get_all_block_hashes_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<Arc<HashMap<EpochId, HashSet<CryptoHash>>>, Error> {
        Ok(self
            .read_with_cache(
                DBCol::BlockPerHeight,
                &self.block_hash_per_height,
                &index_to_bytes(height),
            )?
            .unwrap_or_default())
    }

    /// Returns a HashSet of Chunk Hashes for current Height
    pub fn get_all_chunk_hashes_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<HashSet<ChunkHash>, Error> {
        Ok(self
            .store
            .get_ser(DBCol::ChunkHashesByHeight, &index_to_bytes(height))?
            .unwrap_or_default())
    }

    /// Returns a HashSet of Header Hashes for current Height
    pub fn get_all_header_hashes_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<HashSet<CryptoHash>, Error> {
        Ok(self
            .store
            .get_ser(DBCol::HeaderHashesByHeight, &index_to_bytes(height))?
            .unwrap_or_default())
    }

    pub fn get_state_header(
        &self,
        shard_id: ShardId,
        block_hash: CryptoHash,
    ) -> Result<ShardStateSyncResponseHeader, Error> {
        let key = borsh::to_vec(&StateHeaderKey(shard_id, block_hash))?;
        match self.store.get_ser(DBCol::StateHeaders, &key) {
            Ok(Some(header)) => Ok(header),
            _ => Err(Error::Other("Cannot get shard_state_header".into())),
        }
    }

    /// Returns latest known height and time it was seen.
    pub fn get_latest_known(&self) -> Result<LatestKnown, Error> {
        self.latest_known
            .get_or_try_init(|| {
                option_to_not_found(
                    self.store.get_ser(DBCol::BlockMisc, LATEST_KNOWN_KEY),
                    "LATEST_KNOWN_KEY",
                )
            })
            .cloned()
    }

    /// Save the latest known.
    pub fn save_latest_known(&mut self, latest_known: LatestKnown) -> Result<(), Error> {
        let mut store_update = self.store.store_update();
        store_update.set_ser(DBCol::BlockMisc, LATEST_KNOWN_KEY, &latest_known)?;
        self.latest_known = once_cell::unsync::OnceCell::from(latest_known);
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

        let storage_key = KeyForStateChanges::for_block(block_hash);

        let mut block_changes = storage_key.find_iter(&self.store);

        Ok(StateChangesKinds::from_changes(&mut block_changes)?)
    }

    pub fn get_state_changes_with_cause_in_block(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<StateChanges, Error> {
        let storage_key = KeyForStateChanges::for_block(block_hash);

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
                    let data_key = TrieKey::Account { account_id: account_id.clone() };
                    let storage_key = KeyForStateChanges::from_trie_key(block_hash, &data_key);
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
                    };
                    let storage_key = KeyForStateChanges::from_trie_key(block_hash, &data_key);
                    let changes_per_key = storage_key.find_exact_iter(&self.store);
                    changes.extend(StateChanges::from_access_key_changes(changes_per_key)?);
                }
                changes
            }
            StateChangesRequest::AllAccessKeyChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = trie_key_parsers::get_raw_prefix_for_access_keys(account_id);
                    let storage_key = KeyForStateChanges::from_raw_key(block_hash, &data_key);
                    let changes_per_key_prefix = storage_key.find_iter(&self.store);
                    changes.extend(StateChanges::from_access_key_changes(changes_per_key_prefix)?);
                }
                changes
            }
            StateChangesRequest::ContractCodeChanges { account_ids } => {
                let mut changes = StateChanges::new();
                for account_id in account_ids {
                    let data_key = TrieKey::ContractCode { account_id: account_id.clone() };
                    let storage_key = KeyForStateChanges::from_trie_key(block_hash, &data_key);
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
                    let storage_key = KeyForStateChanges::from_raw_key(block_hash, &data_key);
                    let changes_per_key_prefix = storage_key.find_iter(&self.store);
                    changes.extend(StateChanges::from_data_changes(changes_per_key_prefix)?);
                }
                changes
            }
        })
    }

    pub fn get_store_statistics(&self) -> Option<StoreStatistics> {
        self.store.get_store_statistics()
    }

    fn read_with_cache<'a, T: BorshDeserialize + Clone + 'a>(
        &self,
        col: DBCol,
        cache: &'a CellLruCache<Vec<u8>, T>,
        key: &[u8],
    ) -> io::Result<Option<T>> {
        if let Some(value) = cache.get(key) {
            return Ok(Some(value));
        }
        if let Some(result) = self.store.get_ser::<T>(col, key)? {
            cache.put(key.to_vec(), result.clone());
            return Ok(Some(result));
        }
        Ok(None)
    }

    /// Constructs key 'STATE_SYNC_DUMP:<ShardId>',
    /// for example 'STATE_SYNC_DUMP:2' for shard_id=2.
    /// Doesn't contain epoch_id, because only one dump process per shard is allowed.
    fn state_sync_dump_progress_key(shard_id: ShardId) -> Vec<u8> {
        let mut key = STATE_SYNC_DUMP_KEY.to_vec();
        key.extend(b":".to_vec());
        key.extend(shard_id.to_le_bytes());
        key
    }

    /// Retrieves STATE_SYNC_DUMP for the given shard.
    pub fn get_state_sync_dump_progress(
        &self,
        shard_id: ShardId,
    ) -> Result<StateSyncDumpProgress, Error> {
        option_to_not_found(
            self.store
                .get_ser(DBCol::BlockMisc, &ChainStore::state_sync_dump_progress_key(shard_id)),
            format!("STATE_SYNC_DUMP:{}", shard_id),
        )
    }

    /// Updates STATE_SYNC_DUMP for the given shard.
    pub fn set_state_sync_dump_progress(
        &self,
        shard_id: ShardId,
        value: Option<StateSyncDumpProgress>,
    ) -> Result<(), Error> {
        let mut store_update = self.store.store_update();
        let key = ChainStore::state_sync_dump_progress_key(shard_id);
        match value {
            None => store_update.delete(DBCol::BlockMisc, &key),
            Some(value) => store_update.set_ser(DBCol::BlockMisc, &key, &value)?,
        }
        store_update.commit().map_err(|err| err.into())
    }
}

impl ChainStoreAccess for ChainStore {
    fn chain_store(&self) -> &ChainStore {
        &self
    }

    fn store(&self) -> &Store {
        &self.store
    }
    /// The chain head.
    fn head(&self) -> Result<Tip, Error> {
        if let Some(ref tip) = self.head {
            Ok(tip.clone())
        } else {
            option_to_not_found(self.store.get_ser(DBCol::BlockMisc, HEAD_KEY), "HEAD")
        }
    }

    /// The chain Blocks Tail height, used by GC.
    fn tail(&self) -> Result<BlockHeight, Error> {
        if let Some(tail) = self.tail.as_ref() {
            Ok(*tail)
        } else {
            self.store
                .get_ser(DBCol::BlockMisc, TAIL_KEY)
                .map(|option| option.unwrap_or(self.genesis_height))
                .map_err(|e| e.into())
        }
    }

    /// The chain Chunks Tail height, used by GC.
    fn chunk_tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(DBCol::BlockMisc, CHUNK_TAIL_KEY)
            .map(|option| option.unwrap_or(self.genesis_height))
            .map_err(|e| e.into())
    }

    fn fork_tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(DBCol::BlockMisc, FORK_TAIL_KEY)
            .map(|option| option.unwrap_or(self.genesis_height))
            .map_err(|e| e.into())
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&self) -> Result<BlockHeader, Error> {
        self.get_block_header(&self.head()?.last_block_hash)
    }

    /// Largest height for which we created a doomslug endorsement
    fn largest_target_height(&self) -> Result<BlockHeight, Error> {
        match self.store.get_ser(DBCol::BlockMisc, LARGEST_TARGET_HEIGHT_KEY) {
            Ok(Some(o)) => Ok(o),
            Ok(None) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// Head of the header chain (not the same thing as head_header).
    fn header_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(DBCol::BlockMisc, HEADER_HEAD_KEY), "HEADER_HEAD")
    }

    /// Final head of the chain.
    fn final_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(DBCol::BlockMisc, FINAL_HEAD_KEY), "FINAL HEAD")
    }

    /// Get full block.
    fn get_block(&self, h: &CryptoHash) -> Result<Block, Error> {
        option_to_not_found(
            self.read_with_cache(DBCol::Block, &self.blocks, h.as_ref()),
            format_args!("BLOCK: {}", h),
        )
    }

    /// Get full chunk.
    fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<ShardChunk>, Error> {
        match self.read_with_cache(DBCol::Chunks, &self.chunks, chunk_hash.as_ref()) {
            Ok(Some(shard_chunk)) => Ok(shard_chunk),
            _ => Err(Error::ChunkMissing(chunk_hash.clone())),
        }
    }

    /// Get partial chunk.
    fn get_partial_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<PartialEncodedChunk>, Error> {
        match self.read_with_cache(DBCol::PartialChunks, &self.partial_chunks, chunk_hash.as_ref())
        {
            Ok(Some(shard_chunk)) => Ok(shard_chunk),
            _ => Err(Error::ChunkMissing(chunk_hash.clone())),
        }
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(DBCol::Block, h.as_ref()).map_err(|e| e.into())
    }

    fn chunk_exists(&self, h: &ChunkHash) -> Result<bool, Error> {
        if self.chunks.get(h.as_ref()).is_some() {
            Ok(true)
        } else {
            self.store.exists(DBCol::Chunks, h.as_ref()).map_err(|e| e.into())
        }
    }

    /// Get previous header.
    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.get_block_header(header.prev_hash())
    }

    /// Information from applying block.
    fn get_block_extra(&self, block_hash: &CryptoHash) -> Result<Arc<BlockExtra>, Error> {
        option_to_not_found(
            self.read_with_cache(DBCol::BlockExtra, &self.block_extras, block_hash.as_ref()),
            format_args!("BLOCK EXTRA: {}", block_hash),
        )
    }

    /// Information from applying chunk.
    fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        option_to_not_found(
            self.read_with_cache(
                DBCol::ChunkExtra,
                &self.chunk_extras,
                &get_block_shard_uid(block_hash, shard_uid),
            ),
            format_args!("CHUNK EXTRA: {}:{:?}", block_hash, shard_uid),
        )
    }

    /// Get block header.
    fn get_block_header(&self, h: &CryptoHash) -> Result<BlockHeader, Error> {
        option_to_not_found(
            self.read_with_cache(DBCol::BlockHeader, &self.headers, h.as_ref()),
            format_args!("BLOCK HEADER: {}", h),
        )
    }

    /// Returns hash of the block on the main chain for given height.
    fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::BlockHeight, &index_to_bytes(height)),
            format_args!("BLOCK HEIGHT: {}", height),
        )
        // TODO: cache needs to be deleted when things get updated.
        //        option_to_not_found(
        //            self.read_with_cache(
        //                DBCol::BlockHeight,
        //                &mut self.height,
        //                &index_to_bytes(height),
        //            ),
        //            format_args!("BLOCK HEIGHT: {}", height),
        //        )
    }

    fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.read_with_cache(DBCol::NextBlockHashes, &self.next_block_hashes, hash.as_ref()),
            format_args!("NEXT BLOCK HASH: {}", hash),
        )
    }

    fn get_epoch_light_client_block(
        &self,
        hash: &CryptoHash,
    ) -> Result<Arc<LightClientBlockView>, Error> {
        option_to_not_found(
            self.read_with_cache(
                DBCol::EpochLightClientBlocks,
                &self.epoch_light_client_blocks,
                hash.as_ref(),
            ),
            format_args!("EPOCH LIGHT CLIENT BLOCK: {}", hash),
        )
    }

    fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error> {
        option_to_not_found(
            self.read_with_cache(DBCol::BlockRefCount, &self.block_refcounts, block_hash.as_ref()),
            format_args!("BLOCK REFCOUNT: {}", block_hash),
        )
    }

    /// Get outgoing receipts *generated* from shard `shard_id` in block `prev_hash`
    /// Note that this function is different from get_outgoing_receipts_for_shard, see comments there
    fn get_outgoing_receipts(
        &self,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<Receipt>>, Error> {
        option_to_not_found(
            self.read_with_cache(
                DBCol::OutgoingReceipts,
                &self.outgoing_receipts,
                &get_block_shard_id(prev_block_hash, shard_id),
            ),
            format_args!("OUTGOING RECEIPT: {} {}", prev_block_hash, shard_id),
        )
    }

    fn get_incoming_receipts(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error> {
        option_to_not_found(
            self.read_with_cache(
                DBCol::IncomingReceipts,
                &self.incoming_receipts,
                &get_block_shard_id(block_hash, shard_id),
            ),
            format_args!("INCOMING RECEIPT: {} {}", block_hash, shard_id),
        )
    }

    fn get_blocks_to_catchup(&self, hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        Ok(self.store.get_ser(DBCol::BlocksToCatchup, hash.as_ref())?.unwrap_or_default())
    }

    fn is_block_challenged(&self, hash: &CryptoHash) -> Result<bool, Error> {
        Ok(self.store.get_ser(DBCol::ChallengedBlocks, hash.as_ref())?.unwrap_or_default())
    }

    fn is_invalid_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<Arc<EncodedShardChunk>>, Error> {
        self.read_with_cache(DBCol::InvalidChunks, &self.invalid_chunks, chunk_hash.as_ref())
            .map_err(|err| err.into())
    }

    fn get_transaction(
        &self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<Arc<SignedTransaction>>, Error> {
        self.read_with_cache(DBCol::Transactions, &self.transactions, tx_hash.as_ref())
            .map_err(|e| e.into())
    }

    fn get_receipt(&self, receipt_id: &CryptoHash) -> Result<Option<Arc<Receipt>>, Error> {
        self.read_with_cache(DBCol::Receipts, &self.receipts, receipt_id.as_ref())
            .map_err(|e| e.into())
    }

    fn get_genesis_height(&self) -> BlockHeight {
        self.genesis_height
    }

    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        option_to_not_found(
            self.read_with_cache(
                DBCol::BlockMerkleTree,
                &self.block_merkle_tree,
                block_hash.as_ref(),
            ),
            format_args!("BLOCK MERKLE TREE: {}", block_hash),
        )
    }

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.read_with_cache(
                DBCol::BlockOrdinal,
                &self.block_ordinal_to_hash,
                &index_to_bytes(block_ordinal),
            ),
            format_args!("BLOCK ORDINAL: {}", block_ordinal),
        )
    }

    fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error> {
        self.read_with_cache(
            DBCol::ProcessedBlockHeights,
            &self.processed_block_heights,
            &index_to_bytes(height),
        )
        .map(|r| r.is_some())
        .map_err(|e| e.into())
    }
}

/// Cache update for ChainStore
#[derive(Default)]
pub(crate) struct ChainStoreCacheUpdate {
    blocks: HashMap<CryptoHash, Block>,
    headers: HashMap<CryptoHash, BlockHeader>,
    block_extras: HashMap<CryptoHash, Arc<BlockExtra>>,
    chunk_extras: HashMap<(CryptoHash, ShardUId), Arc<ChunkExtra>>,
    chunks: HashMap<ChunkHash, Arc<ShardChunk>>,
    partial_chunks: HashMap<ChunkHash, Arc<PartialEncodedChunk>>,
    block_hash_per_height: HashMap<BlockHeight, HashMap<EpochId, HashSet<CryptoHash>>>,
    pub(crate) height_to_hashes: HashMap<BlockHeight, Option<CryptoHash>>,
    next_block_hashes: HashMap<CryptoHash, CryptoHash>,
    epoch_light_client_blocks: HashMap<CryptoHash, Arc<LightClientBlockView>>,
    outgoing_receipts: HashMap<(CryptoHash, ShardId), Arc<Vec<Receipt>>>,
    incoming_receipts: HashMap<(CryptoHash, ShardId), Arc<Vec<ReceiptProof>>>,
    outcomes: HashMap<(CryptoHash, CryptoHash), ExecutionOutcomeWithProof>,
    outcome_ids: HashMap<(CryptoHash, ShardId), Vec<CryptoHash>>,
    invalid_chunks: HashMap<ChunkHash, Arc<EncodedShardChunk>>,
    transactions: HashMap<CryptoHash, Arc<SignedTransaction>>,
    receipts: HashMap<CryptoHash, Arc<Receipt>>,
    block_refcounts: HashMap<CryptoHash, u64>,
    block_merkle_tree: HashMap<CryptoHash, Arc<PartialMerkleTree>>,
    block_ordinal_to_hash: HashMap<NumBlocks, CryptoHash>,
    processed_block_heights: HashSet<BlockHeight>,
}

/// Provides layer to update chain without touching the underlying database.
/// This serves few purposes, main one is that even if executable exists/fails during update the database is in consistent state.
pub struct ChainStoreUpdate<'a> {
    chain_store: &'a mut ChainStore,
    store_updates: Vec<StoreUpdate>,
    /// Blocks added during this update. Takes ownership (unclear how to not do it because of failure exists).
    pub(crate) chain_store_cache_update: ChainStoreCacheUpdate,
    head: Option<Tip>,
    tail: Option<BlockHeight>,
    chunk_tail: Option<BlockHeight>,
    fork_tail: Option<BlockHeight>,
    header_head: Option<Tip>,
    final_head: Option<Tip>,
    largest_target_height: Option<BlockHeight>,
    trie_changes: Vec<WrappedTrieChanges>,
    state_transition_data: HashMap<(CryptoHash, ShardId), StoredChunkStateTransitionData>,
    add_blocks_to_catchup: Vec<(CryptoHash, CryptoHash)>,
    // A pair (prev_hash, hash) to be removed from blocks to catchup
    remove_blocks_to_catchup: Vec<(CryptoHash, CryptoHash)>,
    // A prev_hash to be removed with all the hashes associated with it
    remove_prev_blocks_to_catchup: Vec<CryptoHash>,
    add_state_sync_infos: Vec<StateSyncInfo>,
    remove_state_sync_infos: Vec<CryptoHash>,
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
            state_transition_data: Default::default(),
            add_blocks_to_catchup: vec![],
            remove_blocks_to_catchup: vec![],
            remove_prev_blocks_to_catchup: vec![],
            add_state_sync_infos: vec![],
            remove_state_sync_infos: vec![],
            challenged_blocks: HashSet::default(),
        }
    }
}

impl<'a> ChainStoreAccess for ChainStoreUpdate<'a> {
    fn chain_store(&self) -> &ChainStore {
        &self.chain_store
    }

    fn store(&self) -> &Store {
        &self.chain_store.store
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
            Ok(*tail)
        } else {
            self.chain_store.tail()
        }
    }

    /// The chain Chunks Tail height, used by GC.
    fn chunk_tail(&self) -> Result<BlockHeight, Error> {
        if let Some(chunk_tail) = &self.chunk_tail {
            Ok(*chunk_tail)
        } else {
            self.chain_store.chunk_tail()
        }
    }

    /// Fork tail used by GC
    fn fork_tail(&self) -> Result<BlockHeight, Error> {
        if let Some(fork_tail) = &self.fork_tail {
            Ok(*fork_tail)
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
            Ok(*largest_target_height)
        } else {
            self.chain_store.largest_target_height()
        }
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    fn head_header(&self) -> Result<BlockHeader, Error> {
        self.get_block_header(&(self.head()?.last_block_hash))
    }

    /// Get full block.
    fn get_block(&self, h: &CryptoHash) -> Result<Block, Error> {
        if let Some(block) = self.chain_store_cache_update.blocks.get(h) {
            Ok(block.clone())
        } else {
            self.chain_store.get_block(h)
        }
    }

    /// Does this full block exist?
    fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        Ok(self.chain_store_cache_update.blocks.contains_key(h)
            || self.chain_store.block_exists(h)?)
    }

    fn chunk_exists(&self, h: &ChunkHash) -> Result<bool, Error> {
        Ok(self.chain_store_cache_update.chunks.contains_key(h)
            || self.chain_store.chunk_exists(h)?)
    }

    /// Get previous header.
    fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.get_block_header(header.prev_hash())
    }

    fn get_block_extra(&self, block_hash: &CryptoHash) -> Result<Arc<BlockExtra>, Error> {
        if let Some(block_extra) = self.chain_store_cache_update.block_extras.get(block_hash) {
            Ok(Arc::clone(block_extra))
        } else {
            self.chain_store.get_block_extra(block_hash)
        }
    }

    /// Get state root hash after applying header with given hash.
    fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        if let Some(chunk_extra) =
            self.chain_store_cache_update.chunk_extras.get(&(*block_hash, *shard_uid))
        {
            Ok(Arc::clone(chunk_extra))
        } else {
            self.chain_store.get_chunk_extra(block_hash, shard_uid)
        }
    }

    /// Get block header.
    fn get_block_header(&self, hash: &CryptoHash) -> Result<BlockHeader, Error> {
        if let Some(header) = self.chain_store_cache_update.headers.get(hash).cloned() {
            Ok(header)
        } else {
            self.chain_store.get_block_header(hash)
        }
    }

    /// Get block header from the current chain by height.
    fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        match self.chain_store_cache_update.height_to_hashes.get(&height) {
            Some(Some(hash)) => Ok(*hash),
            Some(None) => Err(Error::DBNotFoundErr(format!("BLOCK HEIGHT: {}", height))),
            None => self.chain_store.get_block_hash_by_height(height),
        }
    }

    fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error> {
        if let Some(refcount) = self.chain_store_cache_update.block_refcounts.get(block_hash) {
            Ok(*refcount)
        } else {
            let refcount = match self.chain_store.get_block_refcount(block_hash) {
                Ok(refcount) => refcount,
                Err(e) => match e {
                    Error::DBNotFoundErr(_) => 0,
                    _ => return Err(e),
                },
            };
            Ok(refcount)
        }
    }

    fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error> {
        if let Some(next_hash) = self.chain_store_cache_update.next_block_hashes.get(hash) {
            Ok(*next_hash)
        } else {
            self.chain_store.get_next_block_hash(hash)
        }
    }

    fn get_epoch_light_client_block(
        &self,
        hash: &CryptoHash,
    ) -> Result<Arc<LightClientBlockView>, Error> {
        if let Some(light_client_block) =
            self.chain_store_cache_update.epoch_light_client_blocks.get(hash)
        {
            Ok(Arc::clone(light_client_block))
        } else {
            self.chain_store.get_epoch_light_client_block(hash)
        }
    }

    /// Get receipts produced for block with given hash.
    fn get_outgoing_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<Receipt>>, Error> {
        if let Some(receipts) =
            self.chain_store_cache_update.outgoing_receipts.get(&(*hash, shard_id))
        {
            Ok(Arc::clone(receipts))
        } else {
            self.chain_store.get_outgoing_receipts(hash, shard_id)
        }
    }

    /// Get receipts produced for block with given hash.
    fn get_incoming_receipts(
        &self,
        hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error> {
        if let Some(receipt_proofs) =
            self.chain_store_cache_update.incoming_receipts.get(&(*hash, shard_id))
        {
            Ok(Arc::clone(receipt_proofs))
        } else {
            self.chain_store.get_incoming_receipts(hash, shard_id)
        }
    }

    fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<ShardChunk>, Error> {
        if let Some(chunk) = self.chain_store_cache_update.chunks.get(chunk_hash) {
            Ok(Arc::clone(chunk))
        } else {
            self.chain_store.get_chunk(chunk_hash)
        }
    }

    fn get_partial_chunk(&self, chunk_hash: &ChunkHash) -> Result<Arc<PartialEncodedChunk>, Error> {
        if let Some(partial_chunk) = self.chain_store_cache_update.partial_chunks.get(chunk_hash) {
            Ok(Arc::clone(partial_chunk))
        } else {
            self.chain_store.get_partial_chunk(chunk_hash)
        }
    }

    fn get_chunk_clone_from_header(&self, header: &ShardChunkHeader) -> Result<ShardChunk, Error> {
        if let Some(chunk) = self.chain_store_cache_update.chunks.get(&header.chunk_hash()) {
            Ok(ShardChunk::clone(chunk))
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

    fn is_block_challenged(&self, hash: &CryptoHash) -> Result<bool, Error> {
        if self.challenged_blocks.contains(hash) {
            return Ok(true);
        }
        self.chain_store.is_block_challenged(hash)
    }

    fn is_invalid_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<Arc<EncodedShardChunk>>, Error> {
        if let Some(chunk) = self.chain_store_cache_update.invalid_chunks.get(chunk_hash) {
            Ok(Some(Arc::clone(chunk)))
        } else {
            self.chain_store.is_invalid_chunk(chunk_hash)
        }
    }

    fn get_transaction(
        &self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<Arc<SignedTransaction>>, Error> {
        if let Some(tx) = self.chain_store_cache_update.transactions.get(tx_hash) {
            Ok(Some(Arc::clone(tx)))
        } else {
            self.chain_store.get_transaction(tx_hash)
        }
    }

    fn get_receipt(&self, receipt_id: &CryptoHash) -> Result<Option<Arc<Receipt>>, Error> {
        if let Some(receipt) = self.chain_store_cache_update.receipts.get(receipt_id) {
            Ok(Some(Arc::clone(receipt)))
        } else {
            self.chain_store.get_receipt(receipt_id)
        }
    }

    fn get_genesis_height(&self) -> BlockHeight {
        self.chain_store.genesis_height
    }

    fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<PartialMerkleTree>, Error> {
        if let Some(merkle_tree) = self.chain_store_cache_update.block_merkle_tree.get(block_hash) {
            Ok(Arc::clone(&merkle_tree))
        } else {
            self.chain_store.get_block_merkle_tree(block_hash)
        }
    }

    fn get_block_hash_from_ordinal(&self, block_ordinal: NumBlocks) -> Result<CryptoHash, Error> {
        if let Some(block_hash) =
            self.chain_store_cache_update.block_ordinal_to_hash.get(&block_ordinal)
        {
            Ok(*block_hash)
        } else {
            self.chain_store.get_block_hash_from_ordinal(block_ordinal)
        }
    }

    fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error> {
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

    /// This function checks that the block is not on a chain with challenged blocks and updates
    /// fields in ChainStore that stores information of the canonical chain
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
            // Override block ordinal to hash mapping for blocks in between.
            // At this point block_merkle_tree for header is already saved.
            let block_ordinal = self.get_block_merkle_tree(&header_hash)?.size();
            self.chain_store_cache_update.block_ordinal_to_hash.insert(block_ordinal, header_hash);
            match self.get_block_hash_by_height(header_height) {
                Ok(cur_hash) if cur_hash == header_hash => {
                    // Found common ancestor.
                    return Ok(());
                }
                _ => {
                    // TODO: remove this check from this function and use Chain::check_if_challenged_block_on_chain
                    // I'm not doing that now because I'm afraid that this will make header sync take
                    // even longer.
                    if self.is_block_challenged(&header_hash)? {
                        return Err(Error::ChallengedBlockOnChain);
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
        // Bowen: It seems that height_to_hashes is used to update DBCol::BlockHeight, which stores blocks,
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

        match self.header_head() {
            Ok(prev_tip) => {
                if prev_tip.height > t.height {
                    for height in (t.height + 1)..=prev_tip.height {
                        self.chain_store_cache_update.height_to_hashes.insert(height, None);
                    }
                }
            }
            Err(err) => match err {
                Error::DBNotFoundErr(_) => {}
                e => return Err(e),
            },
        }

        // save block ordinal and height if we need to update header head
        let block_ordinal = self.get_block_merkle_tree(&t.last_block_hash)?.size();
        self.chain_store_cache_update
            .block_ordinal_to_hash
            .insert(block_ordinal, t.last_block_hash);
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

    #[cfg(feature = "test_features")]
    pub fn adv_save_latest_known(&mut self, height: BlockHeight) -> Result<(), Error> {
        let header = self.get_block_header_by_height(height)?;
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
        self.chain_store_cache_update.block_extras.insert(*block_hash, Arc::new(block_extra));
    }

    /// Save post applying chunk extra info.
    pub fn save_chunk_extra(
        &mut self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
        chunk_extra: ChunkExtra,
    ) {
        self.chain_store_cache_update
            .chunk_extras
            .insert((*block_hash, *shard_uid), Arc::new(chunk_extra));
    }

    pub fn save_chunk(&mut self, chunk: ShardChunk) {
        for transaction in chunk.transactions() {
            self.chain_store_cache_update
                .transactions
                .insert(transaction.get_hash(), Arc::new(transaction.clone()));
        }
        for receipt in chunk.prev_outgoing_receipts() {
            self.chain_store_cache_update
                .receipts
                .insert(*receipt.receipt_id(), Arc::new(receipt.clone()));
        }
        self.chain_store_cache_update.chunks.insert(chunk.chunk_hash(), Arc::new(chunk));
    }

    pub fn save_partial_chunk(&mut self, partial_chunk: PartialEncodedChunk) {
        self.chain_store_cache_update
            .partial_chunks
            .insert(partial_chunk.chunk_hash(), Arc::new(partial_chunk));
    }

    pub fn save_block_merkle_tree(
        &mut self,
        block_hash: CryptoHash,
        block_merkle_tree: PartialMerkleTree,
    ) {
        self.chain_store_cache_update
            .block_merkle_tree
            .insert(block_hash, Arc::new(block_merkle_tree));
    }

    fn update_and_save_block_merkle_tree(&mut self, header: &BlockHeader) -> Result<(), Error> {
        if header.is_genesis() {
            self.save_block_merkle_tree(*header.hash(), PartialMerkleTree::default());
        } else {
            let prev_hash = header.prev_hash();
            let old_merkle_tree = self.get_block_merkle_tree(prev_hash)?;
            let mut new_merkle_tree = PartialMerkleTree::clone(&old_merkle_tree);
            new_merkle_tree.insert(*prev_hash);
            self.save_block_merkle_tree(*header.hash(), new_merkle_tree);
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
        self.chain_store_cache_update.next_block_hashes.insert(*hash, next_hash);
    }

    pub fn save_epoch_light_client_block(
        &mut self,
        epoch_hash: &CryptoHash,
        light_client_block: LightClientBlockView,
    ) {
        self.chain_store_cache_update
            .epoch_light_client_blocks
            .insert(*epoch_hash, Arc::new(light_client_block));
    }

    // save the outgoing receipts generated by chunk from block `hash` for shard `shard_id`
    pub fn save_outgoing_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        outgoing_receipts: Vec<Receipt>,
    ) {
        self.chain_store_cache_update
            .outgoing_receipts
            .insert((*hash, shard_id), Arc::new(outgoing_receipts));
    }

    pub fn save_incoming_receipt(
        &mut self,
        hash: &CryptoHash,
        shard_id: ShardId,
        receipt_proof: Arc<Vec<ReceiptProof>>,
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
            self.chain_store_cache_update.outcomes.insert(
                (outcome_with_id.id, *block_hash),
                ExecutionOutcomeWithProof { outcome: outcome_with_id.outcome, proof },
            );
        }
        self.chain_store_cache_update.outcome_ids.insert((*block_hash, shard_id), outcome_ids);
    }

    pub fn save_trie_changes(&mut self, trie_changes: WrappedTrieChanges) {
        self.trie_changes.push(trie_changes);
    }

    pub fn save_state_transition_data(
        &mut self,
        block_hash: CryptoHash,
        shard_id: ShardId,
        partial_storage: Option<PartialStorage>,
        applied_receipts_hash: CryptoHash,
    ) {
        if let Some(partial_storage) = partial_storage {
            self.state_transition_data.insert(
                (block_hash, shard_id),
                StoredChunkStateTransitionData {
                    base_state: partial_storage.nodes,
                    receipts_hash: applied_receipts_hash,
                },
            );
        }
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

    pub fn add_state_sync_info(&mut self, info: StateSyncInfo) {
        self.add_state_sync_infos.push(info);
    }

    pub fn remove_state_sync_info(&mut self, hash: CryptoHash) {
        self.remove_state_sync_infos.push(hash);
    }

    pub fn save_challenged_block(&mut self, hash: CryptoHash) {
        self.challenged_blocks.insert(hash);
    }

    pub fn save_invalid_chunk(&mut self, chunk: EncodedShardChunk) {
        self.chain_store_cache_update.invalid_chunks.insert(chunk.chunk_hash(), Arc::new(chunk));
    }

    pub fn save_block_height_processed(&mut self, height: BlockHeight) {
        self.chain_store_cache_update.processed_block_heights.insert(height);
    }

    pub fn inc_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let refcount = match self.get_block_refcount(block_hash) {
            Ok(refcount) => refcount,
            Err(e) => match e {
                Error::DBNotFoundErr(_) => 0,
                _ => return Err(e),
            },
        };
        self.chain_store_cache_update.block_refcounts.insert(*block_hash, refcount + 1);
        Ok(())
    }

    pub fn dec_block_refcount(&mut self, block_hash: &CryptoHash) -> Result<(), Error> {
        let refcount = self.get_block_refcount(block_hash)?;
        if refcount > 0 {
            self.chain_store_cache_update.block_refcounts.insert(*block_hash, refcount - 1);
            Ok(())
        } else {
            debug_assert!(false, "refcount can not be negative");
            Err(Error::Other(format!("cannot decrease refcount for {:?}", block_hash)))
        }
    }

    pub fn reset_tail(&mut self) {
        self.tail = None;
        self.chunk_tail = None;
        self.fork_tail = None;
    }

    pub fn update_tail(&mut self, height: BlockHeight) -> Result<(), Error> {
        self.tail = Some(height);
        let genesis_height = self.get_genesis_height();
        // When fork tail is behind tail, it doesn't hurt to set it to tail for consistency.
        if self.fork_tail()? < height {
            self.fork_tail = Some(height);
        }

        let chunk_tail = self.chunk_tail()?;
        if chunk_tail == genesis_height {
            // For consistency, Chunk Tail should be set if Tail is set
            self.chunk_tail = Some(self.get_genesis_height());
        }
        Ok(())
    }

    pub fn update_fork_tail(&mut self, height: BlockHeight) {
        self.fork_tail = Some(height);
    }

    pub fn update_chunk_tail(&mut self, height: BlockHeight) {
        self.chunk_tail = Some(height);
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
            store_update.set_ser(DBCol::BlockMisc, key, &t)?;
        }
        Ok(())
    }

    /// Only used in mock network
    /// Create a new ChainStoreUpdate that copies the necessary chain state related to `block_hash`
    /// from `source_store` to the current store.
    pub fn copy_chain_state_as_of_block(
        chain_store: &'a mut ChainStore,
        block_hash: &CryptoHash,
        source_epoch_manager: &dyn EpochManagerAdapter,
        source_store: &ChainStore,
    ) -> Result<ChainStoreUpdate<'a>, Error> {
        let mut chain_store_update = ChainStoreUpdate::new(chain_store);
        let block = source_store.get_block(block_hash)?;
        let header = block.header().clone();
        let height = header.height();
        let tip = Tip {
            height,
            last_block_hash: *block_hash,
            prev_block_hash: *header.prev_hash(),
            epoch_id: *header.epoch_id(),
            next_epoch_id: *header.next_epoch_id(),
        };
        chain_store_update.head = Some(tip.clone());
        chain_store_update.tail = Some(height);
        chain_store_update.chunk_tail = Some(height);
        chain_store_update.fork_tail = Some(height);
        chain_store_update.header_head = Some(tip.clone());
        chain_store_update.final_head = Some(tip);
        chain_store_update.chain_store_cache_update.blocks.insert(*block_hash, block.clone());
        chain_store_update.chain_store_cache_update.headers.insert(*block_hash, header.clone());
        // store all headers until header.last_final_block
        // needed to light client
        let mut prev_hash = *header.prev_hash();
        let last_final_hash = header.last_final_block();
        loop {
            let header = source_store.get_block_header(&prev_hash)?;
            chain_store_update.chain_store_cache_update.headers.insert(prev_hash, header.clone());
            if &prev_hash == last_final_hash {
                break;
            } else {
                chain_store_update
                    .chain_store_cache_update
                    .next_block_hashes
                    .insert(*header.prev_hash(), prev_hash);
                prev_hash = *header.prev_hash();
            }
        }
        chain_store_update
            .chain_store_cache_update
            .block_extras
            .insert(*block_hash, source_store.get_block_extra(block_hash)?);
        let shard_layout = source_epoch_manager.get_shard_layout(&header.epoch_id())?;
        for shard_uid in shard_layout.shard_uids() {
            chain_store_update.chain_store_cache_update.chunk_extras.insert(
                (*block_hash, shard_uid),
                source_store.get_chunk_extra(block_hash, &shard_uid)?.clone(),
            );
        }
        for (shard_index, chunk_header) in block.chunks().iter().enumerate() {
            let shard_id = shard_layout.get_shard_id(shard_index);
            let chunk_hash = chunk_header.chunk_hash();
            chain_store_update
                .chain_store_cache_update
                .chunks
                .insert(chunk_hash.clone(), source_store.get_chunk(&chunk_hash)?.clone());
            chain_store_update.chain_store_cache_update.outgoing_receipts.insert(
                (*block_hash, shard_id),
                source_store.get_outgoing_receipts(block_hash, shard_id)?.clone(),
            );
            chain_store_update.chain_store_cache_update.incoming_receipts.insert(
                (*block_hash, shard_id),
                source_store.get_incoming_receipts(block_hash, shard_id)?.clone(),
            );
            let outcome_ids =
                source_store.get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id)?;
            for id in outcome_ids.iter() {
                if let Some(existing_outcome) =
                    source_store.get_outcome_by_id_and_block_hash(id, block_hash)?
                {
                    chain_store_update
                        .chain_store_cache_update
                        .outcomes
                        .insert((*id, *block_hash), existing_outcome);
                }
            }
            chain_store_update
                .chain_store_cache_update
                .outcome_ids
                .insert((*block_hash, shard_id), outcome_ids);
        }
        chain_store_update
            .chain_store_cache_update
            .height_to_hashes
            .insert(height, Some(*block_hash));
        chain_store_update
            .chain_store_cache_update
            .next_block_hashes
            .insert(*header.prev_hash(), *block_hash);
        let block_merkle_tree = source_store.get_block_merkle_tree(block_hash)?;
        chain_store_update
            .chain_store_cache_update
            .block_merkle_tree
            .insert(*block_hash, block_merkle_tree.clone());
        chain_store_update
            .chain_store_cache_update
            .block_ordinal_to_hash
            .insert(block_merkle_tree.size(), *block_hash);
        chain_store_update.chain_store_cache_update.processed_block_heights.insert(height);

        // other information not directly related to this block
        chain_store_update.chain_store_cache_update.height_to_hashes.insert(
            source_store.genesis_height,
            Some(source_store.get_block_hash_by_height(source_store.genesis_height)?),
        );
        Ok(chain_store_update)
    }

    #[tracing::instrument(level = "debug", target = "store", "ChainUpdate::finalize", skip_all)]
    fn finalize(&mut self) -> Result<StoreUpdate, Error> {
        let mut store_update = self.store().store_update();
        {
            let _span = tracing::trace_span!(target: "store", "write_col_misc").entered();
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
        }
        {
            let _span = tracing::trace_span!(target: "store", "write_block").entered();
            debug_assert!(self.chain_store_cache_update.blocks.len() <= 1);
            for (hash, block) in self.chain_store_cache_update.blocks.iter() {
                let mut map = HashMap::clone(
                    self.chain_store
                        .get_all_block_hashes_by_height(block.header().height())?
                        .as_ref(),
                );
                map.entry(*block.header().epoch_id())
                    .or_insert_with(|| HashSet::new())
                    .insert(*hash);
                store_update.set_ser(
                    DBCol::BlockPerHeight,
                    &index_to_bytes(block.header().height()),
                    &map,
                )?;
                self.chain_store_cache_update
                    .block_hash_per_height
                    .insert(block.header().height(), map);
                store_update.insert_ser(DBCol::Block, hash.as_ref(), block)?;
            }
            let mut header_hashes_by_height: HashMap<BlockHeight, HashSet<CryptoHash>> =
                HashMap::new();
            for (hash, header) in self.chain_store_cache_update.headers.iter() {
                if self.chain_store.get_block_header(hash).is_ok() {
                    // No need to add same Header once again
                    continue;
                }

                header_hashes_by_height
                    .entry(header.height())
                    .or_insert_with(|| {
                        self.chain_store
                            .get_all_header_hashes_by_height(header.height())
                            .unwrap_or_default()
                    })
                    .insert(*hash);
                store_update.insert_ser(DBCol::BlockHeader, hash.as_ref(), header)?;
            }
            for (height, hash_set) in header_hashes_by_height {
                store_update.set_ser(
                    DBCol::HeaderHashesByHeight,
                    &index_to_bytes(height),
                    &hash_set,
                )?;
            }
            for ((block_hash, shard_uid), chunk_extra) in
                self.chain_store_cache_update.chunk_extras.iter()
            {
                store_update.set_ser(
                    DBCol::ChunkExtra,
                    &get_block_shard_uid(block_hash, shard_uid),
                    chunk_extra,
                )?;
            }
            for (block_hash, block_extra) in self.chain_store_cache_update.block_extras.iter() {
                store_update.insert_ser(DBCol::BlockExtra, block_hash.as_ref(), block_extra)?;
            }
        }

        {
            let _span = tracing::trace_span!(target: "store", "write_chunk").entered();

            let mut chunk_hashes_by_height: HashMap<BlockHeight, HashSet<ChunkHash>> =
                HashMap::new();
            for (chunk_hash, chunk) in self.chain_store_cache_update.chunks.iter() {
                if self.chain_store.chunk_exists(chunk_hash)? {
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
                    let bytes = borsh::to_vec(&tx).expect("Borsh cannot fail");
                    store_update.increment_refcount(
                        DBCol::Transactions,
                        tx.get_hash().as_ref(),
                        &bytes,
                    );
                }

                // Increase receipt refcounts for all included receipts
                for receipt in chunk.prev_outgoing_receipts().iter() {
                    let bytes = borsh::to_vec(&receipt).expect("Borsh cannot fail");
                    store_update.increment_refcount(
                        DBCol::Receipts,
                        receipt.get_hash().as_ref(),
                        &bytes,
                    );
                }

                store_update.insert_ser(DBCol::Chunks, chunk_hash.as_ref(), chunk)?;
            }
            for (height, hash_set) in chunk_hashes_by_height {
                store_update.set_ser(
                    DBCol::ChunkHashesByHeight,
                    &index_to_bytes(height),
                    &hash_set,
                )?;
            }
            for (chunk_hash, partial_chunk) in self.chain_store_cache_update.partial_chunks.iter() {
                store_update.insert_ser(
                    DBCol::PartialChunks,
                    chunk_hash.as_ref(),
                    partial_chunk,
                )?;
            }
        }

        for (height, hash) in self.chain_store_cache_update.height_to_hashes.iter() {
            if let Some(hash) = hash {
                store_update.set_ser(DBCol::BlockHeight, &index_to_bytes(*height), hash)?;
            } else {
                store_update.delete(DBCol::BlockHeight, &index_to_bytes(*height));
            }
        }
        for (block_hash, next_hash) in self.chain_store_cache_update.next_block_hashes.iter() {
            store_update.set_ser(DBCol::NextBlockHashes, block_hash.as_ref(), next_hash)?;
        }
        for (epoch_hash, light_client_block) in
            self.chain_store_cache_update.epoch_light_client_blocks.iter()
        {
            store_update.set_ser(
                DBCol::EpochLightClientBlocks,
                epoch_hash.as_ref(),
                light_client_block,
            )?;
        }
        {
            let _span =
                tracing::trace_span!(target: "store", "write_incoming_and_outgoing_receipts")
                    .entered();

            for ((block_hash, shard_id), receipt) in
                self.chain_store_cache_update.outgoing_receipts.iter()
            {
                store_update.set_ser(
                    DBCol::OutgoingReceipts,
                    &get_block_shard_id(block_hash, *shard_id),
                    receipt,
                )?;
            }
            for ((block_hash, shard_id), receipt) in
                self.chain_store_cache_update.incoming_receipts.iter()
            {
                store_update.set_ser(
                    DBCol::IncomingReceipts,
                    &get_block_shard_id(block_hash, *shard_id),
                    receipt,
                )?;
            }
        }

        {
            let _span = tracing::trace_span!(target: "store", "write_outcomes").entered();

            for ((outcome_id, block_hash), outcome_with_proof) in
                self.chain_store_cache_update.outcomes.iter()
            {
                store_update.insert_ser(
                    DBCol::TransactionResultForBlock,
                    &get_outcome_id_block_hash(outcome_id, block_hash),
                    &outcome_with_proof,
                )?;
            }
            for ((block_hash, shard_id), ids) in self.chain_store_cache_update.outcome_ids.iter() {
                store_update.set_ser(
                    DBCol::OutcomeIds,
                    &get_block_shard_id(block_hash, *shard_id),
                    &ids,
                )?;
            }
        }

        for (block_hash, refcount) in self.chain_store_cache_update.block_refcounts.iter() {
            store_update.set_ser(DBCol::BlockRefCount, block_hash.as_ref(), refcount)?;
        }
        for (block_hash, block_merkle_tree) in
            self.chain_store_cache_update.block_merkle_tree.iter()
        {
            store_update.set_ser(DBCol::BlockMerkleTree, block_hash.as_ref(), block_merkle_tree)?;
        }
        for (block_ordinal, block_hash) in
            self.chain_store_cache_update.block_ordinal_to_hash.iter()
        {
            store_update.set_ser(
                DBCol::BlockOrdinal,
                &index_to_bytes(*block_ordinal),
                block_hash,
            )?;
        }

        // Convert trie changes to database ops for trie nodes.
        // Create separate store update for deletions, because we want to update cache and don't want to remove nodes
        // from the store.
        {
            let _span = tracing::trace_span!(target: "store", "write_trie_changes").entered();
            let mut deletions_store_update = self.store().trie_store().store_update();
            for mut wrapped_trie_changes in self.trie_changes.drain(..) {
                wrapped_trie_changes.apply_mem_changes();
                wrapped_trie_changes.insertions_into(&mut store_update.trie_store_update());
                wrapped_trie_changes.deletions_into(&mut deletions_store_update);
                wrapped_trie_changes.state_changes_into(&mut store_update.trie_store_update());

                if self.chain_store.save_trie_changes {
                    wrapped_trie_changes.trie_changes_into(&mut store_update.trie_store_update());
                }
            }

            for ((block_hash, shard_id), state_transition_data) in
                self.state_transition_data.drain()
            {
                store_update.set_ser(
                    DBCol::StateTransitionData,
                    &get_block_shard_id(&block_hash, shard_id),
                    &state_transition_data,
                )?;
            }
        }
        {
            let _span = tracing::trace_span!(target: "store", "write_catchup").entered();

            let mut affected_catchup_blocks = HashSet::new();
            for (prev_hash, hash) in self.remove_blocks_to_catchup.drain(..) {
                assert!(!affected_catchup_blocks.contains(&prev_hash));
                if affected_catchup_blocks.contains(&prev_hash) {
                    return Err(Error::Other(
                        "Multiple changes to the store affect the same catchup block".to_string(),
                    ));
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

                if !prev_table.is_empty() {
                    store_update.set_ser(
                        DBCol::BlocksToCatchup,
                        prev_hash.as_ref(),
                        &prev_table,
                    )?;
                } else {
                    store_update.delete(DBCol::BlocksToCatchup, prev_hash.as_ref());
                }
            }
            for prev_hash in self.remove_prev_blocks_to_catchup.drain(..) {
                assert!(!affected_catchup_blocks.contains(&prev_hash));
                if affected_catchup_blocks.contains(&prev_hash) {
                    return Err(Error::Other(
                        "Multiple changes to the store affect the same catchup block".to_string(),
                    ));
                }
                affected_catchup_blocks.insert(prev_hash);

                store_update.delete(DBCol::BlocksToCatchup, prev_hash.as_ref());
            }
            for (prev_hash, new_hash) in self.add_blocks_to_catchup.drain(..) {
                assert!(!affected_catchup_blocks.contains(&prev_hash));
                if affected_catchup_blocks.contains(&prev_hash) {
                    return Err(Error::Other(
                        "Multiple changes to the store affect the same catchup block".to_string(),
                    ));
                }
                affected_catchup_blocks.insert(prev_hash);

                let mut prev_table =
                    self.chain_store.get_blocks_to_catchup(&prev_hash).unwrap_or_else(|_| vec![]);
                prev_table.push(new_hash);
                store_update.set_ser(DBCol::BlocksToCatchup, prev_hash.as_ref(), &prev_table)?;
            }
        }

        for state_sync_info in self.add_state_sync_infos.drain(..) {
            store_update.set_ser(
                DBCol::StateDlInfos,
                state_sync_info.epoch_tail_hash.as_ref(),
                &state_sync_info,
            )?;
        }
        for hash in self.remove_state_sync_infos.drain(..) {
            store_update.delete(DBCol::StateDlInfos, hash.as_ref());
        }
        for hash in self.challenged_blocks.drain() {
            store_update.set_ser(DBCol::ChallengedBlocks, hash.as_ref(), &true)?;
        }
        for (chunk_hash, chunk) in self.chain_store_cache_update.invalid_chunks.iter() {
            store_update.insert_ser(DBCol::InvalidChunks, chunk_hash.as_ref(), chunk)?;
        }
        for block_height in self.chain_store_cache_update.processed_block_heights.iter() {
            store_update.set_ser(
                DBCol::ProcessedBlockHeights,
                &index_to_bytes(*block_height),
                &(),
            )?;
        }
        for other in self.store_updates.drain(..) {
            store_update.merge(other);
        }
        Ok(store_update)
    }

    #[tracing::instrument(level = "debug", target = "store", "ChainStoreUpdate::commit", skip_all)]
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
            height_to_hashes,
            next_block_hashes,
            epoch_light_client_blocks,
            outgoing_receipts,
            incoming_receipts,
            invalid_chunks,
            transactions,
            receipts,
            block_refcounts,
            block_merkle_tree,
            block_ordinal_to_hash,
            processed_block_heights,

            outcomes: _,
            outcome_ids: _,
        } = self.chain_store_cache_update;
        for (hash, block) in blocks {
            self.chain_store.blocks.put(hash.into(), block);
        }
        for (hash, header) in headers {
            self.chain_store.headers.put(hash.into(), header);
        }
        for (hash, block_extra) in block_extras {
            self.chain_store.block_extras.put(hash.into(), block_extra);
        }
        for ((block_hash, shard_uid), chunk_extra) in chunk_extras {
            let key = get_block_shard_uid(&block_hash, &shard_uid);
            self.chain_store.chunk_extras.put(key, chunk_extra);
        }
        for (hash, chunk) in chunks {
            self.chain_store.chunks.put(hash.into(), chunk);
        }
        for (hash, partial_chunk) in partial_chunks {
            self.chain_store.partial_chunks.put(hash.into(), partial_chunk);
        }
        for (height, epoch_id_to_hash) in block_hash_per_height {
            self.chain_store
                .block_hash_per_height
                .put(index_to_bytes(height).to_vec(), Arc::new(epoch_id_to_hash));
        }
        for (height, block_hash) in height_to_hashes {
            let bytes = index_to_bytes(height);
            if let Some(hash) = block_hash {
                self.chain_store.height.put(bytes.to_vec(), hash);
            } else {
                self.chain_store.height.pop(&bytes.to_vec());
            }
        }
        for (block_hash, next_hash) in next_block_hashes {
            self.chain_store.next_block_hashes.put(block_hash.into(), next_hash);
        }
        for (epoch_hash, light_client_block) in epoch_light_client_blocks {
            self.chain_store.epoch_light_client_blocks.put(epoch_hash.into(), light_client_block);
        }
        for ((block_hash, shard_id), shard_outgoing_receipts) in outgoing_receipts {
            let key = get_block_shard_id(&block_hash, shard_id);
            self.chain_store.outgoing_receipts.put(key, shard_outgoing_receipts);
        }
        for ((block_hash, shard_id), shard_incoming_receipts) in incoming_receipts {
            let key = get_block_shard_id(&block_hash, shard_id);
            self.chain_store.incoming_receipts.put(key, shard_incoming_receipts);
        }
        for (hash, invalid_chunk) in invalid_chunks {
            self.chain_store.invalid_chunks.put(hash.into(), invalid_chunk);
        }
        for (hash, transaction) in transactions {
            self.chain_store.transactions.put(hash.into(), transaction);
        }
        for (receipt_id, receipt) in receipts {
            self.chain_store.receipts.put(receipt_id.into(), receipt);
        }
        for (block_hash, refcount) in block_refcounts {
            self.chain_store.block_refcounts.put(block_hash.into(), refcount);
        }
        for (block_hash, merkle_tree) in block_merkle_tree {
            self.chain_store.block_merkle_tree.put(block_hash.into(), merkle_tree);
        }
        for (block_ordinal, block_hash) in block_ordinal_to_hash {
            self.chain_store
                .block_ordinal_to_hash
                .put(index_to_bytes(block_ordinal).to_vec(), block_hash);
        }
        for block_height in processed_block_heights {
            self.chain_store.processed_block_heights.put(index_to_bytes(block_height).to_vec(), ());
        }
        self.chain_store.head = self.head;
        self.chain_store.tail = self.tail;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use near_async::time::Clock;
    use std::sync::Arc;

    use crate::test_utils::get_chain;
    use near_primitives::errors::InvalidTxError;
    use near_primitives::hash::hash;
    use near_primitives::test_utils::create_test_signer;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::types::EpochId;
    use near_primitives::utils::index_to_bytes;

    #[test]
    fn test_tx_validity_long_fork() {
        let transaction_validity_period = 5;
        let mut chain = get_chain(Clock::real());
        let genesis = chain.get_block_by_height(0).unwrap();
        let signer = Arc::new(create_test_signer("test1"));
        let short_fork = [TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build()];
        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block_header(short_fork[0].header().clone()).unwrap();
        store_update.commit().unwrap();

        let short_fork_head = short_fork[0].header().clone();
        assert!(chain
            .mut_chain_store()
            .check_transaction_validity_period(
                &short_fork_head,
                genesis.hash(),
                transaction_validity_period
            )
            .is_ok());
        let mut long_fork = vec![];
        let mut prev_block = genesis;
        for i in 1..(transaction_validity_period + 3) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
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
            .mut_chain_store()
            .check_transaction_validity_period(
                cur_header,
                valid_base_hash,
                transaction_validity_period
            )
            .is_ok());
        let invalid_base_hash = long_fork[0].hash();
        assert_eq!(
            chain.mut_chain_store().check_transaction_validity_period(
                cur_header,
                invalid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_normal_case() {
        let transaction_validity_period = 5;
        let mut chain = get_chain(Clock::real());
        let genesis = chain.get_block_by_height(0).unwrap();
        let signer = Arc::new(create_test_signer("test1"));
        let mut blocks = vec![];
        let mut prev_block = genesis;
        for i in 1..(transaction_validity_period + 2) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
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
            .mut_chain_store()
            .check_transaction_validity_period(
                cur_header,
                valid_base_hash,
                transaction_validity_period
            )
            .is_ok());
        let new_block = TestBlockBuilder::new(Clock::real(), &blocks.last().unwrap(), signer)
            .height(transaction_validity_period + 3)
            .build();

        let mut store_update = chain.mut_chain_store().store_update();
        store_update.save_block_header(new_block.header().clone()).unwrap();
        store_update
            .update_height_if_not_challenged(new_block.header().height(), *new_block.hash())
            .unwrap();
        store_update.commit().unwrap();
        assert_eq!(
            chain.mut_chain_store().check_transaction_validity_period(
                new_block.header(),
                valid_base_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_tx_validity_off_by_one() {
        let transaction_validity_period = 5;
        let mut chain = get_chain(Clock::real());
        let genesis = chain.get_block_by_height(0).unwrap();
        let genesis_hash = *genesis.hash();
        let signer = Arc::new(create_test_signer("test1"));
        let mut short_fork = vec![];
        let mut prev_block = genesis.clone();
        for i in 1..(transaction_validity_period + 2) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            short_fork.push(block);
            store_update.commit().unwrap();
        }

        let short_fork_head = short_fork.last().unwrap().header().clone();
        assert_eq!(
            chain.mut_chain_store().check_transaction_validity_period(
                &short_fork_head,
                &genesis_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
        let mut long_fork = vec![];
        let mut prev_block = genesis;
        for i in 1..(transaction_validity_period * 5) {
            let mut store_update = chain.mut_chain_store().store_update();
            let block =
                TestBlockBuilder::new(Clock::real(), &prev_block, signer.clone()).height(i).build();
            prev_block = block.clone();
            store_update.save_block_header(block.header().clone()).unwrap();
            long_fork.push(block);
            store_update.commit().unwrap();
        }
        let long_fork_head = &long_fork.last().unwrap().header();
        assert_eq!(
            chain.mut_chain_store().check_transaction_validity_period(
                long_fork_head,
                &genesis_hash,
                transaction_validity_period
            ),
            Err(InvalidTxError::Expired)
        );
    }

    #[test]
    fn test_cache_invalidation() {
        let mut chain = get_chain(Clock::real());
        let genesis = chain.get_block_by_height(0).unwrap();
        let signer = Arc::new(create_test_signer("test1"));
        let block1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
        let mut block2 = block1.clone();
        block2.mut_header().set_epoch_id(EpochId(hash(&[1, 2, 3])));
        block2.mut_header().resign(&*signer);

        let mut store_update = chain.mut_chain_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[1])));
        store_update
            .chain_store_cache_update
            .blocks
            .insert(*block1.header().hash(), block1.clone());
        store_update.commit().unwrap();

        let block_hash = chain.mut_chain_store().height.get(&index_to_bytes(1).to_vec());
        let epoch_id_to_hash =
            chain.mut_chain_store().block_hash_per_height.get(&index_to_bytes(1).to_vec());

        let mut store_update = chain.mut_chain_store().store_update();
        store_update.chain_store_cache_update.height_to_hashes.insert(1, Some(hash(&[2])));
        store_update
            .chain_store_cache_update
            .blocks
            .insert(*block2.header().hash(), block2.clone());
        store_update.commit().unwrap();

        let block_hash1 = chain.mut_chain_store().height.get(&index_to_bytes(1).to_vec());
        let epoch_id_to_hash1 =
            chain.mut_chain_store().block_hash_per_height.get(&index_to_bytes(1).to_vec());

        assert_ne!(block_hash, block_hash1);
        assert_ne!(epoch_id_to_hash, epoch_id_to_hash1);
    }
}
