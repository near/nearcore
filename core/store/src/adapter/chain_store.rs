use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};
use crate::{
    CHUNK_TAIL_KEY, DBCol, FINAL_HEAD_KEY, FORK_TAIL_KEY, HEAD_KEY, HEADER_HEAD_KEY,
    LARGEST_TARGET_HEIGHT_KEY, Store, StoreUpdate, TAIL_KEY, get_genesis_height,
};
use near_chain_primitives::Error;
use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::chunk_apply_stats::ChunkApplyStats;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardUId, get_block_shard_uid};
use near_primitives::sharding::{
    ChunkHash, EncodedShardChunk, PartialEncodedChunk, ReceiptProof, ShardChunk,
};
use near_primitives::state_sync::{ShardStateSyncResponseHeader, StateHeaderKey};
use near_primitives::transaction::{ExecutionOutcomeWithProof, SignedTransaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, EpochId, NumBlocks, ShardId};
use near_primitives::utils::{get_block_shard_id, get_outcome_id_block_hash, index_to_bytes};
use near_primitives::views::LightClientBlockView;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;

#[derive(Clone)]
pub struct ChainStoreAdapter {
    store: Store,
    /// Genesis block height.
    genesis_height: BlockHeight,
}

impl StoreAdapter for ChainStoreAdapter {
    fn store_ref(&self) -> &Store {
        &self.store
    }
}

impl ChainStoreAdapter {
    pub fn new(store: Store) -> Self {
        let genesis_height = get_genesis_height(&store)
            .expect("Store failed on fetching genesis height")
            .expect("Genesis height not found in storage");
        Self { store, genesis_height }
    }

    pub fn store_update(&self) -> ChainStoreUpdateAdapter<'static> {
        ChainStoreUpdateAdapter {
            store_update: StoreUpdateHolder::Owned(self.store.store_update()),
        }
    }

    pub fn genesis_height(&self) -> BlockHeight {
        self.genesis_height
    }

    /// The chain head.
    pub fn head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(DBCol::BlockMisc, HEAD_KEY), "HEAD")
    }

    /// The chain Blocks Tail height.
    pub fn tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(DBCol::BlockMisc, TAIL_KEY)
            .map(|option| option.unwrap_or(self.genesis_height))
            .map_err(|e| e.into())
    }

    /// The chain Chunks Tail height.
    pub fn chunk_tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(DBCol::BlockMisc, CHUNK_TAIL_KEY)
            .map(|option| option.unwrap_or(self.genesis_height))
            .map_err(|e| e.into())
    }

    /// Tail height of the fork cleaning process.
    pub fn fork_tail(&self) -> Result<BlockHeight, Error> {
        self.store
            .get_ser(DBCol::BlockMisc, FORK_TAIL_KEY)
            .map(|option| option.unwrap_or(self.genesis_height))
            .map_err(|e| e.into())
    }

    /// Head of the header chain (not the same thing as head_header).
    pub fn header_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(DBCol::BlockMisc, HEADER_HEAD_KEY), "HEADER_HEAD")
    }

    /// Header of the block at the head of the block chain (not the same thing as header_head).
    pub fn head_header(&self) -> Result<BlockHeader, Error> {
        let last_block_hash = self.head()?.last_block_hash;
        option_to_not_found(
            self.store.get_ser(DBCol::BlockHeader, last_block_hash.as_ref()),
            format_args!("BLOCK HEADER: {}", last_block_hash),
        )
    }

    /// The chain final head. It is guaranteed to be monotonically increasing.
    pub fn final_head(&self) -> Result<Tip, Error> {
        option_to_not_found(self.store.get_ser(DBCol::BlockMisc, FINAL_HEAD_KEY), "FINAL HEAD")
    }

    /// Largest approval target height sent by us
    pub fn largest_target_height(&self) -> Result<BlockHeight, Error> {
        match self.store.get_ser(DBCol::BlockMisc, LARGEST_TARGET_HEIGHT_KEY) {
            Ok(Some(o)) => Ok(o),
            Ok(None) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }

    /// Get full block.
    pub fn get_block(&self, block_hash: &CryptoHash) -> Result<Block, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::Block, block_hash.as_ref()),
            format_args!("BLOCK: {}", block_hash),
        )
    }

    /// Returns a number of references for Block with `block_hash`
    pub fn get_block_refcount(&self, block_hash: &CryptoHash) -> Result<u64, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::BlockRefCount, block_hash.as_ref()),
            format_args!("BLOCK REFCOUNT: {}", block_hash),
        )
    }

    /// Does this full block exist?
    pub fn block_exists(&self, h: &CryptoHash) -> Result<bool, Error> {
        self.store.exists(DBCol::Block, h.as_ref()).map_err(|e| e.into())
    }

    /// Get block header.
    pub fn get_block_header(&self, h: &CryptoHash) -> Result<BlockHeader, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::BlockHeader, h.as_ref()),
            format_args!("BLOCK HEADER: {}", h),
        )
    }

    /// Get block height.
    pub fn get_block_height(&self, hash: &CryptoHash) -> Result<BlockHeight, Error> {
        if hash == &CryptoHash::default() {
            Ok(self.genesis_height)
        } else {
            Ok(self.get_block_header(hash)?.height())
        }
    }

    /// Get previous header.
    pub fn get_previous_header(&self, header: &BlockHeader) -> Result<BlockHeader, Error> {
        self.get_block_header(header.prev_hash())
    }

    /// Returns hash of the block on the main chain for given height.
    pub fn get_block_hash_by_height(&self, height: BlockHeight) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::BlockHeight, &index_to_bytes(height)),
            format_args!("BLOCK HEIGHT: {}", height),
        )
    }

    /// Returns a hashmap of epoch id -> set of all blocks got for current (height, epoch_id)
    pub fn get_all_block_hashes_by_height(
        &self,
        height: BlockHeight,
    ) -> Result<Arc<HashMap<EpochId, HashSet<CryptoHash>>>, Error> {
        Ok(self.store.get_ser(DBCol::BlockPerHeight, &index_to_bytes(height))?.unwrap_or_default())
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

    /// Returns block header from the current chain for given height if present.
    pub fn get_block_header_by_height(&self, height: BlockHeight) -> Result<BlockHeader, Error> {
        let hash = self.get_block_hash_by_height(height)?;
        self.get_block_header(&hash)
    }

    pub fn get_next_block_hash(&self, hash: &CryptoHash) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::NextBlockHashes, hash.as_ref()),
            format_args!("NEXT BLOCK HASH: {}", hash),
        )
    }

    /// Get full chunk.
    pub fn get_chunk(&self, chunk_hash: &ChunkHash) -> Result<ShardChunk, Error> {
        match self.store.get_ser(DBCol::Chunks, chunk_hash.as_ref()) {
            Ok(Some(shard_chunk)) => Ok(shard_chunk),
            _ => Err(Error::ChunkMissing(chunk_hash.clone())),
        }
    }

    /// Get partial chunk.
    pub fn get_partial_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Arc<PartialEncodedChunk>, Error> {
        match self.store.get_ser(DBCol::PartialChunks, chunk_hash.as_ref()) {
            Ok(Some(shard_chunk)) => Ok(shard_chunk),
            _ => Err(Error::ChunkMissing(chunk_hash.clone())),
        }
    }

    /// Does this chunk exist?
    pub fn chunk_exists(&self, h: &ChunkHash) -> Result<bool, Error> {
        self.store.exists(DBCol::Chunks, h.as_ref()).map_err(|e| e.into())
    }

    /// Returns encoded chunk if it's invalid otherwise None.
    pub fn is_invalid_chunk(
        &self,
        chunk_hash: &ChunkHash,
    ) -> Result<Option<Arc<EncodedShardChunk>>, Error> {
        self.store.get_ser(DBCol::InvalidChunks, chunk_hash.as_ref()).map_err(|err| err.into())
    }

    /// Information from applying chunk.
    pub fn get_chunk_extra(
        &self,
        block_hash: &CryptoHash,
        shard_uid: &ShardUId,
    ) -> Result<Arc<ChunkExtra>, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::ChunkExtra, &get_block_shard_uid(block_hash, shard_uid)),
            format_args!("CHUNK EXTRA: {}:{:?}", block_hash, shard_uid),
        )
    }

    pub fn get_chunk_apply_stats(
        &self,
        block_hash: &CryptoHash,
        shard_id: &ShardId,
    ) -> Result<Option<ChunkApplyStats>, Error> {
        self.store
            .get_ser(DBCol::ChunkApplyStats, &get_block_shard_id(block_hash, *shard_id))
            .map_err(|e| e.into())
    }

    pub fn get_outgoing_receipts(
        &self,
        prev_block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<Receipt>>, Error> {
        option_to_not_found(
            self.store
                .get_ser(DBCol::OutgoingReceipts, &get_block_shard_id(prev_block_hash, shard_id)),
            format_args!("OUTGOING RECEIPT: {} {}", prev_block_hash, shard_id),
        )
    }

    pub fn get_incoming_receipts(
        &self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<Arc<Vec<ReceiptProof>>, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::IncomingReceipts, &get_block_shard_id(block_hash, shard_id)),
            format_args!("INCOMING RECEIPT: {} {}", block_hash, shard_id),
        )
    }

    pub fn get_blocks_to_catchup(&self, prev_hash: &CryptoHash) -> Result<Vec<CryptoHash>, Error> {
        Ok(self.store.get_ser(DBCol::BlocksToCatchup, prev_hash.as_ref())?.unwrap_or_default())
    }

    pub fn get_transaction(
        &self,
        tx_hash: &CryptoHash,
    ) -> Result<Option<Arc<SignedTransaction>>, Error> {
        self.store.get_ser(DBCol::Transactions, tx_hash.as_ref()).map_err(|e| e.into())
    }

    /// Fetch a receipt by id, if it is stored in the store.
    ///
    /// Note that not _all_ receipts are persisted. Some receipts are ephemeral,
    /// get processed immediately after creation and don't even get to the
    /// database.
    pub fn get_receipt(&self, receipt_id: &CryptoHash) -> Result<Option<Arc<Receipt>>, Error> {
        self.store.get_ser(DBCol::Receipts, receipt_id.as_ref()).map_err(|e| e.into())
    }

    pub fn get_block_merkle_tree(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<PartialMerkleTree, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::BlockMerkleTree, block_hash.as_ref()),
            format_args!("BLOCK MERKLE TREE: {}", block_hash),
        )
    }

    pub fn get_block_hash_from_ordinal(
        &self,
        block_ordinal: NumBlocks,
    ) -> Result<CryptoHash, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::BlockOrdinal, &index_to_bytes(block_ordinal)),
            format_args!("BLOCK ORDINAL: {}", block_ordinal),
        )
    }

    pub fn get_block_merkle_tree_from_ordinal(
        &self,
        block_ordinal: NumBlocks,
    ) -> Result<PartialMerkleTree, Error> {
        let block_hash = self.get_block_hash_from_ordinal(block_ordinal)?;
        self.get_block_merkle_tree(&block_hash)
    }

    pub fn get_epoch_light_client_block(
        &self,
        hash: &CryptoHash,
    ) -> Result<Arc<LightClientBlockView>, Error> {
        option_to_not_found(
            self.store.get_ser(DBCol::EpochLightClientBlocks, hash.as_ref()),
            format_args!("EPOCH LIGHT CLIENT BLOCK: {}", hash),
        )
    }

    pub fn is_height_processed(&self, height: BlockHeight) -> Result<bool, Error> {
        self.store
            .get(DBCol::ProcessedBlockHeights, &index_to_bytes(height))
            .map(|r| r.is_some())
            .map_err(|e| e.into())
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

    pub fn get_current_epoch_sync_hash(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Option<CryptoHash>, Error> {
        Ok(self.store.get_ser(DBCol::StateSyncHashes, epoch_id.as_ref())?)
    }

    /// Get height of genesis
    pub fn get_genesis_height(&self) -> BlockHeight {
        self.genesis_height
    }
}

pub struct ChainStoreUpdateAdapter<'a> {
    store_update: StoreUpdateHolder<'a>,
}

impl Into<StoreUpdate> for ChainStoreUpdateAdapter<'static> {
    fn into(self) -> StoreUpdate {
        self.store_update.into()
    }
}

impl ChainStoreUpdateAdapter<'static> {
    pub fn commit(self) -> io::Result<()> {
        let store_update: StoreUpdate = self.into();
        store_update.commit()
    }
}

impl<'a> StoreUpdateAdapter for ChainStoreUpdateAdapter<'a> {
    fn store_update(&mut self) -> &mut StoreUpdate {
        &mut self.store_update
    }
}

impl<'a> ChainStoreUpdateAdapter<'a> {
    pub fn new(store_update: &'a mut StoreUpdate) -> Self {
        Self { store_update: StoreUpdateHolder::Reference(store_update) }
    }

    /// Note: Typically while saving the block header we would also like to update
    /// block_header_hashes_by_height and update block_merkle_tree
    /// This is a primitive function and changing only the BlockHeader column can lead to inconsistencies
    pub fn set_block_header_only(&mut self, header: &BlockHeader) {
        self.store_update.insert_ser(DBCol::BlockHeader, header.hash().as_ref(), header).unwrap();
    }

    /// Note: Typically block_header_hashes_by_height is saved while saving the block header
    /// This is a primitive function and changing only the HeaderHashesByHeight column can lead to inconsistencies
    /// Use with update_block_header_hashes_by_height
    pub fn set_block_header_hashes_by_height(
        &mut self,
        height: BlockHeight,
        hash_set: &HashSet<CryptoHash>,
    ) {
        self.store_update
            .set_ser(DBCol::HeaderHashesByHeight, &index_to_bytes(height), hash_set)
            .unwrap();
    }

    /// Note: Typically block_merkle_tree is saved while saving the block header
    /// This is a primitive function and changing only the BlockMerkleTree column can lead to inconsistencies
    pub fn set_block_merkle_tree(
        &mut self,
        block_hash: &CryptoHash,
        block_merkle_tree: &PartialMerkleTree,
    ) {
        self.store_update
            .set_ser(DBCol::BlockMerkleTree, block_hash.as_ref(), block_merkle_tree)
            .unwrap();
    }

    pub fn set_block_ordinal(&mut self, block_ordinal: NumBlocks, block_hash: &CryptoHash) {
        self.store_update
            .set_ser(DBCol::BlockOrdinal, &index_to_bytes(block_ordinal), block_hash)
            .unwrap();
    }

    pub fn set_block_height(&mut self, hash: &CryptoHash, height: BlockHeight) {
        self.store_update
            .set_ser(DBCol::BlockHeight, &borsh::to_vec(&height).unwrap(), hash)
            .unwrap();
    }

    pub fn set_header_head(&mut self, header_head: &Tip) {
        self.store_update.set_ser(DBCol::BlockMisc, HEADER_HEAD_KEY, header_head).unwrap();
    }

    pub fn set_final_head(&mut self, final_head: &Tip) {
        self.store_update.set_ser(DBCol::BlockMisc, FINAL_HEAD_KEY, final_head).unwrap();
    }

    /// This function is normally clubbed with set_block_header_only
    /// This is a primitive function and changing only the HeaderHashesByHeight column can lead to inconsistencies
    pub fn update_block_header_hashes_by_height(&mut self, header: &BlockHeader) {
        let height = header.height();
        let mut hash_set =
            self.store_update.store.chain_store().get_all_header_hashes_by_height(height).unwrap();
        hash_set.insert(*header.hash());
        self.set_block_header_hashes_by_height(height, &hash_set);
    }
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
