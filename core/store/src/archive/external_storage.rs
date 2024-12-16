use super::ExternalStorage;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::{ReceiptProof, ShardChunk, ShardChunkHeader};
use near_primitives::state_sync::ShardStateSyncResponseHeader;
use near_primitives::transaction::{ExecutionOutcomeWithProof, Transaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockExtra, RawStateChangesWithTrieKey, ShardId, ShardIndex};
use near_primitives::{shard_layout::ShardLayout, types::BlockHeight};
use near_schema_checker_lib::ProtocolSchema;

use crate::archive::cold_storage::ColdMigrationStore;
use crate::flat::BlockInfo;
use crate::{metrics, DBCol, Store};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

type OutcomeId = CryptoHash;

/// Updates the archival storage for the block at the given height.
pub(crate) fn update_external_storage(
    storage: &dyn ExternalStorage,
    hot_store: &Store,
    shard_layout: &ShardLayout,
    height: &BlockHeight,
    _num_threads: usize,
) -> io::Result<bool> {
    let _span = tracing::debug_span!(target: "cold_store", "ArchivalStore::update_for_block", height = height);
    let _timer = metrics::COLD_COPY_DURATION.start_timer();

    if hot_store.get_for_cold(DBCol::BlockHeight, &height.to_le_bytes())?.is_none() {
        return Ok(false);
    }

    let block_data = BlockDataBuilder::new(hot_store.clone(), *height).build()?;

    let chunks = block_data.block().chunks();
    for shard_id in shard_layout.shard_ids() {
        let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
        let chunk_header = chunks
            .get(shard_index)
            .unwrap_or_else(|| panic!("Invalid shard index {}", shard_index));
        let shard_data =
            ShardDataBuilder::new(hot_store.clone(), shard_id, shard_index, chunk_header.clone())
                .build()?;
        save_shard_data(storage, *height, shard_id, shard_data)?;
    }
    save_block_data(storage, *height, block_data)?;

    Ok(true)
}

/// Saves the given block data to the external storage.
fn save_block_data(
    storage: &dyn ExternalStorage,
    height: BlockHeight,
    block_data: BlockData,
) -> io::Result<()> {
    let path = PathBuf::from(height.to_string()).join(Path::new("block"));
    let value = borsh::to_vec(&block_data)?;
    storage.put(path.as_path(), &value)
}

/// Saves the given shard data to the external storage.
fn save_shard_data(
    storage: &dyn ExternalStorage,
    height: BlockHeight,
    shard_id: ShardId,
    shard_data: ShardData,
) -> io::Result<()> {
    let path = PathBuf::from(height.to_string()).join(Path::new(&shard_id.to_string()));
    let value = borsh::to_vec(&shard_data)?;
    storage.put(path.as_path(), &value)
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
enum BlockData {
    V1(BlockDataV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
enum ShardData {
    V1(ShardDataV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
struct BlockDataV1 {
    /// Read from `DBCol::Block`.
    block: Block,
    /// Read from `DBCol::BlockExtra`.
    block_extra: BlockExtra,
    /// Read from `DBCol::BlockInfo`.
    block_info: BlockInfo,
    /// Read from `DBCol::NextBlockHashes`.
    next_block_hash: CryptoHash,
    /// Read from `DBCol::TransactionResultForBlock`.
    transaction_result_for_block: HashMap<OutcomeId, ExecutionOutcomeWithProof>,
    /// Read from `DBCol::StateShardUIdMapping`.
    state_shard_uid_mapping: HashMap<ShardUId, ShardUId>,
}

impl BlockData {
    fn block(&self) -> &Block {
        match self {
            BlockData::V1(data) => &data.block,
        }
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
struct ShardDataV1 {
    /// Read from `DBCol::Chunks`.
    chunk: Option<ShardChunk>,
    /// Read from `DBCol::ChunkExtra`.
    chunk_extra: ChunkExtra,
    /// Read from `DBCol::IncomingReceipts`.
    incoming_receipts: Vec<ReceiptProof>,
    /// Read from `DBCol::OutgoingReceipts`.
    outgoing_receipts: Vec<Receipt>,
    /// Read from `DBCol::OutcomeIds`.
    outcome_ids: Vec<OutcomeId>,
    /// Read from `DBCol::Receipts`.
    receipts: Vec<Receipt>,
    /// Read from `DBCol::StateChanges`.
    state_changes: HashMap<Vec<u8>, RawStateChangesWithTrieKey>,
    /// Read from `DBCol::StateHeaders`.
    state_headers: ShardStateSyncResponseHeader,
    /// Read from `DBCol::Transactions`.
    transactions: Vec<Transaction>,
}

struct BlockDataBuilder {
    _store: Store,
    _height: BlockHeight,

    block: Option<Block>,
    block_extra: Option<BlockExtra>,
    block_info: Option<BlockInfo>,
    next_block_hash: Option<CryptoHash>,
    transaction_result_for_block: Option<HashMap<OutcomeId, ExecutionOutcomeWithProof>>,
    state_shard_uid_mapping: Option<HashMap<ShardUId, ShardUId>>,
}

impl BlockDataBuilder {
    fn new(store: Store, height: BlockHeight) -> Self {
        Self {
            _store: store,
            _height: height,
            block: None,
            block_extra: None,
            block_info: None,
            next_block_hash: None,
            transaction_result_for_block: None,
            state_shard_uid_mapping: None,
        }
    }

    /// Builds the [`BlockData`] from the data fetched from the store.
    fn build(mut self) -> io::Result<BlockData> {
        self.fetch_data()?;
        let block_data = BlockData::V1(BlockDataV1 {
            block: self.block.take().expect("block is not set"),
            block_extra: self.block_extra.take().expect("block_extra is not set"),
            block_info: self.block_info.take().expect("block_info is not set"),
            next_block_hash: self.next_block_hash.take().expect("next_block_hash is not set"),
            transaction_result_for_block: self
                .transaction_result_for_block
                .take()
                .expect("transaction_result_for_block is not set"),
            state_shard_uid_mapping: self
                .state_shard_uid_mapping
                .take()
                .expect("state_shard_uid_mapping is not set"),
        });
        Ok(block_data)
    }

    fn fetch_data(&self) -> io::Result<()> {
        unimplemented!("TODO: Implement fetching data from hot_store to fill in the fields")
    }
}

struct ShardDataBuilder {
    _store: Store,
    _shard_id: ShardId,
    _shard_index: ShardIndex,
    _chunk_header: ShardChunkHeader,

    chunk: Option<ShardChunk>,
    chunk_extra: Option<ChunkExtra>,
    incoming_receipts: Option<Vec<ReceiptProof>>,
    outgoing_receipts: Option<Vec<Receipt>>,
    outcome_ids: Option<Vec<OutcomeId>>,
    receipts: Option<Vec<Receipt>>,
    state_changes: Option<HashMap<Vec<u8>, RawStateChangesWithTrieKey>>,
    state_headers: Option<ShardStateSyncResponseHeader>,
    transactions: Option<Vec<Transaction>>,
}

impl ShardDataBuilder {
    fn new(
        store: Store,
        shard_id: ShardId,
        shard_index: ShardIndex,
        chunk_header: ShardChunkHeader,
    ) -> Self {
        Self {
            _store: store,
            _shard_id: shard_id,
            _shard_index: shard_index,
            _chunk_header: chunk_header,
            chunk: None,
            chunk_extra: None,
            incoming_receipts: None,
            outgoing_receipts: None,
            outcome_ids: None,
            receipts: None,
            state_changes: None,
            state_headers: None,
            transactions: None,
        }
    }

    /// Builds the [`ShardData`] from the data fetched from the store.
    fn build(mut self) -> io::Result<ShardData> {
        self.fetch_data()?;
        let chunk_data = ShardData::V1(ShardDataV1 {
            chunk: self.chunk.take(),
            chunk_extra: self.chunk_extra.take().expect("chunk_extra is not set"),
            incoming_receipts: self.incoming_receipts.take().expect("incoming_receipts is not set"),
            outgoing_receipts: self.outgoing_receipts.take().expect("outgoing_receipts is not set"),
            outcome_ids: self.outcome_ids.take().expect("outcome_ids is not set"),
            receipts: self.receipts.take().expect("receipts is not set"),
            state_changes: self.state_changes.take().expect("state_changes is not set"),
            state_headers: self.state_headers.take().expect("state_headers is not set"),
            transactions: self.transactions.take().expect("transactions is not set"),
        });
        Ok(chunk_data)
    }

    fn fetch_data(&self) -> io::Result<()> {
        unimplemented!("TODO: Implement fetching data from hot_store to fill in the fields")
    }
}
