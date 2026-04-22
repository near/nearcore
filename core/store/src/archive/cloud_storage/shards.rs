use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::chunk_apply_stats::ChunkApplyStats;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::{ReceiptProof, ShardChunk};
// TODO(cloud_archival): Re-enable once `get_state_header()` is fixed (see below).
//use near_primitives::state_sync::ShardStateSyncResponseHeader;
use crate::adapter::StoreAdapter;
use crate::archive::cloud_storage::file_id::BatchRange;
use crate::{DBCol, KeyForStateChanges, Store};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, RawStateChangesWithTrieKey};
use near_schema_checker_lib::ProtocolSchema;

/// Versioned container for shard-related data stored in the cloud archive.
/// This is for a single block height (taken from the file path).
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ShardData {
    V1(ShardDataV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardDataV1 {
    /// Read from `DBCol::Chunks`.
    chunk: ShardChunk,
    /// Read from `chunk`, part of `DBCol::Transactions`.
    transactions: Vec<SignedTransaction>,
    /// Read from `chunk`, part of `DBCol::Receipts`.
    receipts: Vec<Receipt>,

    /// Read from `DBCol::OutcomeIds`.
    outcome_ids: Vec<CryptoHash>,
    /// Read from `DBCol::IncomingReceipts`.
    incoming_receipts: Vec<ReceiptProof>,
    /// Read from `DBCol::OutgoingReceipts`.
    outgoing_receipts: Vec<Receipt>,

    /// Read from `DBCol::ChunkExtra`.
    chunk_extra: ChunkExtra,
    /// Read from `DBCol::ChunkApplyStats`.
    chunk_apply_stats: ChunkApplyStats,
    /// Read from `DBCol::StateChanges`.
    state_changes: Vec<RawStateChangesWithTrieKey>,
    // TODO(cloud_archival): Re-enable once `get_state_header()` is fixed (see below).
    // /// Read from `DBCol::StateHeaders`.
    // state_headers: ShardStateSyncResponseHeader,
}

/// Builds a `ShardData` object for the given block height and shard ID by reading data from the store.
pub fn build_shard_data(
    store: &Store,
    genesis_height: BlockHeight,
    shard_layout: &ShardLayout,
    block_height: BlockHeight,
    shard_uid: ShardUId,
) -> Result<ShardData, Error> {
    let chain_store = store.chain_store();
    let chunk_store = store.chunk_store();
    let block_hash = chain_store.get_block_hash_by_height(block_height)?;
    let block = chain_store.get_block(&block_hash)?;
    let shard_id = shard_uid.shard_id();
    let chunk_hash = block
        .chunks()
        .iter_raw()
        .find(|shard_header| shard_header.shard_id() == shard_id)
        .map(|chunk_header| chunk_header.chunk_hash().clone());
    let Some(chunk_hash) = chunk_hash else {
        return Err(Error::Other(format!(
            "shard {shard_id} chunk not found in block at height {block_height}"
        )));
    };

    let chunk = chunk_store.get_chunk(&chunk_hash)?;
    let transactions = chunk.to_transactions().iter().cloned().collect();
    let receipts = chunk.prev_outgoing_receipts().iter().cloned().collect();

    let outcome_ids = chain_store.get_outcomes_by_block_hash_and_shard_id(&block_hash, shard_id);
    // TODO(cloud_archival): Check why this `if` is required and whether there's a cleaner approach.
    let incoming_receipts = if block_height > genesis_height + 1 {
        chain_store.get_incoming_receipts(&block_hash, shard_id)?.to_vec()
    } else {
        Vec::new()
    };
    // TODO(cloud_archival): Check why this `if` is required and whether there's a cleaner approach.
    let outgoing_receipts = if block_height > genesis_height + 1 {
        chain_store.get_outgoing_receipts(&block_hash, shard_id)?.to_vec()
    } else {
        Vec::new()
    };

    let chunk_extra = (*chunk_store.get_chunk_extra(&block_hash, &shard_uid)?).clone();
    let state_changes = get_state_changes(store, shard_layout, &block_hash, shard_uid)?;
    let chunk_apply_stats =
        chunk_store.get_chunk_apply_stats(&block_hash, &shard_id).ok_or_else(|| {
            Error::DBNotFoundErr(format!(
                "CHUNK APPLY STATS, block height: {}, shard ID: {:?}",
                block_height, shard_id
            ))
        })?;

    // TODO(cloud_archival) Investigate why get_state_header() is failing and fix it
    // let state_headers = store.get_state_header(shard_id, block_hash)?;

    let shard_data = ShardDataV1 {
        chunk,
        transactions,
        receipts,
        outcome_ids,
        incoming_receipts,
        outgoing_receipts,
        chunk_extra,
        state_changes,
        chunk_apply_stats,
        //state_headers,
    };
    Ok(ShardData::V1(shard_data))
}

// TODO(cloud_archival) Consider calling this function once per block height instead for each shard.
fn get_state_changes(
    store: &Store,
    shard_layout: &ShardLayout,
    block_hash: &CryptoHash,
    shard_uid: ShardUId,
) -> Result<Vec<RawStateChangesWithTrieKey>, Error> {
    let storage_key = KeyForStateChanges::for_block(&block_hash);
    let mut state_changes = vec![];
    for (key, changes) in store
        .iter_prefix_ser::<RawStateChangesWithTrieKey>(DBCol::StateChanges, storage_key.as_ref())
    {
        let decoded_shard_uid = if let Some(account_id) = changes.trie_key.get_account_id() {
            shard_layout.account_id_to_shard_uid(&account_id)
        } else {
            KeyForStateChanges::delayed_receipt_key_decode_shard_uid(
                &key,
                &block_hash,
                &changes.trie_key,
            )
            .map_err(|err| Error::Other(err.to_string()))?
        };
        if decoded_shard_uid != shard_uid {
            continue;
        }
        state_changes.push(changes);
    }
    Ok(state_changes)
}

impl ShardData {
    pub fn chunk(&self) -> &ShardChunk {
        match self {
            ShardData::V1(data) => &data.chunk,
        }
    }

    pub fn state_changes(&self) -> &[RawStateChangesWithTrieKey] {
        match self {
            ShardData::V1(data) => &data.state_changes,
        }
    }

    pub fn chunk_extra(&self) -> &ChunkExtra {
        match self {
            ShardData::V1(data) => &data.chunk_extra,
        }
    }
}

/// Versioned container for a batch of shard data spanning consecutive heights.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ShardBatch {
    V1(ShardBatchV1),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ShardBatchV1 {
    start_height: BlockHeight,
    end_height: BlockHeight,
    data: Vec<ShardData>,
}

/// Builds a `ShardBatch` by reading shard data for each height in `range`.
pub fn build_shard_batch(
    store: &Store,
    genesis_height: BlockHeight,
    shard_layout: &ShardLayout,
    range: &BatchRange,
    shard_uid: ShardUId,
) -> Result<ShardBatch, Error> {
    let count = (range.end() - range.start() + 1) as usize;
    let mut data = Vec::with_capacity(count);
    for height in range.start()..=range.end() {
        data.push(build_shard_data(store, genesis_height, shard_layout, height, shard_uid)?);
    }
    Ok(ShardBatch::new(range.start(), range.end(), data))
}

impl ShardBatch {
    /// Constructs a `ShardBatch`, asserting the length invariant.
    /// Use `validate_blob` for batches deserialized from cloud storage.
    pub fn new(start_height: BlockHeight, end_height: BlockHeight, data: Vec<ShardData>) -> Self {
        let batch = Self::V1(ShardBatchV1 { start_height, end_height, data });
        batch.validate_blob().expect("ShardBatch::new called with inconsistent data");
        batch
    }

    /// Validates the length invariant: `data.len() == end_height - start_height + 1`.
    /// Returns a human-readable reason on mismatch. Call after deserializing
    /// a blob from cloud storage.
    pub fn validate_blob(&self) -> Result<(), String> {
        let Self::V1(batch) = self;
        let expected = (batch.end_height - batch.start_height + 1) as usize;
        if batch.data.len() != expected {
            return Err(format!(
                "ShardBatch data.len() {} does not match range [{}, {}]",
                batch.data.len(),
                batch.start_height,
                batch.end_height,
            ));
        }
        Ok(())
    }

    pub fn start_height(&self) -> BlockHeight {
        let ShardBatch::V1(batch) = self;
        batch.start_height
    }

    pub fn end_height(&self) -> BlockHeight {
        let ShardBatch::V1(batch) = self;
        batch.end_height
    }

    /// Returns the shard data at `height` within this batch. `height` must
    /// be within the batch range — passing an out-of-range height is a
    /// programmer error and panics.
    pub fn get_shard_at_height(&self, height: BlockHeight) -> &ShardData {
        let ShardBatch::V1(batch) = self;
        assert!(
            height >= batch.start_height && height <= batch.end_height,
            "height {height} out of batch range [{}, {}]",
            batch.start_height,
            batch.end_height,
        );
        let index = (height - batch.start_height) as usize;
        &batch.data[index]
    }
}
