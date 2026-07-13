use crate::adapter::StoreAdapter;
use crate::adapter::chain_store::option_to_not_found;
use crate::archive::cloud_storage::batch::BatchRange;
use crate::flat::FlatStorageManager;
use crate::trie::AccessOptions;
use crate::{DBCol, KeyForStateChanges, ShardTries, StateSnapshotConfig, Store, TrieConfig};
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain_primitives::Error;
use near_primitives::chunk_apply_stats::ChunkApplyStats;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ProcessedReceiptMetadata, Receipt, ReceiptSource, ReceiptToTxInfo};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::{ReceiptProof, ShardChunk};
use near_primitives::transaction::ExecutionOutcomeWithProof;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::ShardId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{BlockHeight, RawStateChangesWithTrieKey};
use near_primitives::utils::get_block_shard_id;
use near_schema_checker_lib::ProtocolSchema;
use std::collections::BTreeMap;

/// Earlier value of each key changed in one block. `None` = key did not exist.
/// A `BTreeMap` keeps entries ordered by key, so the serialized blob bytes are
/// deterministic across writers.
pub type InverseStateChanges = BTreeMap<TrieKey, Option<Vec<u8>>>;

/// Versioned container for shard-related data stored in the cloud archive.
/// This is for a single block height (taken from the file path).
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ShardData {
    V1(ShardDataV1),
}

// Short-lived deserialized blob held only in small bounded collections;
// the size disparity is transient, not worth a heap indirection per read.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ShardDataV1 {
    NewChunk(NewChunkData),
    Carried(CarriedData),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct NewChunkData {
    /// Hash of the containing block.
    block_hash: CryptoHash,
    /// Read from `DBCol::Chunks`.
    chunk: ShardChunk,
    /// Read from `DBCol::IncomingReceipts`. `None` when no new chunk in
    /// the block produces a receipt targeting this shard.
    incoming_receipts: Option<Vec<ReceiptProof>>,
    /// Read from `DBCol::OutgoingReceipts`.
    outgoing_receipts: Vec<Receipt>,
    /// Read from `DBCol::ChunkExtra`.
    chunk_extra: ChunkExtra,
    /// Read from `DBCol::ChunkApplyStats`.
    chunk_apply_stats: ChunkApplyStats,
    /// Read from `DBCol::StateChanges`.
    state_changes: Vec<RawStateChangesWithTrieKey>,
    /// Read from `DBCol::OutcomeIds` and `DBCol::TransactionResultForBlock`.
    transaction_result_for_block: Vec<(CryptoHash, ExecutionOutcomeWithProof)>,
    /// Read from `DBCol::ProcessedReceiptIds` (entries tagged
    /// `ReceiptSource::ReceiptToTxGc`) and `DBCol::ReceiptToTx`.
    receipt_to_tx: Vec<(CryptoHash, ReceiptToTxInfo)>,
    /// Earlier value of each key in `state_changes`. `None` if not computed.
    inverse_state_changes: Option<InverseStateChanges>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct CarriedData {
    /// Hash of the containing block.
    block_hash: CryptoHash,
    /// Read from `DBCol::ChunkExtra`.
    chunk_extra: ChunkExtra,
    /// Read from `DBCol::ChunkApplyStats`.
    chunk_apply_stats: ChunkApplyStats,
    /// Read from `DBCol::StateChanges`.
    state_changes: Vec<RawStateChangesWithTrieKey>,
    /// Read from `DBCol::IncomingReceipts`. `None` when no new chunk in
    /// the block produces a receipt targeting this shard.
    incoming_receipts: Option<Vec<ReceiptProof>>,
    /// Earlier value of each key in `state_changes`. `None` if not computed.
    inverse_state_changes: Option<InverseStateChanges>,
}

/// Context for building a shard's inverse state changes: a disk-backed
/// `ShardTries` for pre-image lookups and the gap ceiling at or below which
/// blocks carry inverse changes.
struct InverseDeltasContext {
    tries: ShardTries,
    ceiling: BlockHeight,
}

/// `Ok(None)` at skipped heights (no block). Attaches inverse state changes when
/// `inverse_deltas_context` is set and the block is within the resharding gap.
fn build_shard_data(
    store: &Store,
    shard_layout: &ShardLayout,
    block_height: BlockHeight,
    shard_uid: ShardUId,
    inverse_deltas_context: Option<&InverseDeltasContext>,
) -> Result<Option<ShardData>, Error> {
    let chain_store = store.chain_store();
    let chunk_store = store.chunk_store();
    let block_hash = match chain_store.get_block_hash_by_height(block_height) {
        Ok(hash) => hash,
        Err(Error::DBNotFoundErr(_)) => return Ok(None),
        Err(other) => return Err(other),
    };
    let block = chain_store.get_block(&block_hash)?;
    let shard_id = shard_uid.shard_id();
    let chunk_header =
        block.chunks().iter_raw().find(|shard_header| shard_header.shard_id() == shard_id).cloned();
    let Some(chunk_header) = chunk_header else {
        return Err(Error::Other(format!(
            "shard {shard_id} chunk not found in block at height {block_height}"
        )));
    };

    let chunk_extra = (*chunk_store.get_chunk_extra(&block_hash, &shard_uid)?).clone();
    let chunk_apply_stats = option_to_not_found(
        chunk_store.get_chunk_apply_stats(&block_hash, &shard_id),
        format_args!("CHUNK APPLY STATS: height {block_height}, shard {shard_id:?}"),
    )?;
    let state_changes = get_state_changes(store, shard_layout, &block_hash, shard_uid)?;
    let inverse_state_changes = build_inverse_state_changes(
        store,
        inverse_deltas_context,
        shard_uid,
        &block_hash,
        block_height,
        &state_changes,
    )?;
    // `IncomingReceipts` is only written at `(block, shard)` when at least
    // one new chunk in `block` produces a receipt targeting `shard`;
    // tolerate absence.
    // TODO(cloud_archival): regression test for an all-chunks-missing block.
    let incoming_receipts = match chain_store.get_incoming_receipts(&block_hash, shard_id) {
        Ok(r) => Some(r.to_vec()),
        Err(Error::DBNotFoundErr(_)) => None,
        Err(e) => return Err(e),
    };

    if !chunk_header.is_new_chunk(block_height) {
        return Ok(Some(ShardData::V1(ShardDataV1::Carried(CarriedData {
            block_hash,
            chunk_extra,
            chunk_apply_stats,
            state_changes,
            incoming_receipts,
            inverse_state_changes,
        }))));
    }

    let chunk = chunk_store.get_chunk(chunk_header.chunk_hash())?;
    let outgoing_receipts = chain_store.get_outgoing_receipts(&block_hash, shard_id)?.to_vec();
    let transaction_result_for_block =
        build_transaction_result_for_block(store, &block_hash, shard_id)?;
    let receipt_to_tx = build_receipt_to_tx(store, &block_hash, shard_id)?;

    Ok(Some(ShardData::V1(ShardDataV1::NewChunk(NewChunkData {
        block_hash,
        chunk,
        incoming_receipts,
        outgoing_receipts,
        chunk_extra,
        chunk_apply_stats,
        state_changes,
        transaction_result_for_block,
        receipt_to_tx,
        inverse_state_changes,
    }))))
}

fn build_transaction_result_for_block(
    store: &Store,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<Vec<(CryptoHash, ExecutionOutcomeWithProof)>, Error> {
    let chain_store = store.chain_store();
    let outcome_ids = chain_store.get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id);
    let mut transaction_result_for_block = Vec::with_capacity(outcome_ids.len());
    for outcome_id in outcome_ids {
        let outcome = option_to_not_found(
            chain_store.get_outcome_by_id_and_block_hash(&outcome_id, block_hash),
            format_args!(
                "TRANSACTION RESULT FOR BLOCK: outcome_id {outcome_id}, block_hash {block_hash}"
            ),
        )?;
        transaction_result_for_block.push((outcome_id, outcome));
    }
    // Sort so blob bytes are deterministic regardless of chunk-apply enumeration order.
    transaction_result_for_block.sort_by_key(|(id, _)| *id);
    Ok(transaction_result_for_block)
}

fn build_receipt_to_tx(
    store: &Store,
    block_hash: &CryptoHash,
    shard_id: ShardId,
) -> Result<Vec<(CryptoHash, ReceiptToTxInfo)>, Error> {
    let chain_store = store.chain_store();
    let processed_receipt_ids: Vec<ProcessedReceiptMetadata> = store
        .get_ser(DBCol::ProcessedReceiptIds, &get_block_shard_id(block_hash, shard_id))
        .unwrap_or_default();
    let mut receipt_to_tx = Vec::new();
    for metadata in &processed_receipt_ids {
        if !matches!(metadata.source(), ReceiptSource::ReceiptToTxGc) {
            continue;
        }
        let receipt_id = *metadata.receipt_id();
        let info = option_to_not_found(
            chain_store.get_receipt_to_tx(&receipt_id),
            format_args!("RECEIPT TO TX: receipt_id {receipt_id}"),
        )?;
        receipt_to_tx.push((receipt_id, info));
    }
    // Sort so blob bytes are deterministic regardless of chunk-apply enumeration order.
    receipt_to_tx.sort_by_key(|(id, _)| *id);
    Ok(receipt_to_tx)
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

/// The reverse of the block's `forward` state changes for `shard_uid`, or `None`
/// when there is no inverse context or the block is above the gap ceiling. Each
/// changed key maps to its value at the previous block's post-state root (`None`
/// if the key was absent then). Keyed by `TrieKey` in a `BTreeMap`, so every
/// writer emits the same blob.
fn build_inverse_state_changes(
    store: &Store,
    inverse_deltas_context: Option<&InverseDeltasContext>,
    shard_uid: ShardUId,
    block_hash: &CryptoHash,
    block_height: BlockHeight,
    forward: &[RawStateChangesWithTrieKey],
) -> Result<Option<InverseStateChanges>, Error> {
    let Some(context) = inverse_deltas_context else {
        return Ok(None);
    };
    if block_height > context.ceiling {
        return Ok(None);
    }
    // No changes to reverse.
    if forward.is_empty() {
        return Ok(Some(InverseStateChanges::new()));
    }
    let header = store.chain_store().get_block_header(block_hash)?;
    let prev_chunk_extra = store.chunk_store().get_chunk_extra(header.prev_hash(), &shard_uid)?;
    let prev_state_root = *prev_chunk_extra.state_root();
    let trie = context.tries.get_trie_for_shard(shard_uid, prev_state_root);
    let mut inverse = InverseStateChanges::new();
    for change in forward {
        let prev_value = trie
            .get(&change.trie_key.to_vec(), AccessOptions::DEFAULT)
            .map_err(|err| Error::Other(format!("inverse lookup failed: {err}")))?;
        inverse.insert(change.trie_key.clone(), prev_value);
    }
    Ok(Some(inverse))
}

impl ShardData {
    pub fn block_hash(&self) -> &CryptoHash {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => &d.block_hash,
            ShardData::V1(ShardDataV1::Carried(d)) => &d.block_hash,
        }
    }

    pub fn chunk(&self) -> Option<&ShardChunk> {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => Some(&d.chunk),
            ShardData::V1(ShardDataV1::Carried(_)) => None,
        }
    }

    pub fn state_changes(&self) -> &[RawStateChangesWithTrieKey] {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => &d.state_changes,
            ShardData::V1(ShardDataV1::Carried(d)) => &d.state_changes,
        }
    }

    pub fn chunk_extra(&self) -> &ChunkExtra {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => &d.chunk_extra,
            ShardData::V1(ShardDataV1::Carried(d)) => &d.chunk_extra,
        }
    }

    pub fn chunk_apply_stats(&self) -> &ChunkApplyStats {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => &d.chunk_apply_stats,
            ShardData::V1(ShardDataV1::Carried(d)) => &d.chunk_apply_stats,
        }
    }

    pub fn transaction_result_for_block(
        &self,
    ) -> Option<&[(CryptoHash, ExecutionOutcomeWithProof)]> {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => Some(&d.transaction_result_for_block),
            ShardData::V1(ShardDataV1::Carried(_)) => None,
        }
    }

    pub fn receipt_to_tx(&self) -> Option<&[(CryptoHash, ReceiptToTxInfo)]> {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => Some(&d.receipt_to_tx),
            ShardData::V1(ShardDataV1::Carried(_)) => None,
        }
    }

    pub fn outgoing_receipts(&self) -> Option<&[Receipt]> {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => Some(&d.outgoing_receipts),
            ShardData::V1(ShardDataV1::Carried(_)) => None,
        }
    }

    pub fn inverse_state_changes(&self) -> Option<&InverseStateChanges> {
        match self {
            ShardData::V1(ShardDataV1::NewChunk(d)) => d.inverse_state_changes.as_ref(),
            ShardData::V1(ShardDataV1::Carried(d)) => d.inverse_state_changes.as_ref(),
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
    /// One entry per height; `None` at skipped slots.
    data: Vec<Option<ShardData>>,
}

/// Builds a `ShardBatch` by reading shard data for each height in `range`.
/// Pushes `None` for skipped slots.
pub fn build_shard_batch(
    store: &Store,
    shard_layout: &ShardLayout,
    range: &BatchRange,
    shard_uid: ShardUId,
    inverse_ceiling: Option<BlockHeight>,
) -> Result<ShardBatch, Error> {
    // Inverse changes read pre-image values from disk; a fresh `ShardTries`
    // avoids the writer's memtries, which may not hold the child layout's roots.
    // Skipped when the whole range sits above the ceiling, so no block needs it.
    let inverse_deltas_context =
        inverse_ceiling.filter(|&ceiling| ceiling >= range.start()).map(|ceiling| {
            InverseDeltasContext {
                tries: ShardTries::new(
                    store.trie_store(),
                    TrieConfig::default(),
                    FlatStorageManager::new(store.flat_store()),
                    StateSnapshotConfig::Disabled,
                ),
                ceiling,
            }
        });
    let count = (range.end() - range.start() + 1) as usize;
    let mut data = Vec::with_capacity(count);
    for height in range.start()..=range.end() {
        data.push(build_shard_data(
            store,
            shard_layout,
            height,
            shard_uid,
            inverse_deltas_context.as_ref(),
        )?);
    }
    Ok(ShardBatch::new(range.start(), range.end(), data))
}

impl ShardBatch {
    /// Constructs a `ShardBatch`, asserting the length invariant.
    /// Use `validate_blob` for batches deserialized from cloud storage.
    pub fn new(
        start_height: BlockHeight,
        end_height: BlockHeight,
        data: Vec<Option<ShardData>>,
    ) -> Self {
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

    /// Returns the shard data at `height` within this batch, or `None` if
    /// the height is a skipped slot. `height` must be within the batch
    /// range - passing an out-of-range height is a programmer error and
    /// panics.
    pub fn get_data_at_height(&self, height: BlockHeight) -> Option<&ShardData> {
        let ShardBatch::V1(batch) = self;
        assert!(
            height >= batch.start_height && height <= batch.end_height,
            "height {height} out of batch range [{}, {}]",
            batch.start_height,
            batch.end_height,
        );
        let index = (height - batch.start_height) as usize;
        batch.data[index].as_ref()
    }
}
