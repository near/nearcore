use crate::Block;
use crate::types::{ApplyChunkBlockContext, ApplyChunkResult, RuntimeAdapter};
use crate::update_shard::NewChunkResult;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::block::BlockHeader;
use near_primitives::chunk_apply_stats::ChunkApplyStats;
use near_primitives::congestion_info::{BlockCongestionInfo, ExtendedCongestionInfo};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ProcessedReceipt, ProcessedReceiptMetadata, ReceiptSource, ReceiptToTxInfo,
};
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{BlockExecutionResults, BlockHeight, ShardId};
use near_store::adapter::trie_store::TrieStoreUpdateAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{StoreUpdate, WrappedTrieChanges};
use std::collections::BTreeMap;

/// Per-shard apply persistence config. Holds the column-gating flags read
/// at the per-shard apply persistence call site. Lives in the chain crate
/// so both the spice executor (client crate) and the chain-side apply path
/// (chain crate) can share it.
#[derive(Clone, Debug)]
pub struct ChunkExecutorConfig {
    pub save_trie_changes: bool,
    pub save_tx_outcomes: bool,
    pub save_receipt_to_tx: bool,
    pub save_state_changes: bool,
}

impl Default for ChunkExecutorConfig {
    fn default() -> Self {
        Self {
            save_trie_changes: true,
            save_tx_outcomes: true,
            save_receipt_to_tx: true,
            save_state_changes: true,
        }
    }
}

/// Per-shard persistence for one chunk's apply outputs. Writes into the
/// caller-provided `store_update`; the caller decides commit lifecycle.
///
/// Spice calls this per shard with a fresh `StoreUpdate` and commits per
/// shard (per-shard atomicity). Chain calls it via the `ChainStoreUpdate`
/// bridge for each shard and lets `ChainStoreUpdate::finalize` commit at
/// the end of the block (per-block atomicity).
///
/// Replay safety: the `ChunkExtra` sentinel, the refcounted `State` trie
/// insertions, and the refcounted `Receipts` writes all land in the SAME
/// `StoreUpdate` and commit together. A crash leaves neither the sentinel
/// nor the rc writes; the caller's `chunk_extra_exists` guard then
/// re-applies cleanly. Splitting these across store updates would
/// double-count refcounts.
pub fn apply_chunk_postprocessing(
    store_update: &mut StoreUpdate,
    runtime_adapter: &dyn RuntimeAdapter,
    block: &Block,
    result: NewChunkResult,
    config: &ChunkExecutorConfig,
) -> Result<(), Error> {
    let block_hash = block.hash();
    let prev_hash = block.header().prev_hash();
    let height = block.header().height();
    let NewChunkResult { gas_limit, shard_uid, apply_result } = result;
    let shard_id = shard_uid.shard_id();

    let (_, outcome_paths) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    let chunk_extra = apply_result.to_chunk_extra(gas_limit);

    let ApplyChunkResult {
        mut trie_changes,
        outcomes,
        outgoing_receipts,
        processed_receipts,
        receipt_to_tx,
        stats,
        ..
    } = apply_result;

    // Sentinel write — must be in `store_update` with the refcounted writes below.
    store_update.chunk_store_update().set_chunk_extra(block_hash, &shard_uid, &chunk_extra);

    write_flat_state_delta(
        store_update,
        runtime_adapter,
        block_hash,
        prev_hash,
        height,
        shard_uid,
        &trie_changes,
    )?;

    // Trie deletions go into a SEPARATE `StoreUpdate` that is intentionally
    // DROPPED without commit at the end of this function — see the doc on
    // `write_trie_changes`. Built outside the helper so the dropped-without-
    // commit lifecycle is visible at this level.
    let mut deletions_store_update = runtime_adapter.store().trie_store().store_update();
    write_trie_changes(
        &mut store_update.trie_store_update(),
        &mut deletions_store_update,
        block_hash,
        &mut trie_changes,
        config,
    );

    store_update.chain_store_update().set_outgoing_receipt(
        block_hash,
        shard_id,
        &outgoing_receipts,
    );
    write_processed_receipts(
        store_update,
        block_hash,
        shard_id,
        processed_receipts,
        &receipt_to_tx,
        config,
    );

    if config.save_tx_outcomes {
        store_update.chain_store_update().set_outcomes_with_proofs(
            block_hash,
            shard_id,
            outcomes,
            outcome_paths,
        );
    }
    if config.save_receipt_to_tx {
        store_update.chain_store_update().set_receipt_to_tx(&receipt_to_tx);
    }
    store_update.chunk_store_update().set_chunk_apply_stats(
        block_hash,
        shard_id,
        &ChunkApplyStats::V1(stats),
    );
    Ok(())
}

/// Compute the flat-state delta for this shard's apply and merge it into
/// `store_update`. Reads `trie_changes.state_changes()`, which is a borrow
/// that becomes invalid once `state_changes_into` drains them — so this
/// must run before [`write_trie_changes`].
fn write_flat_state_delta(
    store_update: &mut StoreUpdate,
    runtime_adapter: &dyn RuntimeAdapter,
    block_hash: &CryptoHash,
    prev_hash: &CryptoHash,
    height: BlockHeight,
    shard_uid: ShardUId,
    trie_changes: &WrappedTrieChanges,
) -> Result<(), Error> {
    let flat = runtime_adapter.get_flat_storage_manager().save_flat_state_changes(
        *block_hash,
        *prev_hash,
        height,
        shard_uid,
        trie_changes.state_changes(),
    )?;
    store_update.merge(flat.into());
    Ok(())
}

/// Mirrors `ChainStoreUpdate::finalize`'s trie loop.
///
/// **Why `deletions_store_update` is a SEPARATE update.** `deletions_into`
/// writes both to disk and to the in-memory trie cache. We *do* want to
/// invalidate the cache for the deleted nodes (so future reads don't return
/// stale data via the cache), but we *do not* want to remove the nodes from
/// disk here — GC removes nodes from disk on its own schedule, after every
/// reference is gone. Writing deletions to the same `StoreUpdate` as the
/// insertions and committing it would remove the nodes from disk
/// prematurely (and potentially under-refcount entries that other blocks
/// still reference). The deletion-only `TrieStoreUpdateAdapter` is
/// constructed at the call site and **dropped without commit** when it
/// leaves scope — only the cache-update side effect of `deletions_into`
/// survives, which is exactly what we want.
fn write_trie_changes(
    trie_store_update: &mut TrieStoreUpdateAdapter,
    deletions_store_update: &mut TrieStoreUpdateAdapter,
    block_hash: &CryptoHash,
    trie_changes: &mut WrappedTrieChanges,
    config: &ChunkExecutorConfig,
) {
    trie_changes.apply_mem_changes();
    trie_changes.insertions_into(trie_store_update);
    trie_changes.deletions_into(deletions_store_update);
    if config.save_state_changes {
        trie_changes.state_changes_into(block_hash, trie_store_update);
    }
    if config.save_trie_changes {
        trie_changes.trie_changes_into(block_hash, trie_store_update);
    }
}

/// Build the `ProcessedReceiptIds` metadata + companion refcounted `Receipts`
/// writes. Consumes `processed_receipts` (the refcounted writes follow);
/// borrows `receipt_to_tx`. Metadata derivation lives here (not in the
/// adapter setter) so the `ReceiptToTxGc` markers stay a call-site decision.
fn write_processed_receipts(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    processed_receipts: Vec<ProcessedReceipt>,
    receipt_to_tx: &[(CryptoHash, ReceiptToTxInfo)],
    config: &ChunkExecutorConfig,
) {
    let mut metadata: Vec<ProcessedReceiptMetadata> = processed_receipts
        .iter()
        .map(|pr| ProcessedReceiptMetadata::new(*pr.receipt.receipt_id(), pr.source.clone()))
        .collect();
    if config.save_receipt_to_tx {
        for (id, _) in receipt_to_tx {
            metadata.push(ProcessedReceiptMetadata::new(*id, ReceiptSource::ReceiptToTxGc));
        }
    }
    let receipts: Vec<_> = processed_receipts.into_iter().map(|pr| pr.receipt).collect();
    store_update
        .chain_store_update()
        .set_processed_receipt_ids(block_hash, shard_id, &metadata, &receipts);
}

/// Maps `DBNotFoundErr` to `None` and any other error through. Lets a read
/// that may be absent compose with `?`.
pub fn optional<T>(res: Result<T, Error>) -> Result<Option<T>, Error> {
    match res {
        Ok(value) => Ok(Some(value)),
        Err(Error::DBNotFoundErr(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

pub fn build_spice_apply_chunk_block_context(
    block_header: &BlockHeader,
    prev_block_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<ApplyChunkBlockContext, Error> {
    // TODO(spice): gas price should be based on execution results and not part of the
    // block since it's calculated based on gas usage during execution.
    let gas_price = block_header.next_gas_price();
    let congestion_info =
        build_block_congestion_info(block_header, prev_block_execution_results, epoch_manager)?;
    let bandwidth_requests =
        build_block_bandwidth_requests(block_header, prev_block_execution_results, epoch_manager)?;
    Ok(ApplyChunkBlockContext::from_header(
        block_header,
        gas_price,
        congestion_info,
        bandwidth_requests,
    ))
}

fn build_block_bandwidth_requests(
    block_header: &BlockHeader,
    prev_block_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<BlockBandwidthRequests, Error> {
    let shard_layout = epoch_manager.get_shard_layout(&block_header.epoch_id())?;
    let prev_block_hash = block_header.prev_hash();
    let mut result = BTreeMap::new();
    // TODO(spice-resharding): double-check if shards for block or prev_block should be
    // used as keys.
    for shard_id in shard_layout.shard_ids() {
        let (_, prev_block_shard_id, _) =
            epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
        let prev_shard_execution_result = prev_block_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("block execution result should contain execution results for all shards");
        if let Some(bandwidth_requests) =
            prev_shard_execution_result.chunk_extra.bandwidth_requests()
        {
            result.insert(shard_id, bandwidth_requests.clone());
        }
    }
    Ok(BlockBandwidthRequests { shards_bandwidth_requests: result })
}

fn build_block_congestion_info(
    block_header: &BlockHeader,
    prev_block_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<BlockCongestionInfo, Error> {
    let shard_layout = epoch_manager.get_shard_layout(&block_header.epoch_id())?;
    let prev_block_hash = block_header.prev_hash();
    let mut result = BTreeMap::new();
    // TODO(spice-resharding): double-check if shards for block or prev_block should be
    // used as keys.
    for shard_id in shard_layout.shard_ids() {
        let (_, prev_block_shard_id, _) =
            epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
        let prev_shard_execution_result = prev_block_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("block execution result should contain execution results for all shards");
        let congestion_info = prev_shard_execution_result.chunk_extra.congestion_info();
        let missed_chunks_count = 0;
        result.insert(shard_id, ExtendedCongestionInfo::new(congestion_info, missed_chunks_count));
    }
    Ok(BlockCongestionInfo::new(result))
}
