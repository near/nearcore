use crate::Block;
use crate::spice::core::get_execution_result_from_store;
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
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::{BlockExecutionResults, BlockHeight, ShardId};
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::trie_store::TrieStoreUpdateAdapter;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{StoreUpdate, WrappedTrieChanges};
use std::collections::BTreeMap;

/// Column-gating flags read by `apply_chunk_postprocessing`. Lives in the
/// chain crate so both the spice executor and the chain-side apply path
/// share it.
#[derive(Clone, Debug)]
pub struct ChunkPersistenceConfig {
    pub save_trie_changes: bool,
    pub save_tx_outcomes: bool,
    pub save_receipt_to_tx: bool,
    pub save_state_changes: bool,
}

impl Default for ChunkPersistenceConfig {
    fn default() -> Self {
        Self {
            save_trie_changes: true,
            save_tx_outcomes: true,
            save_receipt_to_tx: true,
            save_state_changes: true,
        }
    }
}

/// Per-shard apply persistence for one chunk. Writes into the caller-provided
/// `store_update`; the caller decides the commit lifecycle. Spice commits per
/// shard (per-shard atomicity); chain commits per block via
/// `ChainStoreUpdate::finalize` (per-block atomicity).
///
/// Replay safety: the `ChunkExtra` write (which marks this shard's apply as
/// done), the refcounted `State` trie insertions, and the refcounted `Receipts`
/// writes must all land in the SAME `StoreUpdate`. A crash then leaves neither
/// the `ChunkExtra` nor the rc writes; the caller's `chunk_extra_exists` guard
/// re-applies cleanly. Splitting across updates would double-count refcounts
/// on replay.
pub fn apply_chunk_postprocessing(
    store_update: &mut StoreUpdate,
    runtime_adapter: &dyn RuntimeAdapter,
    block: &Block,
    result: NewChunkResult,
    config: &ChunkPersistenceConfig,
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

    // `ChunkExtra` marks this shard's apply as done; must share `store_update` with the refcounted writes below.
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

    // Deletions go into a SEPARATE `StoreUpdate` that is intentionally DROPPED
    // without commit — only the in-memory cache invalidation survives. Built
    // here so the lifecycle is visible at the call site. See `write_trie_changes`.
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

/// Compute the flat-state delta and merge it into `store_update`. Reads
/// `trie_changes.state_changes()`, a borrow that `state_changes_into` drains —
/// so this must run before `write_trie_changes`.
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

/// Apply this chunk's trie changes. Insertions and refcount-incrementing writes
/// go into the shared `StoreUpdate`; deletions go into a separate `StoreUpdate`
/// that the caller drops without committing. Optional state-changes and
/// trie-changes columns are gated by `config`.
///
/// `deletions_into` has two effects: write to disk AND invalidate the in-memory
/// trie cache. We want the cache invalidation (stale reads otherwise) but NOT
/// the disk write — GC removes nodes from disk on its own schedule, after every
/// reference is gone. Committing deletions here would remove still-referenced
/// nodes prematurely. Dropping the deletion-only update keeps only the cache
/// invalidation side effect.
fn write_trie_changes(
    trie_store_update: &mut TrieStoreUpdateAdapter,
    deletions_store_update: &mut TrieStoreUpdateAdapter,
    block_hash: &CryptoHash,
    trie_changes: &mut WrappedTrieChanges,
    config: &ChunkPersistenceConfig,
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

/// Build `ProcessedReceiptIds` metadata and the companion refcounted `Receipts`
/// writes. Consumes `processed_receipts`; borrows `receipt_to_tx`. Metadata
/// derivation stays here (not in the adapter setter) so the `ReceiptToTxGc`
/// markers remain a call-site decision.
fn write_processed_receipts(
    store_update: &mut StoreUpdate,
    block_hash: &CryptoHash,
    shard_id: ShardId,
    processed_receipts: Vec<ProcessedReceipt>,
    receipt_to_tx: &[(CryptoHash, ReceiptToTxInfo)],
    config: &ChunkPersistenceConfig,
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

/// Per-shard congestion info for transaction admission, from `block_header`'s
/// executed `ChunkExtra`s (typically the last certified block). Each shard is
/// read via [`spice_shard_congestion_info`], so it works without tracking every
/// shard; shards neither tracked nor yet certified are omitted.
pub fn spice_block_congestion_info(
    chain_store: &ChainStoreAdapter,
    epoch_manager: &dyn EpochManagerAdapter,
    block_header: &BlockHeader,
) -> Result<BlockCongestionInfo, Error> {
    let shard_layout = epoch_manager.get_shard_layout(block_header.epoch_id())?;
    let mut result = BTreeMap::new();
    for shard_id in shard_layout.shard_ids() {
        if let Some(extended) =
            spice_shard_congestion_info(chain_store, &shard_layout, block_header, shard_id)
        {
            result.insert(shard_id, extended);
        }
    }
    Ok(BlockCongestionInfo::new(result))
}

/// Congestion info for a single shard from `block_header`'s executed `ChunkExtra`,
/// for transaction admission. Prefers the locally executed `ChunkExtra`, falling
/// back to the chain-wide certified result in `DBCol::execution_results` (like
/// `SpiceCoreReader::get_trusted_chunk_extra`), so it works without tracking the
/// shard. Returns `None` when the shard is neither tracked nor yet certified.
/// `missed_chunks_count` is 0 (see `build_block_congestion_info`). Prefer this
/// over [`spice_block_congestion_info`] when only one shard's entry is needed.
pub fn spice_shard_congestion_info(
    chain_store: &ChainStoreAdapter,
    shard_layout: &ShardLayout,
    block_header: &BlockHeader,
    shard_id: ShardId,
) -> Option<ExtendedCongestionInfo> {
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, shard_layout);
    let chunk_store = chain_store.chunk_store();
    let congestion_info =
        if let Ok(chunk_extra) = chunk_store.get_chunk_extra(block_header.hash(), &shard_uid) {
            chunk_extra.congestion_info()
        } else {
            get_execution_result_from_store(chain_store, block_header.hash(), shard_id)?
                .chunk_extra
                .congestion_info()
        };
    Some(ExtendedCongestionInfo::new(congestion_info, 0))
}

fn build_block_bandwidth_requests(
    block_header: &BlockHeader,
    prev_block_execution_results: &BlockExecutionResults,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<BlockBandwidthRequests, Error> {
    let shard_layout = epoch_manager.get_shard_layout(&block_header.epoch_id())?;
    let prev_block_hash = block_header.prev_hash();
    let mut result = BTreeMap::new();
    // Keyed by this block's shards (consumed when applying its chunks); the
    // prev-block mapping only locates the source execution result, keyed by the
    // prev block's shard layout.
    // TODO(spice-resharding): across a resharding boundary both children map to the
    // same parent and inherit its requests unsplit. See dynamic_resharding.md.
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
    // Keyed by this block's shards (consumed when applying its chunks); the
    // prev-block mapping only locates the source execution result, keyed by the
    // prev block's shard layout.
    // TODO(spice-resharding): across a resharding boundary both children map to the
    // same parent and inherit its congestion info unsplit. See dynamic_resharding.md.
    for shard_id in shard_layout.shard_ids() {
        let (_, prev_block_shard_id, _) =
            epoch_manager.get_prev_shard_id_from_prev_hash(prev_block_hash, shard_id)?;
        let prev_shard_execution_result = prev_block_execution_results
            .0
            .get(&prev_block_shard_id)
            .expect("block execution result should contain execution results for all shards");
        let congestion_info = prev_shard_execution_result.chunk_extra.congestion_info();
        // Always 0: missing chunks apply as empty new chunks, so shards never
        // stall (see chunk_executor_actor.rs); no missed-chunk backpressure.
        let missed_chunks_count = 0;
        result.insert(shard_id, ExtendedCongestionInfo::new(congestion_info, missed_chunks_count));
    }
    Ok(BlockCongestionInfo::new(result))
}
