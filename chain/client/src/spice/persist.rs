use near_chain::Block;
use near_chain::types::{ApplyChunkResult, RuntimeAdapter};
use near_chain::update_shard::NewChunkResult;
use near_chain_primitives::Error;
use near_primitives::chunk_apply_stats::ChunkApplyStats;
use near_primitives::receipt::{ProcessedReceiptMetadata, ReceiptSource};
use near_store::Store;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};

/// SPICE-only per-shard persistence: commit one shard's apply outputs for `block`
/// in a single atomic `StoreUpdate`, replacing the monolithic
/// `ChainUpdate::apply_chunk_postprocessing` + `ChainStoreUpdate` path the
/// prototype used to hack-reuse. Mirrors the columns written by the `NewChunk`
/// arm of `process_apply_chunk_result` + `ChainStoreUpdate::finalize`.
///
/// Replay safety (findings.md #2): the `ChunkExtra` sentinel, the refcounted
/// `State` trie insertions, and the refcounted `Receipts` writes all land in the
/// SAME `StoreUpdate` and commit together. A crash leaves neither the sentinel
/// nor the rc writes; the caller's `chunk_extra_exists` guard then re-applies
/// cleanly. Splitting these across store updates would double-count refcounts.
///
/// Config-gated writes (`save_tx_outcomes` / `save_receipt_to_tx`) back RPC
/// features (#34) and are written only when enabled. `save_state_changes` is
/// always true for the per-shard executor, so state changes are unconditional.
/// SPICE never saves state-transition data (`should_save_state_transition_data`
/// is always false), so that column is intentionally omitted.
#[allow(clippy::too_many_arguments)]
pub(crate) fn commit_per_shard_outputs(
    store: &Store,
    runtime_adapter: &dyn RuntimeAdapter,
    block: &Block,
    result: NewChunkResult,
    save_trie_changes: bool,
    save_tx_outcomes: bool,
    save_receipt_to_tx: bool,
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

    let mut su = store.store_update();

    // Sentinel + refcounted writes share this one StoreUpdate (see doc comment).
    su.chunk_store_update().set_chunk_extra(block_hash, &shard_uid, &chunk_extra);

    // Flat-state delta. Reads `state_changes()` before `state_changes_into` drains
    // them below, so it must come first.
    let flat = runtime_adapter.get_flat_storage_manager().save_flat_state_changes(
        *block_hash,
        *prev_hash,
        height,
        shard_uid,
        trie_changes.state_changes(),
    )?;
    su.merge(flat.into());

    // Trie nodes: insertions (refcounted `State`) join the committed update;
    // deletions only update the in-memory cache via a separate, dropped update —
    // mirrors `ChainStoreUpdate::finalize` (we don't remove nodes from the store
    // here; GC does that).
    trie_changes.apply_mem_changes();
    trie_changes.insertions_into(&mut su.trie_store_update());
    let mut deletions = store.trie_store().store_update();
    trie_changes.deletions_into(&mut deletions);
    trie_changes.state_changes_into(block_hash, &mut su.trie_store_update());
    if save_trie_changes {
        trie_changes.trie_changes_into(block_hash, &mut su.trie_store_update());
    }

    su.chain_store_update().set_outgoing_receipt(block_hash, shard_id, &outgoing_receipts);

    // `ProcessedReceiptIds` metadata + refcounted `Receipts`. The metadata is built
    // here (rather than in the setter) so the `save_receipt_to_tx` GC entries stay
    // a call-site decision, matching `ChainStoreUpdate::save_processed_receipt_ids`.
    let mut metadata: Vec<ProcessedReceiptMetadata> = processed_receipts
        .iter()
        .map(|pr| ProcessedReceiptMetadata::new(*pr.receipt.receipt_id(), pr.source.clone()))
        .collect();
    if save_receipt_to_tx {
        for (id, _) in &receipt_to_tx {
            metadata.push(ProcessedReceiptMetadata::new(*id, ReceiptSource::ReceiptToTxGc));
        }
    }
    let receipts: Vec<_> = processed_receipts.into_iter().map(|pr| pr.receipt).collect();
    su.chain_store_update().set_processed_receipt_ids(block_hash, shard_id, &metadata, &receipts);

    if save_tx_outcomes {
        su.chain_store_update().set_outcomes_with_proofs(
            block_hash,
            shard_id,
            outcomes,
            outcome_paths,
        );
    }
    if save_receipt_to_tx {
        su.chain_store_update().set_receipt_to_tx(&receipt_to_tx);
    }
    su.chunk_store_update().set_chunk_apply_stats(
        block_hash,
        shard_id,
        &ChunkApplyStats::V1(stats),
    );

    su.commit();
    Ok(())
}
