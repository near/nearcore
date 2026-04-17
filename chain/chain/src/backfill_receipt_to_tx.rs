use crate::{ChainStore, ChainStoreAccess, Error};
use anyhow::Context;
use near_o11y::tracing;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction, ReceiptToTxInfo,
    ReceiptToTxInfoV1,
};
use near_primitives::types::BlockHeight;
use near_primitives::utils::get_block_shard_id_rev;
use near_store::{DBCol, NodeStorage, Store};
use rayon::prelude::*;

/// Routes the three distinct storage roles used by the ReceiptToTx backfill:
/// reads of chain state, writes of ReceiptToTx entries, and checkpoint state.
///
/// Constructed via [`BackfillStorage::for_node`] so the wiring for split-storage
/// archival nodes is decided in a single place — both the CLI and the background
/// actor share it, preventing the two call sites from drifting apart.
pub struct BackfillStorage {
    /// Chain reads (blocks, receipts, outcomes). On split-storage archival nodes
    /// this is `split_store` so fall-through to cold works for GC'd hot data.
    pub read_store: Store,
    /// Where ReceiptToTx entries are written. On split-storage archival nodes
    /// this is `cold_store` directly — the hot→cold copy pipeline does not
    /// cover backfill writes.
    pub write_store: Store,
    /// Where the backfill checkpoint is stored. Always `hot_store`: the
    /// checkpoint is transient operational state, not archival data.
    pub checkpoint_store: Store,
}

impl BackfillStorage {
    /// Wire the three roles correctly for a given [`NodeStorage`].
    ///
    /// - `read_store`: `split_store` if present, else `hot_store`.
    /// - `write_store`: `cold_store` if present, else `hot_store`.
    /// - `checkpoint_store`: always `hot_store` (checkpoints are transient).
    ///
    /// IMPORTANT: on split-storage archival nodes, ReceiptToTx entries written
    /// by this backfill land only in cold. The RPC read path (`GetReceiptToTx`)
    /// works because the view_runtime is constructed from `split_store` in
    /// `nearcore/src/lib.rs` (`view_runtime` = `NightshadeRuntime::from_config(.., split_store, ..)`),
    /// and split_store reads hot-first with a fall-through to cold. If that
    /// view_runtime wiring ever changes to use `hot_store` directly, backfilled
    /// entries become invisible to RPC queries. Keep the view_runtime ↔
    /// [`BackfillStorage`] invariant aligned.
    pub fn for_node(storage: &NodeStorage) -> Self {
        let hot = storage.get_hot_store();
        Self {
            read_store: storage.get_split_store().unwrap_or_else(|| hot.clone()),
            write_store: storage.get_cold_store().unwrap_or_else(|| hot.clone()),
            checkpoint_store: hot,
        }
    }
}

/// Checkpoint key for the forward-direction CLI backfill tool.
/// The CLI processes heights ascending (genesis -> head) and stores the highest completed height.
///
/// Since ReceiptToTx is an insert-only column, duplicate writes for overlapping heights are
/// safe (idempotent). The CLI is intended for one-shot bulk backfill with rayon parallelism.
pub const BACKFILL_CHECKPOINT_KEY: &[u8] = b"BACKFILL_RECEIPT_TO_TX";

/// Stats returned by backfill processing.
#[derive(Default, Clone, Copy)]
pub struct BackfillStats {
    pub blocks_processed: u64,
    pub entries_written: u64,
    pub heights_skipped: u64,
    /// Execution outcomes referenced by `OutcomeIds` but not readable via the chain store.
    /// Non-zero usually indicates partial GC or data corruption — the backfill can't produce
    /// a `ReceiptToTxInfo` entry for the affected receipts.
    pub missing_outcomes: u64,
    /// Child receipts referenced by an outcome's `receipt_ids` but missing from the store.
    pub missing_child_receipts: u64,
    /// Parent receipts (for receipt-origin outcomes) missing from the store.
    pub missing_parent_receipts: u64,
}

impl BackfillStats {
    /// Total "dropped" data points: receipts we could not backfill because something upstream
    /// was missing. Used as the denominator in the data-quality threshold check.
    pub fn missing_total(&self) -> u64 {
        self.missing_outcomes + self.missing_child_receipts + self.missing_parent_receipts
    }

    /// Fold another set of stats into this one.
    pub fn add(&mut self, other: &BackfillStats) {
        self.blocks_processed += other.blocks_processed;
        self.entries_written += other.entries_written;
        self.heights_skipped += other.heights_skipped;
        self.missing_outcomes += other.missing_outcomes;
        self.missing_child_receipts += other.missing_child_receipts;
        self.missing_parent_receipts += other.missing_parent_receipts;
    }
}

/// Result of processing a single height.
///
/// `None` is only returned by [`process_height`] when there is no block at the height
/// (the checkpoint/skipped case). When there IS a block, the result is always
/// `Some(HeightResult)` — even if `entries` is empty — so missing-entity counters
/// are not lost.
pub struct HeightResult {
    pub entries: Vec<(CryptoHash, ReceiptToTxInfo)>,
    pub missing_outcomes: u64,
    pub missing_child_receipts: u64,
    pub missing_parent_receipts: u64,
}

/// Process a single height: read all execution outcomes and build ReceiptToTxInfo entries.
///
/// Returns `Ok(None)` only when there is no block at `height` (counted as a skipped height).
/// Returns `Ok(Some(_))` whenever the height has a block — the `HeightResult` carries the
/// entries produced and the per-category counts of data that was expected but missing.
pub fn process_height(
    chain_store: &ChainStore,
    height: BlockHeight,
) -> anyhow::Result<Option<HeightResult>> {
    let block_hash = match chain_store.get_block_hash_by_height(height) {
        Ok(hash) => hash,
        Err(Error::DBNotFoundErr(_)) => return Ok(None),
        Err(e) => {
            return Err(e).context(format!("failed to get block hash at height {height}"));
        }
    };

    let read_store = chain_store.store();
    let mut entries = Vec::new();
    let mut missing_outcomes: u64 = 0;
    let mut missing_child_receipts: u64 = 0;
    let mut missing_parent_receipts: u64 = 0;

    for (key, outcome_ids) in
        read_store.iter_prefix_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, block_hash.as_ref())
    {
        let (_, shard_id) = get_block_shard_id_rev(&key).expect("invalid OutcomeIds key");

        for outcome_id in outcome_ids {
            let outcome_with_proof =
                match chain_store.get_outcome_by_id_and_block_hash(&outcome_id, &block_hash) {
                    Some(o) => o,
                    None => {
                        missing_outcomes += 1;
                        tracing::warn!(
                            %outcome_id,
                            height,
                            %shard_id,
                            "missing execution outcome, skipping"
                        );
                        continue;
                    }
                };
            let outcome = &outcome_with_proof.outcome;

            if outcome.receipt_ids.is_empty() {
                continue;
            }

            let is_tx = read_store.exists(DBCol::Transactions, outcome_id.as_ref());

            // For receipt outcomes, look up the parent receipt once (not per child).
            let parent_receipt = if !is_tx { chain_store.get_receipt(&outcome_id) } else { None };

            for child_receipt_id in &outcome.receipt_ids {
                let child_receipt = match chain_store.get_receipt(child_receipt_id) {
                    Some(r) => r,
                    None => {
                        missing_child_receipts += 1;
                        tracing::debug!(
                            %child_receipt_id,
                            %outcome_id,
                            height,
                            "missing child receipt, skipping"
                        );
                        continue;
                    }
                };

                let info = if is_tx {
                    ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                        origin: ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                            tx_hash: outcome_id,
                            sender_account_id: outcome.executor_id.clone(),
                        }),
                        receiver_account_id: child_receipt.receiver_id().clone(),
                        shard_id,
                    })
                } else {
                    let parent = match &parent_receipt {
                        Some(r) => r,
                        None => {
                            missing_parent_receipts += 1;
                            tracing::debug!(
                                %outcome_id,
                                height,
                                "missing parent receipt for receipt outcome, skipping"
                            );
                            continue;
                        }
                    };
                    ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                        origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                            parent_receipt_id: outcome_id,
                            parent_predecessor_id: parent.predecessor_id().clone(),
                        }),
                        receiver_account_id: child_receipt.receiver_id().clone(),
                        shard_id,
                    })
                };

                entries.push((*child_receipt_id, info));
            }
        }
    }

    Ok(Some(HeightResult {
        entries,
        missing_outcomes,
        missing_child_receipts,
        missing_parent_receipts,
    }))
}

/// Process one batch of heights end-to-end: parallel reads, sequential writes, checkpoint.
///
/// Used by the CLI with ascending `heights` and checkpoint key [`BACKFILL_CHECKPOINT_KEY`].
/// The caller controls iteration direction by the order of `heights` — `par_iter` preserves
/// input order, so writes happen in the same order the caller supplied.
///
/// Semantics:
/// 1. Reads for all `heights` run in parallel on `pool`.
/// 2. Results are applied to `storage.write_store` in input order (single `store_update`).
/// 3. If `checkpoint` is `Some((key, height))`, that entry is written to
///    `storage.checkpoint_store` *after* entries are committed. The two commits are
///    non-atomic; a crash between them replays safely because `DBCol::ReceiptToTx` is
///    insert-only. Pass `None` to skip the checkpoint write (e.g. one-shot CLI reruns).
pub fn process_one_batch(
    chain_store: &ChainStore,
    storage: &BackfillStorage,
    pool: &rayon::ThreadPool,
    heights: &[BlockHeight],
    checkpoint: Option<(&[u8], BlockHeight)>,
) -> anyhow::Result<BackfillStats> {
    let mut stats = BackfillStats::default();

    let results: Vec<_> =
        pool.install(|| heights.par_iter().map(|h| process_height(chain_store, *h)).collect());

    let mut entry_update = storage.write_store.store_update();
    for result in results {
        match result {
            Ok(Some(height_result)) => {
                for (receipt_id, info) in height_result.entries {
                    entry_update.insert_ser(DBCol::ReceiptToTx, receipt_id.as_ref(), &info);
                    stats.entries_written += 1;
                }
                stats.blocks_processed += 1;
                stats.missing_outcomes += height_result.missing_outcomes;
                stats.missing_child_receipts += height_result.missing_child_receipts;
                stats.missing_parent_receipts += height_result.missing_parent_receipts;
            }
            Ok(None) => {
                stats.heights_skipped += 1;
            }
            Err(e) => return Err(e),
        }
    }
    entry_update.commit();

    if let Some((key, height)) = checkpoint {
        let mut cp_update = storage.checkpoint_store.store_update();
        cp_update.set_ser(DBCol::Misc, key, &height);
        cp_update.commit();
    }

    Ok(stats)
}
