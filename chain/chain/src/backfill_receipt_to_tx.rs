use crate::{ChainStore, ChainStoreAccess, Error};
use anyhow::Context;
use borsh::BorshDeserialize;
use near_o11y::tracing;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    Receipt, ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction, ReceiptToTxInfo,
    ReceiptToTxInfoV1,
};
use near_primitives::transaction::ExecutionOutcomeWithProof;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash};
use near_store::{DBCol, NodeStorage, Store};
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use thiserror::Error;

/// Errors emitted by [`process_one_batch`] that callers may want to inspect.
///
/// These are returned through `anyhow::Error` so they can be downcast at the
/// actor layer (e.g. to record dedicated metrics) while still flowing through
/// the existing `anyhow::Result` plumbing.
#[derive(Debug, Error)]
pub enum BackfillError {
    /// Two within-batch entries shared a receipt id but disagreed on the
    /// `ReceiptToTxInfo` payload. The batch is quarantined: the checkpoint is
    /// not advanced and no entries are written. The actor surfaces this as a
    /// metric increment plus an error-level log.
    #[error(
        "backfill produced two different ReceiptToTxInfo values for receipt_id {key} \
         (prev={prev_value:02x?}, new={new_value:02x?}); \
         this indicates non-determinism in chain data resolution"
    )]
    ValueDivergence { key: CryptoHash, prev_value: Vec<u8>, new_value: Vec<u8> },
}

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
    /// When `Some`, [`process_one_batch`] writes ReceiptToTx entries via
    /// RocksDB SST file ingestion under this directory; when `None`, it
    /// falls back to per-key `StoreUpdate.insert` and `commit()`.
    ///
    /// SST ingest avoids the compaction death spiral the legacy path triggers
    /// on archival cold storage (many small writes → memtable flush to L0 →
    /// L0 stack faster than compaction can merge → batch latency goes from
    /// seconds to many minutes). The per-key path is kept for callers whose
    /// underlying database doesn't implement `ingest_external_sst_files`
    /// (e.g. the in-memory test store) or that haven't opted in yet.
    ///
    /// The directory must live on the same filesystem as `write_store` so
    /// the underlying ingest can rename files into the DB instead of copying.
    /// Callers are responsible for choosing a path that no other writer
    /// (CLI or actor) will share, and for cleaning it up at startup.
    pub sst_temp_dir: Option<PathBuf>,
}

impl BackfillStorage {
    /// Wire the three roles correctly for a given [`NodeStorage`].
    ///
    /// - `read_store`: `split_store` if present, else `hot_store`.
    /// - `write_store`: `cold_store` if present, else `hot_store`.
    /// - `checkpoint_store`: always `hot_store` (checkpoints are transient).
    /// - `sst_temp_dir`: passed through verbatim. Path resolution lives at the
    ///   caller (mirroring the migration-time SST ingest path) so this
    ///   constructor stays purely about role wiring.
    ///
    /// IMPORTANT: on split-storage archival nodes, ReceiptToTx entries written
    /// by this backfill land only in cold. The RPC read path (`GetReceiptToTx`)
    /// works because the view_runtime is constructed from `split_store` in
    /// `nearcore/src/lib.rs` (`view_runtime` = `NightshadeRuntime::from_config(.., split_store, ..)`),
    /// and split_store reads hot-first with a fall-through to cold. If that
    /// view_runtime wiring ever changes to use `hot_store` directly, backfilled
    /// entries become invisible to RPC queries. Keep the view_runtime ↔
    /// [`BackfillStorage`] invariant aligned.
    pub fn for_node(storage: &NodeStorage, sst_temp_dir: Option<PathBuf>) -> Self {
        let hot = storage.get_hot_store();
        Self {
            read_store: storage.get_split_store().unwrap_or_else(|| hot.clone()),
            write_store: storage.get_cold_store().unwrap_or_else(|| hot.clone()),
            checkpoint_store: hot,
            sst_temp_dir,
        }
    }
}

/// Checkpoint key for the forward-direction CLI backfill tool.
/// The CLI processes heights ascending (genesis -> head) and stores the highest completed height.
///
/// Both the CLI and the background actor can run concurrently. Since ReceiptToTx is an
/// insert-only column, duplicate writes for overlapping heights are safe (idempotent).
/// The CLI is intended for one-shot bulk backfill with rayon parallelism, while the
/// actor handles steady-state background backfill in descending order.
pub const BACKFILL_CHECKPOINT_KEY: &[u8] = b"BACKFILL_RECEIPT_TO_TX";

/// Checkpoint key for the backward-direction background actor.
/// Stores the lowest completed height (everything from this height upward is done).
/// The actor processes heights descending (head -> genesis) so recent receipts are
/// queryable first.
pub const BACKFILL_CHECKPOINT_KEY_LOW: &[u8] = b"BACKFILL_RECEIPT_TO_TX_LOW";

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

/// One outcome's worth of state carried between the per-height MultiGet
/// stages.  Built from Stage 1's `TransactionResultForBlock` MultiGet
/// result; consumed by the assembly loop after Stage 2A/2B fill in receipts
/// and is_tx.
struct StagedOutcome {
    outcome_id: CryptoHash,
    shard_id: ShardId,
    outcome: ExecutionOutcomeWithProof,
}

/// Resolved primitives needed to assemble a `ReceiptToTxInfo`.
///
/// Both `process_height` (single-height MultiGet path) and the bucket-ETL
/// pipeline produce these by their own routes; the kernel
/// [`build_receipt_to_tx_info`] consumes them and produces byte-identical
/// output. Skipping (missing parent / child / outcome) is decided by the
/// caller — once a `BuiltOriginInputs` is constructed, an entry will be
/// produced.
pub struct BuiltOriginInputs {
    pub outcome_id: CryptoHash,
    pub shard_id: ShardId,
    pub child_receiver_id: AccountId,
    pub origin: BuiltOrigin,
}

pub enum BuiltOrigin {
    FromTransaction { sender_id: AccountId },
    FromReceipt { parent_predecessor_id: AccountId },
}

/// Assemble a `ReceiptToTxInfo` from already-resolved primitives.
///
/// This is the shared kernel between `process_height` and the bucket-ETL
/// pipeline. Producing the same `BuiltOriginInputs` from either path is the
/// guarantee the equivalence tests rely on.
pub fn build_receipt_to_tx_info(inputs: BuiltOriginInputs) -> ReceiptToTxInfo {
    let BuiltOriginInputs { outcome_id, shard_id, child_receiver_id, origin } = inputs;
    let origin = match origin {
        BuiltOrigin::FromTransaction { sender_id } => {
            ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                tx_hash: outcome_id,
                sender_account_id: sender_id,
            })
        }
        BuiltOrigin::FromReceipt { parent_predecessor_id } => {
            ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                parent_receipt_id: outcome_id,
                parent_predecessor_id,
            })
        }
    };
    ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
        origin,
        receiver_account_id: child_receiver_id,
        shard_id,
    })
}

/// Process a single height: read all execution outcomes and build ReceiptToTxInfo entries.
///
/// Returns `Ok(None)` only when there is no block at `height` (counted as a skipped height).
/// Returns `Ok(Some(_))` whenever the height has a block — the `HeightResult` carries the
/// entries produced and the per-category counts of data that was expected but missing.
///
/// **Read pattern:** the per-height work collapses the chain reads into
/// three single-CF batched MultiGets:
///
/// * Stage 1 — `TransactionResultForBlock` MultiGet for all outcome
///   composite keys (30-50 keys).
/// * Stage 2A — `Receipts` pinned multi-get for parents+children
///   combined (~190 keys, single CF, pinned slices).
/// * Stage 2B — `Transactions` `multi_exists` to classify each outcome
///   as tx-origin vs receipt-origin (30-50 keys). Refcount stripping
///   ensures tombstones are not misclassified as live transactions.
///
/// The `OutcomeIds` prefix iter is unchanged: it's already a sequential
/// range scan after one disk seek and the keys are clustered by block
/// hash, so it has nothing to gain from MultiGet.
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

    // Pass 0 — walk the OutcomeIds prefix iterator, collect the
    // (outcome_id, shard_id) descriptors for the height.
    let mut outcome_descriptors: Vec<(CryptoHash, ShardId)> = Vec::new();
    for (key, outcome_ids) in
        read_store.iter_prefix_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, block_hash.as_ref())
    {
        let (_, shard_id) = get_block_shard_id_rev(&key).expect("invalid OutcomeIds key");
        for outcome_id in outcome_ids {
            outcome_descriptors.push((outcome_id, shard_id));
        }
    }

    if outcome_descriptors.is_empty() {
        return Ok(Some(HeightResult {
            entries,
            missing_outcomes,
            missing_child_receipts,
            missing_parent_receipts,
        }));
    }

    // Stage 1 — single MultiGet on TransactionResultForBlock for all
    // outcome composite keys.  The composite key is
    // `get_outcome_id_block_hash(outcome_id, block_hash)` which is what
    // `ChainStore::get_outcome_by_id_and_block_hash` builds internally.
    let outcome_keys_owned: Vec<Vec<u8>> = outcome_descriptors
        .iter()
        .map(|(oid, _)| get_outcome_id_block_hash(oid, &block_hash))
        .collect();
    let outcome_key_refs: Vec<&[u8]> = outcome_keys_owned.iter().map(Vec::as_slice).collect();
    let outcome_results = read_store.multi_get(DBCol::TransactionResultForBlock, &outcome_key_refs);

    // Build the StagedOutcome list.  Skip outcomes with no children — they
    // contribute nothing to ReceiptToTx and also nothing to Stage 2.
    let mut staged: Vec<StagedOutcome> = Vec::with_capacity(outcome_descriptors.len());
    for ((outcome_id, shard_id), result) in
        outcome_descriptors.iter().zip(outcome_results.into_iter())
    {
        match result {
            Some(bytes) => {
                let outcome = ExecutionOutcomeWithProof::try_from_slice(bytes.as_slice())
                    .expect("borsh deserialization should not fail");
                if outcome.outcome.receipt_ids.is_empty() {
                    continue;
                }
                staged.push(StagedOutcome {
                    outcome_id: *outcome_id,
                    shard_id: *shard_id,
                    outcome,
                });
            }
            None => {
                missing_outcomes += 1;
                tracing::warn!(
                    %outcome_id,
                    height,
                    %shard_id,
                    "missing execution outcome, skipping"
                );
            }
        }
    }

    if staged.is_empty() {
        return Ok(Some(HeightResult {
            entries,
            missing_outcomes,
            missing_child_receipts,
            missing_parent_receipts,
        }));
    }

    // Stage 2B — multi_exists on `Transactions` to classify each outcome
    // as tx-origin (true) or receipt-origin (false).  The Store::multi_exists
    // wrapper applies refcount-aware logic via Store::multi_get, so RC
    // tombstones for `Transactions` (an RC column) are correctly reported
    // as absent.
    let outcome_id_owned: Vec<&CryptoHash> = staged.iter().map(|s| &s.outcome_id).collect();
    let outcome_id_refs: Vec<&[u8]> = outcome_id_owned.iter().map(|h| h.as_ref()).collect();
    let is_tx_results = read_store.multi_exists(DBCol::Transactions, &outcome_id_refs);

    // Stage 2A — single batched MultiGet on `Receipts` covering BOTH parent
    // receipts (one per receipt-origin outcome, ~6-15 per height) AND child
    // receipts (every entry in every outcome's `receipt_ids`, ~120-180 per
    // height).  Combining them into one call lets RocksDB sort across the
    // whole 195-key set internally, which gives the kernel block layer a
    // larger LBA-sortable submission than splitting per-role would.
    let mut parent_keys: Vec<&[u8]> = Vec::new();
    // For each parent in `parent_keys`, the index into `staged` it belongs to.
    let mut parent_to_staged_idx: Vec<usize> = Vec::new();
    let mut child_keys: Vec<&[u8]> = Vec::new();
    // For each child in `child_keys`, (staged_idx, position-within-outcome).
    let mut child_origins: Vec<(usize, usize)> = Vec::new();

    for (staged_idx, s) in staged.iter().enumerate() {
        if !is_tx_results[staged_idx] {
            parent_keys.push(s.outcome_id.as_ref());
            parent_to_staged_idx.push(staged_idx);
        }
        for (child_pos, child_id) in s.outcome.outcome.receipt_ids.iter().enumerate() {
            child_keys.push(child_id.as_ref());
            child_origins.push((staged_idx, child_pos));
        }
    }

    let parent_count = parent_keys.len();
    let mut combined_receipt_keys: Vec<&[u8]> = Vec::with_capacity(parent_count + child_keys.len());
    combined_receipt_keys.extend_from_slice(&parent_keys);
    combined_receipt_keys.extend_from_slice(&child_keys);
    let receipt_results = read_store.multi_get(DBCol::Receipts, &combined_receipt_keys);
    let (parent_results, child_results) = receipt_results.split_at(parent_count);

    // Deserialize parents into a sparse lookup table indexed by `staged_idx`.
    // None entries are fine — we'll count them as `missing_parent_receipts`
    // when the assembly loop tries to use them.
    let mut parent_by_staged_idx: HashMap<usize, Receipt> = HashMap::with_capacity(parent_count);
    for (i, &staged_idx) in parent_to_staged_idx.iter().enumerate() {
        if let Some(bytes) = &parent_results[i] {
            let receipt = Receipt::try_from_slice(bytes.as_slice())
                .expect("borsh deserialization should not fail");
            parent_by_staged_idx.insert(staged_idx, receipt);
        }
    }

    // Assembly — walk children in input order; build entries.
    for ((staged_idx, child_pos), child_result) in child_origins.iter().zip(child_results.iter()) {
        let s = &staged[*staged_idx];
        let outcome = &s.outcome.outcome;
        let child_receipt_id = &outcome.receipt_ids[*child_pos];
        let is_tx = is_tx_results[*staged_idx];

        let child_receipt = match child_result {
            Some(bytes) => Receipt::try_from_slice(bytes.as_slice())
                .expect("borsh deserialization should not fail"),
            None => {
                missing_child_receipts += 1;
                tracing::debug!(
                    %child_receipt_id,
                    outcome_id = %s.outcome_id,
                    height,
                    "missing child receipt, skipping"
                );
                continue;
            }
        };

        let origin = if is_tx {
            BuiltOrigin::FromTransaction { sender_id: outcome.executor_id.clone() }
        } else {
            let parent = match parent_by_staged_idx.get(staged_idx) {
                Some(r) => r,
                None => {
                    missing_parent_receipts += 1;
                    tracing::debug!(
                        outcome_id = %s.outcome_id,
                        height,
                        "missing parent receipt for receipt outcome, skipping"
                    );
                    continue;
                }
            };
            BuiltOrigin::FromReceipt { parent_predecessor_id: parent.predecessor_id().clone() }
        };

        let info = build_receipt_to_tx_info(BuiltOriginInputs {
            outcome_id: s.outcome_id,
            shard_id: s.shard_id,
            child_receiver_id: child_receipt.receiver_id().clone(),
            origin,
        });
        entries.push((*child_receipt_id, info));
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
/// Shared by both the CLI (ascending direction, checkpoint key = [`BACKFILL_CHECKPOINT_KEY`])
/// and the background actor (descending direction, checkpoint key = [`BACKFILL_CHECKPOINT_KEY_LOW`]).
/// The caller controls iteration direction by the order of `heights` — `par_iter` preserves
/// input order.
///
/// Semantics:
/// 1. Reads for all `heights` run in parallel on `pool`.
/// 2. Results are sorted+deduped by receipt id and applied to `storage.write_store` either
///    via SST file ingestion (when `storage.sst_temp_dir` is `Some`) or per-key
///    `StoreUpdate.insert` + `commit()` (when `None`).
/// 3. If `checkpoint` is `Some((key, height))`, that entry is written to
///    `storage.checkpoint_store` *after* entries are committed. The two commits are
///    non-atomic; a crash between them replays safely because `DBCol::ReceiptToTx` is
///    insert-only. Pass `None` to skip the checkpoint write (e.g. one-shot CLI reruns).
///
/// # Production deployment assumption (SST path)
///
/// The SST path assumes a fresh archival deployment with **no pre-existing
/// ReceiptToTx data**. Within a single actor instance no key overlap is
/// possible: each height is processed exactly once, receipt ids are unique
/// across the chain by construction, and the same-batch dedupe below catches
/// the rare case where one receipt id is observed at multiple heights inside
/// the same batch (e.g. cross-shard receipts visible at producer + receiver
/// heights).
///
/// Cross-batch overlap with already-stored keys can only arise if the CLI
/// tool ran first or if the CLI and actor run concurrently. Neither scenario
/// happens on this deploy; `test_sequential_forward_then_backward_converge`
/// is the regression net for the hypothetical concurrent path.
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

    let mut entries: Vec<(CryptoHash, Vec<u8>)> = Vec::new();
    for result in results {
        match result {
            Ok(Some(height_result)) => {
                for (receipt_id, info) in height_result.entries {
                    let value = borsh::to_vec(&info).context("serialize ReceiptToTxInfo")?;
                    entries.push((receipt_id, value));
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

    if !entries.is_empty() {
        let deduped = sort_and_dedupe_entries(entries)?;
        stats.entries_written = deduped.len() as u64;

        match &storage.sst_temp_dir {
            Some(temp_dir) => {
                let kvs: Vec<(&[u8], &[u8])> =
                    deduped.iter().map(|(k, v)| (k.as_ref(), v.as_slice())).collect();
                storage.write_store.ingest_insert_only_sst(DBCol::ReceiptToTx, &kvs, temp_dir)?;
            }
            None => {
                let mut entry_update = storage.write_store.store_update();
                for (receipt_id, value) in deduped {
                    entry_update.insert(DBCol::ReceiptToTx, receipt_id.as_ref().to_vec(), value);
                }
                entry_update.commit();
            }
        }
    }

    if let Some((key, height)) = checkpoint {
        let mut cp_update = storage.checkpoint_store.store_update();
        cp_update.set_ser(DBCol::Misc, key, &height);
        cp_update.commit();
    }

    Ok(stats)
}

/// Sort `entries` by key and drop exact duplicates. Returns
/// [`BackfillError::ValueDivergence`] if any duplicate key has a different
/// value, so the caller can quarantine the batch (no checkpoint advance, no
/// entries written) instead of letting the SST ingest silently accept one
/// of the conflicting values.
fn sort_and_dedupe_entries(
    mut entries: Vec<(CryptoHash, Vec<u8>)>,
) -> Result<Vec<(CryptoHash, Vec<u8>)>, BackfillError> {
    entries.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    let mut deduped: Vec<(CryptoHash, Vec<u8>)> = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        match deduped.last() {
            Some((prev_k, prev_v)) if prev_k.as_ref() == k.as_ref() => {
                if prev_v != &v {
                    return Err(BackfillError::ValueDivergence {
                        key: k,
                        prev_value: prev_v.clone(),
                        new_value: v,
                    });
                }
            }
            _ => deduped.push((k, v)),
        }
    }
    Ok(deduped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> CryptoHash {
        CryptoHash([byte; 32])
    }

    #[test]
    fn sort_and_dedupe_passes_unique_entries_through() {
        let input = vec![(h(2), b"bb".to_vec()), (h(1), b"aa".to_vec()), (h(3), b"cc".to_vec())];
        let out = sort_and_dedupe_entries(input).expect("unique keys must succeed");
        assert_eq!(
            out,
            vec![(h(1), b"aa".to_vec()), (h(2), b"bb".to_vec()), (h(3), b"cc".to_vec())]
        );
    }

    #[test]
    fn sort_and_dedupe_collapses_exact_duplicates() {
        let input = vec![(h(1), b"aa".to_vec()), (h(1), b"aa".to_vec()), (h(2), b"bb".to_vec())];
        let out = sort_and_dedupe_entries(input).expect("identical duplicate must collapse");
        assert_eq!(out, vec![(h(1), b"aa".to_vec()), (h(2), b"bb".to_vec())]);
    }

    #[test]
    fn sort_and_dedupe_surfaces_value_divergence() {
        let input =
            vec![(h(1), b"aa".to_vec()), (h(1), b"different".to_vec()), (h(2), b"bb".to_vec())];
        let err = sort_and_dedupe_entries(input).expect_err("divergent values must error");
        match err {
            BackfillError::ValueDivergence { key, prev_value, new_value } => {
                assert_eq!(key, h(1));
                // The "kept" value is whichever sorted first by Vec::sort_by stability;
                // both inputs have the same key so order between them is preserved.
                // We only check both byte payloads were captured.
                let payloads = [&prev_value[..], &new_value[..]];
                assert!(payloads.contains(&&b"aa"[..]));
                assert!(payloads.contains(&&b"different"[..]));
            }
        }
    }

    #[test]
    fn sort_and_dedupe_handles_empty_input() {
        let out = sort_and_dedupe_entries(Vec::new()).expect("empty input is valid");
        assert!(out.is_empty());
    }
}
