//! Bucket-ETL pipeline that rebuilds `DBCol::ReceiptToTx` from cold-store data
//! without random reads.
//!
//! Four parallel-by-prefix passes with a deterministic shuffle:
//!
//! - **Pass A**: scan `DBCol::Receipts` partitioned by first byte; emit
//!   `(receipt_id, receiver_id, predecessor_id)` per receipt to per-prefix files.
//! - **Pass B**: parallel block walk; per outcome emit either a tx-origin
//!   demand row (bucketed by `child_id_first_byte`) or a needs-parent row
//!   (bucketed by `parent_receipt_id_first_byte`).
//! - **Pass C**: per `parent_id` prefix, join needs-parent rows against the
//!   receipts extract for that prefix to fill in `parent.predecessor_id`.
//!   Output is re-bucketed by `child_id_first_byte` (the deterministic
//!   shuffle that eliminates cross-prefix lookups in Pass D).
//! - **Pass D**: per `child_id` prefix, join tx-origin demands (Pass B) +
//!   resolved receipt-origin demands (Pass C) against the receipts extract
//!   to fill in `child.receiver_id`. Build `ReceiptToTxInfo` via the shared
//!   kernel, sort+dedupe with `BackfillError::ValueDivergence` quarantine,
//!   validate SST invariants, write per-prefix output files.
//!
//! Restart machinery:
//! - per-stage `.done` markers in `{scratch_dir}/.markers/`
//! - per-Pass-B-worker height marker; on restart, each worker resumes from
//!   its last marker rather than slice-start.
//! - `options.json` records the invocation; mismatched options on resume
//!   abort with a clear error.
//!
//! Phase 1 prototype: in-memory sort within each prefix is sufficient at
//! expected scale (per-prefix bucket << RAM). External-sort is Phase 3.

use crate::backfill_receipt_to_tx::{
    BackfillError, BackfillStorage, BuiltOrigin, BuiltOriginInputs, build_receipt_to_tx_info,
    process_height,
};
use crate::{ChainStore, ChainStoreAccess, Error};
use anyhow::{Context, bail};
use borsh::{BorshDeserialize, BorshSerialize};
use near_o11y::tracing;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptToTxInfo};
use near_primitives::transaction::ExecutionOutcomeWithProof;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash};
use near_store::DBCol;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Strict upper bound the SST ingest path enforces; mirrored here so Pass D
/// rejects any output bucket the Phase 3 ingest would refuse.
pub const MAX_BUCKET_SERIALIZED_BYTES: u64 = 256 * 1024 * 1024;

/// Production default for Pass B per-worker height marker cadence. Tests
/// override to small values to exercise the resume path on small ranges.
const DEFAULT_MARKER_BLOCK_INTERVAL: u64 = 100_000;

const STAGE_A: &str = "pass_a";
const STAGE_B: &str = "pass_b";
const STAGE_C: &str = "pass_c";
const STAGE_D: &str = "pass_d";

#[derive(Clone)]
pub struct BucketEtlOptions {
    pub from_height: Option<BlockHeight>,
    pub to_height: Option<BlockHeight>,
    pub num_threads: usize,
    pub scratch_dir: PathBuf,
    pub output_mode: BucketEtlOutputMode,
    /// Block interval at which Pass B workers persist a height marker and
    /// flush accumulated buffers. Tests override to small values.
    pub marker_block_interval: u64,
    /// Test-only knob: cause Pass B to abort once any worker reaches this
    /// height. Drives `test_bucket_etl_resumes_after_simulated_crash`. The
    /// field is unconditional (rather than `cfg(feature = "test_features")`)
    /// to avoid feature-mismatch initializer errors in callers that don't
    /// declare a `test_features` feature themselves; `None` is the
    /// production default and is checked at the only use site.
    pub crash_after_pass_b_height: Option<BlockHeight>,
}

impl BucketEtlOptions {
    pub fn measure_only(scratch_dir: PathBuf) -> Self {
        Self {
            from_height: None,
            to_height: None,
            num_threads: 4,
            scratch_dir,
            output_mode: BucketEtlOutputMode::MeasureOnly,
            marker_block_interval: DEFAULT_MARKER_BLOCK_INTERVAL,
            crash_after_pass_b_height: None,
        }
    }
}

#[derive(Clone)]
pub enum BucketEtlOutputMode {
    /// Validate output buckets but don't write to any store. Operational mode
    /// used on the production VM to gate the run.
    MeasureOnly,
    /// Test-only mode: write the validated output to `BackfillStorage.write_store`
    /// via the legacy per-key insert path. Not exposed via the CLI.
    WriteToStore,
    /// Run `process_height` over `[from_height, to_height]` after Pass D and
    /// byte-compare resulting `ReceiptToTx` entries.
    CompareAgainstActor { from_height: BlockHeight, to_height: BlockHeight },
}

#[derive(Default, Debug, Clone, Copy)]
pub struct BucketEtlStats {
    pub pass_a_rows: u64,
    pub pass_b_tx_origin_rows: u64,
    pub pass_b_needs_parent_rows: u64,
    pub pass_c_rows: u64,
    pub pass_d_rows: u64,
    pub blocks_processed: u64,
    pub heights_skipped: u64,
    pub missing_outcomes: u64,
    pub missing_child_receipts: u64,
    pub missing_parent_receipts: u64,
    pub compare_divergences: u64,
}

// ===== Bucket file row types =====

/// Pass A output: per receipt, the primitives needed by Pass C (predecessor)
/// and Pass D (receiver) joins.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct ReceiptExtractRow {
    pub receipt_id: CryptoHash,
    pub receiver_id: AccountId,
    pub predecessor_id: AccountId,
}

/// Pass B output (bucketed by `child_id` first byte): tx-origin demand row.
/// Carries everything Pass D needs for tx-origin entries (no parent lookup).
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct TxOriginDemandRow {
    pub child_id: CryptoHash,
    pub shard_id: ShardId,
    pub tx_hash: CryptoHash,
    pub sender_account_id: AccountId,
}

/// Pass B output (bucketed by `parent_receipt_id` first byte): receipt-origin
/// row pending parent.predecessor_id resolution in Pass C.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct NeedsParentRow {
    pub child_id: CryptoHash,
    pub shard_id: ShardId,
    pub parent_receipt_id: CryptoHash,
}

/// Pass C output (re-bucketed by `child_id` first byte): receipt-origin demand
/// row with parent.predecessor_id filled in from Pass A's extract.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct ReceiptOriginDemandRow {
    pub child_id: CryptoHash,
    pub shard_id: ShardId,
    pub parent_receipt_id: CryptoHash,
    pub parent_predecessor_id: AccountId,
}

/// Pass D output: the final `(child_id, ReceiptToTxInfo)` entry to ingest.
#[derive(BorshSerialize, BorshDeserialize, Clone, PartialEq, Eq, Debug)]
pub struct OutputRow {
    pub child_id: CryptoHash,
    pub info: ReceiptToTxInfo,
}

// ===== Bucket file format =====

/// Length-prefixed borsh records. `[u32 LE length][borsh bytes]...`.
pub struct BucketWriter<W: Write> {
    inner: W,
    bytes_written: u64,
}

impl<W: Write> BucketWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, bytes_written: 0 }
    }

    pub fn write<T: BorshSerialize>(&mut self, row: &T) -> anyhow::Result<()> {
        let bytes = borsh::to_vec(row).context("borsh serialize bucket row")?;
        let len: u32 = bytes.len().try_into().context("bucket row exceeds u32::MAX")?;
        self.inner.write_all(&len.to_le_bytes()).context("write bucket row length")?;
        self.inner.write_all(&bytes).context("write bucket row payload")?;
        self.bytes_written += 4 + bytes.len() as u64;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub fn finish(mut self) -> anyhow::Result<W> {
        self.inner.flush().context("flush bucket writer")?;
        Ok(self.inner)
    }
}

pub struct BucketReader<R: Read> {
    inner: R,
}

impl<R: Read> BucketReader<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: Read> Iterator for BucketReader<R> {
    type Item = anyhow::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_bytes = [0u8; 4];
        match self.inner.read_exact(&mut len_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(anyhow::Error::from(e).context("read bucket row length"))),
        }
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut buf = vec![0u8; len];
        if let Err(e) = self.inner.read_exact(&mut buf) {
            return Some(Err(anyhow::Error::from(e).context("read bucket row payload")));
        }
        Some(Ok(buf))
    }
}

/// Read a whole bucket file into a `Vec<T>`. The file must exist.
pub fn read_bucket<T: BorshDeserialize>(path: &Path) -> anyhow::Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let f = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BucketReader::new(BufReader::new(f));
    let mut out = Vec::new();
    for row in reader {
        let bytes = row?;
        let value = T::try_from_slice(&bytes).context("borsh deserialize bucket row")?;
        out.push(value);
    }
    Ok(out)
}

/// Atomic-rename based bucket writer: writes to `<path>.tmp`, fsyncs, renames.
pub fn write_bucket<T: BorshSerialize>(path: &Path, rows: &[T]) -> anyhow::Result<u64> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }
    let tmp = with_tmp_suffix(path);
    let f = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    let mut w = BucketWriter::new(BufWriter::new(f));
    for row in rows {
        w.write(row)?;
    }
    let bytes = w.bytes_written();
    let bw = w.finish()?;
    let f = bw.into_inner().context("bufwriter into_inner")?;
    f.sync_all().context("fsync bucket")?;
    drop(f);
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(bytes)
}

fn with_tmp_suffix(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Sort by key, surface `BackfillError::ValueDivergence` on any same-key pair
/// whose serialized values differ, and validate the SST-ingest invariants
/// (strictly sorted, no duplicate keys, ≤`max_bytes` total).
///
/// The serialized-value comparison mirrors `sort_and_dedupe_entries` in
/// `backfill_receipt_to_tx.rs`: identical entries collapse, divergent ones
/// quarantine the whole batch. Tests use [`finalize_output_bucket_with_max`]
/// with a small `max_bytes` to exercise the size-limit path.
pub fn finalize_output_bucket_with_max(
    mut rows: Vec<OutputRow>,
    max_bytes: u64,
) -> anyhow::Result<Vec<OutputRow>> {
    rows.sort_by(|a, b| a.child_id.as_ref().cmp(b.child_id.as_ref()));
    let mut deduped: Vec<OutputRow> = Vec::with_capacity(rows.len());
    let mut total_bytes: u64 = 0;
    for row in rows {
        match deduped.last() {
            Some(prev) if prev.child_id == row.child_id => {
                let prev_bytes =
                    borsh::to_vec(&prev.info).context("serialize prev ReceiptToTxInfo")?;
                let new_bytes =
                    borsh::to_vec(&row.info).context("serialize new ReceiptToTxInfo")?;
                if prev_bytes != new_bytes {
                    return Err(BackfillError::ValueDivergence {
                        key: row.child_id,
                        prev_value: prev_bytes,
                        new_value: new_bytes,
                    }
                    .into());
                }
                // identical: collapse
            }
            _ => {
                let bytes = borsh::to_vec(&row).context("serialize OutputRow")?;
                total_bytes += 4 + bytes.len() as u64;
                if total_bytes > max_bytes {
                    bail!(
                        "Pass D output bucket exceeds {} bytes (last key {}); \
                         exceeds Store::ingest_insert_only_sst limit",
                        max_bytes,
                        row.child_id,
                    );
                }
                deduped.push(row);
            }
        }
    }
    // Strictly-sorted check (defensive — sort_by + dedupe should guarantee, but
    // exists to catch a regression in the validator itself).
    for pair in deduped.windows(2) {
        if pair[0].child_id.as_ref() >= pair[1].child_id.as_ref() {
            bail!(
                "Pass D output bucket is not strictly sorted: {} >= {}",
                pair[0].child_id,
                pair[1].child_id,
            );
        }
    }
    Ok(deduped)
}

pub fn finalize_output_bucket(rows: Vec<OutputRow>) -> anyhow::Result<Vec<OutputRow>> {
    finalize_output_bucket_with_max(rows, MAX_BUCKET_SERIALIZED_BYTES)
}

/// Validate an already-written output bucket file against the SST-ingest
/// invariants. Used by `test_bucket_etl_rejects_invalid_output_bucket` to
/// confirm the validator rejects hand-corrupted files.
pub fn validate_output_bucket_file_with_max(path: &Path, max_bytes: u64) -> anyhow::Result<()> {
    let rows: Vec<OutputRow> = read_bucket(path)?;
    let mut total_bytes: u64 = 0;
    for (i, row) in rows.iter().enumerate() {
        let bytes = borsh::to_vec(row).context("serialize OutputRow")?;
        total_bytes += 4 + bytes.len() as u64;
        if total_bytes > max_bytes {
            bail!("bucket {} exceeds {} bytes serialized", path.display(), max_bytes);
        }
        if i > 0 {
            let prev = &rows[i - 1];
            if prev.child_id.as_ref() >= row.child_id.as_ref() {
                if prev.child_id == row.child_id {
                    bail!("bucket {} contains duplicate key {}", path.display(), row.child_id);
                }
                bail!(
                    "bucket {} keys not strictly sorted: {} >= {}",
                    path.display(),
                    prev.child_id,
                    row.child_id
                );
            }
        }
    }
    Ok(())
}

pub fn validate_output_bucket_file(path: &Path) -> anyhow::Result<()> {
    validate_output_bucket_file_with_max(path, MAX_BUCKET_SERIALIZED_BYTES)
}

// ===== Restart helpers =====

fn markers_dir(scratch: &Path) -> PathBuf {
    scratch.join(".markers")
}

fn stage_marker_path(scratch: &Path, stage: &str) -> PathBuf {
    markers_dir(scratch).join(format!("{stage}.done"))
}

fn stage_done(scratch: &Path, stage: &str) -> bool {
    stage_marker_path(scratch, stage).exists()
}

fn mark_stage_done(scratch: &Path, stage: &str) -> anyhow::Result<()> {
    let dir = markers_dir(scratch);
    fs::create_dir_all(&dir).with_context(|| format!("mkdir {}", dir.display()))?;
    let path = stage_marker_path(scratch, stage);
    let tmp = with_tmp_suffix(&path);
    fs::write(&tmp, b"").with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, &path).context("atomic-rename stage marker")?;
    Ok(())
}

fn options_hash_path(scratch: &Path) -> PathBuf {
    scratch.join("options.json")
}

/// Hash the load-bearing fields of an invocation. Takes the *resolved*
/// `from`/`to` heights (after `genesis`/`head` defaults are applied) so a
/// no-arg run that picks up new heights between invocations does NOT match
/// the previous fingerprint and silently skip Pass B. `output_mode` is
/// excluded — running measure-only first then compare-mode against the
/// same scratch is a legitimate use case.
///
/// Path bytes go in via `as_os_str().as_encoded_bytes()` so non-UTF-8 paths
/// fingerprint distinctly (`to_string_lossy` would collapse them).
fn compute_options_hash(
    scratch_dir: &Path,
    from: BlockHeight,
    to: BlockHeight,
    num_threads: usize,
    marker_block_interval: u64,
) -> String {
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&from.to_le_bytes());
    buf.extend_from_slice(&to.to_le_bytes());
    buf.extend_from_slice(&(num_threads as u64).to_le_bytes());
    buf.extend_from_slice(&marker_block_interval.to_le_bytes());
    buf.extend_from_slice(scratch_dir.as_os_str().as_encoded_bytes());
    CryptoHash::hash_bytes(&buf).to_string()
}

fn check_or_write_options_hash(
    scratch: &Path,
    from: BlockHeight,
    to: BlockHeight,
    num_threads: usize,
    marker_block_interval: u64,
) -> anyhow::Result<()> {
    let path = options_hash_path(scratch);
    let want = compute_options_hash(scratch, from, to, num_threads, marker_block_interval);
    if path.exists() {
        let got = fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        if got.trim() != want {
            bail!(
                "scratch_dir {} was created with different options (hash mismatch); \
                 either use a fresh --scratch-dir or delete the existing one",
                scratch.display(),
            );
        }
    } else {
        fs::create_dir_all(scratch).with_context(|| format!("mkdir {}", scratch.display()))?;
        let tmp = with_tmp_suffix(&path);
        fs::write(&tmp, want.as_bytes()).with_context(|| format!("write {}", tmp.display()))?;
        fs::rename(&tmp, &path).context("atomic-rename options.json")?;
    }
    Ok(())
}

fn worker_marker_path(scratch: &Path, worker: usize) -> PathBuf {
    scratch.join(STAGE_B).join(format!("worker_{worker}")).join("marker")
}

/// Read the per-worker height marker.
///
/// Returns `Ok(None)` when the marker file is missing (legitimate first run).
/// Returns `Err(...)` when the file exists but its contents are invalid —
/// silently re-doing all of Pass B's work for a worker because of a corrupt
/// 8-byte file would erase the resume guarantee, so we fail loudly instead.
fn read_resume_height(scratch: &Path, worker: usize) -> anyhow::Result<Option<BlockHeight>> {
    let path = worker_marker_path(scratch, worker);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(anyhow::Error::from(e))
                .with_context(|| format!("read worker {worker} marker at {}", path.display()));
        }
    };
    if bytes.len() != 8 {
        bail!(
            "worker {worker} marker at {} has {} bytes; expected 8 — corruption suspected, \
             refusing to re-do completed work",
            path.display(),
            bytes.len()
        );
    }
    let arr: [u8; 8] = bytes.try_into().expect("checked length above");
    Ok(Some(u64::from_le_bytes(arr)))
}

fn write_resume_height(scratch: &Path, worker: usize, height: BlockHeight) -> anyhow::Result<()> {
    let path = worker_marker_path(scratch, worker);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("mkdir {}", parent.display()))?;
    }
    let tmp = with_tmp_suffix(&path);
    fs::write(&tmp, height.to_le_bytes()).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, &path).context("atomic-rename worker marker")?;
    Ok(())
}

// ===== Path helpers =====

fn pass_a_path(scratch: &Path, prefix: u8) -> PathBuf {
    scratch.join(STAGE_A).join(format!("prefix_{prefix:02x}.bucket"))
}

fn pass_b_seg_demand_path(scratch: &Path, worker: usize, seg: u32, prefix: u8) -> PathBuf {
    scratch
        .join(STAGE_B)
        .join(format!("worker_{worker}"))
        .join(format!("seg_{seg}"))
        .join(format!("demand_{prefix:02x}.bucket"))
}

fn pass_b_seg_needs_parent_path(scratch: &Path, worker: usize, seg: u32, prefix: u8) -> PathBuf {
    scratch
        .join(STAGE_B)
        .join(format!("worker_{worker}"))
        .join(format!("seg_{seg}"))
        .join(format!("needs_parent_{prefix:02x}.bucket"))
}

fn pass_b_consolidated_demand_path(scratch: &Path, prefix: u8) -> PathBuf {
    scratch.join(STAGE_B).join("demand_by_child").join(format!("prefix_{prefix:02x}.bucket"))
}

fn pass_b_consolidated_needs_parent_path(scratch: &Path, prefix: u8) -> PathBuf {
    scratch.join(STAGE_B).join("needs_parent_by_parent").join(format!("prefix_{prefix:02x}.bucket"))
}

fn pass_c_consolidated_path(scratch: &Path, prefix: u8) -> PathBuf {
    scratch.join(STAGE_C).join("demand_by_child").join(format!("prefix_{prefix:02x}.bucket"))
}

pub fn pass_d_output_path(scratch: &Path, prefix: u8) -> PathBuf {
    scratch.join(STAGE_D).join("output").join(format!("prefix_{prefix:02x}.bucket"))
}

// ===== Pass A: receipts extract =====

fn pass_a_receipts_extract(
    storage: &BackfillStorage,
    opts: &BucketEtlOptions,
) -> anyhow::Result<u64> {
    let store = &storage.read_store;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(opts.num_threads.max(1))
        .build()
        .context("build pass A thread pool")?;
    let scratch = opts.scratch_dir.clone();
    fs::create_dir_all(scratch.join(STAGE_A))
        .with_context(|| format!("mkdir {}", scratch.join(STAGE_A).display()))?;

    let total_rows: u64 = pool.install(|| {
        (0u16..=255)
            .into_par_iter()
            .try_fold(
                || 0u64,
                |acc, prefix_u16| -> anyhow::Result<u64> {
                    let prefix = prefix_u16 as u8;
                    let out_path = pass_a_path(&scratch, prefix);
                    if out_path.exists() {
                        // Resume: prefix already done.
                        let rows: Vec<ReceiptExtractRow> = read_bucket(&out_path)?;
                        return Ok(acc + rows.len() as u64);
                    }
                    let lower = [prefix];
                    let upper_storage: Option<[u8; 1]> =
                        if prefix == 0xff { None } else { Some([prefix + 1]) };
                    let upper: Option<&[u8]> = upper_storage.as_ref().map(|b| b.as_slice());

                    let mut rows: Vec<ReceiptExtractRow> = Vec::new();
                    for (key, value) in store.iter_range(DBCol::Receipts, Some(&lower), upper) {
                        let receipt_id = CryptoHash::try_from(key.as_ref()).map_err(|e| {
                            anyhow::anyhow!("invalid receipt id key (len={}): {e}", key.len())
                        })?;
                        let receipt = Receipt::try_from_slice(value.as_ref())
                            .context("borsh deserialize Receipt during Pass A scan")?;
                        rows.push(ReceiptExtractRow {
                            receipt_id,
                            receiver_id: receipt.receiver_id().clone(),
                            predecessor_id: receipt.predecessor_id().clone(),
                        });
                    }
                    // iter_range delivers in sorted order, so rows are already
                    // sorted by receipt_id. Cheap debug_assert keeps a regression
                    // net without paying for a full sort on every prefix in prod.
                    debug_assert!(
                        rows.windows(2).all(|w| w[0].receipt_id.as_ref() < w[1].receipt_id.as_ref()),
                        "Pass A: iter_range broke its sorted-output contract for prefix {prefix:#04x}"
                    );
                    let count = rows.len() as u64;
                    write_bucket(&out_path, &rows)?;
                    Ok(acc + count)
                },
            )
            .try_reduce(|| 0u64, |a, b| Ok(a + b))
    })?;

    mark_stage_done(&scratch, STAGE_A)?;
    Ok(total_rows)
}

// ===== Pass B: parallel block walk =====

#[derive(Default)]
struct PassBHeightStats {
    blocks_processed: u64,
    heights_skipped: u64,
    missing_outcomes: u64,
}

/// Replicates the per-height OutcomeIds + TransactionResultForBlock +
/// `Transactions` `multi_exists` reads from `process_height`, but emits demand
/// rows instead of building entries directly. Receipt MultiGet (parents +
/// children) is replaced by Pass A's receipts extract + Pass C's resolution.
fn pass_b_emit_for_height<TxF, RpF>(
    chain_store: &ChainStore,
    height: BlockHeight,
    mut emit_tx: TxF,
    mut emit_needs_parent: RpF,
) -> anyhow::Result<PassBHeightStats>
where
    TxF: FnMut(TxOriginDemandRow),
    RpF: FnMut(NeedsParentRow),
{
    let block_hash = match chain_store.get_block_hash_by_height(height) {
        Ok(h) => h,
        Err(Error::DBNotFoundErr(_)) => {
            return Ok(PassBHeightStats { heights_skipped: 1, ..Default::default() });
        }
        Err(e) => {
            return Err(anyhow::Error::from(e))
                .with_context(|| format!("get block hash at height {height}"));
        }
    };

    let read_store = chain_store.store();
    let mut stats = PassBHeightStats { blocks_processed: 1, ..Default::default() };

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
        return Ok(stats);
    }

    let outcome_keys_owned: Vec<Vec<u8>> = outcome_descriptors
        .iter()
        .map(|(oid, _)| get_outcome_id_block_hash(oid, &block_hash))
        .collect();
    let outcome_key_refs: Vec<&[u8]> = outcome_keys_owned.iter().map(Vec::as_slice).collect();
    let outcome_results = read_store.multi_get(DBCol::TransactionResultForBlock, &outcome_key_refs);

    let mut staged: Vec<(CryptoHash, ShardId, ExecutionOutcomeWithProof)> = Vec::new();
    for ((outcome_id, shard_id), result) in
        outcome_descriptors.iter().zip(outcome_results.into_iter())
    {
        match result {
            Some(bytes) => {
                let owp = ExecutionOutcomeWithProof::try_from_slice(bytes.as_slice())
                    .expect("borsh deserialization should not fail");
                if owp.outcome.receipt_ids.is_empty() {
                    continue;
                }
                staged.push((*outcome_id, *shard_id, owp));
            }
            None => {
                stats.missing_outcomes += 1;
            }
        }
    }
    if staged.is_empty() {
        return Ok(stats);
    }

    // RC-aware classification: tombstones in `Transactions` must be reported
    // as absent. `multi_exists` dispatches via `multi_get` which strips refcounts.
    let outcome_id_owned: Vec<&CryptoHash> = staged.iter().map(|(o, _, _)| o).collect();
    let outcome_id_refs: Vec<&[u8]> = outcome_id_owned.iter().map(|h| h.as_ref()).collect();
    let is_tx_results = read_store.multi_exists(DBCol::Transactions, &outcome_id_refs);

    for (i, (outcome_id, shard_id, owp)) in staged.into_iter().enumerate() {
        let outcome = owp.outcome;
        let is_tx = is_tx_results[i];
        for child_id in &outcome.receipt_ids {
            if is_tx {
                emit_tx(TxOriginDemandRow {
                    child_id: *child_id,
                    shard_id,
                    tx_hash: outcome_id,
                    sender_account_id: outcome.executor_id.clone(),
                });
            } else {
                emit_needs_parent(NeedsParentRow {
                    child_id: *child_id,
                    shard_id,
                    parent_receipt_id: outcome_id,
                });
            }
        }
    }
    Ok(stats)
}

struct PassBWorkerOutcome {
    tx_origin_rows: u64,
    needs_parent_rows: u64,
    blocks_processed: u64,
    heights_skipped: u64,
    missing_outcomes: u64,
}

#[derive(Debug)]
pub struct CrashAfterPassBHeight(pub BlockHeight);
impl std::fmt::Display for CrashAfterPassBHeight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test-only crash_after_pass_b_height triggered at {}", self.0)
    }
}
impl std::error::Error for CrashAfterPassBHeight {}

fn pass_b_block_walk(
    chain_store: &ChainStore,
    opts: &BucketEtlOptions,
    from_height: BlockHeight,
    to_height: BlockHeight,
) -> anyhow::Result<(BucketEtlStats, Vec<PassBWorkerOutcome>)> {
    let num_workers = opts.num_threads.max(1);
    let total_heights = to_height.saturating_sub(from_height) + 1;
    let per_worker = total_heights.div_ceil(num_workers as u64).max(1);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_workers)
        .build()
        .context("build pass B thread pool")?;

    // Compute each worker's height slice up front.
    let slices: Vec<(usize, BlockHeight, BlockHeight)> = (0..num_workers)
        .map(|w| {
            let start = from_height + per_worker * w as u64;
            let end = (start + per_worker - 1).min(to_height);
            (w, start, end)
        })
        .filter(|(_, start, _)| *start <= to_height)
        .collect();

    let outcomes: Vec<anyhow::Result<PassBWorkerOutcome>> = pool.install(|| {
        slices
            .par_iter()
            .map(|(w, start, end)| pass_b_worker(chain_store, opts, *w, *start, *end))
            .collect()
    });

    let mut worker_outcomes: Vec<PassBWorkerOutcome> = Vec::with_capacity(outcomes.len());
    for o in outcomes {
        worker_outcomes.push(o?);
    }

    // Aggregate stats; consolidation happens after this fn returns.
    let mut stats = BucketEtlStats::default();
    for o in &worker_outcomes {
        stats.pass_b_tx_origin_rows += o.tx_origin_rows;
        stats.pass_b_needs_parent_rows += o.needs_parent_rows;
        stats.blocks_processed += o.blocks_processed;
        stats.heights_skipped += o.heights_skipped;
        stats.missing_outcomes += o.missing_outcomes;
    }
    Ok((stats, worker_outcomes))
}

fn pass_b_worker(
    chain_store: &ChainStore,
    opts: &BucketEtlOptions,
    worker: usize,
    slice_start: BlockHeight,
    slice_end: BlockHeight,
) -> anyhow::Result<PassBWorkerOutcome> {
    let scratch = &opts.scratch_dir;
    let resume_height = read_resume_height(scratch, worker)?;
    let start = match resume_height {
        Some(h) if h >= slice_start && h <= slice_end => h + 1,
        _ => slice_start,
    };
    if start > slice_end {
        // Already done.
        return Ok(PassBWorkerOutcome {
            tx_origin_rows: 0,
            needs_parent_rows: 0,
            blocks_processed: 0,
            heights_skipped: 0,
            missing_outcomes: 0,
        });
    }

    let mut tx_origin_rows: u64 = 0;
    let mut needs_parent_rows: u64 = 0;
    let mut blocks_processed: u64 = 0;
    let mut heights_skipped: u64 = 0;
    let mut missing_outcomes: u64 = 0;

    // Per-prefix in-memory buffers; flushed on marker boundary or at end of slice.
    let mut tx_buf: Vec<Vec<TxOriginDemandRow>> = (0..256).map(|_| Vec::new()).collect();
    let mut np_buf: Vec<Vec<NeedsParentRow>> = (0..256).map(|_| Vec::new()).collect();
    let mut seg = list_existing_segments(scratch, worker);

    let marker_interval = opts.marker_block_interval.max(1);
    let mut last_marker_height =
        resume_height.unwrap_or_else(|| slice_start.saturating_sub(1));

    let mut h = start;
    while h <= slice_end {
        if let Some(crash_h) = opts.crash_after_pass_b_height {
            if h == crash_h {
                // Flush whatever we have so the partial segment is durable, then
                // bail. The next run will pick up at h based on the marker.
                flush_segment(scratch, worker, seg, &mut tx_buf, &mut np_buf)?;
                return Err(CrashAfterPassBHeight(h).into());
            }
        }

        let stats = pass_b_emit_for_height(
            chain_store,
            h,
            |row| {
                let p = row.child_id.as_ref()[0];
                tx_buf[p as usize].push(row);
                tx_origin_rows += 1;
            },
            |row| {
                let p = row.parent_receipt_id.as_ref()[0];
                np_buf[p as usize].push(row);
                needs_parent_rows += 1;
            },
        )?;
        blocks_processed += stats.blocks_processed;
        heights_skipped += stats.heights_skipped;
        missing_outcomes += stats.missing_outcomes;

        // Marker boundary: if we've crossed an interval boundary, flush + advance marker.
        if h - last_marker_height >= marker_interval {
            flush_segment(scratch, worker, seg, &mut tx_buf, &mut np_buf)?;
            seg += 1;
            write_resume_height(scratch, worker, h)?;
            last_marker_height = h;
        }
        h += 1;
    }

    // End of slice: flush final segment + advance marker to slice_end.
    flush_segment(scratch, worker, seg, &mut tx_buf, &mut np_buf)?;
    write_resume_height(scratch, worker, slice_end)?;

    Ok(PassBWorkerOutcome {
        tx_origin_rows,
        needs_parent_rows,
        blocks_processed,
        heights_skipped,
        missing_outcomes,
    })
}

/// List existing segment counter for a worker. The next-to-write segment index
/// is the max(seg_S directories) + 1, or 0 if none exist.
fn list_existing_segments(scratch: &Path, worker: usize) -> u32 {
    let dir = scratch.join(STAGE_B).join(format!("worker_{worker}"));
    let Ok(entries) = fs::read_dir(&dir) else {
        return 0;
    };
    let mut max_seg: Option<u32> = None;
    for e in entries.flatten() {
        let Some(name) = e.file_name().to_str().map(str::to_owned) else { continue };
        if let Some(rest) = name.strip_prefix("seg_") {
            if let Ok(n) = rest.parse::<u32>() {
                max_seg = Some(max_seg.map_or(n, |m| m.max(n)));
            }
        }
    }
    max_seg.map(|m| m + 1).unwrap_or(0)
}

fn flush_segment(
    scratch: &Path,
    worker: usize,
    seg: u32,
    tx_buf: &mut [Vec<TxOriginDemandRow>],
    np_buf: &mut [Vec<NeedsParentRow>],
) -> anyhow::Result<()> {
    for prefix_idx in 0..256u16 {
        let prefix = prefix_idx as u8;
        let txs = std::mem::take(&mut tx_buf[prefix as usize]);
        if !txs.is_empty() {
            let mut sorted = txs;
            sorted.sort_by(|a, b| a.child_id.as_ref().cmp(b.child_id.as_ref()));
            write_bucket(&pass_b_seg_demand_path(scratch, worker, seg, prefix), &sorted)?;
        }
        let nps = std::mem::take(&mut np_buf[prefix as usize]);
        if !nps.is_empty() {
            let mut sorted = nps;
            sorted.sort_by(|a, b| a.parent_receipt_id.as_ref().cmp(b.parent_receipt_id.as_ref()));
            write_bucket(&pass_b_seg_needs_parent_path(scratch, worker, seg, prefix), &sorted)?;
        }
    }
    Ok(())
}

/// After all Pass B workers complete, k-way merge per-prefix segment files
/// into a single sorted `pass_b/{demand_by_child,needs_parent_by_parent}/prefix_XX.bucket`.
fn pass_b_consolidate(scratch: &Path, num_workers: usize) -> anyhow::Result<()> {
    // Discover number of segments per worker.
    let segs_per_worker: Vec<u32> =
        (0..num_workers).map(|w| list_existing_segments(scratch, w)).collect();

    // Consolidate demand-by-child.
    (0u16..=255).into_par_iter().try_for_each(|prefix_idx| -> anyhow::Result<()> {
        let prefix = prefix_idx as u8;
        let out = pass_b_consolidated_demand_path(scratch, prefix);
        if out.exists() {
            return Ok(());
        }
        let mut all: Vec<TxOriginDemandRow> = Vec::new();
        for (w, segs) in segs_per_worker.iter().enumerate() {
            for s in 0..*segs {
                let p = pass_b_seg_demand_path(scratch, w, s, prefix);
                if p.exists() {
                    let rows: Vec<TxOriginDemandRow> = read_bucket(&p)?;
                    all.extend(rows);
                }
            }
        }
        all.sort_by(|a, b| a.child_id.as_ref().cmp(b.child_id.as_ref()));
        write_bucket(&out, &all)?;
        Ok(())
    })?;

    // Consolidate needs-parent-by-parent.
    (0u16..=255).into_par_iter().try_for_each(|prefix_idx| -> anyhow::Result<()> {
        let prefix = prefix_idx as u8;
        let out = pass_b_consolidated_needs_parent_path(scratch, prefix);
        if out.exists() {
            return Ok(());
        }
        let mut all: Vec<NeedsParentRow> = Vec::new();
        for (w, segs) in segs_per_worker.iter().enumerate() {
            for s in 0..*segs {
                let p = pass_b_seg_needs_parent_path(scratch, w, s, prefix);
                if p.exists() {
                    let rows: Vec<NeedsParentRow> = read_bucket(&p)?;
                    all.extend(rows);
                }
            }
        }
        all.sort_by(|a, b| a.parent_receipt_id.as_ref().cmp(b.parent_receipt_id.as_ref()));
        write_bucket(&out, &all)?;
        Ok(())
    })?;

    Ok(())
}

// ===== Pass C: resolve parent predecessors =====

fn pass_c_resolve_parents(opts: &BucketEtlOptions) -> anyhow::Result<(u64, u64)> {
    let scratch = &opts.scratch_dir;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(opts.num_threads.max(1))
        .build()
        .context("build pass C thread pool")?;

    // Each YY worker writes its per-ZZ output into a global locked buffer; we
    // serialize per-ZZ in a final consolidation step. For the prototype this
    // keeps the file-descriptor count bounded.
    let zz_buffers: Vec<Mutex<Vec<ReceiptOriginDemandRow>>> =
        (0..256).map(|_| Mutex::new(Vec::new())).collect();
    let missing_total: Mutex<u64> = Mutex::new(0);
    let total_rows: Mutex<u64> = Mutex::new(0);

    pool.install(|| {
        (0u16..=255).into_par_iter().try_for_each(|yy| -> anyhow::Result<()> {
            let yy = yy as u8;
            let extract_path = pass_a_path(scratch, yy);
            let needs_parent_path = pass_b_consolidated_needs_parent_path(scratch, yy);
            let extract_rows: Vec<ReceiptExtractRow> = read_bucket(&extract_path)?;
            // Build hashmap for parent lookup.
            let map: HashMap<CryptoHash, &ReceiptExtractRow> =
                extract_rows.iter().map(|r| (r.receipt_id, r)).collect();

            let needs_parent_rows: Vec<NeedsParentRow> = read_bucket(&needs_parent_path)?;
            let mut local_missing: u64 = 0;
            let mut local_emitted: u64 = 0;
            // Local per-ZZ buffer to minimize lock contention.
            let mut local_zz: Vec<Vec<ReceiptOriginDemandRow>> =
                (0..256).map(|_| Vec::new()).collect();

            for np in needs_parent_rows {
                match map.get(&np.parent_receipt_id) {
                    Some(extract) => {
                        let zz = np.child_id.as_ref()[0] as usize;
                        local_zz[zz].push(ReceiptOriginDemandRow {
                            child_id: np.child_id,
                            shard_id: np.shard_id,
                            parent_receipt_id: np.parent_receipt_id,
                            parent_predecessor_id: extract.predecessor_id.clone(),
                        });
                        local_emitted += 1;
                    }
                    None => {
                        local_missing += 1;
                    }
                }
            }

            for (zz, rows) in local_zz.into_iter().enumerate() {
                if !rows.is_empty() {
                    zz_buffers[zz].lock().extend(rows);
                }
            }
            *missing_total.lock() += local_missing;
            *total_rows.lock() += local_emitted;
            Ok(())
        })
    })?;

    // Sort + flush per-ZZ.
    (0u16..=255).into_par_iter().try_for_each(|zz| -> anyhow::Result<()> {
        let zz = zz as u8;
        let out = pass_c_consolidated_path(scratch, zz);
        if out.exists() {
            return Ok(());
        }
        let mut rows = std::mem::take(&mut *zz_buffers[zz as usize].lock());
        rows.sort_by(|a, b| a.child_id.as_ref().cmp(b.child_id.as_ref()));
        write_bucket(&out, &rows)?;
        Ok(())
    })?;

    let total = *total_rows.lock();
    let missing = *missing_total.lock();
    Ok((total, missing))
}

// ===== Pass D: final per-prefix join =====

fn pass_d_final_join(opts: &BucketEtlOptions) -> anyhow::Result<(u64, u64)> {
    let scratch = &opts.scratch_dir;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(opts.num_threads.max(1))
        .build()
        .context("build pass D thread pool")?;

    let total_rows: Mutex<u64> = Mutex::new(0);
    let missing_children: Mutex<u64> = Mutex::new(0);

    pool.install(|| {
        (0u16..=255).into_par_iter().try_for_each(|prefix_idx| -> anyhow::Result<()> {
            let prefix = prefix_idx as u8;
            let out_path = pass_d_output_path(scratch, prefix);
            if out_path.exists() {
                return Ok(());
            }

            // Load receipts extract for this prefix; build a child_id → receiver_id map.
            let extract_rows: Vec<ReceiptExtractRow> = read_bucket(&pass_a_path(scratch, prefix))?;
            let receiver_map: HashMap<CryptoHash, AccountId> =
                extract_rows.into_iter().map(|r| (r.receipt_id, r.receiver_id)).collect();

            let mut output: Vec<OutputRow> = Vec::new();
            let mut local_missing: u64 = 0;

            // Tx-origin demands (Pass B).
            let demand_b: Vec<TxOriginDemandRow> =
                read_bucket(&pass_b_consolidated_demand_path(scratch, prefix))?;
            for d in demand_b {
                let receiver = match receiver_map.get(&d.child_id) {
                    Some(r) => r.clone(),
                    None => {
                        local_missing += 1;
                        continue;
                    }
                };
                let info = build_receipt_to_tx_info(BuiltOriginInputs {
                    outcome_id: d.tx_hash,
                    shard_id: d.shard_id,
                    child_receiver_id: receiver,
                    origin: BuiltOrigin::FromTransaction { sender_id: d.sender_account_id },
                });
                output.push(OutputRow { child_id: d.child_id, info });
            }

            // Receipt-origin demands (Pass C resolved).
            let demand_c: Vec<ReceiptOriginDemandRow> =
                read_bucket(&pass_c_consolidated_path(scratch, prefix))?;
            for d in demand_c {
                let receiver = match receiver_map.get(&d.child_id) {
                    Some(r) => r.clone(),
                    None => {
                        local_missing += 1;
                        continue;
                    }
                };
                let info = build_receipt_to_tx_info(BuiltOriginInputs {
                    outcome_id: d.parent_receipt_id,
                    shard_id: d.shard_id,
                    child_receiver_id: receiver,
                    origin: BuiltOrigin::FromReceipt {
                        parent_predecessor_id: d.parent_predecessor_id,
                    },
                });
                output.push(OutputRow { child_id: d.child_id, info });
            }

            let validated = finalize_output_bucket(output)?;
            let count = validated.len() as u64;
            write_bucket(&out_path, &validated)?;

            *total_rows.lock() += count;
            *missing_children.lock() += local_missing;
            Ok(())
        })
    })?;

    let total = *total_rows.lock();
    let missing = *missing_children.lock();
    Ok((total, missing))
}

// ===== Output-mode handlers =====

fn write_outputs_to_store(
    storage: &BackfillStorage,
    opts: &BucketEtlOptions,
) -> anyhow::Result<()> {
    let scratch = &opts.scratch_dir;
    for prefix_idx in 0..256u16 {
        let prefix = prefix_idx as u8;
        let path = pass_d_output_path(scratch, prefix);
        if !path.exists() {
            continue;
        }
        let rows: Vec<OutputRow> = read_bucket(&path)?;
        if rows.is_empty() {
            continue;
        }
        let mut update = storage.write_store.store_update();
        for row in rows {
            let value = borsh::to_vec(&row.info).context("serialize ReceiptToTxInfo for write")?;
            update.insert(DBCol::ReceiptToTx, row.child_id.as_ref().to_vec(), value);
        }
        update.commit();
    }
    Ok(())
}

pub fn compare_against_actor(
    chain_store: &ChainStore,
    opts: &BucketEtlOptions,
    from_height: BlockHeight,
    to_height: BlockHeight,
) -> anyhow::Result<u64> {
    let scratch = &opts.scratch_dir;

    // Run process_height over the range and collect entries.
    let mut actor_entries: HashMap<CryptoHash, Vec<u8>> = HashMap::new();
    for h in from_height..=to_height {
        let result = process_height(chain_store, h)?;
        if let Some(hr) = result {
            for (child_id, info) in hr.entries {
                let bytes = borsh::to_vec(&info).context("serialize actor entry")?;
                if let Some(prev) = actor_entries.insert(child_id, bytes.clone()) {
                    // Same key seen at multiple heights: must agree (otherwise
                    // the actor itself would diverge).
                    if prev != bytes {
                        bail!(
                            "actor produced divergent values for receipt {} across heights",
                            child_id
                        );
                    }
                }
            }
        }
    }

    // Read bucket-ETL outputs.
    let mut etl_entries: HashMap<CryptoHash, Vec<u8>> = HashMap::new();
    for prefix_idx in 0..256u16 {
        let prefix = prefix_idx as u8;
        let path = pass_d_output_path(scratch, prefix);
        if !path.exists() {
            continue;
        }
        let rows: Vec<OutputRow> = read_bucket(&path)?;
        for row in rows {
            let bytes = borsh::to_vec(&row.info).context("serialize etl entry")?;
            etl_entries.insert(row.child_id, bytes);
        }
    }

    let mut divergences: Vec<(CryptoHash, &'static str)> = Vec::new();
    let mut printed = 0usize;

    // Keys present in actor but missing or different in ETL.
    for (k, v) in &actor_entries {
        match etl_entries.get(k) {
            None => {
                if printed < 10 {
                    tracing::error!(receipt = %k, "compare: present in actor, missing in bucket-ETL");
                    printed += 1;
                }
                divergences.push((*k, "missing_in_etl"));
            }
            Some(ev) if ev != v => {
                if printed < 10 {
                    tracing::error!(
                        receipt = %k,
                        "compare: value divergence between actor and bucket-ETL"
                    );
                    printed += 1;
                }
                divergences.push((*k, "value_diff"));
            }
            _ => {}
        }
    }
    // Keys present in ETL but missing in actor.
    for k in etl_entries.keys() {
        if !actor_entries.contains_key(k) {
            if printed < 10 {
                tracing::error!(receipt = %k, "compare: present in bucket-ETL, missing in actor");
                printed += 1;
            }
            divergences.push((*k, "missing_in_actor"));
        }
    }
    Ok(divergences.len() as u64)
}

// ===== Top-level driver =====

pub fn run_bucket_etl(
    chain_store: &ChainStore,
    storage: &BackfillStorage,
    opts: BucketEtlOptions,
) -> anyhow::Result<BucketEtlStats> {
    fs::create_dir_all(&opts.scratch_dir)
        .with_context(|| format!("mkdir {}", opts.scratch_dir.display()))?;

    let mut stats = BucketEtlStats::default();

    // Resolve the height range *before* writing the options-hash so the
    // fingerprint pins to concrete heights. Otherwise a no-arg run that
    // resumes after the chain head moves would match the previous hash and
    // silently skip the new heights via `STAGE_B.done`.
    let genesis_height = chain_store.get_genesis_height();
    let head_height = chain_store.head().context("get chain head")?.height;
    let from = opts.from_height.unwrap_or(genesis_height).max(genesis_height);
    let to = opts.to_height.unwrap_or(head_height).min(head_height);
    if from > to {
        tracing::info!(from, to, "bucket-ETL: empty height range");
        return Ok(stats);
    }
    check_or_write_options_hash(
        &opts.scratch_dir,
        from,
        to,
        opts.num_threads,
        opts.marker_block_interval,
    )?;

    // Pass A.
    if !stage_done(&opts.scratch_dir, STAGE_A) {
        tracing::info!("bucket-ETL: starting Pass A (receipts extract)");
        let rows = pass_a_receipts_extract(storage, &opts)?;
        stats.pass_a_rows = rows;
        tracing::info!(rows, "bucket-ETL: Pass A done");
    } else {
        tracing::info!("bucket-ETL: Pass A already complete, skipping");
        // Recover row count for stats.
        for prefix_idx in 0..256u16 {
            let p = pass_a_path(&opts.scratch_dir, prefix_idx as u8);
            if p.exists() {
                let rows: Vec<ReceiptExtractRow> = read_bucket(&p)?;
                stats.pass_a_rows += rows.len() as u64;
            }
        }
    }

    // Pass B.
    if !stage_done(&opts.scratch_dir, STAGE_B) {
        tracing::info!(from, to, "bucket-ETL: starting Pass B (block walk)");
        let (b_stats, _) = pass_b_block_walk(chain_store, &opts, from, to)?;
        stats.pass_b_tx_origin_rows = b_stats.pass_b_tx_origin_rows;
        stats.pass_b_needs_parent_rows = b_stats.pass_b_needs_parent_rows;
        stats.blocks_processed = b_stats.blocks_processed;
        stats.heights_skipped = b_stats.heights_skipped;
        stats.missing_outcomes = b_stats.missing_outcomes;
        // Consolidate per-worker × per-segment files.
        pass_b_consolidate(&opts.scratch_dir, opts.num_threads.max(1))?;
        mark_stage_done(&opts.scratch_dir, STAGE_B)?;
        tracing::info!(
            tx_origin = stats.pass_b_tx_origin_rows,
            needs_parent = stats.pass_b_needs_parent_rows,
            blocks_processed = stats.blocks_processed,
            "bucket-ETL: Pass B done"
        );
    } else {
        tracing::info!("bucket-ETL: Pass B already complete, skipping");
    }

    // Pass C.
    if !stage_done(&opts.scratch_dir, STAGE_C) {
        tracing::info!("bucket-ETL: starting Pass C (parent resolution)");
        let (rows, missing) = pass_c_resolve_parents(&opts)?;
        stats.pass_c_rows = rows;
        stats.missing_parent_receipts = missing;
        mark_stage_done(&opts.scratch_dir, STAGE_C)?;
        tracing::info!(rows, missing_parent_receipts = missing, "bucket-ETL: Pass C done");
    } else {
        tracing::info!("bucket-ETL: Pass C already complete, skipping");
    }

    // Pass D.
    if !stage_done(&opts.scratch_dir, STAGE_D) {
        tracing::info!("bucket-ETL: starting Pass D (final join + validate)");
        let (rows, missing) = pass_d_final_join(&opts)?;
        stats.pass_d_rows = rows;
        stats.missing_child_receipts = missing;
        mark_stage_done(&opts.scratch_dir, STAGE_D)?;
        tracing::info!(rows, missing_child_receipts = missing, "bucket-ETL: Pass D done");
    } else {
        tracing::info!("bucket-ETL: Pass D already complete, skipping");
    }

    // Output mode dispatch.
    match &opts.output_mode {
        BucketEtlOutputMode::MeasureOnly => {
            tracing::info!("bucket-ETL: measure-only mode, no DB writes");
        }
        BucketEtlOutputMode::WriteToStore => {
            tracing::info!(
                "bucket-ETL: WriteToStore (test-only) — writing to BackfillStorage.write_store"
            );
            write_outputs_to_store(storage, &opts)?;
        }
        BucketEtlOutputMode::CompareAgainstActor { from_height, to_height } => {
            tracing::info!(
                from = from_height,
                to = to_height,
                "bucket-ETL: comparing against actor over range"
            );
            stats.compare_divergences =
                compare_against_actor(chain_store, &opts, *from_height, *to_height)?;
            if stats.compare_divergences > 0 {
                tracing::error!(
                    divergences = stats.compare_divergences,
                    "bucket-ETL: compare-mode divergences detected"
                );
            } else {
                tracing::info!("bucket-ETL: compare-mode clean (zero divergences)");
            }
        }
    }

    Ok(stats)
}
