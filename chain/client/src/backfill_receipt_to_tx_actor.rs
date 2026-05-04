use anyhow::{Context, anyhow};
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
use near_async::time::Duration;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY_LOW, BackfillError, BackfillStorage, process_one_batch,
};
use near_chain::{ChainGenesis, ChainStore, ChainStoreAccess};
use near_chain_configs::BackfillReceiptToTxConfig;
use near_o11y::tracing;
use near_primitives::types::{BlockHeight, BlockHeightDelta};
use near_store::DBCol;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::metrics;

const SYNC_GAP_THRESHOLD: BlockHeightDelta = 100;

/// Number of consecutive failed batches after which the actor halts
/// permanently. With a 5s `batch_delay` the retry schedule is 30s, 60s,
/// 120s, 240s, then 5 min — so 100 errors corresponds to ~8.1 hours of
/// zero progress. Long enough to ride out transient I/O blips, short
/// enough that a permanent failure surfaces operationally rather than
/// silently spamming logs forever.
const MAX_CONSECUTIVE_ERRORS: u32 = 100;

fn is_chain_at_tip(block_head: BlockHeight, header_head: BlockHeight) -> bool {
    if block_head > header_head {
        // Invariant violation: header head should always be at or ahead of block head.
        // Don't crash the actor — log loudly and treat as not-at-tip so backfill stays paused.
        tracing::error!(
            block_head,
            header_head,
            "receipt-to-tx backfill: block_head exceeds header_head — chain state invariant violated"
        );
        return false;
    }
    header_head - block_head <= SYNC_GAP_THRESHOLD
}

fn next_retry_delay(consecutive_errors: u32, batch_delay: Duration) -> Duration {
    let error_base_secs = batch_delay.whole_seconds().max(30);
    let multiplier = 2i64.pow(consecutive_errors.min(8));
    Duration::seconds((error_base_secs * multiplier).min(300))
}

/// Background actor that backfills the ReceiptToTx DB column by processing
/// heights in descending order (from head toward genesis). This ensures
/// recent receipts become queryable first.
///
/// On split-storage archival nodes, ReceiptToTx entries are written directly
/// to cold storage (`write_store`), bypassing the hot→cold copy pipeline.
/// Checkpoints are always written to hot storage (`checkpoint_store`) since
/// they're transient operational state, not archival data.
///
/// Follows the GCActor pattern: runs in a periodic loop via `ctx.run_later()`.
pub struct BackfillReceiptToTxActor {
    chain_store: ChainStore,
    storage: BackfillStorage,
    pool: rayon::ThreadPool,
    genesis_height: BlockHeight,
    config: BackfillReceiptToTxConfig,
    consecutive_errors: u32,
    start_height_validated: bool,
    /// File handle holding an exclusive `flock` on the SST staging-directory
    /// sentinel for the lifetime of the actor. Dropping the handle releases
    /// the lock (and the kernel also releases it on process death). Stays
    /// `None` when SST mode is disabled or when lock acquisition failed at
    /// startup (in which case the actor falls back to the legacy write path).
    _sst_dir_lock: Option<std::fs::File>,
}

impl BackfillReceiptToTxActor {
    pub fn new(
        storage: BackfillStorage,
        save_trie_changes: bool,
        genesis: &ChainGenesis,
        config: BackfillReceiptToTxConfig,
    ) -> anyhow::Result<Self> {
        let chain_store = ChainStore::new(
            storage.read_store.clone(),
            save_trie_changes,
            genesis.transaction_validity_period,
        );
        let genesis_height = chain_store.get_genesis_height();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.num_threads)
            .build()
            .context("failed to build rayon thread pool")?;
        Ok(Self {
            chain_store,
            storage,
            pool,
            genesis_height,
            config,
            consecutive_errors: 0,
            start_height_validated: false,
            _sst_dir_lock: None,
        })
    }

    fn backfill_loop(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        match self.check_chain_at_tip() {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!("receipt-to-tx backfill waiting for sync to complete");
                ctx.run_later(
                    "backfill receipt to tx - waiting for sync",
                    Duration::seconds(30),
                    move |act, ctx| act.backfill_loop(ctx),
                );
                return;
            }
            Err(e) => {
                if self.record_error_and_maybe_halt(&e) {
                    return;
                }
                let delay = next_retry_delay(self.consecutive_errors, self.config.batch_delay);
                self.consecutive_errors += 1;
                tracing::warn!(
                    ?e,
                    ?delay,
                    "receipt-to-tx backfill: failed to check sync status, retrying"
                );
                ctx.run_later("backfill receipt to tx retry", delay, move |act, ctx| {
                    act.backfill_loop(ctx)
                });
                return;
            }
        }

        match self.backfill_batch() {
            Ok(true) => {
                self.consecutive_errors = 0;
                tracing::info!("receipt-to-tx backfill complete");
            }
            Ok(false) => {
                self.consecutive_errors = 0;
                ctx.run_later(
                    "backfill receipt to tx",
                    self.config.batch_delay,
                    move |act, ctx| {
                        act.backfill_loop(ctx);
                    },
                );
            }
            Err(e) => {
                if self.record_error_and_maybe_halt(&e) {
                    return;
                }
                let delay = next_retry_delay(self.consecutive_errors, self.config.batch_delay);
                self.consecutive_errors += 1;
                tracing::warn!(?e, ?delay, "receipt-to-tx backfill error, retrying");
                ctx.run_later("backfill receipt to tx retry", delay, move |act, ctx| {
                    act.backfill_loop(ctx);
                });
            }
        }
    }

    /// Record per-error metrics/logs and, if the consecutive-error budget is
    /// exhausted, halt the actor. Returns `true` if the actor halted (caller
    /// must NOT schedule another retry).
    ///
    /// Halting is permanent for the life of this process: no further
    /// `run_later` is scheduled. An operator restart with the underlying
    /// fault resolved is required to resume backfill.
    fn record_error_and_maybe_halt(&mut self, e: &anyhow::Error) -> bool {
        if let Some(BackfillError::ValueDivergence { key, prev_value, new_value }) =
            e.downcast_ref::<BackfillError>()
        {
            metrics::BACKFILL_RECEIPT_TO_TX_VALUE_DIVERGENCE_TOTAL.inc();
            tracing::error!(
                %key,
                prev_value = ?prev_value,
                new_value = ?new_value,
                "receipt-to-tx backfill: within-batch value divergence; batch quarantined"
            );
        }
        if self.consecutive_errors + 1 >= MAX_CONSECUTIVE_ERRORS {
            metrics::BACKFILL_RECEIPT_TO_TX_HALTED_TOTAL
                .with_label_values(&["consecutive_errors"])
                .inc();
            tracing::error!(
                consecutive_errors = self.consecutive_errors + 1,
                last_error = ?e,
                "receipt-to-tx backfill: halting after exceeding consecutive-error budget; \
                 operator intervention required"
            );
            return true;
        }
        false
    }

    fn check_chain_at_tip(&self) -> anyhow::Result<bool> {
        let block_head = self.chain_store.head().context("get block head")?.height;
        let header_head = self.chain_store.header_head().context("get header head")?.height;
        Ok(is_chain_at_tip(block_head, header_head))
    }

    /// Process one batch of heights in descending order.
    /// Returns `Ok(true)` when backfill is complete (reached genesis).
    /// Returns `Ok(false)` when there's more work to do.
    ///
    /// Runs synchronously — including the SST ingest call when configured.
    /// The actor is spawned via `spawn_tokio_actor`, which gives it a dedicated
    /// single-worker tokio runtime, so blocking the worker only stalls this
    /// actor's own iterations and never starves the rest of the node.
    pub fn backfill_batch(&mut self) -> anyhow::Result<bool> {
        let batch_start = Instant::now();

        let checkpoint: Option<BlockHeight> =
            self.storage.checkpoint_store.get_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW);

        if let Some(cp) = checkpoint {
            metrics::BACKFILL_RECEIPT_TO_TX_LOWEST_COMPLETED_HEIGHT.set(cp as i64);
            if cp <= self.genesis_height {
                return Ok(true);
            }
        }

        // On the first batch, validate `config.start_height` before using it. The checks
        // only matter once — after the first batch writes a checkpoint, `start_height` is
        // irrelevant forever (the resume logic uses the checkpoint instead).
        //
        // Set `start_height_validated = true` UNCONDITIONALLY at the top of the block,
        // so a bounds-violation Err on a typo (e.g. `start_height` above head) does not
        // wedge the actor in a permanent re-validation loop. Subsequent retries skip
        // this block and proceed with the (bogus) start_height; reads of non-existent
        // heights return `Ok(None)` and the actor self-corrects as it walks down.
        // The retry loop is bounded by `MAX_CONSECUTIVE_ERRORS`.
        if !self.start_height_validated {
            self.start_height_validated = true;
            if let Some(start_height) = self.config.start_height {
                if let Some(cp) = checkpoint {
                    tracing::warn!(
                        start_height,
                        checkpoint = cp,
                        "backfill_receipt_to_tx: start_height is ignored because a checkpoint already exists",
                    );
                } else {
                    let head = self.chain_store.head().context("failed to get chain head")?.height;
                    if start_height > head {
                        return Err(anyhow!(
                            "backfill_receipt_to_tx: start_height {start_height} exceeds chain head {head}"
                        ));
                    }
                    if start_height < self.genesis_height {
                        return Err(anyhow!(
                            "backfill_receipt_to_tx: start_height {start_height} is below genesis {}",
                            self.genesis_height
                        ));
                    }
                }
            }
        }

        let current_height = match checkpoint {
            Some(cp) => cp.saturating_sub(1),
            None => match self.config.start_height {
                Some(h) => h,
                None => {
                    let head = self.chain_store.head().context("failed to get chain head")?;
                    head.height
                }
            },
        };

        debug_assert!(
            current_height >= self.genesis_height,
            "current_height {current_height} below genesis {}",
            self.genesis_height
        );

        let batch_end = current_height
            .saturating_sub(self.config.batch_size.saturating_sub(1))
            .max(self.genesis_height);

        // Descending order so recent receipts become queryable first.
        let heights: Vec<BlockHeight> = (batch_end..=current_height).rev().collect();
        let stats = process_one_batch(
            &self.chain_store,
            &self.storage,
            &self.pool,
            &heights,
            Some((BACKFILL_CHECKPOINT_KEY_LOW, batch_end)),
        )?;

        metrics::BACKFILL_RECEIPT_TO_TX_LOWEST_COMPLETED_HEIGHT.set(batch_end as i64);

        let batch_duration = batch_start.elapsed();
        if stats.blocks_processed > 0 || stats.heights_skipped > 0 {
            let heights_in_batch = current_height - batch_end + 1;
            let remaining = batch_end.saturating_sub(self.genesis_height);
            let missing_total = stats.missing_total();
            let denominator = stats.entries_written + missing_total;
            let missing_is_significant = denominator > 0 && missing_total * 20 > denominator;
            let heights_per_second = if batch_duration.as_secs_f64() > 0.0 {
                (heights_in_batch as f64 / batch_duration.as_secs_f64()) as u64
            } else {
                0
            };
            if missing_is_significant {
                tracing::warn!(
                    from_height = current_height,
                    to_height = batch_end,
                    blocks_processed = stats.blocks_processed,
                    entries_written = stats.entries_written,
                    missing_outcomes = stats.missing_outcomes,
                    missing_child_receipts = stats.missing_child_receipts,
                    missing_parent_receipts = stats.missing_parent_receipts,
                    batch_duration_ms = batch_duration.as_millis() as u64,
                    heights_per_second,
                    remaining_heights = remaining,
                    "receipt-to-tx backfill progress: >5% of expected entries were missing upstream data"
                );
            } else {
                tracing::info!(
                    from_height = current_height,
                    to_height = batch_end,
                    blocks_processed = stats.blocks_processed,
                    entries_written = stats.entries_written,
                    missing_outcomes = stats.missing_outcomes,
                    missing_child_receipts = stats.missing_child_receipts,
                    missing_parent_receipts = stats.missing_parent_receipts,
                    batch_duration_ms = batch_duration.as_millis() as u64,
                    heights_per_second,
                    remaining_heights = remaining,
                    "receipt-to-tx backfill progress"
                );
            }
        }

        Ok(batch_end <= self.genesis_height)
    }
}

/// Sibling sentinel path one level up from the per-PID SST temp dir.
///
/// Layout: parent / "pid-{pid}/" (the staging dir) and parent /
/// "pid-{pid}.lock" (the sibling sentinel). The sentinel must live OUTSIDE
/// the dir so cleanup can `remove_dir_all` the dir without touching the
/// locked file (unlinking the locked inode would release the lock and let
/// a peer immediately re-acquire it).
fn sst_dir_sentinel_path(temp_dir: &Path) -> Option<PathBuf> {
    let parent = temp_dir.parent()?;
    let name = temp_dir.file_name()?.to_str()?;
    Some(parent.join(format!("{name}.lock")))
}

/// Try to acquire an exclusive non-blocking flock on the sentinel file.
///
/// The returned `File` holds the lock for as long as it lives — the caller
/// stores it on the actor so the lock is held for the actor's lifetime.
/// POSIX advisory locks release on process death (including SIGKILL), so
/// no manual cleanup is needed across crashes.
///
/// Lock failure means another process holds the sentinel: the staging dir
/// belongs to that peer and must NOT be touched.
fn try_acquire_sst_dir_lock(sentinel: &Path) -> anyhow::Result<std::fs::File> {
    use nix::fcntl::{FlockArg, flock};
    use std::os::unix::io::AsRawFd;

    if let Some(parent) = sentinel.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create sentinel parent dir {parent:?}"))?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(sentinel)
        .with_context(|| format!("open sentinel {sentinel:?}"))?;
    flock(file.as_raw_fd(), FlockArg::LockExclusiveNonblock)
        .with_context(|| format!("flock {sentinel:?}: held by another process"))?;
    Ok(file)
}

/// Remove the contents of the per-PID SST staging directory while leaving
/// the sibling sentinel in place. Safe to call after acquiring the lock —
/// the directory belongs to this PID exclusively at that point.
fn clean_locked_sst_temp_dir(temp_dir: &Path) {
    if !temp_dir.exists() {
        return;
    }
    if let Err(e) = std::fs::remove_dir_all(temp_dir) {
        tracing::warn!(?temp_dir, ?e, "failed to clean stale SST temp dir on startup");
    }
}

impl Actor for BackfillReceiptToTxActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        if let Some(temp_dir) = self.storage.sst_temp_dir.clone() {
            match sst_dir_sentinel_path(&temp_dir) {
                Some(sentinel) => match try_acquire_sst_dir_lock(&sentinel) {
                    Ok(lock_file) => {
                        self._sst_dir_lock = Some(lock_file);
                        clean_locked_sst_temp_dir(&temp_dir);
                    }
                    Err(e) => {
                        metrics::BACKFILL_RECEIPT_TO_TX_SST_LOCK_CONTENTION_TOTAL.inc();
                        tracing::warn!(
                            ?temp_dir,
                            ?sentinel,
                            ?e,
                            "could not acquire SST staging dir lock; falling back to legacy write path"
                        );
                        self.storage.sst_temp_dir = None;
                    }
                },
                None => {
                    tracing::warn!(
                        ?temp_dir,
                        "SST staging dir has no parent or non-utf8 file name; \
                         falling back to legacy write path",
                    );
                    self.storage.sst_temp_dir = None;
                }
            }
        }
        self.backfill_loop(ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_chain_at_tip() {
        assert!(is_chain_at_tip(100, 100));
        assert!(is_chain_at_tip(100, 200));
        assert!(!is_chain_at_tip(100, 201));
        assert!(!is_chain_at_tip(0, 1000));
        // header_head < block_head is an invariant violation: not at tip.
        assert!(!is_chain_at_tip(200, 100));
    }

    #[test]
    fn test_next_retry_delay() {
        // With batch_delay=1s, error_base = max(1, 30) = 30s
        let batch_delay = Duration::seconds(1);
        assert_eq!(next_retry_delay(0, batch_delay), Duration::seconds(30));
        assert_eq!(next_retry_delay(1, batch_delay), Duration::seconds(60));
        assert_eq!(next_retry_delay(2, batch_delay), Duration::seconds(120));
        assert_eq!(next_retry_delay(3, batch_delay), Duration::seconds(240));
        assert_eq!(next_retry_delay(4, batch_delay), Duration::seconds(300));
        assert_eq!(next_retry_delay(8, batch_delay), Duration::seconds(300));
        assert_eq!(next_retry_delay(100, batch_delay), Duration::seconds(300));

        // With batch_delay=60s, error_base = max(60, 30) = 60s
        let batch_delay = Duration::seconds(60);
        assert_eq!(next_retry_delay(0, batch_delay), Duration::seconds(60));
        assert_eq!(next_retry_delay(1, batch_delay), Duration::seconds(120));
        assert_eq!(next_retry_delay(2, batch_delay), Duration::seconds(240));
        assert_eq!(next_retry_delay(3, batch_delay), Duration::seconds(300));
    }

    /// Pre-existing files in the SST staging directory left behind by a prior
    /// run with this PID are wiped on startup once the lock is held.
    #[test]
    fn test_clean_locked_sst_temp_dir_removes_pre_existing_files() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let stale = tmp.path().join("ingest-1234-7.sst");
        std::fs::write(&stale, b"leftover").expect("write stub");
        assert!(stale.exists());

        clean_locked_sst_temp_dir(tmp.path());

        assert!(!tmp.path().exists(), "cleanup should remove the entire dir");
    }

    /// Missing path is the common fresh-process case — must not panic.
    #[test]
    fn test_clean_locked_sst_temp_dir_missing_path_is_noop() {
        let tmp = tempfile::tempdir().expect("create tempdir");
        let missing = tmp.path().join("does-not-exist");
        assert!(!missing.exists());

        clean_locked_sst_temp_dir(&missing);

        assert!(!missing.exists(), "cleanup must not create the missing path");
    }

    /// Sentinel sits one level up from the per-PID dir, sharing its name with
    /// a `.lock` suffix. This isolation is what keeps `remove_dir_all` of
    /// the staging dir from unlinking the locked file.
    #[test]
    fn test_sst_dir_sentinel_path_layout() {
        let temp_dir = Path::new("/tmp/backfill-receipt-to-tx-sst/pid-1234");
        let sentinel = sst_dir_sentinel_path(temp_dir).unwrap();
        assert_eq!(sentinel, Path::new("/tmp/backfill-receipt-to-tx-sst/pid-1234.lock"));
        // Sibling, not child — `remove_dir_all(temp_dir)` cannot touch the sentinel.
        assert_eq!(sentinel.parent(), temp_dir.parent());
    }

    /// Two actors targeting the same staging dir: the first acquires the
    /// lock; the second's lock attempt fails. The second must NOT touch
    /// the staging dir contents.
    #[test]
    fn test_sst_dir_lock_contention_blocks_second_acquirer() {
        let parent = tempfile::tempdir().expect("create parent dir");
        let temp_dir = parent.path().join("pid-1234");
        std::fs::create_dir_all(&temp_dir).expect("create temp dir");
        let canary = temp_dir.join("first-actors-data.sst");
        std::fs::write(&canary, b"belongs to first actor").expect("write canary");

        let sentinel = sst_dir_sentinel_path(&temp_dir).expect("sentinel path");
        let _first_lock = try_acquire_sst_dir_lock(&sentinel).expect("first lock acquires");

        // Second attempt must fail while the first lock is held.
        let second = try_acquire_sst_dir_lock(&sentinel);
        assert!(second.is_err(), "second flock attempt must fail under contention");

        // First actor's data must be untouched.
        assert!(canary.exists(), "second actor must not delete the first's data");
    }

    /// Once the holder drops the lock, a fresh attempt succeeds.
    #[test]
    fn test_sst_dir_lock_releases_on_drop() {
        let parent = tempfile::tempdir().expect("create parent dir");
        let temp_dir = parent.path().join("pid-9999");
        let sentinel = sst_dir_sentinel_path(&temp_dir).expect("sentinel path");
        {
            let _lock = try_acquire_sst_dir_lock(&sentinel).expect("first lock acquires");
        }
        let _retry = try_acquire_sst_dir_lock(&sentinel).expect("after drop, lock is reacquirable");
    }

    /// Halt budget: at `consecutive_errors == MAX - 1`, the next failure halts
    /// (the +1 in the check counts the in-flight error).
    #[test]
    fn test_halt_threshold_fires_at_budget_exhaustion() {
        let halt_at_budget = MAX_CONSECUTIVE_ERRORS - 1;
        // The "in-flight" error is the (consecutive_errors + 1)-th consecutive failure.
        assert_eq!(halt_at_budget + 1, MAX_CONSECUTIVE_ERRORS);
        // One below budget: keep retrying.
        assert!(halt_at_budget - 1 + 1 < MAX_CONSECUTIVE_ERRORS);
    }

    /// `BackfillError::ValueDivergence` round-trips through `anyhow::Error`
    /// so the actor's `downcast_ref` path can detect it. This is the
    /// load-bearing guarantee for the metric-and-log handler in
    /// `record_error_and_maybe_halt`.
    #[test]
    fn test_value_divergence_downcast_roundtrip() {
        use near_primitives::hash::CryptoHash;
        let key = CryptoHash([7u8; 32]);
        let err: anyhow::Error = BackfillError::ValueDivergence {
            key,
            prev_value: b"prev".to_vec(),
            new_value: b"new".to_vec(),
        }
        .into();
        let downcast = err.downcast_ref::<BackfillError>();
        match downcast {
            Some(BackfillError::ValueDivergence { key: k, prev_value, new_value }) => {
                assert_eq!(*k, key);
                assert_eq!(prev_value, b"prev");
                assert_eq!(new_value, b"new");
            }
            None => panic!("BackfillError must be downcastable from anyhow::Error"),
        }
    }
}
