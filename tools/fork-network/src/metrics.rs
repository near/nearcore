//! Progress metrics for `neard fork-network`.
//!
//! `amend-access-keys` is a single command that runs for hours (~3h50m on the Jul 24 mainnet
//! fork) with no progress signal other than one log line per commit batch, so operators cannot
//! tell a slow run from a wedged one. These metrics make the run legible: which phase it is in,
//! how far each shard's flat-state scan has got, and how long each commit stage takes.
//!
//! Everything here is read through the `/metrics` endpoint started by [`crate::metrics_server`].

use near_o11y::metrics::{
    Gauge, GaugeVec, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    exponential_buckets, try_create_gauge, try_create_gauge_vec, try_create_histogram_vec,
    try_create_int_counter, try_create_int_counter_vec, try_create_int_gauge,
    try_create_int_gauge_vec,
};
use near_primitives::shard_layout::ShardUId;
use near_primitives::trie_key::col;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// One past the largest column byte, used to normalise a flat-state key into a 0..1 keyspace
/// position. Keys start with the column byte, so without this the position would top out at
/// around 23/256 and a finished scan would read as 9% done.
///
/// Derived from [`col::ALL_COLUMNS_WITH_NAMES`] rather than written down: the column ids are
/// not contiguous (21 is reserved for the `TrieKey::GasKeyNonce` discriminant), so the count
/// and the maximum differ, and a new column must not silently make this too small.
const NUM_TRIE_KEY_COLUMNS: u64 = max_trie_key_column() as u64 + 1;

const fn max_trie_key_column() -> u8 {
    let columns = col::ALL_COLUMNS_WITH_NAMES;
    let mut max = 0;
    let mut i = 0;
    while i < columns.len() {
        if columns[i].0 > max {
            max = columns[i].0;
        }
        i += 1;
    }
    max
}

/// How many keys to scan between progress reports. Reporting is deliberately coarse: the
/// counters below are incremented per key, but the position gauge and the progress log line
/// are only updated this often.
///
/// The log line matters as much as the gauge — layer 1 of the image-creation progress signal
/// is a `mtime` poll on the neard log file, so a pass that scans silently for an hour reads as
/// wedged. Kept small enough that even a slow shard keeps the log ticking every few minutes;
/// a whole mainnet fork emits a few thousand of these lines, which is nothing.
pub(crate) const SCAN_PROGRESS_INTERVAL: u64 = 250_000;

/// Values of the `near_fork_network_phase` gauge. Process-wide; each subcommand only visits a
/// subset of these.
#[derive(Clone, Copy)]
#[repr(i64)]
pub(crate) enum Phase {
    Idle = 0,
    /// Checkpointing the hot store into `data/fork-snapshot`.
    Snapshot = 1,
    /// Advancing flat heads to a common block and persisting the fork parameters.
    WriteForkInfo = 2,
    /// Dropping DB columns the forked chain doesn't need.
    ClearColumns = 3,
    LoadMemtries = 4,
    /// Rewriting every shard's state; see `near_fork_network_shard_phase` for per-shard detail.
    PrepareState = 5,
    BandwidthScheduler = 6,
    DelayedReceipts = 7,
    FinalizeState = 8,
    AddValidatorAccounts = 9,
    AddUserAccounts = 10,
    WriteGenesis = 11,
    Reset = 12,
    Done = 13,
}

static PHASE: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_fork_network_phase",
        "Current phase of the running fork-network subcommand, enum-encoded: 0 - idle, \
         1 - snapshot, 2 - write fork info, 3 - clear columns, 4 - load memtries, \
         5 - prepare state, 6 - bandwidth scheduler, 7 - delayed receipts, 8 - finalize state, \
         9 - add validator accounts, 10 - add user accounts, 11 - write genesis, 12 - reset, \
         13 - done",
    )
    .unwrap()
});

static PHASE_STARTED: LazyLock<Gauge> = LazyLock::new(|| {
    try_create_gauge(
        "near_fork_network_phase_started_seconds",
        "Unix time at which the current phase started. Together with the phase gauge this \
         gives the time in phase without needing to hold history.",
    )
    .unwrap()
});

/// Sets the process-wide phase and stamps its start time.
pub(crate) fn set_phase(phase: Phase) {
    PHASE_STARTED.set(unix_time_seconds());
    PHASE.set(phase as i64);
}

/// Registers the phase gauges at their initial values, so the series exist from the moment the
/// process starts serving `/metrics` rather than appearing partway through a multi-hour run.
pub(crate) fn init() {
    set_phase(Phase::Idle);
    USER_ACCOUNTS_CREATED.reset();
    USER_ACCOUNTS_EXPECTED.set(0);
}

fn unix_time_seconds() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or_default()
}

/// Values of the `near_fork_network_shard_phase` gauge.
#[derive(Clone, Copy)]
#[repr(i64)]
pub(crate) enum ShardPhase {
    Idle = 0,
    /// Full flat-state scan rewriting accounts, access keys, receipts and contract data.
    Pass1 = 1,
    /// Second full flat-state scan, adding a full access key to accounts that lack one.
    Pass2 = 2,
    Done = 3,
}

static SHARD_PHASE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_fork_network_shard_phase",
        "Per-shard phase of prepare_shard_state, enum-encoded: 0 - idle, 1 - pass 1 \
         (rewrite state), 2 - pass 2 (add missing full access keys), 3 - done. Shards run in \
         parallel, so they do not advance together.",
        &["shard_uid"],
    )
    .unwrap()
});

static KEYS_SCANNED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_fork_network_keys_scanned_total",
        "Flat-state keys read so far, by shard and scan pass",
        &["shard_uid", "pass"],
    )
    .unwrap()
});

static KEYS_EXPECTED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    try_create_int_gauge_vec(
        "near_fork_network_keys_expected",
        "Number of flat-state keys a shard holds, as counted by pass 1. Zero until pass 1 \
         finishes. Pass 1 also writes to flat state, so this is close to but not exactly the \
         number pass 2 will scan.",
        &["shard_uid"],
    )
    .unwrap()
});

static POSITION_ESTIMATE: LazyLock<GaugeVec> = LazyLock::new(|| {
    try_create_gauge_vec(
        "near_fork_network_position_estimate",
        "Estimated fraction of a shard's flat-state scan completed, 0..1. Pass 2 divides by \
         the key count pass 1 measured and can read slightly above 1. Pass 1 has no known \
         total and instead uses the current key's position in the ordered keyspace, which is \
         monotone but not proportional to time: keys are far denser in some trie columns than \
         others.",
        &["shard_uid", "pass"],
    )
    .unwrap()
});

static RECORDS_PARSED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_fork_network_records_parsed_total",
        "Flat-state entries pass 1 decoded into a StateRecord and rewrote",
        &["shard_uid"],
    )
    .unwrap()
});

static RECORDS_NOT_PARSED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_fork_network_records_not_parsed_total",
        "Flat-state entries pass 1 could not decode into a StateRecord and skipped",
        &["shard_uid"],
    )
    .unwrap()
});

static REF_VALUES_RETRIEVED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_fork_network_ref_values_retrieved_total",
        "Values pass 1 had to fetch from the State column because flat state only held a ref. \
         These are random reads and dominate the cost of the scan.",
        &["shard_uid"],
    )
    .unwrap()
});

static ACCESS_KEYS_ADDED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_fork_network_access_keys_added_total",
        "Full access keys pass 2 added to accounts that had none",
        &["shard_uid"],
    )
    .unwrap()
});

static KEYS_WRITTEN: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_fork_network_keys_written_total",
        "Trie keys written or removed by committed batches, by shard",
        &["shard_uid"],
    )
    .unwrap()
});

static BATCHES_COMMITTED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    try_create_int_counter_vec(
        "near_fork_network_batches_committed_total",
        "Non-empty state update batches committed, by shard",
        &["shard_uid"],
    )
    .unwrap()
});

static COMMIT_STAGE_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    try_create_histogram_vec(
        "near_fork_network_commit_stage_duration_seconds",
        "Time spent in each stage of committing one batch of state updates for a shard. \
         `store_commit` is the RocksDB write and is the stage that stalls under compaction \
         pressure.",
        &["shard_uid", "stage"],
        // 10ms .. ~5.5 minutes: batch commits are normally seconds, and the point of the
        // metric is to catch the pathological tail.
        Some(exponential_buckets(0.01, 2.0, 16).unwrap()),
    )
    .unwrap()
});

static USER_ACCOUNTS_CREATED: LazyLock<IntCounter> = LazyLock::new(|| {
    try_create_int_counter(
        "near_fork_network_user_accounts_created_total",
        "Benchmark user accounts written to state by set-validators",
    )
    .unwrap()
});

static USER_ACCOUNTS_EXPECTED: LazyLock<IntGauge> = LazyLock::new(|| {
    try_create_int_gauge(
        "near_fork_network_user_accounts_expected",
        "Benchmark user accounts set-validators will write in total. Unlike the flat-state \
         scans this total is known up front, so the ratio is a true percentage.",
    )
    .unwrap()
});

/// Handles for one shard's scan of flat state. Every label lookup happens here, at
/// construction, and never in the scan loop — these loops run for billions of iterations.
pub(crate) struct ScanMetrics {
    keys_scanned: IntCounter,
    position: Gauge,
    /// Number of keys the scan expects to read, when known. Pass 2 knows it from pass 1;
    /// pass 1 does not, and falls back to a keyspace position estimate.
    total: Option<u64>,
}

impl ScanMetrics {
    /// Records one key read. Called per iteration, so it must stay a single atomic add.
    pub(crate) fn key_scanned(&self) {
        self.keys_scanned.inc();
    }

    /// Publishes how far the scan has got and returns it, so the caller can log the same number
    /// the gauge carries — the log is the only progress signal available over SSH. `key` is the
    /// key just read and `scanned` the number of keys read so far. Called every
    /// [`SCAN_PROGRESS_INTERVAL`] keys, not per key.
    pub(crate) fn report_position(&self, scanned: u64, key: &[u8]) -> f64 {
        let position = match self.total {
            Some(total) if total > 0 => scanned as f64 / total as f64,
            _ => keyspace_position(key),
        };
        self.position.set(position);
        position
    }

    /// Marks the scan as finished, so a pass that stops just short of the last key does not
    /// leave the gauge at 0.97 forever.
    pub(crate) fn report_complete(&self) {
        self.position.set(1.0);
    }
}

/// Monotone 0..1 position of `key` in a shard's ordered flat-state keyspace.
///
/// Flat-state keys within a shard are `column_byte || trie_key` and are iterated in byte
/// order, so the leading bytes locate the scan in the keyspace without knowing the total. It
/// is monotone but not proportional to time — `CONTRACT_DATA` holds far more keys than the
/// columns before it — so it is a progress bar, not an ETA.
fn keyspace_position(key: &[u8]) -> f64 {
    let mut prefix = [0u8; 8];
    let len = key.len().min(prefix.len());
    prefix[..len].copy_from_slice(&key[..len]);
    // Normalise by the prefix value a key one past the last column would have, so a full scan
    // spans roughly the whole 0..1 range instead of the bottom tenth of it.
    let max = NUM_TRIE_KEY_COLUMNS as f64 * (1u64 << 56) as f64;
    (u64::from_be_bytes(prefix) as f64 / max).min(1.0)
}

/// All of one shard's `prepare_shard_state` metrics, with label values pre-bound.
pub(crate) struct ShardMetrics {
    phase: IntGauge,
    keys_expected: IntGauge,
    pass1: ScanMetrics,
    pass2: ScanMetrics,
    records_parsed: IntCounter,
    records_not_parsed: IntCounter,
    ref_values_retrieved: IntCounter,
    access_keys_added: IntCounter,
}

impl ShardMetrics {
    pub(crate) fn new(shard_uid: ShardUId) -> Self {
        let shard = shard_uid.to_string();
        let scan = |pass: &str| ScanMetrics {
            keys_scanned: KEYS_SCANNED.with_label_values(&[shard.as_str(), pass]),
            position: POSITION_ESTIMATE.with_label_values(&[shard.as_str(), pass]),
            total: None,
        };
        let metrics = Self {
            phase: SHARD_PHASE.with_label_values(&[&shard]),
            keys_expected: KEYS_EXPECTED.with_label_values(&[&shard]),
            pass1: scan("1"),
            pass2: scan("2"),
            records_parsed: RECORDS_PARSED.with_label_values(&[&shard]),
            records_not_parsed: RECORDS_NOT_PARSED.with_label_values(&[&shard]),
            ref_values_retrieved: REF_VALUES_RETRIEVED.with_label_values(&[&shard]),
            access_keys_added: ACCESS_KEYS_ADDED.with_label_values(&[&shard]),
        };
        // Zero-initialise so every series exists before the shard starts rewriting state.
        metrics.set_phase(ShardPhase::Idle);
        metrics.keys_expected.set(0);
        metrics.pass1.position.set(0.0);
        metrics.pass2.position.set(0.0);
        // These two are incremented from the commit path rather than held here; touching them
        // creates the series at zero so a rate() covers the very first commit.
        KEYS_WRITTEN.with_label_values(&[shard.as_str()]);
        BATCHES_COMMITTED.with_label_values(&[shard.as_str()]);
        metrics
    }

    pub(crate) fn set_phase(&self, phase: ShardPhase) {
        self.phase.set(phase as i64);
    }

    pub(crate) fn pass1(&self) -> &ScanMetrics {
        &self.pass1
    }

    /// Pass 2's scan handles, using the key count pass 1 measured as the denominator.
    pub(crate) fn pass2(&self, keys_expected: u64) -> ScanMetrics {
        ScanMetrics {
            keys_scanned: self.pass2.keys_scanned.clone(),
            position: self.pass2.position.clone(),
            total: Some(keys_expected),
        }
    }

    /// Records the number of keys pass 1 found in this shard.
    pub(crate) fn set_keys_expected(&self, keys_expected: u64) {
        self.keys_expected.set(keys_expected as i64);
    }

    pub(crate) fn record_parsed(&self) {
        self.records_parsed.inc();
    }

    pub(crate) fn record_not_parsed(&self) {
        self.records_not_parsed.inc();
    }

    pub(crate) fn ref_value_retrieved(&self) {
        self.ref_values_retrieved.inc();
    }

    pub(crate) fn access_key_added(&self) {
        self.access_keys_added.inc();
    }
}

/// Creates every per-shard series at zero before the shards start work, so a dashboard shows
/// all shards from t=0 instead of one appearing as each gets scheduled.
pub(crate) fn init_shards(shard_uids: &[ShardUId]) {
    for shard_uid in shard_uids {
        ShardMetrics::new(*shard_uid);
    }
}

/// Timers for the stages of one shard's batch commit. These are the four writes most likely to
/// stall on RocksDB, and none of them was timed before.
pub(crate) struct CommitStages {
    /// Time spent waiting for the shard's update-state lock, which every shard's rayon thread
    /// can contend for. Counted inside `total`.
    pub(crate) lock_wait: Histogram,
    pub(crate) apply_to_flat_state: Histogram,
    pub(crate) trie_update: Histogram,
    pub(crate) apply_all: Histogram,
    pub(crate) apply_memtrie_changes: Histogram,
    pub(crate) store_commit: Histogram,
    pub(crate) total: Histogram,
}

impl CommitStages {
    pub(crate) fn new(shard_uid: ShardUId) -> Self {
        let shard = shard_uid.to_string();
        let stage = |stage: &str| COMMIT_STAGE_DURATION.with_label_values(&[shard.as_str(), stage]);
        Self {
            lock_wait: stage("lock_wait"),
            apply_to_flat_state: stage("apply_to_flat_state"),
            trie_update: stage("trie_update"),
            apply_all: stage("apply_all"),
            apply_memtrie_changes: stage("apply_memtrie_changes"),
            store_commit: stage("store_commit"),
            total: stage("total"),
        }
    }
}

/// Records a committed batch of `num_updates` trie keys for `shard_uid`.
pub(crate) fn batch_committed(shard_uid: ShardUId, num_updates: usize) {
    let shard = shard_uid.to_string();
    KEYS_WRITTEN.with_label_values(&[&shard]).inc_by(num_updates as u64);
    BATCHES_COMMITTED.with_label_values(&[&shard]).inc();
}

pub(crate) fn set_user_accounts_expected(total: u64) {
    USER_ACCOUNTS_EXPECTED.set(total as i64);
}

pub(crate) fn user_account_created() {
    USER_ACCOUNTS_CREATED.inc();
}

#[cfg(test)]
mod tests {
    use super::{NUM_TRIE_KEY_COLUMNS, keyspace_position, max_trie_key_column};
    use near_primitives::trie_key::col;

    /// Guards the normalisation constant against a new trie key column being added. Getting
    /// this too small makes a scan of the last column report 100% while it is still running.
    #[test]
    fn normalisation_covers_every_column() {
        for (column, name) in col::ALL_COLUMNS_WITH_NAMES {
            assert!(
                (column as u64) < NUM_TRIE_KEY_COLUMNS,
                "column {name} ({column}) is not below NUM_TRIE_KEY_COLUMNS \
                 ({NUM_TRIE_KEY_COLUMNS})",
            );
        }
    }

    #[test]
    fn keyspace_position_is_monotone_and_spans_the_range() {
        let account_start = keyspace_position(&[col::ACCOUNT]);
        let account_mid = keyspace_position(&[col::ACCOUNT, b'm', b'i', b'd']);
        let data = keyspace_position(&[col::CONTRACT_DATA, b'a']);
        let last = keyspace_position(&[max_trie_key_column(), 0xff, 0xff, 0xff]);

        assert_eq!(account_start, 0.0);
        assert!(account_start < account_mid, "{account_start} < {account_mid}");
        assert!(account_mid < data, "{account_mid} < {data}");
        assert!(data < last, "{data} < {last}");
        // The last column should read as nearly complete rather than a few percent — but
        // strictly below 1.0, or a scan still in its final column reports as finished. This
        // has to stay strict: `keyspace_position` clamps, so `<= 1.0` would always hold.
        assert!(last > 0.9, "{last} > 0.9");
        assert!(last < 1.0, "{last} < 1.0");
    }

    #[test]
    fn keyspace_position_handles_short_keys() {
        assert_eq!(keyspace_position(&[]), 0.0);
        assert!(keyspace_position(&[col::CONTRACT_DATA]) > 0.0);
    }
}
