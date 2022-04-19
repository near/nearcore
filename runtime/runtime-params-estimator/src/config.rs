use near_vm_runner::internal::VMKind;
use std::path::PathBuf;

use crate::rocksdb::RocksDBTestConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GasMetric {
    // If we measure gas in number of executed instructions, must run under simulator.
    ICount,
    // If we measure gas in elapsed time.
    Time,
}

/// Configuration which we use to run measurements.
#[derive(Debug, Clone)]
pub struct Config {
    /// How many warm up iterations per block should we run.
    pub warmup_iters_per_block: usize,
    /// How many iterations per block are we going to try.
    pub iter_per_block: usize,
    /// Total active accounts.
    pub active_accounts: usize,
    /// Number of the transactions in the block.
    pub block_sizes: Vec<usize>,
    /// Where state dump is located in case we need to create a testbed.
    pub state_dump_path: PathBuf,
    /// Metric used for counting.
    pub metric: GasMetric,
    /// VMKind used
    pub vm_kind: VMKind,
    /// When non-none, only the specified costs will be measured.
    pub costs_to_measure: Option<Vec<String>>,
    /// Configuration specific to raw RocksDB tests. Does NOT affect normal tests that use RocksDB through the nearcore interface.
    pub rocksdb_test_config: RocksDBTestConfig,
    /// Print extra details on estimations.
    pub debug: bool,
    /// Print JSON output for estimation results.
    pub json_output: bool,
    /// Clear all OS caches between measured blocks.
    pub drop_os_cache: bool,
}
