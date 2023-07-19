use clap::Parser;
use indicatif::{ProgressBar, ProgressIterator};
use std::collections::BTreeMap;
use std::fmt::{Display, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::ValueRef;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use near_store::flat::store_helper::iter_flat_state_entries;
use near_store::{Store, TrieStorage};

use crate::utils::open_rocksdb;

#[derive(Parser)]
pub(crate) struct StatePerfCommand {
    /// Number of requests to use for the performance evaluation.
    /// Increasing this value results in more precise measurements, but longer test execution.
    #[arg(short, long, default_value_t = 10000)]
    samples: usize,

    /// Number of requests to use for the database warmup.
    /// Those requests will be excluded from the measurements.
    #[arg(short, long, default_value_t = 1000)]
    warmup_samples: usize,
}

impl StatePerfCommand {
    pub(crate) fn run(&self, home: &Path) -> anyhow::Result<()> {
        let rocksdb = Arc::new(open_rocksdb(home)?);
        let store = near_store::NodeStorage::new(rocksdb).get_hot_store();
        eprintln!("Start State perf test");
        let mut perf_context = PerfContext::new();
        let total_samples = self.warmup_samples + self.samples;
        for (sample_i, (shard_uid, value_ref)) in
            generate_state_requests(store.clone(), total_samples).into_iter().enumerate().progress()
        {
            let trie_storage = near_store::TrieDBStorage::new(store.clone(), shard_uid);
            let include_sample = sample_i >= self.warmup_samples;
            if include_sample {
                perf_context.reset();
            }
            trie_storage.retrieve_raw_bytes(&value_ref.hash).unwrap();
            if include_sample {
                perf_context.record();
            }
        }
        eprintln!("Finished State perf test");
        println!("{}", perf_context.format());
        Ok(())
    }
}

struct PerfContext {
    rocksdb_context: rocksdb::perf::PerfContext,
    start: Instant,
    measurements_per_block_reads: BTreeMap<usize, Measurements>,
    measurements_overall: Measurements,
}

#[derive(Default)]
struct Measurements {
    samples: usize,
    total_observed_latency: Duration,
    total_read_block_latency: Duration,
    samples_with_merge: usize,
}

impl Measurements {
    fn record(
        &mut self,
        observed_latency: Duration,
        read_block_latency: Duration,
        has_merge: bool,
    ) {
        self.samples += 1;
        self.total_observed_latency += observed_latency;
        self.total_read_block_latency += read_block_latency;
        if has_merge {
            self.samples_with_merge += 1;
        }
    }

    fn avg_observed_latency(&self) -> Duration {
        self.total_observed_latency / (self.samples as u32)
    }

    fn avg_read_block_latency(&self) -> Duration {
        self.total_read_block_latency / (self.samples as u32)
    }
}

impl Display for Measurements {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "avg observed_latency: {:?}, block_read_time: {:?}, samples with merge: {}",
            self.avg_observed_latency(),
            self.avg_read_block_latency(),
            format_samples(self.samples_with_merge, self.samples)
        )
    }
}

impl PerfContext {
    fn new() -> Self {
        rocksdb::perf::set_perf_stats(rocksdb::perf::PerfStatsLevel::EnableTime);
        Self {
            rocksdb_context: rocksdb::perf::PerfContext::default(),
            start: Instant::now(),
            measurements_per_block_reads: BTreeMap::new(),
            measurements_overall: Measurements::default(),
        }
    }

    fn reset(&mut self) {
        self.rocksdb_context.reset();
        self.start = Instant::now();
    }

    fn record(&mut self) {
        let observed_latency = self.start.elapsed();
        let block_read_cnt =
            self.rocksdb_context.metric(rocksdb::PerfMetric::BlockReadCount) as usize;
        let read_block_latency =
            Duration::from_nanos(self.rocksdb_context.metric(rocksdb::PerfMetric::BlockReadTime));
        assert!(observed_latency > read_block_latency);
        // This is a hack to check if at least one merge operator was executed during this request,
        // will be replaced by a proper metric after `internal_merge_point_lookup_count` is added to
        // rust-rocksdb
        let has_merge =
            self.rocksdb_context.metric(rocksdb::PerfMetric::MergeOperatorTimeNanos) > 0;
        self.measurements_per_block_reads.entry(block_read_cnt).or_default().record(
            observed_latency,
            read_block_latency,
            has_merge,
        );
        self.measurements_overall.record(observed_latency, read_block_latency, has_merge);
    }

    fn format(&self) -> String {
        let mut ret = String::new();
        writeln!(&mut ret, "overall | {}", self.measurements_overall).unwrap();
        for (&block_read_cnt, measurements) in &self.measurements_per_block_reads {
            writeln!(
                &mut ret,
                "block_read_count: {block_read_cnt}, samples: {}: | {}",
                format_samples(measurements.samples, self.measurements_overall.samples),
                measurements
            )
            .unwrap();
        }
        ret
    }
}

fn generate_state_requests(store: Store, samples: usize) -> Vec<(ShardUId, ValueRef)> {
    eprintln!("Generate {samples} requests to State");
    let shard_uids = ShardLayout::get_simple_nightshade_layout().get_shard_uids();
    let mut ret = Vec::new();
    let progress = ProgressBar::new(samples as u64);
    for &shard_uid in &shard_uids {
        let shard_samples = samples / shard_uids.len();
        let mut keys_read = std::collections::HashSet::new();
        for value_ref in iter_flat_state_entries(shard_uid, &store, None, None)
            .flat_map(|res| res.map(|(_, value)| value.to_value_ref()))
        {
            if value_ref.length > 4096 || !keys_read.insert(value_ref.hash) {
                continue;
            }
            ret.push((shard_uid, value_ref));
            progress.inc(1);
            if keys_read.len() == shard_samples {
                break;
            }
        }
    }
    progress.finish();
    // Shuffle to avoid clustering requests to the same shard
    ret.shuffle(&mut StdRng::seed_from_u64(42));
    eprintln!("Finished requests generation");
    ret
}

fn format_samples(positive: usize, total: usize) -> String {
    format!(
        "{positive} ({:.2}%)",
        if total == 0 { 0.0 } else { 100.0 * positive as f64 / total as f64 }
    )
}
