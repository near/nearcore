use crate::utils::resolve_column;
use anyhow::{Context, bail};
use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use near_store::{DBCol, Store};
use nearcore::open_storage;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
pub struct ScanThroughputCommand {
    /// Column name (matches DBCol variant string, e.g. "Receipts",
    /// "TransactionResultForBlock", "Transactions").
    #[clap(long)]
    col: String,

    /// Use Store::iter_raw_bytes (skips refcount processing). Default: Store::iter.
    /// Use this for closer-to-physical scan throughput.
    #[clap(long)]
    raw_bytes: bool,

    /// Stop after this many bytes read. Default: scan everything (slow).
    #[clap(long)]
    limit_bytes: Option<bytesize::ByteSize>,

    /// Scan only this first-byte prefix (hex 0x00-0xfe). 0xff rejected
    /// (would overflow upper bound). State column rejected (iter_range panics).
    #[clap(long)]
    prefix_byte: Option<String>,

    /// Number of parallel scan workers. When >1, the first-byte keyspace is
    /// partitioned into N equal chunks and each thread scans its chunk
    /// concurrently. Incompatible with --raw-bytes and --prefix-byte; requires
    /// --limit-bytes (otherwise threads run forever).
    #[clap(long, default_value_t = 1)]
    parallel: usize,
}

impl ScanThroughputCommand {
    pub fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let col = resolve_column(&self.col)?;
        if col == DBCol::State {
            bail!("State column not supported (iter_range panics on it)");
        }

        let near_config =
            nearcore::config::load_config(home, genesis_validation).context("loading config")?;
        let node_storage = open_storage(home, &near_config).context("opening storage")?;

        let store = node_storage.get_cold_store().context("cold store not configured")?;

        if self.parallel > 1 {
            if self.raw_bytes {
                bail!("--raw-bytes incompatible with --parallel > 1");
            }
            if self.prefix_byte.is_some() {
                bail!("--prefix-byte incompatible with --parallel > 1");
            }
            if self.limit_bytes.is_none() {
                bail!("--parallel > 1 requires --limit-bytes (else threads run forever)");
            }
            return self.run_parallel(col, store);
        }

        let limit = self.limit_bytes.map(|b| b.as_u64()).unwrap_or(u64::MAX);

        let prefix_pair = self.prefix_byte.as_deref().map(parse_prefix_byte).transpose()?;

        let iter = match (&prefix_pair, self.raw_bytes) {
            (Some((lower, upper)), false) => {
                store.iter_range(col, Some(lower.as_slice()), Some(upper.as_slice()))
            }
            (None, false) => store.iter(col),
            (Some(_), true) => {
                bail!("--raw-bytes with --prefix-byte not supported here; pick one")
            }
            (None, true) => store.iter_raw_bytes(col),
        };

        let start = Instant::now();
        let mut keys: u64 = 0;
        let mut key_bytes: u64 = 0;
        let mut value_bytes: u64 = 0;
        let mut limit_hit = false;

        for (key, value) in iter {
            keys += 1;
            key_bytes += key.len() as u64;
            value_bytes += value.len() as u64;
            if key_bytes + value_bytes >= limit {
                limit_hit = true;
                break;
            }
        }

        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        let total = key_bytes + value_bytes;
        let mb_s = if secs > 0.0 { (total as f64) / 1_000_000.0 / secs } else { 0.0 };
        let keys_s = if secs > 0.0 { (keys as f64) / secs } else { 0.0 };
        let avg_value = if keys > 0 { value_bytes as f64 / keys as f64 } else { 0.0 };

        println!("column:               {:?}", col);
        println!("raw_bytes:            {}", self.raw_bytes);
        println!("prefix_byte:          {:?}", self.prefix_byte);
        println!("limit_bytes:          {:?}", self.limit_bytes);
        println!("limit_hit:            {}", limit_hit);
        println!("wall_time_s:          {:.2}", secs);
        println!("keys_read:            {}", keys);
        println!("key_bytes:            {}", key_bytes);
        println!("value_bytes:          {}", value_bytes);
        println!("total_bytes_read:     {} ({:.2} GB)", total, total as f64 / 1e9);
        println!("throughput_MB_s:      {:.1}  (logical, post-decompression)", mb_s);
        println!("throughput_keys_s:    {:.0}", keys_s);
        println!("avg_value_bytes:      {:.1}", avg_value);
        println!();
        println!("note: throughput is LOGICAL bytes (post-decompress, post-refcount).");
        println!(
            "      compare with iostat -xz physical disk MB/s to detect compression-bound vs disk-bound."
        );

        Ok(())
    }

    fn run_parallel(&self, col: DBCol, store: Store) -> anyhow::Result<()> {
        let n = self.parallel;
        let total_limit = self.limit_bytes.unwrap().as_u64();
        let per_worker_limit = total_limit / n as u64;

        let store = Arc::new(store);
        let start = Instant::now();

        let handles: Vec<_> = (0..n)
            .map(|i| {
                let lower = vec![((i * 256) / n) as u8];
                let upper: Option<Vec<u8>> =
                    if i == n - 1 { None } else { Some(vec![(((i + 1) * 256) / n) as u8]) };
                let store = store.clone();
                std::thread::spawn(move || {
                    let iter = store.iter_range(col, Some(lower.as_slice()), upper.as_deref());
                    let t0 = Instant::now();
                    let mut keys: u64 = 0;
                    let mut key_bytes: u64 = 0;
                    let mut value_bytes: u64 = 0;
                    let mut limit_hit = false;
                    for (k, v) in iter {
                        keys += 1;
                        key_bytes += k.len() as u64;
                        value_bytes += v.len() as u64;
                        if key_bytes + value_bytes >= per_worker_limit {
                            limit_hit = true;
                            break;
                        }
                    }
                    (i, keys, key_bytes, value_bytes, t0.elapsed(), limit_hit)
                })
            })
            .collect();

        let results: Vec<_> =
            handles.into_iter().map(|h| h.join().expect("worker panicked")).collect();
        let total_wall = start.elapsed();

        let mut agg_keys: u64 = 0;
        let mut agg_bytes: u64 = 0;
        let mut max_thread_wall = Duration::ZERO;
        let mut all_limit_hit = true;

        println!("column:                    {:?}", col);
        println!("parallel_workers:          {}", n);
        println!("per_worker_limit_bytes:    {}", per_worker_limit);
        println!();
        println!("--- per-worker results ---");
        for (i, k, kb, vb, dur, lh) in &results {
            let secs = dur.as_secs_f64();
            let bytes = kb + vb;
            let mb_s = if secs > 0.0 { (bytes as f64) / 1_000_000.0 / secs } else { 0.0 };
            println!(
                "  worker {i}: keys={k} bytes={bytes} ({:.2} GB) wall={:.1}s {:.1} MB/s limit_hit={lh}",
                bytes as f64 / 1e9,
                secs,
                mb_s
            );
            agg_keys += k;
            agg_bytes += bytes;
            if *dur > max_thread_wall {
                max_thread_wall = *dur;
            }
            if !lh {
                all_limit_hit = false;
            }
        }

        let secs = total_wall.as_secs_f64();
        let mb_s = if secs > 0.0 { (agg_bytes as f64) / 1_000_000.0 / secs } else { 0.0 };

        println!();
        println!("--- aggregate ---");
        println!("total_wall_s:              {:.2}", secs);
        println!("max_thread_wall_s:         {:.2}", max_thread_wall.as_secs_f64());
        println!("total_keys_read:           {}", agg_keys);
        println!("total_bytes_read:          {} ({:.2} GB)", agg_bytes, agg_bytes as f64 / 1e9);
        println!("aggregate_throughput_MB_s: {:.1}  (logical, post-decompression)", mb_s);
        println!("all_limit_hit:             {}", all_limit_hit);
        println!();
        println!("note: aggregate throughput is total_bytes_read / total_wall_s.");
        println!(
            "      compare to single-thread baseline; ~Nx scaling means LVM-disk parallelism worked."
        );

        Ok(())
    }
}

/// Parse "0x00"-"0xfe" into (lower, upper) byte vectors. Reject 0xff to avoid
/// overflow in upper bound construction.
fn parse_prefix_byte(s: &str) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let s = s.trim_start_matches("0x").trim_start_matches("0X");
    let byte = u8::from_str_radix(s, 16).context("parsing prefix-byte hex")?;
    if byte == 0xff {
        bail!(
            "prefix-byte 0xff not supported (upper bound would overflow); use a different prefix"
        );
    }
    Ok((vec![byte], vec![byte + 1]))
}
