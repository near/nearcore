use crate::utils::resolve_column;
use anyhow::{Context, bail};
use clap::Parser;
use near_chain_configs::GenesisValidationMode;
use near_store::DBCol;
use nearcore::open_storage;
use std::path::PathBuf;
use std::time::Instant;

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
