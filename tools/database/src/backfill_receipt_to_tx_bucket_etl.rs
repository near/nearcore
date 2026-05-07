use anyhow::{Context, bail};
use clap::Parser;
use near_chain::ChainStore;
use near_chain::backfill_receipt_to_tx::BackfillStorage;
use near_chain::backfill_receipt_to_tx_bucket_etl::{
    BucketEtlOptions, BucketEtlOutputMode, run_bucket_etl,
};
use near_chain_configs::GenesisValidationMode;
use near_o11y::tracing;
use near_primitives::types::BlockHeight;
use nearcore::open_storage;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// Bucket-ETL backfill of `DBCol::ReceiptToTx`. Phase 1 prototype: writes
/// validated output buckets to a scratch directory and either reports stats
/// (`--measure-only`, default) or compares against `process_height` over a
/// height range (`--compare-from-height` / `--compare-to-height`).
///
/// **No DB writes.** The CLI never writes to `DBCol::ReceiptToTx`.
/// Production ingest is gated by Phase 3.
#[derive(Parser)]
pub(crate) struct BackfillReceiptToTxBucketEtlCommand {
    /// Scratch directory for bucket files. **Must live on a different physical
    /// filesystem than cold-data** (so Pass A's sequential scans don't
    /// contend with Pass A/D's writes on the same HDDs). The pre-flight check
    /// rejects same-filesystem configurations.
    #[arg(long)]
    scratch_dir: PathBuf,

    /// Start height (inclusive). Defaults to chain genesis.
    #[arg(long)]
    from_block_height: Option<BlockHeight>,

    /// End height (inclusive). Defaults to chain head.
    #[arg(long)]
    to_block_height: Option<BlockHeight>,

    /// Worker count for the rayon pools used by Pass A-D. The Phase 0 spike
    /// found 4 workers saturate the LVM-backed cold-store HDDs.
    #[arg(long, default_value_t = 4)]
    num_threads: usize,

    /// Pass B per-worker height marker cadence. Workers flush buffers and
    /// persist a marker every N blocks so a crash mid-Pass-B can resume
    /// without redoing the whole slice.
    #[arg(long, default_value_t = 100_000)]
    marker_block_interval: u64,

    /// Run `process_height` over `[compare_from_height, compare_to_height]`
    /// after Pass D and byte-compare resulting `ReceiptToTx` entries against
    /// the bucket-ETL output. Mutually requires `--compare-to-height`.
    ///
    /// MEMORY: the comparator materializes both sides as a HashMap over
    /// the height range — roughly 4 GB per ~1M blocks. Use bounded ranges
    /// (e.g. 100k–1M blocks per invocation) and sweep the chain in chunks.
    /// A streaming sort-merge is a Phase 3 follow-up.
    #[arg(long, requires = "compare_to_height")]
    compare_from_height: Option<BlockHeight>,

    /// See `--compare-from-height`.
    #[arg(long, requires = "compare_from_height")]
    compare_to_height: Option<BlockHeight>,
}

impl BackfillReceiptToTxBucketEtlCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(home, genesis_validation)
            .context("failed to load config")?;

        // Pre-flight: scratch_dir must be on a different physical filesystem
        // than cold-data. Pass A's sequential scans + Pass A/D's writes
        // hammering the same HDDs would erase the cold-store throughput
        // budget the Phase 0 spike measured.
        if let Some(cold_store) = near_config.config.cold_store.as_ref() {
            // The cold-store path is resolved relative to home when relative;
            // None means the default location under home.
            let cold_path = match cold_store.path.as_ref() {
                Some(p) if p.is_absolute() => p.clone(),
                Some(p) => home.join(p),
                None => home.clone(),
            };
            ensure_separate_filesystems(&self.scratch_dir, &cold_path)
                .context("scratch_dir / cold-data filesystem pre-flight check failed")?;
        } else {
            tracing::warn!(
                "no cold_store configured; skipping scratch_dir filesystem check. \
                 (This CLI is intended for cold-store backfill on archival nodes.)"
            );
        }

        let node_storage = open_storage(home, &near_config).context("failed to open storage")?;
        let storage = BackfillStorage::for_node(&node_storage, None);
        let chain_store = ChainStore::new(
            storage.read_store.clone(),
            near_config.client_config.save_trie_changes,
            near_config.genesis.config.transaction_validity_period,
        );

        let output_mode = match (self.compare_from_height, self.compare_to_height) {
            (Some(from), Some(to)) => {
                if from > to {
                    bail!("--compare-from-height ({from}) > --compare-to-height ({to})");
                }
                BucketEtlOutputMode::CompareAgainstActor { from_height: from, to_height: to }
            }
            _ => BucketEtlOutputMode::MeasureOnly,
        };

        let opts = BucketEtlOptions {
            from_height: self.from_block_height,
            to_height: self.to_block_height,
            num_threads: self.num_threads,
            scratch_dir: self.scratch_dir.clone(),
            output_mode,
            marker_block_interval: self.marker_block_interval,
            crash_after_pass_b_height: None,
        };

        let start = Instant::now();
        let stats = run_bucket_etl(&chain_store, &storage, opts)?;
        let elapsed = start.elapsed();

        println!("bucket-ETL stats:");
        println!("  pass_a_rows:                {}", stats.pass_a_rows);
        println!("  pass_b_tx_origin_rows:      {}", stats.pass_b_tx_origin_rows);
        println!("  pass_b_needs_parent_rows:   {}", stats.pass_b_needs_parent_rows);
        println!("  pass_c_rows:                {}", stats.pass_c_rows);
        println!("  pass_d_rows:                {}", stats.pass_d_rows);
        println!("  blocks_processed:           {}", stats.blocks_processed);
        println!("  heights_skipped:            {}", stats.heights_skipped);
        println!("  missing_outcomes:           {}", stats.missing_outcomes);
        println!("  missing_child_receipts:     {}", stats.missing_child_receipts);
        println!("  missing_parent_receipts:    {}", stats.missing_parent_receipts);
        if matches!(self.compare_from_height, Some(_)) {
            println!("  compare_divergences:        {}", stats.compare_divergences);
        }
        println!("  wall_secs:                  {:.2}", elapsed.as_secs_f64());

        if matches!(self.compare_from_height, Some(_)) && stats.compare_divergences > 0 {
            bail!(
                "compare-mode reported {} divergences; see logs for first 10 keys",
                stats.compare_divergences
            );
        }

        Ok(())
    }
}

/// Resolve each path's filesystem device via `df --output=source` and reject
/// configurations where they share a device. The HDDs backing the cold-store
/// LVM volume saturated under Pass A's sequential scan in the Phase 0 spike;
/// putting scratch on the same device would block writes behind reads and
/// destroy the throughput budget.
fn ensure_separate_filesystems(scratch_dir: &Path, cold_path: &Path) -> anyhow::Result<()> {
    // Materialize scratch_dir before invoking df, since df requires an
    // existing path.
    std::fs::create_dir_all(scratch_dir)
        .with_context(|| format!("create scratch_dir {}", scratch_dir.display()))?;

    let scratch_dev = df_source(scratch_dir)?;
    let cold_dev = df_source(cold_path)?;

    if scratch_dev == cold_dev {
        bail!(
            "scratch_dir ({scratch}) and cold-data path ({cold}) resolve to the same device ({dev}). \
             scratch_dir must be on a different physical filesystem than cold-data to avoid \
             HDD contention with Pass A reads. Either choose a different --scratch-dir or \
             move cold-data; see plan-phase-1 §Performance for rationale.",
            scratch = scratch_dir.display(),
            cold = cold_path.display(),
            dev = scratch_dev,
        );
    }
    tracing::info!(
        scratch_device = %scratch_dev,
        cold_device = %cold_dev,
        "filesystem pre-flight: scratch and cold are on separate devices"
    );
    Ok(())
}

fn df_source(path: &Path) -> anyhow::Result<String> {
    let output = Command::new("df")
        .arg("--output=source")
        .arg(path)
        .output()
        .with_context(|| format!("invoke df on {}", path.display()))?;
    if !output.status.success() {
        bail!(
            "df failed for {}: status={}, stderr={}",
            path.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8(output.stdout).context("df output is not utf-8")?;
    // df output: header line ("Filesystem") then one source line.
    let source = stdout.lines().nth(1).unwrap_or("").trim().to_owned();
    if source.is_empty() {
        bail!("df returned no source for {}", path.display());
    }
    Ok(source)
}
