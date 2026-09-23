use anyhow::Context;
use clap::Parser;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use near_chain::ChainStore;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY, BackfillStats, BackfillStorage, process_one_batch,
};
use near_chain_configs::GenesisValidationMode;
use near_o11y::tracing;
use near_primitives::types::BlockHeight;
use near_store::DBCol;
use nearcore::open_storage;
use std::path::PathBuf;
use std::time::Instant;

/// Options controlling how the forward-direction backfill runs.
pub struct BackfillOptions {
    /// Number of heights to process per DB write batch.
    pub batch_size: usize,
    /// Number of parallel threads for reading block data.
    pub num_threads: usize,
    /// Whether to persist a checkpoint in DBCol::Misc after each batch.
    pub use_checkpoint: bool,
}

#[derive(Parser)]
pub(crate) struct BackfillReceiptToTxCommand {
    /// Start backfill from this block height (inclusive). Overrides checkpoint.
    #[arg(long)]
    from_block_height: Option<BlockHeight>,

    /// End backfill at this block height (inclusive). Defaults to chain head.
    #[arg(long)]
    to_block_height: Option<BlockHeight>,

    /// Number of heights to process per DB write batch.
    #[arg(long, default_value_t = 1_000)]
    batch_size: usize,

    /// Number of parallel threads for reading block data. High default to saturate the disk
    /// I/O queue on HDD-backed archival nodes where most threads block on seeks.
    #[arg(long, default_value_t = 128)]
    num_threads: usize,

    /// Disable checkpoint persistence. Useful for re-processing specific height ranges.
    #[arg(long, default_value_t = false)]
    no_checkpoint: bool,
}

impl BackfillReceiptToTxCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(home, genesis_validation)
            .context("failed to load config")?;
        if !near_config.client_config.save_tx_outcomes {
            tracing::warn!(
                "save_tx_outcomes is disabled in config; \
                 backfill may produce incorrect origin data"
            );
        }
        let node_storage = open_storage(home, &near_config).context("failed to open storage")?;

        let storage = BackfillStorage::for_node(&node_storage);

        let chain_store = ChainStore::new(
            storage.read_store.clone(),
            near_config.client_config.save_trie_changes,
            near_config.genesis.config.transaction_validity_period,
        );

        let genesis_height = chain_store.get_genesis_height();
        let head_height = chain_store.head().context("failed to get chain head")?.height;

        let from_height = match self.from_block_height {
            Some(h) => h,
            None => storage
                .checkpoint_store
                .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
                .map(|h| h + 1)
                .unwrap_or(genesis_height),
        };
        let to_height = self.to_block_height.unwrap_or(head_height).min(head_height);

        if from_height > to_height {
            tracing::info!(from_height, to_height, "nothing to backfill (empty range)");
            return Ok(());
        }

        tracing::info!(
            from_height,
            to_height,
            "starting receipt-to-tx backfill ({} heights)",
            to_height - from_height + 1
        );

        let progress = ProgressBar::new(to_height - from_height + 1);
        progress.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} heights ({eta} remaining)",
                )
                .unwrap()
                .progress_chars("=> "),
        );

        let start = Instant::now();
        let options = BackfillOptions {
            batch_size: self.batch_size,
            num_threads: self.num_threads,
            use_checkpoint: !self.no_checkpoint,
        };
        let stats = backfill_receipt_to_tx(
            &chain_store,
            &storage,
            from_height,
            to_height,
            &options,
            Some(&progress),
        )?;

        progress.finish_with_message("done");
        let elapsed = start.elapsed();

        tracing::info!(
            blocks_processed = stats.blocks_processed,
            entries_written = stats.entries_written,
            heights_skipped = stats.heights_skipped,
            elapsed_secs = elapsed.as_secs(),
            "backfill complete"
        );

        Ok(())
    }
}

/// Core backfill logic. Extracted as a standalone function for testability.
///
/// Uses [`process_one_batch`] to drive parallel reads / sequential writes per chunk, so
/// the per-batch semantics match the background actor exactly. The CLI runs ascending
/// (genesis → head) and records the highest completed height under
/// [`BACKFILL_CHECKPOINT_KEY`] when `options.use_checkpoint` is set.
pub fn backfill_receipt_to_tx(
    chain_store: &ChainStore,
    storage: &BackfillStorage,
    from_height: BlockHeight,
    to_height: BlockHeight,
    options: &BackfillOptions,
    progress: Option<&ProgressBar>,
) -> anyhow::Result<BackfillStats> {
    let mut stats = BackfillStats::default();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.num_threads)
        .build()
        .context("failed to build thread pool")?;

    let batch_size = options.batch_size.max(1) as u64;
    let mut height = from_height;

    while height <= to_height {
        let chunk_end = (height + batch_size - 1).min(to_height);
        let blocks_before = stats.blocks_processed;

        let heights: Vec<BlockHeight> = (height..=chunk_end).collect();
        let checkpoint = options.use_checkpoint.then_some((BACKFILL_CHECKPOINT_KEY, chunk_end));
        let batch_stats = process_one_batch(chain_store, storage, &pool, &heights, checkpoint)?;
        stats.add(&batch_stats);

        if let Some(p) = progress {
            p.inc(heights.len() as u64);
        }

        if stats.blocks_processed / 10_000 > blocks_before / 10_000 {
            tracing::info!(
                height = chunk_end,
                blocks_processed = stats.blocks_processed,
                entries_written = stats.entries_written,
                missing_outcomes = stats.missing_outcomes,
                missing_child_receipts = stats.missing_child_receipts,
                missing_parent_receipts = stats.missing_parent_receipts,
                "backfill progress"
            );
        }

        height = chunk_end + 1;
    }

    Ok(stats)
}
