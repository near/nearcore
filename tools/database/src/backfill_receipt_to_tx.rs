use anyhow::Context;
use clap::Parser;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use near_chain::ChainStore;
use near_chain::backfill_receipt_to_tx::{BACKFILL_CHECKPOINT_KEY, BackfillStats, process_height};
use near_chain_configs::GenesisValidationMode;
use near_o11y::tracing;
use near_primitives::types::BlockHeight;
use near_store::{DBCol, Store};
use nearcore::open_storage;
use rayon::prelude::*;
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

    /// Number of parallel threads for reading block data.
    #[arg(long, default_value_t = 1000)]
    num_threads: usize,
}

impl BackfillReceiptToTxCommand {
    pub(crate) fn run(
        &self,
        home: &PathBuf,
        genesis_validation: GenesisValidationMode,
    ) -> anyhow::Result<()> {
        let near_config = nearcore::config::load_config(home, genesis_validation)
            .context("failed to load config")?;
        let node_storage = open_storage(home, &near_config).context("failed to open storage")?;

        let hot_store = node_storage.get_hot_store();
        let read_store = node_storage.get_split_store().unwrap_or_else(|| hot_store.clone());
        let write_store = node_storage.get_cold_store().unwrap_or_else(|| hot_store.clone());

        let chain_store = ChainStore::new(
            read_store.clone(),
            near_config.client_config.save_trie_changes,
            near_config.genesis.config.transaction_validity_period,
        );

        let genesis_height = chain_store.get_genesis_height();
        let head_height = chain_store.head().context("failed to get chain head")?.height;

        let from_height = match self.from_block_height {
            Some(h) => h,
            None => hot_store
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
            use_checkpoint: true,
        };
        let stats = backfill_receipt_to_tx(
            &chain_store,
            &read_store,
            &write_store,
            &hot_store,
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
/// Processes heights in parallel chunks using rayon to hide HDD seek latency.
/// Reads are parallelized across `num_threads`, while writes are sequential
/// in height order to maintain deterministic checkpoint behavior.
///
/// When `use_checkpoint` is true, the checkpoint is stored in `DBCol::Misc`
/// after entries are committed. The two writes are non-atomic, but safe:
/// on crash between them, the batch is replayed (insert-only column, idempotent).
pub fn backfill_receipt_to_tx(
    chain_store: &ChainStore,
    read_store: &Store,
    write_store: &Store,
    checkpoint_store: &Store,
    from_height: BlockHeight,
    to_height: BlockHeight,
    options: &BackfillOptions,
    progress: Option<&ProgressBar>,
) -> anyhow::Result<BackfillStats> {
    let mut stats = BackfillStats { blocks_processed: 0, entries_written: 0, heights_skipped: 0 };

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.num_threads)
        .build()
        .context("failed to build thread pool")?;

    let batch_size = options.batch_size.max(1) as u64;
    let mut height = from_height;

    while height <= to_height {
        let chunk_end = (height + batch_size - 1).min(to_height);
        let blocks_before = stats.blocks_processed;

        // Parallel reads
        let results: Vec<_> = pool.install(|| {
            (height..=chunk_end)
                .into_par_iter()
                .map(|h| process_height(chain_store, read_store, h))
                .collect()
        });

        // Sequential writes in height order (par_iter preserves input order)
        let mut entry_update = write_store.store_update();
        for result in results {
            match result {
                Ok(Some(entries)) => {
                    for (receipt_id, info) in entries {
                        entry_update.insert_ser(DBCol::ReceiptToTx, receipt_id.as_ref(), &info);
                        stats.entries_written += 1;
                    }
                    stats.blocks_processed += 1;
                }
                Ok(None) => {
                    stats.heights_skipped += 1;
                }
                Err(e) => return Err(e),
            }
            if let Some(p) = progress {
                p.inc(1);
            }
        }
        entry_update.commit();

        // Checkpoint after entries are persisted. Non-atomic with entries, but safe:
        // crash between the two replays work (insert-only column, idempotent).
        if options.use_checkpoint {
            let mut cp_update = checkpoint_store.store_update();
            cp_update.set_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY, &chunk_end);
            cp_update.commit();
        }

        if stats.blocks_processed / 10_000 > blocks_before / 10_000 {
            tracing::info!(
                height = chunk_end,
                blocks_processed = stats.blocks_processed,
                entries_written = stats.entries_written,
                "backfill progress"
            );
        }

        height = chunk_end + 1;
    }

    Ok(stats)
}
