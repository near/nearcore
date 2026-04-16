use anyhow::Context;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
use near_async::time::Duration;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY_LOW, BackfillStats, process_height,
};
use near_chain::{ChainGenesis, ChainStore, ChainStoreAccess};
use near_chain_configs::BackfillReceiptToTxConfig;
use near_o11y::tracing;
use near_primitives::types::BlockHeight;
use near_store::{DBCol, Store};
use std::time::Instant;

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
    read_store: Store,
    write_store: Store,
    /// Checkpoints are always stored in hot storage.
    checkpoint_store: Store,
    genesis_height: BlockHeight,
    config: BackfillReceiptToTxConfig,
    /// The height where backfill started (set on first batch), used for progress tracking.
    initial_height: Option<BlockHeight>,
}

impl BackfillReceiptToTxActor {
    pub fn new(
        read_store: Store,
        write_store: Store,
        checkpoint_store: Store,
        save_trie_changes: bool,
        genesis: &ChainGenesis,
        config: BackfillReceiptToTxConfig,
    ) -> Self {
        let chain_store = ChainStore::new(
            read_store.clone(),
            save_trie_changes,
            genesis.transaction_validity_period,
        );
        let genesis_height = chain_store.get_genesis_height();
        Self {
            chain_store,
            read_store,
            write_store,
            checkpoint_store,
            genesis_height,
            config,
            initial_height: None,
        }
    }

    fn backfill_loop(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        match self.backfill_batch() {
            Ok(true) => {
                // Backfill complete — stop scheduling.
                tracing::info!("receipt-to-tx backfill complete");
            }
            Ok(false) => {
                // More work to do — schedule next batch.
                ctx.run_later(
                    "backfill receipt to tx",
                    self.config.batch_delay,
                    move |act, ctx| {
                        act.backfill_loop(ctx);
                    },
                );
            }
            Err(e) => {
                tracing::warn!(?e, "receipt-to-tx backfill error, retrying in 30s");
                ctx.run_later(
                    "backfill receipt to tx retry",
                    Duration::seconds(30),
                    move |act, ctx| {
                        act.backfill_loop(ctx);
                    },
                );
            }
        }
    }

    /// Process one batch of heights in descending order.
    /// Returns `Ok(true)` when backfill is complete (reached genesis).
    /// Returns `Ok(false)` when there's more work to do.
    pub fn backfill_batch(&mut self) -> anyhow::Result<bool> {
        let batch_start = Instant::now();

        let checkpoint: Option<BlockHeight> =
            self.checkpoint_store.get_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW);

        // If checkpoint exists and is at or below genesis, we're done.
        if let Some(cp) = checkpoint {
            if cp <= self.genesis_height {
                return Ok(true);
            }
        }

        let current_height = match checkpoint {
            Some(cp) => cp - 1,
            None => match self.config.start_height {
                Some(h) => h,
                None => {
                    let head = self.chain_store.head().context("failed to get chain head")?;
                    head.height
                }
            },
        };

        if current_height < self.genesis_height {
            return Ok(true);
        }

        if self.initial_height.is_none() {
            self.initial_height = Some(current_height);
        }

        let batch_end = current_height
            .saturating_sub(self.config.batch_size.saturating_sub(1))
            .max(self.genesis_height);

        let mut stats =
            BackfillStats { blocks_processed: 0, entries_written: 0, heights_skipped: 0 };
        let mut entry_update = self.write_store.store_update();

        // Process heights in descending order.
        for h in (batch_end..=current_height).rev() {
            match process_height(&self.chain_store, &self.read_store, h)? {
                Some(entries) => {
                    for (receipt_id, info) in entries {
                        entry_update.insert_ser(DBCol::ReceiptToTx, receipt_id.as_ref(), &info);
                        stats.entries_written += 1;
                    }
                    stats.blocks_processed += 1;
                }
                None => {
                    stats.heights_skipped += 1;
                }
            }
        }
        entry_update.commit();

        // Checkpoint after entries are persisted. Non-atomic with entries, but safe:
        // crash between the two replays work (insert-only column, idempotent).
        let mut cp_update = self.checkpoint_store.store_update();
        cp_update.set_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW, &batch_end);
        cp_update.commit();

        let batch_duration = batch_start.elapsed();
        if stats.blocks_processed > 0 || stats.heights_skipped > 0 {
            let heights_in_batch = current_height - batch_end + 1;
            let remaining = batch_end.saturating_sub(self.genesis_height);
            let total =
                self.initial_height.unwrap_or(current_height).saturating_sub(self.genesis_height);
            let progress_pct =
                if total > 0 { ((total - remaining) * 100 / total) as u32 } else { 100 };
            tracing::info!(
                from_height = current_height,
                to_height = batch_end,
                blocks_processed = stats.blocks_processed,
                entries_written = stats.entries_written,
                batch_duration_ms = batch_duration.as_millis() as u64,
                heights_per_second = if batch_duration.as_secs_f64() > 0.0 {
                    (heights_in_batch as f64 / batch_duration.as_secs_f64()) as u64
                } else {
                    0
                },
                remaining_heights = remaining,
                progress_pct,
                "receipt-to-tx backfill progress"
            );
        }

        Ok(batch_end <= self.genesis_height)
    }
}

impl Actor for BackfillReceiptToTxActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.backfill_loop(ctx);
    }
}
