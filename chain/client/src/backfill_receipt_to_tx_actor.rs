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

/// Background actor that backfills the ReceiptToTx DB column by processing
/// heights in descending order (from head toward genesis). This ensures
/// recent receipts become queryable first.
///
/// Follows the GCActor pattern: runs in a periodic loop via `ctx.run_later()`.
pub struct BackfillReceiptToTxActor {
    chain_store: ChainStore,
    read_store: Store,
    write_store: Store,
    genesis_height: BlockHeight,
    config: BackfillReceiptToTxConfig,
}

impl BackfillReceiptToTxActor {
    pub fn new(
        read_store: Store,
        write_store: Store,
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
        Self { chain_store, read_store, write_store, genesis_height, config }
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
    fn backfill_batch(&self) -> anyhow::Result<bool> {
        let checkpoint: Option<BlockHeight> =
            self.write_store.get_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW);

        // If checkpoint exists and is at or below genesis, we're done.
        if let Some(cp) = checkpoint {
            if cp <= self.genesis_height {
                return Ok(true);
            }
        }

        let current_height = match checkpoint {
            Some(cp) => cp - 1,
            None => {
                // First run: start from head.
                let head = self.chain_store.head().context("failed to get chain head")?;
                head.height
            }
        };

        if current_height < self.genesis_height {
            return Ok(true);
        }

        let batch_end = current_height
            .saturating_sub(self.config.batch_size.saturating_sub(1))
            .max(self.genesis_height);

        let mut stats =
            BackfillStats { blocks_processed: 0, entries_written: 0, heights_skipped: 0 };
        let mut store_update = self.write_store.store_update();

        // Process heights in descending order.
        for h in (batch_end..=current_height).rev() {
            match process_height(&self.chain_store, &self.read_store, h)? {
                Some(entries) => {
                    for (receipt_id, info) in entries {
                        store_update.insert_ser(DBCol::ReceiptToTx, receipt_id.as_ref(), &info);
                        stats.entries_written += 1;
                    }
                    stats.blocks_processed += 1;
                }
                None => {
                    stats.heights_skipped += 1;
                }
            }
        }

        // Checkpoint = lowest height processed in this batch.
        store_update.set_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW, &batch_end);
        store_update.commit();

        if stats.blocks_processed > 0 {
            tracing::info!(
                from_height = current_height,
                to_height = batch_end,
                blocks_processed = stats.blocks_processed,
                entries_written = stats.entries_written,
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
