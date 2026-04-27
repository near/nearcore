use anyhow::{Context, anyhow};
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::Actor;
use near_async::time::Duration;
use near_chain::backfill_receipt_to_tx::{
    BACKFILL_CHECKPOINT_KEY_LOW, BackfillStorage, process_one_batch,
};
use near_chain::{ChainGenesis, ChainStore, ChainStoreAccess};
use near_chain_configs::BackfillReceiptToTxConfig;
use near_o11y::tracing;
use near_primitives::types::{BlockHeight, BlockHeightDelta};
use near_store::DBCol;
use std::time::Instant;

use crate::metrics;

const SYNC_GAP_THRESHOLD: BlockHeightDelta = 100;

fn is_chain_at_tip(block_head: BlockHeight, header_head: BlockHeight) -> bool {
    header_head.saturating_sub(block_head) <= SYNC_GAP_THRESHOLD
}

fn next_retry_delay(consecutive_errors: u32, batch_delay: Duration) -> Duration {
    let error_base_secs = batch_delay.whole_seconds().max(30);
    let multiplier = 2i64.pow(consecutive_errors.min(8));
    Duration::seconds((error_base_secs * multiplier).min(300))
}

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
    storage: BackfillStorage,
    pool: rayon::ThreadPool,
    genesis_height: BlockHeight,
    config: BackfillReceiptToTxConfig,
    consecutive_errors: u32,
    start_height_validated: bool,
}

impl BackfillReceiptToTxActor {
    pub fn new(
        storage: BackfillStorage,
        save_trie_changes: bool,
        genesis: &ChainGenesis,
        config: BackfillReceiptToTxConfig,
    ) -> anyhow::Result<Self> {
        let chain_store = ChainStore::new(
            storage.read_store.clone(),
            save_trie_changes,
            genesis.transaction_validity_period,
        );
        let genesis_height = chain_store.get_genesis_height();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(config.num_threads)
            .build()
            .context("failed to build rayon thread pool")?;
        Ok(Self {
            chain_store,
            storage,
            pool,
            genesis_height,
            config,
            consecutive_errors: 0,
            start_height_validated: false,
        })
    }

    fn backfill_loop(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        match self.check_chain_at_tip() {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!("receipt-to-tx backfill waiting for sync to complete");
                ctx.run_later(
                    "backfill receipt to tx - waiting for sync",
                    Duration::seconds(30),
                    move |act, ctx| act.backfill_loop(ctx),
                );
                return;
            }
            Err(e) => {
                let delay = next_retry_delay(self.consecutive_errors, self.config.batch_delay);
                self.consecutive_errors += 1;
                tracing::warn!(
                    ?e,
                    ?delay,
                    "receipt-to-tx backfill: failed to check sync status, retrying"
                );
                ctx.run_later("backfill receipt to tx retry", delay, move |act, ctx| {
                    act.backfill_loop(ctx)
                });
                return;
            }
        }

        match self.backfill_batch() {
            Ok(true) => {
                self.consecutive_errors = 0;
                tracing::info!("receipt-to-tx backfill complete");
            }
            Ok(false) => {
                self.consecutive_errors = 0;
                ctx.run_later(
                    "backfill receipt to tx",
                    self.config.batch_delay,
                    move |act, ctx| {
                        act.backfill_loop(ctx);
                    },
                );
            }
            Err(e) => {
                let delay = next_retry_delay(self.consecutive_errors, self.config.batch_delay);
                self.consecutive_errors += 1;
                tracing::warn!(?e, ?delay, "receipt-to-tx backfill error, retrying");
                ctx.run_later("backfill receipt to tx retry", delay, move |act, ctx| {
                    act.backfill_loop(ctx);
                });
            }
        }
    }

    fn check_chain_at_tip(&self) -> anyhow::Result<bool> {
        let block_head = self.chain_store.head().context("get block head")?.height;
        let header_head = self.chain_store.header_head().context("get header head")?.height;
        Ok(is_chain_at_tip(block_head, header_head))
    }

    /// Process one batch of heights in descending order.
    /// Returns `Ok(true)` when backfill is complete (reached genesis).
    /// Returns `Ok(false)` when there's more work to do.
    pub fn backfill_batch(&mut self) -> anyhow::Result<bool> {
        let batch_start = Instant::now();

        let checkpoint: Option<BlockHeight> =
            self.storage.checkpoint_store.get_ser(DBCol::Misc, BACKFILL_CHECKPOINT_KEY_LOW);

        if let Some(cp) = checkpoint {
            metrics::BACKFILL_RECEIPT_TO_TX_LOWEST_COMPLETED_HEIGHT.set(cp as i64);
            if cp <= self.genesis_height {
                return Ok(true);
            }
        }

        // On the first batch, validate `config.start_height` before using it. The checks
        // only matter once — after the first batch writes a checkpoint, `start_height` is
        // irrelevant forever (the resume logic uses the checkpoint instead).
        if !self.start_height_validated {
            if let Some(start_height) = self.config.start_height {
                if let Some(cp) = checkpoint {
                    tracing::warn!(
                        start_height,
                        checkpoint = cp,
                        "backfill_receipt_to_tx: start_height is ignored because a checkpoint already exists",
                    );
                } else {
                    let head = self.chain_store.head().context("failed to get chain head")?.height;
                    if start_height > head {
                        return Err(anyhow!(
                            "backfill_receipt_to_tx: start_height {start_height} exceeds chain head {head}"
                        ));
                    }
                    if start_height < self.genesis_height {
                        return Err(anyhow!(
                            "backfill_receipt_to_tx: start_height {start_height} is below genesis {}",
                            self.genesis_height
                        ));
                    }
                }
            }
            self.start_height_validated = true;
        }

        let current_height = match checkpoint {
            Some(cp) => cp.saturating_sub(1),
            None => match self.config.start_height {
                Some(h) => h,
                None => {
                    let head = self.chain_store.head().context("failed to get chain head")?;
                    head.height
                }
            },
        };

        debug_assert!(
            current_height >= self.genesis_height,
            "current_height {current_height} below genesis {}",
            self.genesis_height
        );

        let batch_end = current_height
            .saturating_sub(self.config.batch_size.saturating_sub(1))
            .max(self.genesis_height);

        // Descending order so recent receipts become queryable first.
        let heights: Vec<BlockHeight> = (batch_end..=current_height).rev().collect();
        let stats = process_one_batch(
            &self.chain_store,
            &self.storage,
            &self.pool,
            &heights,
            Some((BACKFILL_CHECKPOINT_KEY_LOW, batch_end)),
        )?;

        metrics::BACKFILL_RECEIPT_TO_TX_LOWEST_COMPLETED_HEIGHT.set(batch_end as i64);

        let batch_duration = batch_start.elapsed();
        if stats.blocks_processed > 0 || stats.heights_skipped > 0 {
            let heights_in_batch = current_height - batch_end + 1;
            let remaining = batch_end.saturating_sub(self.genesis_height);
            let missing_total = stats.missing_total();
            let denominator = stats.entries_written + missing_total;
            let missing_is_significant = denominator > 0 && missing_total * 20 > denominator;
            let heights_per_second = if batch_duration.as_secs_f64() > 0.0 {
                (heights_in_batch as f64 / batch_duration.as_secs_f64()) as u64
            } else {
                0
            };
            if missing_is_significant {
                tracing::warn!(
                    from_height = current_height,
                    to_height = batch_end,
                    blocks_processed = stats.blocks_processed,
                    entries_written = stats.entries_written,
                    missing_outcomes = stats.missing_outcomes,
                    missing_child_receipts = stats.missing_child_receipts,
                    missing_parent_receipts = stats.missing_parent_receipts,
                    batch_duration_ms = batch_duration.as_millis() as u64,
                    heights_per_second,
                    remaining_heights = remaining,
                    "receipt-to-tx backfill progress: >5% of expected entries were missing upstream data"
                );
            } else {
                tracing::info!(
                    from_height = current_height,
                    to_height = batch_end,
                    blocks_processed = stats.blocks_processed,
                    entries_written = stats.entries_written,
                    missing_outcomes = stats.missing_outcomes,
                    missing_child_receipts = stats.missing_child_receipts,
                    missing_parent_receipts = stats.missing_parent_receipts,
                    batch_duration_ms = batch_duration.as_millis() as u64,
                    heights_per_second,
                    remaining_heights = remaining,
                    "receipt-to-tx backfill progress"
                );
            }
        }

        Ok(batch_end <= self.genesis_height)
    }
}

impl Actor for BackfillReceiptToTxActor {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.backfill_loop(ctx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_chain_at_tip() {
        assert!(is_chain_at_tip(100, 100));
        assert!(is_chain_at_tip(100, 200));
        assert!(!is_chain_at_tip(100, 201));
        assert!(!is_chain_at_tip(0, 1000));
        // header_head < block_head (saturating_sub handles gracefully)
        assert!(is_chain_at_tip(200, 100));
    }

    #[test]
    fn test_next_retry_delay() {
        // With batch_delay=1s, error_base = max(1, 30) = 30s
        let batch_delay = Duration::seconds(1);
        assert_eq!(next_retry_delay(0, batch_delay), Duration::seconds(30));
        assert_eq!(next_retry_delay(1, batch_delay), Duration::seconds(60));
        assert_eq!(next_retry_delay(2, batch_delay), Duration::seconds(120));
        assert_eq!(next_retry_delay(3, batch_delay), Duration::seconds(240));
        assert_eq!(next_retry_delay(4, batch_delay), Duration::seconds(300));
        assert_eq!(next_retry_delay(8, batch_delay), Duration::seconds(300));
        assert_eq!(next_retry_delay(100, batch_delay), Duration::seconds(300));

        // With batch_delay=60s, error_base = max(60, 30) = 60s
        let batch_delay = Duration::seconds(60);
        assert_eq!(next_retry_delay(0, batch_delay), Duration::seconds(60));
        assert_eq!(next_retry_delay(1, batch_delay), Duration::seconds(120));
        assert_eq!(next_retry_delay(2, batch_delay), Duration::seconds(240));
        assert_eq!(next_retry_delay(3, batch_delay), Duration::seconds(300));
    }
}
