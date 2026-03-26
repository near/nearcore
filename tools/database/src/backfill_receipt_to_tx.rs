use anyhow::Context;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use near_chain::ChainStore;
use near_chain_configs::GenesisValidationMode;
use near_o11y::tracing;
use near_primitives::receipt::{
    ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction, ReceiptToTxInfo,
    ReceiptToTxInfoV1,
};
use near_primitives::types::BlockHeight;
use near_store::{DBCol, Store};
use nearcore::open_storage;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

const CHECKPOINT_FILENAME: &str = "backfill-receipt-to-tx.checkpoint";

/// Stats returned by the backfill function.
pub struct BackfillStats {
    pub blocks_processed: u64,
    pub entries_written: u64,
    pub heights_skipped: u64,
}

#[derive(Parser)]
pub(crate) struct BackfillReceiptToTxCommand {
    /// Start backfill from this block height (inclusive). Overrides checkpoint.
    #[arg(long)]
    from_block_height: Option<BlockHeight>,

    /// End backfill at this block height (inclusive). Defaults to chain head.
    #[arg(long)]
    to_block_height: Option<BlockHeight>,

    /// Number of entries to batch before committing a DB write.
    #[arg(long, default_value_t = 10_000)]
    batch_size: usize,
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

        let read_store =
            node_storage.get_split_store().unwrap_or_else(|| node_storage.get_hot_store());
        let write_store = node_storage.get_hot_store();

        let chain_store = ChainStore::new(
            read_store.clone(),
            near_config.client_config.save_trie_changes,
            near_config.genesis.config.transaction_validity_period,
        );

        let genesis_height = chain_store.get_genesis_height();
        let head_height = chain_store.head().context("failed to get chain head")?.height;

        let checkpoint_path = home.join("data").join(CHECKPOINT_FILENAME);

        let from_height = match self.from_block_height {
            Some(h) => h,
            None => read_checkpoint(&checkpoint_path).map(|h| h + 1).unwrap_or(genesis_height),
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
        let stats = backfill_receipt_to_tx(
            &chain_store,
            &read_store,
            &write_store,
            from_height,
            to_height,
            self.batch_size,
            Some(&checkpoint_path),
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
/// Iterates forward through block heights, reads execution outcomes for each
/// block/shard, and reconstructs `ReceiptToTxInfo` entries by looking up
/// transaction and receipt data from the DB.
pub fn backfill_receipt_to_tx(
    chain_store: &ChainStore,
    read_store: &Store,
    write_store: &Store,
    from_height: BlockHeight,
    to_height: BlockHeight,
    batch_size: usize,
    checkpoint_path: Option<&Path>,
    progress: Option<&ProgressBar>,
) -> anyhow::Result<BackfillStats> {
    let mut store_update = write_store.store_update();
    let mut batch_count: usize = 0;
    let mut stats = BackfillStats { blocks_processed: 0, entries_written: 0, heights_skipped: 0 };

    for height in from_height..=to_height {
        let block_hash = match chain_store.get_block_hash_by_height(height) {
            Ok(hash) => hash,
            Err(near_chain::Error::DBNotFoundErr(_)) => {
                stats.heights_skipped += 1;
                if let Some(p) = progress {
                    p.inc(1);
                }
                continue;
            }
            Err(e) => {
                return Err(e).context(format!("failed to get block hash at height {height}"));
            }
        };

        let block = chain_store
            .get_block(&block_hash)
            .context(format!("failed to get block at height {height}"))?;

        for chunk_header in block.chunks().iter() {
            let shard_id = chunk_header.shard_id();
            let outcome_ids =
                chain_store.get_outcomes_by_block_hash_and_shard_id(&block_hash, shard_id);

            for outcome_id in outcome_ids {
                let outcome_with_proof =
                    match chain_store.get_outcome_by_id_and_block_hash(&outcome_id, &block_hash) {
                        Some(o) => o,
                        None => {
                            tracing::warn!(
                                %outcome_id,
                                height,
                                %shard_id,
                                "missing execution outcome, skipping"
                            );
                            continue;
                        }
                    };
                let outcome = &outcome_with_proof.outcome;

                if outcome.receipt_ids.is_empty() {
                    continue;
                }

                let is_tx = read_store.exists(DBCol::Transactions, outcome_id.as_ref());

                // For receipt outcomes, look up the parent receipt once (not per child).
                let parent_receipt =
                    if !is_tx { chain_store.get_receipt(&outcome_id) } else { None };

                for child_receipt_id in &outcome.receipt_ids {
                    let child_receipt = match chain_store.get_receipt(child_receipt_id) {
                        Some(r) => r,
                        None => {
                            tracing::debug!(
                                %child_receipt_id,
                                %outcome_id,
                                height,
                                "missing child receipt, skipping"
                            );
                            continue;
                        }
                    };

                    let info = if is_tx {
                        ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                            origin: ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                                tx_hash: outcome_id,
                                sender_account_id: outcome.executor_id.clone(),
                            }),
                            receiver_account_id: child_receipt.receiver_id().clone(),
                            shard_id,
                        })
                    } else {
                        let parent = match &parent_receipt {
                            Some(r) => r,
                            None => {
                                tracing::debug!(
                                    %outcome_id,
                                    height,
                                    "missing parent receipt for receipt outcome, skipping"
                                );
                                continue;
                            }
                        };
                        ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                            origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                                parent_receipt_id: outcome_id,
                                parent_predecessor_id: parent.predecessor_id().clone(),
                            }),
                            receiver_account_id: child_receipt.receiver_id().clone(),
                            shard_id,
                        })
                    };

                    store_update.insert_ser(DBCol::ReceiptToTx, child_receipt_id.as_ref(), &info);
                    batch_count += 1;
                    stats.entries_written += 1;

                    if batch_count >= batch_size {
                        store_update.commit();
                        store_update = write_store.store_update();
                        batch_count = 0;
                        if let Some(path) = checkpoint_path {
                            write_checkpoint(path, height)?;
                        }
                    }
                }
            }
        }

        stats.blocks_processed += 1;
        if let Some(p) = progress {
            p.inc(1);
        }

        if stats.blocks_processed % 10_000 == 0 {
            tracing::info!(
                height,
                blocks_processed = stats.blocks_processed,
                entries_written = stats.entries_written,
                "backfill progress"
            );
        }
    }

    // Flush remaining batch.
    if batch_count > 0 {
        store_update.commit();
        if let Some(path) = checkpoint_path {
            write_checkpoint(path, to_height)?;
        }
    }

    Ok(stats)
}

fn read_checkpoint(path: &Path) -> Option<BlockHeight> {
    let content = fs::read_to_string(path).ok()?;
    content.trim().parse::<BlockHeight>().ok()
}

fn write_checkpoint(path: &Path, height: BlockHeight) -> anyhow::Result<()> {
    fs::write(path, height.to_string()).context("failed to write checkpoint file")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.checkpoint");

        assert_eq!(read_checkpoint(&path), None);

        write_checkpoint(&path, 12345).unwrap();
        assert_eq!(read_checkpoint(&path), Some(12345));

        write_checkpoint(&path, 99999).unwrap();
        assert_eq!(read_checkpoint(&path), Some(99999));
    }
}
