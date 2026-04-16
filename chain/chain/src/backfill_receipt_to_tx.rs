use crate::{ChainStore, Error};
use anyhow::Context;
use near_o11y::tracing;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction, ReceiptToTxInfo,
    ReceiptToTxInfoV1,
};
use near_primitives::types::BlockHeight;
use near_primitives::utils::get_block_shard_id_rev;
use near_store::{DBCol, Store};

/// Checkpoint key for the forward-direction CLI backfill tool.
/// The CLI processes heights ascending (genesis -> head) and stores the highest completed height.
///
/// Both the CLI and the background actor can run concurrently. Since ReceiptToTx is an
/// insert-only column, duplicate writes for overlapping heights are safe (idempotent).
/// The CLI is intended for one-shot bulk backfill with rayon parallelism, while the
/// actor handles steady-state background backfill in descending order.
pub const BACKFILL_CHECKPOINT_KEY: &[u8] = b"BACKFILL_RECEIPT_TO_TX";

/// Checkpoint key for the backward-direction background actor.
/// Stores the lowest completed height (everything from this height upward is done).
/// The actor processes heights descending (head -> genesis) so recent receipts are
/// queryable first.
pub const BACKFILL_CHECKPOINT_KEY_LOW: &[u8] = b"BACKFILL_RECEIPT_TO_TX_LOW";

/// Stats returned by backfill processing.
pub struct BackfillStats {
    pub blocks_processed: u64,
    pub entries_written: u64,
    pub heights_skipped: u64,
}

/// Process a single height: read all execution outcomes and build ReceiptToTxInfo entries.
///
/// Returns `Ok(None)` for skipped heights (no block at this height or no entries produced).
/// Returns `Ok(Some(entries))` with ReceiptToTx entries to write.
pub fn process_height(
    chain_store: &ChainStore,
    read_store: &Store,
    height: BlockHeight,
) -> anyhow::Result<Option<Vec<(CryptoHash, ReceiptToTxInfo)>>> {
    let block_hash = match chain_store.get_block_hash_by_height(height) {
        Ok(hash) => hash,
        Err(Error::DBNotFoundErr(_)) => return Ok(None),
        Err(e) => {
            return Err(e).context(format!("failed to get block hash at height {height}"));
        }
    };

    let mut entries = Vec::new();

    for (key, outcome_ids) in
        read_store.iter_prefix_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, block_hash.as_ref())
    {
        let (_, shard_id) = get_block_shard_id_rev(&key).expect("invalid OutcomeIds key");

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
            let parent_receipt = if !is_tx { chain_store.get_receipt(&outcome_id) } else { None };

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

                entries.push((*child_receipt_id, info));
            }
        }
    }

    if entries.is_empty() {
        return Ok(None);
    }

    Ok(Some(entries))
}
