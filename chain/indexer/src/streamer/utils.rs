use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_parameters::RuntimeConfig;
use near_primitives::receipt::Receipt;
use near_primitives::views::{self, ExecutionStatusView};
use node_runtime::config::calculate_tx_cost;

use crate::INDEXER;
use crate::streamer::IndexerViewClientFetcher;

use super::errors::FailedToFetchData;

pub(crate) async fn convert_transactions_sir_into_local_receipts(
    client: &IndexerViewClientFetcher,
    runtime_config: &RuntimeConfig,
    txs: Vec<&IndexerTransactionWithOutcome>,
    block: &views::BlockView,
) -> Result<Vec<views::ReceiptView>, FailedToFetchData> {
    if txs.is_empty() {
        return Ok(vec![]);
    }
    let prev_block = client.fetch_block(block.header.prev_hash).await?;
    let prev_block_gas_price = prev_block.header.gas_price;

    let local_receipts: Vec<views::ReceiptView> = txs
        .into_iter()
        .filter_map(|indexer_tx| {
            let tx = &indexer_tx.transaction;
            assert_eq!(tx.signer_id, tx.receiver_id);
            let outcome = &indexer_tx.outcome.execution_outcome.outcome;
            let ExecutionStatusView::SuccessReceiptId(receipt_id) = outcome.status else {
                tracing::debug!(
                    target: INDEXER,
                    block_hash = %block.header.hash,
                    tx_hash = %tx.hash,
                    status = ?outcome.status,
                    "skip failed local tx",
                );
                return None;
            };
            let actions: Vec<_> = tx
                .actions
                .iter()
                .map(|action| {
                    near_primitives::transaction::Action::try_from(action.clone()).unwrap()
                })
                .collect();
            let cost = calculate_tx_cost(
                &tx.receiver_id,
                &tx.signer_id,
                &actions,
                &runtime_config,
                prev_block_gas_price,
            )
            .unwrap();
            let receipt = Receipt::from_tx(
                receipt_id,
                tx.signer_id.clone(),
                tx.receiver_id.clone(),
                tx.public_key.clone(),
                cost.receipt_gas_price,
                actions,
            );
            let receipt_view: views::ReceiptView = receipt.into();
            Some(receipt_view)
        })
        .collect();

    Ok(local_receipts)
}
