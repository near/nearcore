use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_parameters::RuntimeConfig;
use near_primitives::action::Action;
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

    let mut local_receipts = Vec::new();
    for indexer_tx in txs {
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
            continue;
        };
        let actions: Vec<_> =
            tx.actions.iter().cloned().map(Action::try_from).map(Result::unwrap).collect();
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
        local_receipts.push(receipt_view);
    }

    Ok(local_receipts)
}
