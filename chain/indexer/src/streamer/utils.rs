use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_parameters::RuntimeConfig;
use near_primitives::action::Action;
use near_primitives::receipt::Receipt;
use near_primitives::transaction::TransactionKey;
use near_primitives::types::Balance;
use near_primitives::views::{ExecutionStatusView, ReceiptView};
use node_runtime::config::calculate_tx_cost;

use crate::INDEXER;

pub(crate) fn convert_transactions_sir_into_local_receipts<'a>(
    tx_iter: impl IntoIterator<Item = &'a IndexerTransactionWithOutcome>,
    runtime_config: &RuntimeConfig,
    gas_price: Balance,
) -> Vec<ReceiptView> {
    let mut local_receipts = Vec::new();
    for indexer_tx in tx_iter {
        let tx = &indexer_tx.transaction;
        assert_eq!(tx.signer_id, tx.receiver_id);
        let outcome = &indexer_tx.outcome.execution_outcome.outcome;
        let ExecutionStatusView::SuccessReceiptId(receipt_id) = outcome.status else {
            tracing::debug!(
                target: INDEXER,
                block_hash = %indexer_tx.outcome.execution_outcome.block_hash,
                tx_hash = %tx.hash,
                status = ?outcome.status,
                "skip failed local tx",
            );
            continue;
        };
        let actions: Vec<_> =
            tx.actions.iter().cloned().map(Action::try_from).map(Result::unwrap).collect();
        let cost =
            calculate_tx_cost(&tx.receiver_id, &tx.signer_id, &actions, &runtime_config, gas_price)
                .unwrap();
        let tx_key = match tx.nonce_index {
            Some(_) => TransactionKey::GasKey {
                public_key: tx.public_key.clone(),
                nonce_index: tx.nonce_index.unwrap(),
            },
            None => TransactionKey::AccessKey { public_key: tx.public_key.clone() },
        };
        let receipt = Receipt::from_tx(
            receipt_id,
            tx.signer_id.clone(),
            tx.receiver_id.clone(),
            tx_key,
            cost.receipt_gas_price,
            actions,
        );
        let receipt_view: ReceiptView = receipt.into();
        local_receipts.push(receipt_view);
    }
    local_receipts
}
