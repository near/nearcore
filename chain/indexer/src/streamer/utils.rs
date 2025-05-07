use actix::Addr;

use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_parameters::RuntimeConfig;
use near_primitives::types::ProtocolVersion;
use near_primitives::views;
use node_runtime::config::tx_cost;

use super::errors::FailedToFetchData;
use super::fetchers::fetch_block;

pub(crate) async fn convert_transactions_sir_into_local_receipts(
    client: &Addr<near_client::ViewClientActor>,
    runtime_config: &RuntimeConfig,
    txs: Vec<&IndexerTransactionWithOutcome>,
    block: &views::BlockView,
    protocol_version: ProtocolVersion,
) -> Result<Vec<views::ReceiptView>, FailedToFetchData> {
    if txs.is_empty() {
        return Ok(vec![]);
    }
    let prev_block = fetch_block(&client, block.header.prev_hash).await?;
    let prev_block_gas_price = prev_block.header.gas_price;

    let local_receipts: Vec<views::ReceiptView> = txs
        .into_iter()
        .map(|indexer_tx| {
            assert_eq!(indexer_tx.transaction.signer_id, indexer_tx.transaction.receiver_id);
            let tx = near_primitives::transaction::Transaction::V0(
                near_primitives::transaction::TransactionV0 {
                    signer_id: indexer_tx.transaction.signer_id.clone(),
                    public_key: indexer_tx.transaction.public_key.clone(),
                    nonce: indexer_tx.transaction.nonce,
                    receiver_id: indexer_tx.transaction.receiver_id.clone(),
                    block_hash: block.header.hash,
                    actions: indexer_tx
                        .transaction
                        .actions
                        .clone()
                        .into_iter()
                        .map(|action| {
                            near_primitives::transaction::Action::try_from(action).unwrap()
                        })
                        .collect(),
                },
            );
            // Can't use ValidatedTransaction here because transactions in a chunk can be invalid (RelaxedChunkValidation feature)
            let cost =
                tx_cost(&runtime_config, &tx, prev_block_gas_price, protocol_version).unwrap();
            views::ReceiptView {
                predecessor_id: indexer_tx.transaction.signer_id.clone(),
                receiver_id: indexer_tx.transaction.receiver_id.clone(),
                receipt_id: *indexer_tx
                    .outcome
                    .execution_outcome
                    .outcome
                    .receipt_ids
                    .first()
                    .expect("The transaction ExecutionOutcome should have one receipt id in vec"),
                receipt: views::ReceiptEnumView::Action {
                    signer_id: indexer_tx.transaction.signer_id.clone(),
                    signer_public_key: indexer_tx.transaction.public_key.clone(),
                    gas_price: cost.receipt_gas_price,
                    output_data_receivers: vec![],
                    input_data_ids: vec![],
                    actions: indexer_tx.transaction.actions.clone(),
                    is_promise_yield: false,
                },
                priority: 0,
            }
        })
        .collect();

    Ok(local_receipts)
}
