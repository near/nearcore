use actix::Addr;

use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_parameters::RuntimeConfig;
use near_primitives::version::ProtocolVersion;
use near_primitives::views;
use node_runtime::config::get_receipt_gas_price;

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

    let local_receipts: Vec<views::ReceiptView> =
        txs.into_iter()
            .map(|tx| {
                let (receipt_gas_price, _) = get_receipt_gas_price(
                    &runtime_config,
                    &near_primitives::transaction::Transaction::V0(
                        near_primitives::transaction::TransactionV0 {
                            signer_id: tx.transaction.signer_id.clone(),
                            public_key: tx.transaction.public_key.clone(),
                            nonce: tx.transaction.nonce,
                            receiver_id: tx.transaction.receiver_id.clone(),
                            block_hash: block.header.hash,
                            actions: tx
                                .transaction
                                .actions
                                .clone()
                                .into_iter()
                                .map(|action| {
                                    near_primitives::transaction::Action::try_from(action).unwrap()
                                })
                                .collect(),
                        },
                    ),
                    protocol_version,
                    prev_block_gas_price,
                )
                .expect("TransactionCost returned IntegerOverflowError");
                views::ReceiptView {
                    predecessor_id: tx.transaction.signer_id.clone(),
                    receiver_id: tx.transaction.receiver_id.clone(),
                    receipt_id: *tx.outcome.execution_outcome.outcome.receipt_ids.first().expect(
                        "The transaction ExecutionOutcome should have one receipt id in vec",
                    ),
                    receipt: views::ReceiptEnumView::Action {
                        signer_id: tx.transaction.signer_id.clone(),
                        signer_public_key: tx.transaction.public_key.clone(),
                        gas_price: receipt_gas_price,
                        output_data_receivers: vec![],
                        input_data_ids: vec![],
                        actions: tx.transaction.actions.clone(),
                        is_promise_yield: false,
                    },
                    priority: 0,
                }
            })
            .collect();

    Ok(local_receipts)
}
