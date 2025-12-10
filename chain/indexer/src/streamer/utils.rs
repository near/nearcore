use near_indexer_primitives::IndexerTransactionWithOutcome;
use near_parameters::RuntimeConfig;
use near_primitives::action::Action;
use near_primitives::receipt::Receipt;
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
        
        // Note: We cannot convert ActionView back to Action for DeployContract actions
        // because the view only contains the code hash, not the actual code.
        // Skip transactions with DeployContract actions.
        let has_deploy_contract = tx.actions.iter().any(|action| {
            matches!(
                action,
                near_primitives::views::ActionView::DeployContract { .. }
                    | near_primitives::views::ActionView::DeployGlobalContract { .. }
                    | near_primitives::views::ActionView::DeployGlobalContractByAccountId { .. }
            )
        });
        
        if has_deploy_contract {
            tracing::debug!(
                target: INDEXER,
                block_hash = %indexer_tx.outcome.execution_outcome.block_hash,
                tx_hash = %tx.hash,
                "skip local tx with deploy contract action - cannot reconstruct action from view",
            );
            continue;
        }
        
        // For other actions, we can still create a receipt (though this is not ideal)
        // Note: This conversion is deprecated and will be removed in the future
        let actions: Vec<_> = tx
            .actions
            .iter()
            .cloned()
            .filter_map(|action_view| {
                // Convert non-deploy-contract actions
                match action_view {
                    near_primitives::views::ActionView::CreateAccount => {
                        Some(Action::CreateAccount(
                            near_primitives::action::CreateAccountAction {},
                        ))
                    }
                    near_primitives::views::ActionView::FunctionCall {
                        method_name,
                        args,
                        gas,
                        deposit,
                    } => Some(Action::FunctionCall(Box::new(
                        near_primitives::action::FunctionCallAction {
                            method_name,
                            args: args.into(),
                            gas,
                            deposit,
                        },
                    ))),
                    near_primitives::views::ActionView::Transfer { deposit } => {
                        Some(Action::Transfer(near_primitives::action::TransferAction {
                            deposit,
                        }))
                    }
                    near_primitives::views::ActionView::Stake { stake, public_key } => {
                        Some(Action::Stake(Box::new(near_primitives::action::StakeAction {
                            stake,
                            public_key,
                        })))
                    }
                    near_primitives::views::ActionView::AddKey {
                        public_key,
                        access_key,
                    } => Some(Action::AddKey(Box::new(
                        near_primitives::action::AddKeyAction {
                            public_key,
                            access_key: access_key.into(),
                        },
                    ))),
                    near_primitives::views::ActionView::DeleteKey { public_key } => {
                        Some(Action::DeleteKey(Box::new(
                            near_primitives::action::DeleteKeyAction { public_key },
                        )))
                    }
                    near_primitives::views::ActionView::DeleteAccount { beneficiary_id } => {
                        Some(Action::DeleteAccount(
                            near_primitives::action::DeleteAccountAction { beneficiary_id },
                        ))
                    }
                    _ => None, // Skip any other unsupported actions
                }
            })
            .collect();
        
        let cost =
            calculate_tx_cost(&tx.receiver_id, &tx.signer_id, &actions, &runtime_config, gas_price)
                .unwrap();
        let receipt = Receipt::from_tx(
            receipt_id,
            tx.signer_id.clone(),
            tx.receiver_id.clone(),
            tx.public_key.clone(),
            cost.receipt_gas_price,
            actions,
        );
        let receipt_view: ReceiptView = receipt.into();
        local_receipts.push(receipt_view);
    }
    local_receipts
}
