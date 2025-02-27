use actix::Addr;
use near_parameters::RuntimeConfigView;
use near_primitives::hash::CryptoHash;

use crate::adapters::transactions::{ExecutionToReceipts, convert_block_changes_to_transactions};

pub async fn test_convert_block_changes_to_transactions(
    view_client_addr: &Addr<near_client::ViewClientActor>,
    runtime_config: &RuntimeConfigView,
) {
    // cspell:ignore nfvalidator
    let block_hash = near_primitives::hash::CryptoHash::default();
    let nfvalidator1_receipt_processing_hash = near_primitives::hash::CryptoHash([1u8; 32]);
    let nfvalidator2_action_receipt_gas_reward_hash = near_primitives::hash::CryptoHash([2u8; 32]);
    let accounts_changes = vec![
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ValidatorAccountsUpdate,
            value: near_primitives::views::StateChangeValueView::AccountUpdate {
                account_id: "nfvalidator1.near".parse().unwrap(),
                account: near_primitives::views::AccountView {
                    amount: 5000000000000000000,
                    code_hash: CryptoHash::default(),
                    locked: 400000000000000000000000000000,
                    storage_paid_at: 0,
                    storage_usage: 200000,
                    global_contract_hash: None,
                    global_contract_account_id: None,
                },
            },
        },
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                receipt_hash: nfvalidator1_receipt_processing_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccountUpdate {
                account_id: "nfvalidator1.near".parse().unwrap(),
                account: near_primitives::views::AccountView {
                    amount: 4000000000000000000,
                    code_hash: CryptoHash::default(),
                    locked: 400000000000000000000000000000,
                    storage_paid_at: 0,
                    storage_usage: 200000,
                    global_contract_hash: None,
                    global_contract_account_id: None,
                },
            },
        },
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ValidatorAccountsUpdate,
            value: near_primitives::views::StateChangeValueView::AccountUpdate {
                account_id: "nfvalidator2.near".parse().unwrap(),
                account: near_primitives::views::AccountView {
                    amount: 7000000000000000000,
                    code_hash: CryptoHash::default(),
                    locked: 400000000000000000000000000000,
                    storage_paid_at: 0,
                    storage_usage: 200000,
                    global_contract_hash: None,
                    global_contract_account_id: None,
                },
            },
        },
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ActionReceiptGasReward {
                receipt_hash: nfvalidator2_action_receipt_gas_reward_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccountUpdate {
                account_id: "nfvalidator2.near".parse().unwrap(),
                account: near_primitives::views::AccountView {
                    amount: 8000000000000000000,
                    code_hash: CryptoHash::default(),
                    locked: 400000000000000000000000000000,
                    storage_paid_at: 0,
                    storage_usage: 200000,
                    global_contract_hash: None,
                    global_contract_account_id: None,
                },
            },
        },
    ];
    let mut accounts_previous_state = std::collections::HashMap::new();
    accounts_previous_state.insert(
        "nfvalidator1.near".parse().unwrap(),
        near_primitives::views::AccountView {
            amount: 4000000000000000000,
            code_hash: CryptoHash::default(),
            locked: 400000000000000000000000000000,
            storage_paid_at: 0,
            storage_usage: 200000,
            global_contract_hash: None,
            global_contract_account_id: None,
        },
    );
    accounts_previous_state.insert(
        "nfvalidator2.near".parse().unwrap(),
        near_primitives::views::AccountView {
            amount: 6000000000000000000,
            code_hash: CryptoHash::default(),
            locked: 400000000000000000000000000000,
            storage_paid_at: 0,
            storage_usage: 200000,
            global_contract_hash: None,
            global_contract_account_id: None,
        },
    );
    let transactions = convert_block_changes_to_transactions(
        view_client_addr,
        runtime_config,
        &block_hash,
        accounts_changes,
        accounts_previous_state,
        ExecutionToReceipts::empty(),
    )
    .await
    .unwrap();
    assert_eq!(transactions.len(), 3);
    assert!(transactions.iter().all(|(transaction_hash, transaction)| {
        &transaction.transaction_identifier.hash == transaction_hash
    }));

    let validators_update_transaction =
        &transactions[&format!("block-validators-update:{}", block_hash)];
    insta::assert_debug_snapshot!("validators_update_transaction", validators_update_transaction);

    let nfvalidator1_receipt_processing_transaction =
        &transactions[&format!("receipt:{}", nfvalidator1_receipt_processing_hash)];
    insta::assert_debug_snapshot!(
        "nfvalidator1_receipt_processing_transaction",
        nfvalidator1_receipt_processing_transaction
    );

    let nfvalidator2_action_receipt_gas_reward_transaction =
        &transactions[&format!("receipt:{}", nfvalidator2_action_receipt_gas_reward_hash)];
    insta::assert_debug_snapshot!(
        "nfvalidator2_action_receipt_gas_reward_transaction",
        nfvalidator2_action_receipt_gas_reward_transaction
    );
}
