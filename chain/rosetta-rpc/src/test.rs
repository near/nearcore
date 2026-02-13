use std::collections::HashMap;

use near_parameters::RuntimeConfigView;
use near_primitives::hash::CryptoHash;
use near_primitives::types::Balance;

use crate::adapters::transactions::{ExecutionToReceipts, convert_block_changes_to_transactions};
use near_async::multithread::MultithreadRuntimeHandle;
use near_client::ViewClientActor;

pub async fn test_convert_block_changes_to_transactions(
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
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
                    amount: Balance::from_yoctonear(5000000000000000000),
                    code_hash: CryptoHash::default(),
                    locked: Balance::from_near(400_000),
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
                    amount: Balance::from_yoctonear(4000000000000000000),
                    code_hash: CryptoHash::default(),
                    locked: Balance::from_near(400_000),
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
                    amount: Balance::from_yoctonear(7000000000000000000),
                    code_hash: CryptoHash::default(),
                    locked: Balance::from_near(400_000),
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
                    amount: Balance::from_yoctonear(8000000000000000000),
                    code_hash: CryptoHash::default(),
                    locked: Balance::from_near(400_000),
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
            amount: Balance::from_yoctonear(4000000000000000000),
            code_hash: CryptoHash::default(),
            locked: Balance::from_near(400_000),
            storage_paid_at: 0,
            storage_usage: 200000,
            global_contract_hash: None,
            global_contract_account_id: None,
        },
    );
    accounts_previous_state.insert(
        "nfvalidator2.near".parse().unwrap(),
        near_primitives::views::AccountView {
            amount: Balance::from_yoctonear(6000000000000000000),
            code_hash: CryptoHash::default(),
            locked: Balance::from_near(400_000),
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
        crate::gas_key_utils::GasKeyInfo::empty(),
        vec![],
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

/// Tests per-receipt gas key balance changes: transfer in, gas deduction, and key deletion.
pub async fn test_gas_key_changes_to_transactions(
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
    runtime_config: &RuntimeConfigView,
) {
    let block_hash = CryptoHash::default();
    let transfer_receipt_hash = CryptoHash([1u8; 32]);
    let gas_usage_receipt_hash = CryptoHash([3u8; 32]);
    let delete_key_receipt_hash = CryptoHash([4u8; 32]);

    // nfvalidator1: had key_a (0.5 NEAR) and key_b (0.3 NEAR) in prev block.
    // In current block:
    //   - Receipt 1: key_b balance updated to 0.5 NEAR (+0.2 NEAR transfer)
    //   - Receipt 3: key_a balance decreased to 0.3 NEAR (-0.2 NEAR gas usage)
    //   - Receipt 4: key_a deleted (burned remaining 0.3 NEAR)
    let key_a =
        near_crypto::SecretKey::from_seed(near_crypto::KeyType::ED25519, "gas-key-a").public_key();
    let key_b =
        near_crypto::SecretKey::from_seed(near_crypto::KeyType::ED25519, "gas-key-b").public_key();
    let previous_gas_keys = crate::gas_key_utils::GasKeyInfo::from_entries([(
        "nfvalidator1.near".parse().unwrap(),
        HashMap::from([
            (key_a.clone(), Balance::from_millinear(500)),
            (key_b.clone(), Balance::from_millinear(300)),
        ]),
    )]);

    let access_key_changes = vec![
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                receipt_hash: transfer_receipt_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                account_id: "nfvalidator1.near".parse().unwrap(),
                public_key: key_b,
                access_key: near_primitives::views::AccessKeyView {
                    nonce: 1,
                    permission: near_primitives::views::AccessKeyPermissionView::GasKeyFullAccess {
                        balance: Balance::from_millinear(500),
                        num_nonces: 1,
                    },
                },
            },
        },
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                receipt_hash: gas_usage_receipt_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                account_id: "nfvalidator1.near".parse().unwrap(),
                public_key: key_a.clone(),
                access_key: near_primitives::views::AccessKeyView {
                    nonce: 2,
                    permission: near_primitives::views::AccessKeyPermissionView::GasKeyFullAccess {
                        balance: Balance::from_millinear(300),
                        num_nonces: 1,
                    },
                },
            },
        },
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                receipt_hash: delete_key_receipt_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccessKeyDeletion {
                account_id: "nfvalidator1.near".parse().unwrap(),
                public_key: key_a,
            },
        },
    ];

    let transactions = convert_block_changes_to_transactions(
        view_client_addr,
        runtime_config,
        &block_hash,
        vec![],
        HashMap::new(),
        ExecutionToReceipts::empty(),
        previous_gas_keys,
        access_key_changes,
    )
    .await
    .unwrap();

    // key_b update (+0.2 NEAR) should be attributed to the transfer receipt.
    let transfer_tx = &transactions[&format!("receipt:{}", transfer_receipt_hash)];
    let gas_key_ops: Vec<_> = transfer_tx
        .operations
        .iter()
        .filter(|op| {
            op.account
                .sub_account
                .as_ref()
                .is_some_and(|s| s.address == crate::models::SubAccount::GasKey)
        })
        .collect();
    assert_eq!(gas_key_ops.len(), 1, "expected Transfer on SubAccount::GasKey");
    assert_eq!(gas_key_ops[0].type_, crate::models::OperationType::Transfer);

    // key_a gas usage (-0.2 NEAR) should be attributed to the gas usage receipt.
    let gas_usage_tx = &transactions[&format!("receipt:{}", gas_usage_receipt_hash)];
    let gas_key_ops: Vec<_> = gas_usage_tx
        .operations
        .iter()
        .filter(|op| {
            op.account
                .sub_account
                .as_ref()
                .is_some_and(|s| s.address == crate::models::SubAccount::GasKey)
        })
        .collect();
    assert_eq!(gas_key_ops.len(), 1, "expected Transfer (deduction) on SubAccount::GasKey");
    assert_eq!(gas_key_ops[0].type_, crate::models::OperationType::Transfer);

    // key_a deletion (burn remaining 0.3 NEAR) should be attributed to the delete key receipt.
    let delete_tx = &transactions[&format!("receipt:{}", delete_key_receipt_hash)];
    let gas_key_ops: Vec<_> = delete_tx
        .operations
        .iter()
        .filter(|op| {
            op.account
                .sub_account
                .as_ref()
                .is_some_and(|s| s.address == crate::models::SubAccount::GasKey)
        })
        .collect();
    assert_eq!(gas_key_ops.len(), 1, "expected GasKeyBalanceBurnt on SubAccount::GasKey");
    assert_eq!(gas_key_ops[0].type_, crate::models::OperationType::GasKeyBalanceBurnt);
}
