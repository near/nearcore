use crate::adapters::transactions::{ExecutionToReceipts, convert_block_changes_to_transactions};
use near_async::multithread::MultithreadRuntimeHandle;
use near_client::ViewClientActor;
use near_crypto::SecretKey;
use near_parameters::RuntimeConfigView;
use near_primitives::hash::CryptoHash;
use near_primitives::types::Balance;
use near_primitives::views::{ActionView, SignedTransactionView};
use std::collections::HashMap;

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

/// Tests per-receipt gas key balance changes: transfer in, gas deduction, key deletion,
/// and transfer_fee_type metadata (GasPrepayment for TransactionProcessing, GasRefund
/// for system predecessor, none for regular receipts).
pub async fn test_gas_key_changes_to_transactions(
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
    runtime_config: &RuntimeConfigView,
) {
    let block_hash = CryptoHash::default();
    let transfer_receipt_hash = CryptoHash([1u8; 32]);
    let refund_receipt_hash = CryptoHash([2u8; 32]);
    let gas_usage_receipt_hash = CryptoHash([3u8; 32]);
    let delete_key_receipt_hash = CryptoHash([4u8; 32]);
    let gas_prepay_tx_hash = CryptoHash([5u8; 32]);

    // nfvalidator1: had key_a (0.5 NEAR), key_b (0.3 NEAR), key_c (0.1 NEAR),
    // key_d (0.4 NEAR) in prev block.
    // In current block:
    //   - Receipt 1: key_b balance updated to 0.5 NEAR (+0.2 NEAR transfer, no fee type)
    //   - Receipt 2 (from "system"): key_c balance updated to 0.2 NEAR (+0.1 NEAR refund)
    //   - Receipt 3: key_a balance decreased to 0.3 NEAR (-0.2 NEAR gas usage, no fee type)
    //   - Receipt 4: key_a deleted (burned remaining 0.3 NEAR)
    //   - Transaction 5: key_d balance decreased to 0.2 NEAR (-0.2 NEAR gas prepayment)
    let key_a = SecretKey::from_seed(near_crypto::KeyType::ED25519, "gas-key-a").public_key();
    let key_b = SecretKey::from_seed(near_crypto::KeyType::ED25519, "gas-key-b").public_key();
    let key_c = SecretKey::from_seed(near_crypto::KeyType::ED25519, "gas-key-c").public_key();
    let key_d = SecretKey::from_seed(near_crypto::KeyType::ED25519, "gas-key-d").public_key();
    let previous_gas_keys = crate::gas_key_utils::GasKeyInfo::from_entries([(
        "nfvalidator1.near".parse().unwrap(),
        HashMap::from([
            (key_a.clone(), Balance::from_millinear(500)),
            (key_b.clone(), Balance::from_millinear(300)),
            (key_c.clone(), Balance::from_millinear(100)),
            (key_d.clone(), Balance::from_millinear(400)),
        ]),
    )]);

    let exec_to_rx = ExecutionToReceipts::with_data(
        HashMap::from([
            (transfer_receipt_hash, "alice.near".parse().unwrap()),
            (refund_receipt_hash, "system".parse().unwrap()),
            (gas_usage_receipt_hash, "bob.near".parse().unwrap()),
            (delete_key_receipt_hash, "bob.near".parse().unwrap()),
        ]),
        HashMap::from([(
            gas_prepay_tx_hash,
            SignedTransactionView {
                signer_id: "nfvalidator1.near".parse().unwrap(),
                public_key: SecretKey::from_seed(near_crypto::KeyType::ED25519, "signer")
                    .public_key(),
                nonce: 1,
                receiver_id: "nfvalidator1.near".parse().unwrap(),
                actions: vec![],
                _priority_fee: 0,
                signature: near_crypto::Signature::default(),
                hash: gas_prepay_tx_hash,
                nonce_index: None,
                nonce_mode: None,
            },
        )]),
    );

    let access_key_changes = vec![
        // Receipt from non-system predecessor: no fee type
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
        // Receipt from "system": GasRefund
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                receipt_hash: refund_receipt_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                account_id: "nfvalidator1.near".parse().unwrap(),
                public_key: key_c,
                access_key: near_primitives::views::AccessKeyView {
                    nonce: 1,
                    permission: near_primitives::views::AccessKeyPermissionView::GasKeyFullAccess {
                        balance: Balance::from_millinear(200),
                        num_nonces: 1,
                    },
                },
            },
        },
        // Receipt from non-system predecessor: no fee type
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
        // Key deletion
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                receipt_hash: delete_key_receipt_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccessKeyDeletion {
                account_id: "nfvalidator1.near".parse().unwrap(),
                public_key: key_a,
            },
        },
        // TransactionProcessing: GasPrepayment
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::TransactionProcessing {
                tx_hash: gas_prepay_tx_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccessKeyUpdate {
                account_id: "nfvalidator1.near".parse().unwrap(),
                public_key: key_d,
                access_key: near_primitives::views::AccessKeyView {
                    nonce: 3,
                    permission: near_primitives::views::AccessKeyPermissionView::GasKeyFullAccess {
                        balance: Balance::from_millinear(200),
                        num_nonces: 1,
                    },
                },
            },
        },
    ];

    let transactions = convert_block_changes_to_transactions(
        view_client_addr,
        runtime_config,
        &block_hash,
        vec![],
        HashMap::new(),
        exec_to_rx,
        previous_gas_keys,
        access_key_changes,
    )
    .await
    .unwrap();

    let fee_type =
        |op: &crate::models::Operation| op.metadata.as_ref().and_then(|m| m.transfer_fee_type);

    // key_b update (+0.2 NEAR): non-system receipt, no fee type.
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
    assert_eq!(fee_type(gas_key_ops[0]), None);
    // +0.2 NEAR: key_b went from 0.3 to 0.5 NEAR
    assert_eq!(
        gas_key_ops[0].amount,
        Some(crate::models::Amount::from_yoctonear_diff(crate::utils::SignedDiff::cmp(
            Balance::from_millinear(300).as_yoctonear(),
            Balance::from_millinear(500).as_yoctonear(),
        )))
    );

    // key_c refund (+0.1 NEAR): system receipt, GasRefund.
    let refund_tx = &transactions[&format!("receipt:{}", refund_receipt_hash)];
    let gas_key_ops: Vec<_> = refund_tx
        .operations
        .iter()
        .filter(|op| {
            op.account
                .sub_account
                .as_ref()
                .is_some_and(|s| s.address == crate::models::SubAccount::GasKey)
        })
        .collect();
    assert_eq!(gas_key_ops.len(), 1, "expected Transfer (refund) on SubAccount::GasKey");
    assert_eq!(gas_key_ops[0].type_, crate::models::OperationType::Transfer);
    assert_eq!(
        fee_type(gas_key_ops[0]),
        Some(crate::models::OperationMetadataTransferFeeType::GasRefund)
    );
    // +0.1 NEAR: key_c went from 0.1 to 0.2 NEAR
    assert_eq!(
        gas_key_ops[0].amount,
        Some(crate::models::Amount::from_yoctonear_diff(crate::utils::SignedDiff::cmp(
            Balance::from_millinear(100).as_yoctonear(),
            Balance::from_millinear(200).as_yoctonear(),
        )))
    );

    // key_a gas usage (-0.2 NEAR): non-system receipt, no fee type.
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
    assert_eq!(fee_type(gas_key_ops[0]), None);
    // -0.2 NEAR: key_a went from 0.5 to 0.3 NEAR
    assert_eq!(
        gas_key_ops[0].amount,
        Some(crate::models::Amount::from_yoctonear_diff(crate::utils::SignedDiff::cmp(
            Balance::from_millinear(500).as_yoctonear(),
            Balance::from_millinear(300).as_yoctonear(),
        )))
    );

    // key_a deletion (burn remaining 0.3 NEAR).
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
    // -0.3 NEAR: key_a had 0.3 NEAR remaining (after gas usage) when deleted
    assert_eq!(
        gas_key_ops[0].amount,
        Some(-crate::models::Amount::from_balance(Balance::from_millinear(300)))
    );

    // key_d TransactionProcessing (-0.2 NEAR): GasPrepayment.
    let prepay_tx = &transactions[&format!("tx:{}", gas_prepay_tx_hash)];
    let gas_key_ops: Vec<_> = prepay_tx
        .operations
        .iter()
        .filter(|op| {
            op.account
                .sub_account
                .as_ref()
                .is_some_and(|s| s.address == crate::models::SubAccount::GasKey)
        })
        .collect();
    assert_eq!(gas_key_ops.len(), 1, "expected Transfer (prepay) on SubAccount::GasKey");
    assert_eq!(gas_key_ops[0].type_, crate::models::OperationType::Transfer);
    assert_eq!(
        fee_type(gas_key_ops[0]),
        Some(crate::models::OperationMetadataTransferFeeType::GasPrepayment)
    );
    // -0.2 NEAR: key_d went from 0.4 to 0.2 NEAR
    assert_eq!(
        gas_key_ops[0].amount,
        Some(crate::models::Amount::from_yoctonear_diff(crate::utils::SignedDiff::cmp(
            Balance::from_millinear(400).as_yoctonear(),
            Balance::from_millinear(200).as_yoctonear(),
        )))
    );
}

/// Tests that a Stake transaction during TransactionProcessing produces a single
/// liquid balance operation with GasPrepayment metadata (since Stake has no deposit
/// that needs separating, the entire liquid change is gas).
pub async fn test_stake_gas_prepayment(
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
    runtime_config: &RuntimeConfigView,
) {
    let block_hash = CryptoHash::default();
    let stake_tx_hash = CryptoHash([10u8; 32]);

    // Stake action has no deposit during TransactionProcessing — only gas is charged.
    let exec_to_rx = ExecutionToReceipts::with_data(
        HashMap::new(),
        HashMap::from([(
            stake_tx_hash,
            SignedTransactionView {
                signer_id: "alice.near".parse().unwrap(),
                public_key: SecretKey::from_seed(near_crypto::KeyType::ED25519, "alice")
                    .public_key(),
                nonce: 1,
                receiver_id: "alice.near".parse().unwrap(),
                actions: vec![ActionView::Stake {
                    stake: Balance::from_near(100),
                    public_key: SecretKey::from_seed(near_crypto::KeyType::ED25519, "alice")
                        .public_key(),
                }],
                _priority_fee: 0,
                signature: near_crypto::Signature::default(),
                hash: stake_tx_hash,
                nonce_index: None,
                nonce_mode: None,
            },
        )]),
    );

    // Previous state: 10 NEAR liquid. After TransactionProcessing: 9.999 NEAR liquid (gas charged).
    // Locked stays the same during TransactionProcessing (stake adjustment happens in receipt).
    let accounts_changes = vec![near_primitives::views::StateChangeWithCauseView {
        cause: near_primitives::views::StateChangeCauseView::TransactionProcessing {
            tx_hash: stake_tx_hash,
        },
        value: near_primitives::views::StateChangeValueView::AccountUpdate {
            account_id: "alice.near".parse().unwrap(),
            account: near_primitives::views::AccountView {
                amount: Balance::from_millinear(9999),
                code_hash: CryptoHash::default(),
                locked: Balance::from_near(100),
                storage_paid_at: 0,
                storage_usage: 0,
                global_contract_hash: None,
                global_contract_account_id: None,
            },
        },
    }];
    let mut accounts_previous_state = HashMap::new();
    accounts_previous_state.insert(
        "alice.near".parse().unwrap(),
        near_primitives::views::AccountView {
            amount: Balance::from_near(10),
            code_hash: CryptoHash::default(),
            locked: Balance::from_near(100),
            storage_paid_at: 0,
            storage_usage: 0,
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
        exec_to_rx,
        crate::gas_key_utils::GasKeyInfo::empty(),
        vec![],
    )
    .await
    .unwrap();

    let fee_type =
        |op: &crate::models::Operation| op.metadata.as_ref().and_then(|m| m.transfer_fee_type);

    let stake_tx = &transactions[&format!("tx:{}", stake_tx_hash)];
    // Should produce exactly one liquid balance operation (gas charge).
    // Locked didn't change, so no locked balance operation.
    let liquid_ops: Vec<_> =
        stake_tx.operations.iter().filter(|op| op.account.sub_account.is_none()).collect();
    assert_eq!(liquid_ops.len(), 1, "expected single liquid balance op (gas charge)");
    assert_eq!(liquid_ops[0].type_, crate::models::OperationType::Transfer);
    // The gas charge should be marked as GasPrepayment.
    assert_eq!(
        fee_type(liquid_ops[0]),
        Some(crate::models::OperationMetadataTransferFeeType::GasPrepayment),
        "Stake transaction gas charge must have GasPrepayment metadata"
    );
}

/// Tests that a FunctionCall transaction with non-zero deposit during TransactionProcessing
/// produces two operations: one for the deposit and one for gas (GasPrepayment).
pub async fn test_function_call_deposit_separation(
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
    runtime_config: &RuntimeConfigView,
) {
    let block_hash = CryptoHash::default();
    let fc_tx_hash = CryptoHash([11u8; 32]);
    let fc_deposit = Balance::from_near(5);

    let exec_to_rx = ExecutionToReceipts::with_data(
        HashMap::new(),
        HashMap::from([(
            fc_tx_hash,
            SignedTransactionView {
                signer_id: "bob.near".parse().unwrap(),
                public_key: SecretKey::from_seed(near_crypto::KeyType::ED25519, "bob").public_key(),
                nonce: 1,
                receiver_id: "contract.near".parse().unwrap(),
                actions: vec![ActionView::FunctionCall {
                    method_name: "do_something".to_string(),
                    args: vec![].into(),
                    gas: near_primitives::types::Gas::from_gas(30_000_000_000_000),
                    deposit: fc_deposit,
                }],
                _priority_fee: 0,
                signature: near_crypto::Signature::default(),
                hash: fc_tx_hash,
                nonce_index: None,
                nonce_mode: None,
            },
        )]),
    );

    // Previous: 20 NEAR liquid. After TransactionProcessing: 14.999 NEAR (5 NEAR deposit + 0.001 gas).
    let accounts_changes = vec![near_primitives::views::StateChangeWithCauseView {
        cause: near_primitives::views::StateChangeCauseView::TransactionProcessing {
            tx_hash: fc_tx_hash,
        },
        value: near_primitives::views::StateChangeValueView::AccountUpdate {
            account_id: "bob.near".parse().unwrap(),
            account: near_primitives::views::AccountView {
                amount: Balance::from_millinear(14999),
                code_hash: CryptoHash::default(),
                locked: Balance::ZERO,
                storage_paid_at: 0,
                storage_usage: 0,
                global_contract_hash: None,
                global_contract_account_id: None,
            },
        },
    }];
    let mut accounts_previous_state = HashMap::new();
    accounts_previous_state.insert(
        "bob.near".parse().unwrap(),
        near_primitives::views::AccountView {
            amount: Balance::from_near(20),
            code_hash: CryptoHash::default(),
            locked: Balance::ZERO,
            storage_paid_at: 0,
            storage_usage: 0,
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
        exec_to_rx,
        crate::gas_key_utils::GasKeyInfo::empty(),
        vec![],
    )
    .await
    .unwrap();

    let fee_type =
        |op: &crate::models::Operation| op.metadata.as_ref().and_then(|m| m.transfer_fee_type);

    let fc_tx = &transactions[&format!("tx:{}", fc_tx_hash)];
    let liquid_ops: Vec<_> =
        fc_tx.operations.iter().filter(|op| op.account.sub_account.is_none()).collect();
    // Should produce two operations: deposit deduction + gas charge.
    assert_eq!(liquid_ops.len(), 2, "expected two ops: deposit + gas");

    // First op: the deposit (negative, no fee type).
    assert_eq!(liquid_ops[0].amount, Some(-crate::models::Amount::from_balance(fc_deposit)));
    assert_eq!(fee_type(liquid_ops[0]), None, "deposit op should have no fee type");

    // Second op: the gas charge (with GasPrepayment).
    assert_eq!(
        fee_type(liquid_ops[1]),
        Some(crate::models::OperationMetadataTransferFeeType::GasPrepayment),
        "gas charge op must have GasPrepayment metadata"
    );
}

/// Tests that execution_status propagates from ExecutionToReceipts statuses
/// into the Rosetta Transaction metadata.
pub async fn test_execution_status_propagation(
    view_client_addr: &MultithreadRuntimeHandle<ViewClientActor>,
    runtime_config: &RuntimeConfigView,
) {
    let block_hash = CryptoHash::default();
    let success_tx_hash = CryptoHash([20u8; 32]);
    let failure_receipt_hash = CryptoHash([21u8; 32]);

    let exec_to_rx = ExecutionToReceipts::with_data(
        HashMap::from([(failure_receipt_hash, "alice.near".parse().unwrap())]),
        HashMap::from([(
            success_tx_hash,
            SignedTransactionView {
                signer_id: "bob.near".parse().unwrap(),
                public_key: SecretKey::from_seed(near_crypto::KeyType::ED25519, "bob").public_key(),
                nonce: 1,
                receiver_id: "bob.near".parse().unwrap(),
                actions: vec![],
                _priority_fee: 0,
                signature: near_crypto::Signature::default(),
                hash: success_tx_hash,
                nonce_index: None,
                nonce_mode: None,
            },
        )]),
    )
    .with_statuses(HashMap::from([
        (success_tx_hash, crate::models::ExecutionStatus::Success),
        (failure_receipt_hash, crate::models::ExecutionStatus::Failure),
    ]));

    let accounts_changes = vec![
        // TransactionProcessing cause → should get Success status
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::TransactionProcessing {
                tx_hash: success_tx_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccountUpdate {
                account_id: "bob.near".parse().unwrap(),
                account: near_primitives::views::AccountView {
                    amount: Balance::from_millinear(9999),
                    code_hash: CryptoHash::default(),
                    locked: Balance::ZERO,
                    storage_paid_at: 0,
                    storage_usage: 0,
                    global_contract_hash: None,
                    global_contract_account_id: None,
                },
            },
        },
        // ReceiptProcessing cause → should get Failure status
        near_primitives::views::StateChangeWithCauseView {
            cause: near_primitives::views::StateChangeCauseView::ReceiptProcessing {
                receipt_hash: failure_receipt_hash,
            },
            value: near_primitives::views::StateChangeValueView::AccountUpdate {
                account_id: "alice.near".parse().unwrap(),
                account: near_primitives::views::AccountView {
                    amount: Balance::from_millinear(9998),
                    code_hash: CryptoHash::default(),
                    locked: Balance::ZERO,
                    storage_paid_at: 0,
                    storage_usage: 0,
                    global_contract_hash: None,
                    global_contract_account_id: None,
                },
            },
        },
    ];

    let mut accounts_previous_state = HashMap::new();
    accounts_previous_state.insert(
        "bob.near".parse().unwrap(),
        near_primitives::views::AccountView {
            amount: Balance::from_near(10),
            code_hash: CryptoHash::default(),
            locked: Balance::ZERO,
            storage_paid_at: 0,
            storage_usage: 0,
            global_contract_hash: None,
            global_contract_account_id: None,
        },
    );
    accounts_previous_state.insert(
        "alice.near".parse().unwrap(),
        near_primitives::views::AccountView {
            amount: Balance::from_near(10),
            code_hash: CryptoHash::default(),
            locked: Balance::ZERO,
            storage_paid_at: 0,
            storage_usage: 0,
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
        exec_to_rx,
        crate::gas_key_utils::GasKeyInfo::empty(),
        vec![],
    )
    .await
    .unwrap();

    // TransactionProcessing tx should have Success execution_status.
    let success_tx = &transactions[&format!("tx:{}", success_tx_hash)];
    assert_eq!(
        success_tx.metadata.execution_status,
        Some(crate::models::ExecutionStatus::Success),
        "TransactionProcessing tx must have execution_status: Success"
    );

    // ReceiptProcessing tx should have Failure execution_status.
    let failure_tx = &transactions[&format!("receipt:{}", failure_receipt_hash)];
    assert_eq!(
        failure_tx.metadata.execution_status,
        Some(crate::models::ExecutionStatus::Failure),
        "ReceiptProcessing tx must have execution_status: Failure"
    );
}
