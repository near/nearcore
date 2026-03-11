use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, Gas, ShardId};
use near_store::DBCol;

const EPOCH_LENGTH: u64 = 5;

/// When `save_receipt_to_tx` is false, no ReceiptToTx entries should be written.
#[test]
fn test_save_receipt_to_tx_false() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let signer = create_user_test_signer(&user_account);
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.validator().submit_tx(tx);

    // Run enough blocks and wait for execution to complete.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Get receipt ID from the transaction outcome.
    let outcome = env.validator().client().chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_id = outcome.outcome_with_id.outcome.receipt_ids[0];

    // Verify ReceiptToTx entry does NOT exist.
    let store = env.validator().store();
    assert!(
        store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref()).is_none(),
        "receipt_to_tx should not be saved when save_receipt_to_tx is false"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// ReceiptToTx data persists to disk and survives a client restart.
#[test]
fn test_receipt_to_tx_persists_across_restart() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gc_num_epochs_to_keep(20)
        .build();

    let restart_node_data = &env.node_datas[0];
    let restart_account = restart_node_data.account_id.clone();
    let restart_identifier = restart_node_data.identifier.clone();
    let stable_node_idx = 1;

    let signer = create_user_test_signer(&user_account);
    let block_hash = env.node_for_account(&restart_account).head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );

    // Use execute_tx to submit and wait for the transaction to complete.
    // This is more reliable than submit_tx + run_until_executed_height in
    // multi-validator setups because it polls until the result is available.
    let outcome =
        env.runner_for_account(&restart_account).execute_tx(tx, Duration::seconds(10)).unwrap();
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // Verify ReceiptToTx entry exists before restart.
    let info_before = env
        .node_for_account(&restart_account)
        .store()
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
        .expect("receipt_to_tx entry should exist before restart");
    let ReceiptToTxInfo::V1(v1) = &info_before;
    assert_eq!(v1.shard_id, ShardId::new(0), "shard_id should be 0 in single-shard setup");

    // Kill and restart the node.
    let killed_node_state = env.kill_node(&restart_identifier);
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);
    let new_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_identifier, killed_node_state);

    // Verify ReceiptToTx entry still exists after restart and matches.
    let info_after = env
        .node_for_account(&restart_account)
        .store()
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
        .expect("receipt_to_tx entry should persist across restart");
    assert_eq!(info_before, info_after, "receipt_to_tx entry should be identical after restart");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// `save_receipt_to_tx` works independently from `save_tx_outcomes`.
/// When `save_tx_outcomes = false` but `save_receipt_to_tx = true`, ReceiptToTx entries
/// should still be written.
#[test]
fn test_save_receipt_to_tx_independent_of_outcomes() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_tx_outcomes = false;
        })
        .build();

    let signer = create_user_test_signer(&user_account);
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.validator().submit_tx(tx);

    // Run enough blocks and wait for execution to complete.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Verify: outcome NOT saved (save_tx_outcomes=false).
    assert!(
        env.validator().client().chain.get_execution_outcome(&tx_hash).is_err(),
        "outcomes should not be saved when save_tx_outcomes is false"
    );

    // Find receipt IDs by iterating ReceiptToTx entries whose origin
    // is FromTransaction with matching tx_hash.
    let store = env.validator().store();
    let found_receipt =
        store.iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx).any(|(_, info)| match &info {
            ReceiptToTxInfo::V1(v1) => matches!(
                &v1.origin,
                ReceiptOrigin::FromTransaction(origin) if origin.tx_hash == tx_hash
            ),
        });
    assert!(found_receipt, "receipt_to_tx entry should exist even when save_tx_outcomes is false");

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// When both save_receipt_to_tx and save_tx_outcomes are false, neither
/// OutcomeIds nor ReceiptToTx entries should be written.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_no_index_when_both_disabled() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_tx_outcomes = false;
            config.save_receipt_to_tx = false;
        })
        .build();

    // Capture baselines before sending the transaction.
    let outcome_ids_before = env.validator().store().iter(DBCol::OutcomeIds).count();
    let receipt_to_tx_before = env.validator().store().iter(DBCol::ReceiptToTx).count();

    let signer = create_user_test_signer(&user_account);
    let nonce_before = env
        .validator()
        .view_access_key_query(&user_account, &signer.public_key())
        .expect("access key should exist")
        .nonce;

    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account.clone(),
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.validator().submit_tx(tx);

    // Run enough blocks and wait for execution to complete.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Verify outcomes are NOT saved (save_tx_outcomes=false).
    assert!(
        env.validator().client().chain.get_execution_outcome(&tx_hash).is_err(),
        "outcomes should not be saved when save_tx_outcomes is false"
    );

    // Verify the transaction actually executed by checking the nonce advanced.
    let nonce_after = env
        .validator()
        .view_access_key_query(&user_account, &signer.public_key())
        .expect("access key should exist")
        .nonce;
    assert!(nonce_after > nonce_before, "nonce should have advanced after tx execution");

    let store = env.validator().store();

    // No new OutcomeIds should be written.
    assert_eq!(
        store.iter(DBCol::OutcomeIds).count(),
        outcome_ids_before,
        "no new OutcomeIds when both save_tx_outcomes and save_receipt_to_tx are false"
    );

    // No new ReceiptToTx should be written.
    assert_eq!(
        store.iter(DBCol::ReceiptToTx).count(),
        receipt_to_tx_before,
        "no new ReceiptToTx when save_receipt_to_tx is false"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Restart persistence in index-only mode
/// (save_tx_outcomes=false, save_receipt_to_tx=true).
#[test]
fn test_receipt_to_tx_persists_across_restart_index_only() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gc_num_epochs_to_keep(20)
        .config_modifier(|config, _| {
            config.save_tx_outcomes = false;
        })
        .build();

    let restart_node_data = &env.node_datas[0];
    let restart_account = restart_node_data.account_id.clone();
    let restart_identifier = restart_node_data.identifier.clone();
    let stable_node_idx = 1;

    let signer = create_user_test_signer(&user_account);
    let block_hash = env.node_for_account(&restart_account).head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.node_for_account(&restart_account).submit_tx(tx);

    // Run until ReceiptToTx entries for our tx appear. We can't use execute_tx
    // or get_execution_outcome since save_tx_outcomes=false.
    let tx_hash_copy = tx_hash;
    env.runner_for_account(&restart_account).run_until(
        |node| {
            node.store().iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx).any(
                |(_, info)| match &info {
                    ReceiptToTxInfo::V1(v1) => matches!(
                        &v1.origin,
                        ReceiptOrigin::FromTransaction(origin) if origin.tx_hash == tx_hash_copy
                    ),
                },
            )
        },
        Duration::seconds(10),
    );

    // Collect the ReceiptToTx entries for verification after restart.
    let our_entries: Vec<(CryptoHash, ReceiptToTxInfo)> = env
        .node_for_account(&restart_account)
        .store()
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .filter_map(|(key, info)| {
            let receipt_id = CryptoHash::try_from(key.as_ref()).unwrap();
            match &info {
                ReceiptToTxInfo::V1(v1) => match &v1.origin {
                    ReceiptOrigin::FromTransaction(origin) if origin.tx_hash == tx_hash => {
                        Some((receipt_id, info))
                    }
                    _ => None,
                },
            }
        })
        .collect();

    let receipt_ids: Vec<CryptoHash> = our_entries.iter().map(|(id, _)| *id).collect();
    let infos_before: Vec<&ReceiptToTxInfo> = our_entries.iter().map(|(_, info)| info).collect();

    // Kill and restart the node.
    let killed_node_state = env.kill_node(&restart_identifier);
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);
    let new_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_identifier, killed_node_state);

    // Verify entries persist after restart.
    let store = env.node_for_account(&restart_account).store();
    for (i, receipt_id) in receipt_ids.iter().enumerate() {
        let info_after = store
            .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
            .expect("receipt_to_tx should persist across restart in index-only mode");
        assert_eq!(
            infos_before[i], &info_after,
            "receipt_to_tx entry should be identical after restart"
        );
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Refund receipts (from unspent gas) should get a ReceiptToTx entry with
/// `ReceiptOrigin::FromReceipt` pointing to the parent action receipt.
#[test]
fn test_refund_receipt_has_receipt_to_tx() {
    init_test_logger();

    let user_account = create_account_id("account0");

    // Use a non-zero gas price so that unspent gas generates a balance refund receipt.
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(min_gas_price, min_gas_price)
        .build();

    let signer = create_user_test_signer(&user_account);

    // Deploy the test contract.
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call a cheap method with 300 TGas — most gas is unused, generating a refund.
    let call_tx = SignedTransaction::call(
        2,
        user_account.clone(),
        user_account.clone(),
        &signer,
        Balance::ZERO,
        "log_something".to_owned(),
        vec![],
        Gas::from_teragas(300),
        env.validator().head().last_block_hash,
    );
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();

    // The action receipt is the first receipt spawned by the transaction.
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // The action receipt should have spawned a gas refund receipt as its child.
    // Since log_something doesn't create any promises, the only child is the refund.
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == action_receipt_id)
        .expect("action receipt outcome should exist");
    assert!(
        !action_outcome.outcome.receipt_ids.is_empty(),
        "action receipt should have spawned a refund receipt from unspent gas"
    );
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];

    // Verify the refund receipt has a ReceiptToTx entry with FromReceipt origin.
    let store = env.validator().store();
    let info = store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, refund_receipt_id.as_ref())
        .expect("refund receipt should have a ReceiptToTx entry");

    let ReceiptToTxInfo::V1(v1) = &info;
    assert_eq!(v1.shard_id, ShardId::new(0), "shard_id should be 0 in single-shard setup");
    assert_eq!(v1.receiver_account_id, user_account, "receiver should be the user account");
    match &v1.origin {
        ReceiptOrigin::FromReceipt(origin) => {
            assert_eq!(
                origin.parent_receipt_id, action_receipt_id,
                "refund receipt's parent should be the action receipt"
            );
            assert_eq!(
                origin.parent_predecessor_id, user_account,
                "parent_predecessor_id should be the user account"
            );
        }
        other => panic!("expected FromReceipt origin for refund receipt, got {other:?}"),
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
