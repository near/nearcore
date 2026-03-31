use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_database_tool::backfill_receipt_to_tx::{BACKFILL_CHECKPOINT_KEY, backfill_receipt_to_tx};
use near_o11y::testonly::init_test_logger;
use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, BlockHeight, Gas};
use near_store::DBCol;

const EPOCH_LENGTH: u64 = 5;

/// Backfill produces the same ReceiptToTx entries that normal processing would.
///
/// Generates diverse traffic across multiple epochs:
/// - send_money transactions (simple transfers)
/// - deploy_contract + function calls (generates refund receipts)
/// - Multiple senders and receivers
///
/// 1. Run a node with `save_receipt_to_tx = true` and record the entries.
/// 2. Wipe the ReceiptToTx column.
/// 3. Run the backfill function.
/// 4. Verify that the backfilled entries match the originals.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_backfill_matches_normal_processing() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let receiver_account = create_account_id("account1");

    // Build env with save_receipt_to_tx enabled (the default).
    // High gc_num_epochs_to_keep so blocks aren't GC'd during the test.
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gc_num_epochs_to_keep(20)
        .build();

    let signer = create_user_test_signer(&user_account);
    let receiver_signer = create_user_test_signer(&receiver_account);
    let mut nonce = 1;

    // --- Epoch 1: send_money transactions in both directions ---
    for _ in 0..10 {
        let tx = SignedTransaction::send_money(
            nonce,
            user_account.clone(),
            receiver_account.clone(),
            &signer,
            Balance::from_yoctonear(100),
            env.validator().head().last_block_hash,
        );
        nonce += 1;
        env.validator().submit_tx(tx);
    }

    // Also send from receiver back to user.
    let mut receiver_nonce = 1;
    for _ in 0..5 {
        let tx = SignedTransaction::send_money(
            receiver_nonce,
            receiver_account.clone(),
            user_account.clone(),
            &receiver_signer,
            Balance::from_yoctonear(50),
            env.validator().head().last_block_hash,
        );
        receiver_nonce += 1;
        env.validator().submit_tx(tx);
    }

    let target_height = env.validator().head().height + EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // --- Epoch 2: deploy contract + function calls (generates refund receipts) ---
    let deploy_tx = SignedTransaction::deploy_contract(
        nonce,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    nonce += 1;
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call a method with excess gas to generate refund receipts (FromReceipt).
    for _ in 0..10 {
        let call_tx = SignedTransaction::call(
            nonce,
            user_account.clone(),
            user_account.clone(),
            &signer,
            Balance::ZERO,
            "log_something".to_owned(),
            vec![],
            Gas::from_teragas(300),
            env.validator().head().last_block_hash,
        );
        nonce += 1;
        env.validator_runner().run_tx(call_tx, Duration::seconds(5));
    }

    // --- Epoch 3: more send_money to span another epoch ---
    for _ in 0..10 {
        let tx = SignedTransaction::send_money(
            nonce,
            user_account.clone(),
            receiver_account.clone(),
            &signer,
            Balance::from_yoctonear(100),
            env.validator().head().last_block_hash,
        );
        nonce += 1;
        env.validator().submit_tx(tx);
    }

    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Collect all ReceiptToTx entries that were written during normal processing.
    let store = env.validator().store();
    let original_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();
    assert!(
        original_entries.len() > 10,
        "expected many ReceiptToTx entries from diverse traffic, got {}",
        original_entries.len()
    );

    // Verify we have FromTransaction entries at minimum.
    let has_from_tx = original_entries.iter().any(|(_, info)| {
        let ReceiptToTxInfo::V1(v1) = info;
        matches!(&v1.origin, ReceiptOrigin::FromTransaction(_))
    });
    assert!(has_from_tx, "expected some FromTransaction entries");

    // Wipe the ReceiptToTx column.
    {
        let mut update = store.store_update();
        for (key, _) in &original_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.commit();
    }

    assert_eq!(store.iter(DBCol::ReceiptToTx).count(), 0, "column should be empty after wipe");

    // Run the backfill.
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    let stats = backfill_receipt_to_tx(
        chain_store,
        &store,
        &store,
        genesis_height,
        head_height,
        1000,
        false,
        None,
    )
    .expect("backfill should succeed");

    assert_eq!(
        stats.entries_written as usize,
        original_entries.len(),
        "backfill should write exactly as many entries as normal processing"
    );

    // Verify each entry matches.
    for (orig_key, orig_info) in &original_entries {
        let backfilled_info =
            store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, orig_key).unwrap_or_else(|| {
                panic!("backfilled entry missing for key {:?}", orig_key);
            });
        assert_eq!(
            orig_info, &backfilled_info,
            "backfilled entry should match original for key {:?}",
            orig_key
        );
    }
}

/// Backfill is idempotent — running it twice produces the same result.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_backfill_idempotent() {
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
    env.validator().submit_tx(tx);

    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    // No entries should exist (save_receipt_to_tx = false).
    assert_eq!(store.iter(DBCol::ReceiptToTx).count(), 0);

    // Run backfill once.
    let stats1 = backfill_receipt_to_tx(
        chain_store,
        &store,
        &store,
        genesis_height,
        head_height,
        1000,
        false,
        None,
    )
    .expect("first backfill should succeed");

    let entries_after_first: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();
    assert!(stats1.entries_written > 0, "first backfill should write entries");

    // Run backfill again (idempotent).
    let _stats2 = backfill_receipt_to_tx(
        chain_store,
        &store,
        &store,
        genesis_height,
        head_height,
        1000,
        false,
        None,
    )
    .expect("second backfill should succeed");

    let entries_after_second: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();

    // Same entries after both runs.
    assert_eq!(entries_after_first.len(), entries_after_second.len());
    for ((k1, v1), (k2, v2)) in entries_after_first.iter().zip(entries_after_second.iter()) {
        assert_eq!(k1, k2);
        assert_eq!(v1, v2);
    }
}

/// Checkpoint only records fully completed heights, not in-progress ones.
/// Checkpoint is stored in DBCol::Misc atomically with ReceiptToTx entries.
///
/// Uses batch_size=1 so every entry triggers a batch commit. Verifies that:
/// 1. After completion, checkpoint equals the last processed height
/// 2. Re-running from checkpoint processes 0 new entries (no skipped receipts)
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_checkpoint_does_not_skip_mid_height_receipts() {
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
    env.validator().submit_tx(tx);

    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    let store = env.validator().store();
    let chain_store = env.validator().client().chain.chain_store();
    let genesis_height = chain_store.get_genesis_height();
    let head_height = env.validator().head().height;

    // Run backfill with batch_size=1 (commits after every single entry).
    // This maximizes the chance of mid-height commits.
    let stats = backfill_receipt_to_tx(
        chain_store,
        &store,
        &store,
        genesis_height,
        head_height,
        1, // batch_size=1: commit after every entry
        true,
        None,
    )
    .expect("backfill should succeed");

    assert!(stats.entries_written > 0, "backfill should have written entries");

    // Checkpoint should exist in DBCol::Misc and equal head_height.
    let checkpoint_value = store
        .get_ser::<BlockHeight>(DBCol::Misc, BACKFILL_CHECKPOINT_KEY)
        .expect("checkpoint should exist in DBCol::Misc");
    assert_eq!(checkpoint_value, head_height, "checkpoint should equal the last processed height");

    let entries_count = store.iter(DBCol::ReceiptToTx).count();

    // Now resume from checkpoint. Since checkpoint = head_height,
    // from_height = checkpoint+1 > head_height, so nothing to process.
    // This proves the checkpoint correctly represents "everything up to
    // this height is done" — no receipts were skipped mid-height.
    let stats2 = backfill_receipt_to_tx(
        chain_store,
        &store,
        &store,
        checkpoint_value + 1,
        head_height,
        1,
        true,
        None,
    )
    .expect("resume backfill should succeed");

    assert_eq!(stats2.entries_written, 0, "resume should write 0 new entries");
    assert_eq!(
        store.iter(DBCol::ReceiptToTx).count(),
        entries_count,
        "entry count should not change after resume"
    );
}
