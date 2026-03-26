use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_database_tool::backfill_receipt_to_tx::backfill_receipt_to_tx;
use near_o11y::testonly::init_test_logger;
use near_primitives::receipt::{ReceiptOrigin, ReceiptOriginTransaction, ReceiptToTxInfo};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::Balance;
use near_store::DBCol;

const EPOCH_LENGTH: u64 = 5;

/// Backfill produces the same ReceiptToTx entries that normal processing would.
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
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .build();

    let signer = create_user_test_signer(&user_account);
    let block_hash = env.validator().head().last_block_hash;

    // Submit a send_money transaction (creates a FromTransaction receipt).
    let tx = SignedTransaction::send_money(
        1,
        user_account,
        receiver_account,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.validator().submit_tx(tx);

    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Collect all ReceiptToTx entries that were written during normal processing.
    let store = env.validator().store();
    let original_entries: Vec<(Vec<u8>, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(k, v)| (k.to_vec(), v))
        .collect();
    assert!(
        !original_entries.is_empty(),
        "expected some ReceiptToTx entries from normal processing"
    );

    // Verify the first receipt has FromTransaction origin.
    let outcome = env.validator().client().chain.get_execution_outcome(&tx_hash).unwrap();
    let first_receipt_id = outcome.outcome_with_id.outcome.receipt_ids[0];
    let first_entry = store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, first_receipt_id.as_ref())
        .expect("first receipt should have ReceiptToTx entry");
    let ReceiptToTxInfo::V1(ref v1) = first_entry;
    assert!(
        matches!(&v1.origin, ReceiptOrigin::FromTransaction(ReceiptOriginTransaction { tx_hash: h, .. }) if *h == tx_hash),
        "first receipt should originate from the transaction"
    );

    // Wipe the ReceiptToTx column.
    {
        let mut update = store.store_update();
        for (key, _) in &original_entries {
            update.delete(DBCol::ReceiptToTx, key);
        }
        update.commit();
    }

    // Verify it's wiped.
    assert_eq!(
        store.iter(DBCol::ReceiptToTx).count(),
        0,
        "ReceiptToTx column should be empty after wipe"
    );

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
        None, // no checkpoint file in tests
        None, // no progress bar in tests
    )
    .expect("backfill should succeed");

    assert!(stats.entries_written > 0, "backfill should have written entries");

    // Verify backfill produced the same number of entries as normal processing.
    assert_eq!(
        original_entries.len(),
        store.iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx).count(),
        "backfill should produce the same number of entries as normal processing"
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
        None,
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
        None,
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
