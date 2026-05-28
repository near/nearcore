//! Two-column tx/receipt classifier behavior under hint-scan resolution.

use super::*;

/// (true, true) origin-row collision: outcome id present in both Transactions
/// and Receipts. The classifier skips, the scan exhausts, terminal error is
/// `UnknownReceipt`.
#[test]
fn test_hint_classifier_skips_on_both_origin_rows_present() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let (tx_hash, receipt_id, height) = send_self_money(&mut env, &user_account, 1);

    // Force the (true, true) ambiguity by writing a fake receipt row at the
    // tx hash. The resolver must skip the ambiguous candidate; the scan
    // exhausts the window and returns `UnknownReceipt`.
    let store = env.validator().store();
    let mut update = store.store_update();
    let fake_receipt_bytes = vec![0u8; 64];
    update.increment_refcount(DBCol::Receipts, tx_hash.as_ref(), &fake_receipt_bytes);
    update.commit();

    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id,
            block_height: Some(height),
            shard_id: Some(ShardId::new(0)),
            window: Some(0),
        },
    );
    assert!(
        matches!(result, Err(GetReceiptToTxError::UnknownReceipt(_))),
        "ambiguous candidate must be skipped and the scan exhausted; got {result:?}"
    );
}
