//! Per-request outcome budget enforcement for the hint-fallback scanner.

use super::*;

/// Synthetic stress case: a single OutcomeIds row has more entries than the
/// per-request hint-scan budget. The resolver should stop mid-scan and return
/// `BudgetExceeded` instead of scanning the whole row.
#[test]
fn test_hint_budget_exceeded_in_scan() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let configured_limit = env.validator().client().config.receipt_to_tx_max_outcomes_per_request;
    let block_height = env.validator().head().height;
    let block_hash = env.validator().head().last_block_hash;
    let shard_id = ShardId::new(0);
    let outcome_ids: Vec<CryptoHash> =
        (0..=configured_limit).map(|i| CryptoHash::hash_bytes(&i.to_le_bytes())).collect();
    let store = env.validator().store();
    let mut update = store.store_update();
    update.set_ser(DBCol::OutcomeIds, &get_block_shard_id(&block_hash, shard_id), &outcome_ids);
    update.commit();

    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"absent"),
            block_height: Some(block_height),
            shard_id: Some(shard_id),
            window: Some(0),
        },
    );
    match result {
        Err(GetReceiptToTxError::BudgetExceeded { scanned, limit }) => {
            assert_eq!(scanned, configured_limit);
            assert_eq!(limit, configured_limit);
        }
        other => panic!("expected BudgetExceeded, got {other:?}"),
    }
}
