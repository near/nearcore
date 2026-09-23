//! Per-request outcome budget enforcement for the hint-fallback scanner.

use super::*;

/// Synthetic stress: single OutcomeIds row > per-request budget. Resolver
/// stops mid-scan, returns `BudgetExceeded` instead of scanning whole row.
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

/// All-shards budget drain: height-only hint (no `shard_id`) → handler
/// enumerates every shard at hint height. Inject `limit / 4` synthetic
/// `OutcomeIds` on each of five shards — `5 * limit / 4` total, over budget
/// regardless of shard-iteration order. Shared budget exhausts mid-scan,
/// handler returns `BudgetExceeded { scanned, limit }`, proving budget is
/// shared across enumeration, not per-shard.
///
/// Numbers derived from `config.receipt_to_tx_max_outcomes_per_request` so
/// a future default change can't silently invalidate the test.
#[test]
fn test_hint_budget_exceeded_all_shards_drain() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .num_shards(5)
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let configured_limit = env.validator().client().config.receipt_to_tx_max_outcomes_per_request;
    assert!(
        configured_limit.is_multiple_of(4),
        "test math depends on limit being divisible by 4; got {configured_limit}"
    );
    let per_shard = configured_limit / 4;

    let block_height = env.validator().head().height;
    let block_hash = env.validator().head().last_block_hash;
    let queried_receipt_id = CryptoHash::hash_bytes(b"queried-receipt");

    let store = env.validator().store();
    let mut update = store.store_update();
    for shard_index in 0..5u64 {
        let shard_id = ShardId::new(shard_index);
        let drain_outcomes: Vec<CryptoHash> = (0..per_shard)
            .map(|i| CryptoHash::hash_bytes(&(shard_index * 1_000_000 + i).to_le_bytes()))
            .collect();
        update.set_ser(
            DBCol::OutcomeIds,
            &get_block_shard_id(&block_hash, shard_id),
            &drain_outcomes,
        );
    }
    update.commit();

    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: queried_receipt_id,
            block_height: Some(block_height),
            shard_id: None,
            window: Some(0),
        },
    );
    match result {
        Err(GetReceiptToTxError::BudgetExceeded { scanned, limit }) => {
            assert_eq!(scanned, configured_limit);
            assert_eq!(limit, configured_limit);
        }
        other => panic!("expected BudgetExceeded across the all-shards enumeration; got {other:?}"),
    }
}
