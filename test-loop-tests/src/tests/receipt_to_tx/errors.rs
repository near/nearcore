//! Error-path tests: `WindowTooLarge`, `MalformedHint`, `OutcomesNotStored`,
//! `DepthExceeded`, and unresolvable-height handling.

use super::*;

/// `window > receipt_to_tx_max_hint_window` is rejected up front with
/// `WindowTooLarge`.
#[test]
fn test_hint_fallback_window_too_large() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();

    // ClientConfig::test() seeds receipt_to_tx_max_hint_window to 20.
    let configured_max: BlockHeightDelta = 20;
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"any"),
            block_height: Some(100),
            shard_id: Some(ShardId::new(0)),
            window: Some(configured_max + 1),
        },
    );
    match result {
        Err(GetReceiptToTxError::WindowTooLarge { requested, maximum }) => {
            assert_eq!(requested, configured_max + 1);
            assert_eq!(maximum, configured_max);
        }
        other => panic!("expected WindowTooLarge, got {other:?}"),
    }
}

/// `block_height` near 0 + a wide window must not panic on underflow. The
/// center-out iterator saturates at 0; receipt lookup just misses.
#[test]
fn test_hint_fallback_window_underflow() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();

    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"absent"),
            block_height: Some(2),
            shard_id: Some(ShardId::new(0)),
            window: Some(10),
        },
    );
    assert!(
        matches!(result, Err(GetReceiptToTxError::UnknownReceipt(_))),
        "underflow-safe scan should miss cleanly, got {result:?}"
    );
}

/// `save_receipt_to_tx=false`, `save_tx_outcomes=false`, hint provided →
/// `OutcomesNotStored` (fires only because the scan was attempted).
#[test]
fn test_hint_fallback_outcomes_not_stored_only_on_scan() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
            config.save_tx_outcomes = false;
        })
        .build();

    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"any"),
            block_height: Some(100),
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    );
    assert!(
        matches!(result, Err(GetReceiptToTxError::OutcomesNotStored)),
        "expected OutcomesNotStored, got {result:?}"
    );
}

/// `shard_id` without `block_height` → `MalformedHint`.
#[test]
fn test_hint_malformed_only_shard_id() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"any"),
            block_height: None,
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    );
    assert!(
        matches!(result, Err(GetReceiptToTxError::MalformedHint(_))),
        "expected MalformedHint, got {result:?}"
    );
}

/// `window` without `block_height` → `MalformedHint`.
#[test]
fn test_hint_malformed_window_without_height() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"any"),
            block_height: None,
            shard_id: None,
            window: Some(5),
        },
    );
    assert!(
        matches!(result, Err(GetReceiptToTxError::MalformedHint(_))),
        "expected MalformedHint, got {result:?}"
    );
}

/// Hint height past the chain head (not locally resolvable) must surface as
/// `UnknownReceipt`. The handler must not fall back to the head epoch's
/// shard layout, which could scan the wrong shards on a post-resharding node.
#[test]
fn test_hint_height_unresolvable_returns_unknown_receipt() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let head_height = env.validator().head().height;
    // Height that doesn't exist locally (well past the head). The handler
    // should not silently fall back to head_header.
    let bogus_height = head_height + 1_000_000;
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"any"),
            block_height: Some(bogus_height),
            shard_id: None,
            window: Some(0),
        },
    );
    match result {
        Err(GetReceiptToTxError::UnknownReceipt(_)) => {}
        other => panic!("expected UnknownReceipt for unresolvable hint height, got {other:?}"),
    }
}

/// Handler-level test: write synthetic ReceiptToTx rows forming a chain of
/// 1001 FromReceipt entries (exceeding the MAX_DEPTH=1000 limit). Verify
/// that DepthExceeded is returned with the originally queried receipt_id.
#[test]
fn test_receipt_to_tx_depth_exceeded() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();

    let store = env.validator().store();
    let mut store_update = store.store_update();

    // Build a chain of 1002 receipt IDs: receipt_0 → receipt_1 → ... → receipt_1001.
    // receipt_0 through receipt_1000 are FromReceipt pointing to the next.
    // receipt_1001 is FromTransaction (the terminal node — but we'll never reach it).
    let chain_len = 1002usize;
    let receipt_ids: Vec<CryptoHash> =
        (0..chain_len).map(|i| CryptoHash::hash_bytes(&(i as u32).to_le_bytes())).collect();

    // Write the terminal node (receipt_101 → tx).
    store_update.insert_ser(
        DBCol::ReceiptToTx,
        receipt_ids[chain_len - 1].as_ref(),
        &ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                tx_hash: CryptoHash::hash_bytes(b"tx"),
                sender_account_id: "sender".parse().unwrap(),
            }),
            receiver_account_id: "receiver".parse().unwrap(),
            shard_id: ShardId::new(0),
        }),
    );

    // Write each intermediate node (receipt_i → receipt_{i+1}).
    for i in 0..chain_len - 1 {
        store_update.insert_ser(
            DBCol::ReceiptToTx,
            receipt_ids[i].as_ref(),
            &ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                    parent_receipt_id: receipt_ids[i + 1],
                    parent_predecessor_id: "system".parse().unwrap(),
                }),
                receiver_account_id: "receiver".parse().unwrap(),
                shard_id: ShardId::new(0),
            }),
        );
    }

    store_update.commit();

    // Query receipt_0 — needs 1001 hops to reach the tx, exceeding MAX_DEPTH=1000.
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    let result = view_client.handle(receipt_to_tx_req(receipt_ids[0]));

    match result {
        Err(GetReceiptToTxError::DepthExceeded { receipt_id, limit }) => {
            assert_eq!(
                receipt_id, receipt_ids[0],
                "error should report the originally queried receipt"
            );
            assert_eq!(limit, 1000, "limit should be MAX_DEPTH=1000");
        }
        other => panic!("expected DepthExceeded error, got: {other:?}"),
    }

    // Sanity check: querying receipt_2 (999 FromReceipt hops + 1 terminal lookup
    // = 1000 iterations) should succeed since it's exactly at the limit.
    let result = view_client.handle(receipt_to_tx_req(receipt_ids[2]));
    assert!(result.is_ok(), "100 hops should succeed, got: {result:?}");
    let response = result.unwrap();
    assert_eq!(response.transaction_hash, CryptoHash::hash_bytes(b"tx"));
    assert_eq!(response.sender_account_id.as_str(), "sender");
}
