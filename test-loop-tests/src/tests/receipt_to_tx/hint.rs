//! Hint-fallback walks: tests exercising optional `(block_height,
//! shard_id, window)` hint params of `EXPERIMENTAL_receipt_to_tx`. Column
//! path is source of truth when populated; hint scan is fallback.

use super::*;

/// `save_receipt_to_tx=false`, hint at tx execution height → terminal tx
/// returned. Single-hop column miss falls back to scan, walks same outcome
/// locally.
#[test]
fn test_hint_fallback_resolves_tx_origin() {
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

    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id,
            block_height: Some(height),
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    )
    .expect("hint resolves to tx origin");
    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// `save_receipt_to_tx=false`, height-only hint, 2-shard setup. Handler
/// doesn't know creating shard → hop 1 enumerates all shards at hint
/// height + finds tx outcome.
///
/// Gated off under spice: spice execution model places cross-shard tx
/// outcome on different block than receipt's hint height; scan misses.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_height_only_resolves_all_shards() {
    init_test_logger();
    let sender_account = create_account_id("account0");
    let receiver_account: AccountId = "test1".parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .num_shards(2)
        .add_user_account(&sender_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let signer = create_user_test_signer(&sender_account);
    let tx = SignedTransaction::send_money(
        1,
        sender_account.clone(),
        receiver_account,
        &signer,
        Balance::from_yoctonear(100),
        env.validator().head().last_block_hash,
    );
    let tx_hash = tx.get_hash();
    let outcome = env.validator_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let tx_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&outcome.transaction_outcome.block_hash)
        .unwrap()
        .height();

    let response = handle(
        &mut env,
        GetReceiptToTx { receipt_id, block_height: Some(tx_height), shard_id: None, window: None },
    )
    .expect("height-only hint scans all shards, resolves to tx origin");
    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, sender_account);
}

/// `save_receipt_to_tx=false`, contract refund chain (depth 2). Hint at
/// action receipt's execution height. Handler walks both hops server-side
/// via repeated hint scans.
///
/// Gated off under spice: spice model produces refund + action receipts
/// on different blocks than standard model; computed hint coords don't
/// match outcome rows scan inspects.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_fallback_resolves_through_refund_chain() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .gas_prices(min_gas_price, min_gas_price)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let signer = create_user_test_signer(&user_account);
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

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
    let call_tx_hash = call_tx.get_hash();
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();

    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: refund_receipt_id,
            block_height: Some(action_height),
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    )
    .expect("hint walk resolves refund → action receipt → tx");
    assert_eq!(response.transaction_hash, call_tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// Cross-shard depth-2 walk, `save_receipt_to_tx=false`. Hop 1 uses
/// supplied action shard to resolve refund → action receipt. Hop-2 scan
/// shard-narrowed via handler's predecessor-account derivation: action
/// receipt's `parent_predecessor_id` resolves to sender shard, ancestor
/// scan goes straight to shard 0 + finds originating tx without enumerating
/// all shards.
///
/// Gated off under spice: spice model lands cross-shard refund / action
/// receipts on different blocks than standard; hint coords don't line up
/// with outcome rows resolver scans.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_cross_shard_walk_resolves_via_predecessor_shard() {
    init_test_logger();
    let sender_account = create_account_id("account0");
    let receiver_account: AccountId = "test1".parse().unwrap();
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .num_shards(2)
        .add_user_account(&sender_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .gas_prices(min_gas_price, min_gas_price)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let receiver_signer = create_user_test_signer(&receiver_account);
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &receiver_account,
        near_test_contracts::rs_contract().to_vec(),
        &receiver_signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    let sender_signer = create_user_test_signer(&sender_account);
    let call_tx = SignedTransaction::call(
        1,
        sender_account.clone(),
        receiver_account,
        &sender_signer,
        Balance::ZERO,
        "log_something".to_owned(),
        vec![],
        Gas::from_teragas(300),
        env.validator().head().last_block_hash,
    );
    let call_tx_hash = call_tx.get_hash();
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();
    let action_shard = shard_containing_outcome(
        &env,
        action_height,
        action_receipt_id,
        &[ShardId::new(0), ShardId::new(1)],
    );

    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: refund_receipt_id,
            block_height: Some(action_height),
            shard_id: Some(action_shard),
            window: None,
        },
    )
    .expect("cross-shard refund chain resolves via all-shards ancestor scan");
    assert_eq!(response.transaction_hash, call_tx_hash);
    assert_eq!(response.sender_account_id, sender_account);
}

/// `save_receipt_to_tx=true`, `save_tx_outcomes=false`, hint supplied but
/// unused — column resolves every hop. Must succeed; `OutcomesNotStored`
/// fires only when scan is needed.
#[test]
fn test_hint_with_column_populated_save_tx_outcomes_false_succeeds() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_tx_outcomes = false;
        })
        .build();

    let signer = create_user_test_signer(&user_account);
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account,
        &signer,
        Balance::from_yoctonear(100),
        env.validator().head().last_block_hash,
    );
    let tx_hash = tx.get_hash();
    env.validator().submit_tx(tx);
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Find receipt_id from ReceiptToTx column directly — outcomes not stored.
    let store = env.validator().store();
    let (receipt_id, _) = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .find_map(|(k, info)| match &info {
            ReceiptToTxInfo::V1(v1) => match &v1.origin {
                ReceiptOrigin::FromTransaction(o) if o.tx_hash == tx_hash => {
                    Some((CryptoHash::try_from(k.as_ref()).unwrap(), info))
                }
                _ => None,
            },
        })
        .expect("column entry exists");

    // Hint that, if triggered, would hit OutcomesNotStored. Column path
    // answers first, ignores hint.
    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id,
            block_height: Some(1),
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    )
    .expect("column hit must short-circuit OutcomesNotStored");
    assert_eq!(response.transaction_hash, tx_hash);
}

/// Hint far outside receipt's window → `UnknownReceipt` (no column entry,
/// scan window exhausted).
#[test]
fn test_hint_fallback_wrong_height() {
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

    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1);
    let bogus_hint = height + 100;
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id,
            block_height: Some(bogus_hint),
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    );
    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => {
            assert_eq!(id, receipt_id, "reports queried receipt that wasn't found");
        }
        other => panic!("expected UnknownReceipt, got {other:?}"),
    }
}

/// Synthetic chain: column has child → FromReceipt(P), column entry for P
/// absent. Hint supplied. Next iter's column-miss scan picks up P's coords
/// + resolves terminally. Regression guard for mixed column-hit /
/// hint-fallback walks.
#[test]
fn test_hint_column_then_fallback_boundary() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .gas_prices(min_gas_price, min_gas_price)
        .build();

    let signer = create_user_test_signer(&user_account);
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

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
    let call_tx_hash = call_tx.get_hash();
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();

    // Delete column entry for parent (action_receipt_id) → next iter
    // column-misses, falls back to hint scan.
    let store = env.validator().store();
    let mut store_update = store.store_update();
    store_update.delete(DBCol::ReceiptToTx, action_receipt_id.as_ref());
    store_update.commit();
    assert!(
        store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, action_receipt_id.as_ref()).is_none(),
        "test setup: action receipt's column entry must be gone"
    );

    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: refund_receipt_id,
            block_height: Some(action_height),
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    )
    .expect("column hit then next-hop column-miss scan resolves to terminal tx");
    assert_eq!(response.transaction_hash, call_tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// Cross-shard hint walk: 2-shard setup, `save_receipt_to_tx=false`,
/// cross-shard transfer (sender shard 0, receiver shard 1).
///
/// Action receipt executes on *receiver's* shard; tx executes on
/// *sender's* shard. Hint at action receipt's execution shard can't find
/// tx outcome on originating shard — center-out scan walks wrong shard's
/// `OutcomeIds` rows.
///
/// Hint exactly at action receipt's execution coords. Hint scan misses
/// (tx outcome on other shard), no column entry, handler returns
/// `UnknownReceipt` at cross-shard boundary rather than fabricating.
///
/// Regression guard for documented best-effort failure mode. Future
/// shard-aware hint derivation (e.g. via
/// `parent.predecessor_id() -> account_id_to_shard_id`) → update test
/// to reflect new contract.
#[test]
fn test_hint_fallback_cross_shard_returns_unknown_receipt() {
    init_test_logger();
    let sender_account = create_account_id("account0");
    let receiver_account: AccountId = "test1".parse().unwrap();

    let mut env = TestLoopBuilder::new()
        .num_shards(2)
        .add_user_account(&sender_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let signer = create_user_test_signer(&sender_account);
    let tx = SignedTransaction::send_money(
        1,
        sender_account,
        receiver_account,
        &signer,
        Balance::from_yoctonear(100),
        env.validator().head().last_block_hash,
    );
    let outcome = env.validator_runner().execute_tx(tx, Duration::seconds(10)).unwrap();

    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == action_receipt_id)
        .expect("action receipt outcome exists");
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();

    // Identify which shard action receipt executed on. Don't hardcode
    // layout — multi_shard(2) puts boundary at "test1" but future layout
    // change still leaves test exercising cross-shard scenario.
    let action_shard = shard_containing_outcome(
        &env,
        action_height,
        action_receipt_id,
        &[ShardId::new(0), ShardId::new(1)],
    );

    // Query action receipt itself, hint at *its* execution shard. Tx
    // outcome that produced this receipt lives on the *other* shard,
    // so hint scan walks wrong shard's outcomes + misses.
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    let result = view_client.handle(GetReceiptToTx {
        receipt_id: action_receipt_id,
        block_height: Some(action_height),
        shard_id: Some(action_shard),
        window: None,
    });

    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => {
            assert_eq!(
                id, action_receipt_id,
                "expected UnknownReceipt for the action receipt; its producing tx is on the other shard"
            );
        }
        other => panic!(
            "expected UnknownReceipt at the cross-shard hop; got {other:?}. \
             If a future change adds shard-aware hint advancement, update this test."
        ),
    }
}

/// Stale-hint fall-through: next-hop column-miss scan misses AND no later
/// column entry hits → walk returns `UnknownReceipt` rather than fabricate.
///
/// Sibling scenario (refresh misses, column hits, terminal Ok) covered by
/// `test_hint_column_then_fallback_boundary`.
#[test]
fn test_hint_stale_then_column_miss_returns_unknown() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .gas_prices(min_gas_price, min_gas_price)
        .build();

    let signer = create_user_test_signer(&user_account);
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    let call_tx = SignedTransaction::call(
        2,
        user_account.clone(),
        user_account,
        &signer,
        Balance::ZERO,
        "log_something".to_owned(),
        vec![],
        Gas::from_teragas(300),
        env.validator().head().last_block_hash,
    );
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();

    // Delete column entry for action_receipt_id (parent walk recurses on).
    // save_receipt_to_tx still on → hop 1 column-hits for refund_receipt_id,
    // returns FromReceipt(action_receipt_id). Hop 2 column-misses parent;
    // with wildly stale hint, scan also misses → walk surfaces UnknownReceipt.
    let store = env.validator().store();
    let mut store_update = store.store_update();
    store_update.delete(DBCol::ReceiptToTx, action_receipt_id.as_ref());
    store_update.commit();

    let stale_height = action_height + 10 * EPOCH_LENGTH;
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: refund_receipt_id,
            block_height: Some(stale_height),
            shard_id: Some(ShardId::new(0)),
            window: Some(2),
        },
    );
    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => {
            assert_eq!(id, action_receipt_id);
        }
        other => {
            panic!("expected UnknownReceipt at the stale-hint cross-hop miss, got {other:?}")
        }
    }
}

/// 2-hop self-call walk with ancestor scan distance pinned to 0. Hint
/// resolver visits only anchor height for hop 1+ → test passes only if
/// producing outcome of parent receipt lives at the anchor itself. For
/// same-account self-call, `process_local_receipts` runs action receipt
/// in same `apply()` call as emitting tx → both outcomes share a block.
/// Locks anchor-inclusion invariant at integration layer.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_ancestor_includes_anchor() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .gas_prices(min_gas_price, min_gas_price)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
            // Anchor-only: hop 1+ inspects only resolved parent's own
            // execution block. Successful walk proves anchor visited.
            config.receipt_to_tx_max_hop_distance = 0;
        })
        .build();

    let signer = create_user_test_signer(&user_account);
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

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
    let call_tx_hash = call_tx.get_hash();
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();

    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: refund_receipt_id,
            block_height: Some(action_height),
            shard_id: Some(ShardId::new(0)),
            window: None,
        },
    )
    .expect("anchor-only ancestor scan resolves local same-shard receipts");
    assert_eq!(response.transaction_hash, call_tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// Inject emit→execute delay > `receipt_to_tx_max_hop_distance` via
/// cross-shard transfer. `max_hop_distance=0`, caller `window=0` →
/// next-iter column-miss scan in Ancestor mode can't reach tx's producing
/// block. Walk surfaces `UnknownReceipt`. Documents fail-fast: ancestor
/// misses when configured distance is too tight for actual emit-to-execute
/// delay.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_ancestor_distance_misses_when_delay_exceeds_config() {
    init_test_logger();
    let sender_account = create_account_id("account0");
    let receiver_account: AccountId = "test1".parse().unwrap();
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .num_shards(2)
        .add_user_account(&sender_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .gas_prices(min_gas_price, min_gas_price)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
            // Distance 0 collapses ancestor scan to anchor-only.
            config.receipt_to_tx_max_hop_distance = 0;
        })
        .build();

    let signer = create_user_test_signer(&sender_account);
    let tx = SignedTransaction::send_money(
        1,
        sender_account,
        receiver_account,
        &signer,
        Balance::from_yoctonear(100),
        env.validator().head().last_block_hash,
    );
    let outcome = env.validator_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();
    let action_shard = shard_containing_outcome(
        &env,
        action_height,
        action_receipt_id,
        &[ShardId::new(0), ShardId::new(1)],
    );

    // window=0 forces hop 0 to inspect anchor height only; combined with
    // max_hop_distance=0 walk has zero slack to reach cross-shard tx
    // outcome (executed a block earlier).
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: action_receipt_id,
            block_height: Some(action_height),
            shard_id: Some(action_shard),
            window: Some(0),
        },
    );
    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => {
            assert_eq!(id, action_receipt_id);
        }
        other => panic!("expected UnknownReceipt under tight max_hop_distance, got {other:?}"),
    }
}

/// Operator override: raise `receipt_to_tx_max_hint_window` past default,
/// verify `window` over old default is accepted instead of rejected by
/// up-front cap check. Locks operator-tunable contract on new field.
#[test]
fn test_hint_window_config_override() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.receipt_to_tx_max_hint_window = 50;
        })
        .build();

    // 30 > default cap (20), < operator-raised cap (50). Handler accepts;
    // no matching receipt → walk terminates UnknownReceipt, not
    // WindowTooLarge.
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: CryptoHash::hash_bytes(b"absent"),
            block_height: Some(100),
            shard_id: Some(ShardId::new(0)),
            window: Some(30),
        },
    );
    assert!(
        matches!(result, Err(GetReceiptToTxError::UnknownReceipt(_))),
        "window=30 must be accepted under max_hint_window=50; got {result:?}"
    );
}

/// Pin load-bearing band for hop-1+ ancestor scan width:
/// `d ∈ (effective_window, max_hop_distance]` where
/// `d = parent_execution_height − grandparent_execution_height`.
///
/// Setup mirrors `test_hint_cross_shard_walk_resolves_via_predecessor_shard`
/// (cross-shard contract call producing refund chain) but tightens caller's
/// window to 0 so `effective_window = 0 < d ≤ max_hop_distance = 20`.
/// Natural cross-shard delay puts producing tx outcome a block earlier
/// than action receipt on sender shard.
///
/// Hop 0 finds action receipt at `(action_height, action_shard)` with
/// `window = 0` (anchor only). Without hop-1+ ancestor scan width
/// (`max_hop_distance = 20`) covering gap between action receipt and its
/// producing tx, walk would terminate `UnknownReceipt`. Pins load-bearing
/// claim so future restructure preserving same coverage keeps passing.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_ancestor_gap_band() {
    init_test_logger();
    let sender_account = create_account_id("account0");
    let receiver_account: AccountId = "test1".parse().unwrap();
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .num_shards(2)
        .add_user_account(&sender_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .gas_prices(min_gas_price, min_gas_price)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let receiver_signer = create_user_test_signer(&receiver_account);
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &receiver_account,
        near_test_contracts::rs_contract().to_vec(),
        &receiver_signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    let sender_signer = create_user_test_signer(&sender_account);
    let call_tx = SignedTransaction::call(
        1,
        sender_account.clone(),
        receiver_account,
        &sender_signer,
        Balance::ZERO,
        "log_something".to_owned(),
        vec![],
        Gas::from_teragas(300),
        env.validator().head().last_block_hash,
    );
    let call_tx_hash = call_tx.get_hash();
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
    let action_height = env
        .validator()
        .client()
        .chain
        .get_block_header(&action_outcome.block_hash)
        .unwrap()
        .height();
    let action_shard = shard_containing_outcome(
        &env,
        action_height,
        action_receipt_id,
        &[ShardId::new(0), ShardId::new(1)],
    );

    // window=0 → effective_window=0 < d on hop 1+. Walk succeeds only
    // because hop-1+ ancestor scan width (max_hop_distance default 20)
    // reaches producing tx outcome a block earlier on sender shard.
    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: refund_receipt_id,
            block_height: Some(action_height),
            shard_id: Some(action_shard),
            window: Some(0),
        },
    )
    .expect("ancestor gap band must be covered by hop-1+ scan width");
    assert_eq!(response.transaction_hash, call_tx_hash);
    assert_eq!(response.sender_account_id, sender_account);
}
