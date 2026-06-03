//! Hint-fallback walks: tests exercising optional `(block_height,
//! shard_id, window)` hint params of `EXPERIMENTAL_receipt_to_tx`. Column
//! path is source of truth when populated; hint scan is fallback.

use super::*;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::setups::derive_new_epoch_config_from_boundary;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_crypto::{KeyType, PublicKey};
use near_primitives::action::{Action, TransferAction};
use near_primitives::epoch_manager::{
    DynamicReshardingConfig, EpochConfigStore, ShardLayoutConfig,
};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV0};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{ExecutionOutcome, ExecutionOutcomeWithProof};
use near_primitives::types::BlockHeight;
use near_primitives::utils::get_outcome_id_block_hash;
use near_primitives::version::PROTOCOL_VERSION;
use std::collections::BTreeMap;
use std::sync::Arc;

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
/// Under spice the cross-shard tx outcome lands on a different block than the
/// receipt's hint height, so the scan misses.
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
/// Under spice the refund + action receipts land on different blocks than the
/// standard model, so the computed hint coords don't match the outcome rows the
/// scan inspects.
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
/// receipt's `parent_predecessor_id` → sender shard (plus ancestors it split
/// out of), so the scan targets that small lineage and finds the originating
/// tx without enumerating all shards.
///
/// Under spice the cross-shard refund / action receipts land on different blocks
/// than the standard model, so the hint coords don't line up with the outcome
/// rows the resolver scans.
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

/// Adjacent header pair straddling a static shard split, shared by the resharding
/// hint tests. `boundary_account` lives on the splitting shard, so its pre-split
/// id is the retired parent. Serving node (node 0) tracks every shard, GC off →
/// pre-split block + outcome rows stay readable for a post-split query.
struct ReshardBoundary {
    env: TestLoopEnv,
    /// First post-split height (parent retired). Query anchor; `h_pre == h_anchor - 1`.
    h_anchor: BlockHeight,
    /// Hash at the last pre-split height (parent still present).
    block_hash_pre: CryptoHash,
    /// Hash at `h_anchor`.
    block_hash_anchor: CryptoHash,
    /// `boundary_account`'s pre-split shard — the retired parent.
    parent_shard_id: ShardId,
    /// `boundary_account`'s post-split shard.
    child_shard_id: ShardId,
    /// Post-split shard that did NOT split → its own parent
    /// (`try_get_parent_shard_id(s) == Ok(Some(s))`), the input that loops a
    /// naive parent-walk forever.
    unchanged_shard_id: ShardId,
}

/// Resharding mode for `setup_reshard_boundary`. Both retire `boundary_account`'s
/// shard identically (parent gone from `shard_ids()`, children map back), so
/// everything downstream is shared.
#[derive(Clone, Copy)]
enum ReshardKind {
    /// Static V2: protocol upgrade swaps in a layout derived from `boundary_account`.
    /// Split point known ahead of time, content-independent.
    Static,
    /// Dynamic V3: `force_split_shards` splits the shard at runtime. Split point is
    /// trie-derived, not `boundary_account` (which only picks the shard + a post-split
    /// child); fires only if the target shard has splittable state.
    Dynamic,
}

/// Split `boundary_account`'s shard (V2 or V3 per `kind`), run until the parent is
/// retired at head, then pin the straddle pair from ACTUAL header layouts: adjacent
/// heights where the parent id is present, then retired. Not epoch arithmetic — that
/// risks an anchor still resolving pre-split, passing by accident.
fn setup_reshard_boundary(kind: ReshardKind, boundary_account: &AccountId) -> ReshardBoundary {
    let epoch_length: u64 = 7;
    let base_shard_layout = ShardLayout::multi_shard(3, 3);

    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);
    let mut genesis_builder = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .epoch_length(epoch_length);
    if matches!(kind, ReshardKind::Dynamic) {
        // V3 force-split fires only if the target shard has splittable trie state:
        // `find_mem_usage_split` returns `NotFound` on a sparse shard → no split →
        // parent never retires → flip scan panics. Seed accounts co-located with
        // `boundary_account` (all sort before "test1", the first `multi_shard(3, 3)`
        // boundary). V2 is content-independent, needs none.
        let split_shard_accounts: Vec<AccountId> =
            (0..8).map(|i| format!("account{i}").parse().unwrap()).collect();
        genesis_builder = genesis_builder
            .add_user_accounts_simple(&split_shard_accounts, Balance::from_near(1_000_000));
    }
    let genesis = genesis_builder.build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();
    // Needed before building the store: V3 forces the split on this shard id.
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(boundary_account);

    let epoch_config_store = match kind {
        ReshardKind::Static => {
            let (new_epoch_config, _) =
                derive_new_epoch_config_from_boundary(&base_epoch_config, boundary_account);
            EpochConfigStore::test(BTreeMap::from_iter(vec![
                (genesis.config.protocol_version, Arc::new(base_epoch_config)),
                (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
            ]))
        }
        ReshardKind::Dynamic => {
            let mut dynamic_epoch_config = base_epoch_config.clone();
            dynamic_epoch_config.shard_layout_config = ShardLayoutConfig::Dynamic {
                dynamic_resharding_config: DynamicReshardingConfig {
                    memory_usage_threshold: u64::MAX,
                    min_child_memory_usage: u64::MAX,
                    max_number_of_shards: 100,
                    min_epochs_between_resharding: 1.try_into().unwrap(),
                    force_split_shards: vec![parent_shard_id],
                    block_split_shards: vec![],
                },
            };
            EpochConfigStore::test(BTreeMap::from_iter(vec![
                (genesis.config.protocol_version, Arc::new(base_epoch_config)),
                (genesis.config.protocol_version + 1, Arc::new(dynamic_epoch_config)),
            ]))
        }
    };

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        // Serving node must track every shard across the split, else the
        // handler rejects the query.
        .track_all_shards()
        // Keep the pre-split block + outcome rows alive for the query; 20 exceeds
        // V3's later activation.
        .gc_num_epochs_to_keep(20)
        .config_modifier(|config, _| {
            // Force the column miss so the hint scan runs. `save_tx_outcomes`
            // stays default (true), else the scan errors before reading storage.
            config.save_receipt_to_tx = false;
        })
        .build();

    // Run until the parent is retired at head (⇔ split active). Kind-agnostic: V3
    // has no precomputed layout to compare. V3 activates later (~epoch 4 vs ~2),
    // so wider budget.
    let timeout_epochs = match kind {
        ReshardKind::Static => 8,
        ReshardKind::Dynamic => 12,
    };
    let epoch_manager = env.validator().client().epoch_manager.clone();
    env.validator_runner().run_until(
        |node| {
            let epoch_id = node.head().epoch_id;
            let layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
            !layout.shard_ids().any(|s| s == parent_shard_id)
        },
        Duration::seconds((timeout_epochs * epoch_length) as i64),
    );

    let head_height = env.validator().head().height;

    let (h_anchor, block_hash_pre, block_hash_anchor, child_shard_id, unchanged_shard_id) = {
        let client = env.validator().client();
        let layout_has_parent = |height: u64| -> Option<bool> {
            let header = client.chain.get_block_header_by_height(height).ok()?;
            let layout = epoch_manager.get_shard_layout(header.epoch_id()).ok()?;
            Some(layout.shard_ids().any(|s| s == parent_shard_id))
        };

        let mut flip = None;
        for h in 1..head_height {
            if layout_has_parent(h) == Some(true) && layout_has_parent(h + 1) == Some(false) {
                flip = Some((h, h + 1));
                break;
            }
        }
        let (h_pre, h_anchor) =
            flip.expect("must find an adjacent height pair where the parent shard is retired");

        // Assert the flip explicitly (retired-parent guarantee).
        let pre_header = client.chain.get_block_header_by_height(h_pre).unwrap();
        let pre_layout = epoch_manager.get_shard_layout(pre_header.epoch_id()).unwrap();
        let post_header = client.chain.get_block_header_by_height(h_anchor).unwrap();
        let post_layout = epoch_manager.get_shard_layout(post_header.epoch_id()).unwrap();
        assert!(
            pre_layout.shard_ids().any(|s| s == parent_shard_id),
            "pre-split layout must contain the parent shard"
        );
        assert!(
            !post_layout.shard_ids().any(|s| s == parent_shard_id),
            "post-split layout must have retired the parent shard"
        );
        assert_eq!(
            h_anchor - h_pre,
            1,
            "anchor must be the immediate successor of the pre-split height"
        );

        let block_hash_pre = client.chain.get_block_hash_by_height(h_pre).unwrap();
        let block_hash_anchor = client.chain.get_block_hash_by_height(h_anchor).unwrap();
        let child_shard_id = post_layout.account_id_to_shard_id(boundary_account);
        // An unchanged shard maps to itself in the post-split parent map (the
        // split touched only `parent_shard_id`).
        let unchanged_shard_id = post_layout
            .shard_ids()
            .find(|&s| matches!(post_layout.try_get_parent_shard_id(s), Ok(Some(p)) if p == s))
            .expect("post-split layout must retain at least one unchanged shard");
        (h_anchor, block_hash_pre, block_hash_anchor, child_shard_id, unchanged_shard_id)
    };

    ReshardBoundary {
        env,
        h_anchor,
        block_hash_pre,
        block_hash_anchor,
        parent_shard_id,
        child_shard_id,
        unchanged_shard_id,
    }
}

/// Single-hop `FromTransaction` across a reshard, shared by the V2 and V3 variants:
/// a producing tx outcome on the pre-split parent shard must stay resolvable from a
/// hint scan anchored *after* the split. Pre-fix the scan used the anchor-height
/// layout only → the retired parent's `OutcomeIds` went unread → wrong
/// `UnknownReceipt`; `resolve_scan_shards` now unions every layout the window spans.
/// Exercises the `Enumerate` (no-hint) and `Hint` (child id) seeds; `Account` is
/// covered by `test_hint_multi_hop_resolves_across_resharding`. One synthetic
/// tx-origin outcome on the parent shard, queried immediately — no cross-boundary
/// execution, GC, or block-placement noise.
fn assert_single_hop_resolves_across_reshard(
    boundary_account: &AccountId,
    boundary: ReshardBoundary,
) {
    let ReshardBoundary {
        mut env, h_anchor, block_hash_pre, parent_shard_id, child_shard_id, ..
    } = boundary;

    // Inject one synthetic tx-origin outcome on the retired parent shard, into
    // node 0's store (the single validator).
    let tx_outcome_id = CryptoHash::hash_bytes(b"reshard-hint-synthetic-tx");
    let child_receipt_id = CryptoHash::hash_bytes(b"reshard-hint-synthetic-child-receipt");
    let child_receipt = Receipt::new_balance_refund(boundary_account, Balance::ZERO);

    let store = env.validator().store();
    let outcome_ids_key = get_block_shard_id(&block_hash_pre, parent_shard_id);
    // Append, don't overwrite: H_pre is a real block whose parent-shard
    // chunk already has outcome rows.
    let mut outcome_ids =
        store.get_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, &outcome_ids_key).unwrap_or_default();
    assert!(!outcome_ids.contains(&tx_outcome_id), "synthetic outcome id must be unique");
    outcome_ids.push(tx_outcome_id);

    let mut update = store.store_update();
    update.set_ser(DBCol::OutcomeIds, &outcome_ids_key, &outcome_ids);
    update.insert_ser(
        DBCol::TransactionResultForBlock,
        &get_outcome_id_block_hash(&tx_outcome_id, &block_hash_pre),
        &ExecutionOutcomeWithProof {
            proof: vec![],
            outcome: ExecutionOutcome {
                receipt_ids: vec![child_receipt_id],
                executor_id: boundary_account.clone(),
                ..Default::default()
            },
        },
    );
    // Present in Transactions, absent in Receipts → classifier reads
    // `(true, false)` = FromTransaction with `tx_hash = tx_outcome_id`.
    update.increment_refcount(DBCol::Transactions, tx_outcome_id.as_ref(), &[0u8; 1]);
    // The queried child receipt must decode as a real `Receipt`.
    update.increment_refcount(
        DBCol::Receipts,
        child_receipt_id.as_ref(),
        &borsh::to_vec(&child_receipt).unwrap(),
    );
    update.commit();

    // Positive control: an explicit retired-parent hint scans that shard, so it
    // resolves regardless of the fix — proves the synthetic rows are valid and
    // guards against a broken fixture passing the asserts below for the wrong reason.
    let control = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: child_receipt_id,
            block_height: Some(h_anchor),
            shard_id: Some(parent_shard_id),
            window: Some(5),
        },
    );
    assert_eq!(
        control
            .expect(
                "positive control: explicit parent-shard hint must resolve the synthetic outcome"
            )
            .transaction_hash,
        tx_outcome_id,
        "fixture sanity: producing outcome is readable when the retired parent shard is scanned directly"
    );

    // Query anchored POST-split. Pre-fix both miss: `None` walks the anchor layout
    // (children only, parent unscanned); `Some(child)` scans only the child unless
    // the fix maps it back through ancestor layouts.
    for shard_hint in [None, Some(child_shard_id)] {
        let result = handle(
            &mut env,
            GetReceiptToTx {
                receipt_id: child_receipt_id,
                block_height: Some(h_anchor),
                shard_id: shard_hint,
                window: Some(5),
            },
        );
        assert_eq!(
            result.expect("hint scan must resolve across the reshard").transaction_hash,
            tx_outcome_id,
            "shard hint {shard_hint:?}: producing outcome on the retired parent shard must resolve"
        );
    }
}

/// Single-hop across a STATIC V2 reshard — the version the fix is most often
/// exercised against.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_resolves_across_resharding() {
    init_test_logger();
    let boundary_account: AccountId = "boundary".parse().unwrap();
    assert_single_hop_resolves_across_reshard(
        &boundary_account,
        setup_reshard_boundary(ReshardKind::Static, &boundary_account),
    );
}

/// Single-hop across a DYNAMIC V3 reshard — the only test asserting the live
/// dynamic mode (the rest drive static V2). Split point is trie-derived, so
/// `setup_reshard_boundary` seeds splittable state + reads the child id at runtime.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn slow_test_hint_resolves_across_dynamic_resharding() {
    init_test_logger();
    let boundary_account: AccountId = "boundary".parse().unwrap();
    assert_single_hop_resolves_across_reshard(
        &boundary_account,
        setup_reshard_boundary(ReshardKind::Dynamic, &boundary_account),
    );
}

/// Resharding boundary, multi-hop: the `Account` seed (`FromReceipt` reseed) must
/// reach a producing tx on the retired parent shard one hop after the walk
/// crossed onto a post-split child shard.
///
/// 2-hop synthetic chain on the shared straddle:
///   - Hop 1, CHILD shard at `h_anchor`: `child_receipt_id` produced by
///     `parent_receipt_id` (predecessor `boundary_account`). In `Receipts`, not
///     `Transactions` → `FromReceipt` → reseed `Account(boundary_account)`,
///     switch to `Scan::Ancestor`.
///   - Hop 2, RETIRED PARENT shard at `h_pre`: `parent_receipt_id` produced by
///     `tx_id`. In `Transactions`, not `Receipts` → `FromTransaction` (terminal).
///
/// Hop 2 works only because `resolve_scan_shards` unions
/// `account_id_to_shard_id(boundary_account)` across every layout the ancestor
/// window (`[h_anchor - max_hop_distance, h_anchor]`) spans: pre-split `h_pre`
/// still maps `boundary_account` → parent. The pre-fix reseed used only the
/// post-split layout (→ child), so hop 2 scanned the child and missed the tx.
///
/// `derive_new_epoch_config_from_boundary` derives a STATIC (V2) layout → proves
/// the static half; V3 rides the same code, not asserted here.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_multi_hop_resolves_across_resharding() {
    init_test_logger();

    let boundary_account: AccountId = "boundary".parse().unwrap();
    let ReshardBoundary {
        mut env,
        h_anchor,
        block_hash_pre,
        block_hash_anchor,
        parent_shard_id,
        child_shard_id,
        ..
    } = setup_reshard_boundary(ReshardKind::Static, &boundary_account);

    // Terminal tx, the hop-2 receipt it produced, and the queried child receipt
    // that receipt produced.
    let tx_id = CryptoHash::hash_bytes(b"reshard-multihop-tx");
    let parent_receipt_id = CryptoHash::hash_bytes(b"reshard-multihop-parent-receipt");
    let child_receipt_id = CryptoHash::hash_bytes(b"reshard-multihop-child-receipt");

    // Hop-2 receipt must decode with `predecessor_id == boundary_account` so the
    // `FromReceipt` reseed targets boundary's shard lineage. `new_balance_refund`
    // hardcodes predecessor "system", so build the V0 literal. `receiver_id ==
    // boundary_account` is coherent (executes on boundary's child shard at
    // `h_anchor`). `Balance::ZERO` — balance fields are `Balance`, not int.
    let parent_receipt = Receipt::V0(ReceiptV0 {
        predecessor_id: boundary_account.clone(),
        receiver_id: boundary_account.clone(),
        receipt_id: parent_receipt_id,
        receipt: ReceiptEnum::Action(ActionReceipt {
            signer_id: boundary_account.clone(),
            signer_public_key: PublicKey::empty(KeyType::ED25519),
            gas_price: Balance::ZERO,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![Action::Transfer(TransferAction { deposit: Balance::ZERO })],
        }),
    });
    // The queried child receipt only needs to decode — the hint scan reads it to
    // fill `receiver_account_id`; its predecessor is irrelevant to the walk.
    let child_receipt = Receipt::new_balance_refund(&boundary_account, Balance::ZERO);

    let store = env.validator().store();

    // Read existing OutcomeIds rows (both are real blocks with chunk outcomes)
    // before opening the update; append, don't overwrite.
    let anchor_ids_key = get_block_shard_id(&block_hash_anchor, child_shard_id);
    let mut anchor_ids =
        store.get_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, &anchor_ids_key).unwrap_or_default();
    assert!(!anchor_ids.contains(&parent_receipt_id), "synthetic outcome id must be unique");
    anchor_ids.push(parent_receipt_id);

    let pre_ids_key = get_block_shard_id(&block_hash_pre, parent_shard_id);
    let mut pre_ids =
        store.get_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, &pre_ids_key).unwrap_or_default();
    assert!(!pre_ids.contains(&tx_id), "synthetic outcome id must be unique");
    pre_ids.push(tx_id);

    let mut update = store.store_update();

    // Hop 1 — post-split child shard at `h_anchor`. `parent_receipt_id`
    // produces `child_receipt_id`; present in Receipts, absent in Transactions
    // → FromReceipt(parent_predecessor_id = boundary_account).
    update.set_ser(DBCol::OutcomeIds, &anchor_ids_key, &anchor_ids);
    update.insert_ser(
        DBCol::TransactionResultForBlock,
        &get_outcome_id_block_hash(&parent_receipt_id, &block_hash_anchor),
        &ExecutionOutcomeWithProof {
            proof: vec![],
            outcome: ExecutionOutcome {
                receipt_ids: vec![child_receipt_id],
                executor_id: boundary_account.clone(),
                ..Default::default()
            },
        },
    );
    update.increment_refcount(
        DBCol::Receipts,
        parent_receipt_id.as_ref(),
        &borsh::to_vec(&parent_receipt).unwrap(),
    );
    update.increment_refcount(
        DBCol::Receipts,
        child_receipt_id.as_ref(),
        &borsh::to_vec(&child_receipt).unwrap(),
    );

    // Hop 2 — retired parent shard at `h_pre`. `tx_id` produces
    // `parent_receipt_id`; present in Transactions, absent in Receipts →
    // FromTransaction with `tx_hash = tx_id`, `sender = boundary_account`.
    update.set_ser(DBCol::OutcomeIds, &pre_ids_key, &pre_ids);
    update.insert_ser(
        DBCol::TransactionResultForBlock,
        &get_outcome_id_block_hash(&tx_id, &block_hash_pre),
        &ExecutionOutcomeWithProof {
            proof: vec![],
            outcome: ExecutionOutcome {
                receipt_ids: vec![parent_receipt_id],
                executor_id: boundary_account.clone(),
                ..Default::default()
            },
        },
    );
    update.increment_refcount(DBCol::Transactions, tx_id.as_ref(), &[0u8; 1]);
    update.commit();

    // Query the child receipt, anchored POST-split, no shard hint. Hop 1 finds
    // `parent_receipt_id` on the child shard and reseeds to
    // `Account(boundary_account)`; hop 2's ancestor window spans `h_pre`, whose
    // pre-split layout maps `boundary_account` to the retired parent, where
    // `tx_id` is found.
    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: child_receipt_id,
            block_height: Some(h_anchor),
            shard_id: None,
            window: Some(5),
        },
    )
    .expect("multi-hop walk must resolve across the reshard to the terminal tx");
    assert_eq!(
        response.transaction_hash, tx_id,
        "hop 2 must find the producing tx on the retired parent shard"
    );
    assert_eq!(response.sender_account_id, boundary_account);
}

/// Resharding boundary, self-parenting shard hint: a hint naming a shard that
/// did NOT split must not hang the parent-shard walk.
///
/// `try_get_parent_shard_id` reports an unchanged shard (and a V3 non-split-child)
/// as its own parent, so the lineage walk in `resolve_scan_shards` must stop at
/// that fixed point. Without the guard it spins forever on the post-split layout;
/// with it the walk terminates and, finding no outcome, returns `UnknownReceipt`.
///
/// Regression guard: revert the `parent == id` break → this test hangs (the
/// test-loop has no wall-clock escape from a synchronous CPU loop).
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_self_parenting_shard_does_not_loop() {
    init_test_logger();

    let boundary_account: AccountId = "boundary".parse().unwrap();
    let ReshardBoundary { mut env, h_anchor, unchanged_shard_id, .. } =
        setup_reshard_boundary(ReshardKind::Static, &boundary_account);

    // Hint the unchanged shard with a window spanning the post-split layout,
    // where it is its own parent. No synthetic outcome is injected, so a
    // terminating walk must report `UnknownReceipt` rather than hang.
    let receipt_id = CryptoHash::hash_bytes(b"reshard-self-parent-absent-receipt");
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id,
            block_height: Some(h_anchor),
            shard_id: Some(unchanged_shard_id),
            window: Some(5),
        },
    );
    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => assert_eq!(id, receipt_id),
        other => panic!("expected UnknownReceipt from a terminating walk, got {other:?}"),
    }
}

/// Forward gap: mirror of the backward single-hop. Producer lives FORWARD on a
/// post-split CHILD while the hint names the pre-split PARENT. `hint_lineage` walks
/// UP, never DOWN, so the parent hint misses the child; the no-hint `Enumerate` seed
/// covers it (unions every layout's `shard_ids`). Tx-origin injected on the CHILD at
/// `h_anchor`, query anchored at `h_anchor - 1` so `CenterOut`'s `+window` reaches it.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_hint_forward_gap() {
    init_test_logger();

    let boundary_account: AccountId = "boundary".parse().unwrap();
    let ReshardBoundary {
        mut env,
        h_anchor,
        block_hash_anchor,
        parent_shard_id,
        child_shard_id,
        ..
    } = setup_reshard_boundary(ReshardKind::Static, &boundary_account);

    // Inject tx-origin on the POST-split child shard at the anchor block (forward
    // mirror of the backward test). Child shard has real outcome rows to append to.
    let tx_outcome_id = CryptoHash::hash_bytes(b"reshard-forward-gap-synthetic-tx");
    let child_receipt_id = CryptoHash::hash_bytes(b"reshard-forward-gap-synthetic-child-receipt");
    let child_receipt = Receipt::new_balance_refund(&boundary_account, Balance::ZERO);

    let store = env.validator().store();
    let outcome_ids_key = get_block_shard_id(&block_hash_anchor, child_shard_id);
    let mut outcome_ids =
        store.get_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds, &outcome_ids_key).unwrap_or_default();
    assert!(!outcome_ids.contains(&tx_outcome_id), "synthetic outcome id must be unique");
    outcome_ids.push(tx_outcome_id);

    let mut update = store.store_update();
    update.set_ser(DBCol::OutcomeIds, &outcome_ids_key, &outcome_ids);
    update.insert_ser(
        DBCol::TransactionResultForBlock,
        &get_outcome_id_block_hash(&tx_outcome_id, &block_hash_anchor),
        &ExecutionOutcomeWithProof {
            proof: vec![],
            outcome: ExecutionOutcome {
                receipt_ids: vec![child_receipt_id],
                executor_id: boundary_account.clone(),
                ..Default::default()
            },
        },
    );
    update.increment_refcount(DBCol::Transactions, tx_outcome_id.as_ref(), &[0u8; 1]);
    update.increment_refcount(
        DBCol::Receipts,
        child_receipt_id.as_ref(),
        &borsh::to_vec(&child_receipt).unwrap(),
    );
    update.commit();

    // Anchor one block before the split (`h_anchor - 1 == h_pre`); `CenterOut`'s
    // `+window` then reaches `h_anchor` where the producer lives.
    let anchor = h_anchor - 1;

    // No hint → `Enumerate` unions every layout, incl. the post-split child → resolves.
    let response = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: child_receipt_id,
            block_height: Some(anchor),
            shard_id: None,
            window: Some(5),
        },
    )
    .expect("no-hint enumerate must resolve the forward-child producer across the reshard");
    assert_eq!(response.transaction_hash, tx_outcome_id);
    assert_eq!(response.sender_account_id, boundary_account);

    // Documents the forward-gap boundary: a pre-split parent hint scans up (parent +
    // ancestors), never the descendant child, so it misses. Flip if the `Hint` seed
    // ever walks descendants.
    let result = handle(
        &mut env,
        GetReceiptToTx {
            receipt_id: child_receipt_id,
            block_height: Some(anchor),
            shard_id: Some(parent_shard_id),
            window: Some(5),
        },
    );
    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => assert_eq!(id, child_receipt_id),
        other => panic!("expected UnknownReceipt from the parent-only hint walk, got {other:?}"),
    }
}
