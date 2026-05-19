use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_client_primitives::types::{
    GetReceiptParentByHint, GetReceiptParentByHintError, GetReceiptParentByHintResponse,
};
use near_jsonrpc_primitives::types::receipts::{
    ReceiptOriginView, RpcReceiptParentByHintRequest, RpcReceiptParentByHintResponse,
};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ReceiptOrigin, ReceiptToTxInfo};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Gas, ShardId};

const EPOCH_LENGTH: u64 = 5;

/// Send the GetReceiptParentByHint message directly to the validator's
/// ViewClientActor. Returns the typed result, mirroring the RPC path but
/// avoiding the jsonrpc round-trip for tests that don't need it.
fn handle_message(
    env: &mut crate::setup::env::TestLoopEnv,
    msg: GetReceiptParentByHint,
) -> Result<GetReceiptParentByHintResponse, GetReceiptParentByHintError> {
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    view_client.handle(msg)
}

/// Send a simple send_money tx and return (tx_hash, receipt_id, executed_height).
/// `executed_height` is the height at which the tx outcome landed (== receipt creation block).
fn send_self_money(
    env: &mut crate::setup::env::TestLoopEnv,
    user_account: &AccountId,
    nonce: u64,
    use_rpc: bool,
) -> (CryptoHash, CryptoHash, u64) {
    let signer = create_user_test_signer(user_account);
    let head = if use_rpc { env.rpc_node().head() } else { env.validator().head() };
    let tx = SignedTransaction::send_money(
        nonce,
        user_account.clone(),
        user_account.clone(),
        &signer,
        Balance::from_yoctonear(100),
        head.last_block_hash,
    );
    let tx_hash = tx.get_hash();
    let outcome = if use_rpc {
        env.rpc_runner().execute_tx(tx, Duration::seconds(10)).unwrap()
    } else {
        env.validator_runner().execute_tx(tx, Duration::seconds(10)).unwrap()
    };
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let executed_height = outcome.transaction_outcome.block_hash;
    let height = if use_rpc {
        env.rpc_node().client().chain.get_block_header(&executed_height).unwrap().height()
    } else {
        env.validator().client().chain.get_block_header(&executed_height).unwrap().height()
    };
    (tx_hash, receipt_id, height)
}

/// `save_receipt_to_tx=false` + hint with the tx execution block → FromTransaction.
#[test]
fn test_hint_resolves_tx_origin_no_column() {
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

    let (tx_hash, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    let response = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height,
            shard_id: ShardId::new(0),
            window: None,
        },
    )
    .expect("hint should resolve to tx origin");

    let ReceiptToTxInfo::V1(v1) = response.info;
    match v1.origin {
        ReceiptOrigin::FromTransaction(origin) => {
            assert_eq!(origin.tx_hash, tx_hash);
            assert_eq!(origin.sender_account_id, user_account);
        }
        other => panic!("expected FromTransaction, got {other:?}"),
    }
}

/// `save_receipt_to_tx=false` + a contract refund receipt (depth 2). Hint =
/// action receipt's execution height → FromReceipt origin.
#[test]
fn test_hint_resolves_receipt_origin_no_column() {
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
    let outcome = env.validator_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome =
        outcome.receipts_outcome.iter().find(|r| r.id == action_receipt_id).unwrap();
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
    let action_block_hash = action_outcome.block_hash;
    let action_height =
        env.validator().client().chain.get_block_header(&action_block_hash).unwrap().height();

    let response = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id: refund_receipt_id,
            block_height: action_height,
            shard_id: ShardId::new(0),
            window: None,
        },
    )
    .expect("hint should resolve to receipt origin");

    let ReceiptToTxInfo::V1(v1) = response.info;
    match v1.origin {
        ReceiptOrigin::FromReceipt(origin) => {
            assert_eq!(origin.parent_receipt_id, action_receipt_id);
            assert_eq!(origin.parent_predecessor_id, user_account);
        }
        other => panic!("expected FromReceipt origin, got {other:?}"),
    }
}

/// Hint height far outside the receipt's creation window → not found.
#[test]
fn test_hint_wrong_height_hard_error() {
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

    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    // Hint a height far above the actual execution height, beyond the default ±5 window.
    let bogus_hint = height + 100;
    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: bogus_hint,
            shard_id: ShardId::new(0),
            window: None,
        },
    );
    match result {
        Err(GetReceiptParentByHintError::ReceiptNotFoundInHintWindow {
            effective_window, ..
        }) => {
            assert_eq!(effective_window, 5);
        }
        other => panic!("expected ReceiptNotFoundInHintWindow, got {other:?}"),
    }
}

/// Correct height, wrong shard → not found (the OutcomeIds row is keyed by
/// shard_id, so a wrong shard is empty).
#[test]
fn test_hint_wrong_shard_hard_error() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let mut env = TestLoopBuilder::new()
        .num_shards(2)
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    // Look up which shard hosted the receipt's creating outcome by scanning both shards,
    // then query the *other* shard. Robust to changes in shard-layout boundaries.
    let chain_store = env.validator().client().chain.chain_store();
    let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
    let host_shard = [ShardId::new(0), ShardId::new(1)]
        .into_iter()
        .find(|sid| {
            !chain_store.get_outcomes_by_block_hash_and_shard_id(&block_hash, *sid).is_empty()
        })
        .expect("one of the shards must host outcomes at the tx height");
    let wrong_shard = if host_shard == ShardId::new(0) { ShardId::new(1) } else { ShardId::new(0) };

    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height,
            shard_id: wrong_shard,
            window: None,
        },
    );
    match result {
        Err(GetReceiptParentByHintError::ReceiptNotFoundInHintWindow { shard_id, .. }) => {
            assert_eq!(shard_id, wrong_shard);
        }
        other => panic!("expected ReceiptNotFoundInHintWindow, got {other:?}"),
    }
}

/// Correct shard, off-by-(window+1) height → not found.
#[test]
fn test_hint_correct_shard_wrong_height() {
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

    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);
    let bogus_height = height + 6;

    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: bogus_height,
            shard_id: ShardId::new(0),
            window: Some(5),
        },
    );
    match result {
        Err(GetReceiptParentByHintError::ReceiptNotFoundInHintWindow {
            effective_window, ..
        }) => {
            assert_eq!(effective_window, 5);
        }
        other => panic!("expected ReceiptNotFoundInHintWindow, got {other:?}"),
    }
}

/// `window=0` + exact-height success; `window=0` + off-by-one fail.
#[test]
fn test_hint_window_override() {
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

    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    // Exact height + window=0 succeeds.
    let ok = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height,
            shard_id: ShardId::new(0),
            window: Some(0),
        },
    );
    assert!(ok.is_ok(), "exact-height window=0 should succeed: {ok:?}");

    // Off-by-one with window=0 fails.
    let fail = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height + 1,
            shard_id: ShardId::new(0),
            window: Some(0),
        },
    );
    match fail {
        Err(GetReceiptParentByHintError::ReceiptNotFoundInHintWindow {
            effective_window, ..
        }) => {
            assert_eq!(effective_window, 0);
        }
        other => panic!("expected ReceiptNotFoundInHintWindow, got {other:?}"),
    }
}

/// `block_height=2`, `window=10` — must not panic on underflow.
#[test]
fn test_hint_window_underflow() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();
    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id: CryptoHash::hash_bytes(b"absent"),
            block_height: 2,
            shard_id: ShardId::new(0),
            window: Some(10),
        },
    );
    match result {
        Err(GetReceiptParentByHintError::ReceiptNotFoundInHintWindow {
            effective_window, ..
        }) => {
            assert_eq!(effective_window, 10);
        }
        other => panic!("expected ReceiptNotFoundInHintWindow, got {other:?}"),
    }
}

/// `window=MAX_HINT_WINDOW+1` → WindowTooLarge.
#[test]
fn test_hint_window_too_large() {
    init_test_logger();
    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();
    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id: CryptoHash::hash_bytes(b"any"),
            block_height: 100,
            shard_id: ShardId::new(0),
            window: Some(near_chain::receipt_to_tx::MAX_HINT_WINDOW + 1),
        },
    );
    match result {
        Err(GetReceiptParentByHintError::WindowTooLarge { requested, maximum }) => {
            assert_eq!(requested, near_chain::receipt_to_tx::MAX_HINT_WINDOW + 1);
            assert_eq!(maximum, near_chain::receipt_to_tx::MAX_HINT_WINDOW);
        }
        other => panic!("expected WindowTooLarge, got {other:?}"),
    }
}

/// `save_tx_outcomes=false` → OutcomesNotStored.
#[test]
fn test_hint_outcomes_not_stored() {
    init_test_logger();
    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .config_modifier(|config, _| {
            config.save_tx_outcomes = false;
        })
        .build();
    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id: CryptoHash::hash_bytes(b"any"),
            block_height: 100,
            shard_id: ShardId::new(0),
            window: None,
        },
    );
    assert!(
        matches!(result, Err(GetReceiptParentByHintError::OutcomesNotStored)),
        "expected OutcomesNotStored, got {result:?}"
    );
}

/// Bug-coverage for the static-vs-epoch-aware predicate split. A node with
/// `TrackedShardsConfig::Accounts(...)` whose account lives on the queried shard
/// **must succeed**. The earlier static `tracks_shard()` predicate returned
/// `false` for the `Accounts` variant unconditionally; the epoch-aware
/// `rpc_tracks_shard_at_epoch` correctly returns `true` because the account is
/// on the queried shard. If this test regresses to `ShardNotTracked`, the
/// handler is back on the broken static predicate.
#[test]
fn test_hint_shard_tracked_via_account() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let tracked_accounts = vec![user_account.clone()];
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(move |config, _| {
            config.save_receipt_to_tx = false;
            config.tracked_shards_config = TrackedShardsConfig::Accounts(tracked_accounts.clone());
        })
        .build();

    let (tx_hash, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    let response = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height,
            shard_id: ShardId::new(0),
            window: None,
        },
    )
    .expect("Accounts variant should be recognized by the epoch-aware tracking check");

    let ReceiptToTxInfo::V1(v1) = response.info;
    match v1.origin {
        ReceiptOrigin::FromTransaction(origin) => {
            assert_eq!(origin.tx_hash, tx_hash);
            assert_eq!(origin.sender_account_id, user_account);
        }
        other => panic!("expected FromTransaction, got {other:?}"),
    }
}

/// The response surfaces the parent outcome's execution coordinates so the
/// caller can drive recursion without needing an out-of-band block lookup. For
/// a single-hop tx-origin resolution, those coordinates equal the input hint
/// (the parent tx executed at the same height we scanned).
#[test]
fn test_hint_response_carries_parent_outcome_coords() {
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

    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    let response = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height,
            shard_id: ShardId::new(0),
            window: None,
        },
    )
    .expect("hint should resolve");

    assert_eq!(
        response.outcome_block_height, height,
        "parent tx executed at the hinted height; response should echo it"
    );
    assert_eq!(response.outcome_shard_id, ShardId::new(0));
}

/// Multi-hop chain resolves through repeated hint calls (caller-driven recursion).
#[test]
fn test_hint_recursion_pattern() {
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

    // Hop 1: refund_receipt_id -> action_receipt_id (FromReceipt).
    let hop1 = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id: refund_receipt_id,
            block_height: action_height,
            shard_id: ShardId::new(0),
            window: None,
        },
    )
    .unwrap();
    let parent_receipt_id = match &hop1.info {
        ReceiptToTxInfo::V1(v1) => match &v1.origin {
            ReceiptOrigin::FromReceipt(o) => o.parent_receipt_id,
            ReceiptOrigin::FromTransaction(_) => panic!("hop 1 should be FromReceipt"),
        },
    };
    assert_eq!(parent_receipt_id, action_receipt_id);

    // Hop 2: action_receipt_id -> call_tx_hash (FromTransaction). The caller
    // drives recursion using only hop 1's response — no external block lookup.
    // Hop 1 says the action receipt executed at hop1.outcome_block_height; the
    // action receipt was created one block earlier (where the tx executed), so
    // the center-out scan with the default window picks it up.
    let hop2 = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id: parent_receipt_id,
            block_height: hop1.outcome_block_height,
            shard_id: hop1.outcome_shard_id,
            window: None,
        },
    )
    .unwrap();
    match hop2.info {
        ReceiptToTxInfo::V1(v1) => match v1.origin {
            ReceiptOrigin::FromTransaction(o) => {
                assert_eq!(o.tx_hash, call_tx_hash);
                assert_eq!(o.sender_account_id, user_account);
            }
            other => panic!("hop 2 should be FromTransaction, got {other:?}"),
        },
    }
}

/// Wire-format JSON snapshot tests for ReceiptOriginView and RpcReceiptParentByHintError.
#[test]
fn test_hint_wire_format_snapshots() {
    let from_tx = ReceiptOriginView::FromTransaction {
        tx_hash: CryptoHash::hash_bytes(b"tx"),
        sender_account_id: "alice".parse().unwrap(),
    };
    let v = serde_json::to_value(&from_tx).unwrap();
    assert_eq!(v["kind"], "from_transaction");
    assert!(v["tx_hash"].is_string());
    assert_eq!(v["sender_account_id"], "alice");

    let from_receipt = ReceiptOriginView::FromReceipt {
        parent_receipt_id: CryptoHash::hash_bytes(b"r"),
        parent_predecessor_id: "bob".parse().unwrap(),
    };
    let v = serde_json::to_value(&from_receipt).unwrap();
    assert_eq!(v["kind"], "from_receipt");
    assert!(v["parent_receipt_id"].is_string());
    assert_eq!(v["parent_predecessor_id"], "bob");

    use near_jsonrpc_primitives::types::receipts::RpcReceiptParentByHintError;
    // Unit variant: only `name` is emitted.
    let v = serde_json::to_value(RpcReceiptParentByHintError::OutcomesNotStored).unwrap();
    assert_eq!(v["name"], "OUTCOMES_NOT_STORED");
    assert!(v.get("info").is_none(), "unit variant should not have `info`: {v:?}");

    // Struct variants: `name` + `info` object with all fields.
    let v = serde_json::to_value(RpcReceiptParentByHintError::ShardNotTracked {
        shard_id: ShardId::new(7),
    })
    .unwrap();
    assert_eq!(v["name"], "SHARD_NOT_TRACKED");
    assert_eq!(v["info"]["shard_id"], 7);

    let v = serde_json::to_value(RpcReceiptParentByHintError::WindowTooLarge {
        requested: 51,
        maximum: 50,
    })
    .unwrap();
    assert_eq!(v["name"], "WINDOW_TOO_LARGE");
    assert_eq!(v["info"]["requested"], 51);
    assert_eq!(v["info"]["maximum"], 50);

    let v = serde_json::to_value(RpcReceiptParentByHintError::ReceiptNotFoundInHintWindow {
        receipt_id: CryptoHash::hash_bytes(b"r"),
        block_height: 100,
        shard_id: ShardId::new(0),
        effective_window: 5,
    })
    .unwrap();
    assert_eq!(v["name"], "RECEIPT_NOT_FOUND_IN_HINT_WINDOW");
    assert_eq!(v["info"]["block_height"], 100);
    assert_eq!(v["info"]["shard_id"], 0);
    assert_eq!(v["info"]["effective_window"], 5);
    assert!(v["info"]["receipt_id"].is_string());

    let v = serde_json::to_value(RpcReceiptParentByHintError::InternalError {
        error_message: "x".into(),
    })
    .unwrap();
    assert_eq!(v["name"], "INTERNAL_ERROR");
    assert_eq!(v["info"]["error_message"], "x");
}

/// 3-block synthesized window with missing data scattered around — the resolver
/// must skip and continue until a real hit appears.
#[test]
fn test_hint_skips_missing_data() {
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

    // A real receipt at a real height; we'll hint a couple of blocks away so the
    // resolver must walk past empty `OutcomeIds` rows before hitting the right one.
    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);
    let off_by_two = if height > 2 { height - 2 } else { 0 };

    let response = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: off_by_two,
            shard_id: ShardId::new(0),
            window: None,
        },
    )
    .expect("center-out scan should pick up the real height within ±5");
    let ReceiptToTxInfo::V1(v1) = response.info;
    assert!(matches!(v1.origin, ReceiptOrigin::FromTransaction(_)));
}

/// The classifier refuses to guess when an outcome id appears in BOTH
/// `DBCol::Transactions` and `DBCol::Receipts`. Such collisions can't occur
/// in normal chain operation (the two id spaces are disjoint by construction)
/// but they can be reached via corrupted storage or bad backfill. The old
/// `is_tx = exists(Transactions)` short-circuit silently treated such an
/// outcome as a transaction; the two-column classifier logs an error and
/// skips, so the scan exhausts the window and returns the typed "not found"
/// error instead of fabricating a bogus origin.
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

    let (tx_hash, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    // Force the (true, true) ambiguity: the tx's outcome_id (== tx_hash) lives
    // in `DBCol::Transactions` naturally; impersonate a receipt with the same
    // hash by inserting bytes into `DBCol::Receipts` under the same key.
    let store = env.validator().store();
    let mut update = store.store_update();
    let fake_receipt_bytes = vec![0u8; 64];
    update.increment_refcount(near_store::DBCol::Receipts, tx_hash.as_ref(), &fake_receipt_bytes);
    update.commit();

    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height,
            shard_id: ShardId::new(0),
            window: Some(0),
        },
    );
    assert!(
        matches!(result, Err(GetReceiptParentByHintError::ReceiptNotFoundInHintWindow { .. })),
        "the only outcome candidate is ambiguous and should be skipped, expected \
         ReceiptNotFoundInHintWindow, got {result:?}"
    );
}

/// `save_receipt_to_tx=true` env where the column has the entry. The new
/// endpoint never consults the column — it always scans. Locks in the
/// "hint-only" decision (changes to add a column fast-path would fail here).
#[test]
fn test_hint_with_column_populated_still_scans() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .track_all_shards()
        .build();

    let (_, receipt_id, height) = send_self_money(&mut env, &user_account, 1, false);

    // Sanity-check: the column entry exists (column path is healthy for this env).
    let store = env.validator().store();
    assert!(
        store
            .get_ser::<ReceiptToTxInfo>(near_store::DBCol::ReceiptToTx, receipt_id.as_ref())
            .is_some()
    );

    // With window=0 and the *wrong* height: the column would have answered. The
    // hint endpoint does not consult the column, so it must fail here.
    let result = handle_message(
        &mut env,
        GetReceiptParentByHint {
            receipt_id,
            block_height: height + 100,
            shard_id: ShardId::new(0),
            window: Some(0),
        },
    );
    assert!(
        matches!(result, Err(GetReceiptParentByHintError::ReceiptNotFoundInHintWindow { .. })),
        "expected ReceiptNotFoundInHintWindow, got {result:?}"
    );
}

/// RPC: exact-height hint resolves successfully end-to-end through the JSON-RPC stack.
#[test]
fn test_hint_rpc_end_to_end() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let (tx_hash, receipt_id, height) = send_self_money(&mut env, &user_account, 1, true);

    let response: RpcReceiptParentByHintResponse = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_receipt_parent_by_hint(RpcReceiptParentByHintRequest {
                    receipt_id,
                    block_height: height,
                    shard_id: ShardId::new(0),
                    window: None,
                })
            },
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.parent_outcome_block_height, height);
    assert_eq!(response.parent_outcome_shard_id, ShardId::new(0));
    match response.origin {
        ReceiptOriginView::FromTransaction { tx_hash: h, sender_account_id } => {
            assert_eq!(h, tx_hash);
            assert_eq!(sender_account_id, user_account);
        }
        other => panic!("expected FromTransaction view, got {other:?}"),
    }
}
