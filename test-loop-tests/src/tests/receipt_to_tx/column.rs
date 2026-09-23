//! Column-only path: tests exercising `DBCol::ReceiptToTx` column without
//! hint fallback.

use super::*;

/// `save_receipt_to_tx=false` → no ReceiptToTx entries written.
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

    // Run blocks + wait for execution.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Receipt ID from tx outcome.
    let outcome = env.validator().client().chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_id = outcome.outcome_with_id.outcome.receipt_ids[0];

    // ReceiptToTx entry must NOT exist.
    let store = env.validator().store();
    assert!(
        store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref()).is_none(),
        "receipt_to_tx must not be saved when save_receipt_to_tx=false"
    );

    // No ReceiptToTxGc entries in ProcessedReceiptIds.
    let has_receipt_to_tx_gc = store
        .iter_ser::<Vec<ProcessedReceiptMetadata>>(DBCol::ProcessedReceiptIds)
        .flat_map(|(_, entries)| entries)
        .any(|entry| matches!(entry.source(), ReceiptSource::ReceiptToTxGc));
    assert!(
        !has_receipt_to_tx_gc,
        "ProcessedReceiptIds must not contain ReceiptToTxGc when save_receipt_to_tx=false"
    );
}

/// ReceiptToTx persists to disk + survives client restart.
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

    // execute_tx polls until result available. More reliable than
    // submit_tx + run_until_executed_height in multi-validator setups.
    let outcome =
        env.runner_for_account(&restart_account).execute_tx(tx, Duration::seconds(10)).unwrap();
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // ReceiptToTx entry exists before restart.
    let info_before = env
        .node_for_account(&restart_account)
        .store()
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
        .expect("receipt_to_tx entry exists before restart");
    let ReceiptToTxInfo::V1(v1) = &info_before;
    assert_eq!(v1.shard_id, ShardId::new(0), "shard_id == 0 in single-shard setup");

    // Kill + restart node.
    let killed_node_state = env.kill_node(&restart_identifier);
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);
    let new_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_identifier, killed_node_state);

    // ReceiptToTx entry exists after restart + matches.
    let info_after = env
        .node_for_account(&restart_account)
        .store()
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
        .expect("receipt_to_tx entry persists across restart");
    assert_eq!(info_before, info_after, "receipt_to_tx entry identical after restart");
}

/// `save_receipt_to_tx` independent of `save_tx_outcomes`.
/// `save_tx_outcomes=false` + `save_receipt_to_tx=true` → ReceiptToTx
/// entries still written.
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

    // Run blocks + wait for execution.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Outcome NOT saved (save_tx_outcomes=false).
    assert!(
        env.validator().client().chain.get_execution_outcome(&tx_hash).is_err(),
        "outcomes must not be saved when save_tx_outcomes=false"
    );

    // Iterate ReceiptToTx entries; find FromTransaction with matching
    // tx_hash.
    let store = env.validator().store();
    let found_receipt =
        store.iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx).any(|(_, info)| match &info {
            ReceiptToTxInfo::V1(v1) => matches!(
                &v1.origin,
                ReceiptOrigin::FromTransaction(origin) if origin.tx_hash == tx_hash
            ),
        });
    assert!(found_receipt, "receipt_to_tx entry exists even when save_tx_outcomes=false");
}

/// `save_receipt_to_tx=false` + `save_tx_outcomes=false` → neither
/// OutcomeIds nor ReceiptToTx entries written.
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

    // Capture baselines before tx.
    let outcome_ids_before = env.validator().store().iter(DBCol::OutcomeIds).count();
    let receipt_to_tx_before = env.validator().store().iter(DBCol::ReceiptToTx).count();

    let signer = create_user_test_signer(&user_account);
    let nonce_before = env
        .validator()
        .view_access_key_query(&user_account, &signer.public_key())
        .expect("access key exists")
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

    // Run blocks + wait for execution.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Outcomes NOT saved (save_tx_outcomes=false).
    assert!(
        env.validator().client().chain.get_execution_outcome(&tx_hash).is_err(),
        "outcomes must not be saved when save_tx_outcomes=false"
    );

    // Confirm tx executed via nonce advance.
    let nonce_after = env
        .validator()
        .view_access_key_query(&user_account, &signer.public_key())
        .expect("access key exists")
        .nonce;
    assert!(nonce_after > nonce_before, "nonce advanced after tx execution");

    let store = env.validator().store();

    // No new OutcomeIds.
    assert_eq!(
        store.iter(DBCol::OutcomeIds).count(),
        outcome_ids_before,
        "no new OutcomeIds when both save_tx_outcomes and save_receipt_to_tx are false"
    );

    // No new ReceiptToTx.
    assert_eq!(
        store.iter(DBCol::ReceiptToTx).count(),
        receipt_to_tx_before,
        "no new ReceiptToTx when save_receipt_to_tx is false"
    );
}

/// Restart persistence in index-only mode (save_tx_outcomes=false,
/// save_receipt_to_tx=true).
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

    // Run until ReceiptToTx entries for tx appear. Can't use execute_tx
    // or get_execution_outcome — save_tx_outcomes=false.
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

    // Collect ReceiptToTx entries for post-restart check.
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

    // Kill + restart node.
    let killed_node_state = env.kill_node(&restart_identifier);
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);
    let new_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_identifier, killed_node_state);

    // Entries persist after restart.
    let store = env.node_for_account(&restart_account).store();
    for (i, receipt_id) in receipt_ids.iter().enumerate() {
        let info_after = store
            .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
            .expect("receipt_to_tx persists across restart in index-only mode");
        assert_eq!(infos_before[i], &info_after, "receipt_to_tx entry identical after restart");
    }
}

/// Refund receipts (from unspent gas) get ReceiptToTx entry with
/// `ReceiptOrigin::FromReceipt` → parent action receipt.
#[test]
fn test_refund_receipt_has_receipt_to_tx() {
    init_test_logger();

    let user_account = create_account_id("account0");

    // Non-zero gas price → unspent gas generates balance refund receipt.
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(min_gas_price, min_gas_price)
        .build();

    let signer = create_user_test_signer(&user_account);

    // Deploy test contract.
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Cheap method, 300 TGas — most unused → refund.
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

    // Action receipt = first receipt spawned by tx.
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // Action receipt spawns gas refund receipt as child. log_something
    // creates no promises → only child is the refund.
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == action_receipt_id)
        .expect("action receipt outcome exists");
    assert!(
        !action_outcome.outcome.receipt_ids.is_empty(),
        "action receipt spawns refund from unspent gas"
    );
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];

    // Refund receipt has ReceiptToTx entry with FromReceipt origin.
    let store = env.validator().store();
    let info = store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, refund_receipt_id.as_ref())
        .expect("refund receipt has ReceiptToTx entry");

    let ReceiptToTxInfo::V1(v1) = &info;
    assert_eq!(v1.shard_id, ShardId::new(0), "shard_id == 0 in single-shard setup");
    assert_eq!(v1.receiver_account_id, user_account, "receiver == user account");
    match &v1.origin {
        ReceiptOrigin::FromReceipt(origin) => {
            assert_eq!(
                origin.parent_receipt_id, action_receipt_id,
                "refund receipt parent == action receipt"
            );
            assert_eq!(
                origin.parent_predecessor_id, user_account,
                "parent_predecessor_id == user account"
            );
        }
        other => panic!("expected FromReceipt origin for refund, got {other:?}"),
    }
}

/// RPC: send tx, get receipt_id from outcome, call
/// EXPERIMENTAL_receipt_to_tx, verify response matches.
#[test]
fn test_receipt_to_tx_rpc_direct() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .build();

    let signer = create_user_test_signer(&user_account);
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account.clone(),
        &signer,
        Balance::from_yoctonear(100),
        env.rpc_node().head().last_block_hash,
    );
    let tx_hash = tx.get_hash();
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(receipt_id)),
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// RPC: deploy contract, call method that generates refund receipt
/// (depth=2: tx → action → refund). Query refund receipt_id, verify
/// resolves back to original tx.
#[test]
fn test_receipt_to_tx_rpc_chain_walk() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(min_gas_price, min_gas_price)
        .build();

    let signer = create_user_test_signer(&user_account);

    // Deploy test contract.
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.rpc_node().head().last_block_hash,
    );
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Cheap method, 300 TGas — most unused → refund.
    let call_tx = SignedTransaction::call(
        2,
        user_account.clone(),
        user_account.clone(),
        &signer,
        Balance::ZERO,
        "log_something".to_owned(),
        vec![],
        Gas::from_teragas(300),
        env.rpc_node().head().last_block_hash,
    );
    let call_tx_hash = call_tx.get_hash();
    let outcome = env.rpc_runner().execute_tx(call_tx, Duration::seconds(10)).unwrap();

    // Refund receipt = action receipt's child.
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == action_receipt_id)
        .expect("action receipt outcome exists");
    assert!(
        !action_outcome.outcome.receipt_ids.is_empty(),
        "action receipt spawned refund receipt"
    );
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];

    // Query refund receipt — resolves through chain back to tx.
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(refund_receipt_id)),
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, call_tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// RPC: query nonexistent receipt_id. Verify error.
#[test]
fn test_receipt_to_tx_rpc_unknown_receipt() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().epoch_length(EPOCH_LENGTH).build();

    let fake_receipt_id = CryptoHash::hash_bytes(b"nonexistent");
    let result = env.rpc_runner().run_with_jsonrpc_client(
        |client| client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(fake_receipt_id)),
        Duration::seconds(5),
    );

    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("UNKNOWN_RECEIPT"), "expected UNKNOWN_RECEIPT error, got: {err_str}");
}

/// RPC: save_receipt_to_tx=false → endpoint returns Unsupported.
#[test]
fn test_receipt_to_tx_rpc_unsupported_disabled() {
    init_test_logger();

    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .epoch_length(EPOCH_LENGTH)
        .config_modifier(|config, _| {
            config.save_receipt_to_tx = false;
        })
        .build();

    let fake_receipt_id = CryptoHash::hash_bytes(b"any");
    let result = env.rpc_runner().run_with_jsonrpc_client(
        |client| client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(fake_receipt_id)),
        Duration::seconds(5),
    );

    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("UNSUPPORTED"), "expected UNSUPPORTED error, got: {err_str}");
    assert!(
        err_str.contains("save_receipt_to_tx"),
        "error mentions save_receipt_to_tx, got: {err_str}"
    );
}

/// tracked_shards_config != AllShards → handler returns Unsupported.
/// enable_rpc() forces AllShards on RPC node, so send GetReceiptToTx
/// directly to a validator's ViewClientActor (partial tracking).
#[test]
fn test_receipt_to_tx_unsupported_partial_tracking() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).build();

    // Validator has TrackedShardsConfig::NoShards (single-shard), not
    // AllShards. Send GetReceiptToTx direct to its ViewClientActor.
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    let result = view_client.handle(receipt_to_tx_req(CryptoHash::hash_bytes(b"any")));

    match result {
        Err(GetReceiptToTxError::Unsupported(msg)) => {
            assert!(msg.contains("all shards"), "error mentions all shards, got: {msg}");
        }
        other => panic!("expected Unsupported error, got: {other:?}"),
    }
}

/// RPC: insert synthetic ReceiptToTx chain depth 4 (receipt_0 → receipt_1
/// → receipt_2 → receipt_3 → tx) into RPC node's store, query receipt_0
/// via RPC. Validates recursive loop beyond first parent hop.
///
/// Natural receipt chains > depth 2 hard to capture in
/// FinalExecutionOutcomeView (promise callbacks not returned via
/// promise_return are untracked). Synthetic entries test loop cleanly.
#[test]
fn test_receipt_to_tx_rpc_deep_chain_walk() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().epoch_length(EPOCH_LENGTH).build();

    let tx_hash = CryptoHash::hash_bytes(b"deep_tx");
    let sender: AccountId = "sender.near".parse().unwrap();

    // Chain of 4 receipts: receipt_0 → ... → receipt_3 → tx.
    let receipt_ids: Vec<CryptoHash> =
        (0..4).map(|i| CryptoHash::hash_bytes(&(i as u32).to_le_bytes())).collect();

    let store = env.rpc_node().store();
    let mut store_update = store.store_update();

    // Terminal: receipt_3 → FromTransaction.
    store_update.insert_ser(
        DBCol::ReceiptToTx,
        receipt_ids[3].as_ref(),
        &ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromTransaction(ReceiptOriginTransaction {
                tx_hash,
                sender_account_id: sender.clone(),
            }),
            receiver_account_id: sender.clone(),
            shard_id: ShardId::new(0),
        }),
    );

    // Intermediates: receipt_i → FromReceipt(receipt_{i+1}).
    for i in 0..3 {
        store_update.insert_ser(
            DBCol::ReceiptToTx,
            receipt_ids[i].as_ref(),
            &ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
                origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                    parent_receipt_id: receipt_ids[i + 1],
                    parent_predecessor_id: sender.clone(),
                }),
                receiver_account_id: sender.clone(),
                shard_id: ShardId::new(0),
            }),
        );
    }

    store_update.commit();

    // Query receipt_0 via RPC — walks 4 hops back to tx_hash.
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(receipt_ids[0])),
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, sender);
}

/// Handler-level: construct receipt chain in store with parent missing
/// mid-walk. UnknownReceipt returned with missing parent's receipt_id
/// (not originally queried receipt). Locks semantics: start-missing and
/// parent-missing both map to UnknownReceipt with the missing id.
#[test]
fn test_receipt_to_tx_unknown_parent_mid_walk() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();

    let child_receipt_id = CryptoHash::hash_bytes(b"child");
    let missing_parent_id = CryptoHash::hash_bytes(b"missing_parent");

    // ReceiptToTx entry pointing to nonexistent parent.
    let store = env.validator().store();
    let mut store_update = store.store_update();
    store_update.insert_ser(
        DBCol::ReceiptToTx,
        child_receipt_id.as_ref(),
        &ReceiptToTxInfo::V1(ReceiptToTxInfoV1 {
            origin: ReceiptOrigin::FromReceipt(ReceiptOriginReceipt {
                parent_receipt_id: missing_parent_id,
                parent_predecessor_id: "system".parse().unwrap(),
            }),
            receiver_account_id: "test".parse().unwrap(),
            shard_id: ShardId::new(0),
        }),
    );
    store_update.commit();

    // Query child receipt — handler walks to missing_parent, fails.
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    let result = view_client.handle(receipt_to_tx_req(child_receipt_id));

    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => {
            assert_eq!(
                id, missing_parent_id,
                "error reports missing parent, not originally queried receipt"
            );
        }
        other => panic!("expected UnknownReceipt for missing parent, got: {other:?}"),
    }
}

/// RPC cross-shard chain-walk: 2-shard setup, cross-shard transfer.
/// Query child receipt on receiving shard, verify resolves back to tx.
#[test]
fn test_receipt_to_tx_rpc_cross_shard() {
    init_test_logger();

    // Two accounts on different shards with multi_shard(2). Boundary at
    // "test1":
    //   shard 0: < "test1" (e.g. "account0")
    //   shard 1: >= "test1" (e.g. "test1", "validator0")
    // create_account_id makes "accountN" names.
    let sender_account = create_account_id("account0");
    let receiver_account: AccountId = "test1".parse().unwrap();

    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .num_shards(2)
        .add_user_account(&sender_account, Balance::from_near(1_000_000))
        .add_user_account(&receiver_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(min_gas_price, min_gas_price)
        .build();

    let signer = create_user_test_signer(&sender_account);

    // Cross-shard transfer: sender (shard 0) → receiver (shard 1).
    let tx = SignedTransaction::send_money(
        1,
        sender_account.clone(),
        receiver_account,
        &signer,
        Balance::from_yoctonear(100),
        env.rpc_node().head().last_block_hash,
    );
    let tx_hash = tx.get_hash();
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(10)).unwrap();

    // Tx spawns action receipt crossing shards.
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // Query via RPC — resolves cross-shard receipt back to original tx.
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(receipt_id)),
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, sender_account);

    // Refund receipt (depth 2 cross-shard) resolves correctly.
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == receipt_id)
        .expect("action receipt outcome exists");
    if !action_outcome.outcome.receipt_ids.is_empty() {
        let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
        let refund_response = env
            .rpc_runner()
            .run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(refund_receipt_id))
                },
                Duration::seconds(5),
            )
            .unwrap();
        assert_eq!(refund_response.transaction_hash, tx_hash);
        assert_eq!(refund_response.sender_account_id, sender_account);
    }
}

/// Regression: column-only path (no hint) keeps exact pre-hint behavior.
#[test]
fn test_column_unaffected_without_hint() {
    init_test_logger();
    let user_account = create_account_id("account0");
    let mut env = TestLoopBuilder::new()
        .enable_rpc()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .build();

    let signer = create_user_test_signer(&user_account);
    let tx = SignedTransaction::send_money(
        1,
        user_account.clone(),
        user_account.clone(),
        &signer,
        Balance::from_yoctonear(100),
        env.rpc_node().head().last_block_hash,
    );
    let tx_hash = tx.get_hash();
    let outcome = env.rpc_runner().execute_tx(tx, Duration::seconds(10)).unwrap();
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| client.EXPERIMENTAL_receipt_to_tx(receipt_to_tx_rpc_req(receipt_id)),
            Duration::seconds(5),
        )
        .unwrap();
    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}
