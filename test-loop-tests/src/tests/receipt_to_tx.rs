use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::messaging::Handler;
use near_async::time::Duration;
use near_client_primitives::types::{GetReceiptToTx, GetReceiptToTxError};
use near_jsonrpc_primitives::types::receipts::{ReceiptReference, RpcReceiptToTxRequest};
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ProcessedReceiptMetadata, ReceiptOrigin, ReceiptOriginReceipt, ReceiptOriginTransaction,
    ReceiptSource, ReceiptToTxInfo, ReceiptToTxInfoV1,
};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance, Gas, ShardId};
use near_store::DBCol;

const EPOCH_LENGTH: u64 = 5;

/// When `save_receipt_to_tx` is false, no ReceiptToTx entries should be written.
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

    // Run enough blocks and wait for execution to complete.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Get receipt ID from the transaction outcome.
    let outcome = env.validator().client().chain.get_execution_outcome(&tx_hash).unwrap();
    let receipt_id = outcome.outcome_with_id.outcome.receipt_ids[0];

    // Verify ReceiptToTx entry does NOT exist.
    let store = env.validator().store();
    assert!(
        store.get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref()).is_none(),
        "receipt_to_tx should not be saved when save_receipt_to_tx is false"
    );

    // Verify no ReceiptToTxGc entries in ProcessedReceiptIds.
    let has_receipt_to_tx_gc = store
        .iter_ser::<Vec<ProcessedReceiptMetadata>>(DBCol::ProcessedReceiptIds)
        .flat_map(|(_, entries)| entries)
        .any(|entry| matches!(entry.source(), ReceiptSource::ReceiptToTxGc));
    assert!(
        !has_receipt_to_tx_gc,
        "ProcessedReceiptIds should not contain ReceiptToTxGc entries when save_receipt_to_tx is false"
    );
}

/// ReceiptToTx data persists to disk and survives a client restart.
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

    // Use execute_tx to submit and wait for the transaction to complete.
    // This is more reliable than submit_tx + run_until_executed_height in
    // multi-validator setups because it polls until the result is available.
    let outcome =
        env.runner_for_account(&restart_account).execute_tx(tx, Duration::seconds(10)).unwrap();
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // Verify ReceiptToTx entry exists before restart.
    let info_before = env
        .node_for_account(&restart_account)
        .store()
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
        .expect("receipt_to_tx entry should exist before restart");
    let ReceiptToTxInfo::V1(v1) = &info_before;
    assert_eq!(v1.shard_id, ShardId::new(0), "shard_id should be 0 in single-shard setup");

    // Kill and restart the node.
    let killed_node_state = env.kill_node(&restart_identifier);
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);
    let new_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_identifier, killed_node_state);

    // Verify ReceiptToTx entry still exists after restart and matches.
    let info_after = env
        .node_for_account(&restart_account)
        .store()
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
        .expect("receipt_to_tx entry should persist across restart");
    assert_eq!(info_before, info_after, "receipt_to_tx entry should be identical after restart");
}

/// `save_receipt_to_tx` works independently from `save_tx_outcomes`.
/// When `save_tx_outcomes = false` but `save_receipt_to_tx = true`, ReceiptToTx entries
/// should still be written.
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

    // Run enough blocks and wait for execution to complete.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Verify: outcome NOT saved (save_tx_outcomes=false).
    assert!(
        env.validator().client().chain.get_execution_outcome(&tx_hash).is_err(),
        "outcomes should not be saved when save_tx_outcomes is false"
    );

    // Find receipt IDs by iterating ReceiptToTx entries whose origin
    // is FromTransaction with matching tx_hash.
    let store = env.validator().store();
    let found_receipt =
        store.iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx).any(|(_, info)| match &info {
            ReceiptToTxInfo::V1(v1) => matches!(
                &v1.origin,
                ReceiptOrigin::FromTransaction(origin) if origin.tx_hash == tx_hash
            ),
        });
    assert!(found_receipt, "receipt_to_tx entry should exist even when save_tx_outcomes is false");
}

/// When both save_receipt_to_tx and save_tx_outcomes are false, neither
/// OutcomeIds nor ReceiptToTx entries should be written.
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

    // Capture baselines before sending the transaction.
    let outcome_ids_before = env.validator().store().iter(DBCol::OutcomeIds).count();
    let receipt_to_tx_before = env.validator().store().iter(DBCol::ReceiptToTx).count();

    let signer = create_user_test_signer(&user_account);
    let nonce_before = env
        .validator()
        .view_access_key_query(&user_account, &signer.public_key())
        .expect("access key should exist")
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

    // Run enough blocks and wait for execution to complete.
    let target_height = env.validator().head().height + 2 * EPOCH_LENGTH;
    env.validator_runner().run_until_executed_height(target_height);

    // Verify outcomes are NOT saved (save_tx_outcomes=false).
    assert!(
        env.validator().client().chain.get_execution_outcome(&tx_hash).is_err(),
        "outcomes should not be saved when save_tx_outcomes is false"
    );

    // Verify the transaction actually executed by checking the nonce advanced.
    let nonce_after = env
        .validator()
        .view_access_key_query(&user_account, &signer.public_key())
        .expect("access key should exist")
        .nonce;
    assert!(nonce_after > nonce_before, "nonce should have advanced after tx execution");

    let store = env.validator().store();

    // No new OutcomeIds should be written.
    assert_eq!(
        store.iter(DBCol::OutcomeIds).count(),
        outcome_ids_before,
        "no new OutcomeIds when both save_tx_outcomes and save_receipt_to_tx are false"
    );

    // No new ReceiptToTx should be written.
    assert_eq!(
        store.iter(DBCol::ReceiptToTx).count(),
        receipt_to_tx_before,
        "no new ReceiptToTx when save_receipt_to_tx is false"
    );
}

/// Restart persistence in index-only mode
/// (save_tx_outcomes=false, save_receipt_to_tx=true).
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

    // Run until ReceiptToTx entries for our tx appear. We can't use execute_tx
    // or get_execution_outcome since save_tx_outcomes=false.
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

    // Collect the ReceiptToTx entries for verification after restart.
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

    // Kill and restart the node.
    let killed_node_state = env.kill_node(&restart_identifier);
    env.node_runner(stable_node_idx).run_for_number_of_blocks(5);
    let new_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_identifier, killed_node_state);

    // Verify entries persist after restart.
    let store = env.node_for_account(&restart_account).store();
    for (i, receipt_id) in receipt_ids.iter().enumerate() {
        let info_after = store
            .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, receipt_id.as_ref())
            .expect("receipt_to_tx should persist across restart in index-only mode");
        assert_eq!(
            infos_before[i], &info_after,
            "receipt_to_tx entry should be identical after restart"
        );
    }
}

/// Refund receipts (from unspent gas) should get a ReceiptToTx entry with
/// `ReceiptOrigin::FromReceipt` pointing to the parent action receipt.
#[test]
fn test_refund_receipt_has_receipt_to_tx() {
    init_test_logger();

    let user_account = create_account_id("account0");

    // Use a non-zero gas price so that unspent gas generates a balance refund receipt.
    let min_gas_price = Balance::from_yoctonear(100_000_000);
    let mut env = TestLoopBuilder::new()
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .epoch_length(EPOCH_LENGTH)
        .gas_prices(min_gas_price, min_gas_price)
        .build();

    let signer = create_user_test_signer(&user_account);

    // Deploy the test contract.
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.validator().head().last_block_hash,
    );
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call a cheap method with 300 TGas — most gas is unused, generating a refund.
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

    // The action receipt is the first receipt spawned by the transaction.
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // The action receipt should have spawned a gas refund receipt as its child.
    // Since log_something doesn't create any promises, the only child is the refund.
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == action_receipt_id)
        .expect("action receipt outcome should exist");
    assert!(
        !action_outcome.outcome.receipt_ids.is_empty(),
        "action receipt should have spawned a refund receipt from unspent gas"
    );
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];

    // Verify the refund receipt has a ReceiptToTx entry with FromReceipt origin.
    let store = env.validator().store();
    let info = store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, refund_receipt_id.as_ref())
        .expect("refund receipt should have a ReceiptToTx entry");

    let ReceiptToTxInfo::V1(v1) = &info;
    assert_eq!(v1.shard_id, ShardId::new(0), "shard_id should be 0 in single-shard setup");
    assert_eq!(v1.receiver_account_id, user_account, "receiver should be the user account");
    match &v1.origin {
        ReceiptOrigin::FromReceipt(origin) => {
            assert_eq!(
                origin.parent_receipt_id, action_receipt_id,
                "refund receipt's parent should be the action receipt"
            );
            assert_eq!(
                origin.parent_predecessor_id, user_account,
                "parent_predecessor_id should be the user account"
            );
        }
        other => panic!("expected FromReceipt origin for refund receipt, got {other:?}"),
    }
}

/// RPC: Send a transaction, get the receipt_id from the outcome, call
/// EXPERIMENTAL_receipt_to_tx, and verify the response matches.
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
            |client| {
                client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                    receipt_reference: ReceiptReference { receipt_id },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// RPC: Deploy a contract, call a method that generates a refund receipt
/// (depth=2: tx → action receipt → refund receipt). Query the refund
/// receipt_id and verify it resolves back to the original transaction.
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

    // Deploy the test contract.
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &signer,
        env.rpc_node().head().last_block_hash,
    );
    env.rpc_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call a cheap method with 300 TGas — most gas is unused, generating a refund.
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

    // Find the refund receipt: the action receipt's child.
    let action_receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == action_receipt_id)
        .expect("action receipt outcome should exist");
    assert!(
        !action_outcome.outcome.receipt_ids.is_empty(),
        "action receipt should have spawned a refund receipt"
    );
    let refund_receipt_id = action_outcome.outcome.receipt_ids[0];

    // Query the refund receipt — should resolve through the chain back to the tx.
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                    receipt_reference: ReceiptReference { receipt_id: refund_receipt_id },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, call_tx_hash);
    assert_eq!(response.sender_account_id, user_account);
}

/// RPC: Query a random receipt_id that doesn't exist. Verify error.
#[test]
fn test_receipt_to_tx_rpc_unknown_receipt() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().epoch_length(EPOCH_LENGTH).build();

    let fake_receipt_id = CryptoHash::hash_bytes(b"nonexistent");
    let result = env.rpc_runner().run_with_jsonrpc_client(
        |client| {
            client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                receipt_reference: ReceiptReference { receipt_id: fake_receipt_id },
            })
        },
        Duration::seconds(5),
    );

    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("UNKNOWN_RECEIPT"), "expected UNKNOWN_RECEIPT error, got: {err_str}");
}

/// RPC: When save_receipt_to_tx=false, verify that the endpoint returns
/// an Unsupported error.
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
        |client| {
            client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                receipt_reference: ReceiptReference { receipt_id: fake_receipt_id },
            })
        },
        Duration::seconds(5),
    );

    let err = result.unwrap_err();
    let err_str = format!("{:?}", err);
    assert!(err_str.contains("UNSUPPORTED"), "expected UNSUPPORTED error, got: {err_str}");
    assert!(
        err_str.contains("save_receipt_to_tx"),
        "error should mention save_receipt_to_tx, got: {err_str}"
    );
}

/// When tracked_shards_config is not AllShards, the handler should return
/// Unsupported. Since enable_rpc() forces AllShards on the RPC node, we
/// send the GetReceiptToTx message directly to a validator node's
/// ViewClientActor which has partial shard tracking.
#[test]
fn test_receipt_to_tx_unsupported_partial_tracking() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).build();

    // The validator node has TrackedShardsConfig::NoShards (single-shard tracking),
    // not AllShards. Send GetReceiptToTx directly to its ViewClientActor.
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    let result = view_client.handle(GetReceiptToTx { receipt_id: CryptoHash::hash_bytes(b"any") });

    match result {
        Err(GetReceiptToTxError::Unsupported(msg)) => {
            assert!(msg.contains("all shards"), "error should mention all shards, got: {msg}");
        }
        other => panic!("expected Unsupported error, got: {other:?}"),
    }
}

/// RPC: Insert synthetic ReceiptToTx entries forming a chain of depth 4
/// (receipt_0 → receipt_1 → receipt_2 → receipt_3 → tx) into the RPC node's
/// store, then query receipt_0 through the RPC endpoint. This validates
/// the recursive loop beyond just the first parent hop.
///
/// Natural receipt chains deeper than 2 are hard to capture in
/// FinalExecutionOutcomeView (promise callbacks not returned via
/// promise_return aren't tracked). Synthetic entries let us test the loop
/// cleanly.
#[test]
fn test_receipt_to_tx_rpc_deep_chain_walk() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().enable_rpc().epoch_length(EPOCH_LENGTH).build();

    let tx_hash = CryptoHash::hash_bytes(b"deep_tx");
    let sender: AccountId = "sender.near".parse().unwrap();

    // Build a chain of 4 receipts: receipt_0 → receipt_1 → receipt_2 → receipt_3 → tx.
    let receipt_ids: Vec<CryptoHash> =
        (0..4).map(|i| CryptoHash::hash_bytes(&(i as u32).to_le_bytes())).collect();

    let store = env.rpc_node().store();
    let mut store_update = store.store_update();

    // Terminal node: receipt_3 → FromTransaction.
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

    // Intermediate nodes: receipt_i → FromReceipt(receipt_{i+1}).
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

    // Query receipt_0 via RPC — should walk 4 hops to resolve back to tx_hash.
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                    receipt_reference: ReceiptReference { receipt_id: receipt_ids[0] },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, sender);
}

/// Handler-level test: construct a receipt chain in the store where a parent
/// is missing mid-walk. Verify that UnknownReceipt is returned with the
/// missing parent's receipt_id (not the originally queried receipt).
/// This locks in the semantics: both start-missing and parent-missing
/// map to UnknownReceipt with the specific receipt_id that was not found.
#[test]
fn test_receipt_to_tx_unknown_parent_mid_walk() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().epoch_length(EPOCH_LENGTH).track_all_shards().build();

    let child_receipt_id = CryptoHash::hash_bytes(b"child");
    let missing_parent_id = CryptoHash::hash_bytes(b"missing_parent");

    // Write a single ReceiptToTx entry that points to a parent that doesn't exist.
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

    // Query the child receipt — the handler should walk to missing_parent and fail.
    let handle = env.node_datas[0].view_client_sender.actor_handle();
    let view_client: &mut near_client::ViewClientActor = env.test_loop.data.get_mut(&handle);
    let result = view_client.handle(GetReceiptToTx { receipt_id: child_receipt_id });

    match result {
        Err(GetReceiptToTxError::UnknownReceipt(id)) => {
            assert_eq!(
                id, missing_parent_id,
                "error should report the missing parent, not the originally queried receipt"
            );
        }
        other => panic!("expected UnknownReceipt for missing parent, got: {other:?}"),
    }
}

/// RPC cross-shard chain-walk: 2-shard setup, send a cross-shard transfer,
/// query the child receipt on the receiving shard, verify it resolves
/// back to the original transaction.
#[test]
fn test_receipt_to_tx_rpc_cross_shard() {
    init_test_logger();

    // Two accounts that will land on different shards with multi_shard(2).
    // multi_shard(2) creates boundary at "test1", so:
    //   shard 0: accounts < "test1" (e.g. "account0")
    //   shard 1: accounts >= "test1" (e.g. "test1", "validator0")
    // We use create_account_id which creates "accountN" names.
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

    // Send money cross-shard: sender_account (shard 0) → receiver_account (shard 1).
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

    // The tx creates an action receipt that crosses shards.
    let receipt_id = outcome.transaction_outcome.outcome.receipt_ids[0];

    // Query via RPC — should resolve the cross-shard receipt back to the original tx.
    let response = env
        .rpc_runner()
        .run_with_jsonrpc_client(
            |client| {
                client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                    receipt_reference: ReceiptReference { receipt_id },
                })
            },
            Duration::seconds(5),
        )
        .unwrap();

    assert_eq!(response.transaction_hash, tx_hash);
    assert_eq!(response.sender_account_id, sender_account);

    // Also verify a refund receipt (depth 2 cross-shard) resolves correctly.
    let action_outcome = outcome
        .receipts_outcome
        .iter()
        .find(|r| r.id == receipt_id)
        .expect("action receipt outcome should exist");
    if !action_outcome.outcome.receipt_ids.is_empty() {
        let refund_receipt_id = action_outcome.outcome.receipt_ids[0];
        let refund_response = env
            .rpc_runner()
            .run_with_jsonrpc_client(
                |client| {
                    client.EXPERIMENTAL_receipt_to_tx(RpcReceiptToTxRequest {
                        receipt_reference: ReceiptReference { receipt_id: refund_receipt_id },
                    })
                },
                Duration::seconds(5),
            )
            .unwrap();
        assert_eq!(refund_response.transaction_hash, tx_hash);
        assert_eq!(refund_response.sender_account_id, sender_account);
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
    let result = view_client.handle(GetReceiptToTx { receipt_id: receipt_ids[0] });

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
    let result = view_client.handle(GetReceiptToTx { receipt_id: receipt_ids[2] });
    assert!(result.is_ok(), "100 hops should succeed, got: {result:?}");
    let response = result.unwrap();
    assert_eq!(response.transaction_hash, CryptoHash::hash_bytes(b"tx"));
    assert_eq!(response.sender_account_id.as_str(), "sender");
}
