use std::collections::HashSet;

use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_o11y::testonly::init_test_logger;
use near_parameters::config::TEST_CONFIG_YIELD_TIMEOUT_LENGTH;
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{
    ProcessedReceiptMetadata, Receipt, ReceiptEnum, ReceiptOrigin, ReceiptSource, ReceiptToTxInfo,
    VersionedReceiptEnum,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, ShardId};
use near_primitives::utils::get_block_shard_id;
use near_store::{DBCol, ShardUId};

use crate::setup;
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_account_id, create_validators_spec, validators_spec_clients};

const EPOCH_LENGTH: u64 = 5;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

/// Tests that processed local and instant receipts are saved to the Receipts column
/// and later garbage collected.
///
/// Deploys a contract, then calls `call_yield_create_return_promise` which produces
/// a local receipt (from the transaction) and a PromiseYield instant receipt.
/// Verifies both exist in DBCol::Receipts with appropriate metadata, then runs enough
/// epochs for GC to kick in and verifies cleanup.
#[test]
fn test_processed_receipt_ids_gc() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build()
        .warmup();

    let signer = create_user_test_signer(&user_account);

    // Deploy the test contract.
    let contract_code = near_test_contracts::rs_contract().to_vec();
    let block_hash = env.validator().head().last_block_hash;
    let deploy_tx =
        SignedTransaction::deploy_contract(1, &user_account, contract_code, &signer, block_hash);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call yield_create — produces a local receipt and a PromiseYield instant receipt.
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::from_actions(
        2,
        user_account.clone(),
        user_account,
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_create_return_promise".to_string(),
            args: vec![42u8; 16],
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.validator().submit_tx(tx);

    // Wait for the local receipt to be processed (the transaction won't fully
    // complete because the yield callback is waiting for a resume).
    let tx_outcome =
        env.validator_runner().run_until_outcome_available(tx_hash, Duration::seconds(5));
    let [local_receipt_id] = tx_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single receipt from transaction")
    };
    let local_outcome =
        env.validator_runner().run_until_outcome_available(local_receipt_id, Duration::seconds(5));
    // The local receipt produces exactly one child receipt — the PromiseYield instant receipt.
    let [instant_receipt_id] = local_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single receipt from local receipt execution")
    };

    let receipt_execution_block_hash = local_outcome.block_hash;
    let metadata_key = get_block_shard_id(&receipt_execution_block_hash, ShardId::new(0));

    // Verify local receipt exists in DBCol::Receipts.
    let store = env.validator().store();
    let receipt = store
        .get_ser::<Receipt>(DBCol::Receipts, local_receipt_id.as_ref())
        .expect("local receipt should exist in DBCol::Receipts after processing");
    assert_eq!(receipt.receipt_id(), &local_receipt_id);

    // Verify instant receipt exists in DBCol::Receipts.
    let receipt = store
        .get_ser::<Receipt>(DBCol::Receipts, instant_receipt_id.as_ref())
        .expect("instant receipt should exist in DBCol::Receipts after processing");
    assert_eq!(receipt.receipt_id(), &instant_receipt_id);
    assert_matches!(receipt.versioned_receipt(), VersionedReceiptEnum::PromiseYield(_));

    // Verify both local and instant metadata exist in DBCol::ProcessedReceiptIds.
    let all_metadata = store
        .get_ser::<Vec<ProcessedReceiptMetadata>>(DBCol::ProcessedReceiptIds, &metadata_key)
        .expect("metadata should exist in DBCol::ProcessedReceiptIds after processing");
    assert_eq!(
        all_metadata,
        vec![
            ProcessedReceiptMetadata::new(instant_receipt_id, ReceiptSource::Instant),
            ProcessedReceiptMetadata::new(local_receipt_id, ReceiptSource::Local),
        ]
    );

    #[cfg(feature = "test_features")]
    env.validator_mut().validate_store();

    // Run enough epochs for GC to clean up the receipts.
    let num_blocks = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    env.validator_runner().run_for_number_of_blocks(num_blocks as usize);

    // Verify the receipts have been garbage collected.
    let store = env.validator().store();
    assert!(
        store.get(DBCol::Receipts, local_receipt_id.as_ref()).is_none(),
        "local receipt should be garbage collected from DBCol::Receipts"
    );
    assert!(
        store.get(DBCol::Receipts, instant_receipt_id.as_ref()).is_none(),
        "instant receipt should be garbage collected from DBCol::Receipts"
    );
    assert!(
        store.get(DBCol::ProcessedReceiptIds, &metadata_key).is_none(),
        "receipt metadata should be garbage collected from DBCol::ProcessedReceiptIds"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that ReceiptToTx entries are saved for local receipts, instant receipts, and
/// transaction→receipt mappings, then properly garbage collected.
///
/// Deploys a contract, then calls `call_yield_create_return_promise` which produces
/// a local receipt (from the transaction) and a PromiseYield instant receipt.
/// Verifies ReceiptToTx entries exist with correct ReceiptOrigin variants, then runs
/// enough epochs for GC to kick in and verifies cleanup.
#[test]
fn test_receipt_to_tx_saved_and_gced() {
    init_test_logger();

    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);
    let user_account = create_account_id("account0");

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&[user_account.clone()], Balance::from_near(1_000_000))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build()
        .warmup();

    let signer = create_user_test_signer(&user_account);

    // Deploy the test contract.
    let contract_code = near_test_contracts::rs_contract().to_vec();
    let block_hash = env.validator().head().last_block_hash;
    let deploy_tx =
        SignedTransaction::deploy_contract(1, &user_account, contract_code, &signer, block_hash);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call yield_create — produces a local receipt and a PromiseYield instant receipt.
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::from_actions(
        2,
        user_account.clone(),
        user_account.clone(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_create_return_promise".to_string(),
            args: vec![42u8; 16],
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.validator().submit_tx(tx);

    // Wait for the transaction outcome (tx → local receipt).
    let tx_outcome =
        env.validator_runner().run_until_outcome_available(tx_hash, Duration::seconds(5));
    let [local_receipt_id] = tx_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single receipt from transaction")
    };

    // Wait for the local receipt outcome (local receipt → instant receipt).
    let local_outcome =
        env.validator_runner().run_until_outcome_available(local_receipt_id, Duration::seconds(5));
    let [instant_receipt_id] = local_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single receipt from local receipt execution")
    };

    let store = env.validator().store();

    // Verify ReceiptToTx entry for the tx→local receipt mapping.
    let tx_receipt_info = store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, local_receipt_id.as_ref())
        .expect("receipt_to_tx entry should exist for local receipt created from transaction");
    match &tx_receipt_info {
        ReceiptToTxInfo::V1(v1) => {
            assert_matches!(&v1.origin, ReceiptOrigin::FromTransaction(origin) => {
                assert_eq!(origin.tx_hash, tx_hash, "tx_hash should match the originating transaction");
                assert_eq!(origin.sender_account_id, user_account, "sender should match");
            });
            assert_eq!(v1.receiver_account_id, user_account, "receiver should match");
            assert_eq!(v1.shard_id, ShardId::new(0), "shard_id should be 0 in single-shard setup");
        }
    }

    // Verify ReceiptToTx entry for the local→instant receipt mapping.
    let instant_receipt_info = store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, instant_receipt_id.as_ref())
        .expect("receipt_to_tx entry should exist for instant receipt created from local receipt");
    match &instant_receipt_info {
        ReceiptToTxInfo::V1(v1) => {
            assert_matches!(&v1.origin, ReceiptOrigin::FromReceipt(origin) => {
                assert_eq!(origin.parent_receipt_id, local_receipt_id,
                    "parent should be the local receipt");
            });
            assert_eq!(v1.shard_id, ShardId::new(0), "shard_id should be 0 in single-shard setup");
        }
    }

    #[cfg(feature = "test_features")]
    env.validator_mut().validate_store();

    // Run enough epochs for GC to clean up.
    let num_blocks = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    env.validator_runner().run_for_number_of_blocks(num_blocks as usize);

    // Verify ReceiptToTx entries have been garbage collected.
    // GC deletes ReceiptToTx entries for receipt IDs that appear as outcome IDs in the block.
    let store = env.validator().store();
    assert!(
        store.get(DBCol::ReceiptToTx, local_receipt_id.as_ref()).is_none(),
        "receipt_to_tx for local receipt should be garbage collected"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that ReceiptToTx entries are properly garbage collected even when
/// `save_tx_outcomes` is false. This verifies that the OutcomeIds index is
/// still written (needed for GC) when only save_receipt_to_tx is enabled.
#[test]
fn test_receipt_to_tx_gc_with_outcomes_disabled() {
    init_test_logger();

    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);
    let user_account = create_account_id("account0");

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(EPOCH_LENGTH)
        .shard_layout(ShardLayout::single_shard())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&[user_account.clone()], Balance::from_near(1_000_000))
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .config_modifier(|config, _| {
            config.save_tx_outcomes = false;
        })
        .build()
        .warmup();

    let signer = create_user_test_signer(&user_account);

    // Send a simple transfer transaction. This produces a receipt that will
    // have a ReceiptToTx mapping.
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

    // Run enough blocks for the transaction to be processed.
    env.validator_runner().run_for_number_of_blocks(5);

    // Verify: outcome is NOT saved (save_tx_outcomes=false).
    assert!(
        env.validator().client().chain.get_execution_outcome(&tx_hash).is_err(),
        "outcomes should not be saved when save_tx_outcomes is false"
    );

    // Find ReceiptToTx entries by iterating the column. Since outcomes are disabled,
    // we cannot use run_until_outcome_available to discover receipt IDs.
    let store = env.validator().store();
    let receipt_to_tx_entries: Vec<(CryptoHash, ReceiptToTxInfo)> = store
        .iter_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx)
        .map(|(key, info)| {
            let receipt_id = CryptoHash::try_from(key.as_ref()).unwrap();
            (receipt_id, info)
        })
        .collect();

    // There should be at least one entry from our transaction.
    let matching_entries: Vec<_> = receipt_to_tx_entries
        .iter()
        .filter(|(_, info)| match info {
            ReceiptToTxInfo::V1(v1) => matches!(
                &v1.origin,
                ReceiptOrigin::FromTransaction(origin) if origin.tx_hash == tx_hash
            ),
        })
        .collect();
    assert!(
        !matching_entries.is_empty(),
        "receipt_to_tx entries should exist for the transaction even with save_tx_outcomes=false"
    );

    let receipt_ids: Vec<CryptoHash> =
        matching_entries.iter().map(|(receipt_id, _)| *receipt_id).collect();

    // Store validator now understands index-only mode (save_tx_outcomes=false),
    // so it skips the TransactionResultForBlock check for OutcomeIds entries.
    #[cfg(feature = "test_features")]
    env.validator_mut().validate_store();

    // Run enough epochs for GC to clean up.
    let num_blocks = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    env.validator_runner().run_for_number_of_blocks(num_blocks as usize);

    // Verify ReceiptToTx entries have been garbage collected.
    let store = env.validator().store();
    for receipt_id in &receipt_ids {
        assert!(
            store.get(DBCol::ReceiptToTx, receipt_id.as_ref()).is_none(),
            "receipt_to_tx for {receipt_id} should be garbage collected"
        );
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that ReceiptToTx entries for data receipts are garbage collected.
///
/// Data receipts are generated when an action receipt with `output_data_receivers` executes
/// (e.g., from `promise_create` + `promise_then`). Data receipts don't produce execution
/// outcomes, so GC (which iterates OutcomeIds to find receipt IDs to delete) never finds them.
///
/// This test is expected to FAIL with current code, exposing the GC leak.
#[test]
fn test_data_receipt_receipt_to_tx_gc() {
    init_test_logger();

    let user_account = create_account_id("account0");

    let mut env = TestLoopBuilder::new()
        .epoch_length(EPOCH_LENGTH)
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .build()
        .warmup();

    let signer = create_user_test_signer(&user_account);

    // Deploy the test contract.
    let contract_code = near_test_contracts::rs_contract().to_vec();
    let block_hash = env.validator().head().last_block_hash;
    let deploy_tx =
        SignedTransaction::deploy_contract(1, &user_account, contract_code, &signer, block_hash);
    env.validator_runner().run_tx(deploy_tx, Duration::seconds(5));

    // Call call_promise with create+then to generate a cross-contract call with callback.
    // This produces:
    //   - Action receipt A (calls log_something, has output_data_receivers)
    //   - Action receipt C (callback, depends on A's data via input_data_ids)
    //   - DataReceipt D (delivers A's return value to C) — created when A executes
    // All three get ReceiptToTx entries, but D has no execution outcome.
    let args = serde_json::json!([
        {
            "create": {
                "account_id": user_account.as_str(),
                "method_name": "log_something",
                "arguments": [],
                "amount": "0",
                "gas": 50_000_000_000_000u64
            },
            "id": 0
        },
        {
            "then": {
                "promise_index": 0,
                "account_id": user_account.as_str(),
                "method_name": "log_something",
                "arguments": [],
                "amount": "0",
                "gas": 50_000_000_000_000u64
            },
            "id": 1
        }
    ]);
    let block_hash = env.validator().head().last_block_hash;
    let tx = SignedTransaction::from_actions(
        2,
        user_account.clone(),
        user_account,
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_promise".to_string(),
            args: serde_json::to_vec(&args).unwrap(),
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        block_hash,
    );
    env.validator_runner().run_tx(tx, Duration::seconds(5));

    // Run a few more blocks to ensure all receipts (including data receipts) are processed.
    env.validator_runner().run_for_number_of_blocks(5);

    // Find data receipt IDs directly from OutgoingReceipts (all receipts produced by execution).
    let store = env.validator().store();
    let data_receipt_ids: Vec<CryptoHash> = store
        .iter_ser::<Vec<Receipt>>(DBCol::OutgoingReceipts)
        .flat_map(|(_, receipts)| receipts)
        .filter(|r| matches!(r.receipt(), ReceiptEnum::Data(_)))
        .map(|r| *r.receipt_id())
        .collect();
    assert!(!data_receipt_ids.is_empty(), "should have data receipts from promise create+then");

    // Verify each data receipt has a ReceiptToTx entry.
    for receipt_id in &data_receipt_ids {
        assert!(
            store.get(DBCol::ReceiptToTx, receipt_id.as_ref()).is_some(),
            "data receipt {receipt_id} should have a ReceiptToTx entry"
        );
    }

    // Assert data receipt IDs are absent from OutcomeIds — this is why GC can't find them.
    let all_outcome_ids: HashSet<CryptoHash> =
        store.iter_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds).flat_map(|(_, ids)| ids).collect();
    for receipt_id in &data_receipt_ids {
        assert!(
            !all_outcome_ids.contains(receipt_id),
            "data receipt {receipt_id} should NOT appear in OutcomeIds"
        );
    }

    #[cfg(feature = "test_features")]
    env.validator_mut().validate_store();

    // Run enough epochs for GC.
    let num_blocks = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    env.validator_runner().run_for_number_of_blocks(num_blocks as usize);

    // Assert data receipt ReceiptToTx entries survive GC (they shouldn't, but do due to the leak).
    let store = env.validator().store();
    for receipt_id in &data_receipt_ids {
        assert!(
            store.get(DBCol::ReceiptToTx, receipt_id.as_ref()).is_none(),
            "receipt_to_tx for data receipt {receipt_id} should be garbage collected"
        );
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that ReceiptToTx entries for PromiseResume receipts are garbage collected.
///
/// PromiseResume receipts are created when a yield times out. The PromiseResume receipt's
/// ID never appears in OutcomeIds, so GC (which iterates OutcomeIds to find receipt IDs
/// to delete from ReceiptToTx) never finds the resume receipt.
///
/// This test is expected to FAIL with current code, exposing the GC leak.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_promise_resume_receipt_to_tx_gc() {
    init_test_logger();

    let user_account = create_account_id("account0");
    let signer = create_user_test_signer(&user_account);

    let runtime_config = RuntimeConfig::test();
    assert_eq!(
        runtime_config.wasm_config.limit_config.yield_timeout_length_in_blocks,
        TEST_CONFIG_YIELD_TIMEOUT_LENGTH
    );
    let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);

    let mut env = TestLoopBuilder::new()
        .genesis_height(0)
        .epoch_length(EPOCH_LENGTH)
        .add_user_account(&user_account, Balance::from_near(1_000_000))
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .runtime_config_store(runtime_config_store)
        .skip_warmup()
        .build();

    // Deploy the test contract.
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
    let deploy_tx = SignedTransaction::deploy_contract(
        1,
        &user_account,
        near_test_contracts::rs_contract().into(),
        &signer,
        *genesis_block.hash(),
    );
    env.validator().submit_tx(deploy_tx);
    env.validator_runner().run_until_head_height(2);

    // Call yield_create — creates a yield that will timeout.
    let yield_tx = SignedTransaction::from_actions(
        2,
        user_account.clone(),
        user_account.clone(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_create_return_promise".to_string(),
            args: vec![42u8; 16],
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
    );
    env.validator().submit_tx(yield_tx);
    env.validator_runner().run_until_head_height(4);

    // The yield was created at height 4, timeout fires at 4 + TEST_CONFIG_YIELD_TIMEOUT_LENGTH.
    let yield_timeout_height = 4 + TEST_CONFIG_YIELD_TIMEOUT_LENGTH;

    // Advance to timeout height — PromiseResume receipt is produced.
    env.validator_runner().run_until_head_height(yield_timeout_height);

    // Find PromiseResume receipt IDs from the outgoing receipts at timeout height.
    let resume_receipt_ids = {
        let node = env.validator();
        let client = node.client();
        let head = client.chain.head().unwrap();
        let shard_layout = client.epoch_manager.get_shard_layout(&head.epoch_id).unwrap();
        let shard_id = shard_layout.account_id_to_shard_id(&user_account);
        let mut result = vec![];
        for receipt in client
            .chain
            .get_outgoing_receipts_for_shard(head.last_block_hash, shard_id, head.height)
            .unwrap()
        {
            if let ReceiptEnum::PromiseResume(_) = receipt.receipt() {
                result.push(*receipt.receipt_id());
            }
        }
        result
    };
    assert_eq!(resume_receipt_ids.len(), 1, "expected one PromiseResume receipt at timeout");
    let resume_receipt_id = resume_receipt_ids[0];

    // Advance one more block for the resume receipt to execute.
    env.validator_runner().run_until_head_height(yield_timeout_height + 1);

    // Verify ReceiptToTx entry exists for the resume receipt.
    let store = env.validator().store();
    store
        .get_ser::<ReceiptToTxInfo>(DBCol::ReceiptToTx, resume_receipt_id.as_ref())
        .expect("receipt_to_tx entry should exist for PromiseResume receipt");

    // Assert resume receipt ID is absent from OutcomeIds — this is why GC can't find it.
    let all_outcome_ids: HashSet<CryptoHash> =
        store.iter_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds).flat_map(|(_, ids)| ids).collect();
    assert!(
        !all_outcome_ids.contains(&resume_receipt_id),
        "PromiseResume receipt ID should NOT appear in OutcomeIds"
    );

    #[cfg(feature = "test_features")]
    env.validator_mut().validate_store();

    // Run enough epochs for GC.
    let num_blocks = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    env.validator_runner().run_for_number_of_blocks(num_blocks as usize);

    // The PromiseResume receipt ID is absent from OutcomeIds, so GC never finds it.
    let store = env.validator().store();
    assert!(
        store.get(DBCol::ReceiptToTx, resume_receipt_id.as_ref()).is_none(),
        "receipt_to_tx for PromiseResume receipt should be garbage collected"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that ReceiptToTx entries for cross-shard action receipts are GC'd on a
/// node that only tracks the source shard.
///
/// ReceiptToTx is written on the source shard when a receipt is created. gc_outcomes
/// deletes ReceiptToTx based on OutcomeIds, but outcomes are recorded on the destination
/// shard where the receipt executes. A node tracking only the source shard never sees
/// the destination shard's outcomes, so gc_outcomes can never delete the entry.
///
/// gc_outgoing_receipts handles this by deleting ReceiptToTx for ALL outgoing receipts
/// when the source block is GC'd.
#[test]
fn test_cross_shard_receipt_to_tx_gc_on_source_only_node() {
    init_test_logger();

    const CROSS_SHARD_EPOCH_LENGTH: u64 = 10;
    const CROSS_SHARD_GC_NUM_EPOCHS_TO_KEEP: u64 = 3;
    // Small gc_step_period ensures GC runs frequently on the observer.
    const GC_STEP_PERIOD: Duration =
        Duration::milliseconds(setup::builder::MIN_BLOCK_PROD_TIME as i64);

    let validators_spec = create_validators_spec(1, 0);
    let validator_id = validators_spec_clients(&validators_spec)[0].clone();
    let observer_id = create_account_id("observer");

    // Use "account5" as boundary — sender is "account0" (before boundary),
    // receiver is "account9" (after boundary). This gives us 2 shards.
    let boundary_account = create_account_id("account5");
    let sender = create_account_id("account0");
    let receiver = create_account_id("account9");
    let shard_layout = ShardLayout::multi_shard_custom(vec![boundary_account], 1);

    // Verify sender and receiver are on different shards.
    let sender_shard_id = shard_layout.account_id_to_shard_id(&sender);
    let receiver_shard_id = shard_layout.account_id_to_shard_id(&receiver);
    assert_ne!(
        sender_shard_id, receiver_shard_id,
        "sender and receiver must be on different shards for cross-shard test"
    );
    let source_shard_uid = ShardUId::from_shard_id_and_layout(sender_shard_id, &shard_layout);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(CROSS_SHARD_EPOCH_LENGTH)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(
            &[sender.clone(), receiver.clone()],
            Balance::from_near(1_000_000),
        )
        .build();

    let clients = vec![validator_id.clone(), observer_id.clone()];
    let source_shard_uid_clone = source_shard_uid;
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .gc_num_epochs_to_keep(CROSS_SHARD_GC_NUM_EPOCHS_TO_KEEP)
        .config_modifier(move |config, client_index| {
            if client_index == 0 {
                // Validator tracks all shards.
                config.tracked_shards_config = TrackedShardsConfig::AllShards;
            } else {
                // Observer tracks only the source shard + short GC period.
                config.tracked_shards_config =
                    TrackedShardsConfig::Shards(vec![source_shard_uid_clone]);
                config.gc.gc_step_period = GC_STEP_PERIOD;
            }
        })
        .build()
        .warmup();

    let signer = create_user_test_signer(&sender);

    // Step 1: Submit a cross-shard transfer (sender → receiver).
    let block_hash = env.node_for_account(&validator_id).head().last_block_hash;
    let tx = SignedTransaction::send_money(
        1,
        sender.clone(),
        receiver,
        &signer,
        Balance::from_yoctonear(100),
        block_hash,
    );
    let tx_hash = tx.get_hash();
    env.node_for_account(&validator_id).submit_tx(tx);

    // Step 2: Wait for the tx outcome on the validator; extract receipt_id.
    let tx_outcome = env
        .runner_for_account(&validator_id)
        .run_until_outcome_available(tx_hash, Duration::seconds(10));
    let [receipt_id] = tx_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single receipt from transaction")
    };

    // Step 3: Prove the receipt actually executed on the destination shard (all-shards validator).
    env.runner_for_account(&validator_id)
        .run_until_outcome_available(receipt_id, Duration::seconds(10));

    // Step 4: On the observer, verify the ReceiptToTx entry exists.
    let observer_store = env.node_for_account(&observer_id).store();
    assert!(
        observer_store.get(DBCol::ReceiptToTx, receipt_id.as_ref()).is_some(),
        "ReceiptToTx entry should exist on observer for the cross-shard receipt"
    );

    // Step 5: Verify the observer CANNOT see the receipt's execution outcome.
    // This proves gc_outcomes can never delete this entry on the observer.
    let all_outcome_ids: HashSet<CryptoHash> = observer_store
        .iter_ser::<Vec<CryptoHash>>(DBCol::OutcomeIds)
        .flat_map(|(_, ids)| ids)
        .collect();
    assert!(
        !all_outcome_ids.contains(&receipt_id),
        "cross-shard receipt should NOT appear in OutcomeIds on the source-only observer"
    );
    assert!(
        env.node_for_account(&observer_id)
            .client()
            .chain
            .get_execution_outcome(&receipt_id)
            .is_err(),
        "observer should not have execution outcome for cross-shard receipt"
    );

    // Step 6: Run enough blocks for GC to kick in.
    let num_blocks = CROSS_SHARD_EPOCH_LENGTH * CROSS_SHARD_GC_NUM_EPOCHS_TO_KEEP + 10;
    env.runner_for_account(&validator_id).run_for_number_of_blocks(num_blocks as usize);

    // Step 7: Verify the ReceiptToTx entry has been garbage collected on the observer.
    let observer_store = env.node_for_account(&observer_id).store();
    assert!(
        observer_store.get(DBCol::ReceiptToTx, receipt_id.as_ref()).is_none(),
        "ReceiptToTx for cross-shard receipt should be garbage collected on source-only observer"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
