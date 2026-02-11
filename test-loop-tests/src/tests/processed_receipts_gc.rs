use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::gas::Gas;
use near_primitives::receipt::{
    ProcessedReceiptMetadata, Receipt, ReceiptSource, VersionedReceiptEnum,
};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{Balance, ShardId};
use near_primitives::utils::get_block_shard_id;
use near_store::DBCol;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_account_id, create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

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
#[cfg_attr(not(feature = "nightly"), ignore)]
fn test_processed_receipt_ids_gc() {
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

    let node = TestLoopNode::for_account(&env.node_datas, &env.node_datas[0].account_id);
    let signer = create_user_test_signer(&user_account);

    // Deploy the test contract.
    let contract_code = near_test_contracts::rs_contract().to_vec();
    let block_hash = node.head(env.test_loop_data()).last_block_hash;
    let deploy_tx =
        SignedTransaction::deploy_contract(1, &user_account, contract_code, &signer, block_hash);
    node.run_tx(&mut env.test_loop, deploy_tx, Duration::seconds(5));

    // Call yield_create — produces a local receipt and a PromiseYield instant receipt.
    let block_hash = node.head(env.test_loop_data()).last_block_hash;
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
    node.submit_tx(tx);

    // Wait for the local receipt to be processed (the transaction won't fully
    // complete because the yield callback is waiting for a resume).
    let tx_outcome =
        node.run_until_outcome_available(&mut env.test_loop, tx_hash, Duration::seconds(5));
    let [local_receipt_id] = tx_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single receipt from transaction")
    };
    let local_outcome = node.run_until_outcome_available(
        &mut env.test_loop,
        local_receipt_id,
        Duration::seconds(5),
    );
    // The local receipt produces exactly one child receipt — the PromiseYield instant receipt.
    let [instant_receipt_id] = local_outcome.outcome_with_id.outcome.receipt_ids[..] else {
        panic!("expected single receipt from local receipt execution")
    };

    let receipt_execution_block_hash = local_outcome.block_hash;
    let metadata_key = get_block_shard_id(&receipt_execution_block_hash, ShardId::new(0));

    // Verify local receipt exists in DBCol::Receipts.
    let store = node.store(env.test_loop_data());
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
            ProcessedReceiptMetadata::new(local_receipt_id, ReceiptSource::Local),
            ProcessedReceiptMetadata::new(instant_receipt_id, ReceiptSource::Instant),
        ]
    );

    #[cfg(feature = "test_features")]
    node.validate_store(&mut env.test_loop.data);

    // Run enough epochs for GC to clean up the receipts.
    let num_blocks = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    node.run_for_number_of_blocks(&mut env.test_loop, num_blocks as usize);

    // Verify the receipts have been garbage collected.
    let store = node.store(env.test_loop_data());
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
