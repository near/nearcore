use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::Balance;
use near_store::DBCol;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_account_id, create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

const EPOCH_LENGTH: u64 = 5;
const GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

/// Tests that local receipts are saved to the Receipts column and later garbage collected.
///
/// Sets up a single-validator single-shard network, deploys a contract (producing a local receipt),
/// verifies the receipt exists in DBCol::Receipts, then runs enough epochs for GC to kick in and
/// verifies the receipt is cleaned up.
#[test]
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

    // Deploy a contract — the deploy action produces a local receipt.
    let contract_code = near_test_contracts::rs_contract().to_vec();
    let block_hash = node.head(env.test_loop_data()).last_block_hash;
    let signer = create_user_test_signer(&user_account);
    let tx =
        SignedTransaction::deploy_contract(1, &user_account, contract_code, &signer, block_hash);
    let tx_hash = tx.get_hash();
    node.run_tx(&mut env.test_loop, tx, Duration::seconds(5));

    // Get the receipt ID produced by the transaction.
    let receipt_id = node.tx_receipt_id(env.test_loop_data(), tx_hash);

    // Verify the receipt exists in the Receipts column.
    let store = node.store(env.test_loop_data());
    assert!(
        store.get(DBCol::Receipts, receipt_id.as_ref()).is_some(),
        "receipt should exist in DBCol::Receipts after processing"
    );

    // Run enough epochs for GC to clean up the receipt.
    let num_blocks = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP + 1;
    node.run_for_number_of_blocks(&mut env.test_loop, num_blocks as usize);

    // Verify the receipt has been garbage collected.
    let store = node.store(env.test_loop_data());
    assert!(
        store.get(DBCol::Receipts, receipt_id.as_ref()).is_none(),
        "receipt should be garbage collected from DBCol::Receipts"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
