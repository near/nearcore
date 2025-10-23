use std::iter::repeat_with;

use itertools::Itertools;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::gas::Gas;
use near_primitives::receipt::{DelayedReceiptIndices, StateStoredReceipt};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{ExecutionStatus, SignedTransaction};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{Balance, Nonce, ShardId};

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_account_id, create_validators_spec, validators_spec_clients_with_rpc,
};
use crate::utils::node::TestLoopNode;

#[test]
fn delayed_receipt_example_test() {
    init_test_logger();

    let user_account = create_account_id("user");
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let gas_limit = Gas::from_teragas(300);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout_single_shard()
        .validators_spec(validators_spec)
        .gas_limit(gas_limit)
        .add_user_account_simple(user_account.clone(), Balance::from_near(10))
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let rpc_node = TestLoopNode::rpc(&env.node_datas);
    let mut nonce: Nonce = 0;
    let mut next_nonce = || {
        nonce += 1;
        nonce
    };

    let deploy_test_contract_tx = SignedTransaction::deploy_contract(
        next_nonce(),
        &user_account,
        near_test_contracts::rs_contract().to_vec(),
        &create_user_test_signer(&user_account),
        rpc_node.head(env.test_loop_data()).last_block_hash,
    );
    rpc_node.run_tx(&mut env.test_loop, deploy_test_contract_tx, Duration::seconds(2));

    // Each transaction generates local receipt consuming more than a half
    // the chunk space, so chunk can only fit 2 such receipts.
    let gas_to_burn = gas_limit.checked_div(2).unwrap().checked_add(Gas::from_gas(1)).unwrap();
    let txs = repeat_with(|| {
        SignedTransaction::call(
            next_nonce(),
            user_account.clone(),
            user_account.clone(),
            &create_user_test_signer(&user_account),
            Balance::ZERO,
            "burn_gas_raw".to_owned(),
            gas_to_burn.as_gas().to_le_bytes().to_vec(),
            gas_limit,
            rpc_node.head(env.test_loop_data()).last_block_hash,
        )
    })
    .take(3)
    .collect_vec();
    for tx in &txs {
        rpc_node.submit_tx(tx.clone());
    }
    env.test_loop.run_until(
        |test_loop_data| {
            let head_block = rpc_node.head_block(test_loop_data);
            let chunk = rpc_node.block_chunks(test_loop_data, &head_block).pop().unwrap();
            chunk.to_transactions() == &txs
        },
        Duration::seconds(2),
    );

    let trie = rpc_node.trie(
        env.test_loop_data(),
        rpc_node.head(env.test_loop_data()).last_block_hash,
        ShardId::new(0),
    );
    let delayed_receipt_indices: DelayedReceiptIndices =
        near_store::get(&trie, &TrieKey::DelayedReceiptIndices).unwrap().unwrap();
    assert_eq!(
        delayed_receipt_indices,
        DelayedReceiptIndices { first_index: 0, next_available_index: 1 }
    );
    let delayed_receipt: StateStoredReceipt = near_store::get(
        &trie,
        &TrieKey::DelayedReceipt { index: delayed_receipt_indices.first_index },
    )
    .unwrap()
    .unwrap();
    let last_tx_outcome = rpc_node
        .client(env.test_loop_data())
        .chain
        .get_execution_outcome(txs.last().unwrap().hash())
        .unwrap();
    assert_eq!(
        last_tx_outcome.outcome_with_id.outcome.status,
        ExecutionStatus::SuccessReceiptId(*delayed_receipt.get_receipt().receipt_id())
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
