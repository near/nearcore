use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::InMemorySigner;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::receipt::ReceiptEnum::{PromiseResume, PromiseYield};
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::AccountId;
use near_primitives::views::FinalExecutionStatus;

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;

// The height of the next block after environment setup is complete.
const NEXT_BLOCK_HEIGHT_AFTER_SETUP: u64 = 3;

fn get_outgoing_receipts_from_latest_block(env: &TestEnv) -> Vec<Receipt> {
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let epoch_id = *genesis_block.header().epoch_id();
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_id = shard_layout.account_id_to_shard_id(&"test0".parse::<AccountId>().unwrap());
    let last_block_hash = env.clients[0].chain.head().unwrap().last_block_hash;
    let last_block_height = env.clients[0].chain.head().unwrap().height;

    env.clients[0]
        .chain
        .get_outgoing_receipts_for_shard(last_block_hash, shard_id, last_block_height)
        .unwrap()
}

fn get_promise_yield_data_ids_from_latest_block(env: &TestEnv) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_block(&env) {
        if let PromiseYield(action_receipt) = receipt.receipt() {
            result.push(action_receipt.input_data_ids[0]);
        }
    }
    result
}

fn get_promise_resume_data_ids_from_latest_block(env: &TestEnv) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_block(&env) {
        if let PromiseResume(data_receipt) = receipt.receipt() {
            result.push(data_receipt.data_id);
        }
    }
    result
}

/// Create environment with deployed test contract.
fn prepare_env(test_env_gas_limit: Option<u64>) -> TestEnv {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    if let Some(gas_limit) = test_env_gas_limit {
        genesis.config.gas_limit = gas_limit;
    }
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());

    // Submit transaction deploying contract to test0
    let tx = SignedTransaction::from_actions(
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
        0,
    );
    let tx_hash = tx.get_hash();
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    // Allow two blocks for the contract to be deployed
    for i in 1..3 {
        env.produce_block(0, i);
    }
    assert!(matches!(
        env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(_),
    ));

    let last_block_height = env.clients[0].chain.head().unwrap().height;
    assert_eq!(NEXT_BLOCK_HEIGHT_AFTER_SETUP, last_block_height + 1);

    env
}

/// In this test, yield and resume are invoked in separate transactions as quickly as possible.
#[test]
fn yield_then_resume() {
    let mut env = prepare_env(None);
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let mut next_block_height = NEXT_BLOCK_HEIGHT_AFTER_SETUP;
    let yield_payload = vec![6u8; 16];

    // Add a transaction invoking `yield_create`.
    let yield_transaction = SignedTransaction::from_actions(
        200,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_create_return_promise".to_string(),
            args: yield_payload.clone(),
            gas: 300_000_000_000_000,
            deposit: 0,
        }))],
        *genesis_block.hash(),
        0,
    );
    let yield_tx_hash = yield_transaction.get_hash();
    assert_eq!(
        env.rpc_handlers[0].process_tx(yield_transaction, false, false),
        ProcessTxResponse::ValidTx
    );

    // Allow the yield create to be included and processed.
    for _ in 0..2 {
        env.produce_block(0, next_block_height);
        next_block_height += 1;
    }
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::Started
    );
    assert_eq!(get_promise_yield_data_ids_from_latest_block(&env).len(), 1);

    // Add another transaction invoking `yield_resume`.
    let resume_transaction = SignedTransaction::from_actions(
        201,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_resume_read_data_id_from_storage".to_string(),
            args: yield_payload,
            gas: 300_000_000_000_000,
            deposit: 0,
        }))],
        *genesis_block.hash(),
        0,
    );
    assert_eq!(
        env.rpc_handlers[0].process_tx(resume_transaction, false, false),
        ProcessTxResponse::ValidTx
    );

    // Allow the yield resume to be included and processed.
    for _ in 0..2 {
        env.produce_block(0, next_block_height);
        next_block_height += 1;
    }
    assert_eq!(get_promise_resume_data_ids_from_latest_block(&env).len(), 1);

    // In the next block the callback is executed and the promise resolves to its final result.
    env.produce_block(0, next_block_height);
    assert_eq!(
        env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
    );
}
