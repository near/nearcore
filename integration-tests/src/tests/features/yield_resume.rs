use near_chain_configs::Genesis;
use near_client::ProcessTxResponse;
use near_crypto::InMemorySigner;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::receipt::ReceiptEnum::PromiseResume;
use near_primitives::receipt::VersionedReceiptEnum::PromiseYield;
use near_primitives::transaction::{
    Action, DeployContractAction, FunctionCallAction, SignedTransaction,
};
use near_primitives::types::AccountId;
use near_primitives::types::{Balance, Gas};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use crate::tests::features::yield_timeouts::get_yield_data_ids_in_latest_state;

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
        if let PromiseYield(action_receipt) = receipt.versioned_receipt() {
            result.push(action_receipt.input_data_ids()[0]);
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
        genesis.config.gas_limit = Gas::from_gas(gas_limit);
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

/// Submit one transaction which yields and saves data_id to state.
/// Process 2 blocks.
/// Submit another transaction which reads data_id from state and resumes.
/// Yield-resume should work.
#[test]
fn test_yield_then_resume_one_block_apart() {
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
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
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
    if ProtocolFeature::InstantPromiseYield.enabled(PROTOCOL_VERSION) {
        // After InstantPromiseYield, the PromiseYield receipt is applied instantly, it's not an
        // outgoing receipt.
        assert_eq!(get_promise_yield_data_ids_from_latest_block(&env).len(), 0);

        // The PromiseYield receipt should be saved in the state.
        assert_eq!(get_yield_data_ids_in_latest_state(&env).len(), 1);
    } else {
        // Before InstantPromiseYield, there should be an outgoing PromiseYield receipt generated by
        // the latest block.
        assert_eq!(get_promise_yield_data_ids_from_latest_block(&env).len(), 1);
    }

    // Add another transaction invoking `yield_resume`.
    let resume_transaction = SignedTransaction::from_actions(
        201,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_resume_read_data_id_from_storage".to_string(),
            args: yield_payload,
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
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

/// Submit one transaction which yields and saves data_id to state.
/// Submit another transaction which reads data_id from state and resumes.
/// The transactions are executed in the same block.
/// Yield-resume should work.
/// Before ProtocolFeature::InstantYieldResume, the resume transaction failed in this scenario.
/// With the feature everything should work fine.
/// See https://github.com/near/nearcore/issues/14904, this test reproduces case 2)
#[test]
fn test_yield_then_resume_same_block() {
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
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
    );
    let yield_tx_hash = yield_transaction.get_hash();
    assert_eq!(
        env.rpc_handlers[0].process_tx(yield_transaction, false, false),
        ProcessTxResponse::ValidTx
    );

    // Add another transaction invoking `yield_resume`.
    let resume_transaction = SignedTransaction::from_actions(
        201,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_resume_read_data_id_from_storage".to_string(),
            args: yield_payload,
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        *genesis_block.hash(),
    );
    let resume_tx_hash = resume_transaction.get_hash();
    assert_eq!(
        env.rpc_handlers[0].process_tx(resume_transaction, false, false),
        ProcessTxResponse::ValidTx
    );

    // Allow the yield create and resume to be included and processed.
    for _ in 0..3 {
        env.produce_block(0, next_block_height);
        next_block_height += 1;
    }

    if ProtocolFeature::InstantPromiseYield.enabled(PROTOCOL_VERSION) {
        // Resumed callback executed, transaction finished successfully
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::SuccessValue(vec![16u8]),
        );
        // promise_yield_resume returned 1 (resumption succeeded)
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&resume_tx_hash).unwrap().status,
            FinalExecutionStatus::SuccessValue(vec![1u8]),
        );
    } else {
        // Resume callback has not executed, transaction is still waiting for resume
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::Started,
        );
        // promise_yield_resume returned 0 (resumption failed)
        assert_eq!(
            env.clients[0].chain.get_partial_transaction_result(&resume_tx_hash).unwrap().status,
            FinalExecutionStatus::SuccessValue(vec![0u8]),
        );
    }
}
