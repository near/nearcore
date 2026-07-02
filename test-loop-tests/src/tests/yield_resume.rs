use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::tests::yield_timeouts::{
    assert_no_promise_yield_status_in_state, get_yield_data_ids_in_latest_state,
};
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::block::ChunkType;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum, VersionedReceiptEnum};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;
use std::str::FromStr;

// The height of the next block after environment setup is complete.
// Under AccountCostIncrease the deploy_contract receipt also produces a refund receipt
// that is processed in an additional block, so we wait one more block during setup.
fn next_block_height_after_setup() -> u64 {
    if ProtocolFeature::AccountCostIncrease.enabled(PROTOCOL_VERSION) { 4 } else { 3 }
}

fn get_outgoing_receipts_from_latest_executed_block(env: &TestLoopEnv) -> Vec<Receipt> {
    let node = env.validator();
    let client = node.client();
    let last_executed = node.last_executed();
    let genesis_block = client.chain.get_block_by_height(0).unwrap();
    let epoch_id = *genesis_block.header().epoch_id();
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_id = shard_layout.account_id_to_shard_id(&"test0".parse::<AccountId>().unwrap());

    client
        .chain
        .get_outgoing_receipts_for_shard(
            last_executed.last_block_hash,
            shard_id,
            last_executed.height,
        )
        .unwrap()
}

fn get_promise_yield_data_ids_from_latest_executed_block(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_executed_block(env) {
        if let VersionedReceiptEnum::PromiseYield(action_receipt) = receipt.versioned_receipt() {
            result.push(action_receipt.input_data_ids()[0]);
        }
    }
    result
}

fn get_promise_resume_data_ids_from_latest_executed_block(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_executed_block(env) {
        if let ReceiptEnum::PromiseResume(data_receipt) = receipt.receipt() {
            result.push(data_receipt.data_id);
        }
    }
    result
}

fn get_transaction_hashes_in_latest_executed_block(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let node = env.validator();
    let client = node.client();
    let last_executed = node.last_executed();
    let chain = &client.chain;
    let block = chain.get_block(&last_executed.last_block_hash).unwrap();

    let mut transactions = Vec::new();
    for c in block.chunks().iter() {
        if let ChunkType::New(new_chunk_header) = c {
            let chunk = chain.get_chunk(new_chunk_header.chunk_hash()).unwrap();
            for tx in chunk.into_transactions() {
                transactions.push(tx.get_hash());
            }
        }
    }
    transactions.sort();
    transactions
}

/// Create environment with deployed test contract.
fn prepare_env() -> TestLoopEnv {
    init_test_logger();

    let test_account: AccountId = "test0".parse().unwrap();
    let test_account_signer = create_user_test_signer(&test_account).into();

    let mut env = TestLoopBuilder::new()
        .genesis_height(0)
        .add_user_account(&test_account, Balance::from_near(1_000_000))
        .skip_warmup()
        .build();

    assert_eq!(env.validator().head().height, 0);

    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
    let deploy_contract_tx = SignedTransaction::deploy_contract(
        1,
        &test_account,
        near_test_contracts::backwards_compatible_rs_contract().into(),
        &test_account_signer,
        *genesis_block.hash(),
    );
    env.validator().submit_tx(deploy_contract_tx.clone());

    // Allow enough blocks for the contract to be deployed (and, under
    // AccountCostIncrease, for the price_surplus refund receipt to be processed).
    let setup_height = next_block_height_after_setup() - 1;
    env.validator_runner().run_until_executed_height(setup_height);
    assert!(matches!(
        env.validator()
            .client()
            .chain
            .get_final_transaction_result(&deploy_contract_tx.get_hash())
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(_),
    ));

    let last_block_height = env.validator().last_executed().height;
    assert_eq!(next_block_height_after_setup(), last_block_height + 1);

    env
}

/// Submit one transaction which yields and saves data_id to state.
/// Process 2 blocks.
/// Submit another transaction which reads data_id from state and resumes.
/// Yield-resume should work.
#[test]
fn test_yield_then_resume_one_block_apart() {
    let mut env = prepare_env();
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
    let mut next_block_height = next_block_height_after_setup();
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
    env.validator().submit_tx(yield_transaction);

    // Allow the yield create to be included and processed.
    for _ in 0..2 {
        env.validator_runner().run_until_executed_height(next_block_height);
        next_block_height += 1;
    }
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started
    );
    // The PromiseYield receipt is applied instantly, it's not an outgoing receipt.
    assert_eq!(get_promise_yield_data_ids_from_latest_executed_block(&env).len(), 0);

    // The PromiseYield receipt should be saved in the state.
    assert_eq!(get_yield_data_ids_in_latest_state(&env).len(), 1);

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
    env.validator().submit_tx(resume_transaction);

    // Allow the yield resume to be included and processed.
    for _ in 0..2 {
        env.validator_runner().run_until_executed_height(next_block_height);
        next_block_height += 1;
    }
    assert_eq!(get_promise_resume_data_ids_from_latest_executed_block(&env).len(), 1);

    // In the next block the callback is executed and the promise resolves to its final result.
    env.validator_runner().run_until_executed_height(next_block_height);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
    );

    assert_no_promise_yield_status_in_state(&env);
}

/// Submit one transaction which yields and saves data_id to state.
/// Submit another transaction which reads data_id from state and resumes.
/// The transactions are executed in the same block.
/// Yield-resume should work.
/// See https://github.com/near/nearcore/issues/14904, this test reproduces case 2)
#[test]
fn test_yield_then_resume_same_block() {
    let mut env = prepare_env();
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
    let mut next_block_height = next_block_height_after_setup();
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
    env.validator().submit_tx(yield_transaction);

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
    env.validator().submit_tx(resume_transaction);

    // Allow the yield create and resume to be included and processed.
    for _ in 0..2 {
        env.validator_runner().run_until_executed_height(next_block_height);
        next_block_height += 1;
    }

    // Both transactions should be included in the same block.
    let mut expected_txs = vec![resume_tx_hash, yield_tx_hash];
    expected_txs.sort();
    assert_eq!(get_transaction_hashes_in_latest_executed_block(&env), expected_txs);

    // Run one more block to execute the resume callback.
    env.validator_runner().run_until_executed_height(next_block_height);

    {
        let node = env.validator();
        let client = node.client();
        // Resumed callback executed, transaction finished successfully
        assert_eq!(
            client.chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::SuccessValue(vec![16u8]),
        );
        // promise_yield_resume returned 1 (resumption succeeded)
        assert_eq!(
            client.chain.get_partial_transaction_result(&resume_tx_hash).unwrap().status,
            FinalExecutionStatus::SuccessValue(vec![1u8]),
        );
    }

    assert_no_promise_yield_status_in_state(&env);
}

/// Submit one transaction with two actions. The first action yields and saves data_id to the state.
/// The second actions reads data_id from state and resumes.
/// Yield-resume should work.
/// See https://github.com/near/nearcore/issues/14904, this test reproduces case 4)
#[test]
fn test_yield_then_resume_two_actions() {
    let mut env = prepare_env();
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
    let yield_payload = vec![6u8; 16];

    let yield_resume_transaction = SignedTransaction::from_actions(
        200,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_yield_create_return_promise".to_string(),
                args: yield_payload.clone(),
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            })),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_yield_resume_read_data_id_from_storage".to_string(),
                args: yield_payload,
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            })),
        ],
        *genesis_block.hash(),
    );
    env.validator_runner()
        .execute_tx(yield_resume_transaction.clone(), Duration::seconds(5))
        .unwrap();

    {
        let node = env.validator();
        let client = node.client();
        // promise_yield_resume returned 1 (resumption succeeded)
        assert_eq!(
            client
                .chain
                .get_final_transaction_result(&yield_resume_transaction.get_hash())
                .unwrap()
                .status,
            FinalExecutionStatus::SuccessValue(vec![1u8]),
        );
    }

    assert_no_promise_yield_status_in_state(&env);
}

/// Similar to `test_yield_then_resume_two_actions`, but after the first action, another action fails.
/// When processing the receipt fails, the yielded receipt should be cancelled and there should be
/// no PromiseYieldStatus in the state, even though it was written by the first action.
#[test]
fn test_yield_then_resume_two_actions_failure() {
    let mut env = prepare_env();
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
    let yield_payload = vec![6u8; 16];

    let tx = SignedTransaction::from_actions(
        200,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_yield_create_return_promise".to_string(),
                args: yield_payload.clone(),
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            })),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "panic_with_message".to_string(),
                args: Vec::new(),
                gas: Gas::from_teragas(50),
                deposit: Balance::ZERO,
            })),
            Action::FunctionCall(Box::new(FunctionCallAction {
                method_name: "call_yield_resume_read_data_id_from_storage".to_string(),
                args: yield_payload,
                gas: Gas::from_teragas(100),
                deposit: Balance::ZERO,
            })),
        ],
        *genesis_block.hash(),
    );

    let res = env.validator_runner().execute_tx(tx, Duration::seconds(5));
    assert_matches!(res.unwrap().status, FinalExecutionStatus::Failure(_));

    // PromiseYieldStatus change was not committed to the trie.
    assert_no_promise_yield_status_in_state(&env);
}
