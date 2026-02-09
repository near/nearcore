use std::str::FromStr;

use near_async::time::Duration;
use near_chain_configs::test_genesis::ValidatorsSpec;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::block::ChunkType;
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum, VersionedReceiptEnum};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::tests::yield_timeouts::{
    assert_no_promise_yield_status_in_state, get_yield_data_ids_in_latest_state,
};
use crate::utils::account::validators_spec_clients;
use crate::utils::node::TestLoopNode;

// The height of the next block after environment setup is complete.
const NEXT_BLOCK_HEIGHT_AFTER_SETUP: u64 = 3;

fn get_outgoing_receipts_from_latest_block(env: &TestLoopEnv) -> Vec<Receipt> {
    let client = TestLoopNode::for_account(&env.node_datas, &"validator0".parse().unwrap())
        .client(env.test_loop_data());
    let genesis_block = client.chain.get_block_by_height(0).unwrap();
    let epoch_id = *genesis_block.header().epoch_id();
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_id = shard_layout.account_id_to_shard_id(&"test0".parse::<AccountId>().unwrap());
    let last_block_hash = client.chain.head().unwrap().last_block_hash;
    let last_block_height = client.chain.head().unwrap().height;

    client
        .chain
        .get_outgoing_receipts_for_shard(last_block_hash, shard_id, last_block_height)
        .unwrap()
}

fn get_promise_yield_data_ids_from_latest_block(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_block(env) {
        if let VersionedReceiptEnum::PromiseYield(action_receipt) = receipt.versioned_receipt() {
            result.push(action_receipt.input_data_ids()[0]);
        }
    }
    result
}

fn get_promise_resume_data_ids_from_latest_block(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_block(env) {
        if let ReceiptEnum::PromiseResume(data_receipt) = receipt.receipt() {
            result.push(data_receipt.data_id);
        }
    }
    result
}

fn get_transaction_hashes_in_latest_block(env: &TestLoopEnv) -> Vec<CryptoHash> {
    let client = TestLoopNode::for_account(&env.node_datas, &"validator0".parse().unwrap())
        .client(env.test_loop_data());
    let chain = &client.chain;
    let block = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();

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

    let shard_layout = ShardLayout::single_shard();
    let user_accounts = vec![test_account.clone()];
    let initial_balance = Balance::from_near(1_000_000);
    let validators_spec = ValidatorsSpec::DesiredRoles {
        block_and_chunk_producers: vec!["validator0".parse().unwrap()],
        chunk_validators_only: Vec::new(),
    };
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .genesis_height(0)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients.clone())
        .skip_warmup()
        .build();

    let node = TestLoopNode::for_account(&env.node_datas, &clients[0]);
    assert_eq!(node.head(env.test_loop_data()).height, 0);

    let genesis_block = node.client(env.test_loop_data()).chain.get_block_by_height(0).unwrap();
    let deploy_contract_tx = SignedTransaction::deploy_contract(
        1,
        &test_account,
        near_test_contracts::rs_contract().into(),
        &test_account_signer,
        *genesis_block.hash(),
    );
    node.submit_tx(deploy_contract_tx.clone());

    // Allow two blocks for the contract to be deployed
    node.run_until_head_height(&mut env.test_loop, 2);
    assert!(matches!(
        node.client(env.test_loop_data())
            .chain
            .get_final_transaction_result(&deploy_contract_tx.get_hash())
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(_),
    ));

    let last_block_height = node.client(env.test_loop_data()).chain.head().unwrap().height;
    assert_eq!(NEXT_BLOCK_HEIGHT_AFTER_SETUP, last_block_height + 1);

    env
}

/// Submit one transaction which yields and saves data_id to state.
/// Process 2 blocks.
/// Submit another transaction which reads data_id from state and resumes.
/// Yield-resume should work.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_yield_then_resume_one_block_apart() {
    let mut env = prepare_env();
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let node = TestLoopNode::for_account(&env.node_datas, &"validator0".parse().unwrap());
    let genesis_block = node.client(env.test_loop_data()).chain.get_block_by_height(0).unwrap();
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
    node.submit_tx(yield_transaction);

    // Allow the yield create to be included and processed.
    for _ in 0..2 {
        node.run_until_head_height(&mut env.test_loop, next_block_height);
        next_block_height += 1;
    }
    assert_eq!(
        node.client(env.test_loop_data())
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
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
    node.submit_tx(resume_transaction);

    // Allow the yield resume to be included and processed.
    for _ in 0..2 {
        node.run_until_head_height(&mut env.test_loop, next_block_height);
        next_block_height += 1;
    }
    assert_eq!(get_promise_resume_data_ids_from_latest_block(&env).len(), 1);

    // In the next block the callback is executed and the promise resolves to its final result.
    node.run_until_head_height(&mut env.test_loop, next_block_height);
    assert_eq!(
        node.client(env.test_loop_data())
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
    );

    assert_no_promise_yield_status_in_state(&env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Submit one transaction which yields and saves data_id to state.
/// Submit another transaction which reads data_id from state and resumes.
/// The transactions are executed in the same block.
/// Yield-resume should work.
/// Before ProtocolFeature::InstantPromiseYield, the resume transaction failed in this scenario.
/// With the feature everything should work fine.
/// See https://github.com/near/nearcore/issues/14904, this test reproduces case 2)
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_yield_then_resume_same_block() {
    let mut env = prepare_env();
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let node = TestLoopNode::for_account(&env.node_datas, &"validator0".parse().unwrap());
    let genesis_block = node.client(env.test_loop_data()).chain.get_block_by_height(0).unwrap();
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
    node.submit_tx(yield_transaction);

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
    node.submit_tx(resume_transaction);

    // Allow the yield create and resume to be included and processed.
    for _ in 0..2 {
        node.run_until_head_height(&mut env.test_loop, next_block_height);
        next_block_height += 1;
    }

    // Both transactions should be included in the same block.
    let mut expected_txs = vec![resume_tx_hash, yield_tx_hash];
    expected_txs.sort();
    assert_eq!(get_transaction_hashes_in_latest_block(&env), expected_txs);

    // Run one more block to execute the resume callback.
    node.run_until_head_height(&mut env.test_loop, next_block_height);

    let client = node.client(env.test_loop_data());
    if ProtocolFeature::InstantPromiseYield.enabled(PROTOCOL_VERSION) {
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
    } else {
        // Resume callback has not executed, transaction is still waiting for resume
        assert_eq!(
            client.chain.get_partial_transaction_result(&yield_tx_hash).unwrap().status,
            FinalExecutionStatus::Started,
        );
        // promise_yield_resume returned 0 (resumption failed)
        assert_eq!(
            client.chain.get_partial_transaction_result(&resume_tx_hash).unwrap().status,
            FinalExecutionStatus::SuccessValue(vec![0u8]),
        );
    }

    assert_no_promise_yield_status_in_state(&env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
