use std::collections::BTreeMap;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use assert_matches::assert_matches;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_crypto::Signer;
use near_o11y::testonly::init_test_logger;
use near_primitives::action::{Action, FunctionCallAction};
use near_primitives::block::ChunkType;
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::gas::Gas;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, ReceiptEnum, VersionedReceiptEnum};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::FinalExecutionStatus;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::tests::yield_timeouts::{
    assert_no_promise_yield_status_in_state, get_yield_data_ids_in_latest_state,
};
use crate::utils::account::validators_spec_clients;

// The height of the next block after environment setup is complete.
const NEXT_BLOCK_HEIGHT_AFTER_SETUP: u64 = 3;

fn get_outgoing_receipts_from_latest_executed_block(env: &mut TestLoopEnv) -> Vec<Receipt> {
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

fn get_promise_yield_data_ids_from_latest_executed_block(env: &mut TestLoopEnv) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_executed_block(env) {
        if let VersionedReceiptEnum::PromiseYield(action_receipt) = receipt.versioned_receipt() {
            result.push(action_receipt.input_data_ids()[0]);
        }
    }
    result
}

fn get_promise_resume_data_ids_from_latest_executed_block(
    env: &mut TestLoopEnv,
) -> Vec<CryptoHash> {
    let mut result = vec![];
    for receipt in get_outgoing_receipts_from_latest_executed_block(env) {
        if let ReceiptEnum::PromiseResume(data_receipt) = receipt.receipt() {
            result.push(data_receipt.data_id);
        }
    }
    result
}

fn get_transaction_hashes_in_latest_executed_block(env: &mut TestLoopEnv) -> Vec<CryptoHash> {
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
        .clients(clients)
        .skip_warmup()
        .build();

    assert_eq!(env.validator().head().height, 0);

    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
    let deploy_contract_tx = SignedTransaction::deploy_contract(
        1,
        &test_account,
        near_test_contracts::rs_contract().into(),
        &test_account_signer,
        *genesis_block.hash(),
    );
    env.validator().submit_tx(deploy_contract_tx.clone());

    // Allow two blocks for the contract to be deployed
    env.validator_runner().run_until_executed_height(2);
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
    assert_eq!(NEXT_BLOCK_HEIGHT_AFTER_SETUP, last_block_height + 1);

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
    if ProtocolFeature::InstantPromiseYield.enabled(PROTOCOL_VERSION) {
        // After InstantPromiseYield, the PromiseYield receipt is applied instantly, it's not an
        // outgoing receipt.
        assert_eq!(get_promise_yield_data_ids_from_latest_executed_block(&mut env).len(), 0);

        // The PromiseYield receipt should be saved in the state.
        assert_eq!(get_yield_data_ids_in_latest_state(&mut env).len(), 1);
    } else {
        // Before InstantPromiseYield, there should be an outgoing PromiseYield receipt generated by
        // the latest block.
        assert_eq!(get_promise_yield_data_ids_from_latest_executed_block(&mut env).len(), 1);
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
    env.validator().submit_tx(resume_transaction);

    // Allow the yield resume to be included and processed.
    for _ in 0..2 {
        env.validator_runner().run_until_executed_height(next_block_height);
        next_block_height += 1;
    }
    assert_eq!(get_promise_resume_data_ids_from_latest_executed_block(&mut env).len(), 1);

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

    assert_no_promise_yield_status_in_state(&mut env);
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
fn test_yield_then_resume_same_block() {
    let mut env = prepare_env();
    let signer = create_user_test_signer(&AccountId::from_str("test0").unwrap());
    let genesis_block = env.validator().client().chain.get_block_by_height(0).unwrap();
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
    assert_eq!(get_transaction_hashes_in_latest_executed_block(&mut env), expected_txs);

    // Run one more block to execute the resume callback.
    env.validator_runner().run_until_executed_height(next_block_height);

    {
        let node = env.validator();
        let client = node.client();
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
    }

    assert_no_promise_yield_status_in_state(&mut env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Submit one transaction with two actions. The first action yields and saves data_id to the state.
/// The second actions reads data_id from state and resumes.
/// Yield-resume should work.
/// Before ProtocolFeature::YieldResumeImprovements, the resume transaction failed in this scenario.
/// With the feature everything should work fine.
/// See https://github.com/near/nearcore/issues/14904, this test reproduces case 4)
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
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
    env.validator().submit_tx(yield_resume_transaction.clone());
    env.validator_runner().run_for_number_of_blocks(3);

    {
        let node = env.validator();
        let client = node.client();
        if ProtocolFeature::YieldResumeImprovements.enabled(PROTOCOL_VERSION) {
            // promise_yield_resume returned 1 (resumption succeeded)
            assert_eq!(
                client
                    .chain
                    .get_final_transaction_result(&yield_resume_transaction.get_hash())
                    .unwrap()
                    .status,
                FinalExecutionStatus::SuccessValue(vec![1u8]),
            );
        } else {
            // promise_yield_resume returned 0 (resumption failed)
            assert_eq!(
                client
                    .chain
                    .get_partial_transaction_result(&yield_resume_transaction.get_hash())
                    .unwrap()
                    .status,
                FinalExecutionStatus::SuccessValue(vec![0u8]),
            );

            // The receipt has not been resumed. Final status unavailable.
            assert_matches!(
                client.chain.get_final_transaction_result(&yield_resume_transaction.get_hash()),
                Err(_)
            );

            // 200 blocks later the PromiseYield times out and the transaction finishes.
        }
    }

    assert_no_promise_yield_status_in_state(&mut env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Similar to `test_yield_then_resume_two_actions`, but after the first action, another action fails.
/// When processing the receipt fails, the yielded receipt should be cancelled and there should be
/// no PromiseYieldStatus in the state, even though it was written by the first action.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
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
    assert_no_promise_yield_status_in_state(&mut env);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

// 1 node
// Upgrade from protocol before InstantYieldResume and YieldResumeImprovements to after.
// Make sure that a receipt yielded before the protocol upgrade can be resumed after the upgrade.
// Protocol upgrade setup was taken from `fn test_protocol_upgrade`.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_yield_resume_across_protocol_upgrade() {
    init_test_logger();

    let old_protocol = ProtocolFeature::InstantPromiseYield.protocol_version() - 1;
    let new_protocol = ProtocolFeature::YieldResumeImprovements.protocol_version();
    let epoch_length = 5;

    // Currently this test runs only on nightly.
    // If PROTOCOL_VERSION is lower than `new_protocol`, the client will fail with a message like:
    // "The client protocol version is older than the protocol version of the network: 131 > 84"
    if PROTOCOL_VERSION < new_protocol {
        return;
    }

    // The features are not enabled in the old protocol
    assert!(!ProtocolFeature::InstantPromiseYield.enabled(old_protocol));
    assert!(!ProtocolFeature::YieldResumeImprovements.enabled(old_protocol));

    // They are enabled in the new protocol
    assert!(ProtocolFeature::InstantPromiseYield.enabled(new_protocol));
    assert!(ProtocolFeature::YieldResumeImprovements.enabled(new_protocol));

    let test_account: AccountId = "test0".parse().unwrap();
    let test_account_signer: Signer = create_user_test_signer(&test_account).into();

    let shard_layout = ShardLayout::single_shard();
    let user_accounts = vec![test_account.clone()];
    let initial_balance = Balance::from_near(1_000_000);
    let validators_spec = ValidatorsSpec::DesiredRoles {
        block_and_chunk_producers: vec!["validator0".parse().unwrap()],
        chunk_validators_only: Vec::new(),
    };
    let clients = validators_spec_clients(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(old_protocol)
        .shard_layout(shard_layout.clone())
        .epoch_length(epoch_length)
        .validators_spec(validators_spec.clone())
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();

    let genesis_epoch_info = TestEpochConfigBuilder::new()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .build();

    let mainnet_epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let old_epoch_config: EpochConfig =
        mainnet_epoch_config_store.get_config(old_protocol).deref().clone();
    let new_epoch_config: EpochConfig =
        mainnet_epoch_config_store.get_config(new_protocol).deref().clone();

    // Adjust the epoch configs for the test
    let adjust_epoch_config = |mut config: EpochConfig| -> EpochConfig {
        config.epoch_length = epoch_length;
        config.num_block_producer_seats = genesis_epoch_info.num_block_producer_seats;
        config.num_chunk_producer_seats = genesis_epoch_info.num_chunk_producer_seats;
        config.num_chunk_validator_seats = genesis_epoch_info.num_chunk_validator_seats;
        config.with_shard_layout(shard_layout.clone())
    };
    let old_epoch_config = adjust_epoch_config(old_epoch_config);
    let new_epoch_config = adjust_epoch_config(new_epoch_config);

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (old_protocol, Arc::new(old_epoch_config)),
        (new_protocol, Arc::new(new_epoch_config)),
        // Need to pass a config for PROTOCOL_VERSION because some setup code in client asks for the
        // shard layout in PROTOCOL_VERSION to estimate size of a thread pool. Not used in the test
        // itself.
        (PROTOCOL_VERSION, mainnet_epoch_config_store.get_config(PROTOCOL_VERSION).clone()),
    ]));

    // Immediately start voting for the new protocol version
    let protocol_upgrade_schedule = ProtocolUpgradeVotingSchedule::new_immediate(new_protocol);

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .protocol_upgrade_schedule(protocol_upgrade_schedule)
        .clients(clients)
        .build()
        .warmup();

    // We're in the epoch corresponding to the old_protocol_version
    let start_head = env.validator().head();
    assert_eq!(
        env.validator()
            .client()
            .epoch_manager
            .get_epoch_protocol_version(&start_head.epoch_id)
            .unwrap(),
        old_protocol
    );

    // Deploy the contract
    let deploy_contract_tx = SignedTransaction::deploy_contract(
        1,
        &test_account,
        near_test_contracts::rs_contract().into(),
        &test_account_signer,
        start_head.last_block_hash,
    );
    env.validator_runner().run_tx(deploy_contract_tx, Duration::seconds(5));

    // Perform promise_yield_create
    let yield_payload = vec![6u8; 16];
    let yield_transaction = SignedTransaction::from_actions(
        200,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &test_account_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_create_return_promise".to_string(),
            args: yield_payload.clone(),
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        start_head.last_block_hash,
    );
    let yield_tx_hash = yield_transaction.get_hash();
    env.validator().submit_tx(yield_transaction);

    // Allow the yield create to be included and processed.
    env.validator_runner().run_for_number_of_blocks(2);
    assert_eq!(
        env.validator()
            .client()
            .chain
            .get_partial_transaction_result(&yield_tx_hash)
            .unwrap()
            .status,
        FinalExecutionStatus::Started
    );

    // Make sure we're still on the old protocol version
    let epoch_id = env.validator().head().epoch_id;
    assert_eq!(
        env.validator().client().epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap(),
        old_protocol
    );

    // Upgrade to the new protocol version
    env.validator_runner().run_until(
        |node| {
            let cur_epoch_protocol_version = node
                .client()
                .epoch_manager
                .get_epoch_protocol_version(&node.head().epoch_id)
                .unwrap();
            cur_epoch_protocol_version == new_protocol
        },
        Duration::seconds(30),
    );

    // Run resume
    let resume_transaction = SignedTransaction::from_actions(
        201,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &test_account_signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "call_yield_resume_read_data_id_from_storage".to_string(),
            args: yield_payload,
            gas: Gas::from_teragas(300),
            deposit: Balance::ZERO,
        }))],
        start_head.last_block_hash,
    );
    let resume_res = env.validator_runner().run_tx(resume_transaction, Duration::seconds(5));
    assert_eq!(resume_res, vec![1u8]);

    // Allow the resume to execute
    env.validator_runner().run_for_number_of_blocks(2);

    // Resumed callback executed, transaction finished successfully
    assert_eq!(
        env.validator().client().chain.get_final_transaction_result(&yield_tx_hash).unwrap().status,
        FinalExecutionStatus::SuccessValue(vec![16u8]),
    );

    assert_no_promise_yield_status_in_state(&mut env);
    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
