use itertools::Itertools;
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;
use near_vm_runner::ContractCode;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::{TestData, TestLoopEnv};
use crate::test_loop::utils::contract_distribution::{
    assert_all_chunk_endorsements_received, clear_compiled_contract_caches,
    run_until_caches_contain_contract,
};
use crate::test_loop::utils::transactions::{
    call_contract, check_txs, deploy_contract, make_account, make_accounts,
};
use crate::test_loop::utils::{get_head_height, ONE_NEAR};

const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 1;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 1;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;
const NUM_ACCOUNTS: usize = NUM_VALIDATORS;

/// Executes a test that deploys to a contract to an account and calls it.
fn test_contract_distribution_single_account(wait_cache_populate: bool, clear_cache: bool) {
    init_test_logger();
    let accounts = make_accounts(NUM_ACCOUNTS);

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = setup(&accounts);

    let rpc_id = make_account(0);
    let account = rpc_id.clone();
    let contract = ContractCode::new(near_test_contracts::sized_contract(100), None);

    let start_height = get_head_height(&mut test_loop, &node_datas);

    do_deploy_contract(&mut test_loop, &node_datas, &rpc_id, &account, &contract, 1);

    if wait_cache_populate {
        run_until_caches_contain_contract(&mut test_loop, &node_datas, contract.hash());
    }

    if clear_cache {
        clear_compiled_contract_caches(&mut test_loop, &node_datas);
    }

    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &account, 2);
    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &account, 3);

    let end_height = get_head_height(&mut test_loop, &node_datas);
    assert_all_chunk_endorsements_received(&mut test_loop, &node_datas, start_height, end_height);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests a simple scenario where we deploy and call a contract.
#[test]
fn test_contract_distribution_deploy_and_call_single_account() {
    test_contract_distribution_single_account(false, false);
}

/// Tests a simple scenario where we deploy and call a contract.
/// Waits for deploy action to take effect in contract cache before issuing the call actions.
#[test]
fn test_contract_distribution_wait_cache_populate_single_account() {
    test_contract_distribution_single_account(true, false);
}

/// Tests a simple scenario where we deploy a contract, and then we wait for cache to fill and
/// then clear the compiled contract cache and finally call the deployed contract call.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_contract_distribution_call_after_clear_single_account() {
    test_contract_distribution_single_account(true, true);
}

/// Executes a test that deploys to a contract to two different accounts and calls them.
fn test_contract_distribution_different_accounts(wait_cache_populate: bool, clear_cache: bool) {
    init_test_logger();
    let accounts = make_accounts(NUM_ACCOUNTS);

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = setup(&accounts);

    let rpc_id = make_account(0);
    let account0 = make_account(0);
    let account1 = make_account(1);
    let contract = ContractCode::new(near_test_contracts::sized_contract(100), None);

    let start_height = get_head_height(&mut test_loop, &node_datas);

    do_deploy_contract(&mut test_loop, &node_datas, &rpc_id, &account0, &contract, 1);
    do_deploy_contract(&mut test_loop, &node_datas, &rpc_id, &account1, &contract, 2);

    if wait_cache_populate {
        run_until_caches_contain_contract(&mut test_loop, &node_datas, contract.hash());
    }

    if clear_cache {
        clear_compiled_contract_caches(&mut test_loop, &node_datas);
    }

    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &account0, 3);
    do_call_contract(&mut test_loop, &node_datas, &rpc_id, &account1, 4);

    let end_height = get_head_height(&mut test_loop, &node_datas);
    assert_all_chunk_endorsements_received(&mut test_loop, &node_datas, start_height, end_height);

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests a simple scenario where we deploy and call a contract on two different accounts.
#[test]
fn test_contract_distribution_deploy_and_call_different_accounts() {
    test_contract_distribution_different_accounts(false, false);
}

/// Tests a simple scenario where we deploy and call a contract on two different accounts.
/// Waits for deploy action to take effect in contract cache before issuing the call actions.
#[test]
fn test_contract_distribution_wait_cache_populate_different_accounts() {
    test_contract_distribution_different_accounts(false, false);
}

/// Tests a simple scenario where we deploy a contract on two different accounts, and then we wait for cache to fill and
/// then clear the compiled contract cache and finally call the deployed contract on the two accounts.
#[cfg_attr(not(feature = "test_features"), ignore)]
#[test]
fn test_contract_distribution_call_after_clear_different_accounts() {
    test_contract_distribution_different_accounts(true, true);
}

fn setup(accounts: &Vec<AccountId>) -> TestLoopEnv {
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    // All block_and_chunk_producers will be both block and chunk validators.
    let block_and_chunk_producers =
        (0..NUM_BLOCK_AND_CHUNK_PRODUCERS).map(|idx| accounts[idx].as_str()).collect_vec();
    // These are the accounts that are only chunk validators, but not block/chunk producers.
    let chunk_validators_only = (NUM_BLOCK_AND_CHUNK_PRODUCERS..NUM_VALIDATORS)
        .map(|idx| accounts[idx].as_str())
        .collect_vec();

    let clients = accounts.iter().take(NUM_VALIDATORS).cloned().collect_vec();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(GENESIS_HEIGHT)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_single()
        .transaction_validity_period(1000)
        .epoch_length(EPOCH_LENGTH)
        .validators_desired_roles(&block_and_chunk_producers, &chunk_validators_only)
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let (genesis, epoch_config_store) = genesis_builder.build();

    let env =
        builder.genesis(genesis).epoch_config_store(epoch_config_store).clients(clients).build();
    env
}

fn do_deploy_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    account: &AccountId,
    contract: &ContractCode,
    nonce: u64,
) {
    tracing::info!(target: "test", "Deploying contract.");
    let tx =
        deploy_contract(test_loop, &node_datas, rpc_id, account, contract.code().to_vec(), nonce);
    test_loop.run_for(Duration::seconds(2));
    check_txs(test_loop, node_datas, rpc_id, &[tx]);
}

fn do_call_contract(
    test_loop: &mut TestLoopV2,
    node_datas: &Vec<TestData>,
    rpc_id: &AccountId,
    account: &AccountId,
    nonce: u64,
) {
    tracing::info!(target: "test", "Calling contract.");
    let tx = call_contract(
        test_loop,
        node_datas,
        rpc_id,
        account,
        account,
        "main".to_owned(),
        vec![],
        nonce,
    );
    test_loop.run_for(Duration::seconds(2));
    check_txs(test_loop, node_datas, rpc_id, &[tx]);
}
