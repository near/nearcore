use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;
use near_vm_runner::ContractCode;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::contract_distribution::{
    assert_all_chunk_endorsements_received, clear_compiled_contract_caches,
    run_until_caches_contain_contract,
};
use crate::test_loop::utils::transactions::{
    do_call_contract, do_delete_account, do_deploy_contract, make_account, make_accounts,
};
use crate::test_loop::utils::{get_head_height, ONE_NEAR};

const EPOCH_LENGTH: u64 = 10;
const GENESIS_HEIGHT: u64 = 1000;

const NUM_BLOCK_AND_CHUNK_PRODUCERS: usize = 1;
const NUM_CHUNK_VALIDATORS_ONLY: usize = 1;
const NUM_VALIDATORS: usize = NUM_BLOCK_AND_CHUNK_PRODUCERS + NUM_CHUNK_VALIDATORS_ONLY;

/// Executes a test that deploys to a contract to an account and calls it.
fn test_contract_distribution_single_account(wait_cache_populate: bool, clear_cache: bool) {
    init_test_logger();
    // We need 1 more non-validator account to create, deploy-contract, and delete.
    let accounts = make_accounts(NUM_VALIDATORS + 1);

    let mut env = setup(&accounts);

    let rpc_id = make_account(0);
    // Choose an account that is not a validator, so we can delete it.
    let contract_id = accounts[NUM_VALIDATORS].clone();
    let contract = ContractCode::new(near_test_contracts::sized_contract(100), None);
    let method_name = "main".to_owned();
    let args = vec![];

    let start_height = get_head_height(&mut env);

    do_deploy_contract(&mut env, &rpc_id, &contract_id, contract.code().to_vec());

    if wait_cache_populate {
        run_until_caches_contain_contract(&mut env, contract.hash());
    }

    if clear_cache {
        clear_compiled_contract_caches(&mut env);
    }

    do_call_contract(
        &mut env,
        &rpc_id,
        &contract_id,
        &contract_id,
        method_name.clone(),
        args.clone(),
    );
    do_call_contract(&mut env, &rpc_id, &contract_id, &contract_id, method_name, args);

    do_delete_account(&mut env, &rpc_id, &contract_id, &rpc_id);

    let end_height = get_head_height(&mut env);
    assert_all_chunk_endorsements_received(&mut env, start_height, end_height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
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
    // We need 2 more non-validator accounts to create, deploy-contract, and delete.
    let accounts = make_accounts(NUM_VALIDATORS + 2);

    let mut env = setup(&accounts);

    let rpc_id = make_account(0);
    // Choose accounts that are not validators, so we can delete them.
    let contract_id1 = accounts[NUM_VALIDATORS].clone();
    let contract_id2 = accounts[NUM_VALIDATORS + 1].clone();
    let contract = ContractCode::new(near_test_contracts::sized_contract(100), None);
    let method_name = "main".to_owned();
    let args = vec![];

    let start_height = get_head_height(&mut env);

    do_deploy_contract(&mut env, &rpc_id, &contract_id1, contract.code().to_vec());
    do_deploy_contract(&mut env, &rpc_id, &contract_id2, contract.code().to_vec());

    if wait_cache_populate {
        run_until_caches_contain_contract(&mut env, contract.hash());
    }

    if clear_cache {
        clear_compiled_contract_caches(&mut env);
    }

    do_call_contract(
        &mut env,
        &rpc_id,
        &contract_id1,
        &contract_id1,
        method_name.clone(),
        args.clone(),
    );
    do_call_contract(&mut env, &rpc_id, &contract_id2, &contract_id2, method_name, args);

    do_delete_account(&mut env, &rpc_id, &contract_id1, &rpc_id);
    do_delete_account(&mut env, &rpc_id, &contract_id2, &rpc_id);

    let end_height = get_head_height(&mut env);
    assert_all_chunk_endorsements_received(&mut env, start_height, end_height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
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

/// Executes a test that deploys and calls different contracts to the same account.
#[test]
fn test_contract_distribution_deply_and_call_multiple_contracts() {
    init_test_logger();
    // We need 1 more non-validator account to create, deploy-contract, and delete.
    let accounts = make_accounts(NUM_VALIDATORS + 1);

    let mut env = setup(&accounts);

    let rpc_id = make_account(0);
    // Choose accounts that are not validators.
    let contract_id = accounts[NUM_VALIDATORS].clone();
    let contracts = (0..3)
        .map(|i| ContractCode::new(near_test_contracts::sized_contract((i + 1) * 100), None))
        .collect_vec();
    let method_name = "main".to_owned();
    let args = vec![];

    let start_height = get_head_height(&mut env);

    for contract in contracts.iter() {
        do_deploy_contract(&mut env, &rpc_id, &contract_id, contract.code().to_vec());

        run_until_caches_contain_contract(&mut env, contract.hash());

        do_call_contract(
            &mut env,
            &rpc_id,
            &contract_id,
            &contract_id,
            method_name.clone(),
            args.clone(),
        );
        do_call_contract(
            &mut env,
            &rpc_id,
            &contract_id,
            &contract_id,
            method_name.clone(),
            args.clone(),
        );
    }

    do_delete_account(&mut env, &rpc_id, &contract_id, &rpc_id);

    let end_height = get_head_height(&mut env);
    assert_all_chunk_endorsements_received(&mut env, start_height, end_height);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
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
